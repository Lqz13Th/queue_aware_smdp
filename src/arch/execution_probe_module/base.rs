use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use rand::{Rng, SeedableRng, rngs::StdRng};
use serde_json::{Value, json};
use tokio::{
    process::Command,
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};
use tracing::error;

use extrema_infra::{
    arch::market_assets::exchange::prelude::{
        BinanceUmCli, HyperliquidCli, OkxCli, hyperliquid_perp_asset_id_for_dex,
    },
    arch::traits::market_lob::{LobPrivateRest, LobWebsocket},
    errors::{InfraError, InfraResult},
    prelude::*,
};

use crate::arch::{
    action_gateway::{
        ActionContext, ActionGateway, PlaceOutcome, action_channel, cancel_succeeded,
        parse_post_frame, place_outcome,
    },
    schema::{
        AccountSnapshotRecord, DecisionRecord, EventEnvelope, RawEventRecord, SystemRecord,
        empty_envelope,
    },
    storage::{StorageHandle, StorageStream},
};

use super::{
    state::{
        AccountOpenOrder, AccountState, ActiveProbe, ConnectionState, FeedPlan, MarketState,
        OrderUpdate, PendingMarkout, ProbePhase, ProbeSide, QueueBelief, UserFill,
        exchange_timestamp_ns, frame_channel, parse_binance_bbo, parse_hl_book, parse_hl_trades,
        parse_okx_bbo, parse_order_updates, parse_user_fills, parse_xyz_inventory,
    },
    utils::{
        AppConfig, ProcessClock, TASK_BINANCE_AGG_TRADE, TASK_BINANCE_LOB, TASK_HL_ACTION,
        TASK_HL_FAST_L2, TASK_HL_PRIVATE, TASK_HL_STANDARD, TASK_OKX_LOB, TASK_OKX_TRADES_ALL,
        TASK_PROBE_TIMER, hyperliquid_tick_size, make_cloid, minimum_probe_size, reduce_only_size,
        round_price_to_tick, wall_time_ns,
    },
};

#[derive(Clone)]
pub struct RuntimeIdentity {
    pub run_id: String,
    pub host_id: String,
    pub build_commit: String,
    pub clock: ProcessClock,
}

pub struct BuiltProbe {
    pub strategy: ExecutionProbe,
    pub ws_tasks: Vec<WsTaskInfo>,
    pub background_tasks: Vec<JoinHandle<InfraResult<()>>>,
    pub control: ProbeRuntimeControl,
}

pub struct ProbeRuntimeControl {
    pub shutdown: watch::Sender<bool>,
    pub shutdown_complete: mpsc::Receiver<()>,
    pub stop_workers: watch::Sender<bool>,
}

#[derive(Clone, Debug)]
struct RestRequest {
    reason: String,
    scope: RestScope,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RestScope {
    Reconciliation,
    Full,
}

#[derive(Clone, Debug, Default)]
struct RestRequestGate {
    pending: bool,
    last_request_ns: u64,
}

impl RestRequestGate {
    fn try_begin(&mut self, now_ns: u64, minimum_interval_ns: u64) -> bool {
        if self.pending || now_ns.saturating_sub(self.last_request_ns) < minimum_interval_ns {
            return false;
        }
        self.pending = true;
        self.last_request_ns = now_ns;
        true
    }

    fn complete(&mut self) {
        self.pending = false;
    }
}

#[derive(Clone)]
struct RestHandle {
    requests: mpsc::Sender<RestRequest>,
    latest: watch::Receiver<AccountState>,
}

#[derive(Clone)]
pub struct ExecutionProbe {
    registry: Arc<CommandRegistry>,
    config: Arc<AppConfig>,
    identity: RuntimeIdentity,
    storage: StorageHandle,
    feed_plans: Arc<HashMap<u64, FeedPlan>>,
    connections: HashMap<u64, ConnectionState>,
    market: MarketState,
    account: AccountState,
    rest: RestHandle,
    gateway: ActionGateway,
    active_probe: Option<ActiveProbe>,
    pending_markouts: Vec<PendingMarkout>,
    seen_fill_ids: HashSet<String>,
    rng: StdRng,
    event_sequence: u64,
    probe_sequence: u64,
    decision_sequence: u64,
    action_sequence: u64,
    cooldown_until_ns: u64,
    rest_request_gate: RestRequestGate,
    shutdown: watch::Receiver<bool>,
    shutdown_complete: mpsc::Sender<()>,
    shutdown_started: bool,
    shutdown_snapshot_requested_ns: Option<u64>,
    shutdown_notified: bool,
}

pub async fn build_probe(
    config: Arc<AppConfig>,
    identity: RuntimeIdentity,
    storage: StorageHandle,
) -> InfraResult<BuiltProbe> {
    let mut hl = HyperliquidCli::default();
    hl.set_perp_dex(Some(config.markets.hyperliquid_perp_dex.clone()));
    hl.init_inst_index_map().await?;
    hl.init_api_key();
    let auth = hl.auth.clone().ok_or_else(|| {
        InfraError::Msg(
            "Hyperliquid private collection requires HYPERLIQUID_OWNER_ADDRESS and HYPERLIQUID_AGENT_PRIVATE_KEY"
                .into(),
        )
    })?;
    let asset_id = resolve_asset_id(&hl, &config.markets.hyperliquid_instrument)?;
    let account_address = auth
        .vault_address
        .as_deref()
        .unwrap_or(&auth.owner_address)
        .to_string();
    let plans = build_feed_plans(&config, &hl, &account_address).await?;
    let ws_tasks = plans.iter().map(feed_task).collect();
    let feed_plans = Arc::new(
        plans
            .into_iter()
            .map(|plan| (plan.task_id, plan))
            .collect::<HashMap<_, _>>(),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
    let (stop_workers_tx, stop_workers_rx) = watch::channel(false);
    let (rest, rest_task) = start_rest_worker(
        hl,
        Arc::clone(&config),
        identity.clone(),
        storage.clone(),
        stop_workers_rx.clone(),
    );
    let system_task = start_system_worker(
        Arc::clone(&config),
        identity.clone(),
        storage.clone(),
        stop_workers_rx,
    );
    let strategy = ExecutionProbe {
        registry: Arc::new(CommandRegistry::default()),
        config: Arc::clone(&config),
        identity: identity.clone(),
        storage,
        feed_plans,
        connections: HashMap::new(),
        market: MarketState::default(),
        account: AccountState::default(),
        rest,
        gateway: ActionGateway::new(auth, asset_id, identity.clock.clone()),
        active_probe: None,
        pending_markouts: Vec::new(),
        seen_fill_ids: HashSet::new(),
        rng: StdRng::seed_from_u64(config.probe.behavior_seed),
        event_sequence: 0,
        probe_sequence: 0,
        decision_sequence: 0,
        action_sequence: 0,
        cooldown_until_ns: 0,
        rest_request_gate: RestRequestGate::default(),
        shutdown: shutdown_rx,
        shutdown_complete: shutdown_complete_tx,
        shutdown_started: false,
        shutdown_snapshot_requested_ns: None,
        shutdown_notified: false,
    };
    Ok(BuiltProbe {
        strategy,
        ws_tasks,
        background_tasks: vec![rest_task, system_task],
        control: ProbeRuntimeControl {
            shutdown: shutdown_tx,
            shutdown_complete: shutdown_complete_rx,
            stop_workers: stop_workers_tx,
        },
    })
}

impl ExecutionProbe {
    pub(crate) async fn initialize_module(&mut self) {
        self.request_rest("process_start", RestScope::Full);
        if let Err(err) = self
            .record_system(
                "process_start",
                json!({
                    "probe_enabled": self.config.probe.enabled,
                    "feed_count": self.feed_plans.len(),
                    "action_asset_id": self.gateway.asset_id(),
                }),
            )
            .await
        {
            error!(?err, "failed to record process start");
        }
    }

    pub(crate) fn initialize_command_registry(&mut self, registry: Arc<CommandRegistry>) {
        self.registry = registry.clone();
        self.gateway.set_registry(registry);
    }

    pub(crate) fn current_command_registry(&self) -> Arc<CommandRegistry> {
        self.registry.clone()
    }

    pub(crate) async fn handle_ws_event(&mut self, msg: InfraMsg<WsTaskInfo>) {
        let Some(plan) = self.feed_plans.get(&msg.task_id).cloned() else {
            return;
        };
        if let Err(err) = self.connect_feed(plan).await {
            error!(
                task_id = msg.task_id,
                ?err,
                "websocket connect sequence failed"
            );
            let _ = self
                .record_system(
                    "connection_error",
                    json!({"task_id": msg.task_id, "error": err.to_string()}),
                )
                .await;
        }
    }

    pub(crate) async fn handle_ws_other(&mut self, msg: InfraMsg<Vec<WsOtherMessage>>) {
        for raw in msg.data.iter() {
            if let Err(err) = self.handle_raw_frame(msg.task_id, raw).await {
                error!(
                    task_id = msg.task_id,
                    ?err,
                    "raw websocket frame handling failed"
                );
                let _ = self
                    .record_system(
                        "frame_processing_error",
                        json!({"task_id": msg.task_id, "error": err.to_string()}),
                    )
                    .await;
            }
        }
    }

    pub(crate) async fn handle_schedule(&mut self, msg: InfraMsg<AltScheduleEvent>) {
        if msg.task_id != TASK_PROBE_TIMER {
            return;
        }
        if let Err(err) = self.on_timer().await {
            error!(?err, "probe timer failed");
            let _ = self
                .record_system("timer_error", json!({"error": err.to_string()}))
                .await;
        }
    }
}

impl ExecutionProbe {
    async fn connect_feed(&mut self, plan: FeedPlan) -> InfraResult<()> {
        let started_ns = self.identity.clock.monotonic_ns();
        self.market.clear_reconnecting_feed(plan.feed);
        let (connection_id, reconnect_index) = {
            let state = self.connections.entry(plan.task_id).or_default();
            state.generation = state.generation.saturating_add(1);
            state.frame_seq = 0;
            state.connection_id = format!("{}-{}", plan.feed, state.generation);
            state.last_receive_monotonic_ns = None;
            (state.connection_id.clone(), state.generation)
        };
        if plan.task_id == TASK_HL_PRIVATE && reconnect_index > 1 {
            if let Some(probe) = self.active_probe.as_mut() {
                probe.terminal_order_update_monotonic_ns = None;
            }
            self.record_system(
                "private_ws_recovery_evidence_invalidated",
                json!({"reconnect_index": reconnect_index}),
            )
            .await?;
        }
        let handle = self
            .find_ws_handle(&plan.channel, plan.task_id)
            .ok_or_else(|| InfraError::Msg(format!("missing WS handle for {}", plan.feed)))?;
        self.record_system(
            "connection_start",
            json!({
                "task_id": plan.task_id,
                "source": plan.source,
                "feed": plan.feed,
                "connection_id": connection_id,
                "reconnect_index": reconnect_index,
            }),
        )
        .await?;
        let (connect_tx, connect_rx) = oneshot::channel();
        handle
            .send_command(
                TaskCommand::WsConnect {
                    msg: plan.url,
                    ack: AckHandle::new(connect_tx),
                },
                Some((AckStatus::WsConnect, connect_rx)),
            )
            .await?;
        for subscription in &plan.subscriptions {
            let (send_tx, send_rx) = oneshot::channel();
            handle
                .send_command(
                    TaskCommand::WsMessage {
                        msg: subscription.clone(),
                        ack: AckHandle::new(send_tx),
                    },
                    Some((AckStatus::WsMessage, send_rx)),
                )
                .await?;
        }
        self.record_system(
            "connection_ready",
            json!({
                "task_id": plan.task_id,
                "source": plan.source,
                "feed": plan.feed,
                "connection_id": connection_id,
                "subscriptions": plan.subscriptions.len(),
                "elapsed_ms": self.identity.clock.monotonic_ns().saturating_sub(started_ns) as f64 / 1_000_000.0,
            }),
        )
        .await
    }

    async fn handle_raw_frame(&mut self, task_id: u64, raw: &WsOtherMessage) -> InfraResult<()> {
        let received_monotonic_ns = self.identity.clock.monotonic_ns();
        let received_wall_ns = raw.timestamp.saturating_mul(1_000);
        let parsed: Value = serde_json::from_str(&raw.raw_json)
            .map_err(|err| InfraError::Msg(format!("decode raw JSON: {err}")))?;
        let decoded_monotonic_ns = self.identity.clock.monotonic_ns();
        let plan = self
            .feed_plans
            .get(&task_id)
            .cloned()
            .ok_or_else(|| InfraError::Msg(format!("unknown raw task id {task_id}")))?;
        let (connection_id, frame_seq) = {
            let state = self.connections.entry(task_id).or_default();
            state.frame_seq = state.frame_seq.saturating_add(1);
            state.last_receive_monotonic_ns = Some(received_monotonic_ns);
            (state.connection_id.clone(), state.frame_seq)
        };
        let mut envelope = self.new_envelope(
            &format!("raw_{}", plan.feed),
            received_wall_ns,
            received_monotonic_ns,
        );
        envelope.decoded_monotonic_ns = decoded_monotonic_ns;
        envelope.exchange_ts_ns = exchange_timestamp_ns(&parsed);
        envelope.connection_id = Some(connection_id);
        envelope.connection_frame_seq = Some(frame_seq);
        envelope.raw_json = Some(raw.raw_json.clone());
        let record = RawEventRecord {
            envelope,
            source: plan.source.into(),
            feed: plan.feed.into(),
            channel: frame_channel(&parsed).map(str::to_string),
        };
        let stream = if plan.is_action {
            StorageStream::Actions
        } else if plan.is_private {
            StorageStream::PrivateWs
        } else {
            StorageStream::PublicMarket
        };
        self.storage.record(stream, &record).await?;
        if plan.is_action {
            self.process_post_response(&parsed).await?;
        } else if plan.is_private {
            self.process_private_frame(&parsed, received_monotonic_ns)
                .await?;
        } else {
            self.process_public_frame(task_id, &parsed, received_monotonic_ns)
                .await?;
        }
        Ok(())
    }

    async fn process_public_frame(
        &mut self,
        task_id: u64,
        value: &Value,
        received_ns: u64,
    ) -> InfraResult<()> {
        match task_id {
            TASK_HL_STANDARD => match frame_channel(value) {
                Some("bbo") => {
                    if let Some(book) = parse_hl_book(value, received_ns) {
                        self.market.hl_bbo = book;
                        self.market.mark_feed("hl_bbo", received_ns);
                    }
                }
                Some("l2Book") => {
                    if let Some(book) = parse_hl_book(value, received_ns) {
                        self.market.hl_default_l2 = book;
                        self.market.mark_feed("hl_default_l2", received_ns);
                        self.update_queue_from_book();
                    }
                }
                Some("trades") => {
                    let trades = parse_hl_trades(value);
                    if !trades.is_empty() {
                        self.market.mark_feed("hl_trades", received_ns);
                    }
                    for (side, price, size) in trades {
                        self.market.hl_trade_count = self.market.hl_trade_count.saturating_add(1);
                        self.market.hl_signed_trade_size += side.order_sign() * size;
                        if let Some(probe) = self.active_probe.as_mut()
                            && matches!(probe.phase, ProbePhase::Resting | ProbePhase::Partial)
                            && trade_consumes_queue(probe.side, side)
                            && (price - probe.price).abs() <= probe.tick_size * 0.1
                        {
                            probe.queue_belief.observe_trade(size);
                        }
                    }
                }
                Some("activeAssetCtx") => self.market.mark_feed("hl_active_asset_ctx", received_ns),
                _ => {}
            },
            TASK_HL_FAST_L2 => {
                if frame_channel(value) == Some("l2Book")
                    && let Some(book) = parse_hl_book(value, received_ns)
                {
                    self.market.hl_fast_l2 = book;
                    self.market.mark_feed("hl_fast_l2", received_ns);
                    self.update_queue_from_book();
                }
            }
            TASK_BINANCE_LOB | TASK_BINANCE_AGG_TRADE => {
                if let Some((bid, ask)) = parse_binance_bbo(value) {
                    self.market.binance_bid = Some(bid);
                    self.market.binance_ask = Some(ask);
                    self.market.mark_feed("binance_bbo", received_ns);
                }
                match value.get("e").and_then(Value::as_str) {
                    Some("depthUpdate") => self.market.mark_feed("binance_depth", received_ns),
                    Some("aggTrade") => self.market.mark_feed("binance_agg_trade", received_ns),
                    _ => {}
                }
            }
            TASK_OKX_LOB | TASK_OKX_TRADES_ALL => {
                if let Some((bid, ask)) = parse_okx_bbo(value) {
                    self.market.okx_bid = Some(bid);
                    self.market.okx_ask = Some(ask);
                    self.market.mark_feed("okx_bbo", received_ns);
                }
                match value.pointer("/arg/channel").and_then(Value::as_str) {
                    Some("books") => self.market.mark_feed("okx_books", received_ns),
                    Some("trades-all") => self.market.mark_feed("okx_trades_all", received_ns),
                    _ => {}
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn process_private_frame(&mut self, value: &Value, received_ns: u64) -> InfraResult<()> {
        match frame_channel(value) {
            Some("orderUpdates") => {
                for update in parse_order_updates(value) {
                    self.process_order_update(update, received_ns).await?;
                }
            }
            Some("userFills") => {
                for fill in parse_user_fills(value) {
                    self.process_user_fill(fill, received_ns).await?;
                }
            }
            Some("clearinghouseState") => {
                if let Some(inventory) =
                    parse_xyz_inventory(value, &self.config.markets.hyperliquid_coin)
                {
                    self.account.confirmed_inventory = inventory;
                    self.account.snapshot_monotonic_ns = received_ns;
                    self.maybe_finish_flatten().await?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn process_post_response(&mut self, value: &Value) -> InfraResult<()> {
        let Some((request_id, frame)) = parse_post_frame(value)? else {
            return Ok(());
        };
        let Some((place_request, cancel_request, flatten_request)) =
            self.active_probe.as_ref().map(|probe| {
                (
                    probe.post_request_id,
                    probe.cancel_request_id,
                    probe.flatten_request_id,
                )
            })
        else {
            return self
                .record_system("orphan_post_response", json!({"request_id": request_id}))
                .await;
        };
        if request_id == place_request {
            match place_outcome(frame)? {
                PlaceOutcome::Resting(oid) => {
                    if let Some(probe) = self.active_probe.as_mut() {
                        probe.oid = Some(oid);
                        probe.phase = ProbePhase::PostAccepted;
                    }
                    self.record_system(
                        "place_post_accepted",
                        json!({"request_id": request_id, "oid": oid}),
                    )
                    .await?;
                }
                PlaceOutcome::Filled(oid) => {
                    if let Some(probe) = self.active_probe.as_mut() {
                        probe.oid = Some(oid);
                        probe.phase = ProbePhase::Filled;
                    }
                    self.record_system(
                        "place_immediate_fill",
                        json!({"request_id": request_id, "oid": oid}),
                    )
                    .await?;
                }
                PlaceOutcome::Rejected(reason) => {
                    self.record_system(
                        "place_rejected",
                        json!({"request_id": request_id, "reason": reason}),
                    )
                    .await?;
                    self.record_lifecycle_decision(
                        "own_order_ack",
                        json!({"action":"terminate","reason":"place_rejected"}),
                        vec!["exchange_rejected_place".into()],
                    )
                    .await?;
                    self.finish_probe("place_rejected").await?;
                }
            }
        } else if cancel_request == Some(request_id) {
            cancel_succeeded(frame)?;
            self.record_system("cancel_post_accepted", json!({"request_id": request_id}))
                .await?;
        } else if flatten_request == Some(request_id) {
            let outcome = place_outcome(frame)?;
            self.record_system(
                "flatten_post_response",
                json!({"request_id": request_id, "outcome": format!("{outcome:?}")}),
            )
            .await?;
            let needs_reconciliation = match outcome {
                PlaceOutcome::Filled(oid) => {
                    if let Some(probe) = self.active_probe.as_mut() {
                        probe.flatten_oid = Some(oid);
                    }
                    false
                }
                PlaceOutcome::Resting(oid) => {
                    if let Some(probe) = self.active_probe.as_mut() {
                        probe.flatten_oid = Some(oid);
                        probe.phase = ProbePhase::Recovering;
                        probe.recovery_not_before_monotonic_ns = self.identity.clock.monotonic_ns();
                    }
                    self.record_system(
                        "flatten_ioc_unexpected_resting",
                        json!({"request_id": request_id, "oid": oid}),
                    )
                    .await?;
                    true
                }
                PlaceOutcome::Rejected(reason) => {
                    if let Some(probe) = self.active_probe.as_mut() {
                        probe.phase = ProbePhase::Recovering;
                        probe.flatten_request_id = None;
                        probe.flatten_send_monotonic_ns = None;
                        probe.recovery_not_before_monotonic_ns = self.identity.clock.monotonic_ns();
                    }
                    self.record_system(
                        "flatten_ioc_rejected",
                        json!({"request_id": request_id, "reason": reason}),
                    )
                    .await?;
                    true
                }
            };
            if needs_reconciliation {
                self.request_rest("flatten_post_response", RestScope::Reconciliation);
            }
        } else {
            self.record_system("unmatched_post_response", json!({"request_id": request_id}))
                .await?;
        }
        Ok(())
    }

    async fn process_order_update(
        &mut self,
        update: OrderUpdate,
        received_ns: u64,
    ) -> InfraResult<()> {
        if update.coin != self.config.markets.hyperliquid_coin {
            return Ok(());
        }
        let matches = self.active_probe.as_ref().is_some_and(|probe| {
            update.cloid.as_deref() == Some(probe.cloid.as_str()) || probe.oid == Some(update.oid)
        });
        if !matches {
            return Ok(());
        }
        let status = update.status.to_ascii_lowercase();
        if status == "open" || status == "triggered" {
            let first_open = self
                .active_probe
                .as_ref()
                .is_some_and(|probe| probe.open_monotonic_ns.is_none());
            let now_ns = self.identity.clock.monotonic_ns();
            if let Some(probe) = self.active_probe.as_mut() {
                probe.oid = Some(update.oid);
                probe.remaining_size = update.remaining_size;
                probe.cumulative_filled_size =
                    (update.original_size - update.remaining_size).max(0.0);
                probe.phase = if probe.cumulative_filled_size > 0.0 {
                    ProbePhase::Partial
                } else {
                    ProbePhase::Resting
                };
                if first_open {
                    probe.open_monotonic_ns = Some(now_ns);
                    let tick = probe.tick_size;
                    let level = self
                        .market
                        .book_for_queue()
                        .level(probe.side, probe.price, tick);
                    probe.queue_belief = QueueBelief::initialize(
                        level.map(|level| level.size).unwrap_or_default(),
                        level.and_then(|level| level.order_count),
                        probe.remaining_size,
                    );
                    probe.initial_queue_belief = Some(probe.queue_belief.clone());
                }
            }
            if first_open {
                self.record_keep("own_order_open", vec!["acknowledged_resting_order".into()])
                    .await?;
            }
        } else if status == "filled" {
            if let Some(probe) = self.active_probe.as_mut() {
                probe.oid = Some(update.oid);
                probe.remaining_size = 0.0;
                probe.cumulative_filled_size = probe
                    .cumulative_filled_size
                    .max(update.original_size - update.remaining_size);
                probe.phase = ProbePhase::MarkoutPending;
            }
        } else if status.contains("cancel") {
            let update_filled_size = (update.original_size - update.remaining_size).max(0.0);
            let has_fill = self
                .active_probe
                .as_ref()
                .is_some_and(|probe| probe.cumulative_filled_size.max(update_filled_size) > 0.0);
            self.record_system(
                "cancel_effective",
                json!({
                    "oid": update.oid,
                    "status_timestamp_ms": update.status_timestamp_ms,
                    "has_fill": has_fill,
                }),
            )
            .await?;
            if has_fill {
                if let Some(probe) = self.active_probe.as_mut() {
                    probe.remaining_size = 0.0;
                    probe.cumulative_filled_size =
                        probe.cumulative_filled_size.max(update_filled_size);
                    probe.phase = ProbePhase::MarkoutPending;
                }
            } else {
                if let Some(probe) = self.active_probe.as_mut() {
                    probe.remaining_size = 0.0;
                    probe.phase = ProbePhase::Recovering;
                    probe.terminal_order_update_monotonic_ns = Some(received_ns);
                    probe.recovery_not_before_monotonic_ns = self
                        .identity
                        .clock
                        .monotonic_ns()
                        .saturating_add(self.config.probe.cancel_timeout_ms * 1_000_000);
                }
                self.record_lifecycle_decision(
                    "cancel_effective",
                    json!({"action":"reconcile"}),
                    vec!["cancel_confirmed_wait_for_late_fills".into()],
                )
                .await?;
            }
        } else if status.contains("reject") {
            self.record_lifecycle_decision(
                "own_order_ack",
                json!({"action":"terminate","reason":"order_rejected"}),
                vec!["order_update_rejected".into()],
            )
            .await?;
            self.finish_probe("order_rejected").await?;
        }
        Ok(())
    }

    async fn process_user_fill(&mut self, fill: UserFill, received_ns: u64) -> InfraResult<()> {
        if fill.coin != self.config.markets.hyperliquid_coin {
            return Ok(());
        }
        let fill_id = format!("{}:{}", fill.oid, fill.trade_id);
        if !self.seen_fill_ids.insert(fill_id.clone()) {
            return Ok(());
        }
        let flatten_link = self.active_probe.as_ref().and_then(|probe| {
            (probe.flatten_oid == Some(fill.oid))
                .then(|| (probe.probe_id.clone(), probe.flatten_action_id.clone()))
        });
        if let Some((probe_id, action_id)) = flatten_link {
            self.account.confirmed_inventory += fill.side.order_sign() * fill.size;
            self.account.snapshot_monotonic_ns = received_ns;
            self.record_system_to(
                StorageStream::PrivateWs,
                "flatten_fill_link",
                json!({
                    "probe_id": probe_id,
                    "action_id": action_id,
                    "oid": fill.oid,
                    "fill_id": fill_id,
                    "price": fill.price,
                    "size": fill.size,
                    "side": fill.side,
                    "fee": fill.fee,
                    "fee_token": fill.fee_token,
                    "exchange_time_ms": fill.timestamp_ms,
                    "raw_fill": fill.raw,
                }),
            )
            .await?;
            self.maybe_finish_flatten().await?;
            if self.active_probe.is_some() {
                self.request_rest("flatten_user_fill", RestScope::Reconciliation);
            }
            return Ok(());
        }
        let matches = self
            .active_probe
            .as_ref()
            .is_some_and(|probe| probe.oid == Some(fill.oid));
        if !matches {
            return self
                .record_system(
                    "unmatched_user_fill",
                    json!({"oid": fill.oid, "trade_id": fill.trade_id}),
                )
                .await;
        }
        let inventory_before = self.account.confirmed_inventory;
        let fair_value = self.market.fair_value();
        let (
            probe_id,
            decision_id,
            action_id,
            cloid,
            should_cancel,
            side,
            order_age_ms,
            queue_belief,
            cumulative_filled_size,
            remaining_size,
        ) = {
            let probe = self.active_probe.as_mut().expect("checked active probe");
            let order_age_ms = received_ns
                .saturating_sub(probe.open_monotonic_ns.unwrap_or(probe.send_monotonic_ns))
                / 1_000_000;
            probe.first_fill_monotonic_ns.get_or_insert(received_ns);
            probe.cumulative_filled_size += fill.size;
            probe.remaining_size = (probe.remaining_size - fill.size).max(0.0);
            probe.queue_belief.observe_partial_fill();
            probe.phase = if probe.remaining_size > 0.0 {
                ProbePhase::Partial
            } else {
                ProbePhase::MarkoutPending
            };
            (
                probe.probe_id.clone(),
                probe.decision_id.clone(),
                probe.action_id.clone(),
                probe.cloid.clone(),
                probe.remaining_size > 0.0 && probe.cancel_request_id.is_none(),
                probe.side,
                order_age_ms,
                probe.queue_belief.clone(),
                probe.cumulative_filled_size,
                probe.remaining_size,
            )
        };
        self.account.confirmed_inventory += side.order_sign() * fill.size;
        self.account.snapshot_monotonic_ns = received_ns;
        let inventory_after = self.account.confirmed_inventory;
        let spread_edge_at_fill = fair_value.map(|fair| match side {
            ProbeSide::Bid => fair - fill.price,
            ProbeSide::Ask => fill.price - fair,
        });
        self.pending_markouts.push(PendingMarkout {
            probe_id: probe_id.clone(),
            oid: fill.oid,
            fill_id: fill_id.clone(),
            side: fill.side,
            fill_price: fill.price,
            fill_size: fill.size,
            fill_monotonic_ns: received_ns,
            horizons_ms: self.config.probe.markout_horizons_ms.clone(),
            next_horizon_index: 0,
        });
        self.record_system_to(
            StorageStream::PrivateWs,
            "fill_link",
            json!({
                "probe_id": probe_id,
                "decision_id": decision_id,
                "action_id": action_id,
                "cloid": cloid,
                "oid": fill.oid,
                "fill_id": fill_id,
                "price": fill.price,
                "size": fill.size,
                "side": fill.side,
                "fee": fill.fee,
                "fee_token": fill.fee_token,
                "fair_value": fair_value,
                "spread_edge_at_fill": spread_edge_at_fill,
                "maker_fee_rate": self.account.maker_fee,
                "priority_fee": 0,
                "inventory_before": inventory_before,
                "inventory_after": inventory_after,
                "cumulative_filled_size": cumulative_filled_size,
                "remaining_size_after_fill": remaining_size,
                "order_age_ms": order_age_ms,
                "queue_belief": queue_belief,
                "exchange_time_ms": fill.timestamp_ms,
                "raw_fill": fill.raw,
            }),
        )
        .await?;
        self.request_rest("user_fill", RestScope::Reconciliation);
        if should_cancel {
            self.cancel_active(
                "partial_fill",
                vec!["cancel_remainder_after_partial".into()],
            )
            .await?;
        } else {
            self.record_lifecycle_decision(
                "fill",
                json!({"action":"wait_markout"}),
                vec!["full_fill_wait_for_markout_horizons".into()],
            )
            .await?;
        }
        Ok(())
    }

    async fn on_timer(&mut self) -> InfraResult<()> {
        self.refresh_account_state().await?;
        if *self.shutdown.borrow() {
            return self.drive_shutdown().await;
        }
        let now_ns = self.identity.clock.monotonic_ns();
        let account_interval_ns = self
            .config
            .collector
            .account_snapshot_interval_sec
            .saturating_mul(1_000_000_000);
        if now_ns.saturating_sub(self.rest_request_gate.last_request_ns) >= account_interval_ns {
            self.request_rest("periodic", RestScope::Full);
        }
        self.sample_markouts().await?;
        self.evaluate_active_probe().await?;
        if self.active_probe.is_none() {
            self.maybe_start_probe().await?;
        }
        Ok(())
    }

    async fn drive_shutdown(&mut self) -> InfraResult<()> {
        if !self.shutdown_started {
            self.shutdown_started = true;
            if !self.pending_markouts.is_empty() {
                let _ = self
                    .record_system_to(
                        StorageStream::PrivateWs,
                        "markouts_censored",
                        json!({
                            "reason": "manual_stop",
                            "pending_fill_count": self.pending_markouts.len(),
                        }),
                    )
                    .await;
                self.pending_markouts.clear();
            }
            let _ = self
                .record_system("shutdown_requested", json!({"signal": true}))
                .await;
        }

        let phase = self.active_probe.as_ref().map(|probe| probe.phase);
        match phase {
            Some(ProbePhase::Resting | ProbePhase::Partial | ProbePhase::PostAccepted) => {
                self.cancel_active("manual_stop", vec!["shutdown_requested".into()])
                    .await?;
                return Ok(());
            }
            Some(ProbePhase::PlacePending) => {
                if let Some(probe) = self.active_probe.as_mut() {
                    probe.phase = ProbePhase::Recovering;
                    probe.recovery_not_before_monotonic_ns = self.identity.clock.monotonic_ns();
                }
                self.request_rest_throttled(
                    "shutdown_place_recovery",
                    RestScope::Reconciliation,
                    self.identity.clock.monotonic_ns(),
                );
                return Ok(());
            }
            Some(ProbePhase::Filled | ProbePhase::MarkoutPending) => {
                self.maybe_flatten(true).await?;
                return Ok(());
            }
            Some(
                ProbePhase::CancelPending | ProbePhase::FlattenPending | ProbePhase::Recovering,
            ) => {
                self.evaluate_active_probe().await?;
                if self.active_probe.is_some() {
                    self.request_rest_throttled(
                        "shutdown_reconciliation",
                        RestScope::Reconciliation,
                        self.identity.clock.monotonic_ns(),
                    );
                }
                return Ok(());
            }
            None => {}
        }

        let now_ns = self.identity.clock.monotonic_ns();
        let requested_ns = match self.shutdown_snapshot_requested_ns {
            Some(requested_ns) => requested_ns,
            None => {
                if self.request_rest("process_shutdown", RestScope::Full) {
                    self.shutdown_snapshot_requested_ns = Some(now_ns);
                }
                return Ok(());
            }
        };
        if self.account.snapshot_monotonic_ns < requested_ns || self.shutdown_notified {
            return Ok(());
        }

        let record_result = self
            .record_system(
                "process_stop",
                json!({
                    "open_order_count": self.account.open_order_count,
                    "confirmed_inventory": self.account.confirmed_inventory,
                    "account_snapshot_monotonic_ns": self.account.snapshot_monotonic_ns,
                }),
            )
            .await;
        self.shutdown_notified = true;
        let _ = self.shutdown_complete.try_send(());
        record_result
    }

    async fn refresh_account_state(&mut self) -> InfraResult<()> {
        if self.rest.latest.has_changed().unwrap_or(false) {
            let latest = self.rest.latest.borrow_and_update().clone();
            self.rest_request_gate.complete();
            if latest.rest_snapshot_healthy
                && latest.snapshot_monotonic_ns >= self.account.snapshot_monotonic_ns
            {
                self.account = latest;
                self.reconcile_from_rest().await?;
            } else if !latest.rest_snapshot_healthy {
                if self
                    .shutdown_snapshot_requested_ns
                    .is_some_and(|requested_ns| latest.snapshot_monotonic_ns >= requested_ns)
                {
                    self.shutdown_snapshot_requested_ns = None;
                }
                self.record_system(
                    "rest_snapshot_unhealthy",
                    json!({"snapshot_monotonic_ns": latest.snapshot_monotonic_ns}),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn reconcile_from_rest(&mut self) -> InfraResult<()> {
        let Some(probe) = self.active_probe.as_ref() else {
            return Ok(());
        };
        if probe.phase != ProbePhase::Recovering {
            return self.maybe_finish_flatten().await;
        }
        if self.identity.clock.monotonic_ns() < probe.recovery_not_before_monotonic_ns {
            return Ok(());
        }
        let recovered_order = self.account.open_orders.iter().find(|order| {
            Some(order.oid) == probe.oid
                || Some(order.oid) == probe.flatten_oid
                || order.cloid.as_deref() == Some(probe.cloid.as_str())
        });
        if let Some(order) = recovered_order {
            let oid = order.oid;
            if let Some(probe) = self.active_probe.as_mut() {
                probe.oid = Some(oid);
                probe.cancel_request_id = None;
            }
            self.cancel_active("rest_recovery", vec!["order_still_open".into()])
                .await?;
        } else if self.account.confirmed_inventory.abs()
            < self.account.lot_size.unwrap_or(0.0001) * 0.5
        {
            self.finish_probe("rest_inventory_reconciled").await?;
        } else if probe.cumulative_filled_size > 0.0 {
            if let Some(probe) = self.active_probe.as_mut() {
                probe.phase = ProbePhase::MarkoutPending;
                probe.remaining_size = 0.0;
                probe.flatten_request_id = None;
                probe.flatten_send_monotonic_ns = None;
            }
        } else {
            self.finish_probe("rest_reconciled_absent").await?;
        }
        Ok(())
    }

    async fn maybe_start_probe(&mut self) -> InfraResult<()> {
        if !self.config.probe.enabled {
            return Ok(());
        }
        let now_ns = self.identity.clock.monotonic_ns();
        if now_ns < self.cooldown_until_ns || !self.pending_markouts.is_empty() {
            return Ok(());
        }
        let freshness_ns = self.config.probe.market_freshness_ms * 1_000_000;
        let default_l2_freshness_ns = self.config.probe.default_l2_freshness_ms * 1_000_000;
        if !self
            .market
            .required_quotes_fresh(now_ns, freshness_ns, default_l2_freshness_ns)
            || !self.all_connections_started()
            || !self.account.rest_snapshot_healthy
            || self.account.snapshot_monotonic_ns == 0
            || now_ns.saturating_sub(self.account.snapshot_monotonic_ns)
                > self.config.collector.account_snapshot_interval_sec * 2 * 1_000_000_000
            || self.account.open_order_count != 0
            || self
                .account
                .withdrawable
                .is_none_or(|withdrawable| withdrawable < self.config.probe.max_order_notional_usd)
            || self.account.lot_size.is_none()
            || self.account.size_decimals.is_none()
            || self.account.confirmed_inventory.abs()
                > self.account.lot_size.unwrap_or(0.0001) * 0.5
            || self
                .account
                .action_budget_remaining()
                .is_none_or(|remaining| remaining < 10)
            || self.storage.remaining_capacity()
                < (self.config.collector.writer_capacity / 2).max(1)
        {
            return Ok(());
        }
        let (Some(best_bid), Some(best_ask)) =
            (self.market.hl_bbo.best_bid(), self.market.hl_bbo.best_ask())
        else {
            return Ok(());
        };
        let fair_value = (best_bid + best_ask) * 0.5;
        let Some(size_decimals) = self.account.size_decimals else {
            return Ok(());
        };
        let Some(tick) = hyperliquid_tick_size(fair_value, size_decimals) else {
            return Ok(());
        };
        let lot_size = self.account.lot_size.unwrap_or(0.0001);
        let Some(size_text) =
            minimum_probe_size(self.config.probe.probe_notional_usd, fair_value, lot_size)
        else {
            return Ok(());
        };
        let size = size_text
            .parse::<f64>()
            .map_err(|err| InfraError::Msg(format!("parse probe size: {err}")))?;
        if size * fair_value > self.config.probe.max_order_notional_usd + tick * size {
            return Err(InfraError::Msg(format!(
                "minimum valid probe notional {} exceeds configured cap {}",
                size * fair_value,
                self.config.probe.max_order_notional_usd
            )));
        }
        let candidates = self.probe_candidates();
        let selected_index = self.rng.random_range(0..candidates.len());
        let selected = candidates[selected_index].clone();
        let side = if selected["side"] == "bid" {
            ProbeSide::Bid
        } else {
            ProbeSide::Ask
        };
        let level = selected["price_level_ticks"].as_u64().unwrap_or_default() as u32;
        let dwell_sec = selected["planned_dwell_sec"].as_u64().unwrap_or(30);
        let raw_price = match side {
            ProbeSide::Bid => best_bid - f64::from(level) * tick,
            ProbeSide::Ask => best_ask + f64::from(level) * tick,
        };
        let price_text = round_price_to_tick(raw_price, tick, side == ProbeSide::Ask)
            .ok_or_else(|| InfraError::Msg("failed to round probe price".into()))?;
        let price = price_text
            .parse::<f64>()
            .map_err(|err| InfraError::Msg(format!("parse probe price: {err}")))?;

        self.probe_sequence = self.probe_sequence.saturating_add(1);
        let probe_id = format!("probe-{}", self.probe_sequence);
        let cloid = make_cloid(&self.identity.run_id, self.probe_sequence);
        let decision_id = self.next_decision_id();
        let action_id = self.next_action_id();
        let planned_expiry_ns = now_ns + dwell_sec * 1_000_000_000;
        let decision = self.build_decision(
            &probe_id,
            &decision_id,
            "timer",
            candidates.clone(),
            json!({
                "action": "place",
                "side": side,
                "price_level_ticks": level,
                "price": price_text,
                "size": size_text,
                "planned_dwell_sec": dwell_sec,
                "time_in_force": "Alo",
            }),
            1.0 / candidates.len() as f64,
            Some(wall_time_ns()?.saturating_add(dwell_sec * 1_000_000_000)),
            self.config.probe.behavior_seed,
            vec!["eligible_probe_cell".into()],
        );
        self.storage
            .record(StorageStream::Decisions, &decision)
            .await?;
        let action_envelope = self.new_event_envelope("action_dispatch")?;
        let sent = self
            .gateway
            .place_alo(
                action_envelope,
                ActionContext {
                    action_id: action_id.clone(),
                    probe_id: probe_id.clone(),
                    decision_id: decision_id.clone(),
                    intent_id: format!("intent-{action_id}"),
                    cloid: Some(cloid.clone()),
                    oid: None,
                    side: Some(side),
                    price: Some(price_text),
                    size: Some(size_text),
                    remaining_size: None,
                },
            )
            .await?;
        self.storage
            .record(StorageStream::Actions, &sent.record)
            .await?;
        self.active_probe = Some(ActiveProbe {
            probe_id,
            decision_id,
            action_id,
            cloid,
            oid: None,
            side,
            price_level_ticks: level,
            price,
            tick_size: tick,
            size,
            remaining_size: size,
            cumulative_filled_size: 0.0,
            phase: ProbePhase::PlacePending,
            created_monotonic_ns: now_ns,
            send_monotonic_ns: sent.socket_write_monotonic_ns,
            open_monotonic_ns: None,
            first_fill_monotonic_ns: None,
            planned_expiry_monotonic_ns: planned_expiry_ns,
            planned_dwell_ms: dwell_sec * 1_000,
            last_keep_monotonic_ns: now_ns,
            cancel_send_monotonic_ns: None,
            post_request_id: sent.request_id,
            cancel_request_id: None,
            flatten_request_id: None,
            flatten_oid: None,
            flatten_action_id: None,
            flatten_send_monotonic_ns: None,
            terminal_order_update_monotonic_ns: None,
            recovery_not_before_monotonic_ns: 0,
            end_reason: None,
            initial_queue_belief: None,
            queue_belief: QueueBelief::default(),
        });
        Ok(())
    }

    async fn evaluate_active_probe(&mut self) -> InfraResult<()> {
        let Some((
            phase,
            probe_id,
            send_ns,
            cancel_send_ns,
            flatten_send_ns,
            expiry_ns,
            last_keep_ns,
            quote_stale,
        )) = self.active_probe.as_ref().map(|probe| {
            (
                probe.phase,
                probe.probe_id.clone(),
                probe.send_monotonic_ns,
                probe.cancel_send_monotonic_ns,
                probe.flatten_send_monotonic_ns,
                probe.planned_expiry_monotonic_ns,
                probe.last_keep_monotonic_ns,
                self.quote_is_stale(probe),
            )
        })
        else {
            return Ok(());
        };
        let now_ns = self.identity.clock.monotonic_ns();
        if phase == ProbePhase::PlacePending
            && now_ns.saturating_sub(send_ns) >= self.config.probe.post_timeout_ms * 1_000_000
        {
            if let Some(probe) = self.active_probe.as_mut() {
                probe.phase = ProbePhase::Recovering;
                probe.recovery_not_before_monotonic_ns = now_ns;
            }
            self.request_rest("place_timeout", RestScope::Reconciliation);
            return self
                .record_system("place_timeout", json!({"probe_id": probe_id}))
                .await;
        }
        if phase == ProbePhase::CancelPending
            && cancel_send_ns.is_some_and(|sent| {
                now_ns.saturating_sub(sent) >= self.config.probe.cancel_timeout_ms * 1_000_000
            })
        {
            if let Some(probe) = self.active_probe.as_mut() {
                probe.phase = ProbePhase::Recovering;
                probe.recovery_not_before_monotonic_ns = now_ns;
            }
            self.request_rest("cancel_timeout", RestScope::Reconciliation);
            return self
                .record_system("cancel_timeout", json!({"probe_id": probe_id}))
                .await;
        }
        if phase == ProbePhase::FlattenPending
            && flatten_send_ns.is_some_and(|sent| {
                now_ns.saturating_sub(sent) >= self.config.probe.post_timeout_ms * 1_000_000
            })
        {
            if let Some(probe) = self.active_probe.as_mut() {
                probe.phase = ProbePhase::Recovering;
                probe.recovery_not_before_monotonic_ns = now_ns;
            }
            self.request_rest("flatten_timeout", RestScope::Reconciliation);
            return self
                .record_system("flatten_timeout", json!({"probe_id": probe_id}))
                .await;
        }
        if self.active_probe.as_ref().is_some_and(|probe| {
            recovery_request_due(probe.phase, now_ns, probe.recovery_not_before_monotonic_ns)
        }) {
            if self.ws_confirms_flat_recovery(now_ns) {
                self.record_lifecycle_decision(
                    "ws_recovery",
                    json!({"action":"finish","evidence":["order_terminal","inventory_flat"]}),
                    vec!["private_ws_confirmed_flat_after_settlement_window".into()],
                )
                .await?;
                self.finish_probe("ws_order_inventory_reconciled").await?;
                return Ok(());
            }
            self.request_rest_throttled("lifecycle_recovery", RestScope::Reconciliation, now_ns);
        }
        if matches!(phase, ProbePhase::Resting | ProbePhase::Partial)
            && self.storage.remaining_capacity()
                < (self.config.collector.writer_capacity / 10).max(1)
        {
            return self
                .cancel_active(
                    "storage_backpressure",
                    vec!["telemetry_writer_capacity_low".into()],
                )
                .await;
        }
        if matches!(phase, ProbePhase::Resting | ProbePhase::Partial) {
            let freshness_ns = self.config.probe.market_freshness_ms * 1_000_000;
            let default_l2_freshness_ns = self.config.probe.default_l2_freshness_ms * 1_000_000;
            if !self
                .market
                .required_quotes_fresh(now_ns, freshness_ns, default_l2_freshness_ns)
            {
                return self
                    .cancel_active("stale_data", vec!["market_source_stale".into()])
                    .await;
            }
            if now_ns >= expiry_ns {
                return self
                    .cancel_active("planned_expiry", vec!["planned_dwell_elapsed".into()])
                    .await;
            }
            if quote_stale {
                return self
                    .cancel_active(
                        "fair_value_guard",
                        vec!["quote_outside_fair_value_guard".into()],
                    )
                    .await;
            }
            if now_ns.saturating_sub(last_keep_ns) >= self.config.probe.keep_interval_ms * 1_000_000
            {
                self.record_keep("timer", vec!["resting_exposure_remains_eligible".into()])
                    .await?;
            }
        }
        if matches!(phase, ProbePhase::MarkoutPending | ProbePhase::Filled) {
            self.maybe_flatten(false).await?;
        }
        let fair = self.market.fair_value().unwrap_or_default();
        if fair > 0.0
            && self.account.confirmed_inventory.abs() * fair
                > self.config.probe.max_abs_inventory_usd
        {
            self.pending_markouts.clear();
            self.maybe_flatten(true).await?;
        }
        Ok(())
    }

    async fn cancel_active(&mut self, trigger: &str, reasons: Vec<String>) -> InfraResult<()> {
        let Some((probe_id, oid, cloid, side, price, remaining)) =
            self.active_probe.as_ref().map(|probe| {
                (
                    probe.probe_id.clone(),
                    probe.oid,
                    probe.cloid.clone(),
                    probe.side,
                    probe.price.to_string(),
                    probe.remaining_size.to_string(),
                )
            })
        else {
            return Ok(());
        };
        let Some(oid) = oid else {
            if let Some(probe) = self.active_probe.as_mut() {
                probe.phase = ProbePhase::Recovering;
                probe.recovery_not_before_monotonic_ns = self.identity.clock.monotonic_ns();
            }
            self.request_rest(
                &format!("cancel_without_oid:{trigger}"),
                RestScope::Reconciliation,
            );
            return Ok(());
        };
        if self
            .active_probe
            .as_ref()
            .is_some_and(|probe| probe.cancel_request_id.is_some())
        {
            return Ok(());
        }
        if let Some(probe) = self.active_probe.as_mut() {
            probe.end_reason = Some(trigger.into());
        }
        let decision_id = self.next_decision_id();
        let action_id = self.next_action_id();
        let decision = self.build_decision(
            &probe_id,
            &decision_id,
            trigger,
            vec![
                json!({"action":"keep","oid":oid}),
                json!({"action":"cancel","oid":oid}),
            ],
            json!({"action":"cancel","oid":oid}),
            1.0,
            None,
            0,
            reasons,
        );
        let decision_record_error = self
            .storage
            .record(StorageStream::Decisions, &decision)
            .await
            .err();
        let action_envelope = self.new_event_envelope("action_dispatch")?;
        let sent = self
            .gateway
            .cancel(
                action_envelope,
                ActionContext {
                    action_id: action_id.clone(),
                    probe_id: probe_id.clone(),
                    decision_id,
                    intent_id: format!("cancel-{probe_id}-{oid}"),
                    cloid: Some(cloid),
                    oid: Some(oid),
                    side: Some(side),
                    price: Some(price),
                    size: None,
                    remaining_size: Some(remaining),
                },
                oid,
            )
            .await?;
        let action_record_error = self
            .storage
            .record(StorageStream::Actions, &sent.record)
            .await
            .err();
        if let Some(probe) = self.active_probe.as_mut() {
            probe.phase = ProbePhase::CancelPending;
            probe.cancel_send_monotonic_ns = Some(sent.socket_write_monotonic_ns);
            probe.cancel_request_id = Some(sent.request_id);
        }
        match decision_record_error.or(action_record_error) {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    async fn record_keep(&mut self, trigger: &str, reasons: Vec<String>) -> InfraResult<()> {
        let Some((probe_id, oid, remaining_until_expiry)) =
            self.active_probe.as_ref().map(|probe| {
                (
                    probe.probe_id.clone(),
                    probe.oid,
                    probe
                        .planned_expiry_monotonic_ns
                        .saturating_sub(self.identity.clock.monotonic_ns()),
                )
            })
        else {
            return Ok(());
        };
        let decision_id = self.next_decision_id();
        let decision = self.build_decision(
            &probe_id,
            &decision_id,
            trigger,
            vec![
                json!({"action":"keep","oid":oid}),
                json!({"action":"cancel","oid":oid}),
            ],
            json!({"action":"keep","oid":oid}),
            1.0,
            Some(wall_time_ns()?.saturating_add(remaining_until_expiry)),
            0,
            reasons,
        );
        self.storage
            .record(StorageStream::Decisions, &decision)
            .await?;
        if let Some(probe) = self.active_probe.as_mut() {
            probe.last_keep_monotonic_ns = self.identity.clock.monotonic_ns();
            probe.decision_id = decision_id;
        }
        Ok(())
    }

    async fn record_lifecycle_decision(
        &mut self,
        trigger: &str,
        selected_action: Value,
        reasons: Vec<String>,
    ) -> InfraResult<()> {
        let Some(probe_id) = self
            .active_probe
            .as_ref()
            .map(|probe| probe.probe_id.clone())
        else {
            return Ok(());
        };
        let decision_id = self.next_decision_id();
        let decision = self.build_decision(
            &probe_id,
            &decision_id,
            trigger,
            vec![selected_action.clone()],
            selected_action,
            1.0,
            None,
            0,
            reasons,
        );
        self.storage
            .record(StorageStream::Decisions, &decision)
            .await?;
        if let Some(probe) = self.active_probe.as_mut() {
            probe.decision_id = decision_id;
        }
        Ok(())
    }

    async fn sample_markouts(&mut self) -> InfraResult<()> {
        let now_ns = self.identity.clock.monotonic_ns();
        let fair_value = self.market.fair_value();
        let mut samples = Vec::new();
        for pending in &mut self.pending_markouts {
            while pending.next_horizon_index < pending.horizons_ms.len() {
                let horizon_ms = pending.horizons_ms[pending.next_horizon_index];
                if now_ns.saturating_sub(pending.fill_monotonic_ns) < horizon_ms * 1_000_000 {
                    break;
                }
                let signed_markout = fair_value.map(|fair| match pending.side {
                    ProbeSide::Bid => fair - pending.fill_price,
                    ProbeSide::Ask => pending.fill_price - fair,
                });
                samples.push(json!({
                    "probe_id": pending.probe_id,
                    "oid": pending.oid,
                    "fill_id": pending.fill_id,
                    "horizon_ms": horizon_ms,
                    "fill_price": pending.fill_price,
                    "fill_size": pending.fill_size,
                    "fair_value": fair_value,
                    "signed_markout": signed_markout,
                }));
                pending.next_horizon_index += 1;
            }
        }
        for sample in samples {
            self.record_system_to(StorageStream::PrivateWs, "fill_markout", sample)
                .await?;
        }
        self.pending_markouts
            .retain(|pending| !pending.is_complete());
        Ok(())
    }

    async fn maybe_flatten(&mut self, risk_exit: bool) -> InfraResult<()> {
        let Some(probe) = self.active_probe.as_ref() else {
            return Ok(());
        };
        if probe.phase == ProbePhase::FlattenPending
            || (!risk_exit
                && self
                    .pending_markouts
                    .iter()
                    .any(|pending| pending.probe_id == probe.probe_id))
        {
            return Ok(());
        }
        let inventory = self.account.confirmed_inventory;
        let lot_size = self.account.lot_size.unwrap_or(0.0001);
        if inventory.abs() < lot_size * 0.5 {
            if probe.cumulative_filled_size > 0.0 {
                self.request_rest_throttled(
                    "await_inventory_before_flatten",
                    RestScope::Reconciliation,
                    self.identity.clock.monotonic_ns(),
                );
            }
            return Ok(());
        }
        let side = if inventory > 0.0 {
            ProbeSide::Ask
        } else {
            ProbeSide::Bid
        };
        let hl_book = self
            .market
            .freshest_hl_book()
            .map(|(_, book)| book)
            .ok_or_else(|| InfraError::Msg("cannot flatten without a valid HL book".into()))?;
        let bbo = match side {
            ProbeSide::Ask => hl_book.best_bid(),
            ProbeSide::Bid => hl_book.best_ask(),
        }
        .ok_or_else(|| InfraError::Msg("cannot flatten without a valid HL top quote".into()))?;
        let multiplier = match side {
            ProbeSide::Ask => 1.0 - self.config.probe.ioc_slippage_bps / 10_000.0,
            ProbeSide::Bid => 1.0 + self.config.probe.ioc_slippage_bps / 10_000.0,
        };
        let size_decimals = self.account.size_decimals.unwrap_or(4);
        let tick = hyperliquid_tick_size(bbo, size_decimals).unwrap_or(1.0);
        let price_text = round_price_to_tick(bbo * multiplier, tick, side == ProbeSide::Bid)
            .ok_or_else(|| InfraError::Msg("failed to round IOC price".into()))?;
        let size_text = reduce_only_size(inventory.abs(), lot_size)
            .ok_or_else(|| InfraError::Msg("inventory smaller than one lot".into()))?;
        let probe_id = probe.probe_id.clone();
        let decision_id = self.next_decision_id();
        let action_id = self.next_action_id();
        let reason = if risk_exit {
            "hard_inventory_guard"
        } else {
            "markout_complete"
        };
        let decision = self.build_decision(
            &probe_id,
            &decision_id,
            reason,
            vec![json!({
                "action":"ioc","side":side,"price":price_text,"size":size_text,"reduce_only":true,
            })],
            json!({
                "action":"ioc","side":side,"price":price_text,"size":size_text,"reduce_only":true,
            }),
            1.0,
            None,
            0,
            vec![reason.into()],
        );
        let decision_record_error = self
            .storage
            .record(StorageStream::Decisions, &decision)
            .await
            .err();
        let flatten_cloid = make_cloid(
            &self.identity.run_id,
            self.probe_sequence
                .saturating_add(self.action_sequence << 32),
        );
        let action_envelope = self.new_event_envelope("action_dispatch")?;
        let sent = self
            .gateway
            .flatten_ioc(
                action_envelope,
                ActionContext {
                    action_id: action_id.clone(),
                    probe_id,
                    decision_id,
                    intent_id: format!("flatten-{}", self.action_sequence),
                    cloid: Some(flatten_cloid),
                    oid: None,
                    side: Some(side),
                    price: Some(price_text),
                    size: Some(size_text),
                    remaining_size: None,
                },
            )
            .await?;
        let action_record_error = self
            .storage
            .record(StorageStream::Actions, &sent.record)
            .await
            .err();
        if let Some(probe) = self.active_probe.as_mut() {
            probe.phase = ProbePhase::FlattenPending;
            probe.flatten_request_id = Some(sent.request_id);
            probe.flatten_action_id = Some(action_id);
            probe.flatten_send_monotonic_ns = Some(sent.socket_write_monotonic_ns);
        }
        match decision_record_error.or(action_record_error) {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    async fn maybe_finish_flatten(&mut self) -> InfraResult<()> {
        let lot_size = self.account.lot_size.unwrap_or(0.0001);
        if self.active_probe.as_ref().is_some_and(|probe| {
            probe.phase == ProbePhase::FlattenPending
                && self.account.confirmed_inventory.abs() < lot_size * 0.5
        }) {
            self.finish_probe("inventory_reconciled").await?;
        }
        Ok(())
    }

    async fn finish_probe(&mut self, reason: &str) -> InfraResult<()> {
        let now_ns = self.identity.clock.monotonic_ns();
        if let Some(probe) = self.active_probe.take() {
            self.record_system(
                "probe_finalized",
                json!({
                    "probe_id": probe.probe_id,
                    "oid": probe.oid,
                    "side": probe.side,
                    "price": probe.price,
                    "size": probe.size,
                    "filled_size": probe.cumulative_filled_size,
                    "first_fill_monotonic_ns": probe.first_fill_monotonic_ns,
                    "planned_dwell_ms": probe.planned_dwell_ms,
                    "flatten_oid": probe.flatten_oid,
                    "flatten_action_id": probe.flatten_action_id,
                    "actual_dwell_ms": now_ns.saturating_sub(probe.created_monotonic_ns) / 1_000_000,
                    "end_reason": reason,
                    "exposure_trigger": probe.end_reason,
                    "censored": probe.cumulative_filled_size <= 0.0,
                    "initial_queue_belief": probe.initial_queue_belief,
                    "queue_belief": probe.queue_belief,
                }),
            )
            .await?;
        }
        self.cooldown_until_ns = now_ns + self.config.probe.cooldown_ms * 1_000_000;
        Ok(())
    }

    fn update_queue_from_book(&mut self) {
        let Some(probe) = self.active_probe.as_mut() else {
            return;
        };
        if !matches!(probe.phase, ProbePhase::Resting | ProbePhase::Partial) {
            return;
        }
        let tick = probe.tick_size;
        if let Some(level) = self
            .market
            .book_for_queue()
            .level(probe.side, probe.price, tick)
        {
            probe
                .queue_belief
                .observe_visible(level.size, level.order_count);
        }
    }

    fn quote_is_stale(&self, probe: &ActiveProbe) -> bool {
        let Some(fair) = self.market.fair_value() else {
            return true;
        };
        let threshold = self.config.probe.fair_value_guard_ticks * probe.tick_size;
        match probe.side {
            ProbeSide::Bid => probe.price - fair > threshold,
            ProbeSide::Ask => fair - probe.price > threshold,
        }
    }

    fn all_connections_started(&self) -> bool {
        self.feed_plans.keys().all(|task_id| {
            self.connections
                .get(task_id)
                .is_some_and(|state| state.generation > 0)
        })
    }

    fn ws_confirms_flat_recovery(&self, now_ns: u64) -> bool {
        let Some(probe) = self.active_probe.as_ref() else {
            return false;
        };
        let order_terminal = probe
            .terminal_order_update_monotonic_ns
            .is_some_and(|terminal_ns| {
                terminal_ns
                    >= probe
                        .cancel_send_monotonic_ns
                        .unwrap_or(probe.send_monotonic_ns)
            });
        let lot_size = self.account.lot_size.unwrap_or(0.0001);
        recovery_request_due(probe.phase, now_ns, probe.recovery_not_before_monotonic_ns)
            && order_terminal
            && probe.remaining_size <= f64::EPSILON
            && probe.cumulative_filled_size <= f64::EPSILON
            && self.account.rest_snapshot_healthy
            && self.account.snapshot_monotonic_ns > 0
            && self.account.confirmed_inventory.abs() < lot_size * 0.5
    }

    fn request_rest(&mut self, reason: &str, scope: RestScope) -> bool {
        self.request_rest_at(reason, scope, self.identity.clock.monotonic_ns(), 0)
    }

    fn request_rest_throttled(&mut self, reason: &str, scope: RestScope, now_ns: u64) -> bool {
        self.request_rest_at(reason, scope, now_ns, 1_000_000_000)
    }

    fn request_rest_at(
        &mut self,
        reason: &str,
        scope: RestScope,
        now_ns: u64,
        minimum_interval_ns: u64,
    ) -> bool {
        if !self
            .rest_request_gate
            .try_begin(now_ns, minimum_interval_ns)
        {
            return false;
        }
        if self
            .rest
            .requests
            .try_send(RestRequest {
                reason: reason.into(),
                scope,
            })
            .is_err()
        {
            self.rest_request_gate.complete();
            return false;
        }
        true
    }

    fn probe_candidates(&self) -> Vec<Value> {
        let mut candidates = Vec::new();
        for side in [ProbeSide::Bid, ProbeSide::Ask] {
            for level in &self.config.probe.price_levels_ticks {
                for dwell in &self.config.probe.dwell_seconds {
                    candidates.push(json!({
                        "action":"place",
                        "side":side,
                        "price_level_ticks":level,
                        "planned_dwell_sec":dwell,
                        "time_in_force":"Alo",
                    }));
                }
            }
        }
        candidates
    }

    #[allow(clippy::too_many_arguments)]
    fn build_decision(
        &mut self,
        probe_id: &str,
        decision_id: &str,
        trigger: &str,
        candidate_actions: Vec<Value>,
        selected_action: Value,
        selection_probability: f64,
        planned_expiry_ts_ns: Option<u64>,
        random_seed: u64,
        reason_codes: Vec<String>,
    ) -> DecisionRecord {
        let now_ns = self.identity.clock.monotonic_ns();
        DecisionRecord {
            envelope: self
                .new_event_envelope("decision")
                .expect("system clock must be valid"),
            probe_id: probe_id.into(),
            decision_id: decision_id.into(),
            decision_ts_ns: now_ns,
            trigger_type: trigger.into(),
            state_snapshot_id: format!("snapshot-{decision_id}"),
            state_snapshot: self.execution_snapshot(now_ns),
            candidate_actions,
            selected_action,
            behavior_policy_id: self.config.probe.behavior_policy_id.clone(),
            selection_probability,
            random_seed,
            planned_expiry_ts_ns,
            risk_limits: json!({
                "max_order_notional_usd": self.config.probe.max_order_notional_usd,
                "max_abs_inventory_usd": self.config.probe.max_abs_inventory_usd,
                "max_active_probe_orders": 1,
            }),
            reason_codes,
        }
    }

    fn execution_snapshot(&self, now_ns: u64) -> Value {
        let active = self.active_probe.as_ref();
        let pending_bid_inventory = active
            .filter(|probe| probe.side == ProbeSide::Bid)
            .map(|probe| probe.remaining_size)
            .unwrap_or_default();
        let pending_ask_inventory = active
            .filter(|probe| probe.side == ProbeSide::Ask)
            .map(|probe| probe.remaining_size)
            .unwrap_or_default();
        json!({
            "market": self.market.snapshot(now_ns),
            "execution": active.map(|probe| json!({
                "probe_id": probe.probe_id,
                "cloid": probe.cloid,
                "oid": probe.oid,
                "phase": probe.phase,
                "side": probe.side,
                "price": probe.price,
                "tick_size": probe.tick_size,
                "size": probe.size,
                "remaining_size": probe.remaining_size,
                "filled_size": probe.cumulative_filled_size,
                "order_age_ms": now_ns.saturating_sub(probe.open_monotonic_ns.unwrap_or(probe.send_monotonic_ns)) / 1_000_000,
                "queue_belief": probe.queue_belief,
            })),
            "risk": {
                "confirmed_inventory": self.account.confirmed_inventory,
                "pending_bid_inventory": pending_bid_inventory,
                "pending_ask_inventory": pending_ask_inventory,
                "worst_case_long_inventory": self.account.confirmed_inventory + pending_bid_inventory,
                "worst_case_short_inventory": self.account.confirmed_inventory - pending_ask_inventory,
                "account_value": self.account.account_value,
                "withdrawable": self.account.withdrawable,
                "maker_fee": self.account.maker_fee,
                "taker_fee": self.account.taker_fee,
                "action_budget_remaining": self.account.action_budget_remaining(),
                "writer_remaining_capacity": self.storage.remaining_capacity(),
            },
        })
    }

    fn next_decision_id(&mut self) -> String {
        self.decision_sequence = self.decision_sequence.saturating_add(1);
        format!("decision-{}", self.decision_sequence)
    }

    fn next_action_id(&mut self) -> String {
        self.action_sequence = self.action_sequence.saturating_add(1);
        format!("action-{}", self.action_sequence)
    }

    fn new_event_envelope(&mut self, event_type: &str) -> InfraResult<EventEnvelope> {
        let wall_ns = wall_time_ns()?;
        let mono_ns = self.identity.clock.monotonic_ns();
        Ok(self.new_envelope(event_type, wall_ns, mono_ns))
    }

    fn new_envelope(&mut self, event_type: &str, wall_ns: u64, mono_ns: u64) -> EventEnvelope {
        self.event_sequence = self.event_sequence.saturating_add(1);
        empty_envelope(
            &self.identity.run_id,
            &self.identity.host_id,
            &self.identity.build_commit,
            format!("strategy-{}", self.event_sequence),
            event_type,
            wall_ns,
            mono_ns,
        )
    }

    async fn record_system(&mut self, kind: &str, details: Value) -> InfraResult<()> {
        self.record_system_to(StorageStream::System, kind, details)
            .await
    }

    async fn record_system_to(
        &mut self,
        stream: StorageStream,
        kind: &str,
        details: Value,
    ) -> InfraResult<()> {
        let record = SystemRecord {
            envelope: self.new_event_envelope(kind)?,
            kind: kind.into(),
            details,
        };
        self.storage.record(stream, &record).await
    }
}

async fn build_feed_plans(
    config: &AppConfig,
    hl: &HyperliquidCli,
    owner_address: &str,
) -> InfraResult<Vec<FeedPlan>> {
    let hl_inst = [config.markets.hyperliquid_instrument.clone()];
    let hl_bbo = WsChannel::Lob(Some(LobParam::Bbo {
        frequency: Some(LobFrequency::Realtime),
    }));
    let hl_default_l2 = WsChannel::Lob(Some(LobParam::Snapshot {
        depth: None,
        frequency: None,
    }));
    let hl_fast_l2 = WsChannel::Lob(Some(LobParam::Snapshot {
        depth: Some(5),
        frequency: Some(LobFrequency::Ms500),
    }));
    let hl_trades = WsChannel::Trades(Some(TradesParam::AllTrades));
    let hl_url = hl.get_public_connect_msg(&hl_bbo).await?;

    let binance = BinanceUmCli::default();
    let binance_inst = [config.markets.binance_instrument.clone()];
    let binance_bbo = WsChannel::Lob(Some(LobParam::Bbo {
        frequency: Some(LobFrequency::Realtime),
    }));
    let binance_depth = WsChannel::Lob(Some(LobParam::Incremental {
        depth: None,
        frequency: Some(LobFrequency::Ms100),
    }));
    let binance_trades = WsChannel::Trades(Some(TradesParam::AggTrades));

    let okx = OkxCli::default();
    let okx_inst = [config.markets.okx_instrument.clone()];
    let okx_bbo = WsChannel::Lob(Some(LobParam::Bbo {
        frequency: Some(LobFrequency::Ms10),
    }));
    let okx_books = WsChannel::Lob(Some(LobParam::Incremental {
        depth: Some(400),
        frequency: Some(LobFrequency::Ms100),
    }));
    let okx_trades = WsChannel::Trades(Some(TradesParam::AllTrades));

    Ok(vec![
        FeedPlan {
            task_id: TASK_HL_STANDARD,
            market: Market::HyperLiquid,
            channel: WsChannel::Other("hl_standard".into()),
            source: "hyperliquid",
            feed: "hl_standard",
            url: hl_url.clone(),
            subscriptions: vec![
                hl.get_public_sub_msg(&hl_bbo, Some(&hl_inst)).await?,
                hl.get_public_sub_msg(&hl_default_l2, Some(&hl_inst))
                    .await?,
                hl.get_public_sub_msg(&hl_trades, Some(&hl_inst)).await?,
                json!({
                    "method":"subscribe",
                    "subscription":{"type":"activeAssetCtx","coin":config.markets.hyperliquid_coin}
                })
                .to_string(),
            ],
            is_private: false,
            is_action: false,
        },
        FeedPlan {
            task_id: TASK_HL_FAST_L2,
            market: Market::HyperLiquid,
            channel: WsChannel::Other("hl_fast_l2".into()),
            source: "hyperliquid",
            feed: "hl_fast_l2",
            url: hl_url.clone(),
            subscriptions: vec![hl.get_public_sub_msg(&hl_fast_l2, Some(&hl_inst)).await?],
            is_private: false,
            is_action: false,
        },
        FeedPlan {
            task_id: TASK_HL_PRIVATE,
            market: Market::HyperLiquid,
            channel: WsChannel::Other("hl_private".into()),
            source: "hyperliquid",
            feed: "hl_private",
            url: hl_url.clone(),
            subscriptions: vec![
                json!({"method":"subscribe","subscription":{"type":"orderUpdates","user":owner_address}}).to_string(),
                json!({"method":"subscribe","subscription":{"type":"userFills","user":owner_address,"aggregateByTime":false}}).to_string(),
                json!({"method":"subscribe","subscription":{"type":"clearinghouseState","user":owner_address,"dex":config.markets.hyperliquid_perp_dex}}).to_string(),
            ],
            is_private: true,
            is_action: false,
        },
        FeedPlan {
            task_id: TASK_HL_ACTION,
            market: Market::HyperLiquid,
            channel: action_channel(),
            source: "hyperliquid",
            feed: "hl_action",
            url: hl_url,
            subscriptions: vec![],
            is_private: true,
            is_action: true,
        },
        FeedPlan {
            task_id: TASK_BINANCE_LOB,
            market: Market::BinanceUmFutures,
            channel: WsChannel::Other("binance_lob".into()),
            source: "binance",
            feed: "binance_lob",
            url: binance.get_public_connect_msg(&binance_bbo).await?,
            subscriptions: vec![
                binance
                    .get_public_sub_msg(&binance_bbo, Some(&binance_inst))
                    .await?,
                binance
                    .get_public_sub_msg(&binance_depth, Some(&binance_inst))
                    .await?,
            ],
            is_private: false,
            is_action: false,
        },
        FeedPlan {
            task_id: TASK_BINANCE_AGG_TRADE,
            market: Market::BinanceUmFutures,
            channel: WsChannel::Other("binance_agg_trade".into()),
            source: "binance",
            feed: "binance_agg_trade",
            url: binance.get_public_connect_msg(&binance_trades).await?,
            subscriptions: vec![
                binance
                    .get_public_sub_msg(&binance_trades, Some(&binance_inst))
                    .await?,
            ],
            is_private: false,
            is_action: false,
        },
        FeedPlan {
            task_id: TASK_OKX_LOB,
            market: Market::Okx,
            channel: WsChannel::Other("okx_public_lob".into()),
            source: "okx",
            feed: "okx_public_lob",
            url: okx.get_public_connect_msg(&okx_bbo).await?,
            subscriptions: vec![
                okx.get_public_sub_msg(&okx_bbo, Some(&okx_inst)).await?,
                okx.get_public_sub_msg(&okx_books, Some(&okx_inst)).await?,
            ],
            is_private: false,
            is_action: false,
        },
        FeedPlan {
            task_id: TASK_OKX_TRADES_ALL,
            market: Market::Okx,
            channel: WsChannel::Other("okx_trades_all".into()),
            source: "okx",
            feed: "okx_trades_all",
            url: okx.get_public_connect_msg(&okx_trades).await?,
            subscriptions: vec![okx
                .get_public_sub_msg(&okx_trades, Some(&okx_inst))
                .await?],
            is_private: false,
            is_action: false,
        },
    ])
}

fn feed_task(plan: &FeedPlan) -> WsTaskInfo {
    WsTaskInfo {
        market: plan.market.clone(),
        ws_channel: plan.channel.clone(),
        filter_channels: false,
        chunk: 1,
        task_base_id: Some(plan.task_id),
    }
}

fn trade_consumes_queue(resting_side: ProbeSide, taker_side: ProbeSide) -> bool {
    matches!(
        (resting_side, taker_side),
        (ProbeSide::Bid, ProbeSide::Ask) | (ProbeSide::Ask, ProbeSide::Bid)
    )
}

fn recovery_request_due(phase: ProbePhase, now_ns: u64, not_before_ns: u64) -> bool {
    phase == ProbePhase::Recovering && now_ns >= not_before_ns
}

fn resolve_asset_id(cli: &HyperliquidCli, inst: &str) -> InfraResult<u32> {
    let index = cli
        .market_cache
        .inst_index_map
        .get(inst)
        .copied()
        .ok_or_else(|| InfraError::Msg(format!("HL instrument not found: {inst}")))?;
    let dex_index = match cli.market_cache.perp_dex.as_ref() {
        Some(_) => Some(
            cli.market_cache
                .perp_dex_index
                .ok_or_else(|| InfraError::Msg("HL perp dex index not initialized".into()))?,
        ),
        None => None,
    };
    hyperliquid_perp_asset_id_for_dex(index, dex_index)
}

fn start_rest_worker(
    cli: HyperliquidCli,
    config: Arc<AppConfig>,
    identity: RuntimeIdentity,
    storage: StorageHandle,
    mut stop: watch::Receiver<bool>,
) -> (RestHandle, JoinHandle<InfraResult<()>>) {
    let (request_tx, mut request_rx) = mpsc::channel::<RestRequest>(1);
    let (state_tx, state_rx) = watch::channel(AccountState::default());
    let task = tokio::spawn(async move {
        let mut sequence = 0_u64;
        let mut cached_state = AccountState::default();
        loop {
            let request = tokio::select! {
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() { break; }
                    continue;
                }
                request = request_rx.recv() => {
                    let Some(request) = request else { break; };
                    request
                }
            };
            let started_ns = identity.clock.monotonic_ns();
            let insts = [config.markets.hyperliquid_instrument.clone()];
            let assets = ["USDC".to_string()];
            let is_full = request.scope == RestScope::Full;
            let (orders, positions, balances, rate_limit, fees, metadata) = if is_full {
                let (orders, positions, balances, rate_limit, fees, metadata) = tokio::join!(
                    cli.get_open_orders(&config.markets.hyperliquid_instrument, Some(100)),
                    cli.get_positions(Some(&insts)),
                    cli.get_balance(Some(&assets)),
                    cli.get_user_rate_limit(),
                    cli.get_user_fees(),
                    cli.get_perp_meta_raw(),
                );
                (
                    orders,
                    positions,
                    Some(balances),
                    Some(rate_limit),
                    Some(fees),
                    Some(metadata),
                )
            } else {
                let (orders, positions) = tokio::join!(
                    cli.get_open_orders(&config.markets.hyperliquid_instrument, Some(100)),
                    cli.get_positions(Some(&insts)),
                );
                (orders, positions, None, None, None, None)
            };
            let mut state = cached_state.clone();
            state.snapshot_monotonic_ns = started_ns;
            state.rest_snapshot_healthy = false;
            if is_full {
                state.account_value = None;
                state.withdrawable = None;
                state.requests_used = None;
                state.requests_cap = None;
                state.requests_surplus = None;
                state.maker_fee = None;
                state.taker_fee = None;
                state.lot_size = None;
                state.size_decimals = None;
                state.growth_mode = None;
                state.deployer_fee_scale = None;
            }
            let mut errors = Vec::new();
            let orders_json = match orders {
                Ok(orders) => {
                    state.open_order_count = orders.len();
                    state.open_orders = orders
                        .iter()
                        .filter_map(|order| {
                            Some(AccountOpenOrder {
                                oid: order.order_id.parse().ok()?,
                                cloid: order.cli_order_id.clone(),
                                price: order.price,
                                size: order.size,
                                executed_size: order.executed_size,
                            })
                        })
                        .collect();
                    serde_json::to_value(orders).unwrap_or(Value::Null)
                }
                Err(err) => {
                    errors.push(format!("open_orders:{err}"));
                    Value::Null
                }
            };
            let positions_json = match positions {
                Ok(positions) => {
                    state.confirmed_inventory = positions
                        .iter()
                        .find(|position| position.inst == config.markets.hyperliquid_instrument)
                        .map(|position| position.size)
                        .unwrap_or_default();
                    serde_json::to_value(positions).unwrap_or(Value::Null)
                }
                Err(err) => {
                    errors.push(format!("positions:{err}"));
                    Value::Null
                }
            };
            let balances_json = match balances {
                Some(Ok(balances)) => {
                    if let Some(usdc) = balances.iter().find(|balance| balance.asset == "USDC") {
                        state.account_value = Some(usdc.total);
                        state.withdrawable = Some(usdc.available);
                    }
                    serde_json::to_value(balances).unwrap_or(Value::Null)
                }
                Some(Err(err)) => {
                    errors.push(format!("balances:{err}"));
                    Value::Null
                }
                None => json!({"cached": true}),
            };
            let rate_json = match rate_limit {
                Some(Ok(rate)) => {
                    state.requests_used = Some(rate.nRequestsUsed);
                    state.requests_cap = Some(rate.nRequestsCap);
                    state.requests_surplus = Some(rate.nRequestsSurplus);
                    json!({
                        "cumVlm": rate.cumVlm,
                        "nRequestsUsed": rate.nRequestsUsed,
                        "nRequestsCap": rate.nRequestsCap,
                        "nRequestsSurplus": rate.nRequestsSurplus,
                    })
                }
                Some(Err(err)) => {
                    errors.push(format!("user_rate_limit:{err}"));
                    Value::Null
                }
                None => json!({"cached": true}),
            };
            let fees_json = match fees {
                Some(Ok(fees)) => {
                    state.maker_fee = fees.userAddRate.parse().ok();
                    state.taker_fee = fees.userCrossRate.parse().ok();
                    json!({
                        "userCrossRate": fees.userCrossRate,
                        "userAddRate": fees.userAddRate,
                        "userSpotCrossRate": fees.userSpotCrossRate,
                        "userSpotAddRate": fees.userSpotAddRate,
                        "activeReferralDiscount": fees.activeReferralDiscount,
                        "activeStakingDiscount": fees.activeStakingDiscount.map(|value| json!({
                            "bpsOfMaxSupply": value.bpsOfMaxSupply,
                            "discount": value.discount,
                        })),
                    })
                }
                Some(Err(err)) => {
                    errors.push(format!("user_fees:{err}"));
                    Value::Null
                }
                None => json!({"cached": true}),
            };
            let metadata_json = match metadata {
                Some(Ok(metadata)) => {
                    let selected = metadata
                        .universe
                        .iter()
                        .find(|entry| entry.name == config.markets.hyperliquid_coin);
                    if let Some(selected) = selected {
                        state.lot_size = Some(10_f64.powi(-(selected.szDecimals as i32)));
                        state.size_decimals = Some(selected.szDecimals);
                        state.growth_mode = selected.growthMode.clone();
                        state.deployer_fee_scale = selected.deployerFeeScale.clone();
                    } else {
                        errors.push(format!(
                            "metadata:coin_not_found:{}",
                            config.markets.hyperliquid_coin
                        ));
                    }
                    selected.map_or(Value::Null, |entry| {
                        json!({
                            "name": entry.name,
                            "szDecimals": entry.szDecimals,
                            "maxLeverage": entry.maxLeverage,
                            "onlyIsolated": entry.onlyIsolated,
                            "isDelisted": entry.isDelisted,
                            "marginMode": entry.marginMode,
                            "growthMode": entry.growthMode,
                            "deployerFeeScale": entry.deployerFeeScale,
                            "lastFeeScaleChangeTime": entry.lastFeeScaleChangeTime,
                        })
                    })
                }
                Some(Err(err)) => {
                    errors.push(format!("metadata:{err}"));
                    Value::Null
                }
                None => json!({"cached": true}),
            };
            if is_full {
                if state.withdrawable.is_none() {
                    errors.push("balances:USDC_not_found".into());
                }
                if state.maker_fee.is_none() || state.taker_fee.is_none() {
                    errors.push("user_fees:unparseable_rate".into());
                }
            }
            state.rest_snapshot_healthy = errors.is_empty();
            if state.rest_snapshot_healthy {
                cached_state = state.clone();
            }
            sequence = sequence.saturating_add(1);
            let wall_ns = wall_time_ns()?;
            let mono_ns = identity.clock.monotonic_ns();
            let record = AccountSnapshotRecord {
                envelope: empty_envelope(
                    &identity.run_id,
                    &identity.host_id,
                    &identity.build_commit,
                    format!("rest-{sequence}"),
                    "account_snapshot",
                    wall_ns,
                    mono_ns,
                ),
                reason: request.reason,
                snapshot: json!({
                    "scope": match request.scope {
                        RestScope::Reconciliation => "reconciliation",
                        RestScope::Full => "full",
                    },
                    "orders": orders_json,
                    "positions": positions_json,
                    "balances": balances_json,
                    "userRateLimit": rate_json,
                    "userFees": fees_json,
                    "metadata": metadata_json,
                    "errors": errors,
                    "request_started_monotonic_ns": started_ns,
                    "request_elapsed_ms": mono_ns.saturating_sub(started_ns) as f64 / 1_000_000.0,
                }),
            };
            storage
                .record(StorageStream::AccountSnapshots, &record)
                .await?;
            let _ = state_tx.send(state);
        }
        Ok(())
    });
    (
        RestHandle {
            requests: request_tx,
            latest: state_rx,
        },
        task,
    )
}

fn start_system_worker(
    config: Arc<AppConfig>,
    identity: RuntimeIdentity,
    storage: StorageHandle,
    mut stop: watch::Receiver<bool>,
) -> JoinHandle<InfraResult<()>> {
    tokio::spawn(async move {
        let mut sequence = 0_u64;
        let mut tick = interval(Duration::from_secs(
            config.collector.system_snapshot_interval_sec,
        ));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() { break; }
                }
                _ = tick.tick() => {
                    sequence = sequence.saturating_add(1);
                    let chrony = Command::new("chronyc").arg("tracking").output().await;
                    let (chrony_tracking, chrony_error) = match chrony {
                        Ok(output) if output.status.success() => (
                            Some(String::from_utf8_lossy(&output.stdout).into_owned()),
                            None,
                        ),
                        Ok(output) => (
                            None,
                            Some(String::from_utf8_lossy(&output.stderr).into_owned()),
                        ),
                        Err(err) => (None, Some(err.to_string())),
                    };
                    let wall_ns = wall_time_ns()?;
                    let mono_ns = identity.clock.monotonic_ns();
                    let record = SystemRecord {
                        envelope: empty_envelope(
                            &identity.run_id,
                            &identity.host_id,
                            &identity.build_commit,
                            format!("system-worker-{sequence}"),
                            "system_health",
                            wall_ns,
                            mono_ns,
                        ),
                        kind: "system_health".into(),
                        details: json!({
                            "chrony_tracking": chrony_tracking,
                            "chrony_error": chrony_error,
                            "writer_remaining_capacity": storage.remaining_capacity(),
                        }),
                    };
                    storage.record(StorageStream::System, &record).await?;
                }
            }
        }
        Ok(())
    })
}

pub fn schedule_task(interval_ms: u64) -> AltTaskInfo {
    AltTaskInfo {
        alt_task_type: AltTaskType::TimeScheduler(Duration::from_millis(interval_ms)),
        chunk: 1,
        task_base_id: Some(TASK_PROBE_TIMER),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_ids_are_unique() {
        let ids = [
            TASK_HL_STANDARD,
            TASK_HL_FAST_L2,
            TASK_HL_PRIVATE,
            TASK_HL_ACTION,
            TASK_BINANCE_LOB,
            TASK_BINANCE_AGG_TRADE,
            TASK_OKX_LOB,
            TASK_OKX_TRADES_ALL,
        ];
        assert_eq!(ids.iter().copied().collect::<HashSet<_>>().len(), ids.len());
    }

    #[test]
    fn only_opposite_taker_flow_consumes_a_resting_queue() {
        assert!(trade_consumes_queue(ProbeSide::Bid, ProbeSide::Ask));
        assert!(trade_consumes_queue(ProbeSide::Ask, ProbeSide::Bid));
        assert!(!trade_consumes_queue(ProbeSide::Bid, ProbeSide::Bid));
        assert!(!trade_consumes_queue(ProbeSide::Ask, ProbeSide::Ask));
    }

    #[test]
    fn recovery_waits_for_its_settlement_deadline() {
        assert!(!recovery_request_due(ProbePhase::Recovering, 999, 1_000));
        assert!(recovery_request_due(ProbePhase::Recovering, 1_000, 1_000));
        assert!(!recovery_request_due(ProbePhase::Resting, 2_000, 1_000));
    }

    #[test]
    fn rest_request_gate_coalesces_until_the_snapshot_completes() {
        let mut gate = RestRequestGate::default();
        assert!(gate.try_begin(1_000, 0));
        assert!(!gate.try_begin(2_000, 0));
        gate.complete();
        assert!(gate.try_begin(2_000, 0));
    }

    #[test]
    fn urgent_request_is_not_blocked_by_the_throttle_after_completion() {
        let mut gate = RestRequestGate::default();
        assert!(gate.try_begin(1_000, 0));
        gate.complete();
        assert!(!gate.try_begin(1_001, 1_000_000_000));
        assert!(gate.try_begin(1_001, 0));
    }
}
