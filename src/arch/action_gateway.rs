use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use extrema_infra::{
    arch::market_assets::{
        api_general::{OrderParams, get_mills_timestamp},
        base_data::{OrderSide, OrderType},
        exchange::prelude::{
            HYPERLIQUID_ORDER_ACTION_CHANNEL, HyperliquidAuth, HyperliquidCancelAction,
            HyperliquidCancelByOidRequest, HyperliquidOrderAction, HyperliquidSignature,
            hyperliquid_order_from_params,
        },
    },
    errors::{InfraError, InfraResult},
    prelude::{AckHandle, AckStatus, CommandEmitter, CommandRegistry, TaskCommand, WsChannel},
};

use super::{
    execution_probe_module::{
        state::ProbeSide,
        utils::{ProcessClock, TASK_HL_ACTION},
    },
    schema::{ActionRecord, EventEnvelope},
};

#[derive(Clone, Debug)]
pub struct ActionContext {
    pub action_id: String,
    pub probe_id: String,
    pub decision_id: String,
    pub intent_id: String,
    pub cloid: Option<String>,
    pub oid: Option<u64>,
    pub side: Option<ProbeSide>,
    pub price: Option<String>,
    pub size: Option<String>,
    pub remaining_size: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SentAction {
    pub request_id: u64,
    pub nonce: u64,
    pub action_id: String,
    pub socket_write_monotonic_ns: u64,
    pub record: ActionRecord,
}

#[derive(Clone)]
pub struct ActionGateway {
    registry: Arc<CommandRegistry>,
    auth: HyperliquidAuth,
    asset_id: u32,
    clock: ProcessClock,
    next_request_id: u64,
    last_nonce: u64,
}

impl ActionGateway {
    pub fn new(auth: HyperliquidAuth, asset_id: u32, clock: ProcessClock) -> Self {
        Self {
            registry: Arc::new(CommandRegistry::default()),
            auth,
            asset_id,
            clock,
            next_request_id: 1,
            last_nonce: 0,
        }
    }

    pub fn set_registry(&mut self, registry: Arc<CommandRegistry>) {
        self.registry = registry;
    }

    pub fn asset_id(&self) -> u32 {
        self.asset_id
    }

    pub async fn place_alo(
        &mut self,
        envelope: EventEnvelope,
        context: ActionContext,
    ) -> InfraResult<SentAction> {
        let side = context
            .side
            .ok_or_else(|| InfraError::Msg("place action requires side".into()))?;
        let price = context
            .price
            .clone()
            .ok_or_else(|| InfraError::Msg("place action requires price".into()))?;
        let size = context
            .size
            .clone()
            .ok_or_else(|| InfraError::Msg("place action requires size".into()))?;
        let request = hyperliquid_order_from_params(OrderParams {
            inst: self.asset_id.to_string(),
            side: order_side(side),
            size,
            order_type: OrderType::PostOnly,
            price: Some(price),
            reduce_only: Some(false),
            margin_mode: None,
            position_side: None,
            time_in_force: None,
            client_order_id: context.cloid.clone(),
            extra: Default::default(),
        })?;
        let action = HyperliquidOrderAction {
            kind: "order",
            orders: vec![request],
            grouping: "na",
            builder: None,
        };
        self.send_action(envelope, context, "place", "Alo", false, &action)
            .await
    }

    pub async fn cancel(
        &mut self,
        envelope: EventEnvelope,
        context: ActionContext,
        oid: u64,
    ) -> InfraResult<SentAction> {
        let action = HyperliquidCancelAction::ByOid {
            cancels: vec![HyperliquidCancelByOidRequest {
                asset: self.asset_id,
                order_id: oid,
            }],
        };
        self.send_action(envelope, context, "cancel", "Gtc", false, &action)
            .await
    }

    pub async fn flatten_ioc(
        &mut self,
        envelope: EventEnvelope,
        context: ActionContext,
    ) -> InfraResult<SentAction> {
        let side = context
            .side
            .ok_or_else(|| InfraError::Msg("IOC action requires side".into()))?;
        let price = context
            .price
            .clone()
            .ok_or_else(|| InfraError::Msg("IOC action requires price".into()))?;
        let size = context
            .size
            .clone()
            .ok_or_else(|| InfraError::Msg("IOC action requires size".into()))?;
        let request = hyperliquid_order_from_params(OrderParams {
            inst: self.asset_id.to_string(),
            side: order_side(side),
            size,
            order_type: OrderType::Ioc,
            price: Some(price),
            reduce_only: Some(true),
            margin_mode: None,
            position_side: None,
            time_in_force: None,
            client_order_id: context.cloid.clone(),
            extra: Default::default(),
        })?;
        let action = HyperliquidOrderAction {
            kind: "order",
            orders: vec![request],
            grouping: "na",
            builder: None,
        };
        self.send_action(envelope, context, "ioc", "Ioc", true, &action)
            .await
    }

    async fn send_action<A: Serialize>(
        &mut self,
        envelope: EventEnvelope,
        context: ActionContext,
        action_type: &str,
        time_in_force: &str,
        reduce_only: bool,
        action: &A,
    ) -> InfraResult<SentAction> {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        let nonce = self.next_nonce();
        let sign_start_ns = self.clock.monotonic_ns();
        let raw_outbound_json = signed_ws_action(&self.auth, request_id, action, nonce)?;
        let sign_end_ns = self.clock.monotonic_ns();
        let payload_hash = sha256_hex(raw_outbound_json.as_bytes());
        let channel = action_channel();
        let handle = self
            .registry
            .find_ws_handle(&channel, TASK_HL_ACTION)
            .ok_or_else(|| InfraError::Msg("missing Hyperliquid action websocket handle".into()))?;
        let command_enqueue_ns = self.clock.monotonic_ns();
        let (ack_tx, ack_rx) = oneshot::channel();
        handle
            .send_command(
                TaskCommand::WsMessage {
                    msg: raw_outbound_json.clone(),
                    ack: AckHandle::new(ack_tx),
                },
                Some((AckStatus::WsMessage, ack_rx)),
            )
            .await?;
        let socket_write_ns = self.clock.monotonic_ns();
        let record = ActionRecord {
            envelope,
            action_id: context.action_id.clone(),
            probe_id: context.probe_id,
            decision_id: context.decision_id,
            intent_id: context.intent_id,
            request_id,
            nonce,
            action_type: action_type.to_string(),
            transport: "ws".into(),
            cloid: context.cloid,
            oid: context.oid,
            side: context.side.map(|side| side.as_action_side().to_string()),
            price: context.price,
            size: context.size,
            remaining_size: context.remaining_size,
            time_in_force: Some(time_in_force.to_string()),
            reduce_only,
            priority_fee: 0,
            expires_after: None,
            sign_start_ns,
            sign_end_ns,
            command_enqueue_ns,
            socket_write_ns: Some(socket_write_ns),
            raw_outbound_json,
            payload_hash,
            outcome: None,
        };
        Ok(SentAction {
            request_id,
            nonce,
            action_id: context.action_id,
            socket_write_monotonic_ns: socket_write_ns,
            record,
        })
    }

    fn next_nonce(&mut self) -> u64 {
        let now = get_mills_timestamp();
        self.last_nonce = now.max(self.last_nonce.saturating_add(1));
        self.last_nonce
    }
}

pub fn action_channel() -> WsChannel {
    WsChannel::Other(HYPERLIQUID_ORDER_ACTION_CHANNEL.to_string())
}

#[derive(Serialize)]
struct WsPostEnvelope<'a, A: Serialize> {
    method: &'static str,
    id: u64,
    request: WsActionRequest<'a, A>,
}

#[derive(Serialize)]
struct WsActionRequest<'a, A: Serialize> {
    #[serde(rename = "type")]
    kind: &'static str,
    payload: WsActionPayload<'a, A>,
}

#[derive(Serialize)]
struct WsActionPayload<'a, A: Serialize> {
    action: &'a A,
    nonce: u64,
    signature: HyperliquidSignature,
    #[serde(rename = "vaultAddress", skip_serializing_if = "Option::is_none")]
    vault_address: Option<&'a str>,
}

fn signed_ws_action<A: Serialize>(
    auth: &HyperliquidAuth,
    request_id: u64,
    action: &A,
    nonce: u64,
) -> InfraResult<String> {
    let vault_address = auth.vault_address.as_deref();
    let signature = auth.sign_l1_action(action, nonce, vault_address)?;
    serde_json::to_string(&WsPostEnvelope {
        method: "post",
        id: request_id,
        request: WsActionRequest {
            kind: "action",
            payload: WsActionPayload {
                action,
                nonce,
                signature,
                vault_address,
            },
        },
    })
    .map_err(|err| InfraError::Msg(format!("serialize Hyperliquid WS action: {err}")))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlaceOutcome {
    Resting(u64),
    Filled(u64),
    Rejected(String),
}

pub fn parse_post_frame(value: &Value) -> InfraResult<Option<(u64, &Value)>> {
    if value.get("channel").and_then(Value::as_str) != Some("post") {
        return Ok(None);
    }
    let request_id = value
        .pointer("/data/id")
        .and_then(value_as_u64)
        .ok_or_else(|| InfraError::Msg("HL post response missing data.id".into()))?;
    Ok(Some((request_id, value)))
}

pub fn place_outcome(frame: &Value) -> InfraResult<PlaceOutcome> {
    let first = response_statuses(frame)?
        .first()
        .ok_or_else(|| InfraError::Msg("HL place response has no status".into()))?;
    if let Some(oid) = first.pointer("/resting/oid").and_then(value_as_u64) {
        return Ok(PlaceOutcome::Resting(oid));
    }
    if let Some(oid) = first.pointer("/filled/oid").and_then(value_as_u64) {
        return Ok(PlaceOutcome::Filled(oid));
    }
    if let Some(error) = first.get("error").and_then(Value::as_str) {
        return Ok(PlaceOutcome::Rejected(error.to_string()));
    }
    Err(InfraError::Msg(format!(
        "unexpected HL place response: {first}"
    )))
}

pub fn cancel_succeeded(frame: &Value) -> InfraResult<()> {
    let first = response_statuses(frame)?
        .first()
        .ok_or_else(|| InfraError::Msg("HL cancel response has no status".into()))?;
    if first.as_str() == Some("success") {
        return Ok(());
    }
    if let Some(error) = first.get("error").and_then(Value::as_str) {
        return Err(InfraError::Msg(error.to_string()));
    }
    Err(InfraError::Msg(format!(
        "unexpected HL cancel response: {first}"
    )))
}

fn response_statuses(frame: &Value) -> InfraResult<&[Value]> {
    let payload = frame
        .pointer("/data/response/payload")
        .ok_or_else(|| InfraError::Msg("HL post response missing payload".into()))?;
    if payload.get("status").and_then(Value::as_str) != Some("ok") {
        return Err(InfraError::Msg(format!(
            "HL action response not ok: {payload}"
        )));
    }
    payload
        .pointer("/response/data/statuses")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| InfraError::Msg("HL action response missing statuses".into()))
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn order_side(side: ProbeSide) -> OrderSide {
    match side {
        ProbeSide::Bid => OrderSide::BUY,
        ProbeSide::Ask => OrderSide::SELL,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

impl CommandEmitter for ActionGateway {
    fn command_init(&mut self, registry: Arc<CommandRegistry>) {
        self.registry = registry;
    }

    fn command_registry(&self) -> Arc<CommandRegistry> {
        self.registry.clone()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_resting_and_cancel_responses() {
        let resting = json!({
            "channel":"post","data":{"id":1,"response":{"payload":{"status":"ok","response":{"data":{"statuses":[{"resting":{"oid":42}}]}}}}}
        });
        let canceled = json!({
            "channel":"post","data":{"id":2,"response":{"payload":{"status":"ok","response":{"data":{"statuses":["success"]}}}}}
        });
        assert_eq!(place_outcome(&resting).unwrap(), PlaceOutcome::Resting(42));
        assert!(cancel_succeeded(&canceled).is_ok());
    }
}
