use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use extrema_infra::prelude::{Market, WsChannel};

#[derive(Clone, Debug)]
pub struct FeedPlan {
    pub task_id: u64,
    pub market: Market,
    pub channel: WsChannel,
    pub source: &'static str,
    pub feed: &'static str,
    pub url: String,
    pub subscriptions: Vec<String>,
    pub is_private: bool,
    pub is_action: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ConnectionState {
    pub generation: u64,
    pub connection_id: String,
    pub frame_seq: u64,
    pub last_receive_monotonic_ns: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeSide {
    Bid,
    Ask,
}

impl ProbeSide {
    pub fn as_action_side(self) -> &'static str {
        match self {
            Self::Bid => "buy",
            Self::Ask => "sell",
        }
    }

    pub fn order_sign(self) -> f64 {
        match self {
            Self::Bid => 1.0,
            Self::Ask => -1.0,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbePhase {
    PlacePending,
    PostAccepted,
    Resting,
    Partial,
    CancelPending,
    Filled,
    MarkoutPending,
    FlattenPending,
    Recovering,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct QueueBelief {
    pub queue_ahead_lower: f64,
    pub queue_ahead_p10: f64,
    pub queue_ahead_p50: f64,
    pub queue_ahead_p90: f64,
    pub queue_ahead_upper: f64,
    pub same_price_visible_size: f64,
    pub same_price_order_count: Option<u64>,
    pub same_price_public_trades_since_ack: f64,
    pub visible_add_since_ack: f64,
    pub visible_remove_since_ack: f64,
    pub method_version: String,
}

impl QueueBelief {
    pub fn initialize(visible_size: f64, order_count: Option<u64>, own_size: f64) -> Self {
        let possible_ahead = (visible_size - own_size).max(0.0);
        Self {
            queue_ahead_lower: 0.0,
            queue_ahead_p10: possible_ahead * 0.1,
            queue_ahead_p50: possible_ahead * 0.5,
            queue_ahead_p90: possible_ahead * 0.9,
            queue_ahead_upper: possible_ahead,
            same_price_visible_size: visible_size,
            same_price_order_count: order_count,
            same_price_public_trades_since_ack: 0.0,
            visible_add_since_ack: 0.0,
            visible_remove_since_ack: 0.0,
            method_version: "interval_fifo_v1".into(),
        }
    }

    pub fn observe_trade(&mut self, size: f64) {
        if !size.is_finite() || size <= 0.0 {
            return;
        }
        self.same_price_public_trades_since_ack += size;
        self.queue_ahead_lower = (self.queue_ahead_lower - size).max(0.0);
        self.queue_ahead_p10 = (self.queue_ahead_p10 - size).max(0.0);
        self.queue_ahead_p50 = (self.queue_ahead_p50 - size).max(0.0);
        self.queue_ahead_p90 = (self.queue_ahead_p90 - size).max(0.0);
        self.queue_ahead_upper = (self.queue_ahead_upper - size).max(0.0);
    }

    pub fn observe_visible(&mut self, visible_size: f64, order_count: Option<u64>) {
        if !visible_size.is_finite() || visible_size < 0.0 {
            return;
        }
        let delta = visible_size - self.same_price_visible_size;
        if delta > 0.0 {
            self.visible_add_since_ack += delta;
        } else if delta < 0.0 {
            let removed = -delta;
            self.visible_remove_since_ack += removed;
            self.queue_ahead_lower = (self.queue_ahead_lower - removed).max(0.0);
            self.queue_ahead_p10 = (self.queue_ahead_p10 - removed * 0.1).max(0.0);
            self.queue_ahead_p50 = (self.queue_ahead_p50 - removed * 0.5).max(0.0);
            self.queue_ahead_p90 = (self.queue_ahead_p90 - removed * 0.9).max(0.0);
        }
        self.same_price_visible_size = visible_size;
        self.same_price_order_count = order_count;
        self.clamp();
    }

    pub fn observe_partial_fill(&mut self) {
        self.queue_ahead_lower = 0.0;
        self.queue_ahead_p10 = 0.0;
        self.queue_ahead_p50 = 0.0;
        self.queue_ahead_p90 = 0.0;
        self.queue_ahead_upper = 0.0;
    }

    fn clamp(&mut self) {
        self.queue_ahead_p10 = self
            .queue_ahead_p10
            .clamp(self.queue_ahead_lower, self.queue_ahead_upper);
        self.queue_ahead_p50 = self
            .queue_ahead_p50
            .clamp(self.queue_ahead_p10, self.queue_ahead_upper);
        self.queue_ahead_p90 = self
            .queue_ahead_p90
            .clamp(self.queue_ahead_p50, self.queue_ahead_upper);
    }
}

#[derive(Clone, Debug, Default)]
pub struct BookState {
    pub received_monotonic_ns: u64,
    pub exchange_ts_ns: Option<u64>,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
}

impl BookState {
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.first().map(|level| level.price)
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.asks.first().map(|level| level.price)
    }

    pub fn midpoint(&self) -> Option<f64> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        (bid > 0.0 && ask >= bid).then_some((bid + ask) * 0.5)
    }

    pub fn level(&self, side: ProbeSide, price: f64, tick: f64) -> Option<&BookLevel> {
        let levels = match side {
            ProbeSide::Bid => &self.bids,
            ProbeSide::Ask => &self.asks,
        };
        levels
            .iter()
            .find(|level| (level.price - price).abs() <= tick * 0.1)
    }
}

#[derive(Clone, Debug, Default)]
pub struct BookLevel {
    pub price: f64,
    pub size: f64,
    pub order_count: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct MarketState {
    pub feed_last_receive_ns: HashMap<&'static str, u64>,
    pub hl_bbo: BookState,
    pub hl_fast_l2: BookState,
    pub hl_default_l2: BookState,
    pub binance_bid: Option<f64>,
    pub binance_ask: Option<f64>,
    pub okx_bid: Option<f64>,
    pub okx_ask: Option<f64>,
    pub hl_trade_count: u64,
    pub hl_signed_trade_size: f64,
}

impl MarketState {
    pub fn mark_feed(&mut self, feed: &'static str, received_monotonic_ns: u64) {
        self.feed_last_receive_ns
            .insert(feed, received_monotonic_ns);
    }

    pub fn required_quotes_fresh(
        &self,
        now_ns: u64,
        freshness_ns: u64,
        default_l2_freshness_ns: u64,
    ) -> bool {
        let hl_quote_fresh = (self.hl_bbo.midpoint().is_some()
            && self.feed_fresh("hl_bbo", now_ns, freshness_ns))
            || (self.hl_fast_l2.midpoint().is_some()
                && self.feed_fresh("hl_fast_l2", now_ns, freshness_ns));
        let queue_book_fresh = (self.hl_fast_l2.midpoint().is_some()
            && self.feed_fresh("hl_fast_l2", now_ns, freshness_ns))
            || (self.hl_default_l2.midpoint().is_some()
                && self.feed_fresh("hl_default_l2", now_ns, default_l2_freshness_ns));
        let binance_quote_valid = valid_quote(self.binance_bid, self.binance_ask);
        let okx_quote_valid = valid_quote(self.okx_bid, self.okx_ask);

        hl_quote_fresh && queue_book_fresh && binance_quote_valid && okx_quote_valid
    }

    pub fn fair_value(&self) -> Option<f64> {
        self.freshest_hl_book()?.1.midpoint()
    }

    pub fn freshest_hl_book(&self) -> Option<(&'static str, &BookState)> {
        [
            ("hl_bbo", &self.hl_bbo),
            ("hl_fast_l2", &self.hl_fast_l2),
            ("hl_default_l2", &self.hl_default_l2),
        ]
        .into_iter()
        .filter(|(_, book)| book.midpoint().is_some())
        .max_by_key(|(_, book)| book.received_monotonic_ns)
    }

    pub fn clear_reconnecting_feed(&mut self, feed: &str) {
        match feed {
            "hl_standard" => {
                self.hl_bbo = BookState::default();
                self.hl_default_l2 = BookState::default();
                for name in [
                    "hl_bbo",
                    "hl_default_l2",
                    "hl_trades",
                    "hl_active_asset_ctx",
                ] {
                    self.feed_last_receive_ns.remove(name);
                }
            }
            "hl_fast_l2" => {
                self.hl_fast_l2 = BookState::default();
                self.feed_last_receive_ns.remove("hl_fast_l2");
            }
            "binance_lob" => {
                self.binance_bid = None;
                self.binance_ask = None;
                self.feed_last_receive_ns.remove("binance_bbo");
                self.feed_last_receive_ns.remove("binance_depth");
            }
            "binance_agg_trade" => {
                self.feed_last_receive_ns.remove("binance_agg_trade");
            }
            "okx_public_lob" => {
                self.okx_bid = None;
                self.okx_ask = None;
                self.feed_last_receive_ns.remove("okx_bbo");
                self.feed_last_receive_ns.remove("okx_books");
            }
            "okx_trades_all" => {
                self.feed_last_receive_ns.remove("okx_trades_all");
            }
            _ => {}
        }
    }

    fn feed_fresh(&self, feed: &'static str, now_ns: u64, freshness_ns: u64) -> bool {
        self.feed_last_receive_ns
            .get(feed)
            .is_some_and(|timestamp| now_ns.saturating_sub(*timestamp) <= freshness_ns)
    }

    pub fn book_for_queue(&self) -> &BookState {
        if self.hl_fast_l2.received_monotonic_ns >= self.hl_default_l2.received_monotonic_ns {
            &self.hl_fast_l2
        } else {
            &self.hl_default_l2
        }
    }

    pub fn snapshot(&self, now_ns: u64) -> Value {
        let age = |feed: &'static str| {
            self.feed_last_receive_ns
                .get(feed)
                .map(|timestamp| now_ns.saturating_sub(*timestamp) / 1_000_000)
        };
        let freshest_hl = self.freshest_hl_book();
        json!({
            "fair_value": self.fair_value(),
            "fair_value_source": freshest_hl.map(|(source, _)| source),
            "hl_best_bid": freshest_hl.and_then(|(_, book)| book.best_bid()),
            "hl_best_ask": freshest_hl.and_then(|(_, book)| book.best_ask()),
            "binance_bid": self.binance_bid,
            "binance_ask": self.binance_ask,
            "okx_bid": self.okx_bid,
            "okx_ask": self.okx_ask,
            "feed_age_ms": {
                "hl_bbo": age("hl_bbo"),
                "hl_fast_l2": age("hl_fast_l2"),
                "hl_default_l2": age("hl_default_l2"),
                "hl_trades": age("hl_trades"),
                "hl_active_asset_ctx": age("hl_active_asset_ctx"),
                "binance_bbo": age("binance_bbo"),
                "binance_depth": age("binance_depth"),
                "binance_agg_trade": age("binance_agg_trade"),
                "okx_bbo": age("okx_bbo"),
                "okx_books": age("okx_books"),
                "okx_trades_all": age("okx_trades_all"),
            },
            "fast_l2_age_ms": now_ns.saturating_sub(self.hl_fast_l2.received_monotonic_ns) / 1_000_000,
            "default_l2_age_ms": now_ns.saturating_sub(self.hl_default_l2.received_monotonic_ns) / 1_000_000,
            "hl_trade_count": self.hl_trade_count,
            "hl_signed_trade_size": self.hl_signed_trade_size,
        })
    }
}

fn valid_quote(bid: Option<f64>, ask: Option<f64>) -> bool {
    matches!((bid, ask), (Some(bid), Some(ask)) if bid > 0.0 && ask >= bid)
}

#[derive(Clone, Debug)]
pub struct ActiveProbe {
    pub probe_id: String,
    pub decision_id: String,
    pub action_id: String,
    pub cloid: String,
    pub oid: Option<u64>,
    pub side: ProbeSide,
    pub price_level_ticks: u32,
    pub price: f64,
    pub tick_size: f64,
    pub size: f64,
    pub remaining_size: f64,
    pub cumulative_filled_size: f64,
    pub phase: ProbePhase,
    pub created_monotonic_ns: u64,
    pub send_monotonic_ns: u64,
    pub post_accepted_monotonic_ns: Option<u64>,
    pub open_monotonic_ns: Option<u64>,
    pub first_fill_monotonic_ns: Option<u64>,
    pub planned_expiry_monotonic_ns: u64,
    pub planned_dwell_ms: u64,
    pub last_keep_monotonic_ns: u64,
    pub cancel_send_monotonic_ns: Option<u64>,
    pub post_request_id: u64,
    pub cancel_request_id: Option<u64>,
    pub flatten_request_id: Option<u64>,
    pub flatten_oid: Option<u64>,
    pub flatten_action_id: Option<String>,
    pub flatten_send_monotonic_ns: Option<u64>,
    pub terminal_order_update_monotonic_ns: Option<u64>,
    pub recovery_not_before_monotonic_ns: u64,
    pub end_reason: Option<String>,
    pub initial_queue_belief: Option<QueueBelief>,
    pub queue_belief: QueueBelief,
}

#[derive(Clone, Debug)]
pub struct PendingMarkout {
    pub probe_id: String,
    pub oid: u64,
    pub fill_id: String,
    pub side: ProbeSide,
    pub fill_price: f64,
    pub fill_size: f64,
    pub fill_monotonic_ns: u64,
    pub horizons_ms: Vec<u64>,
    pub next_horizon_index: usize,
}

impl PendingMarkout {
    pub fn is_complete(&self) -> bool {
        self.next_horizon_index >= self.horizons_ms.len()
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AccountState {
    pub snapshot_monotonic_ns: u64,
    pub rest_snapshot_healthy: bool,
    pub confirmed_inventory: f64,
    pub account_value: Option<f64>,
    pub withdrawable: Option<f64>,
    pub open_order_count: usize,
    pub open_orders: Vec<AccountOpenOrder>,
    pub requests_used: Option<u64>,
    pub requests_cap: Option<u64>,
    pub requests_surplus: Option<u64>,
    pub maker_fee: Option<f64>,
    pub taker_fee: Option<f64>,
    pub active_referral_discount: Option<f64>,
    pub lot_size: Option<f64>,
    pub size_decimals: Option<u32>,
    pub growth_mode: Option<String>,
    pub deployer_fee_scale: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AccountOpenOrder {
    pub oid: u64,
    pub cloid: Option<String>,
    pub price: f64,
    pub size: f64,
    pub executed_size: f64,
}

impl AccountState {
    pub fn action_budget_remaining(&self) -> Option<u64> {
        Some(self.requests_cap?.saturating_sub(self.requests_used?))
    }
}

#[derive(Clone, Debug)]
pub struct OrderUpdate {
    pub coin: String,
    pub side: ProbeSide,
    pub price: f64,
    pub remaining_size: f64,
    pub original_size: f64,
    pub oid: u64,
    pub cloid: Option<String>,
    pub order_timestamp_ms: u64,
    pub status: String,
    pub status_timestamp_ms: u64,
}

#[derive(Clone, Debug)]
pub struct UserFill {
    pub oid: u64,
    pub trade_id: String,
    pub coin: String,
    pub side: ProbeSide,
    pub price: f64,
    pub size: f64,
    pub timestamp_ms: u64,
    pub fee: Option<f64>,
    pub fee_token: Option<String>,
    pub raw: Value,
}

pub fn frame_channel(value: &Value) -> Option<&str> {
    value.get("channel").and_then(Value::as_str).or_else(|| {
        value
            .pointer("/arg/channel")
            .and_then(Value::as_str)
            .or_else(|| value.get("e").and_then(Value::as_str))
    })
}

pub fn exchange_timestamp_ns(value: &Value) -> Option<u64> {
    let timestamp = [
        value.pointer("/data/time"),
        value.pointer("/data/0/time"),
        value.pointer("/data/0/ts"),
        value.get("E"),
        value.get("T"),
        value.get("ts"),
    ]
    .into_iter()
    .flatten()
    .find_map(json_u64)?;
    Some(timestamp_to_ns(timestamp))
}

pub fn parse_hl_book(value: &Value, received_monotonic_ns: u64) -> Option<BookState> {
    let data = value.get("data")?;
    let exchange_ts_ns = data.get("time").and_then(json_u64).map(timestamp_to_ns);
    if let Some(levels) = data.get("levels").and_then(Value::as_array) {
        return Some(BookState {
            received_monotonic_ns,
            exchange_ts_ns,
            bids: parse_hl_levels(levels.first()),
            asks: parse_hl_levels(levels.get(1)),
        });
    }
    let bbo = data.get("bbo")?.as_array()?;
    Some(BookState {
        received_monotonic_ns,
        exchange_ts_ns,
        bids: bbo
            .first()
            .filter(|value| !value.is_null())
            .and_then(parse_hl_level)
            .into_iter()
            .collect(),
        asks: bbo
            .get(1)
            .filter(|value| !value.is_null())
            .and_then(parse_hl_level)
            .into_iter()
            .collect(),
    })
}

pub fn parse_hl_trades(value: &Value) -> Vec<(ProbeSide, f64, f64)> {
    value
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|trade| {
            let side = match trade.get("side")?.as_str()? {
                "B" => ProbeSide::Bid,
                "A" => ProbeSide::Ask,
                _ => return None,
            };
            Some((
                side,
                json_f64(trade.get("px")?)?,
                json_f64(trade.get("sz")?)?,
            ))
        })
        .collect()
}

pub fn parse_order_updates(value: &Value) -> Vec<OrderUpdate> {
    value
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let order = entry.get("order")?;
            Some(OrderUpdate {
                coin: order.get("coin")?.as_str()?.to_string(),
                side: match order.get("side")?.as_str()? {
                    "B" => ProbeSide::Bid,
                    "A" => ProbeSide::Ask,
                    _ => return None,
                },
                price: json_f64(order.get("limitPx")?)?,
                remaining_size: json_f64(order.get("sz")?)?,
                original_size: json_f64(order.get("origSz")?)?,
                oid: json_u64(order.get("oid")?)?,
                cloid: order
                    .get("cloid")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string),
                order_timestamp_ms: json_u64(order.get("timestamp")?)?,
                status: entry.get("status")?.as_str()?.to_string(),
                status_timestamp_ms: json_u64(entry.get("statusTimestamp")?)?,
            })
        })
        .collect()
}

pub fn parse_user_fills(value: &Value) -> Vec<UserFill> {
    let fills = value
        .pointer("/data/fills")
        .or_else(|| value.get("data"))
        .and_then(Value::as_array);
    fills
        .into_iter()
        .flatten()
        .filter_map(|fill| {
            Some(UserFill {
                oid: json_u64(fill.get("oid")?)?,
                trade_id: fill.get("tid").map(value_to_string).unwrap_or_default(),
                coin: fill.get("coin")?.as_str()?.to_string(),
                side: match fill.get("side")?.as_str()? {
                    "B" => ProbeSide::Bid,
                    "A" => ProbeSide::Ask,
                    _ => return None,
                },
                price: json_f64(fill.get("px")?)?,
                size: json_f64(fill.get("sz")?)?,
                timestamp_ms: json_u64(fill.get("time")?)?,
                fee: fill.get("fee").and_then(json_f64),
                fee_token: fill
                    .get("feeToken")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                raw: fill.clone(),
            })
        })
        .collect()
}

pub fn parse_xyz_inventory(value: &Value, coin: &str) -> Option<f64> {
    let positions = value
        .pointer("/data/clearinghouseState/assetPositions")
        .or_else(|| value.pointer("/data/assetPositions"))?
        .as_array()?;
    positions.iter().find_map(|entry| {
        let position = entry.get("position")?;
        (position.get("coin")?.as_str()? == coin)
            .then(|| position.get("szi").and_then(json_f64))
            .flatten()
    })
}

pub fn parse_binance_bbo(value: &Value) -> Option<(f64, f64)> {
    (value.get("e").and_then(Value::as_str) == Some("bookTicker"))
        .then(|| Some((json_f64(value.get("b")?)?, json_f64(value.get("a")?)?)))?
}

pub fn parse_okx_bbo(value: &Value) -> Option<(f64, f64)> {
    if value.pointer("/arg/channel").and_then(Value::as_str) != Some("bbo-tbt") {
        return None;
    }
    let row = value.pointer("/data/0")?;
    let bid = row
        .get("bids")?
        .as_array()?
        .first()?
        .as_array()?
        .first()
        .and_then(json_f64)?;
    let ask = row
        .get("asks")?
        .as_array()?
        .first()?
        .as_array()?
        .first()
        .and_then(json_f64)?;
    Some((bid, ask))
}

pub fn json_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|number| number.try_into().ok()))
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

pub fn json_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .filter(|number| number.is_finite())
}

fn value_to_string(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

fn timestamp_to_ns(timestamp: u64) -> u64 {
    match timestamp {
        0..=9_999_999_999 => timestamp.saturating_mul(1_000_000_000),
        10_000_000_000..=9_999_999_999_999 => timestamp.saturating_mul(1_000_000),
        10_000_000_000_000..=9_999_999_999_999_999 => timestamp.saturating_mul(1_000),
        _ => timestamp,
    }
}

fn parse_hl_levels(value: Option<&Value>) -> Vec<BookLevel> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(parse_hl_level)
        .collect()
}

fn parse_hl_level(value: &Value) -> Option<BookLevel> {
    Some(BookLevel {
        price: json_f64(value.get("px")?)?,
        size: json_f64(value.get("sz")?)?,
        order_count: value.get("n").and_then(json_u64),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_belief_preserves_uncertainty() {
        let mut belief = QueueBelief::initialize(100.0, Some(10), 1.0);
        belief.observe_trade(20.0);
        assert_eq!(belief.queue_ahead_upper, 79.0);
        assert_eq!(belief.queue_ahead_p50, 29.5);
        belief.observe_visible(70.0, Some(7));
        assert!(belief.queue_ahead_lower <= belief.queue_ahead_p10);
        assert!(belief.queue_ahead_p10 <= belief.queue_ahead_p50);
        assert!(belief.queue_ahead_p50 <= belief.queue_ahead_p90);
        assert!(belief.queue_ahead_p90 <= belief.queue_ahead_upper);
    }

    #[test]
    fn parses_private_order_and_fill_frames() {
        let order = json!({
            "channel": "orderUpdates",
            "data": [{
                "order": {"coin":"xyz:XYZ100","side":"B","limitPx":"29500","sz":"0.0003","origSz":"0.0005","oid":42,"timestamp":1,"cloid":"0xabc"},
                "status":"open","statusTimestamp":2
            }]
        });
        let fills = json!({
            "channel":"userFills",
            "data":{"fills":[{"oid":42,"tid":99,"coin":"xyz:XYZ100","side":"B","px":"29500","sz":"0.0002","time":3,"fee":"0.001","feeToken":"USDC"}]}
        });

        let parsed_order = parse_order_updates(&order);
        assert_eq!(parsed_order[0].oid, 42);
        assert_eq!(parsed_order[0].remaining_size, 0.0003);
        let parsed_fill = parse_user_fills(&fills);
        assert_eq!(parsed_fill[0].trade_id, "99");
        assert_eq!(parsed_fill[0].size, 0.0002);
    }

    #[test]
    fn parses_hl_books_without_losing_order_count() {
        let value = json!({
            "channel":"l2Book",
            "data":{"time":10,"levels":[
                [{"px":"29500","sz":"12.5","n":44}],
                [{"px":"29501","sz":"13.5","n":45}]
            ]}
        });
        let book = parse_hl_book(&value, 100).unwrap();
        assert_eq!(book.bids[0].order_count, Some(44));
        assert_eq!(book.asks[0].price, 29_501.0);
    }

    #[test]
    fn markout_is_complete_only_after_every_horizon() {
        let mut markout = PendingMarkout {
            probe_id: "probe-1".into(),
            oid: 42,
            fill_id: "42:1".into(),
            side: ProbeSide::Bid,
            fill_price: 100.0,
            fill_size: 0.1,
            fill_monotonic_ns: 10,
            horizons_ms: vec![100, 500, 1_000, 5_000],
            next_horizon_index: 3,
        };
        assert!(!markout.is_complete());
        markout.next_horizon_index = 4;
        assert!(markout.is_complete());
    }

    #[test]
    fn quiet_external_quote_remains_valid_without_new_market_events() {
        let mut market = MarketState {
            hl_bbo: test_book(100, 100.0, 101.0),
            hl_fast_l2: test_book(1_000, 100.0, 101.0),
            hl_default_l2: test_book(1, 100.0, 101.0),
            binance_bid: Some(99.0),
            binance_ask: Some(100.0),
            okx_bid: Some(99.0),
            okx_ask: Some(100.0),
            ..Default::default()
        };
        market.mark_feed("hl_bbo", 100);
        market.mark_feed("hl_fast_l2", 1_000);
        market.mark_feed("hl_default_l2", 1);
        market.mark_feed("binance_bbo", 100);
        market.mark_feed("binance_depth", 100);
        market.mark_feed("okx_bbo", 100);
        market.mark_feed("okx_books", 100);

        assert!(market.required_quotes_fresh(2_000, 1_000, 10_000));
    }

    #[test]
    fn venue_requires_an_initialized_quote_even_when_depth_is_live() {
        let mut market = MarketState {
            hl_fast_l2: test_book(1_000, 100.0, 101.0),
            hl_default_l2: test_book(1_000, 100.0, 101.0),
            okx_bid: Some(99.0),
            okx_ask: Some(100.0),
            ..Default::default()
        };
        for feed in ["hl_fast_l2", "hl_default_l2", "binance_depth", "okx_books"] {
            market.mark_feed(feed, 1_000);
        }

        assert!(!market.required_quotes_fresh(2_000, 1_000, 10_000));
    }

    #[test]
    fn fair_value_uses_freshest_valid_hl_book() {
        let mut market = MarketState {
            hl_bbo: test_book(100, 100.0, 102.0),
            hl_fast_l2: test_book(200, 103.0, 104.0),
            hl_default_l2: test_book(300, 106.0, 105.0),
            ..Default::default()
        };

        assert_eq!(market.fair_value(), Some(103.5));
        market.clear_reconnecting_feed("hl_fast_l2");
        assert_eq!(market.fair_value(), Some(101.0));
    }

    #[test]
    fn reconnect_clears_old_quote_and_liveness() {
        let mut market = MarketState {
            binance_bid: Some(99.0),
            binance_ask: Some(100.0),
            ..Default::default()
        };
        market.mark_feed("binance_bbo", 100);
        market.mark_feed("binance_depth", 200);

        market.clear_reconnecting_feed("binance_lob");

        assert_eq!(market.binance_bid, None);
        assert!(!market.feed_last_receive_ns.contains_key("binance_bbo"));
        assert!(!market.feed_last_receive_ns.contains_key("binance_depth"));
    }

    fn test_book(received_monotonic_ns: u64, bid: f64, ask: f64) -> BookState {
        BookState {
            received_monotonic_ns,
            bids: vec![BookLevel {
                price: bid,
                size: 1.0,
                order_count: Some(1),
            }],
            asks: vec![BookLevel {
                price: ask,
                size: 1.0,
                order_count: Some(1),
            }],
            ..Default::default()
        }
    }
}
