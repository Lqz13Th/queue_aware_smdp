use std::{
    env, fs,
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use extrema_infra::{
    arch::market_assets::api_general::normalize_to_string_reduce_only,
    errors::{InfraError, InfraResult},
};

pub const TASK_HL_STANDARD: u64 = 10_001;
pub const TASK_HL_FAST_L2: u64 = 10_002;
pub const TASK_HL_PRIVATE: u64 = 10_003;
pub const TASK_HL_ACTION: u64 = 10_004;
pub const TASK_BINANCE_LOB: u64 = 20_001;
pub const TASK_BINANCE_AGG_TRADE: u64 = 20_002;
pub const TASK_OKX_LOB: u64 = 30_001;
pub const TASK_OKX_TRADES_ALL: u64 = 30_002;
pub const TASK_PROBE_TIMER: u64 = 90_001;

pub const LIVE_GUARD_ENV: &str = "QUEUE_AWARE_SMDP_ALLOW_LIVE";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AppConfig {
    pub collector: CollectorConfig,
    pub markets: MarketConfig,
    pub probe: ProbeConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CollectorConfig {
    pub host_id: String,
    pub data_root: PathBuf,
    #[serde(default = "default_writer_capacity")]
    pub writer_capacity: usize,
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    #[serde(default = "default_sync_interval_sec")]
    pub sync_interval_sec: u64,
    #[serde(default = "default_zstd_level")]
    pub zstd_level: i32,
    #[serde(default = "default_schedule_interval_ms")]
    pub schedule_interval_ms: u64,
    #[serde(default = "default_account_snapshot_interval_sec")]
    pub account_snapshot_interval_sec: u64,
    #[serde(default = "default_system_snapshot_interval_sec")]
    pub system_snapshot_interval_sec: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MarketConfig {
    #[serde(default = "default_hl_instrument")]
    pub hyperliquid_instrument: String,
    #[serde(default = "default_hl_coin")]
    pub hyperliquid_coin: String,
    #[serde(default = "default_hl_dex")]
    pub hyperliquid_perp_dex: String,
    #[serde(default = "default_binance_instrument")]
    pub binance_instrument: String,
    #[serde(default = "default_okx_instrument")]
    pub okx_instrument: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProbeConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub i_understand_live_orders: bool,
    #[serde(default = "default_probe_notional")]
    pub probe_notional_usd: f64,
    #[serde(default = "default_max_order_notional")]
    pub max_order_notional_usd: f64,
    #[serde(default = "default_max_abs_inventory")]
    pub max_abs_inventory_usd: f64,
    #[serde(default = "default_price_levels")]
    pub price_levels_ticks: Vec<u32>,
    #[serde(default = "default_dwell_seconds")]
    pub dwell_seconds: Vec<u64>,
    #[serde(default = "default_keep_interval_ms")]
    pub keep_interval_ms: u64,
    #[serde(default = "default_cooldown_ms")]
    pub cooldown_ms: u64,
    #[serde(default = "default_freshness_ms")]
    pub market_freshness_ms: u64,
    #[serde(default = "default_default_l2_freshness_ms")]
    pub default_l2_freshness_ms: u64,
    #[serde(default = "default_cancel_timeout_ms")]
    pub cancel_timeout_ms: u64,
    #[serde(default = "default_post_timeout_ms")]
    pub post_timeout_ms: u64,
    #[serde(default = "default_markout_horizons_ms")]
    pub markout_horizons_ms: Vec<u64>,
    #[serde(default = "default_ioc_slippage_bps")]
    pub ioc_slippage_bps: f64,
    #[serde(default = "default_fair_value_guard_ticks")]
    pub fair_value_guard_ticks: f64,
    #[serde(default = "default_behavior_seed")]
    pub behavior_seed: u64,
    #[serde(default = "default_behavior_policy_id")]
    pub behavior_policy_id: String,
}

impl AppConfig {
    pub fn load(path: &Path) -> InfraResult<Self> {
        let raw = fs::read_to_string(path)
            .map_err(|err| InfraError::Msg(format!("read {}: {err}", path.display())))?;
        let config: Self = toml::from_str(&raw)
            .map_err(|err| InfraError::Msg(format!("parse {}: {err}", path.display())))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> InfraResult<()> {
        if self.collector.host_id.trim().is_empty() {
            return Err(InfraError::Msg(
                "collector.host_id must not be empty".into(),
            ));
        }
        if self.collector.data_root.as_os_str().is_empty() {
            return Err(InfraError::Msg(
                "collector.data_root must not be empty".into(),
            ));
        }
        if self.collector.writer_capacity == 0
            || self.collector.flush_interval_ms == 0
            || self.collector.sync_interval_sec == 0
            || self.collector.schedule_interval_ms == 0
            || self.collector.account_snapshot_interval_sec == 0
            || self.collector.system_snapshot_interval_sec == 0
        {
            return Err(InfraError::Msg(
                "collector intervals and capacity must be positive".into(),
            ));
        }
        if !(-7..=22).contains(&self.collector.zstd_level) {
            return Err(InfraError::Msg(
                "collector.zstd_level must be between -7 and 22".into(),
            ));
        }
        if self.markets.hyperliquid_instrument.trim().is_empty()
            || self.markets.hyperliquid_coin.trim().is_empty()
            || self.markets.binance_instrument.trim().is_empty()
            || self.markets.okx_instrument.trim().is_empty()
            || self.markets.hyperliquid_perp_dex.trim().is_empty()
        {
            return Err(InfraError::Msg(
                "all market instruments must be configured".into(),
            ));
        }
        if self.probe.enabled && !self.probe.i_understand_live_orders {
            return Err(InfraError::Msg(
                "probe.enabled requires probe.i_understand_live_orders = true".into(),
            ));
        }
        if self.probe.enabled && env::var(LIVE_GUARD_ENV).ok().as_deref() != Some("1") {
            return Err(InfraError::Msg(format!(
                "probe.enabled requires {LIVE_GUARD_ENV}=1"
            )));
        }
        if !self.probe.probe_notional_usd.is_finite()
            || self.probe.probe_notional_usd <= 0.0
            || !self.probe.max_order_notional_usd.is_finite()
            || self.probe.max_order_notional_usd <= 0.0
            || self.probe.probe_notional_usd > self.probe.max_order_notional_usd
        {
            return Err(InfraError::Msg(
                "probe_notional_usd must be positive and no greater than max_order_notional_usd"
                    .into(),
            ));
        }
        if !self.probe.max_abs_inventory_usd.is_finite()
            || self.probe.max_abs_inventory_usd < self.probe.max_order_notional_usd
        {
            return Err(InfraError::Msg(
                "max_abs_inventory_usd must be finite and no smaller than max_order_notional_usd"
                    .into(),
            ));
        }
        if self.probe.price_levels_ticks.is_empty()
            || self.probe.dwell_seconds.is_empty()
            || self.probe.markout_horizons_ms.is_empty()
        {
            return Err(InfraError::Msg(
                "probe price levels, dwell times, and markout horizons must not be empty".into(),
            ));
        }
        if self
            .probe
            .markout_horizons_ms
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(InfraError::Msg(
                "probe.markout_horizons_ms must be strictly increasing".into(),
            ));
        }
        if self.probe.dwell_seconds.contains(&0)
            || self.probe.markout_horizons_ms.contains(&0)
            || self.probe.keep_interval_ms == 0
            || self.probe.market_freshness_ms == 0
            || self.probe.default_l2_freshness_ms == 0
            || self.probe.cancel_timeout_ms == 0
            || self.probe.post_timeout_ms == 0
        {
            return Err(InfraError::Msg(
                "probe dwell, markout, keep, freshness, and timeout values must be positive".into(),
            ));
        }
        if !self.probe.ioc_slippage_bps.is_finite()
            || !(0.0..=100.0).contains(&self.probe.ioc_slippage_bps)
            || !self.probe.fair_value_guard_ticks.is_finite()
            || self.probe.fair_value_guard_ticks <= 0.0
        {
            return Err(InfraError::Msg(
                "IOC slippage must be 0-100 bps and fair-value guard must be positive".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ProcessClock {
    origin: Instant,
    pub process_start_wall_ns: u64,
}

impl ProcessClock {
    pub fn new() -> InfraResult<Self> {
        Ok(Self {
            origin: Instant::now(),
            process_start_wall_ns: wall_time_ns()?,
        })
    }

    pub fn monotonic_ns(&self) -> u64 {
        self.origin.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
    }
}

pub fn wall_time_ns() -> InfraResult<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| InfraError::Msg(format!("system clock before unix epoch: {err}")))?;
    Ok(duration.as_nanos().min(u128::from(u64::MAX)) as u64)
}

pub fn build_commit() -> String {
    option_env!("QUEUE_AWARE_SMDP_BUILD_COMMIT")
        .unwrap_or("unknown")
        .to_string()
}

pub fn make_run_id(host_id: &str, process_start_wall_ns: u64) -> String {
    format!("{host_id}-{process_start_wall_ns}-{}", std::process::id())
}

pub fn make_cloid(run_id: &str, probe_sequence: u64) -> String {
    let mut hash = Sha256::new();
    hash.update(run_id.as_bytes());
    hash.update(probe_sequence.to_be_bytes());
    let digest = hash.finalize();
    format!(
        "0x{}",
        digest[..16]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

pub fn hyperliquid_tick_size(price: f64, size_decimals: u32) -> Option<f64> {
    if !price.is_finite() || price <= 0.0 || size_decimals > 6 {
        return None;
    }
    let digits_before_decimal = price.log10().floor() as i32 + 1;
    let significant_digit_tick = 10_f64.powi(digits_before_decimal - 5);
    let decimal_cap_tick = 10_f64.powi(-((6 - size_decimals) as i32));
    Some(significant_digit_tick.max(decimal_cap_tick))
}

pub fn round_price_to_tick(price: f64, tick: f64, round_up: bool) -> Option<String> {
    if !price.is_finite() || price <= 0.0 || !tick.is_finite() || tick <= 0.0 {
        return None;
    }
    let units = price / tick;
    let rounded = if round_up {
        units.ceil()
    } else {
        units.floor()
    } * tick;
    Some(trim_decimal(rounded, tick))
}

pub fn minimum_probe_size(notional: f64, price: f64, lot_size: f64) -> Option<String> {
    if !notional.is_finite()
        || notional <= 0.0
        || !price.is_finite()
        || price <= 0.0
        || !lot_size.is_finite()
        || lot_size <= 0.0
    {
        return None;
    }
    let lots = (notional / price / lot_size).ceil().max(1.0);
    Some(trim_decimal(lots * lot_size, lot_size))
}

pub fn reduce_only_size(size: f64, lot_size: f64) -> Option<String> {
    if !size.is_finite() || size <= 0.0 || !lot_size.is_finite() || lot_size <= 0.0 {
        return None;
    }
    let value = normalize_to_string_reduce_only(size, lot_size);
    (value.parse::<f64>().ok()? > 0.0).then_some(value)
}

fn trim_decimal(value: f64, step: f64) -> String {
    let decimals = step
        .to_string()
        .split_once('.')
        .map(|(_, fraction)| fraction.trim_end_matches('0').len())
        .unwrap_or(0);
    let text = format!("{value:.decimals$}");
    if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        text
    }
}

fn default_writer_capacity() -> usize {
    32_768
}
fn default_flush_interval_ms() -> u64 {
    250
}
fn default_sync_interval_sec() -> u64 {
    5
}
fn default_zstd_level() -> i32 {
    3
}
fn default_schedule_interval_ms() -> u64 {
    100
}
fn default_account_snapshot_interval_sec() -> u64 {
    30
}
fn default_system_snapshot_interval_sec() -> u64 {
    30
}
fn default_hl_instrument() -> String {
    "XYZ100_USDC_PERP".into()
}
fn default_hl_coin() -> String {
    "xyz:XYZ100".into()
}
fn default_hl_dex() -> String {
    "xyz".into()
}
fn default_binance_instrument() -> String {
    "QQQ_USDT_PERP".into()
}
fn default_okx_instrument() -> String {
    "QQQ_USDT_PERP".into()
}
fn default_probe_notional() -> f64 {
    12.0
}
fn default_max_order_notional() -> f64 {
    15.0
}
fn default_max_abs_inventory() -> f64 {
    20.0
}
fn default_price_levels() -> Vec<u32> {
    vec![0, 1]
}
fn default_dwell_seconds() -> Vec<u64> {
    vec![30, 60, 120, 300]
}
fn default_keep_interval_ms() -> u64 {
    5_000
}
fn default_cooldown_ms() -> u64 {
    10_000
}
fn default_freshness_ms() -> u64 {
    2_000
}
fn default_default_l2_freshness_ms() -> u64 {
    15_000
}
fn default_cancel_timeout_ms() -> u64 {
    5_000
}
fn default_post_timeout_ms() -> u64 {
    5_000
}
fn default_markout_horizons_ms() -> Vec<u64> {
    vec![100, 500, 1_000, 5_000]
}
fn default_ioc_slippage_bps() -> f64 {
    20.0
}
fn default_fair_value_guard_ticks() -> f64 {
    2.0
}
fn default_behavior_seed() -> u64 {
    20_260_827
}
fn default_behavior_policy_id() -> String {
    "uniform_probe_v1".into()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn computes_hyperliquid_price_grid() {
        assert_eq!(hyperliquid_tick_size(29_577.0, 4), Some(1.0));
        assert_eq!(hyperliquid_tick_size(999.9, 4), Some(0.01));
        assert_eq!(hyperliquid_tick_size(1.2345, 4), Some(0.01));
        assert_eq!(
            round_price_to_tick(29_577.9, 1.0, false).as_deref(),
            Some("29577")
        );
        assert_eq!(
            round_price_to_tick(29_577.1, 1.0, true).as_deref(),
            Some("29578")
        );
    }

    #[test]
    fn rounds_probe_size_up_to_lot() {
        assert_eq!(
            minimum_probe_size(12.0, 29_577.0, 0.0001).as_deref(),
            Some("0.0005")
        );
    }

    #[test]
    fn example_config_is_valid_and_collection_only() {
        let config = AppConfig::load(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/strategy_config.toml.example"
        )))
        .unwrap();
        assert!(!config.probe.enabled);
        assert!(!config.probe.i_understand_live_orders);
        assert_eq!(config.markets.hyperliquid_coin, "xyz:XYZ100");
    }

    #[test]
    fn live_probe_requires_explicit_config_acknowledgement() {
        let mut config: AppConfig =
            toml::from_str(include_str!("../../../strategy_config.toml.example")).unwrap();
        config.probe.enabled = true;
        config.probe.i_understand_live_orders = false;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("i_understand_live_orders")
        );
    }
}
