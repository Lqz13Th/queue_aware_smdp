use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub schema_version: u16,
    pub run_id: String,
    pub host_id: String,
    pub process_id: u32,
    pub build_commit: String,
    pub event_id: String,
    pub event_type: String,
    pub exchange_ts_ns: Option<u64>,
    pub received_wall_ns: u64,
    pub received_monotonic_ns: u64,
    pub decoded_monotonic_ns: u64,
    pub connection_id: Option<String>,
    pub connection_frame_seq: Option<u64>,
    pub raw_json: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawEventRecord {
    #[serde(flatten)]
    pub envelope: EventEnvelope,
    pub source: String,
    pub feed: String,
    pub channel: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecisionRecord {
    #[serde(flatten)]
    pub envelope: EventEnvelope,
    pub probe_id: String,
    pub decision_id: String,
    pub decision_ts_ns: u64,
    pub trigger_type: String,
    pub state_snapshot_id: String,
    pub state_snapshot: Value,
    pub candidate_actions: Vec<Value>,
    pub selected_action: Value,
    pub behavior_policy_id: String,
    pub selection_probability: f64,
    pub random_seed: u64,
    pub planned_expiry_ts_ns: Option<u64>,
    pub risk_limits: Value,
    pub reason_codes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionRecord {
    #[serde(flatten)]
    pub envelope: EventEnvelope,
    pub action_id: String,
    pub probe_id: String,
    pub decision_id: String,
    pub intent_id: String,
    pub request_id: u64,
    pub nonce: u64,
    pub action_type: String,
    pub transport: String,
    pub cloid: Option<String>,
    pub oid: Option<u64>,
    pub side: Option<String>,
    pub price: Option<String>,
    pub size: Option<String>,
    pub remaining_size: Option<String>,
    pub time_in_force: Option<String>,
    pub reduce_only: bool,
    pub priority_fee: u64,
    pub expires_after: Option<u64>,
    pub sign_start_ns: u64,
    pub sign_end_ns: u64,
    pub command_enqueue_ns: u64,
    pub socket_write_ns: Option<u64>,
    pub raw_outbound_json: String,
    pub payload_hash: String,
    pub outcome: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountSnapshotRecord {
    #[serde(flatten)]
    pub envelope: EventEnvelope,
    pub reason: String,
    pub snapshot: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SystemRecord {
    #[serde(flatten)]
    pub envelope: EventEnvelope,
    pub kind: String,
    pub details: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunManifest {
    pub schema_version: u16,
    pub run_id: String,
    pub host_id: String,
    pub process_id: u32,
    pub build_commit: String,
    pub process_start_wall_ns: u64,
    pub config_path: String,
    pub probe_enabled: bool,
    pub streams: Vec<String>,
}

pub fn empty_envelope(
    run_id: &str,
    host_id: &str,
    build_commit: &str,
    event_id: String,
    event_type: impl Into<String>,
    wall_ns: u64,
    monotonic_ns: u64,
) -> EventEnvelope {
    EventEnvelope {
        schema_version: SCHEMA_VERSION,
        run_id: run_id.to_string(),
        host_id: host_id.to_string(),
        process_id: std::process::id(),
        build_commit: build_commit.to_string(),
        event_id,
        event_type: event_type.into(),
        exchange_ts_ns: None,
        received_wall_ns: wall_ns,
        received_monotonic_ns: monotonic_ns,
        decoded_monotonic_ns: monotonic_ns,
        connection_id: None,
        connection_frame_seq: None,
        raw_json: None,
    }
}
