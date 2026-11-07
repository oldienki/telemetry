//! Canonical telemetry event type shared by ingest, bus, storage, and APIs.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

/// One telemetry event on the wire and inside the daemon.
///
/// `ts` is the client-side creation timestamp. `ingress_ts` is stamped by the
/// server at reception and is the only timestamp used for wait matching.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryEvent {
    /// Logical event channel (e.g. `logger`, `db.query`).
    pub topic: String,
    /// Unique source/instance identifier.
    pub id: String,
    /// Source classifier or entity group (e.g. `backend`, `kernel`).
    pub class: String,
    /// Client-side creation timestamp, microseconds since UNIX epoch.
    pub ts: u64,
    /// Structured attributes carried by the event.
    #[serde(default)]
    pub payload: Value,
    /// Server-side reception timestamp, microseconds since UNIX epoch.
    #[serde(skip_deserializing)]
    pub ingress_ts: u64,
}

/// Current UNIX epoch time in microseconds.
pub fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}
