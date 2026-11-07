//! Storage layer: `StorageEngine` trait, query filter, bus→storage pipeline.

pub mod memory;
pub mod retention;
pub mod sqlite;

use crate::bus::EventBus;
use crate::event::TelemetryEvent;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tracing::warn;

/// Compound filter for `GET /events` and the wait endpoint's storage check.
///
/// `since`/`until` always compare against `ingress_ts` (server clock), never
/// the client-supplied `ts`.
#[derive(Debug, Clone, Default)]
pub struct QueryFilter {
    pub topic: Option<String>,
    pub class: Option<String>,
    pub id: Option<String>,
    /// Lower bound on `ingress_ts`, inclusive.
    pub since: Option<u64>,
    /// Upper bound on `ingress_ts`, inclusive.
    pub until: Option<u64>,
    pub limit: Option<usize>,
}

impl QueryFilter {
    /// Whether `ev` satisfies every set field of the filter.
    pub fn matches(&self, ev: &TelemetryEvent) -> bool {
        if let Some(t) = &self.topic {
            if &ev.topic != t {
                return false;
            }
        }
        if let Some(c) = &self.class {
            if &ev.class != c {
                return false;
            }
        }
        if let Some(i) = &self.id {
            if &ev.id != i {
                return false;
            }
        }
        if let Some(s) = self.since {
            if ev.ingress_ts < s {
                return false;
            }
        }
        if let Some(u) = self.until {
            if ev.ingress_ts > u {
                return false;
            }
        }
        true
    }
}

/// Pluggable persistence boundary for telemetry events.
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Persist one event.
    async fn append(&self, event: &TelemetryEvent) -> anyhow::Result<()>;
    /// Return events satisfying `filter`, oldest first, honoring `limit`.
    async fn query(&self, filter: &QueryFilter) -> anyhow::Result<Vec<TelemetryEvent>>;
    /// Delete events with `ingress_ts` older than `older_than_ts`.
    /// Returns the number of removed events.
    async fn cleanup(&self, older_than_ts: u64) -> anyhow::Result<usize>;
}

/// Consume the bus and append every event to `engine`. Lagged events are
/// reported and skipped; storage never blocks ingest.
pub fn spawn_pipeline(bus: &EventBus, engine: Arc<dyn StorageEngine>) -> JoinHandle<()> {
    let mut rx = bus.subscribe();
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    if let Err(e) = engine.append(&ev).await {
                        warn!(error = %e, "storage append failed");
                    }
                }
                Err(RecvError::Lagged(n)) => {
                    warn!(skipped = n, "storage pipeline lagged");
                }
                Err(RecvError::Closed) => break,
            }
        }
    })
}

#[cfg(test)]
pub(crate) mod tests {
    //! Shared conformance suite — every `StorageEngine` impl must pass it.
    use super::*;
    use crate::event::TelemetryEvent;
    use serde_json::json;

    pub fn ev(topic: &str, ingress_ts: u64) -> TelemetryEvent {
        TelemetryEvent {
            topic: topic.into(),
            id: "i".into(),
            class: "c".into(),
            ts: 1,
            payload: json!({"k": "v"}),
            ingress_ts,
        }
    }

    /// Minimal contract every backend must honor. Call from each engine's
    /// test module.
    pub async fn conformance(engine: &dyn StorageEngine) {
        engine.append(&ev("a", 10)).await.unwrap();
        engine.append(&ev("b", 20)).await.unwrap();
        engine.append(&ev("a", 30)).await.unwrap();

        // Happy: unfiltered query returns all in order.
        let all = engine.query(&QueryFilter::default()).await.unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].ingress_ts, 10);
        assert_eq!(all[2].ingress_ts, 30);

        // Filtered by topic and time range.
        let f = QueryFilter {
            topic: Some("a".into()),
            since: Some(15),
            until: Some(35),
            limit: None,
            ..Default::default()
        };
        let hits = engine.query(&f).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].ingress_ts, 30);

        // Limit caps results.
        let f = QueryFilter {
            limit: Some(2),
            ..Default::default()
        };
        assert_eq!(engine.query(&f).await.unwrap().len(), 2);

        // Cleanup removes only events strictly older than the bound:
        // ingress_ts 10 < 20 is removed; 20 and 30 survive.
        let removed = engine.cleanup(20).await.unwrap();
        assert_eq!(removed, 1);
        let left = engine.query(&QueryFilter::default()).await.unwrap();
        assert_eq!(left.len(), 2);
        assert!(left.iter().all(|e| e.ingress_ts >= 20));
    }
}
