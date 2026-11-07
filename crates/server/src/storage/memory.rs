//! In-memory ring-buffer storage engine: O(1) eviction, lock-bounded reads.

use crate::event::TelemetryEvent;
use crate::storage::{QueryFilter, StorageEngine};
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::VecDeque;

/// Default ring capacity when `--memory-capacity` is not given.
pub const DEFAULT_CAPACITY: usize = 100_000;

/// Fixed-capacity ring of the most recent events.
pub struct MemoryEngine {
    ring: RwLock<VecDeque<TelemetryEvent>>,
    capacity: usize,
}

impl MemoryEngine {
    /// Create an engine retaining at most `capacity` events.
    pub fn new(capacity: usize) -> Self {
        Self {
            ring: RwLock::new(VecDeque::with_capacity(capacity.min(65_536))),
            capacity,
        }
    }
}

#[async_trait]
impl StorageEngine for MemoryEngine {
    async fn append(&self, event: &TelemetryEvent) -> anyhow::Result<()> {
        let mut ring = self.ring.write();
        if ring.len() >= self.capacity {
            ring.pop_front();
        }
        ring.push_back(event.clone());
        Ok(())
    }

    async fn query(&self, filter: &QueryFilter) -> anyhow::Result<Vec<TelemetryEvent>> {
        let ring = self.ring.read();
        let mut out = Vec::new();
        for ev in ring.iter() {
            if filter.matches(ev) {
                out.push(ev.clone());
                if let Some(limit) = filter.limit {
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    async fn cleanup(&self, older_than_ts: u64) -> anyhow::Result<usize> {
        let mut ring = self.ring.write();
        let before = ring.len();
        ring.retain(|e| e.ingress_ts >= older_than_ts);
        Ok(before - ring.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tests::{conformance, ev};
    use serde_json::json;
    use std::sync::Arc;

    #[tokio::test]
    async fn passes_conformance_suite() {
        conformance(&MemoryEngine::new(100)).await;
    }

    #[tokio::test]
    async fn full_ring_evicts_oldest_first() {
        let eng = MemoryEngine::new(3);
        for i in 1..=4 {
            eng.append(&ev("t", i)).await.unwrap();
        }
        let all = eng.query(&QueryFilter::default()).await.unwrap();
        assert_eq!(
            all.iter().map(|e| e.ingress_ts).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
    }

    #[tokio::test]
    async fn capacity_one_keeps_only_latest() {
        let eng = MemoryEngine::new(1);
        eng.append(&ev("t", 1)).await.unwrap();
        eng.append(&ev("t", 2)).await.unwrap();
        let all = eng.query(&QueryFilter::default()).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].ingress_ts, 2);
    }

    #[tokio::test]
    async fn payload_is_preserved() {
        let eng = MemoryEngine::new(10);
        let mut e = ev("t", 1);
        e.payload = json!({"deep": {"nested": [1, 2, 3]}});
        eng.append(&e).await.unwrap();
        let all = eng.query(&QueryFilter::default()).await.unwrap();
        assert_eq!(all[0].payload["deep"]["nested"][1], json!(2));
    }

    #[tokio::test]
    async fn dyn_dispatch_works() {
        let eng: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new(10));
        eng.append(&ev("t", 1)).await.unwrap();
        assert_eq!(eng.query(&QueryFilter::default()).await.unwrap().len(), 1);
    }
}
