//! Retention task: periodically evicts events older than a TTL via
//! `StorageEngine::cleanup`.

use crate::event::now_micros;
use crate::storage::StorageEngine;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

/// Spawn the retention loop with the default period (max(ttl/2, 1 s)).
pub fn spawn(engine: Arc<dyn StorageEngine>, ttl: Duration) -> JoinHandle<()> {
    let period = (ttl / 2).max(Duration::from_secs(1));
    spawn_at(engine, ttl, period)
}

/// Spawn the retention loop with an explicit tick `period` (testable).
pub fn spawn_at(engine: Arc<dyn StorageEngine>, ttl: Duration, period: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let ttl_micros = ttl.as_micros() as u64;
        let mut tick = tokio::time::interval(period);
        loop {
            tick.tick().await;
            let cutoff = now_micros().saturating_sub(ttl_micros);
            match engine.cleanup(cutoff).await {
                Ok(n) if n > 0 => debug!(removed = n, "retention cleanup"),
                Ok(_) => {}
                Err(e) => warn!(error = %e, "retention cleanup failed"),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::MemoryEngine;
    use crate::storage::tests::ev;
    use crate::storage::QueryFilter;

    #[tokio::test]
    async fn retention_evicts_old_events() {
        let eng: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new(100));
        // ingress_ts=1 is far older than any TTL.
        eng.append(&ev("t", 1)).await.unwrap();
        eng.append(&ev("t", now_micros())).await.unwrap();

        // TTL 5s: the fresh event stays; ingress_ts=1 (epoch) always ages out.
        let task = spawn_at(
            Arc::clone(&eng),
            Duration::from_secs(5),
            Duration::from_millis(10),
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        task.abort();

        let hits = eng.query(&QueryFilter::default()).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].ingress_ts > 1);
    }

    #[tokio::test]
    async fn retention_keeps_fresh_events() {
        let eng: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new(100));
        eng.append(&ev("t", now_micros())).await.unwrap();

        let task = spawn_at(
            Arc::clone(&eng),
            Duration::from_secs(60),
            Duration::from_millis(10),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        task.abort();

        assert_eq!(eng.query(&QueryFilter::default()).await.unwrap().len(), 1);
    }
}
