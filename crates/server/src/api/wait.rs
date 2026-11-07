//! Reactive wait endpoint: `POST /api/v1/wait` blocks until a matching
//! event arrives on the bus, or `timeout_ms` elapses (`408`).

use crate::api::AppState;
use crate::bus::EventBus;
use crate::event::TelemetryEvent;
use crate::storage::QueryFilter;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, oneshot};

/// Predicate attached to a wait request. Every set field must match.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WaitFilter {
    pub topic: Option<String>,
    pub class: Option<String>,
    pub id: Option<String>,
    /// Shallow object containment checked against `payload`.
    pub payload_match: Option<Map<String, Value>>,
}

impl WaitFilter {
    /// Whether `ev` satisfies every set field, including payload containment.
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
        if let Some(pm) = &self.payload_match {
            let Some(obj) = ev.payload.as_object() else {
                return false;
            };
            for (k, v) in pm {
                if obj.get(k) != Some(v) {
                    return false;
                }
            }
        }
        true
    }
}

/// Body of `POST /api/v1/wait`.
#[derive(Debug, Deserialize)]
pub struct WaitRequest {
    /// Only events with `ingress_ts >= since_ts` qualify.
    pub since_ts: u64,
    /// Maximum block time before `408`.
    pub timeout_ms: u64,
    #[serde(default)]
    pub filter: WaitFilter,
}

/// A registered predicate plus its resolution channel.
struct Waiter {
    since_ts: u64,
    filter: WaitFilter,
    tx: oneshot::Sender<TelemetryEvent>,
}

/// Central matcher: one bus consumer resolving all pending waiters.
#[derive(Clone)]
pub struct WaitDispatcher {
    waiters: Arc<Mutex<Vec<Waiter>>>,
}

impl WaitDispatcher {
    /// Subscribe to the bus and start resolving waiters.
    pub fn spawn(bus: &EventBus) -> Self {
        let waiters = Arc::new(Mutex::new(Vec::<Waiter>::new()));
        let shared = Arc::clone(&waiters);
        let mut rx = bus.subscribe();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(ev) => Self::dispatch_event(&shared, ev),
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, "wait dispatcher lagged")
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Self { waiters }
    }

    /// Prune dead waiters and resolve those matching `ev`.
    fn dispatch_event(waiters: &Arc<Mutex<Vec<Waiter>>>, ev: TelemetryEvent) {
        let mut guard = waiters.lock();
        let mut i = 0;
        while i < guard.len() {
            if guard[i].tx.is_closed() {
                guard.remove(i);
                continue;
            }
            if ev.ingress_ts >= guard[i].since_ts && guard[i].filter.matches(&ev) {
                let w = guard.remove(i);
                let _ = w.tx.send(ev.clone());
            } else {
                i += 1;
            }
        }
    }

    /// Register a predicate; resolve via the returned receiver.
    pub fn register(&self, since_ts: u64, filter: WaitFilter) -> oneshot::Receiver<TelemetryEvent> {
        let (tx, rx) = oneshot::channel();
        self.waiters.lock().push(Waiter {
            since_ts,
            filter,
            tx,
        });
        rx
    }
}

/// `POST /api/v1/wait` — resolve with the first matching event or `408`.
///
/// The waiter registers before the storage check, closing the race window
/// where an event could arrive unseen between the two lookups.
pub async fn post_wait(
    State(st): State<Arc<AppState>>,
    Json(req): Json<WaitRequest>,
) -> Result<Json<TelemetryEvent>, StatusCode> {
    let rx = st.dispatch.register(req.since_ts, req.filter.clone());

    let filter = QueryFilter {
        topic: req.filter.topic.clone(),
        class: req.filter.class.clone(),
        id: req.filter.id.clone(),
        since: Some(req.since_ts),
        until: None,
        limit: None,
    };
    if let Ok(hits) = st.storage.query(&filter).await {
        if let Some(ev) = hits.into_iter().find(|e| req.filter.matches(e)) {
            return Ok(Json(ev));
        }
    }

    match tokio::time::timeout(Duration::from_millis(req.timeout_ms), rx).await {
        Ok(Ok(ev)) => Ok(Json(ev)),
        Ok(Err(_)) => Err(StatusCode::INTERNAL_SERVER_ERROR),
        Err(_) => Err(StatusCode::REQUEST_TIMEOUT),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{request, test_app};
    use crate::storage::tests::ev;
    use serde_json::json;

    fn filter_with_payload(k: &str, v: Value) -> WaitFilter {
        let mut m = Map::new();
        m.insert(k.to_string(), v);
        WaitFilter {
            payload_match: Some(m),
            ..Default::default()
        }
    }

    #[test]
    fn filter_matches_all_set_fields() {
        let f = WaitFilter {
            topic: Some("t".into()),
            ..Default::default()
        };
        assert!(f.matches(&ev("t", 1)));
        assert!(!f.matches(&ev("other", 1)));
    }

    #[test]
    fn payload_match_requires_containment() {
        let f = filter_with_payload("k", json!("v"));
        let mut e = ev("t", 1);
        e.payload = json!({"k": "v", "extra": 1});
        assert!(f.matches(&e));
        e.payload = json!({"k": "other"});
        assert!(!f.matches(&e));
        e.payload = json!("not an object");
        assert!(!f.matches(&e));
    }

    #[tokio::test]
    async fn wait_resolves_on_matching_arrival() {
        let (bus, app) = test_app(vec![]).await;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            let _ = bus.publish(ev("a", 100));
        });
        let (status, v) = request(
            app,
            "POST",
            "/api/v1/wait",
            Some(json!({
                "since_ts": 0, "timeout_ms": 2000,
                "filter": {"topic": "a"}
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["topic"], "a");
        assert_eq!(v["ingress_ts"], 100);
    }

    #[tokio::test]
    async fn wait_returns_stored_event_immediately() {
        let (_bus, app) = test_app(vec![ev("a", 10)]).await;
        let (status, v) = request(
            app,
            "POST",
            "/api/v1/wait",
            Some(json!({
                "since_ts": 0, "timeout_ms": 1000,
                "filter": {"topic": "a"}
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["ingress_ts"], 10);
    }

    #[tokio::test]
    async fn wait_ignores_events_older_than_since_ts() {
        // Stored event has ingress_ts 5 < since_ts 50 → must not match.
        let (_bus, app) = test_app(vec![ev("a", 5)]).await;
        let (status, _v) = request(
            app,
            "POST",
            "/api/v1/wait",
            Some(json!({"since_ts": 50, "timeout_ms": 80, "filter": {}})),
        )
        .await;
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    }

    #[tokio::test]
    async fn wait_times_out_with_408() {
        let (_bus, app) = test_app(vec![]).await;
        let (status, _v) = request(
            app,
            "POST",
            "/api/v1/wait",
            Some(json!({"since_ts": 0, "timeout_ms": 50, "filter": {}})),
        )
        .await;
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    }

    #[tokio::test]
    async fn non_object_payload_match_is_client_error() {
        let (_bus, app) = test_app(vec![]).await;
        let (status, _v) = request(
            app,
            "POST",
            "/api/v1/wait",
            Some(json!({
                "since_ts": 0, "timeout_ms": 10,
                "filter": {"payload_match": "not-an-object"}
            })),
        )
        .await;
        assert!(status.is_client_error());
    }

    #[tokio::test]
    async fn non_matching_event_does_not_resolve() {
        let (bus, app) = test_app(vec![]).await;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = bus.publish(ev("unrelated", 100));
        });
        let (status, _v) = request(
            app,
            "POST",
            "/api/v1/wait",
            Some(json!({
                "since_ts": 0, "timeout_ms": 200,
                "filter": {"topic": "wanted"}
            })),
        )
        .await;
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    }
}
