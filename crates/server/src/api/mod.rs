//! HTTP API surface: REST query endpoints, the reactive wait endpoint,
//! the WebSocket stream, and the Grafana adapter.

pub mod grafana;
pub mod rest;
pub mod wait;
pub mod ws;

use crate::bus::EventBus;
use crate::storage::StorageEngine;
use axum::routing::{get, post};
use axum::Router;
use std::sync::Arc;
use wait::WaitDispatcher;

/// Shared state handed to every handler.
pub struct AppState {
    /// Active persistence engine.
    pub storage: Arc<dyn StorageEngine>,
    /// Registry of pending wait predicates.
    pub dispatch: WaitDispatcher,
    /// Event bus (WS stream subscribes, WS ingest publishes).
    pub bus: EventBus,
}

/// Build the HTTP router.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/v1/time", get(rest::get_time))
        .route("/api/v1/events", get(rest::get_events))
        .route("/api/v1/wait", post(wait::post_wait))
        .route("/api/v1/stream", get(ws::get_stream))
        .route("/grafana", get(grafana::get_root))
        .route("/grafana/", get(grafana::get_root))
        .route("/grafana/search", post(grafana::post_search))
        .route("/grafana/query", post(grafana::post_query))
        .with_state(state)
}

#[cfg(test)]
pub(crate) mod tests {
    //! Shared harness: seeded engine + live bus pipeline + dispatcher + router.
    use super::*;
    use crate::bus::EventBus;
    use crate::event::TelemetryEvent;
    use crate::storage::memory::MemoryEngine;
    use crate::storage::{spawn_pipeline, StorageEngine};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::Value;
    use tower::ServiceExt;

    /// Standard seed events used by endpoint tests.
    pub async fn seed() -> Vec<TelemetryEvent> {
        vec![
            crate::storage::tests::ev("a", 10),
            crate::storage::tests::ev("b", 20),
            crate::storage::tests::ev("a", 30),
        ]
    }

    /// Build a fully wired app. `seeded` events are appended directly to
    /// storage (bypassing the bus) to simulate pre-existing history.
    /// Returns the bus (for publishing live events) and the router.
    pub async fn test_app(seeded: Vec<TelemetryEvent>) -> (EventBus, Router) {
        let bus = EventBus::new();
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new(100));
        for e in seeded {
            engine.append(&e).await.unwrap();
        }
        spawn_pipeline(&bus, Arc::clone(&engine));
        let dispatch = wait::WaitDispatcher::spawn(&bus);
        let state = Arc::new(AppState {
            storage: engine,
            dispatch,
            bus: bus.clone(),
        });
        (bus, router(state))
    }

    /// Perform one request; return (status, parsed JSON body or Null).
    pub async fn request(
        app: Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder().method(method).uri(uri);
        let body = match body {
            Some(v) => {
                builder = builder.header("content-type", "application/json");
                Body::from(v.to_string())
            }
            None => Body::empty(),
        };
        let resp = app.oneshot(builder.body(body).unwrap()).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, v)
    }

    /// Seed the standard events and return only the parsed body.
    pub async fn json_body(method: &str, uri: &str, body: Option<Value>) -> Value {
        let (_bus, app) = test_app(seed().await).await;
        request(app, method, uri, body).await.1
    }
}
