//! REST query endpoints: `GET /api/v1/time`, `GET /api/v1/events`.

use crate::api::AppState;
use crate::event::{now_micros, TelemetryEvent};
use crate::storage::QueryFilter;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

/// Server clock baseline for test scripts.
pub async fn get_time() -> Json<serde_json::Value> {
    Json(json!({ "server_ts": now_micros() }))
}

/// `GET /api/v1/events` query parameters.
#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    pub topic: Option<String>,
    pub class: Option<String>,
    pub id: Option<String>,
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub limit: Option<usize>,
}

/// Fetch stored events matching the compound filter.
pub async fn get_events(
    State(st): State<Arc<AppState>>,
    Query(q): Query<EventsQuery>,
) -> Result<Json<Vec<TelemetryEvent>>, StatusCode> {
    let filter = QueryFilter {
        topic: q.topic,
        class: q.class,
        id: q.id,
        since: q.since,
        until: q.until,
        limit: q.limit,
    };
    st.storage
        .query(&filter)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

#[cfg(test)]
mod tests {
    use crate::api::tests::json_body;
    use serde_json::Value;

    #[tokio::test]
    async fn time_returns_server_ts() {
        let v: Value = json_body("GET", "/api/v1/time", None).await;
        assert!(v["server_ts"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn events_filter_by_topic_and_range() {
        let v: Value = json_body(
            "GET",
            "/api/v1/events?topic=a&since=5&until=25&limit=10",
            None,
        )
        .await;
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["topic"], "a");
    }

    #[tokio::test]
    async fn events_empty_result_is_array() {
        let v: Value = json_body("GET", "/api/v1/events?topic=nope", None).await;
        assert_eq!(v.as_array().unwrap().len(), 0);
    }
}
