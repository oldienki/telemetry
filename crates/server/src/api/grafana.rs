//! Grafana adapter: SimpleJSON-style `/grafana` endpoints over `StorageEngine`.
//!
//! - `GET /grafana[/]` — datasource health check, 200.
//! - `POST /grafana/search` — static target list `["events", "events.count"]`.
//! - `POST /grafana/query` — per-target frames: `events`/`type=table` → table
//!   of stored events in range; `events.count`/`type=timeseries` → one
//!   `[{target: topic, datapoints: [[count, to_ms]]}]` per topic.
//!
//! `range.from`/`range.to` accept RFC 3339, `"now"`, or epoch s/ms/µs.
//! Timestamps in frames are milliseconds (Grafana convention).

use crate::api::AppState;
use crate::event::{now_micros, TelemetryEvent};
use crate::storage::QueryFilter;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use time::format_description::well_known::Iso8601;
use time::OffsetDateTime;

/// Health check for the datasource probe.
pub async fn get_root() -> StatusCode {
    StatusCode::OK
}

/// Static search targets understood by `/grafana/query`.
pub async fn post_search() -> Json<Value> {
    Json(json!(["events", "events.count"]))
}

/// Body of `POST /grafana/query` (SimpleJSON shape; unknown fields ignored).
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    #[serde(default)]
    range: Option<Range>,
    #[serde(default)]
    targets: Vec<Target>,
}

/// Time range bounds (`from`/`to`: RFC 3339, `"now"`, or epoch s/ms/µs).
#[derive(Debug, Deserialize)]
struct Range {
    #[serde(default)]
    from: Option<Value>,
    #[serde(default)]
    to: Option<Value>,
}

/// One query target (`target` name + SimpleJSON `type` hint).
#[derive(Debug, Deserialize)]
struct Target {
    #[serde(default)]
    target: Option<String>,
    #[serde(default)]
    r#type: Option<String>,
}

/// Parse a SimpleJSON range bound into µs since epoch.
/// Accepts RFC 3339 strings, `"now"`, or epoch numbers (s/ms/µs auto-detected).
fn parse_bound(v: Option<&Value>) -> Option<u64> {
    match v? {
        Value::Number(n) => {
            let x = n.as_u64()?;
            Some(if x >= 1_000_000_000_000_000 {
                x
            } else if x >= 1_000_000_000_000 {
                x * 1_000
            } else {
                x * 1_000_000
            })
        }
        Value::String(s) => {
            if s == "now" {
                Some(now_micros())
            } else {
                OffsetDateTime::parse(s, &Iso8601::DEFAULT)
                    .ok()
                    .map(|d| d.unix_timestamp_nanos().unsigned_abs() as u64 / 1_000)
            }
        }
        _ => None,
    }
}

/// Table frame for a slice of events (timestamps in ms).
fn table_frame(events: &[TelemetryEvent]) -> Value {
    let rows: Vec<Value> = events
        .iter()
        .map(|e| {
            json!([
                e.ingress_ts as f64 / 1_000.0,
                e.ts as f64 / 1_000.0,
                e.topic,
                e.id,
                e.class,
                e.payload,
            ])
        })
        .collect();
    json!({
        "type": "table",
        "columns": [
            {"text": "ingress_ts"}, {"text": "ts"}, {"text": "topic"},
            {"text": "id"}, {"text": "class"}, {"text": "payload"},
        ],
        "rows": rows,
    })
}

/// One timeseries frame per topic: [[count, to_ms]].
fn count_frames(events: &[TelemetryEvent], to_us: u64) -> Vec<Value> {
    let mut counts: BTreeMap<&str, u64> = BTreeMap::new();
    for e in events {
        *counts.entry(e.topic.as_str()).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(topic, n)| {
            json!({
                "target": topic,
                "datapoints": [[n, to_us / 1_000]],
            })
        })
        .collect()
}

/// `POST /grafana/query` — map targets onto `StorageEngine::query`.
pub async fn post_query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<Value>, StatusCode> {
    let (since, until) = match &req.range {
        Some(r) => (
            parse_bound(r.from.as_ref()),
            parse_bound(r.to.as_ref()).or(Some(now_micros())),
        ),
        None => (None, Some(now_micros())),
    };
    let filter = QueryFilter {
        since,
        until,
        limit: Some(10_000),
        ..Default::default()
    };
    let events = state
        .storage
        .query(&filter)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let to_us = until.unwrap_or(0);
    let results: Vec<Value> = req
        .targets
        .iter()
        .map(|t| {
            let name = t.target.as_deref().unwrap_or_default();
            let kind = t.r#type.as_deref().unwrap_or_default();
            if name == "events" || kind == "table" {
                table_frame(&events)
            } else if name == "events.count" || kind == "timeseries" {
                json!(count_frames(&events, to_us))
            } else {
                // Unknown target → graceful empty frame.
                json!({"target": name, "datapoints": []})
            }
        })
        .collect();
    Ok(Json(json!(results)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{json_body, request, test_app};
    use serde_json::json;
    use tower::ServiceExt;

    #[tokio::test]
    async fn root_is_ok() {
        let (_bus, app) = test_app(vec![]).await;
        let resp = app
            .oneshot(
                axum::http::Request::get("/grafana")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn search_returns_targets() {
        let body = json_body("POST", "/grafana/search", Some(json!({}))).await;
        assert_eq!(body, json!(["events", "events.count"]));
    }

    #[tokio::test]
    async fn query_table_returns_events_in_range() {
        let (_bus, app) = test_app(crate::api::tests::seed().await).await;
        // Seed events have ingress_ts 10/20/30 µs — narrow to 15..35 µs via
        // RFC 3339 with fractional seconds (the real Grafana range format).
        let req = json!({
            "range": {
                "from": "1970-01-01T00:00:00.000015Z",
                "to": "1970-01-01T00:00:00.000035Z",
            },
            "targets": [{"target": "events"}],
        });
        let (status, body) = request(app, "POST", "/grafana/query", Some(req)).await;
        assert_eq!(status, StatusCode::OK);
        let frame = &body[0];
        assert_eq!(frame["type"], "table");
        let rows = frame["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2); // ingress 20, 30
        assert_eq!(rows[0][2], "b");
        assert_eq!(rows[1][2], "a");
    }

    #[tokio::test]
    async fn query_timeseries_counts_per_topic() {
        let (_bus, app) = test_app(crate::api::tests::seed().await).await;
        let req = json!({
            "range": {"from": "now-1h", "to": "now"},
            "targets": [{"target": "events.count"}],
        });
        let (_status, body) = request(app, "POST", "/grafana/query", Some(req)).await;
        let frames = body[0].as_array().unwrap();
        // a→2, b→1 sorted by topic name.
        assert_eq!(frames[0]["target"], "a");
        assert_eq!(frames[0]["datapoints"][0][0], 2);
        assert_eq!(frames[1]["target"], "b");
        assert_eq!(frames[1]["datapoints"][0][0], 1);
    }

    #[tokio::test]
    async fn unknown_target_is_empty_frame() {
        let req = json!({
            "range": {"from": 0, "to": 100},
            "targets": [{"target": "bogus"}],
        });
        let body = json_body("POST", "/grafana/query", Some(req)).await;
        let frame = &body[0];
        assert_eq!(frame["target"], "bogus");
        assert_eq!(frame["datapoints"], json!([]));
    }

    #[tokio::test]
    async fn missing_range_defaults_to_everything() {
        let req = json!({"targets": [{"target": "events"}]});
        let body = json_body("POST", "/grafana/query", Some(req)).await;
        assert_eq!(body[0]["rows"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn empty_storage_returns_empty_rows() {
        let req = json!({
            "range": {"from": 0, "to": "now"},
            "targets": [{"target": "events"}],
        });
        let (_bus, app) = test_app(vec![]).await;
        let (_status, body) = request(app, "POST", "/grafana/query", Some(req)).await;
        assert_eq!(body[0]["rows"], json!([]));
    }
}
