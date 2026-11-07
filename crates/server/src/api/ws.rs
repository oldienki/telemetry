//! `GET /api/v1/stream`: WebSocket live event stream.
//!
//! Outbound: broadcast events as JSON text frames; a lagged broadcast
//! receiver emits `{"type":"lagged","skipped":n}`. Inbound: text and binary
//! frames are decoded exactly like UDP datagrams and published onto the
//! bus — a TCP ingestion fallback for environments where UDP is blocked.
//! Undecodable frames get `{"type":"error","error":"…"}` back.

use crate::api::AppState;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::Response;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;
use tracing::debug;

/// `?topic=` narrows the outbound stream to one topic.
#[derive(Debug, Deserialize)]
pub struct StreamQuery {
    topic: Option<String>,
}

/// Upgrade `GET /api/v1/stream` to the live event socket.
pub async fn get_stream(
    State(state): State<Arc<AppState>>,
    Query(q): Query<StreamQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state, q.topic))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>, topic: Option<String>) {
    let (mut sink, mut stream) = socket.split();
    let mut rx = state.bus.subscribe();
    loop {
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Text(t))) => {
                        if !handle_inbound(&state, &mut sink, t.as_bytes()).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Binary(b))) => {
                        if !handle_inbound(&state, &mut sink, &b).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(_)) => {} // ping/pong handled internally
                }
            }
            evt = rx.recv() => {
                match evt {
                    Ok(ev) => {
                        if topic.as_deref().is_none_or(|t| ev.topic == t) {
                            let text = serde_json::to_string(&ev).unwrap_or_default();
                            if sink.send(Message::Text(text.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(RecvError::Lagged(n)) => {
                        let notice = json!({"type": "lagged", "skipped": n}).to_string();
                        if sink.send(Message::Text(notice.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        }
    }
}

/// Decode an inbound frame and publish it; false = socket dead.
async fn handle_inbound(
    state: &AppState,
    sink: &mut SplitSink<WebSocket, Message>,
    bytes: &[u8],
) -> bool {
    match crate::decode::decode(bytes, crate::event::now_micros()) {
        Ok(ev) => {
            debug!(topic = %ev.topic, "ws ingest");
            // Storage + waiters pick it up through the bus pipeline.
            let _ = state.bus.publish(ev);
            true
        }
        Err(e) => {
            let frame = json!({"type": "error", "error": e.to_string()}).to_string();
            sink.send(Message::Text(frame.into())).await.is_ok()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api::tests::{request, test_app};
    use axum::Router;
    use futures_util::{SinkExt, StreamExt};
    use std::net::SocketAddr;
    use tokio_tungstenite::tungstenite::Message as TMsg;

    /// Serve `app` on an ephemeral port and connect a WS client.
    async fn ws_connect(
        app: Router,
        path: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let (ws, _resp) = tokio_tungstenite::connect_async(format!("ws://{addr}{path}"))
            .await
            .expect("ws connect failed");
        ws
    }

    async fn recv_json(
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> serde_json::Value {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), ws.next())
            .await
            .expect("recv timed out")
            .expect("stream closed")
            .expect("frame error");
        let TMsg::Text(t) = msg else {
            panic!("expected text frame, got {msg:?}")
        };
        serde_json::from_str(&t).unwrap()
    }

    #[tokio::test]
    async fn ws_receives_published_events() {
        let (bus, app) = test_app(vec![]).await;
        let mut ws = ws_connect(app, "/api/v1/stream").await;
        bus.publish(crate::storage::tests::ev("news", 1)).unwrap();
        let v = recv_json(&mut ws).await;
        assert_eq!(v["topic"], "news");
    }

    #[tokio::test]
    async fn ws_topic_filter_blocks_others() {
        let (bus, app) = test_app(vec![]).await;
        let mut ws = ws_connect(app, "/api/v1/stream?topic=a").await;
        bus.publish(crate::storage::tests::ev("b", 1)).unwrap();
        bus.publish(crate::storage::tests::ev("a", 2)).unwrap();
        let v = recv_json(&mut ws).await;
        assert_eq!(v["topic"], "a"); // "b" was filtered out, "a" is first match
    }

    #[tokio::test]
    async fn ws_inbound_json_packet_is_ingested() {
        let (_bus, app) = test_app(vec![]).await;
        let mut ws = ws_connect(app.clone(), "/api/v1/stream").await;
        ws.send(TMsg::Text(
            r#"{"topic":"via-ws","id":"x","class":"c","ts":5,"payload":{}}"#.into(),
        ))
        .await
        .unwrap();
        // The event echoes back through the broadcast subscription.
        let v = recv_json(&mut ws).await;
        assert_eq!(v["topic"], "via-ws");
        assert!(v["ingress_ts"].as_u64().unwrap() > 0);
        // And it landed in storage (TCP ingestion fallback).
        let (_s, events) = request(app, "GET", "/api/v1/events?topic=via-ws", None).await;
        assert_eq!(events.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn ws_inbound_msgpack_packet_is_ingested() {
        let (_bus, app) = test_app(vec![]).await;
        let mut ws = ws_connect(app, "/api/v1/stream?topic=mp").await;
        let packet = rmp_serde::to_vec_named(&serde_json::json!({
            "topic": "mp", "id": "x", "class": "c", "ts": 7, "payload": {"n": 1}
        }))
        .unwrap();
        ws.send(TMsg::Binary(packet.into())).await.unwrap();
        let v = recv_json(&mut ws).await;
        assert_eq!(v["topic"], "mp");
        assert_eq!(v["payload"]["n"], 1);
    }

    #[tokio::test]
    async fn ws_malformed_inbound_gets_error_frame() {
        let (_bus, app) = test_app(vec![]).await;
        let mut ws = ws_connect(app, "/api/v1/stream").await;
        ws.send(TMsg::Text("not json at all".into())).await.unwrap();
        let v = recv_json(&mut ws).await;
        assert_eq!(v["type"], "error");
    }
}
