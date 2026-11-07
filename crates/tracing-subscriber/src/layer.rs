//! `TelemetryLayer`: maps `tracing::Event`s into LightTelemetry packets.

use light_telemetry_client::TelemetryClient;
use serde_json::{Map, Value};
use std::sync::Arc;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// `tracing_subscriber::Layer` that emits every event as a telemetry packet.
///
/// `topic` defaults to `logger`; `class`/`id` come from the client's
/// config. Event fields land in `payload` alongside `level`, `target`,
/// and `message`.
pub struct TelemetryLayer {
    client: Arc<TelemetryClient>,
    topic: String,
}

impl TelemetryLayer {
    /// Wrap an existing client.
    pub fn new(client: Arc<TelemetryClient>) -> Self {
        Self {
            client,
            topic: "logger".to_string(),
        }
    }

    /// Override the topic used for emitted events.
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = topic.into();
        self
    }
}

/// Field visitor that accumulates key/value pairs into a JSON object.
#[derive(Default)]
struct JsonVisitor {
    map: Map<String, Value>,
}

impl JsonVisitor {
    fn insert(&mut self, field: &Field, v: Value) {
        self.map.insert(field.name().to_string(), v);
    }
}

impl Visit for JsonVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.insert(field, Value::String(value.to_string()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.insert(field, Value::Bool(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.insert(field, value.into());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.insert(field, value.into());
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.insert(field, value.into());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // Errors and other debug-only values render as strings.
        self.insert(field, Value::String(format!("{value:?}")));
    }
}

impl<S> Layer<S> for TelemetryLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = JsonVisitor::default();
        event.record(&mut visitor);
        let meta = event.metadata();
        visitor
            .map
            .insert("level".into(), meta.level().to_string().into());
        visitor.map.insert("target".into(), meta.target().into());
        self.client.emit(&self.topic, Value::Object(visitor.map));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use light_telemetry_client::TelemetryConfig;
    use std::net::UdpSocket;
    use std::time::Duration;
    use tracing_subscriber::prelude::*;

    fn client_for(server: &UdpSocket) -> Arc<TelemetryClient> {
        let cfg = TelemetryConfig::new(server.local_addr().unwrap(), "host-1", "svc")
            .with_heartbeat_timeout(None);
        Arc::new(TelemetryClient::new(cfg).unwrap())
    }

    fn recv_packet(server: &UdpSocket) -> serde_json::Value {
        let mut buf = [0u8; 4096];
        server
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (n, _) = server.recv_from(&mut buf).unwrap();
        serde_json::from_slice(&buf[..n]).unwrap()
    }

    #[test]
    fn layer_emits_event_with_fields_and_metadata() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        let layer = TelemetryLayer::new(client_for(&server)).with_topic("logger");
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(answer = 42, user = "alice", "login succeeded");
        });

        let v = recv_packet(&server);
        assert_eq!(v["topic"], "logger");
        assert_eq!(v["id"], "host-1");
        assert_eq!(v["class"], "svc");
        assert_eq!(v["payload"]["level"], "INFO");
        assert_eq!(v["payload"]["answer"], 42);
        assert_eq!(v["payload"]["user"], "alice");
        assert_eq!(v["payload"]["message"], "login succeeded");
    }

    #[test]
    fn layer_honors_custom_topic() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        let layer = TelemetryLayer::new(client_for(&server)).with_topic("audit");
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!("disk nearly full");
        });

        let v = recv_packet(&server);
        assert_eq!(v["topic"], "audit");
        assert_eq!(v["payload"]["level"], "WARN");
    }

    #[test]
    fn layer_survives_unreachable_server() {
        // Discard port; emission must not panic or block the caller.
        let cfg = TelemetryConfig::new("127.0.0.1:9".parse().unwrap(), "h", "c")
            .with_heartbeat_timeout(None);
        let client = Arc::new(TelemetryClient::new(cfg).unwrap());
        let layer = TelemetryLayer::new(client);
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!("nothing hears this");
        });
    }
}
