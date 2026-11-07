//! Internal event bus: a `tokio::sync::broadcast` fan-out of decoded events.
//!
//! Subscribers (storage pipeline, reactive wait engine, WebSocket streams)
//! must tolerate `RecvError::Lagged` when they fall behind.

use crate::event::TelemetryEvent;
use tokio::sync::broadcast;

/// Default broadcast ring capacity.
pub const BUS_CAPACITY: usize = 4_096;

/// Shared handle over the event broadcast channel.
#[derive(Debug, Clone)]
pub struct EventBus {
    sender: broadcast::Sender<TelemetryEvent>,
}

impl EventBus {
    /// Create a bus with [`BUS_CAPACITY`].
    pub fn new() -> Self {
        Self::with_capacity(BUS_CAPACITY)
    }

    /// Create a bus with an explicit ring capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Subscribe a new consumer to the event stream.
    pub fn subscribe(&self) -> broadcast::Receiver<TelemetryEvent> {
        self.sender.subscribe()
    }

    /// Publish an event to all current subscribers.
    ///
    /// Returns `Err` only when no subscribers exist; the event is still
    /// queued for future subscribers and the error is recoverable.
    pub fn publish(&self, event: TelemetryEvent) -> Result<usize, TelemetryEvent> {
        self.sender.send(event).map_err(|e| e.0)
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev() -> TelemetryEvent {
        TelemetryEvent {
            topic: "t".into(),
            id: "i".into(),
            class: "c".into(),
            ts: 1,
            payload: serde_json::Value::Null,
            ingress_ts: 2,
        }
    }

    #[tokio::test]
    async fn subscriber_receives_published_event() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe();
        bus.publish(ev()).unwrap();
        let got = rx.recv().await.unwrap();
        assert_eq!(got.ingress_ts, 2);
    }

    #[tokio::test]
    async fn multiple_subscribers_each_receive() {
        let bus = EventBus::new();
        let mut a = bus.subscribe();
        let mut b = bus.subscribe();
        bus.publish(ev()).unwrap();
        assert_eq!(a.recv().await.unwrap().topic, "t");
        assert_eq!(b.recv().await.unwrap().topic, "t");
    }

    #[tokio::test]
    async fn lagged_subscriber_gets_lagged_error_then_new_events() {
        let bus = EventBus::with_capacity(2);
        let mut rx = bus.subscribe();
        bus.publish(ev()).unwrap();
        bus.publish(ev()).unwrap();
        bus.publish(ev()).unwrap();
        assert!(matches!(
            rx.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
        assert!(rx.recv().await.is_ok());
    }
}
