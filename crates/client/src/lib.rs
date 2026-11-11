//! UDP fire-and-forget telemetry client for LightTelemetry.
//!
//! # Design
//! - `emit` serializes a [`TelemetryPacket`] and sends one datagram. No
//!   buffering, no retries — telemetry must never block the host.
//! - A background thread watches reverse heartbeats from the server; when
//!   they stop, `degraded` flips and `emit` drops before serializing.
//! - Oversized packets are truncated (`payload → {"truncated": true}`).

pub mod config;
mod heartbeat;
mod sender;
pub mod serialize;

use serialize::{encode, now_micros, TelemetryPacket};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Recommended MTU-safe payload size, re-exported for callers.
pub use serialize::MTU_SAFE;

/// Client configuration.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// Server ingest address (`host:port`).
    pub server_addr: SocketAddr,
    /// Unique source identifier (`id` field).
    pub host_id: String,
    /// Source classifier (`class` field).
    pub host_class: String,
    /// `true` → MessagePack wire format; `false` → JSON.
    pub use_msgpack: bool,
    /// Liveness window for the heartbeat watcher. `None` disables the
    /// watcher entirely (degraded never flips). Default: 3 s.
    pub heartbeat_timeout: Option<Duration>,
}

impl TelemetryConfig {
    /// Minimal config: JSON format, 3 s heartbeat timeout.
    pub fn new(
        server_addr: SocketAddr,
        host_id: impl Into<String>,
        host_class: impl Into<String>,
    ) -> Self {
        Self {
            server_addr,
            host_id: host_id.into(),
            host_class: host_class.into(),
            use_msgpack: false,
            heartbeat_timeout: Some(Duration::from_secs(3)),
        }
    }

    /// Select MessagePack wire format.
    pub fn with_msgpack(mut self, enabled: bool) -> Self {
        self.use_msgpack = enabled;
        self
    }

    /// Localhost default: adopt the server's `telemetry.toml`.
    ///
    /// Reads `/etc/telemetry.toml` then `$HOME/.config/telemetry.toml`
    /// (first hit wins, same as the daemon). `udp_bind` becomes the
    /// target with wildcards mapped to loopback; `heartbeat_ms × 3`
    /// becomes the liveness window. Missing/unreadable files or keys
    /// fall back to `127.0.0.1:9876` / 3 s — `local` never fails.
    pub fn local(host_id: impl Into<String>, host_class: impl Into<String>) -> Self {
        let (srv, _) = config::load(None).unwrap_or_default();
        Self::from_server(srv, host_id, host_class)
    }

    /// Like [`Self::local`] but reads an explicit config file that must
    /// exist — errors propagate.
    pub fn local_from(
        path: &std::path::Path,
        host_id: impl Into<String>,
        host_class: impl Into<String>,
    ) -> std::io::Result<Self> {
        let (srv, _) = config::load(Some(path))?;
        Ok(Self::from_server(srv, host_id, host_class))
    }

    /// Shared merge for `local`/`local_from`.
    fn from_server(
        srv: config::ServerConfig,
        host_id: impl Into<String>,
        host_class: impl Into<String>,
    ) -> Self {
        let addr = srv
            .udp_bind
            .map(config::localize)
            .unwrap_or_else(|| "127.0.0.1:9876".parse().unwrap());
        let hb_ms = srv.heartbeat_ms.unwrap_or(1000);
        Self {
            server_addr: addr,
            host_id: host_id.into(),
            host_class: host_class.into(),
            use_msgpack: false,
            heartbeat_timeout: Some(Duration::from_millis(hb_ms * 3)),
        }
    }

    /// Override the heartbeat liveness window; `None` disables watching.
    pub fn with_heartbeat_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.heartbeat_timeout = timeout;
        self
    }
}

/// Client-side counters (useful for tests and diagnostics).
#[derive(Debug, Default)]
pub struct ClientStats {
    /// Packets that passed the degraded gate and entered serialization.
    pub encoded: AtomicU64,
    /// Datagrams successfully handed to the socket.
    pub sent: AtomicU64,
    /// Emissions dropped while degraded.
    pub dropped: AtomicU64,
    /// Packets whose payload was replaced by the truncation marker.
    pub truncated: AtomicU64,
    /// Send or encode failures (server unreachable is *not* an error).
    pub errors: AtomicU64,
}

/// Telemetry client: one UDP socket + optional heartbeat watcher.
pub struct TelemetryClient {
    sender: sender::UdpSender,
    config: TelemetryConfig,
    degraded: Arc<AtomicBool>,
    stats: ClientStats,
    stop: Arc<AtomicBool>,
    watcher: Option<std::thread::JoinHandle<()>>,
}

impl TelemetryClient {
    /// Bind the socket and start the heartbeat watcher (if configured).
    pub fn new(config: TelemetryConfig) -> std::io::Result<Self> {
        let sender = sender::UdpSender::bind(config.server_addr)?;
        let degraded = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let watcher = config.heartbeat_timeout.map(|timeout| {
            heartbeat::spawn_watcher(
                sender.socket(),
                config.server_addr,
                timeout,
                Arc::clone(&degraded),
                Arc::clone(&stop),
                heartbeat::ProbeSpec {
                    host_id: config.host_id.clone(),
                    host_class: config.host_class.clone(),
                    use_msgpack: config.use_msgpack,
                },
            )
        });
        Ok(Self {
            sender,
            config,
            degraded,
            stats: ClientStats::default(),
            stop,
            watcher,
        })
    }

    /// Whether the server is currently considered unreachable.
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Relaxed)
    }

    /// Emit one event. Drops silently when degraded; never retries.
    pub fn emit(&self, topic: &str, payload: serde_json::Value) {
        // Drop-before-serialize fast path.
        if self.is_degraded() {
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let packet = TelemetryPacket {
            topic: topic.to_string(),
            id: self.config.host_id.clone(),
            class: self.config.host_class.clone(),
            ts: now_micros(),
            payload,
        };
        self.stats.encoded.fetch_add(1, Ordering::Relaxed);
        match encode(&packet, self.config.use_msgpack) {
            Ok((bytes, truncated)) => {
                if truncated {
                    self.stats.truncated.fetch_add(1, Ordering::Relaxed);
                }
                match self.sender.send(&bytes) {
                    Ok(()) => {
                        self.stats.sent.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        self.stats.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Err(_) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Local socket address (tests use it to register with a fake server).
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.sender.local_addr()
    }

    /// Snapshot of the internal counters.
    pub fn stats(&self) -> &ClientStats {
        &self.stats
    }
}

impl Drop for TelemetryClient {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.watcher.take() {
            let _ = h.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::net::UdpSocket;

    fn test_config(addr: SocketAddr) -> TelemetryConfig {
        TelemetryConfig::new(addr, "test-host", "test-class").with_heartbeat_timeout(None)
    }

    #[test]
    fn emit_sends_json_datagram() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        let server_addr = server.local_addr().unwrap();
        let client = TelemetryClient::new(test_config(server_addr)).unwrap();
        client.emit("t", json!({"k": 1}));

        let mut buf = [0u8; 4096];
        server
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (n, _from) = server.recv_from(&mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf[..n]).unwrap();
        assert_eq!(v["topic"], "t");
        assert_eq!(v["id"], "test-host");
        assert_eq!(v["class"], "test-class");
        assert!(v["ts"].as_u64().unwrap() > 0);
        assert_eq!(v["payload"]["k"], 1);
        assert_eq!(client.stats().sent.load(Ordering::Relaxed), 1);
        assert_eq!(client.stats().encoded.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn emit_sends_msgpack_when_configured() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        let server_addr = server.local_addr().unwrap();
        let client = TelemetryClient::new(test_config(server_addr).with_msgpack(true)).unwrap();
        client.emit("t", json!({"k": 1}));

        let mut buf = [0u8; 4096];
        server
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (n, _from) = server.recv_from(&mut buf).unwrap();
        // First byte is a msgpack map marker, not '{'.
        assert_ne!(buf[0], b'{');
        let v: serde_json::Value = rmp_serde::from_slice(&buf[..n]).unwrap();
        assert_eq!(v["topic"], "t");
    }

    #[test]
    fn emit_to_unreachable_server_does_not_panic() {
        // Port is bound, send still succeeds at the UDP level; the point is
        // emit() never panics or propagates.
        let addr: SocketAddr = "127.0.0.1:9".parse().unwrap(); // discard port
        let client = TelemetryClient::new(test_config(addr)).unwrap();
        client.emit("t", json!({}));
        assert_eq!(client.stats().sent.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn degraded_drops_before_serialization() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        let server_addr = server.local_addr().unwrap();
        let client = TelemetryClient::new(test_config(server_addr)).unwrap();
        client.degraded.store(true, Ordering::Relaxed);

        let encoded_before = client.stats().encoded.load(Ordering::Relaxed);
        client.emit("t", json!({"expensive": "serialization"}));
        // Serializer must NOT have run — encoded count unchanged.
        assert_eq!(
            client.stats().encoded.load(Ordering::Relaxed),
            encoded_before
        );
        assert_eq!(client.stats().dropped.load(Ordering::Relaxed), 1);
        assert_eq!(client.stats().sent.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn no_watcher_means_never_degraded() {
        // heartbeat_timeout: None — client stays healthy even with no server.
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        let client = TelemetryClient::new(test_config(server.local_addr().unwrap())).unwrap();
        std::thread::sleep(Duration::from_millis(300));
        assert!(!client.is_degraded());
    }

    #[test]
    fn local_from_uses_server_config() {
        let p = std::env::temp_dir().join(format!("lt-srv-{}.toml", std::process::id()));
        std::fs::write(&p, "udp_bind = \"0.0.0.0:45555\"\nheartbeat_ms = 100\n").unwrap();
        let cfg = TelemetryConfig::local_from(&p, "h", "c").unwrap();
        assert_eq!(cfg.server_addr, "127.0.0.1:45555".parse().unwrap());
        assert_eq!(cfg.heartbeat_timeout, Some(Duration::from_millis(300)));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn local_falls_back_to_defaults() {
        // Point HOME at an empty dir so no user config is found; /etc is
        // not writable here either — expect the built-in defaults.
        let dir = std::env::temp_dir().join(format!("lt-home-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let prev = std::env::var("HOME").ok();
        std::env::set_var("HOME", &dir);
        let cfg = TelemetryConfig::local("h", "c");
        if let Some(h) = prev {
            std::env::set_var("HOME", h);
        }
        if std::path::Path::new("/etc/telemetry.toml").exists() {
            return; // cannot assert defaults while a real config exists
        }
        assert_eq!(cfg.server_addr, "127.0.0.1:9876".parse().unwrap());
        assert_eq!(cfg.heartbeat_timeout, Some(Duration::from_secs(3)));
    }

    #[test]
    fn local_from_missing_file_errors() {
        assert!(
            TelemetryConfig::local_from(std::path::Path::new("/nonexistent/t.toml"), "h", "c")
                .is_err()
        );
    }
}
