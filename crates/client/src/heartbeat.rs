//! Heartbeat watcher: listens for server `{"type":"hb","srv_ts":…}` datagrams
//! and flips the shared `degraded` flag when they stop arriving.

use crate::serialize::{encode, now_micros, TelemetryPacket};
use serde::Deserialize;
use serde_json::Value;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Reserved wire topic for keepalive probes; the server refreshes the sender
/// session but never publishes it to the event bus.
const SESSION_TOPIC: &str = "_session";

/// Identity and wire format needed to serialize `_session` keepalive probes.
pub(crate) struct ProbeSpec {
    pub host_id: String,
    pub host_class: String,
    pub use_msgpack: bool,
}

#[derive(Deserialize)]
struct Heartbeat {
    #[serde(rename = "type")]
    kind: String,
    #[allow(dead_code)]
    srv_ts: u64,
}

/// Spawn the watcher thread on `socket`. `server_addr` filters heartbeats to
/// datagrams from the ingest server only. `timeout` is the liveness window:
/// a gap longer than `timeout` marks the client degraded.
///
/// Watches are best-effort: the thread exits silently on socket errors.
pub(crate) fn spawn_watcher(
    socket: Arc<UdpSocket>,
    server_addr: SocketAddr,
    timeout: Duration,
    degraded: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    probe: ProbeSpec,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        // Read timeout keeps the loop responsive to `stop` and bounds how
        // long a stale check can be delayed by a blocked recv. It must stay
        // well below the liveness window or a single blocked recv would
        // itself exceed `timeout` and flap `degraded` right after a fresh
        // heartbeat.
        let read_timeout =
            (timeout / 4).clamp(Duration::from_millis(20), Duration::from_millis(200));
        if socket.set_read_timeout(Some(read_timeout)).is_err() {
            return;
        }
        let mut last_hb: Option<Instant> = None;
        let mut last_probe: Option<Instant> = None;
        let mut buf = [0u8; 2048];
        while !stop.load(Ordering::Relaxed) {
            match socket.recv_from(&mut buf) {
                Ok((n, from)) => {
                    if from == server_addr {
                        if let Ok(hb) = serde_json::from_slice::<Heartbeat>(&buf[..n]) {
                            if hb.kind == "hb" {
                                last_hb = Some(Instant::now());
                            }
                        }
                    }
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut => {}
                Err(_) => break,
            }
            // Degrade only after the watcher had a chance to see at least one
            // heartbeat OR the first full timeout window elapsed.
            let stale = match last_hb {
                Some(t) => t.elapsed() > timeout,
                None => {
                    // No heartbeat yet: stay healthy through the first window.
                    false
                }
            };
            degraded.store(stale, Ordering::Relaxed);
            // While degraded, send one `_session` keepalive probe per
            // `timeout` window. The probe refreshes the server session so
            // heartbeats — and normal emission — can resume on their own.
            if stale {
                if last_probe.is_none_or(|t| t.elapsed() >= timeout) {
                    let packet = TelemetryPacket {
                        topic: SESSION_TOPIC.to_string(),
                        id: probe.host_id.clone(),
                        class: probe.host_class.clone(),
                        ts: now_micros(),
                        payload: Value::Null,
                    };
                    if let Ok((bytes, _)) = encode(&packet, probe.use_msgpack) {
                        let _ = socket.send_to(&bytes, server_addr);
                    }
                    last_probe = Some(Instant::now());
                }
            } else {
                last_probe = None;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe_spec() -> ProbeSpec {
        ProbeSpec {
            host_id: "h".into(),
            host_class: "c".into(),
            use_msgpack: false,
        }
    }

    #[test]
    fn watcher_sets_degraded_when_heartbeats_stop() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        let server_addr = server.local_addr().unwrap();
        let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").unwrap());
        let client_addr = client_sock.local_addr().unwrap();

        let degraded = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = spawn_watcher(
            Arc::clone(&client_sock),
            server_addr,
            Duration::from_millis(150),
            Arc::clone(&degraded),
            Arc::clone(&stop),
            probe_spec(),
        );

        // Send a few heartbeats → stays healthy.
        for _ in 0..3 {
            server
                .send_to(br#"{"type":"hb","srv_ts":1}"#, client_addr)
                .unwrap();
            std::thread::sleep(Duration::from_millis(40));
        }
        std::thread::sleep(Duration::from_millis(50));
        assert!(!degraded.load(Ordering::Relaxed));

        // Silence > timeout → degraded.
        std::thread::sleep(Duration::from_millis(500));
        assert!(degraded.load(Ordering::Relaxed));

        // Heartbeats resume → recovers. Assert inside the window: sleeping
        // past 150 ms would legitimately re-degrade the client.
        for _ in 0..3 {
            server
                .send_to(br#"{"type":"hb","srv_ts":2}"#, client_addr)
                .unwrap();
            std::thread::sleep(Duration::from_millis(40));
        }
        std::thread::sleep(Duration::from_millis(60));
        assert!(!degraded.load(Ordering::Relaxed));

        stop.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn watcher_ignores_non_heartbeat_datagrams() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        let server_addr = server.local_addr().unwrap();
        let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").unwrap());
        let client_addr = client_sock.local_addr().unwrap();

        let degraded = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = spawn_watcher(
            Arc::clone(&client_sock),
            server_addr,
            Duration::from_millis(150),
            Arc::clone(&degraded),
            Arc::clone(&stop),
            probe_spec(),
        );

        // Noise that is not a heartbeat must not refresh liveness.
        server.send_to(b"random junk", client_addr).unwrap();
        std::thread::sleep(Duration::from_millis(500));
        assert!(!degraded.load(Ordering::Relaxed)); // never saw a real hb → stays false

        stop.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    /// Drive the client into degraded state: one heartbeat, then silence.
    /// Returns once `degraded` is set. `server` receives probe datagrams.
    fn drive_degraded(
        server: &UdpSocket,
        client_addr: SocketAddr,
        degraded: &AtomicBool,
        timeout_ms: u64,
    ) {
        server
            .send_to(br#"{"type":"hb","srv_ts":1}"#, client_addr)
            .unwrap();
        std::thread::sleep(Duration::from_millis(40));
        let deadline = Instant::now() + Duration::from_millis(timeout_ms * 6);
        while !degraded.load(Ordering::Relaxed) {
            assert!(Instant::now() < deadline, "client never degraded");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn degraded_watcher_sends_session_probe() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let server_addr = server.local_addr().unwrap();
        let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").unwrap());
        let client_addr = client_sock.local_addr().unwrap();

        let degraded = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = spawn_watcher(
            Arc::clone(&client_sock),
            server_addr,
            Duration::from_millis(150),
            Arc::clone(&degraded),
            Arc::clone(&stop),
            probe_spec(),
        );

        drive_degraded(&server, client_addr, &degraded, 150);

        // A `_session` probe must arrive at the server.
        let mut buf = [0u8; 2048];
        let (n, from) = server.recv_from(&mut buf).unwrap();
        assert_eq!(from, client_addr);
        let v: serde_json::Value = serde_json::from_slice(&buf[..n]).unwrap();
        assert_eq!(v["topic"], "_session");
        assert_eq!(v["id"], "h");
        assert_eq!(v["class"], "c");
        assert!(v["ts"].as_u64().unwrap() > 0);
        assert!(v["payload"].is_null());

        stop.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn probes_are_rate_limited_to_one_per_timeout() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        server
            .set_read_timeout(Some(Duration::from_millis(80)))
            .unwrap();
        let server_addr = server.local_addr().unwrap();
        let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").unwrap());
        let client_addr = client_sock.local_addr().unwrap();

        let degraded = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = spawn_watcher(
            Arc::clone(&client_sock),
            server_addr,
            Duration::from_millis(150),
            Arc::clone(&degraded),
            Arc::clone(&stop),
            probe_spec(),
        );

        drive_degraded(&server, client_addr, &degraded, 150);

        // Over ~2.5 timeout windows of continued silence the server should
        // see at most 3 probes (one per window), never a flood.
        let mut count = 0u32;
        let end = Instant::now() + Duration::from_millis(380);
        while Instant::now() < end {
            let mut buf = [0u8; 2048];
            match server.recv_from(&mut buf) {
                Ok((n, _)) => {
                    let v: serde_json::Value = serde_json::from_slice(&buf[..n]).unwrap();
                    assert_eq!(v["topic"], "_session");
                    count += 1;
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut => {}
                Err(e) => panic!("recv failed: {e}"),
            }
        }
        assert!(count >= 1, "no probe received");
        assert!(count <= 3, "probe flood: {count} datagrams in 380ms");

        stop.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn heartbeat_reply_to_probe_recovers_client() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let server_addr = server.local_addr().unwrap();
        let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").unwrap());
        let client_addr = client_sock.local_addr().unwrap();

        let degraded = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = spawn_watcher(
            Arc::clone(&client_sock),
            server_addr,
            Duration::from_millis(150),
            Arc::clone(&degraded),
            Arc::clone(&stop),
            probe_spec(),
        );

        drive_degraded(&server, client_addr, &degraded, 150);

        // Wait for a probe, then answer with a heartbeat: the self-healing
        // loop a real server provides.
        let mut buf = [0u8; 2048];
        let (_, from) = server.recv_from(&mut buf).unwrap();
        server
            .send_to(br#"{"type":"hb","srv_ts":2}"#, from)
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        while degraded.load(Ordering::Relaxed) {
            assert!(Instant::now() < deadline, "client never recovered");
            std::thread::sleep(Duration::from_millis(20));
        }

        stop.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn msgpack_probe_decodes() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let server_addr = server.local_addr().unwrap();
        let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").unwrap());
        let client_addr = client_sock.local_addr().unwrap();

        let degraded = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = spawn_watcher(
            Arc::clone(&client_sock),
            server_addr,
            Duration::from_millis(150),
            Arc::clone(&degraded),
            Arc::clone(&stop),
            ProbeSpec {
                host_id: "h".into(),
                host_class: "c".into(),
                use_msgpack: true,
            },
        );

        drive_degraded(&server, client_addr, &degraded, 150);

        let mut buf = [0u8; 2048];
        let (n, _) = server.recv_from(&mut buf).unwrap();
        assert_ne!(buf[0], b'{'); // msgpack, not JSON
        let v: serde_json::Value = rmp_serde::from_slice(&buf[..n]).unwrap();
        assert_eq!(v["topic"], "_session");

        stop.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}
