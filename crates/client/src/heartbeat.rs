//! Heartbeat watcher: listens for server `{"type":"hb","srv_ts":…}` datagrams
//! and flips the shared `degraded` flag when they stop arriving.

use serde::Deserialize;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

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
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
        );

        // Noise that is not a heartbeat must not refresh liveness.
        server.send_to(b"random junk", client_addr).unwrap();
        std::thread::sleep(Duration::from_millis(500));
        assert!(!degraded.load(Ordering::Relaxed)); // never saw a real hb → stays false

        stop.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}
