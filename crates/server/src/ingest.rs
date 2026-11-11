//! UDP ingest engine: `SO_REUSEPORT` socket pool feeding a bounded
//! decode channel, plus the reverse-heartbeat broadcaster.
//!
//! Backpressure policy is Drop-Latest: a full channel drops the new
//! datagram and bumps `dropped_packets_total`; the socket task never
//! awaits storage or decoding capacity.

use crate::decode;
use crate::event::now_micros;
use crate::metrics;
use crate::session::SessionRegistry;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

/// Default bounded decode-channel capacity.
pub const INGEST_CHANNEL_CAPACITY: usize = 10_000;

/// Reserved topic for client keepalive probes. Probe datagrams refresh the
/// sender session like any packet but are never published to the event bus.
pub const SESSION_TOPIC: &str = "_session";

/// One raw datagram as received from the socket pool.
#[derive(Debug)]
pub struct RawDatagram {
    /// Datagram bytes (not yet decoded).
    pub bytes: Vec<u8>,
    /// Sender address; registers/refreshes the session.
    pub from: SocketAddr,
}

/// Running ingest engine; drop the handles to stop the tasks.
pub struct Ingest {
    /// All spawned tasks (socket workers + decode worker).
    pub tasks: Vec<JoinHandle<()>>,
    /// Sockets bound by the pool; index 0 sends heartbeats.
    pub sockets: Vec<Arc<UdpSocket>>,
    /// Local address of the ingest port (handy for tests).
    pub local_addr: SocketAddr,
}

/// Bind `workers` `SO_REUSEPORT` sockets on `bind_addr`, spawn a socket
/// task per socket, and a single decode task that publishes to `bus`.
///
/// `decode_worker` keeps ordering simple: one consumer per event stream.
/// `sessions` is shared with the heartbeat broadcaster.
pub async fn spawn(
    bind_addr: SocketAddr,
    workers: usize,
    sessions: Arc<SessionRegistry>,
    bus: crate::bus::EventBus,
) -> std::io::Result<Ingest> {
    let workers = workers.max(1);
    let (tx, mut rx) = mpsc::channel::<RawDatagram>(INGEST_CHANNEL_CAPACITY);
    let mut sockets = Vec::with_capacity(workers);
    let mut tasks = Vec::with_capacity(workers + 1);

    for idx in 0..workers {
        let sock = bind_reuseport(bind_addr)?;
        let local = sock.local_addr()?;
        let sock = Arc::new(UdpSocket::from_std(sock)?);
        debug!(socket = %local, worker = idx, "ingest socket bound");
        sockets.push(Arc::clone(&sock));

        let tx = tx.clone();
        tasks.push(tokio::spawn(async move {
            let mut buf = vec![0u8; decode::MAX_DATAGRAM];
            loop {
                match sock.recv_from(&mut buf).await {
                    Ok((n, from)) => {
                        try_enqueue(
                            &tx,
                            RawDatagram {
                                bytes: buf[..n].to_vec(),
                                from,
                            },
                        );
                    }
                    Err(e) => warn!(error = %e, "udp recv failed"),
                }
            }
        }));
    }

    let local_addr = sockets[0].local_addr()?;
    let decode_task_sessions = Arc::clone(&sessions);
    tasks.push(tokio::spawn(async move {
        while let Some(d) = rx.recv().await {
            decode_task_sessions.touch(d.from);
            match decode::decode(&d.bytes, now_micros()) {
                Ok(ev) => {
                    if ev.topic != SESSION_TOPIC {
                        let _ = bus.publish(ev);
                    }
                }
                Err(e) => {
                    metrics::inc_decode_error();
                    debug!(error = %e, "datagram decode failed");
                }
            }
        }
    }));

    Ok(Ingest {
        tasks,
        sockets,
        local_addr,
    })
}

/// Non-blocking enqueue. Counts a drop when the channel is full or closed.
fn try_enqueue(tx: &mpsc::Sender<RawDatagram>, d: RawDatagram) {
    match tx.try_send(d) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            metrics::inc_dropped();
            debug!("ingest channel full; dropping datagram");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            metrics::inc_dropped();
        }
    }
}

/// Bind a UDP socket with `SO_REUSEPORT` (unix) so several sockets share
/// the port. Falls back to a plain bind where unavailable.
fn bind_reuseport(addr: SocketAddr) -> std::io::Result<std::net::UdpSocket> {
    let domain = if addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let sock = socket2::Socket::new(domain, socket2::Type::DGRAM, None)?;
    sock.set_nonblocking(true)?;
    #[cfg(unix)]
    sock.set_reuse_port(true)?;
    sock.set_reuse_address(true)?;
    sock.bind(&addr.into())?;
    Ok(sock.into())
}

/// Broadcast heartbeats every `interval` to all registered sessions from
/// `socket` (the ingest port, so clients see the server identity).
pub async fn heartbeat_loop(
    socket: Arc<UdpSocket>,
    sessions: Arc<SessionRegistry>,
    interval: Duration,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let payload = format!("{{\"type\":\"hb\",\"srv_ts\":{}}}", now_micros());
        for addr in sessions.active_addrs() {
            if let Err(e) = socket.send_to(payload.as_bytes(), addr).await {
                warn!(client = %addr, error = %e, "heartbeat send failed");
            }
        }
    }
}

/// Periodically evict sessions idle longer than `ttl`.
pub async fn sweeper_loop(sessions: Arc<SessionRegistry>, ttl: Duration, every: Duration) {
    let mut tick = tokio::time::interval(every);
    loop {
        tick.tick().await;
        let n = sessions.evict_stale(ttl);
        if n > 0 {
            debug!(evicted = n, "evicted stale sessions");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::EventBus;
    use crate::decode::DecodeError;

    #[tokio::test]
    async fn try_enqueue_counts_drop_on_full_channel() {
        let (tx, _rx) = mpsc::channel::<RawDatagram>(1);
        let mk = || RawDatagram {
            bytes: vec![0x7B],
            from: "127.0.0.1:9".parse().unwrap(),
        };
        try_enqueue(&tx, mk());
        let before = metrics::dropped_packets_total();
        try_enqueue(&tx, mk()); // full → drop
        assert_eq!(metrics::dropped_packets_total(), before + 1);
    }

    #[tokio::test]
    async fn try_enqueue_counts_drop_on_closed_channel() {
        let (tx, rx) = mpsc::channel::<RawDatagram>(1);
        drop(rx);
        let before = metrics::dropped_packets_total();
        try_enqueue(
            &tx,
            RawDatagram {
                bytes: vec![0x7B],
                from: "127.0.0.1:9".parse().unwrap(),
            },
        );
        assert_eq!(metrics::dropped_packets_total(), before + 1);
    }

    #[tokio::test]
    async fn ingest_decodes_datagram_and_registers_session() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe();
        let sessions = Arc::new(SessionRegistry::new());
        let ing = spawn(
            "127.0.0.1:0".parse().unwrap(),
            1,
            Arc::clone(&sessions),
            bus,
        )
        .await
        .unwrap();

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client
            .send_to(
                br#"{"topic":"t","id":"i","class":"c","ts":1}"#,
                ing.local_addr,
            )
            .await
            .unwrap();

        let ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("event within timeout")
            .unwrap();
        assert_eq!(ev.topic, "t");
        assert!(ev.ingress_ts > 0);

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(sessions.len(), 1);

        for t in ing.tasks {
            t.abort();
        }
    }

    #[tokio::test]
    async fn heartbeat_reaches_registered_client() {
        let sessions = Arc::new(SessionRegistry::new());
        let hb_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client_addr = client.local_addr().unwrap();
        sessions.touch(client_addr);

        let task = tokio::spawn(heartbeat_loop(
            hb_sock,
            Arc::clone(&sessions),
            Duration::from_millis(20),
        ));

        let mut buf = [0u8; 256];
        let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
            .await
            .expect("heartbeat within timeout")
            .unwrap();
        let msg: serde_json::Value = serde_json::from_slice(&buf[..n]).unwrap();
        assert_eq!(msg["type"], "hb");
        assert!(msg["srv_ts"].as_u64().unwrap() > 0);
        task.abort();
    }

    #[tokio::test]
    async fn session_probe_touches_session_without_bus_publish() {
        let bus = EventBus::new();
        let mut rx = bus.subscribe();
        let sessions = Arc::new(SessionRegistry::new());
        let ing = spawn(
            "127.0.0.1:0".parse().unwrap(),
            1,
            Arc::clone(&sessions),
            bus,
        )
        .await
        .unwrap();

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client_addr = client.local_addr().unwrap();
        client
            .send_to(
                br#"{"topic":"_session","id":"i","class":"c","ts":1}"#,
                ing.local_addr,
            )
            .await
            .unwrap();

        // Session is refreshed by the probe...
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(sessions.len(), 1);
        assert!(sessions.active_addrs().contains(&client_addr));

        // ...but the _session event never reaches the bus. A follow-up
        // normal event proves the pipeline is alive and the probe was
        // filtered, not just delayed.
        client
            .send_to(
                br#"{"topic":"real","id":"i","class":"c","ts":2}"#,
                ing.local_addr,
            )
            .await
            .unwrap();
        let ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("normal event within timeout")
            .unwrap();
        assert_eq!(ev.topic, "real");

        for t in ing.tasks {
            t.abort();
        }
    }

    #[tokio::test]
    async fn session_probe_msgpack_is_also_filtered() {
        // Edge: probes may arrive in MessagePack; filtering happens after
        // decode, so the wire format must not matter.
        let bus = EventBus::new();
        let mut rx = bus.subscribe();
        let sessions = Arc::new(SessionRegistry::new());
        let ing = spawn(
            "127.0.0.1:0".parse().unwrap(),
            1,
            Arc::clone(&sessions),
            bus,
        )
        .await
        .unwrap();

        #[derive(serde::Serialize)]
        struct Probe {
            topic: &'static str,
            id: &'static str,
            class: &'static str,
            ts: u64,
        }
        let bytes = rmp_serde::to_vec(&Probe {
            topic: "_session",
            id: "i",
            class: "c",
            ts: 1,
        })
        .unwrap();

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.send_to(&bytes, ing.local_addr).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(sessions.len(), 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(300), rx.recv())
                .await
                .is_err(),
            "_session event leaked to the bus"
        );

        for t in ing.tasks {
            t.abort();
        }
    }

    #[test]
    fn decode_error_counts() {
        let before = metrics::decode_errors_total();
        match decode::decode(b"\x00junk", 0) {
            Err(DecodeError::UnknownFormat(_)) => {}
            _ => panic!("expected UnknownFormat"),
        }
        assert_eq!(metrics::decode_errors_total(), before); // no auto-increment in decode()
    }
}
