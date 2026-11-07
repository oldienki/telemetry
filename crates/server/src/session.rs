//! Client session registry for the reverse heartbeat mechanism.
//!
//! The first datagram from a `SocketAddr` registers a session. A sweeper
//! evicts sessions that stay silent past the TTL.

use dashmap::DashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Per-client session state.
#[derive(Debug)]
pub struct ClientSession {
    /// Last time any datagram arrived from this client.
    pub last_seen: Instant,
}

/// Thread-safe registry of active client sessions.
#[derive(Debug, Default)]
pub struct SessionRegistry {
    sessions: DashMap<SocketAddr, ClientSession>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `addr` or refresh its `last_seen` to now.
    pub fn touch(&self, addr: SocketAddr) {
        self.touch_at(addr, Instant::now());
    }

    /// Insert or refresh a session at an explicit instant (testable).
    pub(crate) fn touch_at(&self, addr: SocketAddr, at: Instant) {
        self.sessions.insert(addr, ClientSession { last_seen: at });
    }

    /// Snapshot of currently registered client addresses.
    pub fn active_addrs(&self) -> Vec<SocketAddr> {
        self.sessions.iter().map(|e| *e.key()).collect()
    }

    /// Evict sessions idle longer than `ttl`. Returns the evicted count.
    pub fn evict_stale(&self, ttl: Duration) -> usize {
        let before = self.sessions.len();
        self.sessions.retain(|_, s| s.last_seen.elapsed() <= ttl);
        before - self.sessions.len()
    }

    /// Number of registered sessions.
    pub fn len(&self) -> usize {
        self.sessions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    #[test]
    fn first_touch_registers_session() {
        let reg = SessionRegistry::new();
        reg.touch(addr(1));
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.active_addrs(), vec![addr(1)]);
    }

    #[test]
    fn repeated_touch_refreshes_without_duplicating() {
        let reg = SessionRegistry::new();
        reg.touch(addr(1));
        reg.touch(addr(1));
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn stale_session_is_evicted() {
        let reg = SessionRegistry::new();
        reg.touch_at(addr(1), Instant::now() - Duration::from_secs(10));
        assert_eq!(reg.evict_stale(Duration::from_secs(5)), 1);
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn fresh_session_survives_sweep() {
        let reg = SessionRegistry::new();
        reg.touch(addr(1));
        assert_eq!(reg.evict_stale(Duration::from_secs(5)), 0);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn zero_ttl_evicts_everything() {
        let reg = SessionRegistry::new();
        reg.touch_at(addr(1), Instant::now() - Duration::from_millis(1));
        assert_eq!(reg.evict_stale(Duration::ZERO), 1);
    }
}
