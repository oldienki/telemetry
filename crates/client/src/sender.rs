//! UDP datagram sender bound to an ephemeral local port.

use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;

/// Thin wrapper over one UDP socket targeting the ingest server.
pub(crate) struct UdpSender {
    socket: Arc<UdpSocket>,
    server_addr: SocketAddr,
}

impl UdpSender {
    /// Bind an ephemeral socket suited to `server_addr`'s family.
    pub fn bind(server_addr: SocketAddr) -> std::io::Result<Self> {
        let local: SocketAddr = if server_addr.is_ipv4() {
            "0.0.0.0:0".parse().unwrap()
        } else {
            "[::]:0".parse().unwrap()
        };
        let socket = UdpSocket::bind(local)?;
        Ok(Self {
            socket: Arc::new(socket),
            server_addr,
        })
    }

    /// Fire-and-forget send. Errors surface to the caller; nothing retries.
    pub fn send(&self, bytes: &[u8]) -> std::io::Result<()> {
        self.socket.send_to(bytes, self.server_addr)?;
        Ok(())
    }

    /// Shared socket handle (the heartbeat watcher reads on the same port).
    pub fn socket(&self) -> Arc<UdpSocket> {
        Arc::clone(&self.socket)
    }

    /// Local socket address, for tests.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}
