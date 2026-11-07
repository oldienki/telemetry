//! light-telemetry-server: UDP telemetry ingest daemon for test environments.

mod bus;
mod decode;
mod event;
mod ingest;
mod metrics;
mod session;

use clap::Parser;
use session::SessionRegistry;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// CLI configuration for the daemon.
#[derive(Debug, Parser)]
#[command(
    name = "light-telemetry-server",
    about = "LightTelemetry ingest daemon"
)]
struct Cli {
    /// Address the UDP ingest pool binds.
    #[arg(long, default_value = "0.0.0.0:9876")]
    udp_bind: SocketAddr,

    /// Number of SO_REUSEPORT ingest sockets.
    #[arg(long)]
    udp_workers: Option<usize>,

    /// Heartbeat broadcast interval in milliseconds.
    #[arg(long, default_value_t = 1000)]
    heartbeat_ms: u64,

    /// Session time-to-live in milliseconds.
    #[arg(long, default_value_t = 5000)]
    session_ttl_ms: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let workers = cli
        .udp_workers
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |n| n.get()));

    let sessions = Arc::new(SessionRegistry::new());
    let bus = bus::EventBus::new();

    let ing = ingest::spawn(cli.udp_bind, workers, Arc::clone(&sessions), bus.clone()).await?;
    tracing::info!(addr = %ing.local_addr, workers, "udp ingest listening");

    let hb_socket = Arc::clone(&ing.sockets[0]);
    let hb_sessions = Arc::clone(&sessions);
    tokio::spawn(ingest::heartbeat_loop(
        hb_socket,
        hb_sessions,
        Duration::from_millis(cli.heartbeat_ms),
    ));
    tokio::spawn(ingest::sweeper_loop(
        sessions.clone(),
        Duration::from_millis(cli.session_ttl_ms),
        Duration::from_millis(cli.session_ttl_ms / 2).max(Duration::from_millis(250)),
    ));

    // Debug log each event; the first real consumer of the bus.
    tokio::spawn(async move {
        let mut rx = bus.subscribe();
        loop {
            match rx.recv().await {
                Ok(ev) => tracing::debug!(topic = %ev.topic, id = %ev.id, "event"),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "event log lagged")
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Periodic stats heartbeat for operators.
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        loop {
            tick.tick().await;
            tracing::info!(
                sessions = sessions.len(),
                dropped = metrics::dropped_packets_total(),
                decode_errors = metrics::decode_errors_total(),
                "stats"
            );
        }
    });

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    for task in &ing.tasks {
        task.abort();
    }
    Ok(())
}
