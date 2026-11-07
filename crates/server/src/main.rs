//! light-telemetry: UDP telemetry ingest daemon for test environments.

mod api;
mod bus;
mod config;
mod decode;
mod event;
mod ingest;
mod metrics;
mod session;
mod storage;

use anyhow::Context;
use clap::{Parser, ValueEnum};
use session::SessionRegistry;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// CLI configuration for the daemon.
#[derive(Debug, Parser)]
#[command(name = "light-telemetry", about = "LightTelemetry ingest daemon")]
struct Cli {
    /// Config file path; else /etc/telemetry.toml then
    /// $HOME/.config/telemetry.toml are tried in order.
    #[arg(long)]
    config: Option<std::path::PathBuf>,

    /// Address the UDP ingest pool binds.
    #[arg(long)]
    udp_bind: Option<SocketAddr>,

    /// Number of SO_REUSEPORT ingest sockets.
    #[arg(long)]
    udp_workers: Option<usize>,

    /// Heartbeat broadcast interval in milliseconds.
    #[arg(long)]
    heartbeat_ms: Option<u64>,

    /// Session time-to-live in milliseconds.
    #[arg(long)]
    session_ttl_ms: Option<u64>,

    /// Address the HTTP API binds.
    #[arg(long)]
    http_bind: Option<SocketAddr>,

    /// Storage engine backend.
    #[arg(long, value_enum)]
    storage: Option<StorageKind>,

    /// Capacity of the in-memory ring buffer.
    #[arg(long)]
    memory_capacity: Option<usize>,

    /// Retention TTL in milliseconds; 0 disables periodic cleanup.
    #[arg(long)]
    retention_ttl_ms: Option<u64>,

    /// Database URL for durable backends (`sqlite:path.db`).
    #[arg(long)]
    db_url: Option<String>,
}

/// Selectable storage backends.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum StorageKind {
    Memory,
    Sqlite,
    Postgres,
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
    let cfg = config::resolve(&cli)?;
    if let Some(p) = &cfg.config_file {
        tracing::info!(path = %p.display(), "loaded config file");
    }
    let workers = cfg
        .udp_workers
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |n| n.get()));

    let sessions = Arc::new(SessionRegistry::new());
    let bus = bus::EventBus::new();

    let engine: Arc<dyn storage::StorageEngine> = match cfg.storage {
        StorageKind::Memory => Arc::new(storage::memory::MemoryEngine::new(cfg.memory_capacity)),
        StorageKind::Sqlite => {
            let url = cfg
                .db_url
                .clone()
                .unwrap_or_else(|| "sqlite:telemetry.db".to_string());
            Arc::new(storage::sqlite::SqliteEngine::connect(&url).await?)
        }
        StorageKind::Postgres => {
            let url = cfg
                .db_url
                .clone()
                .context("db_url is required for postgres storage")?;
            Arc::new(storage::postgres::PgEngine::connect(&url).await?)
        }
    };
    storage::spawn_pipeline(&bus, Arc::clone(&engine));

    if cfg.retention_ttl_ms > 0 {
        storage::retention::spawn(
            Arc::clone(&engine),
            Duration::from_millis(cfg.retention_ttl_ms),
        );
    }

    let dispatch = api::wait::WaitDispatcher::spawn(&bus);
    let state = Arc::new(api::AppState {
        storage: engine,
        dispatch,
        bus: bus.clone(),
    });

    let ing = ingest::spawn(cfg.udp_bind, workers, Arc::clone(&sessions), bus.clone()).await?;
    tracing::info!(addr = %ing.local_addr, workers, "udp ingest listening");

    let hb_socket = Arc::clone(&ing.sockets[0]);
    let hb_sessions = Arc::clone(&sessions);
    tokio::spawn(ingest::heartbeat_loop(
        hb_socket,
        hb_sessions,
        Duration::from_millis(cfg.heartbeat_ms),
    ));
    tokio::spawn(ingest::sweeper_loop(
        sessions.clone(),
        Duration::from_millis(cfg.session_ttl_ms),
        Duration::from_millis(cfg.session_ttl_ms / 2).max(Duration::from_millis(250)),
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

    // HTTP API.
    let listener = tokio::net::TcpListener::bind(cfg.http_bind).await?;
    tracing::info!(addr = %cfg.http_bind, "http api listening");
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, api::router(state)).await {
            tracing::error!(error = %e, "http server failed");
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
