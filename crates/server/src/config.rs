//! File-backed configuration (`telemetry.toml`).
//!
//! Precedence: CLI flag > config file > built-in default.
//! Search order when `--config` is absent:
//!   1. `/etc/telemetry.toml`
//!   2. `$HOME/.config/telemetry.toml`
//!
//! The first existing file wins; none found → all defaults apply.

use anyhow::Context;
use clap::ValueEnum;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crate::{Cli, StorageKind};

/// System-wide config location, checked before the user's.
const ETC_PATH: &str = "/etc/telemetry.toml";
/// User config path under `$HOME` (relative).
const USER_PATH: &str = ".config/telemetry.toml";

/// Every field optional — an absent key falls through to the next
/// source (CLI, then built-in default).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub udp_bind: Option<SocketAddr>,
    pub udp_workers: Option<usize>,
    pub heartbeat_ms: Option<u64>,
    pub session_ttl_ms: Option<u64>,
    pub http_bind: Option<SocketAddr>,
    /// `memory` | `sqlite` | `postgres` (case-insensitive).
    pub storage: Option<String>,
    pub memory_capacity: Option<usize>,
    pub retention_ttl_ms: Option<u64>,
    pub db_url: Option<String>,
}

/// Fully merged runtime configuration.
#[derive(Debug)]
pub struct Settings {
    pub udp_bind: SocketAddr,
    pub udp_workers: Option<usize>,
    pub heartbeat_ms: u64,
    pub session_ttl_ms: u64,
    pub http_bind: SocketAddr,
    pub storage: StorageKind,
    pub memory_capacity: usize,
    pub retention_ttl_ms: u64,
    pub db_url: Option<String>,
    /// File the settings came from, if any.
    pub config_file: Option<PathBuf>,
}

/// Default search candidates, in order.
fn default_paths() -> Vec<PathBuf> {
    let mut v = vec![PathBuf::from(ETC_PATH)];
    if let Ok(home) = std::env::var("HOME") {
        v.push(PathBuf::from(home).join(USER_PATH));
    }
    v
}

/// Parse one TOML file into `FileConfig`.
fn parse(path: &Path) -> anyhow::Result<FileConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read config file {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("invalid TOML in config file {}", path.display()))
}

/// Load config: explicit `--config` path (must exist), else the first
/// existing default path, else empty.
pub fn load(explicit: Option<&Path>) -> anyhow::Result<(FileConfig, Option<PathBuf>)> {
    if let Some(p) = explicit {
        return parse(p).map(|c| (c, Some(p.to_path_buf())));
    }
    for cand in default_paths() {
        if cand.is_file() {
            return parse(&cand).map(|c| (c, Some(cand)));
        }
    }
    Ok((FileConfig::default(), None))
}

/// Merge `cli` over `file` over built-in defaults.
fn merge(cli: &Cli, file: FileConfig, source: Option<PathBuf>) -> anyhow::Result<Settings> {
    let storage = if let Some(s) = cli.storage {
        s
    } else if let Some(s) = &file.storage {
        StorageKind::from_str(s, true)
            .map_err(|e| anyhow::anyhow!("invalid `storage` in config file: {e}"))?
    } else {
        StorageKind::Memory
    };
    Ok(Settings {
        udp_bind: cli
            .udp_bind
            .or(file.udp_bind)
            .unwrap_or_else(|| "0.0.0.0:9876".parse().unwrap()),
        udp_workers: cli.udp_workers.or(file.udp_workers),
        heartbeat_ms: cli.heartbeat_ms.or(file.heartbeat_ms).unwrap_or(1000),
        session_ttl_ms: cli.session_ttl_ms.or(file.session_ttl_ms).unwrap_or(5000),
        http_bind: cli
            .http_bind
            .or(file.http_bind)
            .unwrap_or_else(|| "0.0.0.0:8080".parse().unwrap()),
        storage,
        memory_capacity: cli
            .memory_capacity
            .or(file.memory_capacity)
            .unwrap_or(crate::storage::memory::DEFAULT_CAPACITY),
        retention_ttl_ms: cli.retention_ttl_ms.or(file.retention_ttl_ms).unwrap_or(0),
        db_url: cli.db_url.clone().or(file.db_url),
        config_file: source,
    })
}

/// Entry point: load file config then merge over CLI.
pub fn resolve(cli: &Cli) -> anyhow::Result<Settings> {
    let (file, source) = load(cli.config.as_deref())?;
    merge(cli, file, source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn cli(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("cli parse")
    }

    #[test]
    fn parse_full_file() {
        let cfg: FileConfig = toml::from_str(
            r#"
            udp_bind = "127.0.0.1:9000"
            heartbeat_ms = 250
            storage = "sqlite"
            db_url = "sqlite:test.db"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.udp_bind.unwrap().port(), 9000);
        assert_eq!(cfg.heartbeat_ms, Some(250));
        assert_eq!(cfg.storage.as_deref(), Some("sqlite"));
        assert_eq!(cfg.db_url.as_deref(), Some("sqlite:test.db"));
    }

    #[test]
    fn precedence_cli_over_file_over_default() {
        let file = FileConfig {
            heartbeat_ms: Some(250),
            http_bind: Some("127.0.0.1:7000".parse().unwrap()),
            ..Default::default()
        };
        // CLI sets heartbeat → wins; http falls to file; udp falls to default.
        let c = cli(&["lt", "--heartbeat-ms", "100"]);
        let s = merge(&c, file, None).unwrap();
        assert_eq!(s.heartbeat_ms, 100);
        assert_eq!(s.http_bind.port(), 7000);
        assert_eq!(s.udp_bind.port(), 9876);
        assert_eq!(s.session_ttl_ms, 5000);
        assert!(matches!(s.storage, StorageKind::Memory));
    }

    #[test]
    fn storage_from_file_case_insensitive() {
        let file = FileConfig {
            storage: Some("POSTGRES".into()),
            ..Default::default()
        };
        let c = cli(&["lt"]);
        let s = merge(&c, file, None).unwrap();
        assert!(matches!(s.storage, StorageKind::Postgres));
    }

    #[test]
    fn explicit_missing_path_errors() {
        let err = load(Some(Path::new("/nonexistent/t.toml"))).unwrap_err();
        assert!(err.to_string().contains("cannot read config file"));
    }

    #[test]
    fn malformed_toml_errors() {
        let p = std::env::temp_dir().join(format!("lt-bad-{}.toml", std::process::id()));
        std::fs::write(&p, "not = [valid").unwrap();
        let err = load(Some(&p)).unwrap_err();
        assert!(err.to_string().contains("invalid TOML"));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn unknown_key_rejected() {
        let err = toml::from_str::<FileConfig>("bogus_key = 1").unwrap_err();
        assert!(err.to_string().contains("bogus_key"));
    }

    #[test]
    fn bad_storage_value_errors() {
        let file = FileConfig {
            storage: Some("s3".into()),
            ..Default::default()
        };
        let c = cli(&["lt"]);
        let err = merge(&c, file, None).unwrap_err();
        assert!(err.to_string().contains("invalid `storage`"));
    }
}
