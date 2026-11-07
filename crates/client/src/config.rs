//! Discover the server's `telemetry.toml` so a localhost client can adopt
//! the daemon's `udp_bind` and `heartbeat_ms` without manual wiring.
//!
//! Same search order as the server: explicit path, else
//! `/etc/telemetry.toml`, else `$HOME/.config/telemetry.toml`; the first
//! existing file wins. Only the keys the client consumes are read —
//! server-only keys are ignored.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

const ETC_PATH: &str = "/etc/telemetry.toml";
const USER_PATH: &str = ".config/telemetry.toml";

/// Server keys a client consumes.
#[derive(Debug, Default, PartialEq)]
pub struct ServerConfig {
    /// Daemon UDP ingest bind, e.g. `0.0.0.0:9876`.
    pub udp_bind: Option<SocketAddr>,
    /// Daemon heartbeat interval in milliseconds.
    pub heartbeat_ms: Option<u64>,
}

/// Default search candidates, in order.
fn default_paths() -> Vec<PathBuf> {
    let mut v = vec![PathBuf::from(ETC_PATH)];
    if let Ok(home) = std::env::var("HOME") {
        v.push(PathBuf::from(home).join(USER_PATH));
    }
    v
}

/// Extract `key = value` scalars. Only `udp_bind` and `heartbeat_ms` are
/// recognized; every other key (the file is shared with the server) is
/// skipped. Lines that do not parse are ignored — discovery must never
/// crash a client.
fn parse(text: &str) -> ServerConfig {
    let mut cfg = ServerConfig::default();
    for raw in text.lines() {
        let line = strip_comment(raw).trim();
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        match k.trim() {
            "udp_bind" => cfg.udp_bind = parse_value(v).and_then(|s| s.parse().ok()),
            "heartbeat_ms" => cfg.heartbeat_ms = v.trim().parse().ok(),
            _ => {}
        }
    }
    cfg
}

/// Strip a `#` comment that is not inside a quoted string.
fn strip_comment(line: &str) -> &str {
    let mut in_str = false;
    for (i, c) in line.char_indices() {
        match c {
            '"' => in_str = !in_str,
            '#' if !in_str => return &line[..i],
            _ => {}
        }
    }
    line
}

/// Unquote a TOML string value; pass scalars through verbatim.
fn parse_value(v: &str) -> Option<&str> {
    let v = v.trim();
    let s = v.strip_prefix('"')?.strip_suffix('"')?;
    Some(s)
}

/// Load the server config: `explicit` must exist (error propagates);
/// else the first existing default path; else empty.
pub fn load(explicit: Option<&Path>) -> std::io::Result<(ServerConfig, Option<PathBuf>)> {
    if let Some(p) = explicit {
        let text = std::fs::read_to_string(p)?;
        return Ok((parse(&text), Some(p.to_path_buf())));
    }
    for cand in default_paths() {
        if cand.is_file() {
            let text = std::fs::read_to_string(&cand)?;
            return Ok((parse(&text), Some(cand)));
        }
    }
    Ok((ServerConfig::default(), None))
}

/// Map a bind address to a send target: wildcard → same-family loopback.
pub fn localize(addr: SocketAddr) -> SocketAddr {
    if addr.ip().is_unspecified() {
        let loopback: std::net::IpAddr = if addr.is_ipv6() {
            "::1".parse().unwrap()
        } else {
            "127.0.0.1".parse().unwrap()
        };
        SocketAddr::new(loopback, addr.port())
    } else {
        addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_client_keys_ignores_rest() {
        let cfg = parse(
            r#"
            # daemon settings
            udp_bind = "0.0.0.0:9999"   # ingest
            heartbeat_ms = 250
            storage = "sqlite"
            db_url = "sqlite:x.db"
            retention_ttl_ms = 60000
            "#,
        );
        assert_eq!(
            cfg.udp_bind,
            Some("0.0.0.0:9999".parse::<SocketAddr>().unwrap())
        );
        assert_eq!(cfg.heartbeat_ms, Some(250));
    }

    #[test]
    fn empty_text_yields_defaults() {
        assert_eq!(parse(""), ServerConfig::default());
        assert_eq!(parse("storage = \"memory\"\n"), ServerConfig::default());
    }

    #[test]
    fn hash_inside_string_not_a_comment() {
        let cfg = parse(r#"udp_bind = "127.0.0.1:9#00""#);
        // The '#' sits inside the quotes; the port is "9#00" → invalid addr.
        assert!(cfg.udp_bind.is_none());
    }

    #[test]
    fn localize_maps_wildcards_only() {
        let v4: SocketAddr = "0.0.0.0:9876".parse().unwrap();
        assert_eq!(localize(v4), "127.0.0.1:9876".parse().unwrap());
        let v6: SocketAddr = "[::]:9876".parse().unwrap();
        assert_eq!(localize(v6), "[::1]:9876".parse().unwrap());
        let fixed: SocketAddr = "192.168.1.5:9876".parse().unwrap();
        assert_eq!(localize(fixed), fixed);
    }

    #[test]
    fn load_explicit_and_missing() {
        let dir = std::env::temp_dir();
        let p = dir.join(format!("lt-client-{}.toml", std::process::id()));
        std::fs::write(&p, "udp_bind = \"127.0.0.1:5555\"\nheartbeat_ms = 50\n").unwrap();
        let (cfg, src) = load(Some(&p)).unwrap();
        assert_eq!(cfg.heartbeat_ms, Some(50));
        assert_eq!(src.as_deref(), Some(p.as_path()));
        std::fs::remove_file(&p).ok();

        assert!(load(Some(Path::new("/nonexistent/x.toml"))).is_err());
    }
}
