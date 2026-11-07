# LightTelemetry

Lightweight, dependency-free telemetry for test environments: embedded rigs, hardware-in-the-loop benches, firmware CI — anywhere OpenTelemetry is too heavy to deploy.

**Fire-and-forget over UDP.** Producers emit a packet and move on; no buffers, no retries, no blocking. A server-side heartbeat lets clients detect an unreachable collector and drop emissions *before* paying serialization cost.

## How it works

```
 your app ──UDP──▶ light-telemetry daemon ──▶ REST / WebSocket / Grafana
 (Rust · Python · Go)      ingest 9876           query 8080
```

- **Emit** serializes `{topic, id, class, ts, payload}` (JSON or MessagePack) and sends one datagram.
- **Degrade** — the daemon sends reverse heartbeats; when they stop, the client's watcher flips `degraded` and `emit` drops before serializing. Telemetry never costs a dead server.
- **Ingest** — the daemon binds UDP, groups packets into sessions by `id`, fans out to an in-process bus, storage engine, reactive waiters, and WebSocket subscribers.

## Monorepo layout

| Path | Package | Language |
|---|---|---|
| `crates/server` | `light-telemetry` (binary) | Rust |
| `crates/client` | [`light-telemetry-client`](https://crates.io/crates/light-telemetry-client) | Rust |
| `crates/tracing-subscriber` | `light-telemetry-tracing` | Rust |
| `python/` | [`light-telemetry`](https://pypi.org/project/light-telemetry/) | Python ≥ 3.11 |
| `go/light-telemetry/` | `github.com/oldienki/telemetry/go/light-telemetry` | Go |
| `tests/e2e/` | end-to-end harness | Python |
| `docs/` | design proposals | — |

## Quickstart

### Run the daemon

```bash
just run                      # memory storage, UDP :9876, HTTP :8080
just run-sqlite               # SQLite backend
cargo run -p light-telemetry -- --storage postgres --db-url postgres://…
```

### Emit an event (Python)

```bash
pip install light-telemetry
```

```python
from light_telemetry import TelemetryClient

client = TelemetryClient.local("rig-01", "bench")   # zero-config localhost
client.start()
client.emit("test.start", {"case": "can_tx"})
client.stop()
```

`local` reads the daemon's own `telemetry.toml` (see below) and inherits `udp_bind` + `heartbeat_ms`. No file → `127.0.0.1:9876`.

### Query

```bash
curl 'http://localhost:8080/api/v1/events?topic=test.start&limit=50'
curl 'http://localhost:8080/api/v1/time'
```

## Server configuration

Precedence: **`--flag` > `telemetry.toml` > built-in default.**

File lookup order — `--config <path>` if given, else `/etc/telemetry.toml`, else `$HOME/.config/telemetry.toml` (first hit wins):

```toml
udp_bind        = "0.0.0.0:9876"
udp_workers     = 16
heartbeat_ms    = 1000
session_ttl_ms  = 5000
http_bind       = "0.0.0.0:8080"
storage         = "sqlite"            # memory | sqlite | postgres
db_url          = "sqlite:telemetry.db"
memory_capacity = 100000              # memory engine only
retention_ttl_ms = 86400000           # 0 disables the reaper
```

Every key has a matching `--kebab-case` CLI flag (`--udp-bind`, `--storage`, …). Unknown keys are rejected. Run `light-telemetry --help` for the full list.

## HTTP API

| Endpoint | Method | Purpose |
|---|---|---|
| `/api/v1/time` | GET | `{server_ts}` — clock baseline for test scripts |
| `/api/v1/events` | GET | Stored events; filters: `topic`, `class`, `id`, `since`, `until` (µs), `limit` |
| `/api/v1/wait` | POST | Reactive wait — blocks until a matching event arrives since `since_ts` |
| `/api/v1/stream` | GET (WS) | Live event stream, optional `?topic=` filter |
| `/grafana` | GET | JSON-datasource health probe |
| `/grafana/search` | POST | Metric discovery (topics) |
| `/grafana/query` | POST | Timeseries/table data for panels |

### Wait example

```bash
curl -X POST http://localhost:8080/api/v1/wait \
  -d '{"since_ts": <server_ts>, "timeout_ms": 5000, "filter": {"topic": "test.done", "class": "rig"}}'
```

Returns as soon as a matching event lands — the primitive test harnesses use for deterministic assertions (`408` on timeout). `filter` accepts `topic`, `class`, `id`, and `payload_match` (shallow object containment).

## Clients

All three share the same contract: one datagram per `emit`, MTU-safe truncation (`payload → {"truncated": true}`), heartbeat watcher with degradation, and a `local` constructor adopting `telemetry.toml`.

### Rust

```toml
[dependencies]
light-telemetry-client = "0.1"
```

```rust
let cfg = light_telemetry_client::TelemetryConfig::local("rig-01", "bench");
let client = light_telemetry_client::TelemetryClient::new(cfg)?;
client.emit("test.start", serde_json::json!({"case": "can_tx"}));
```

Add `light-telemetry-tracing` to forward `tracing` events.

### Python

```python
from light_telemetry import TelemetryClient, TelemetryLogHandler
import logging

client = TelemetryClient.local("rig-01", "bench").start()
logging.getLogger().addHandler(TelemetryLogHandler(client))
logging.getLogger("sut").info("ready", extra={"fw": "1.4.2"})
```

### Go

```go
c, err := telemetry.NewLocalClient("rig-01", "bench", "")
c.Emit("test.start", map[string]any{"case": "can_tx"})
```

A `slog` handler ships in the same package.

## Development

```bash
just fmt          # cargo fmt + ruff format + gofmt
just lint         # clippy -D warnings + ruff check + mypy + go vet
just test         # cargo test (all crates) + pytest + go test
just test-e2e     # full stack via docker-compose
```

### Releasing

```bash
just version-bump patch --commit --tag   # bumps Cargo.toml + pyproject + __init__.py,
                                         # tags vX.Y.Z and go/light-telemetry/vX.Y.Z
just publish-rust                        # cargo publish in dep order
just publish-python                      # uv build + uv publish
just publish-go push                     # tag + push (Go releases = git tags)
```

## License

MIT OR Apache-2.0
