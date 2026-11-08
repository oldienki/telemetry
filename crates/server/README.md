# light-telemetry

The LightTelemetry ingest daemon: a single-binary UDP telemetry collector for test environments — embedded rigs, hardware-in-the-loop benches, firmware CI — where OpenTelemetry is too heavy to deploy.

[Project repository](https://github.com/oldienki/telemetry) · [`light-telemetry-client`](https://crates.io/crates/light-telemetry-client) · [`light-telemetry-tracing`](https://crates.io/crates/light-telemetry-tracing)

## Responsibility

This crate is the collector side of the LightTelemetry protocol. It:

- **Ingests** telemetry datagrams (JSON or MessagePack) over a pool of `SO_REUSEPORT` UDP sockets.
- **Decodes** each datagram into a canonical [`TelemetryEvent`](src/event.rs) and stamps it with a server-side `ingress_ts` (µs since epoch) — the only timestamp used for ordering and matching.
- **Fans out** events on an internal broadcast bus to storage, reactive waiters, and live WebSocket subscribers.
- **Persists** events through a pluggable `StorageEngine` (in-memory ring, SQLite, or PostgreSQL) with optional TTL-based retention.
- **Heartbeats back**: broadcasts `{"type":"hb","srv_ts":…}` to every recently-active client so emitters can detect a dead collector and stop paying serialization cost.
- **Serves** a REST query API, a reactive `wait` endpoint, a bidirectional WebSocket stream, and a Grafana JSON-datasource adapter over HTTP.

## Architecture

```
                ┌────────────── SO_REUSEPORT pool (N sockets) ──────────────┐
 UDP :9876 ───▶ │  socket task per socket: recv_from → try_enqueue          │
                └──────────────────────────┬───────────────────────────────┘
                                           ▼  bounded mpsc (10 000, Drop-Latest)
                                    decode task (single)
                                     │      │ format sniff → TelemetryEvent
                                     │      │ + ingress_ts + session.touch
                                           ▼
                              EventBus (tokio broadcast, cap 4 096)
                          ┌───────────┼───────────────┬──────────────┐
                          ▼           ▼               ▼              ▼
                    storage pipeline  WaitDispatcher  WS /stream     debug logger
                          │           (wait predicates) (also WS→bus ingest)
                          ▼
               StorageEngine: memory | sqlite | postgres
                          ▲
               retention reaper (optional TTL cleanup)

 heartbeat_loop ── {"type":"hb"} ──▶ SessionRegistry.active_addrs() (all clients)
 sweeper_loop  ── evicts sessions silent longer than session_ttl_ms
```

Key design decisions:

- **Drop-Latest backpressure.** Socket tasks use `try_send` into a bounded channel; a full channel drops the new datagram and bumps `dropped_packets_total`. Ingest never blocks on storage or decode.
- **Single decode task.** One consumer drains the channel, preserving per-source ordering and keeping the event stream simple.
- **Format sniffing.** First byte selects the decoder: `{` → JSON, MessagePack map markers (`0x80–0x8F`, `0xDE`, `0xDF`) → MessagePack. Datagrams over 65 507 bytes or with unknown first bytes are rejected into `decode_errors_total`.
- **Server clock wins.** `ingress_ts` is stamped at reception; client `ts` is informational only. The `wait` endpoint and all time-range filters compare against `ingress_ts`, so test assertions never depend on clock skew between rig and collector.
- **Sessions by source address.** First datagram from a `SocketAddr` registers it; the heartbeat loop pings all registered addresses from ingest socket #0; a sweeper evicts silent sessions.

## Installation

```bash
cargo install light-telemetry
light-telemetry --help
```

Or from source: `cargo build --release -p light-telemetry` → `target/release/light-telemetry`.

## Usage

```bash
light-telemetry                                    # memory storage, UDP :9876, HTTP :8080
light-telemetry --storage sqlite --db-url sqlite:telemetry.db
light-telemetry --storage postgres --db-url postgres://user:pass@host/db
```

### Configuration

Precedence: **`--flag` > `telemetry.toml` > built-in default.**

File lookup order: `--config <path>`, else `/etc/telemetry.toml`, else `$HOME/.config/telemetry.toml` (first existing file wins). Unknown keys are rejected.

```toml
udp_bind         = "0.0.0.0:9876"
udp_workers      = 16                 # default: available_parallelism
heartbeat_ms     = 1000
session_ttl_ms   = 5000
http_bind        = "0.0.0.0:8080"
storage          = "sqlite"           # memory | sqlite | postgres
db_url           = "sqlite:telemetry.db"
memory_capacity  = 100000             # memory engine only
retention_ttl_ms = 86400000           # 0 disables the reaper
```

Every key has a matching kebab-case CLI flag (`--udp-bind`, `--udp-workers`, `--heartbeat-ms`, `--session-ttl-ms`, `--http-bind`, `--storage`, `--memory-capacity`, `--retention-ttl-ms`, `--db-url`).

### HTTP API

| Endpoint | Method | Purpose |
|---|---|---|
| `/api/v1/time` | GET | `{server_ts}` — clock baseline for test scripts |
| `/api/v1/events` | GET | Stored events; filters: `topic`, `class`, `id`, `since`, `until` (µs), `limit` |
| `/api/v1/wait` | POST | Reactive wait — blocks until a matching event arrives since `since_ts` |
| `/api/v1/stream` | GET (WS) | Live event stream, optional `?topic=`; also accepts inbound frames as a TCP ingest fallback |
| `/grafana` | GET | JSON-datasource health probe |
| `/grafana/search` | POST | Static target list (`events`, `events.count`) |
| `/grafana/query` | POST | Table / timeseries frames for panels |

### The `wait` primitive

`POST /api/v1/wait` registers a predicate *before* checking storage (closing the race window), then blocks until a matching event arrives or `timeout_ms` elapses (`408`):

```bash
curl -X POST http://localhost:8080/api/v1/wait -d '{
  "since_ts": <server_ts>,
  "timeout_ms": 5000,
  "filter": {"topic": "test.done", "class": "rig", "payload_match": {"status": "pass"}}
}'
```

`filter` accepts `topic`, `class`, `id`, and `payload_match` (shallow object containment). This is the primitive test harnesses use for deterministic assertions: emit from the system under test, wait on the collector.

### WebSocket stream

`GET /api/v1/stream` broadcasts every bus event as a JSON text frame. A lagged receiver gets `{"type":"lagged","skipped":n}` and continues. Inbound text/binary frames are decoded exactly like UDP datagrams and published onto the bus — a TCP ingestion fallback for networks that block UDP; undecodable frames get `{"type":"error","error":"…"}` back.

### Emitting clients

- **Rust**: [`light-telemetry-client`](https://crates.io/crates/light-telemetry-client) (+ [`light-telemetry-tracing`](https://crates.io/crates/light-telemetry-tracing) for `tracing` integration)
- **Python**: [`light-telemetry`](https://pypi.org/project/light-telemetry/) (`logging` handler included)
- **Go**: `github.com/oldienki/telemetry/go/light-telemetry` (`slog` handler included)

All clients share the contract: one datagram per emit, MTU-safe truncation, heartbeat-driven degradation, zero-config `local` adoption of `telemetry.toml`.

## Observability

The daemon logs through `tracing` (`RUST_LOG` / `EnvFilter`, default `info`) and emits a periodic stats line every 30 s: active session count, `dropped_packets_total`, `decode_errors_total`.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
