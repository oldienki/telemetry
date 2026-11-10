# LightTelemetry

Lightweight, dependency-free telemetry daemon for test environments: embedded rigs, hardware-in-the-loop benches, firmware CI — anywhere OpenTelemetry is too heavy to deploy.

**Fire-and-forget over UDP.** Producers emit a packet and move on; no buffers, no retries, no blocking. A server-side heartbeat lets clients detect an unreachable collector and drop emissions *before* paying serialization cost.

## How it works

```
your app ──UDP──▶ light-telemetry daemon ──▶ REST / WebSocket / Grafana
(Rust · Python · Go)      ingest 9876           query 8080
```

- **Emit** serializes `{topic, id, class, ts, payload}` (JSON or MessagePack) and sends one datagram.
- **Degrade** — the daemon sends reverse heartbeats; when they stop, the client's watcher flips `degraded` and `emit` drops before serializing. Telemetry never costs a dead server.
- **Ingest** — the daemon binds UDP, groups packets into sessions by `id`, fans out to an in-process bus, storage engine, reactive waiters, and WebSocket subscribers.

## Quick start

```bash
docker run --rm -p 8080:8080 -p 9876:9876/udp oldienki/light-telemetry:latest
```

Emit an event from any client:

```bash
pip install light-telemetry
```

```python
from light_telemetry import TelemetryClient

client = TelemetryClient.local("rig-01", "bench")
client.start()
client.emit("test.start", {"case": "can_tx"})
client.stop()
```

Query the daemon:

```bash
curl 'http://localhost:8080/api/v1/events?topic=test.start&limit=50'
curl -X POST http://localhost:8080/api/v1/wait \
  -d '{"since_ts": <server_ts>, "timeout_ms": 5000, "filter": {"topic": "test.done"}}'
```

## Configuration

Precedence: **`--flag` > `telemetry.toml` > built-in default.**

Mount a config file at `/etc/telemetry.toml` (or pass `--config`):

```toml
udp_bind        = "0.0.0.0:9876"
udp_workers     = 16
heartbeat_ms    = 1000
session_ttl_ms  = 5000
http_bind       = "0.0.0.0:8080"
storage         = "sqlite"            # memory | sqlite | postgres
db_url          = "sqlite:/data/telemetry.db"
memory_capacity = 100000              # memory engine only
retention_ttl_ms = 86400000           # 0 disables the reaper
```

Every key has a matching `--kebab-case` CLI flag. Run `light-telemetry --help` for the full list.

## HTTP API

| Endpoint | Method | Purpose |
|---|---|---|
| `/api/v1/time` | GET | `{server_ts}` — clock baseline for test scripts |
| `/api/v1/events` | GET | Stored events; filters: `topic`, `class`, `id`, `since`, `until` (µs), `limit` |
| `/api/v1/wait` | POST | Reactive wait — blocks until a matching event arrives (`408` on timeout) |
| `/api/v1/stream` | GET (WS) | Live event stream, optional `?topic=` filter |
| `/grafana/*` | GET/POST | Grafana JSON-datasource endpoints |

## Clients

The same contract ships for all languages: one datagram per `emit`, MTU-safe truncation, heartbeat watcher with degradation, and a `local` constructor that adopts `telemetry.toml`.

- **Rust** — [`light-telemetry-client`](https://crates.io/crates/light-telemetry-client) + `light-telemetry-tracing`
- **Python** — [`light-telemetry`](https://pypi.org/project/light-telemetry/) (≥ 3.11)
- **Go** — `github.com/oldienki/telemetry/go/light-telemetry` (includes a `slog` handler)

## Image details

- **Base**: `debian:bookworm-slim`, multi-stage Rust build, `--locked` against `Cargo.lock`
- **User**: runs as non-root `light-telemetry` (uid 10001)
- **Ports**: `8080/tcp` (HTTP API, WebSocket, Grafana), `9876/udp` (telemetry ingest)
- **Tags**: `latest`, plus semver tags matching each release (e.g. `0.1.1`)

## Links

- **Source**: https://github.com/oldienki/telemetry
- **Docs**: https://github.com/oldienki/telemetry#readme
- **License**: MIT OR Apache-2.0
