# light-telemetry

UDP fire-and-forget telemetry client for [LightTelemetry](https://github.com/oldienki/telemetry) — a lightweight observability daemon for test environments and embedded/bare-metal hardware where OpenTelemetry is too heavy.

## Features

- **Fire-and-forget** — `emit` serializes one packet and sends one UDP datagram. No buffering, no retries: telemetry never blocks or crashes your application.
- **Degradation-aware** — a background watcher tracks reverse heartbeats from the server; when they stop, emissions drop before serialization (zero cost when the collector is gone).
- **Two wire formats** — JSON (default) or MessagePack.
- **Auto-discovery** — adopt the server's `telemetry.toml` to find the ingest endpoint and heartbeat timing automatically.
- **`logging` integration** — `TelemetryLogHandler` forwards `logging` records as telemetry events.
- **MTU-safe** — oversized packets are truncated to a marker, never fragmented.

## Install

```bash
pip install light-telemetry
# or
uv add light-telemetry
```

Requires Python ≥ 3.11.

## Quickstart

### Zero-config localhost client

Reads `telemetry.toml` (`/etc/telemetry.toml`, then `~/.config/telemetry.toml`)
written by the server — target address and heartbeat timing come from the
daemon itself. Falls back to `127.0.0.1:9876` if no file exists.

```python
from light_telemetry import TelemetryClient

client = TelemetryClient.local("my-app", "service")
client.start()  # spawns the heartbeat watcher
client.emit("test.start", {"case": "can_tx", "rig": 3})
client.stop()
```

### Explicit endpoint

```python
from light_telemetry import TelemetryClient

with TelemetryClient(
    ("192.168.1.10", 9876),
    host_id="bench-01",
    host_class="rig",
    use_msgpack=True,  # MessagePack wire format
    heartbeat_timeout=3.0,  # s; None disables the watcher
) as client:
    client.emit("firmware.flash", {"target": "stm32", "bytes": 65536})
```

### `logging` integration

```python
import logging
from light_telemetry import TelemetryClient, TelemetryLogHandler

client = TelemetryClient.local("my-app", "service").start()
logging.getLogger().addHandler(TelemetryLogHandler(client, topic="logger"))

log = logging.getLogger("sut")
log.info("device under test ready", extra={"fw": "1.4.2"})
# every record emits: {"level", "logger", "message", + extra fields}
```

## Server

Events land in the [`light-telemetry`](https://github.com/oldienki/telemetry) daemon, which exposes REST queries, a wait-subscription API, a WebSocket stream, and a Grafana-compatible JSON adapter. Storage backends: in-memory, SQLite, PostgreSQL.

## Links

- **Repository / full monorepo** — https://github.com/oldienki/telemetry
- **Rust client** — [`light-telemetry-client`](https://crates.io/crates/light-telemetry-client) on crates.io
- **Go client** — `github.com/oldienki/telemetry/go/light-telemetry`

## License

MIT OR Apache-2.0
