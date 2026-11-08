# light-telemetry-client

UDP fire-and-forget client library for [LightTelemetry](https://github.com/oldienki/telemetry) — lightweight telemetry for test environments (embedded rigs, hardware-in-the-loop benches, firmware CI) where OpenTelemetry is too heavy to deploy.

## Responsibility

This crate is the producer side of the LightTelemetry protocol. It:

- Serializes a [`TelemetryPacket`](serialize::TelemetryPacket) (`{topic, id, class, ts, payload}`) into **one UDP datagram** per `emit` call — JSON or MessagePack.
- Keeps packets **MTU-safe**: payloads that would exceed `MTU_SAFE` (1 432 bytes) are replaced with `{"truncated": true}` so no datagram is ever fragmented.
- Watches **reverse heartbeats** broadcast by the server (`{"type":"hb","srv_ts":…}`) and flips a `degraded` flag when they stop; while degraded, `emit` drops *before* paying serialization cost.
- Discovers the local daemon's `telemetry.toml` so a localhost client needs **zero configuration** — it inherits `udp_bind` and `heartbeat_ms` automatically.

The design contract: telemetry must **never block, retry, or crash the host application**. No buffering, no queues, no async runtime required.

## Architecture

```
TelemetryClient
├── sender::UdpSender      one UdpSocket bound to an ephemeral port,
│                          connected-by-address to the ingest server
├── serialize              TelemetryPacket + encode() → JSON | MessagePack,
│                          MTU truncation to {"truncated": true}
├── heartbeat::watcher     background thread recv_from() on the SAME socket;
│                          filters to server_addr, tracks last "hb" datagram,
│                          flips shared AtomicBool `degraded` on timeout
└── config                 parses the daemon's telemetry.toml (udp_bind,
                           heartbeat_ms) so `local()` is zero-config
```

- **Single socket, dual role.** The watcher receives on the same ephemeral port the sender transmits from — the server learns the client's address from the first datagram and heartbeats back to it. No extra port to open.
- **Drop-Latest upstream.** When `degraded` is set, `emit` increments `dropped` and returns without touching the serializer. The host keeps running at full speed while the collector is down; it resumes automatically when heartbeats return.
- **Fail-safe discovery.** `TelemetryConfig::local()` never fails: missing files, unparseable keys, or wildcard bind addresses (`0.0.0.0`) degrade to `127.0.0.1:9876` / 3 s liveness — worst case the client just sends into the void.

The wire format is pinned by golden test vectors in `spec/test-vectors/`, shared with the Go and Python clients.

## Usage

```toml
[dependencies]
light-telemetry-client = "0.1"
serde_json = "1"
```

### Minimal — explicit server address

```rust
use light_telemetry_client::{TelemetryClient, TelemetryConfig};
use serde_json::json;

let cfg = TelemetryConfig::new("127.0.0.1:9876".parse()?, "rig-01", "bench");
let client = TelemetryClient::new(cfg)?;

client.emit("test.start", json!({"case": "can_tx", "fw": "1.4.2"}));
```

### Zero-config localhost

Adopts the daemon's own `telemetry.toml` (same search order: `/etc/telemetry.toml` → `$HOME/.config/telemetry.toml`):

```rust
let cfg = TelemetryConfig::local("rig-01", "bench");
let client = TelemetryClient::new(cfg)?;
client.emit("boot", json!({"stage": "init"}));
```

### Options

```rust
let cfg = TelemetryConfig::new(addr, "rig-01", "bench")
    .with_msgpack(true)                          // MessagePack instead of JSON
    .with_heartbeat_timeout(Some(Duration::from_secs(5))); // or None to disable
```

### Health & diagnostics

```rust
if client.is_degraded() { /* server heartbeats stopped */ }

let s = client.stats();
// s.sent, s.dropped, s.truncated, s.errors, s.encoded — AtomicU64 counters
```

## API surface

- **`TelemetryConfig`** — `new` / `local` / `local_from` constructors, `with_msgpack`, `with_heartbeat_timeout`.
- **`TelemetryClient`** — `new`, `emit(topic, payload)`, `is_degraded`, `stats`, `local_addr`. `Drop` joins the watcher thread.
- **`ClientStats`** — `encoded`, `sent`, `dropped`, `truncated`, `errors` (`AtomicU64`).
- **`serialize` module** — `TelemetryPacket`, `encode()`, `now_micros()`, `MTU_SAFE` for building packets without a client.
- **`config` module** — `ServerConfig`, `load()`, `localize()` for reading the daemon's config file directly.

## Companion crate

[`light-telemetry-tracing`](https://crates.io/crates/light-telemetry-tracing) forwards `tracing::Event`s through this client as a `tracing_subscriber::Layer`.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
