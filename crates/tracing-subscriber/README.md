# light-telemetry-tracing

A [`tracing_subscriber::Layer`](https://docs.rs/tracing-subscriber) that forwards `tracing` events to a [LightTelemetry](https://github.com/oldienki/telemetry) collector over UDP, via [`light-telemetry-client`](https://crates.io/crates/light-telemetry-client).

## Responsibility

`TelemetryLayer` bridges the `tracing` ecosystem and LightTelemetry: every `tracing::Event` observed by the layer is converted into a telemetry packet and emitted as one UDP datagram. Structured event fields become JSON payload keys, so `tracing::info!(answer = 42, user = "alice", "login succeeded")` arrives at the collector as structured data — not a formatted string.

## Architecture

```
tracing::info!(user = "alice", "login succeeded")
        │
        ▼  Layer::on_event (sync, on the calling thread)
┌─────────────────────┐
│   JsonVisitor       │  implements tracing::field::Visit
│  record_{str,bool,  │  each field → JSON object entry
│  i64,u64,f64,debug} │  Debug-only values render as strings
└─────────┬───────────┘
          ▼  + "level" ("INFO") + "target" ("my_crate::mod")
   payload: {"user":"alice","message":"login succeeded",
             "level":"INFO","target":"app"}
          │
          ▼
  TelemetryClient::emit(topic, payload)   // topic: "logger" (configurable)
          │
          ▼  one UDP datagram → light-telemetry daemon
```

Design properties:

- **Zero async.** The layer is a plain `Layer` impl — it runs on the caller's thread and returns immediately after one `send_to` syscall. No runtime, no worker, no flush step.
- **Never blocks the host.** It inherits everything the client guarantees: no buffering, no retries, drop-before-serialize while the collector's heartbeats are missing, MTU-safe truncation. Logging into a dead collector costs nothing.
- **Fields are data.** `record_i64/u64/f64/bool/str` keep native JSON types; everything else falls back to `record_debug` as a string. `level` and `target` are injected from event metadata.
- **`class`/`id` come from the client config** (e.g. `"rig-01"` / `"bench"`), so one process emits a consistent identity; only `topic` is per-layer (default `logger`, overridable via `with_topic`).

## Usage

```toml
[dependencies]
light-telemetry-tracing = "0.1"
light-telemetry-client = "0.1"
tracing = "0.1"
tracing-subscriber = "0.3"
```

```rust
use light_telemetry_client::{TelemetryClient, TelemetryConfig};
use light_telemetry_tracing::TelemetryLayer;
use std::sync::Arc;
use tracing_subscriber::prelude::*;

// Zero-config: adopts the local daemon's telemetry.toml.
let client = Arc::new(TelemetryClient::new(
    TelemetryConfig::local("rig-01", "bench"),
)?);

tracing_subscriber::registry()
    .with(TelemetryLayer::new(client).with_topic("logger"))
    .init();

tracing::info!(case = "can_tx", "test started");
// → {"topic":"logger","id":"rig-01","class":"bench","ts":…,
//    "payload":{"case":"can_tx","message":"test started",
//               "level":"INFO","target":"app"}}
```

### Alongside `fmt`

Compose with other layers normally — telemetry on, console unchanged:

```rust
tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer())
    .with(TelemetryLayer::new(client))
    .init();
```

### Filtering

Add an `EnvFilter` or per-layer filter as usual (`tracing_subscriber::filter`) if you only want to ship certain targets or levels.

## API surface

- **`TelemetryLayer`** — the only public type.
  - `TelemetryLayer::new(client: Arc<TelemetryClient>)` — wrap a client; topic defaults to `"logger"`.
  - `.with_topic(name)` — override the emitted topic (e.g. `"audit"`).

The layer requires the subscriber to support span lookup (`S: Subscriber + for<'a> LookupSpan<'a>`) — satisfied by `tracing_subscriber::registry()`. Spans are not emitted; only events.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
