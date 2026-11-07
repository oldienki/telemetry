# Technical Design Document: Lightweight Test Telemetry Service (LightTelemetry)

This system is designed to aggregate events and metrics within automated testing environments. The architecture is optimized for minimal overhead on tested services (UDP fire-and-forget), sub-millisecond event delivery to tests via a reactive Assertion API, and direct visualization in Grafana without background indexing delays.

## 1. System Architecture

```text
+-----------------------------------------------------------------------------------+
|                              TESTED ENVIRONMENT                                   |
|                                                                                   |
|  +--------------------+   +--------------------+   +---------------------------+  |
|  |     Go Service     |   |   Python Worker    |   |       Rust Engine         |  |
|  | [slog / zap layer] |   | [logging.Handler]  |   | [tracing-subscriber Layer]|  |
|  +---------+----------+   +---------+----------+   +-------------+-------------+  |
|            |                        |                            |                |
|            +-------------------+    |    +-----------------------+                |
|                                |    |    |                                        |
|                     UDP Ingest |    |    | UDP Heartbeat                          |
|                     (Datagrams)|    |    | (Keep-alive ping)                      |
|                                v    v    v                                        |
+--------------------------------+----+----+----------------------------------------+
                                 |    |    ^
                                 v    v    |
+--------------------------------+----+----+----------------------------------------+
|                      LIGHTTELEMETRY DAEMON (RUST)                                 |
|                                                                                   |
|  +-----------------------------------------------------------------------------+  |
|  |                           UDP Ingest Engine                                 |  |
|  | - Non-blocking Socket Pool (SO_REUSEPORT)                                   |  |
|  | - Protocol Auto-detect (JSON / MessagePack)                                 |  |
|  | - Client Session Registry (IP:Port -> Heartbeat Loop)                       |  |
|  +-------------------------------------+---------------------------------------+  |
|                                        |                                          |
|                                        v                                          |
|  +-----------------------------------------------------------------------------+  |
|  |                         Internal Event Bus                                  |  |
|  |  tokio::sync::broadcast (Fan-out to reactive subscribers & storage pipeline)|  |
|  +-------------------+---------------------------------+-----------------------+  |
|                      |                                 |                          |
|                      v                                 v                          |
|  +-----------------------------------+   +-------------------------------------+  |
|  |         Storage Pipeline          |   |       Reactive Assertion Engine     |  |
|  |  - In-Memory Ring Buffer          |   |  - Predicate Matcher                |  |
|  |  - SQLite (WAL Mode) / PostgreSQL |   |  - Timeout & Future Resolvers       |  |
|  +-------------------+---------------+   +------------------+------------------+  |
|                      |                                      |                     |
|                      +-------------------+  +---------------+                     |
|                                          |  |                                     |
|                                          v  v                                     |
|  +-----------------------------------------------------------------------------+  |
|  |                         HTTP / WebSocket API (Axum)                         |  |
|  |  - REST Query Endpoints (/api/v1/events, /api/v1/time)                      |  |
|  |  - Event-Driven Wait API (POST /api/v1/wait, WebSocket Stream)              |  |
|  |  - Grafana Data Source Compatibility (SimpleJSON / Infinity)                |  |
|  +-------------------------------------+---------------------------------------+  |
+----------------------------------------+------------------------------------------+
                                         |
                                         v
                         +-------------------------------+
                         | Test Runners (pytest / cargo) |
                         | Grafana Dashboards            |
                         +-------------------------------+

```

---

## 2. Project Structure & Monorepo Organization

The project utilizes a **Polyglot Monorepo** approach. It maintains a single source of truth for protocol specifications while using native package managers for each language stack, orchestrated by a unified task runner (`just`).

```text
light-telemetry/
├── .github/
│   └── workflows/
│       ├── ci-server.yml         # Server build and tests (Rust)
│       ├── ci-clients.yml        # Matrix tests for clients (Py, Go, Rust)
│       └── ci-e2e.yml            # End-to-end integration tests
├── spec/                         # Single Source of Truth for protocols
│   ├── schemas/                  # JSON Schemas for payload validation
│   │   ├── packet.schema.json
│   │   └── heartbeat.schema.json
│   └── test-vectors/             # Golden files for serialization tests
│       ├── packet_sample.json
│       └── packet_sample.msgpack
├── server/                       # Core Rust daemon
│   ├── Cargo.toml
│   └── src/
├── clients/
│   ├── rust/                     # Rust client library
│   │   ├── client/               # Base UDP client (Cargo.toml)
│   │   └── tracing-subscriber/   # Tracing integration layer (Cargo.toml)
│   ├── python/                   # Python client + logging.Handler
│   │   ├── pyproject.toml        # Uses uv / hatchling
│   │   ├── src/light_telemetry/
│   │   └── tests/
│   └── go/                       # Go client + slog.Handler
│       ├── go.mod                # Module: github.com/org/repo/clients/go
│       └── telemetry.go
├── tests/
│   └── e2e/                      # Cross-language integration tests
│       ├── docker-compose.yml
│       └── run_e2e.py            # Orchestrates server boot + multi-client ingestion
├── Cargo.toml                    # Root Cargo Workspace (includes server & rust clients)
├── Cargo.lock
└── Justfile                      # Command orchestrator (build, test, lint)

```

**Synchronization Strategy:**
To prevent serialization drift (e.g., Python sending seconds as floats while Go expects integer microseconds), unit tests in all languages must validate their outputs against the JSON Schemas and binary golden files located in `spec/`.

---

## 3. Network Protocol Specification (UDP)

### 3.1. Telemetry Packet Format

The server supports two formats: **Text-based JSON** (for debugging and dynamic languages) and **MessagePack (Binary JSON)** (for minimal network overhead). The format is auto-detected via the first byte: `0x7B` (`{`) indicates JSON, while bytes in MessagePack ranges (`0x80..=0x8F`, `0xDE`, `0xDF`) indicate binary.

**JSON Payload Schema:**

```json
{
  "topic": "db.query",
  "id": "worker-node-04",
  "class": "storage",
  "ts": 1772874900123456,
  "payload": {
    "query": "SELECT * FROM users WHERE id = $1",
    "duration_ms": 1.42,
    "status": "ok"
  }
}

```

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `topic` | `string` | Yes | Logical event channel (e.g., `logger`, `http.request`, `db.query`). |
| `id` | `string` | Yes | Unique source/instance identifier (e.g., UUID, hostname). |
| `class` | `string` | Yes | Source classifier or entity group (e.g., `backend`, `kernel`). |
| `ts` | `uint64` | Yes | Event generation timestamp in **microseconds** (UNIX epoch). |
| `payload` | `object` | No | Arbitrary set of structured attributes. |

*Size Constraint:* Maximum datagram size is strictly capped at **65,507 bytes**. The recommended MTU-safe size is under **1,432 bytes** to prevent IP fragmentation.

### 3.2. Reverse Heartbeat Mechanism

To prevent hosts from sending telemetry into a "dead" socket and wasting CPU cycles, the server implements a session ping mechanism.

```text
Client                                     Server
  |                                          |
  |--- Datagram (Event / Client Init) ------>| (Registers SocketAddr)
  |                                          |
  |<-- Heartbeat (every N ms) ---------------| {"type":"hb","srv_ts":1772874900200000}
  |                                          |
  |--- Datagram ---------------------------->| (Updates last_seen_at)
  |                                          |
  x    (No telemetry received > TTL)         |
  .                                          |
  .                                          | (Evicts SocketAddr from registry)

```

1. **Registration:** Upon receiving the first datagram from a client, the server registers the session in a `DashMap<SocketAddr, ClientSession>`.
2. **Heartbeat Loop:** A background worker broadcasts a heartbeat payload every $T_{\text{hb}}$ (default `1000ms`) to all active `SocketAddr`s.
3. **Session Eviction:** If no messages arrive from a client for $T_{\text{ttl}}$ (default `5000ms`), the host is evicted from the heartbeat broadcast pool.
4. **Client Degradation:** The client maintains an atomic flag. If it misses heartbeats for $2 \times T_{\text{hb}}$, it enters a degraded state, immediately dropping new events without serializing them, reducing CPU overhead to zero.

---

## 4. Server Architecture (Rust)

### 4.1. Core Traits and Storage

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryEvent {
    pub topic: String,
    pub id: String,
    pub class: String,
    pub ts: u64, // Microseconds
    #[serde(default)]
    pub payload: Value,
    #[serde(skip_deserializing)]
    pub ingress_ts: u64, // Server-side reception timestamp
}

#[async_trait::async_trait]
pub trait StorageEngine: Send + Sync {
    async fn append(&self, event: &TelemetryEvent) -> anyhow::Result<()>;
    async fn query(&self, filter: &QueryFilter) -> anyhow::Result<Vec<TelemetryEvent>>;
    async fn cleanup(&self, older_than_ts: u64) -> anyhow::Result<usize>;
}

```

### 4.2. Storage Engines

1. **In-Memory Ring Buffer:** A fixed-capacity `VecDeque<TelemetryEvent>` guarded by a `parking_lot::RwLock`. Offers $O(1)$ eviction and fast binary searches bounded by `ingress_ts`.
2. **SQLite (Integration testing mode):** Configured with `PRAGMA journal_mode = WAL`, optimizing for high-throughput single-node writes.
3. **PostgreSQL:** Native `JSONB` support with GIN indexes, utilized for distributed test suites running on Kubernetes clusters.

---

## 5. Server API Specification

### 5.1. Coordination API

**`GET /api/v1/time`**
Fetches the server's current timestamp. Used by test scripts to establish a baseline before triggering actions.
*Response:* `{"server_ts": 1772874900543210}`

### 5.2. REST Query API

**`GET /api/v1/events`**
Fetches events based on compound filters.

* **Query Parameters:** `topic`, `class`, `id`, `since`, `until`, `limit`.
* **Response:** Returns a JSON array of `TelemetryEvent` objects.

### 5.3. Reactive Assertion API (Test Wait API)

Eliminates race conditions and `sleep()` calls in tests by blocking the HTTP request until a matching event arrives.

**`POST /api/v1/wait`**
*Request Body:*

```json
{
  "since_ts": 1772874900000000,
  "timeout_ms": 3000,
  "filter": {
    "topic": "logger",
    "class": "backend",
    "payload_match": { "level": "ERROR", "code": "AUTH_FAILED" }
  }
}

```

**Resolution Logic:**

1. Checks the ring buffer for existing events in the `[since_ts, now]` window. If found, returns `200 OK` instantly.
2. If not found, registers a oneshot channel with the reactive dispatcher and awaits.
3. As new UDP packets arrive, the dispatcher evaluates the predicate. If a match occurs, the request resolves immediately with the event payload.
4. If `timeout_ms` elapses, returns `408 Request Timeout`.

### 5.4. WebSocket API & Grafana

* **`ws://<host>/api/v1/stream`:** Allows continuous streaming of events (subscribe) or TCP-based ingestion for firewall-restricted environments.
* **Grafana Adapters (`/grafana/search`, `/grafana/query`):** Implements the SimpleJSON / Infinity datasource protocol to render time-series metrics directly from the in-memory buffer without requiring an external database.

---

## 6. Client Libraries Architecture

All clients share core principles: **Zero-Allocation on dropped states** (if heartbeat is lost, string formatting and serialization are bypassed entirely), **Background Async/Thread workers**, and **Lock-free Ring Buffers** for dispatching.

### 6.1. Rust Client (`tracing` Integration)

```rust
use light_telemetry_client::{TelemetryClient, TelemetryConfig};
use light_telemetry_tracing::TelemetryLayer;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = TelemetryClient::new(TelemetryConfig {
        server_addr: "127.0.0.1:9876".parse()?,
        host_id: "auth-srv-1".into(),
        host_class: "backend".into(),
        use_msgpack: true,
    })?;

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(TelemetryLayer::new(client).with_topic("logger"))
        .init();

    tracing::info!(user_id = 42, "User successfully authenticated");
    Ok(())
}

```

### 6.2. Python Client (`logging` Integration)

```python
import logging
from light_telemetry import TelemetryClient, TelemetryLogHandler

telemetry = TelemetryClient(("127.0.0.1", 9876), host_id="py-worker", host_class="worker")
telemetry.start()

logger = logging.getLogger("app")
logger.addHandler(TelemetryLogHandler(telemetry, topic="logger"))

logger.info("Processing transaction", extra={"tx_id": "tx-9901", "amount": 250.0})

```

### 6.3. Go Client (`slog` Integration)

```go
package main

import (
	"log/slog"
	"github.com/company/light-telemetry/clients/go/telemetry"
)

func main() {
	client, _ := telemetry.NewClient(telemetry.Config{
		ServerAddr: "127.0.0.1:9876",
		HostID:     "order-processor-1",
		HostClass:  "backend",
	})
	defer client.Close()

	logger := slog.New(telemetry.NewSlogHandler(client, "logger"))
	slog.SetDefault(logger)

	slog.Info("Order created", slog.String("order_id", "ord-12345"))
}

```

---

## 7. CI/CD & Automation

Tasks are orchestrated via `just` to abstract away ecosystem-specific commands (Cargo, UV, Go modules).

**`Justfile` Core Commands:**

```bash
install-deps:
    cargo check
    cd clients/python && uv sync
    cd clients/go && go mod download

test-units:
    cargo test --workspace
    cd clients/go && go test -v ./...
    cd clients/python && uv run pytest

test-e2e:
    cargo build -p light-telemetry-server
    uv run python3 tests/e2e/run_e2e.py

```

**GitHub Actions Workflow Strategy:**
To optimize pipeline speed in the monorepo, builds are isolated by path triggers.

```yaml
# .github/workflows/ci-clients.yml
name: Clients CI
on:
  push:
    paths:
      - 'clients/**'
      - 'spec/**'

jobs:
  test-go:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.22' }
      - run: cd clients/go && go test -v ./...

  test-python:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v2
      - run: cd clients/python && uv run pytest

```

---

## 8. Edge Cases, Reliability, and Performance

* **UDP Drop and MTU Limitations:**
If a serialized datagram exceeds Ethernet MTU (~1500 bytes), IP-level fragmentation occurs, increasing the chance of packet loss. Clients pre-check buffer lengths: if a payload exceeds 1400 bytes, the `payload` is either truncated with a `"truncated": true` flag, or the client falls back to the WebSocket API.
* **Backpressure Handling:**
The server's UDP ingest worker pushes raw bytes into a bounded queue (`tokio::sync::mpsc::channel`). If backend storage (e.g., PostgreSQL) lags, the channel reaches capacity. The socket worker will intentionally drop new packets (Drop-Latest policy) and increment a `dropped_packets_total` metric, ensuring the server process never runs out of memory or blocks the socket thread.
* **Clock Skew Mitigation:**
Containerized microservices often experience clock drift. The server records both `ts` (client creation time) and `ingress_ts` (server reception time). The reactive Wait API relies strictly on `ingress_ts` by default, fully negating multi-machine clock skew issues during assertions.

