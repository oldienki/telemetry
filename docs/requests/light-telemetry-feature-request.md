# Feature Request: Self-Healing Liveness Gate for light-telemetry Clients

- **Author:** Cascade agent session
- **Date:** 2026-11-09
- **Status:** proposal-sketch
- **Upstream:** `light-telemetry` (`~/Projects/telemetry`, crate `light-telemetry-client` 0.1.1)
- **Supersedes:** none
- **Superseded by:** none

## 1. Summary

The light-telemetry client's heartbeat-liveness gate can deadlock
permanently. The client drops every `emit` while `degraded`, and
`degraded` only clears when a server heartbeat arrives — but the server
only heartbeats *registered sessions*, and a session registers only when
a datagram arrives. A single >5s window of packet loss (or a client's
emission rate slower than one datagram per 5s) evicts the session, stops
the heartbeats, flips `degraded`, and the client can never re-register:
**a transient fault converts into a permanent, silent telemetry
outage.**

Request: make the gate self-healing. Options, in order of preference:

1. Client emits a low-rate keepalive datagram while `degraded` instead
   of dropping everything — the datagram re-registers the session,
   heartbeats resume, the gate clears itself.
2. Bound the `degraded` state with a maximum duration (auto-recover and
   let real emits through).
3. Add a client-initiated `{"type":"hello"}` handshake that the watcher
   sends when it declares staleness.
4. Treat `degraded` as advisory only (count it, still send) — what k4dpi
   currently approximates by disabling the watcher entirely.

## 2. Background

The feature request originates from a live failure in k4dpi integration
testing. The k4dpi router container ran a `TelemetryClient` emitting
`logger`, `dpi_flow`, `matching` and `metrics` topics to a
`light-telemetry` collector container on the same docker network.

Observed behavior:

- Telemetry flowed for ~10–30s after process start, then stopped
  permanently. The collector's periodic stats showed `sessions=1`
  briefly, then `sessions=0` forever, with `dropped=0` and
  `decode_errors=0` — packets simply ceased arriving.
- The client's socket (`0.0.0.0:<ephemeral>`) was alive and bound; the
  route to the server was clean; a hand-crafted datagram sent from the
  same container was received and stored by the server.
- Temporary instrumentation of `TelemetryClient::stats()` showed
  `degraded` flipping `false → true` ~25s after start, after which
  `dropped` climbed continuously while `encoded`/`sent`/`errors` stayed
  flat.
- A standalone probe client in the same container emitted 451 datagrams
  at 10/s and received 45 heartbeats — the server worked correctly for
  sessions that stay registered.

## 3. Problem

### 3.1 Mechanism

- **Client watcher** (`crates/client/src/heartbeat.rs::spawn_watcher`):
  reads `{"type":"hb","srv_ts":…}` datagrams on the shared emit socket,
  matching the sender against `server_addr`. Staleness:
  `last_hb = Some(t) && t.elapsed() > heartbeat_timeout → degraded = true`;
  `last_hb = None → never degraded`. `heartbeat_timeout` defaults to
  `Some(3s)` (`TelemetryConfig::new`, `crates/client/src/lib.rs`).
- **Emit gate** (`lib.rs::emit`): `if self.is_degraded() { dropped += 1;
  return; }` — dropped *before* serialization. Recovery is only possible
  when a heartbeat arrives (`stale = false` on the next watcher loop).
- **Server session registry** (`crates/server/src/session.rs`,
  `ingest.rs`): the decode task calls `sessions.touch(d.from)` for every
  received datagram. `sweeper_loop` evicts sessions idle longer than
  `session_ttl_ms` (default **5s**). `heartbeat_loop` sends a heartbeat
  to `sessions.active_addrs()` every `heartbeat_ms` (default **1s**).

### 3.2 The deadlock

```
t0      client emits normally; datagrams register the session
t1      transient fault: a few datagrams lost in transit / delayed
        (burst vs small ingest channel, ARP revalidation, rx overflow…)
t1+5s   server evicts the session (session_ttl_ms = 5s)
        → heartbeat_loop no longer targets the client
t1+8s   client watcher: last_hb elapsed > heartbeat_timeout (3s)
        → degraded = true → every emit() drops before send
forever client never emits → server never re-registers the session
        → server never heartbeats → degraded never clears
```

The gate is *self-sealing*: the only exit path (a heartbeat) is gated on
the thing the client itself stopped doing (sending datagrams).

### 3.3 Secondary issues in the same code

- **Low-rate emitters flap.** Any emitter slower than one datagram per
  `session_ttl_ms` (5s) oscillates: session evicts between emits →
  heartbeats stop → `degraded` → silence. The gate punishes exactly the
  traffic it exists to protect.
- **Asymmetric semantics.** `last_hb = None` never degrades (a client
  whose heartbeats *never* arrived is treated as healthy forever), while
  one received heartbeat followed by a 3s gap is fatal-forever. The gate
  is weakest where it should be strict and strictest where it should be
  forgiving.
- **Silent failure mode.** Emits drop without logging, so the only
  observable symptom is "telemetry stops a few seconds after start" —
  which cost significant debugging time downstream.

## 4. Proposed Solutions

### Option A — Degraded keepalive probe (preferred)

While `degraded` is set, the client emits one minimal datagram per
`heartbeat_timeout` period instead of dropping everything:

```rust
// emit(): when degraded, allow one datagram per keepalive window
if self.is_degraded() {
    if last_probe.elapsed() < self.config.heartbeat_timeout_or_default() {
        self.stats.dropped.fetch_add(1, Ordering::Relaxed);
        return;
    }
    last_probe = now;   // fall through and send a small probe packet
}
```

- The probe can be a reserved topic (`"_session"`, `{"type":"hello"}`)
  or simply the next real payload — the server `touch`es the session on
  *any* datagram before decode, so either re-registers it.
- Server heartbeats resume → watcher sees `hb` → `degraded` clears →
  normal emission restarts. Fully self-healing.
- Cost: one datagram per ~3s during an outage — negligible.
- No protocol or server change strictly required (any datagram touches
  the session); an explicit `_session` topic keeps probes out of user
  topics if the decode path would otherwise publish them as events.

### Option B — Bounded degraded window

Cap `degraded` at a maximum duration (e.g. `max_degraded = 30s`): after
the cap, clear the flag and emit normally until a heartbeat confirms
liveness again.

- Simplest possible change (one `Instant` + one compare in the watcher
  or in `emit`).
- Self-heals identically to Option A: resumed emits re-register the
  session and heartbeats resume.
- Cost: bounded bursts of real payloads into a possibly-dead sink — for
  fire-and-forget UDP this is acceptable and arguably desirable.

### Option C — Client-initiated hello handshake

On declaring staleness, the watcher (or `emit` on the degraded path)
sends `{"type":"hello","id":…,"class":…}` to `server_addr`.

- Explicit protocol-level registration rather than relying on
  `touch`-on-any-datagram; cleaner semantics if a future server wants to
  distinguish probes from data.
- Requires a small protocol addition and a server-side decision about
  whether hello datagrams are published as events (they should not be).
- More invasive than A/B; A already achieves the same outcome without a
  handshake.

### Option D — Advisory-only gate

Keep the watcher for observability but stop dropping on it: emit always
sends; `degraded` is surfaced through `stats()`/`is_degraded()` only.

- Equivalent in effect to disabling the watcher —
  `TelemetryConfig::with_heartbeat_timeout(None)`, which is what k4dpi
  now does (see §5) — but preserves the signal.
- Abandons the gate's original purpose (early-drop when the sink is
  dead); for a single-homed UDP sink that purpose is questionable
  anyway.

### Server-side complements (optional, not sufficient alone)

- Longer `session_ttl_ms`, or a "dormant" phase where recently-seen
  sessions keep receiving heartbeats for a grace period after eviction —
  reduces flap frequency but cannot fix the client gate by itself.
- Publish ingest-side counters for per-session receive rates so silent
  client black-holes are observable at the collector.

## 5. Current Workaround in k4dpi

`src/k4dpi/src/common/telemetry.rs` constructs the client with
`TelemetryConfig::with_heartbeat_timeout(None)`, which skips
`spawn_watcher` entirely — `degraded` can never flip and `emit` always
sends. This is the right posture for a single-homed fire-and-forget UDP
sink, but it loses the liveness signal and any future upstream fix in
this area would let us re-enable the watcher.

## 6. Recommendation

Implement **Option A** upstream: a keepalive probe on the degraded path.
It is a small, client-only change, preserves the gate's intent (bounded
traffic when the sink is dead — ~1 datagram per window), removes the
permanent deadlock, and automatically heals low-rate emitters. Option B
is an acceptable simpler variant; Option C is worthwhile only if the
protocol later gains explicit session management.

## 7. References

- `crates/client/src/lib.rs` — `TelemetryConfig.heartbeat_timeout`
  default `Some(3s)`; `emit()` drop gate; `ClientStats`.
- `crates/client/src/heartbeat.rs` — `spawn_watcher`, `last_hb` staleness
  logic, read-timeout sizing.
- `crates/client/src/sender.rs` — `UdpSender::send` (`send_to`).
- `crates/server/src/ingest.rs` — `heartbeat_loop`, `sweeper_loop`,
  `decode_task_sessions.touch(d.from)`, `try_enqueue`.
- `crates/server/src/config.rs` — `heartbeat_ms` default 1000,
  `session_ttl_ms` default 5000.
- `crates/server/src/session.rs` — `SessionRegistry`, `ClientSession`,
  `evict_stale`.
- k4dpi consumer side: `src/k4dpi/src/common/telemetry.rs`
  (`TelemetrySink::new`), `src/k4dpi/src/common/logging.rs`
  (`TelemetryRouter`).
