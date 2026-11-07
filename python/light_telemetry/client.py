"""UDP fire-and-forget telemetry client for LightTelemetry.

Design contract (shared with the Rust and Go clients):

- ``emit`` serializes one packet and sends one datagram. No buffering, no
  retries — telemetry must never block the host.
- A background thread watches reverse heartbeats from the server. When they
  stop, ``degraded`` flips and ``emit`` drops before serializing.
- Oversized packets are truncated (``payload -> {"truncated": true}``).
"""

from __future__ import annotations

import json
import socket
import threading
import time
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Self

from .serialize import encode, now_micros


@dataclass
class ClientStats:
    """Client-side counters for diagnostics and tests."""

    encoded: int = 0
    sent: int = 0
    dropped: int = 0
    truncated: int = 0
    errors: int = 0


@dataclass
class _Watcher:
    thread: threading.Thread | None = field(default=None)
    stop: threading.Event = field(default_factory=threading.Event)


class TelemetryClient:
    """One UDP socket plus an optional heartbeat watcher thread."""

    def __init__(
        self,
        server_addr: tuple[str, int],
        host_id: str,
        host_class: str,
        *,
        use_msgpack: bool = False,
        heartbeat_timeout: float | None = 3.0,
    ) -> None:
        """Bind an ephemeral UDP socket toward ``server_addr``.

        ``heartbeat_timeout`` (seconds) is the liveness window for the
        reverse-heartbeat watcher; ``None`` disables the watcher.
        """
        self._server_addr = server_addr
        self._host_id = host_id
        self._host_class = host_class
        self._use_msgpack = use_msgpack
        self._heartbeat_timeout = heartbeat_timeout
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(("0.0.0.0", 0))
        self._degraded = threading.Event()
        self._watcher = _Watcher()
        self.stats = ClientStats()

    # -- lifecycle ------------------------------------------------------

    def start(self) -> Self:
        """Spawn the heartbeat watcher (if configured). Idempotent."""
        if self._heartbeat_timeout is None or self._watcher.thread is not None:
            return self
        self._watcher.thread = threading.Thread(
            target=self._watch,
            name="light-telemetry-heartbeat",
            daemon=True,
        )
        self._watcher.thread.start()
        return self

    def stop(self) -> None:
        """Stop the watcher and release the socket. Safe to call twice."""
        self._watcher.stop.set()
        if self._watcher.thread is not None:
            self._watcher.thread.join(timeout=2.0)
            self._watcher.thread = None
        self._sock.close()

    close = stop

    def __enter__(self) -> Self:
        return self.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()

    # -- state ----------------------------------------------------------

    @property
    def degraded(self) -> bool:
        """Whether the server is currently considered unreachable."""
        return self._degraded.is_set()

    @property
    def local_addr(self) -> tuple[str, int]:
        """Local socket address."""
        host, port = self._sock.getsockname()
        return str(host), int(port)

    # -- emission -------------------------------------------------------

    def emit(self, topic: str, payload: dict[str, Any] | None = None) -> None:
        """Emit one event. Drops silently when degraded; never raises."""
        if self.degraded:
            self.stats.dropped += 1
            return
        packet = {
            "topic": topic,
            "id": self._host_id,
            "class": self._host_class,
            "ts": now_micros(),
            "payload": payload if payload is not None else {},
        }
        self.stats.encoded += 1
        try:
            data, truncated = encode(packet, self._use_msgpack)
            if truncated:
                self.stats.truncated += 1
            self._sock.sendto(data, self._server_addr)
            self.stats.sent += 1
        except Exception:  # noqa: BLE001 — fire-and-forget: never raise to the caller
            self.stats.errors += 1

    # -- heartbeat watcher -----------------------------------------------

    def _watch(self) -> None:
        assert self._heartbeat_timeout is not None
        timeout = self._heartbeat_timeout
        read_timeout = min(max(timeout / 4.0, 0.02), 0.2)
        self._sock.settimeout(read_timeout)
        last_hb: float | None = None
        while not self._watcher.stop.is_set():
            try:
                data, addr = self._sock.recvfrom(2048)
            except TimeoutError:
                data = b""
                addr = ("", 0)
            except OSError:
                break
            if addr == self._server_addr:
                try:
                    msg = json.loads(data)
                    if isinstance(msg, dict) and msg.get("type") == "hb":
                        last_hb = time.monotonic()
                except (ValueError, UnicodeDecodeError):
                    pass
            stale = (
                (time.monotonic() - last_hb) > timeout if last_hb is not None else False
            )
            if stale:
                self._degraded.set()
            else:
                self._degraded.clear()
