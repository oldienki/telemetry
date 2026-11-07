"""Client and handler tests: live UDP, degradation, handler mapping."""

import json
import logging
import socket
import threading
import time
from typing import Any

import msgpack

from light_telemetry import TelemetryClient, TelemetryLogHandler


def _recv(sock: socket.socket, timeout: float = 2.0) -> tuple[bytes, Any]:
    sock.settimeout(timeout)
    return sock.recvfrom(65535)


def _server() -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    return s


def _client(server: socket.socket) -> TelemetryClient:
    return TelemetryClient(
        server.getsockname(), "test-host", "test-class", heartbeat_timeout=None
    )


def test_emit_sends_json_datagram() -> None:
    server = _server()
    client = _client(server)
    client.emit("t", {"k": 1})
    data, _ = _recv(server)
    v = json.loads(data)
    assert v["topic"] == "t"
    assert v["id"] == "test-host"
    assert v["class"] == "test-class"
    assert v["ts"] > 0
    assert v["payload"]["k"] == 1
    assert client.stats.sent == 1
    assert client.stats.encoded == 1
    client.stop()


def test_emit_sends_msgpack_when_configured() -> None:
    server = _server()
    client = TelemetryClient(
        server.getsockname(), "h", "c", use_msgpack=True, heartbeat_timeout=None
    )
    client.emit("t", {"k": 1})
    data, _ = _recv(server)
    assert data[0:1] != b"{"
    assert msgpack.unpackb(data)["topic"] == "t"
    client.stop()


def test_emit_to_unreachable_server_does_not_raise() -> None:
    client = TelemetryClient(("127.0.0.1", 9), "h", "c", heartbeat_timeout=None)
    client.emit("t", {})
    assert client.stats.sent == 1
    client.stop()


def test_degraded_drops_before_serialization() -> None:
    server = _server()
    client = _client(server)
    client._degraded.set()
    encoded_before = client.stats.encoded
    client.emit("t", {"expensive": object()})
    assert client.stats.encoded == encoded_before
    assert client.stats.dropped == 1
    assert client.stats.sent == 0
    client.stop()


def test_socket_error_swallowed() -> None:
    client = TelemetryClient(("127.0.0.1", 9), "h", "c", heartbeat_timeout=None)
    client._sock.close()  # force sendto to fail
    client.emit("t", {})
    assert client.stats.errors == 1


def test_stop_without_start_is_safe() -> None:
    client = TelemetryClient(("127.0.0.1", 9), "h", "c")
    client.stop()
    client.stop()  # idempotent


def test_watcher_marks_degraded_when_heartbeats_stop() -> None:
    server = _server()
    server_addr = server.getsockname()
    client = TelemetryClient(server_addr, "h", "c", heartbeat_timeout=0.15).start()
    try:
        client_addr = client.local_addr
        hb = json.dumps({"type": "hb", "srv_ts": 1}).encode()

        # Heartbeats → stays healthy.
        for _ in range(4):
            server.sendto(hb, client_addr)
            time.sleep(0.04)
        time.sleep(0.08)
        assert not client.degraded

        # Silence → degraded.
        time.sleep(0.4)
        assert client.degraded

        # Resume inside the window → recovers.
        for _ in range(3):
            server.sendto(hb, client_addr)
            time.sleep(0.04)
        time.sleep(0.06)
        assert not client.degraded
    finally:
        client.stop()


def test_watcher_ignores_non_heartbeat_datagrams() -> None:
    server = _server()
    client = TelemetryClient(
        server.getsockname(), "h", "c", heartbeat_timeout=0.15
    ).start()
    try:
        server.sendto(b"random junk", client.local_addr)
        time.sleep(0.3)
        assert not client.degraded  # never saw a real hb
    finally:
        client.stop()


def test_context_manager_lifecycle() -> None:
    server = _server()
    with TelemetryClient(
        server.getsockname(), "h", "c", heartbeat_timeout=None
    ) as client:
        client.emit("t", {})
    assert client.stats.sent == 1


# -- handler --------------------------------------------------------------


def _logging_capture(server: socket.socket) -> tuple[TelemetryClient, logging.Logger]:
    client = _client(server)
    logger = logging.getLogger("telemetry.test.capture")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(TelemetryLogHandler(client, topic="logger"))
    return client, logger


def test_handler_emits_record_with_extra_fields() -> None:
    server = _server()
    client, logger = _logging_capture(server)
    logger.info("login %s", "ok", extra={"user": "alice", "answer": 42})
    data, _ = _recv(server)
    v = json.loads(data)
    assert v["topic"] == "logger"
    p = v["payload"]
    assert p["level"] == "INFO"
    assert p["logger"] == "telemetry.test.capture"
    assert p["message"] == "login ok"
    assert p["user"] == "alice"
    assert p["answer"] == 42
    client.stop()


def test_handler_without_extra() -> None:
    server = _server()
    client, logger = _logging_capture(server)
    logger.warning("plain")
    data, _ = _recv(server)
    p = json.loads(data)["payload"]
    assert p["message"] == "plain"
    assert p["level"] == "WARNING"
    client.stop()


def test_handler_includes_exc_info() -> None:
    server = _server()
    client, logger = _logging_capture(server)
    try:
        raise ValueError("boom")
    except ValueError:
        logger.exception("op failed")
    data, _ = _recv(server)
    p = json.loads(data)["payload"]
    assert "ValueError: boom" in p["exc_info"]
    client.stop()


def test_handler_survives_unreachable_server() -> None:
    client = TelemetryClient(("127.0.0.1", 9), "h", "c", heartbeat_timeout=None)
    logger = logging.getLogger("telemetry.test.dead")
    logger.handlers.clear()
    logger.addHandler(TelemetryLogHandler(client))
    logging.raiseExceptions = False  # simulate production handler behavior
    try:
        logger.error("nothing hears this")  # must not raise
    finally:
        logging.raiseExceptions = True
        client.stop()


def test_concurrent_emit_is_thread_safe_enough() -> None:
    server = _server()
    client = _client(server)

    def worker() -> None:
        for _ in range(20):
            client.emit("t", {})

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert client.stats.encoded == 80
    client.stop()
