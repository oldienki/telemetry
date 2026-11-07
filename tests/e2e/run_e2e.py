#!/usr/bin/env python3
"""LightTelemetry end-to-end harness.

Builds the Rust server, starts it with the memory engine on ephemeral
ports, runs one emitter per language client (Rust / Python / Go), and
asserts each event arrives via `POST /api/v1/wait`. Also smoke-checks
the Grafana adapter, the wait-timeout path, and the client degrade path.

Usage:
    python3 tests/e2e/run_e2e.py [--only rust,python,go] [--keep]

Exit code 0 on success, 1 on any failed check, 2 on infrastructure errors.
Requires: cargo, python3 (uv preferred for the Python client env), go.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
SERVER_BIN = ROOT / "target" / "debug" / "light-telemetry"

PASS = "PASS"
FAIL = "FAIL"
results: list[tuple[str, str, str]] = []


def report(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, PASS if ok else FAIL, detail))
    print(f"  [{PASS if ok else FAIL}] {name} {detail}")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http_json(url: str, body: dict | None = None, timeout: float = 8.0) -> tuple[int, Any]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"} if body else {},
        method="POST" if body is not None else "GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, None


def wait_ready(http: str, deadline: float = 10.0) -> bool:
    end = time.time() + deadline
    while time.time() < end:
        status, _ = http_json(f"{http}/api/v1/time", timeout=1.0)
        if status == 200:
            return True
        time.sleep(0.1)
    return False


def run(cmd: list[str], cwd: Path | None = None, env: dict | None = None) -> tuple[int, str]:
    p = subprocess.run(
        cmd, cwd=cwd or ROOT, env=env or os.environ.copy(),
        capture_output=True, text=True, timeout=120,
    )
    return p.returncode, (p.stdout + p.stderr).strip()


def emit_cmd(kind: str, udp_addr: str, topic: str) -> tuple[list[str], Path | None]:
    if kind == "rust":
        return (
            ["cargo", "run", "-q", "-p", "light-telemetry-client",
             "--example", "e2e_emit", "--", udp_addr, topic, "e2e-rust"],
            ROOT,
        )
    if kind == "python":
        script = str(ROOT / "tests" / "e2e" / "emit_py.py")
        for cmd in (
            ["uv", "run", "--project", str(ROOT / "python"), "python", script],
            [sys.executable, script],
        ):
            rc, _ = run(cmd[:1] + ["--version"] if cmd[0] == "uv" else cmd[:1] + ["--version"])
            if rc == 0:
                return (
                    cmd + [udp_addr, topic, "e2e-python"],
                    ROOT,
                )
        return (["false"], None)
    if kind == "go":
        return (
            ["go", "run", "./cmd/e2e-emit", udp_addr, topic, "e2e-go"],
            ROOT / "go" / "light-telemetry",
        )
    raise ValueError(kind)


def wait_for_event(http: str, topic: str, timeout_ms: int = 5000) -> tuple[int, Any]:
    """Blocking `POST /api/v1/wait` for `topic`; run in a thread."""
    now_us = time.time_ns() // 1000
    return http_json(
        f"{http}/api/v1/wait",
        {"since_ts": now_us, "timeout_ms": timeout_ms, "filter": {"topic": topic}},
        timeout=(timeout_ms / 1000) + 3,
    )


def check_emitter(kind: str, http: str, udp_addr: str) -> None:
    topic = f"e2e.{kind}"
    holder: dict[str, Any] = {}

    def waiter() -> None:
        holder["res"] = wait_for_event(http, topic)

    t = threading.Thread(target=waiter, daemon=True)
    t.start()
    time.sleep(0.3)  # ensure the waiter is registered before emitting
    cmd, cwd = emit_cmd(kind, udp_addr, topic)
    rc, out = run(cmd, cwd=cwd)
    t.join(timeout=10)
    status, body = holder.get("res", (0, None))
    ok = rc == 0 and status == 200 and isinstance(body, dict) and body.get("topic") == topic
    report(f"emitter:{kind}", ok, f"rc={rc} http={status} {out[:120]}")


def check_wait_timeout(http: str) -> None:
    status, _ = wait_for_event(http, "e2e.never.arrives", timeout_ms=400)
    report("wait-timeout-408", status == 408, f"http={status}")


def check_grafana(http: str) -> None:
    status, body = http_json(
        f"{http}/grafana/query",
        {"targets": [{"target": "events"}]},
    )
    ok = status == 200 and isinstance(body, list) and body and body[0].get("rows")
    report("grafana-query", bool(ok), f"http={status}")


def check_degrade() -> None:
    """Fake server: reply with heartbeats to the first datagram, then go
    silent — the client must flip to degraded."""
    fake = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    fake.bind(("127.0.0.1", 0))
    fake_addr = f"127.0.0.1:{fake.getsockname()[1]}"

    def fake_server() -> None:
        try:
            fake.settimeout(3.0)
            _, client = fake.recvfrom(2048)
            hb = json.dumps({"type": "hb", "srv_ts": 1}).encode()
            for _ in range(3):
                fake.sendto(hb, client)
                time.sleep(0.03)
        except socket.timeout:
            pass

    threading.Thread(target=fake_server, daemon=True).start()
    script = str(ROOT / "tests" / "e2e" / "emit_py.py")
    cmd = ["uv", "run", "--project", str(ROOT / "python"), "python", script,
           fake_addr, "e2e.degrade", "e2e-python", "--expect-degrade"]
    rc, out = run(cmd, cwd=ROOT)
    report("client-degrade", rc == 0, f"rc={rc} {out[:120]}")
    fake.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--only", help="comma-separated emitters: rust,python,go")
    ap.add_argument("--skip-degrade", action="store_true")
    ap.add_argument("--keep", action="store_true", help="leave the server running")
    args = ap.parse_args()
    kinds = args.only.split(",") if args.only else ["rust", "python", "go"]

    print("== build server ==")
    rc, out = run(["cargo", "build", "-q", "-p", "light-telemetry"])
    if rc != 0 or not SERVER_BIN.exists():
        print(out)
        return 2
    print(f"  {SERVER_BIN}")

    udp_addr = f"127.0.0.1:{free_port()}"
    http_port = free_port()
    http = f"http://127.0.0.1:{http_port}"
    print(f"== start server (udp={udp_addr} http={http}) ==")
    server = subprocess.Popen(
        [
            str(SERVER_BIN),
            "--udp-bind", udp_addr,
            "--http-bind", f"127.0.0.1:{http_port}",
            "--storage", "memory",
        ],
        cwd=ROOT,
    )
    try:
        if not wait_ready(http):
            print("server did not become ready")
            return 2
        for kind in kinds:
            check_emitter(kind, http, udp_addr)
        check_wait_timeout(http)
        check_grafana(http)
    finally:
        if not args.keep:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()

    if not args.skip_degrade:
        check_degrade()

    print("== summary ==")
    failed = [r for r in results if r[1] == FAIL]
    for name, status, detail in results:
        print(f"  {status}  {name} {detail}")
    print(f"{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
