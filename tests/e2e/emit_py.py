#!/usr/bin/env python3
"""E2E emitter for the Python client.

Usage:
    emit_py.py <addr> <topic> <id>               emit one event, exit 0
    emit_py.py <addr> <topic> <id> --expect-degrade
        Emit one probe event (wakes the fake heartbeat server), wait for
        the liveness window to lapse, then exit 0 iff the client degraded.

Run via the project venv: `uv run --project python python tests/e2e/emit_py.py`
"""

from __future__ import annotations

import sys
import time

from light_telemetry import TelemetryClient


def main() -> int:
    args = sys.argv[1:]
    expect_degrade = "--expect-degrade" in args
    args = [a for a in args if a != "--expect-degrade"]
    if len(args) != 3:
        print("usage: emit_py.py <addr> <topic> <id> [--expect-degrade]", file=sys.stderr)
        return 2
    host, _, port = args[0].rpartition(":")
    client = TelemetryClient(
        (host, int(port)), args[2], "e2e", heartbeat_timeout=0.3
    ).start()
    try:
        if expect_degrade:
            client.emit(args[1], {"probe": True})
            # Fake server replies with a few heartbeats then goes silent;
            # the 0.3 s liveness window lapses within this sleep.
            time.sleep(0.8)
            ok = client.degraded
            print(f"degraded={str(ok).lower()}")
            return 0 if ok else 1
        client.emit(args[1], {"lang": "python"})
        print("emitted")
        return 0
    finally:
        client.stop()


if __name__ == "__main__":
    sys.exit(main())
