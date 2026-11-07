"""Wire packet encoding (JSON or MessagePack) with MTU truncation."""

from __future__ import annotations

import json
import time
from typing import Any, cast

import msgpack

#: Maximum wire size before the payload is replaced by a truncation marker.
MTU_SAFE = 1432


def now_micros() -> int:
    """Current UNIX epoch time in microseconds."""
    return time.time_ns() // 1000


def _dumps(packet: dict[str, Any], use_msgpack: bool) -> bytes:
    if use_msgpack:
        packed = msgpack.packb(packet, default=repr)
        return cast(bytes, packed)
    return json.dumps(packet, separators=(",", ":"), default=str).encode()


def encode(packet: dict[str, Any], use_msgpack: bool) -> tuple[bytes, bool]:
    """Encode ``packet``; truncate payload over :data:`MTU_SAFE`.

    Returns ``(wire_bytes, was_truncated)``.
    """
    data = _dumps(packet, use_msgpack)
    if len(data) <= MTU_SAFE:
        return data, False
    truncated = {**packet, "payload": {"truncated": True}}
    data = _dumps(truncated, use_msgpack)
    return data, True
