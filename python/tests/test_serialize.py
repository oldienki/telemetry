"""Encoding tests: golden vectors, MTU boundary, unicode."""

import json
from pathlib import Path
from typing import Any

import msgpack

from light_telemetry.serialize import MTU_SAFE, encode

SPEC = Path(__file__).resolve().parents[2] / "spec"
GOLDEN: dict[str, Any] = json.loads(
    (SPEC / "test-vectors" / "packet_sample.json").read_text()
)


def test_json_encoding_matches_golden_vector() -> None:
    data, truncated = encode(GOLDEN, use_msgpack=False)
    assert not truncated
    assert json.loads(data) == GOLDEN


def test_msgpack_encoding_matches_golden_vector() -> None:
    data, truncated = encode(GOLDEN, use_msgpack=True)
    assert not truncated
    assert msgpack.unpackb(data) == GOLDEN


def test_oversized_payload_truncated() -> None:
    packet = {**GOLDEN, "payload": {"big": "x" * 2000}}
    data, truncated = encode(packet, use_msgpack=False)
    assert truncated
    assert len(data) <= MTU_SAFE
    assert json.loads(data)["payload"]["truncated"] is True


def test_msgpack_also_truncates() -> None:
    packet = {**GOLDEN, "payload": {"big": "x" * 2000}}
    data, truncated = encode(packet, use_msgpack=True)
    assert truncated
    assert len(data) <= MTU_SAFE
    assert msgpack.unpackb(data)["payload"]["truncated"] is True


def test_exact_mtu_boundary_not_truncated() -> None:
    packet = dict(GOLDEN)
    for n in range(MTU_SAFE + 1):
        packet["payload"] = {"pad": "x" * n}
        data, truncated = encode(packet, use_msgpack=False)
        if len(data) == MTU_SAFE and not truncated:
            # One byte over the exact fit must truncate.
            packet["payload"] = {"pad": "x" * (n + 1)}
            data, truncated = encode(packet, use_msgpack=False)
            assert truncated
            assert len(data) <= MTU_SAFE
            return
    raise AssertionError("no exact-fit payload size found")


def test_unicode_payload_round_trips() -> None:
    packet = {**GOLDEN, "payload": {"msg": "héllo  🚀"}}
    data, _ = encode(packet, use_msgpack=False)
    assert json.loads(data)["payload"]["msg"] == "héllo  🚀"
    data, _ = encode(packet, use_msgpack=True)
    assert msgpack.unpackb(data)["payload"]["msg"] == "héllo  🚀"


def test_non_serializable_values_fall_back_to_str() -> None:
    packet = {**GOLDEN, "payload": {"obj": object()}}
    data, _ = encode(packet, use_msgpack=False)
    assert "obj" in json.loads(data)["payload"]
