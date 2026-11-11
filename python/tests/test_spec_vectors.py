"""Cross-language contract checks: golden vectors must satisfy spec/ schemas."""

import json
from pathlib import Path
from typing import Any

import jsonschema
import msgpack
import pytest

SPEC = Path(__file__).resolve().parents[2] / "spec"


def _load(name: str) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads((SPEC / name).read_text())
    return loaded


def test_packet_vector_validates_against_schema() -> None:
    jsonschema.validate(
        _load("test-vectors/packet_sample.json"), _load("schemas/packet.schema.json")
    )


def test_heartbeat_vector_validates_against_schema() -> None:
    jsonschema.validate(
        _load("test-vectors/heartbeat_sample.json"),
        _load("schemas/heartbeat.schema.json"),
    )


def test_packet_missing_topic_rejected() -> None:
    packet = _load("test-vectors/packet_sample.json")
    del packet["topic"]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(packet, _load("schemas/packet.schema.json"))


def test_packet_negative_ts_rejected() -> None:
    packet = _load("test-vectors/packet_sample.json")
    packet["ts"] = -1
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(packet, _load("schemas/packet.schema.json"))


def test_msgpack_vector_matches_json_vector() -> None:
    expected = _load("test-vectors/packet_sample.json")
    actual = msgpack.unpackb(
        (SPEC / "test-vectors" / "packet_sample.msgpack").read_bytes()
    )
    assert actual == expected
