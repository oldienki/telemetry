"""Tests for server-config discovery and the ``TelemetryClient.local`` ctor."""

from __future__ import annotations

from pathlib import Path

import pytest

from light_telemetry import TelemetryClient, config


@pytest.fixture()
def cfg_file(tmp_path: Path) -> Path:
    p = tmp_path / "telemetry.toml"
    p.write_text('udp_bind = "0.0.0.0:45678"\nheartbeat_ms = 200\nstorage = "sqlite"\n')
    return p


def test_load_explicit(cfg_file: Path) -> None:
    cfg, src = config.load(cfg_file)
    assert src == cfg_file
    assert cfg["udp_bind"] == "0.0.0.0:45678"
    assert cfg["heartbeat_ms"] == 200
    assert "storage" not in cfg  # server-only key ignored


def test_load_missing_explicit_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        config.load(tmp_path / "nope.toml")


@pytest.mark.parametrize(
    ("bind", "want"),
    [
        ("0.0.0.0:9876", ("127.0.0.1", 9876)),
        ("[::]:9876", ("::1", 9876)),
        ("192.168.1.5:999", ("192.168.1.5", 999)),
    ],
)
def test_localize(bind: str, want: tuple[str, int]) -> None:
    assert config.localize(bind) == want


def test_local_client_from_file(cfg_file: Path) -> None:
    c = TelemetryClient.local("h", "c", config_path=str(cfg_file))
    try:
        assert c._server_addr == ("127.0.0.1", 45678)
        assert c._heartbeat_timeout == pytest.approx(0.6)
    finally:
        c.stop()


def test_local_client_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    if Path("/etc/telemetry.toml").exists():
        pytest.skip("system config exists; cannot assert defaults")
    c = TelemetryClient.local("h", "c")
    try:
        assert c._server_addr == ("127.0.0.1", 9876)
        assert c._heartbeat_timeout == pytest.approx(3.0)
    finally:
        c.stop()


def test_default_paths_use_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    paths = config._default_paths()
    assert paths[0] == Path("/etc/telemetry.toml")
    assert paths[1] == tmp_path / ".config" / "telemetry.toml"


def test_user_config_found_via_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    d = tmp_path / ".config"
    d.mkdir()
    (d / "telemetry.toml").write_text("heartbeat_ms = 42\n")
    monkeypatch.setenv("HOME", str(tmp_path))
    if Path("/etc/telemetry.toml").exists():
        pytest.skip("/etc/telemetry.toml would shadow the user file")
    cfg, src = config.load()
    assert src == d / "telemetry.toml"
    assert cfg["heartbeat_ms"] == 42
