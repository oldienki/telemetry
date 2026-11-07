"""Discover the server's ``telemetry.toml`` for localhost clients.

Same search order as the server: explicit path, else
``/etc/telemetry.toml``, else ``$HOME/.config/telemetry.toml``; the first
existing file wins. Only the keys the client consumes are read —
server-only keys are ignored.
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import TypedDict

ETC_PATH = Path("/etc/telemetry.toml")
USER_PATH = Path.home() / ".config" / "telemetry.toml"

DEFAULT_BIND = ("0.0.0.0", 9876)
DEFAULT_HEARTBEAT_MS = 1000


class ServerConfig(TypedDict, total=False):
    """Server keys a client consumes."""

    udp_bind: str
    heartbeat_ms: int


def _default_paths() -> list[Path]:
    paths = [ETC_PATH]
    home = os.environ.get("HOME")
    if home:
        paths.append(Path(home) / ".config" / "telemetry.toml")
    return paths


def load(path: str | Path | None = None) -> tuple[ServerConfig, Path | None]:
    """Load the server config.

    ``path`` must exist (``FileNotFoundError`` propagates); without it the
    first existing default path wins; absent files → empty config.
    """
    if path is not None:
        p = Path(path)
        return _parse(p), p
    for cand in _default_paths():
        if cand.is_file():
            return _parse(cand), cand
    return ServerConfig(), None


def _parse(p: Path) -> ServerConfig:
    data = tomllib.loads(p.read_text())
    cfg: ServerConfig = {}
    if isinstance(v := data.get("udp_bind"), str):
        cfg["udp_bind"] = v
    if isinstance(v := data.get("heartbeat_ms"), int):
        cfg["heartbeat_ms"] = v
    return cfg


def localize(bind: str) -> tuple[str, int]:
    """Map a bind address to a send target: wildcard → loopback."""
    host, _, port = bind.rpartition(":")
    host = host.strip("[]")
    if host in ("0.0.0.0", "*"):
        return "127.0.0.1", int(port)
    if host in ("::", "::1"):
        return "::1", int(port)
    return host or "0.0.0.0", int(port)
