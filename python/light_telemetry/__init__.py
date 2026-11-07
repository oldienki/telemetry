"""UDP fire-and-forget telemetry client for LightTelemetry."""

from . import config
from .client import ClientStats, TelemetryClient
from .handler import TelemetryLogHandler
from .serialize import MTU_SAFE, encode, now_micros

__version__ = "0.1.0"

__all__ = [
    "MTU_SAFE",
    "ClientStats",
    "TelemetryClient",
    "TelemetryLogHandler",
    "config",
    "encode",
    "now_micros",
]
