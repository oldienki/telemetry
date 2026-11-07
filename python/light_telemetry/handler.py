"""``logging.Handler`` that forwards records to a ``TelemetryClient``."""

from __future__ import annotations

import logging
from typing import Any

from .client import TelemetryClient

#: Attributes present on every LogRecord; everything else is user ``extra``.
_RESERVED = frozenset(vars(logging.makeLogRecord({}))) | {
    "taskName",
    "message",
    "asctime",
}


class TelemetryLogHandler(logging.Handler):
    """Emit each ``LogRecord`` as a telemetry packet under ``topic``.

    The payload carries ``level``, ``logger``, ``message``, plus every
    non-reserved record attribute (``extra={...}`` fields). Handler
    failures are reported via ``handleError`` — never raised.
    """

    def __init__(self, client: TelemetryClient, topic: str = "logger") -> None:
        super().__init__()
        self._client = client
        self._topic = topic

    def emit(self, record: logging.LogRecord) -> None:
        try:
            payload: dict[str, Any] = {
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            for key, value in record.__dict__.items():
                if key not in _RESERVED and not key.startswith("_"):
                    payload[key] = value
            if record.exc_info:
                formatter = self.formatter or logging.Formatter()
                payload["exc_info"] = formatter.formatException(record.exc_info)
            self._client.emit(self._topic, payload)
        except Exception:  # noqa: BLE001 — logging.Handler contract: emit never raises
            self.handleError(record)
