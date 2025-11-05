from __future__ import annotations

import logging
from logging.config import dictConfig
from typing import Iterable, Sequence

from settings import get_settings

_DEFAULT_EXTRA_KEYS = (
    "file_id",
    "object_key",
    "row_number",
    "reason",
    "status",
    "processing_ms",
    "error_count",
    "row_count",
    "invalid_value",
)

_configured = False


class ContextualFormatter(logging.Formatter):

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: str = "%",
        extra_keys: Iterable[str] | None = None,
    ) -> None:
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)
        self._extra_keys: Sequence[str] = tuple(extra_keys or _DEFAULT_EXTRA_KEYS)

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        context_parts: list[str] = []
        for key in self._extra_keys:
            if not hasattr(record, key):
                continue
            value = getattr(record, key, None)
            if value is None:
                continue
            context_parts.append(f"{key}={value}")
        if context_parts:
            return f"{message} | {' '.join(context_parts)}"
        return message


def configure_logging(level: str | int | None = None) -> None:
    """Configure application-wide logging with contextual formatting."""
    global _configured
    if _configured:
        return

    settings = get_settings()
    log_level = level if level is not None else settings.log_level

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "contextual": {
                    "()": "logging_config.ContextualFormatter",
                    "fmt": "%(asctime)sZ | %(levelname)s | %(name)s | %(message)s",
                    "datefmt": "%Y-%m-%dT%H:%M:%S",
                    "style": "%",
                    "extra_keys": list(_DEFAULT_EXTRA_KEYS),
                }
            },
            "handlers": {
                "default": {
                    "class": "logging.StreamHandler",
                    "level": log_level,
                    "formatter": "contextual",
                }
            },
            "root": {"handlers": ["default"], "level": log_level},
        }
    )

    _configured = True
