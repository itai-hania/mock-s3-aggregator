from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

DEFAULT_BASE_URL = "http://localhost:8000"
DEFAULT_POLL_INTERVAL = 0.5
DEFAULT_TIMEOUT = 60.0

_BASE_URL_ENV = "API_BASE_URL"
_POLL_INTERVAL_ENV = "CLI_POLL_INTERVAL"
_TIMEOUT_ENV = "CLI_POLL_TIMEOUT"


@dataclass(frozen=True)
class CLIConfig:
    base_url: str = DEFAULT_BASE_URL
    poll_interval: float = DEFAULT_POLL_INTERVAL
    poll_timeout: float = DEFAULT_TIMEOUT


def _read_float(value: Optional[str], default: float) -> float:
    if value is None:
        return default
    candidate = value.strip()
    if not candidate:
        return default
    try:
        parsed = float(candidate)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def load_config(
    base_url: Optional[str] = None,
    poll_interval: Optional[float] = None,
    poll_timeout: Optional[float] = None,
) -> CLIConfig:
    url = base_url or os.getenv(_BASE_URL_ENV) or DEFAULT_BASE_URL
    if poll_interval is None:
        poll_interval = _read_float(os.getenv(_POLL_INTERVAL_ENV), DEFAULT_POLL_INTERVAL)
    if poll_timeout is None:
        poll_timeout = _read_float(os.getenv(_TIMEOUT_ENV), DEFAULT_TIMEOUT)
    return CLIConfig(
        base_url=url.rstrip("/"),
        poll_interval=poll_interval,
        poll_timeout=poll_timeout,
    )
