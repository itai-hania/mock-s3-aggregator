"""Domain models shared across services."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class SensorReading:
    """A single sensor reading parsed from the CSV."""

    sensor_id: str
    timestamp: datetime
    value: float

