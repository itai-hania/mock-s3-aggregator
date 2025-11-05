from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime


@dataclass
class SensorReading:
    sensor_id: str
    timestamp: datetime
    value: float
