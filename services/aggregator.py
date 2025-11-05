"""Aggregation logic for sensor readings."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List

from models.records import SensorReading


@dataclass
class AggregationSummary:
    """Computed statistics for a batch of sensor readings."""

    row_count: int = 0
    min_value: float | None = None
    max_value: float | None = None
    mean_value: float | None = None
    per_sensor_count: Dict[str, int] = field(default_factory=dict)


class Aggregator:
    """Pure aggregation component that can be unit tested in isolation."""

    def aggregate(self, readings: Iterable[SensorReading]) -> AggregationSummary:
        raise NotImplementedError("Aggregation logic not implemented yet.")

    def summarize_errors(self, errors: List[str]) -> List[str]:
        """Placeholder for richer error summaries once implemented."""
        return errors

