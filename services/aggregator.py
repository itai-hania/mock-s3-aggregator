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
        summary = AggregationSummary()
        total = 0.0

        for reading in readings:
            summary.row_count += 1
            value = reading.value
            total += value

            if summary.min_value is None or value < summary.min_value:
                summary.min_value = value
            if summary.max_value is None or value > summary.max_value:
                summary.max_value = value

            summary.per_sensor_count[reading.sensor_id] = (
                summary.per_sensor_count.get(reading.sensor_id, 0) + 1
            )

        if summary.row_count:
            summary.mean_value = total / summary.row_count

        return summary

    def summarize_errors(self, errors: List[str]) -> List[str]:
        """Placeholder for richer error summaries once implemented."""
        return errors

