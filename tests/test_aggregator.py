"""Unit tests for the aggregation logic."""

from __future__ import annotations

from datetime import datetime

from services.aggregator import Aggregator
from models.records import SensorReading


def _reading(sensor_id: str, value: float) -> SensorReading:
    """Helper to build deterministic sensor readings."""

    return SensorReading(sensor_id=sensor_id, timestamp=datetime(2024, 1, 1), value=value)


def test_aggregate_empty_iterable_returns_default_summary() -> None:
    aggregator = Aggregator()

    summary = aggregator.aggregate([])

    assert summary.row_count == 0
    assert summary.min_value is None
    assert summary.max_value is None
    assert summary.mean_value is None
    assert summary.per_sensor_count == {}


def test_aggregate_computes_statistics() -> None:
    aggregator = Aggregator()
    readings = [
        _reading("sensor-a", 10.0),
        _reading("sensor-b", 30.0),
        _reading("sensor-a", 20.0),
    ]

    summary = aggregator.aggregate(readings)

    assert summary.row_count == 3
    assert summary.min_value == 10.0
    assert summary.max_value == 30.0
    assert summary.mean_value == 20.0
    assert summary.per_sensor_count == {"sensor-a": 2, "sensor-b": 1}
