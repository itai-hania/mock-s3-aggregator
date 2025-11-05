from datetime import datetime, timezone

from models.records import SensorReading
from services.aggregator import Aggregator


def test_aggregate_computes_statistics() -> None:
    aggregator = Aggregator()
    readings = [
        SensorReading(sensor_id="sensor-a", timestamp=datetime.now(timezone.utc), value=10.0),
        SensorReading(sensor_id="sensor-a", timestamp=datetime.now(timezone.utc), value=20.0),
        SensorReading(sensor_id="sensor-b", timestamp=datetime.now(timezone.utc), value=5.0),
    ]

    summary = aggregator.aggregate(readings)

    assert summary.row_count == 3
    assert summary.min_value == 5.0
    assert summary.max_value == 20.0
    assert summary.mean_value == (10.0 + 20.0 + 5.0) / 3
    assert summary.per_sensor_count == {"sensor-a": 2, "sensor-b": 1}


def test_aggregate_empty_iterable() -> None:
    aggregator = Aggregator()

    summary = aggregator.aggregate([])

    assert summary.row_count == 0
    assert summary.min_value is None
    assert summary.max_value is None
    assert summary.mean_value is None
    assert summary.per_sensor_count == {}
