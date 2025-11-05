"""Unit tests for the mock DynamoDB datastore implementation."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from app.schemas import Aggregates, ProcessingResult, ProcessingStatus
from datastore.mock_dynamodb import MockDynamoDBTable


def _sample_result(file_id: str = "file-123") -> ProcessingResult:
    return ProcessingResult(
        file_id=file_id,
        status=ProcessingStatus.processed,
        uploaded_at=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        processed_at=datetime(2024, 1, 1, 12, 5, tzinfo=timezone.utc),
        processing_ms=300000,
        aggregates=Aggregates(
            row_count=5,
            min_value=1.0,
            max_value=9.0,
            mean_value=4.2,
            per_sensor_count={"sensor-a": 3, "sensor-b": 2},
        ),
    )


def test_put_and_get_round_trip_returns_deep_copy() -> None:
    table = MockDynamoDBTable(name="processing_results")
    original = _sample_result()

    table.put_item(original)
    fetched = table.get_item(original.file_id)

    assert fetched is not None
    assert fetched == original
    assert fetched is not original

    # Mutating the fetched instance should not affect stored data
    fetched.aggregates.row_count = 42  # type: ignore[assignment]
    fetched_again = table.get_item(original.file_id)
    assert fetched_again is not None
    assert fetched_again.aggregates.row_count == 5  # type: ignore[union-attr]


def test_get_item_returns_none_when_missing() -> None:
    table = MockDynamoDBTable(name="processing_results")

    assert table.get_item("missing-id") is None


def test_put_item_persists_to_disk_and_reloads(tmp_path) -> None:
    path = tmp_path / "mock_db.json"
    table = MockDynamoDBTable(name="processing_results", persistence_path=path)
    result = _sample_result()

    table.put_item(result)

    assert path.exists()
    payload = json.loads(path.read_text())
    assert result.file_id in payload
    assert payload[result.file_id]["status"] == ProcessingStatus.processed

    table_reloaded = MockDynamoDBTable(name="processing_results", persistence_path=path)
    loaded = table_reloaded.get_item(result.file_id)
    assert loaded == result
    assert loaded is not result


def test_scan_returns_all_items_as_deep_copies() -> None:
    table = MockDynamoDBTable(name="processing_results")
    first = _sample_result(file_id="file-1")
    second = _sample_result(file_id="file-2")

    table.put_item(first)
    table.put_item(second)

    scanned = sorted(table.scan(), key=lambda item: item.file_id)
    assert [item.file_id for item in scanned] == ["file-1", "file-2"]

    scanned[0].aggregates.row_count = 99  # type: ignore[assignment]
    rescanned = table.scan()
    assert all(item.aggregates.row_count == 5 for item in rescanned)  # type: ignore[union-attr]
