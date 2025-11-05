from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.schemas import Aggregates, ProcessingStatus
from datastore.mock_dynamodb import MockDynamoDBTable
from services.aggregator import Aggregator
from services.processor import ProcessorService
from storage.mock_s3 import MockS3Bucket


@pytest.fixture()
def processor() -> ProcessorService:
    bucket = MockS3Bucket("test-bucket")
    table = MockDynamoDBTable("test-table")
    aggregator = Aggregator()
    service = ProcessorService(bucket=bucket, table=table, aggregator=aggregator, workers=1)
    yield service
    service.shutdown()


def _process(processor: ProcessorService, file_id: str, contents: str) -> None:
    key = f"{file_id}/data.csv"
    processor.bucket.put_object(key, contents.encode("utf-8"))
    processor._process_file(  # type: ignore[attr-defined]
        file_id=file_id,
        key=key,
        uploaded_at=datetime.now(timezone.utc),
    )


def test_process_file_success(processor: ProcessorService) -> None:
    csv_body = (
        "sensor_id,timestamp,value\n"
        "sensor-a,2024-01-01T00:00:00Z,1.5\n"
        "sensor-b,2024-01-01T01:00:00+00:00,2.5\n"
    )
    _process(processor, "success", csv_body)

    result = processor.fetch_result("success")
    assert result.status is ProcessingStatus.processed
    assert result.aggregates is not None
    assert result.aggregates == Aggregates(
        row_count=2,
        min_value=1.5,
        max_value=2.5,
        mean_value=2.0,
        per_sensor_count={"sensor-a": 1, "sensor-b": 1},
    )
    assert result.errors == []


def test_process_file_partial_with_row_errors(processor: ProcessorService) -> None:
    csv_body = (
        "sensor_id,timestamp,value\n"
        "sensor-a,2024-01-01T00:00:00Z,3.0\n"
        " ,2024-01-01T01:00:00Z,1\n"
        "sensor-b,invalid,2\n"
        "sensor-c,2024-01-01T02:00:00Z,not-a-number\n"
    )
    _process(processor, "partial", csv_body)

    result = processor.fetch_result("partial")
    assert result.status is ProcessingStatus.partial
    assert result.aggregates is not None
    assert result.aggregates.row_count == 1
    assert result.aggregates.per_sensor_count == {"sensor-a": 1}

    reasons = {error.reason for error in result.errors}
    assert reasons == {"missing sensor_id", "invalid timestamp", "invalid numeric value"}
    row_numbers = sorted(error.row_number for error in result.errors)
    assert row_numbers == [3, 4, 5]


def test_process_file_missing_headers_fails(processor: ProcessorService) -> None:
    csv_body = (
        "sensor,timestamp,value\n"
        "sensor-a,2024-01-01T00:00:00Z,1\n"
    )
    _process(processor, "missing", csv_body)

    result = processor.fetch_result("missing")
    assert result.status is ProcessingStatus.failed
    assert result.aggregates is None
    assert result.errors
    assert "missing required columns" in result.errors[0].reason
