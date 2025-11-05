import asyncio
import io

from fastapi import BackgroundTasks, UploadFile

from app.schemas import ProcessingStatus
from datastore.mock_dynamodb import MockDynamoDBTable
from services.aggregator import Aggregator
from services.processor import ProcessorService
from storage.mock_s3 import MockS3Bucket


def _create_upload_file(content: str, filename: str = "data.csv") -> UploadFile:
    return UploadFile(filename=filename, file=io.BytesIO(content.encode("utf-8")))


def _drain_background_tasks(tasks: BackgroundTasks) -> None:
    asyncio.run(tasks())


def _await_result(processor: ProcessorService, file_id: str) -> None:
    with processor._futures_lock:
        future = processor._futures.get(file_id)
    if future is not None:
        future.result(timeout=5)


def test_processor_successful_processing(tmp_path) -> None:
    bucket = MockS3Bucket(name="test", root_path=tmp_path / "s3")
    table = MockDynamoDBTable(name="test", persistence_path=tmp_path / "db.json")
    processor = ProcessorService(bucket=bucket, table=table, aggregator=Aggregator(), workers=1)

    csv_content = """sensor_id,timestamp,value
sensor-1,2024-01-01T00:00:00Z,1.0
sensor-2,2024-01-01T00:01:00+00:00,2.0
"""
    upload = _create_upload_file(csv_content)
    tasks = BackgroundTasks()

    file_id = processor.enqueue_file(tasks, upload)
    _drain_background_tasks(tasks)
    _await_result(processor, file_id)

    result = processor.fetch_result(file_id)
    assert result.status == ProcessingStatus.processed
    assert result.aggregates is not None
    assert result.aggregates.row_count == 2
    assert not result.errors


def test_processor_partial_processing(tmp_path) -> None:
    bucket = MockS3Bucket(name="test", root_path=tmp_path / "s3")
    table = MockDynamoDBTable(name="test", persistence_path=tmp_path / "db.json")
    processor = ProcessorService(bucket=bucket, table=table, aggregator=Aggregator(), workers=1)

    csv_content = """sensor_id,timestamp,value
sensor-1,2024-01-01T00:00:00Z,1.0
sensor-1,2024-01-01T00:02:00Z,not-a-number
"""
    upload = _create_upload_file(csv_content, filename="invalid.csv")
    tasks = BackgroundTasks()

    file_id = processor.enqueue_file(tasks, upload)
    _drain_background_tasks(tasks)
    _await_result(processor, file_id)

    result = processor.fetch_result(file_id)
    assert result.status == ProcessingStatus.partial
    assert result.aggregates is not None
    assert result.aggregates.row_count == 1
    assert len(result.errors) == 1
    assert "invalid numeric value" in result.errors[0].reason


def test_processor_missing_required_header(tmp_path) -> None:
    bucket = MockS3Bucket(name="test", root_path=tmp_path / "s3")
    table = MockDynamoDBTable(name="test", persistence_path=tmp_path / "db.json")
    processor = ProcessorService(bucket=bucket, table=table, aggregator=Aggregator(), workers=1)

    csv_content = """sensor,timestamp,value
sensor-1,2024-01-01T00:00:00Z,1.0
"""
    upload = _create_upload_file(csv_content, filename="missing_header.csv")
    tasks = BackgroundTasks()

    file_id = processor.enqueue_file(tasks, upload)
    _drain_background_tasks(tasks)
    _await_result(processor, file_id)

    result = processor.fetch_result(file_id)
    assert result.status == ProcessingStatus.failed
    assert result.aggregates is None
    assert len(result.errors) == 1
    assert "CSV missing required columns" in result.errors[0].reason


def test_processor_collects_multiple_row_errors(tmp_path) -> None:
    bucket = MockS3Bucket(name="test", root_path=tmp_path / "s3")
    table = MockDynamoDBTable(name="test", persistence_path=tmp_path / "db.json")
    processor = ProcessorService(bucket=bucket, table=table, aggregator=Aggregator(), workers=1)

    csv_content = """sensor_id,timestamp,value
sensor-1,2024-01-01T00:00:00Z,1.0
sensor-2,not-a-timestamp,2.5
sensor-3,2024-01-01T00:02:00Z,
"""
    upload = _create_upload_file(csv_content, filename="row_errors.csv")
    tasks = BackgroundTasks()

    file_id = processor.enqueue_file(tasks, upload)
    _drain_background_tasks(tasks)
    _await_result(processor, file_id)

    result = processor.fetch_result(file_id)
    assert result.status == ProcessingStatus.partial
    assert result.aggregates is not None
    assert result.aggregates.row_count == 1
    reasons = sorted(error.reason for error in result.errors)
    assert reasons == ["invalid timestamp", "missing value"]


def test_processor_streams_from_disk(tmp_path) -> None:
    class StreamingBucket(MockS3Bucket):
        def get_object(self, key: str) -> bytes:  # pragma: no cover - defensive
            raise AssertionError("Processor should stream without loading whole file")

    bucket = StreamingBucket(name="test", root_path=tmp_path / "s3")
    table = MockDynamoDBTable(name="test", persistence_path=tmp_path / "db.json")
    processor = ProcessorService(bucket=bucket, table=table, aggregator=Aggregator(), workers=1)

    csv_content = """sensor_id,timestamp,value
sensor-1,2024-01-01T00:00:00Z,1.0
sensor-2,2024-01-01T00:01:00+00:00,2.0
"""
    upload = _create_upload_file(csv_content, filename="stream.csv")
    tasks = BackgroundTasks()

    file_id = processor.enqueue_file(tasks, upload)
    _drain_background_tasks(tasks)
    _await_result(processor, file_id)

    result = processor.fetch_result(file_id)
    assert result.status == ProcessingStatus.processed
    assert result.aggregates is not None
    assert result.aggregates.row_count == 2
