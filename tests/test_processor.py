import asyncio
import io
import logging
import threading
import time

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


def test_processor_logs_skipped_rows(tmp_path, caplog) -> None:
    bucket = MockS3Bucket(name="test", root_path=tmp_path / "s3")
    table = MockDynamoDBTable(name="test", persistence_path=tmp_path / "db.json")
    processor = ProcessorService(bucket=bucket, table=table, aggregator=Aggregator(), workers=1)

    csv_content = """sensor_id,timestamp,value
sensor-1,2024-01-01T00:00:00Z,1.0
sensor-1,2024-01-01T00:02:00Z,not-a-number
"""
    upload = _create_upload_file(csv_content, filename="invalid.csv")
    tasks = BackgroundTasks()

    with caplog.at_level(logging.WARNING):
        file_id = processor.enqueue_file(tasks, upload)
        _drain_background_tasks(tasks)
        _await_result(processor, file_id)

    records = [record for record in caplog.records if record.name == "services.processor"]
    assert records, "Expected row skip warnings to be logged."

    messages = [record.getMessage() for record in records]
    assert any("Skipping row" in message and "invalid numeric value" in message for message in messages)

    assert any(getattr(record, "file_id", None) == file_id for record in records)
    assert any(getattr(record, "object_key", "").endswith("invalid.csv") for record in records)


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


def test_processor_handles_parallel_jobs(tmp_path) -> None:
    bucket = MockS3Bucket(name="test", root_path=tmp_path / "s3")
    table = MockDynamoDBTable(name="test", persistence_path=tmp_path / "db.json")
    barrier = threading.Barrier(2)
    sleep_seconds = 0.1

    class CoordinatedAggregator(Aggregator):
        def aggregate(self, readings):
            items = list(readings)
            try:
                barrier.wait(timeout=1.0)
            except threading.BrokenBarrierError as exc:
                raise AssertionError("Aggregator workers did not run concurrently") from exc
            time.sleep(sleep_seconds)
            return super().aggregate(items)

    processor = ProcessorService(
        bucket=bucket,
        table=table,
        aggregator=CoordinatedAggregator(),
        workers=2,
    )

    csv_content = """sensor_id,timestamp,value
sensor-1,2024-01-01T00:00:00Z,1.0
sensor-2,2024-01-01T00:01:00Z,2.5
"""
    upload_one = _create_upload_file(csv_content, filename="parallel-one.csv")
    upload_two = _create_upload_file(csv_content, filename="parallel-two.csv")
    tasks_one = BackgroundTasks()
    tasks_two = BackgroundTasks()

    start = time.perf_counter()
    file_id_one = processor.enqueue_file(tasks_one, upload_one)
    file_id_two = processor.enqueue_file(tasks_two, upload_two)

    _drain_background_tasks(tasks_one)
    _drain_background_tasks(tasks_two)

    _await_result(processor, file_id_one)
    _await_result(processor, file_id_two)
    elapsed = time.perf_counter() - start

    result_one = processor.fetch_result(file_id_one)
    result_two = processor.fetch_result(file_id_two)

    assert result_one.status == ProcessingStatus.processed
    assert result_two.status == ProcessingStatus.processed
    assert result_one.aggregates is not None
    assert result_two.aggregates is not None
    assert elapsed < sleep_seconds * 2.5

    processor.shutdown()
