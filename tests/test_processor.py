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

    with processor._futures_lock:  # wait for the worker to finish
        future = processor._futures.get(file_id)
    if future is not None:
        future.result(timeout=5)

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

    with processor._futures_lock:
        future = processor._futures.get(file_id)
    if future is not None:
        future.result(timeout=5)

    result = processor.fetch_result(file_id)
    assert result.status == ProcessingStatus.partial
    assert result.aggregates is not None
    assert result.aggregates.row_count == 1
    assert len(result.errors) == 1
    assert "invalid numeric value" in result.errors[0].reason
