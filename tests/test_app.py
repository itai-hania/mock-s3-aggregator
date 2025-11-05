import time
import uuid
from typing import Dict, Iterator

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from datastore.mock_dynamodb import MockDynamoDBTable
from services.aggregator import Aggregator
from services.processor import ProcessorService, build_default_processor
from storage.mock_s3 import MockS3Bucket


@pytest.fixture
def api_client(tmp_path, monkeypatch) -> Iterator[TestClient]:
    processors: Dict[int, ProcessorService] = {}

    def build_test_processor(workers: int | None = None) -> ProcessorService:
        worker_count = workers or 1
        processor = processors.get(worker_count)
        if processor is None:
            bucket = MockS3Bucket(name="test", root_path=tmp_path / "s3")
            table = MockDynamoDBTable(
                name="test", persistence_path=tmp_path / "db.json"
            )
            processor = ProcessorService(
                bucket=bucket,
                table=table,
                aggregator=Aggregator(),
                workers=worker_count,
            )
            processors[worker_count] = processor
        return processor

    def cache_clear() -> None:
        while processors:
            _, processor = processors.popitem()
            processor.shutdown()

    build_test_processor.cache_clear = cache_clear  # type: ignore[attr-defined]

    monkeypatch.setattr("app.main.build_default_processor", build_test_processor)
    monkeypatch.setattr("app.api.build_default_processor", build_test_processor)
    monkeypatch.setattr("services.processor.build_default_processor", build_test_processor)

    app = create_app()
    with TestClient(app) as client:
        yield client

    cache_clear()


def test_lifespan_shuts_down_processor_and_clears_cache() -> None:
    app = create_app()

    with TestClient(app):
        processor_during = build_default_processor()
        assert processor_during.executor._shutdown is False

    processor_after = build_default_processor()
    try:
        assert processor_after is not processor_during
        assert processor_after.executor._shutdown is False
    finally:
        processor_after.shutdown()
        build_default_processor.cache_clear()


def _poll_for_completion(client: TestClient, file_id: str, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    last_payload: dict | None = None
    while time.monotonic() < deadline:
        response = client.get(f"/files/{file_id}")
        assert response.status_code == 200
        payload = response.json()
        last_payload = payload
        if payload["status"] not in {"uploaded", "processing"}:
            return payload
        time.sleep(0.05)
    pytest.fail(f"Processing for file {file_id} did not complete: {last_payload}")


def test_upload_and_poll_processing(api_client: TestClient) -> None:
    csv_content = """sensor_id,timestamp,value
sensor-1,2024-01-01T00:00:00Z,1.0
sensor-2,2024-01-01T00:01:00Z,2.5
"""

    response = api_client.post(
        "/files",
        files={"file": ("readings.csv", csv_content, "text/csv")},
    )

    assert response.status_code == 202
    payload = response.json()
    assert set(payload.keys()) == {"file_id"}
    file_id = payload["file_id"]
    assert isinstance(file_id, str) and file_id

    result = _poll_for_completion(api_client, file_id)

    assert result["file_id"] == file_id
    assert result["status"] == "processed"
    assert result["aggregates"] is not None
    assert result["aggregates"]["row_count"] == 2
    assert result["errors"] == []
    assert result["uploaded_at"] is not None
    assert result["processed_at"] is not None
    assert isinstance(result["processing_ms"], int)


def test_upload_empty_file_returns_bad_request(api_client: TestClient) -> None:
    response = api_client.post(
        "/files",
        files={"file": ("empty.csv", b"", "text/csv")},
    )

    assert response.status_code == 400
    body = response.json()
    assert body["detail"] == "Uploaded file is empty."


def test_get_missing_file_returns_not_found(api_client: TestClient) -> None:
    missing_id = str(uuid.uuid4())
    response = api_client.get(f"/files/{missing_id}")

    assert response.status_code == 404
    body = response.json()
    assert missing_id in body["detail"]
