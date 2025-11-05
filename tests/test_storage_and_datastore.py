from datetime import datetime, timezone
from pathlib import Path

from app.schemas import Aggregates, ProcessingResult, ProcessingStatus
from datastore.mock_dynamodb import MockDynamoDBTable
from storage.mock_s3 import MockS3Bucket


def test_mock_s3_put_and_get(tmp_path: Path) -> None:
    bucket = MockS3Bucket(name="test", root_path=tmp_path)
    bucket.put_object("file.txt", b"hello")

    assert (tmp_path / "file.txt").read_bytes() == b"hello"
    assert "file.txt" in bucket.list_objects()

    fresh_bucket = MockS3Bucket(name="test", root_path=tmp_path)
    assert fresh_bucket.get_object("file.txt") == b"hello"


def test_mock_s3_missing_key(tmp_path: Path) -> None:
    bucket = MockS3Bucket(name="test", root_path=tmp_path)

    try:
        bucket.get_object("missing.txt")
    except KeyError as exc:
        assert "missing.txt" in str(exc)
    else:  # pragma: no cover - defensive check
        raise AssertionError("Expected a KeyError for missing object")


def test_mock_dynamodb_put_and_get(tmp_path: Path) -> None:
    path = tmp_path / "db.json"
    table = MockDynamoDBTable(name="test", persistence_path=path)
    uploaded_at = datetime.now(timezone.utc)
    result = ProcessingResult(
        file_id="abc",
        status=ProcessingStatus.uploaded,
        uploaded_at=uploaded_at,
        aggregates=Aggregates(
            row_count=1,
            min_value=1.0,
            max_value=1.0,
            mean_value=1.0,
            per_sensor_count={"sensor": 1},
        ),
    )

    table.put_item(result)

    loaded = MockDynamoDBTable(name="test", persistence_path=path).get_item("abc")
    assert loaded is not None
    assert loaded.file_id == "abc"
    assert loaded.aggregates is not None
    assert loaded.aggregates.row_count == 1

