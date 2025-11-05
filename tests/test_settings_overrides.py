from __future__ import annotations

from typing import Iterable

from datastore.mock_dynamodb import build_default_table
from services.processor import build_default_processor
from settings import get_settings
from storage.mock_s3 import build_default_bucket


def _clear_caches(caches: Iterable) -> None:
    for cache in caches:
        cache.cache_clear()


def test_environment_overrides_apply(monkeypatch, tmp_path) -> None:
    bucket_root = tmp_path / "s3"
    table_path = tmp_path / "db.json"

    monkeypatch.setenv("MOCK_S3_BUCKET_NAME", "custom-bucket")
    monkeypatch.setenv("MOCK_S3_ROOT_PATH", str(bucket_root))
    monkeypatch.setenv("MOCK_DYNAMODB_TABLE_NAME", "custom-table")
    monkeypatch.setenv("MOCK_DYNAMODB_PERSISTENCE_PATH", str(table_path))
    monkeypatch.setenv("PROCESSOR_WORKER_COUNT", "2")

    caches = (
        get_settings,
        build_default_bucket,
        build_default_table,
        build_default_processor,
    )
    _clear_caches(caches)

    bucket = build_default_bucket()
    table = build_default_table()
    processor = build_default_processor()

    try:
        assert bucket.name == "custom-bucket"
        assert bucket.root_path == bucket_root
        assert table.name == "custom-table"
        assert table.persistence_path == table_path
        assert processor.executor._max_workers == 2
    finally:
        processor.shutdown()
        build_default_processor.cache_clear()
        build_default_bucket.cache_clear()
        build_default_table.cache_clear()
        get_settings.cache_clear()
