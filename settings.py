from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional


_BUCKET_NAME_ENV = "MOCK_S3_BUCKET_NAME"
_BUCKET_ROOT_ENV = "MOCK_S3_ROOT_PATH"
_TABLE_NAME_ENV = "MOCK_DYNAMODB_TABLE_NAME"
_TABLE_PATH_ENV = "MOCK_DYNAMODB_PERSISTENCE_PATH"
_WORKER_COUNT_ENV = "PROCESSOR_WORKER_COUNT"
_LOG_LEVEL_ENV = "LOG_LEVEL"


@dataclass(frozen=True)
class Settings:
    bucket_name: str
    bucket_root_path: Optional[str]
    table_name: str
    table_persistence_path: Optional[str]
    processor_workers: int
    log_level: str


def _read_str_env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return default
    candidate = value.strip()
    return candidate or default


def _read_optional_env(name: str, default: Optional[str]) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return default
    candidate = value.strip()
    return candidate or None


def _read_worker_count(default: int) -> int:
    value = os.getenv(_WORKER_COUNT_ENV)
    if value is None:
        return default
    candidate = value.strip()
    if not candidate:
        return default
    try:
        parsed = int(candidate)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _read_log_level(default: str) -> str:
    value = os.getenv(_LOG_LEVEL_ENV)
    if value is None:
        return default
    candidate = value.strip()
    if not candidate:
        return default
    return candidate.upper()


@lru_cache
def get_settings() -> Settings:
    return Settings(
        bucket_name=_read_str_env(_BUCKET_NAME_ENV, "uploads"),
        bucket_root_path=_read_optional_env(_BUCKET_ROOT_ENV, "./tmp/mock_s3"),
        table_name=_read_str_env(_TABLE_NAME_ENV, "processing_results"),
        table_persistence_path=_read_optional_env(_TABLE_PATH_ENV, "./tmp/mock_db.json"),
        processor_workers=_read_worker_count(4),
        log_level=_read_log_level("INFO"),
    )
