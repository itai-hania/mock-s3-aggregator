"""Background processing orchestration for CSV files."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from typing import Optional
from uuid import uuid4

from fastapi import BackgroundTasks, UploadFile

from app.schemas import ProcessingResult
from datastore.mock_dynamodb import MockDynamoDBTable, build_default_table
from services.aggregator import Aggregator
from storage.mock_s3 import MockS3Bucket, build_default_bucket


class ProcessorService:
    """Coordinates storage, background parsing, and result retrieval."""

    def __init__(
        self,
        bucket: MockS3Bucket,
        table: MockDynamoDBTable,
        aggregator: Aggregator,
        workers: int = 4,
    ) -> None:
        self.bucket = bucket
        self.table = table
        self.aggregator = aggregator
        self.executor = ThreadPoolExecutor(max_workers=workers)

    def enqueue_file(self, background_tasks: BackgroundTasks, file: UploadFile) -> str:
        """Persist file data and trigger asynchronous processing."""
        raise NotImplementedError("CSV processing pipeline not implemented yet.")

    def fetch_result(self, file_id: str) -> ProcessingResult:
        """Retrieve processing output from persistent storage."""
        raise NotImplementedError("Result retrieval not implemented yet.")

    def shutdown(self) -> None:
        """Clean up executor resources during application shutdown."""
        self.executor.shutdown(wait=False, cancel_futures=True)


@lru_cache
def build_default_processor(
    workers: Optional[int] = None,
) -> ProcessorService:
    """Factory that wires the processor with default mocks."""
    bucket = build_default_bucket()
    table = build_default_table()
    aggregator = Aggregator()
    worker_count = workers or 4
    return ProcessorService(bucket=bucket, table=table, aggregator=aggregator, workers=worker_count)

