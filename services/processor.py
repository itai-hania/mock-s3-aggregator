from __future__ import annotations
import csv
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from threading import Lock
from typing import Dict, Iterator, Optional
from uuid import uuid4
from fastapi import BackgroundTasks, UploadFile
from app.schemas import (
    Aggregates,
    ProcessingError,
    ProcessingResult,
    ProcessingStatus,
)
from datastore.mock_dynamodb import MockDynamoDBTable, build_default_table
from services.aggregator import Aggregator
from models.records import SensorReading
from storage.mock_s3 import MockS3Bucket, build_default_bucket


logger = logging.getLogger(__name__)


class ProcessorService:

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
        self._futures: Dict[str, Future[None]] = {}
        self._futures_lock = Lock()

    def enqueue_file(self, background_tasks: BackgroundTasks, file: UploadFile) -> str:
        file_id = str(uuid4())
        filename = Path(file.filename or "upload.csv").name
        key = f"{file_id}/{filename}"

        file.file.seek(0)
        contents = file.file.read()
        if isinstance(contents, str):
            contents = contents.encode("utf-8")
        if not contents:
            raise ValueError("Uploaded file is empty.")

        self.bucket.put_object(key, contents)

        uploaded_at = datetime.now(timezone.utc)
        initial_record = ProcessingResult(
            file_id=file_id,
            status=ProcessingStatus.uploaded,
            uploaded_at=uploaded_at,
            errors=[],
        )
        self.table.put_item(initial_record)

        future = self.executor.submit(
            self._process_file, file_id=file_id, key=key, uploaded_at=uploaded_at
        )
        with self._futures_lock:
            self._futures[file_id] = future
        future.add_done_callback(lambda _f, fid=file_id: self._clear_future(fid))

        background_tasks.add_task(file.close)
        return file_id

    def fetch_result(self, file_id: str) -> ProcessingResult:
        result = self.table.get_item(file_id)
        if result is None:
            raise KeyError(f"Processing result for file {file_id!r} not found.")
        return result

    def shutdown(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=True)

    def _clear_future(self, file_id: str) -> None:
        with self._futures_lock:
            self._futures.pop(file_id, None)

    def _process_file(self, file_id: str, key: str, uploaded_at: datetime) -> None:
        start_time = time.perf_counter()
        processing_record = ProcessingResult(
            file_id=file_id,
            status=ProcessingStatus.processing,
            uploaded_at=uploaded_at,
            errors=[],
        )
        self.table.put_item(processing_record)

        errors: list[ProcessingError] = []
        aggregates: Optional[Aggregates] = None
        status = ProcessingStatus.processing

        try:
            with self.bucket.open_text_object(key) as text_stream:
                reader = csv.DictReader(text_stream)

                if not reader.fieldnames:
                    raise ValueError("CSV file is missing a header row.")

                normalized = {name.lower().strip(): name for name in reader.fieldnames}
                required = {"sensor_id", "timestamp", "value"}
                missing = sorted(required - normalized.keys())
                if missing:
                    raise ValueError(
                        f"CSV missing required columns: {', '.join(missing)}"
                    )

                sensor_col = normalized["sensor_id"]
                timestamp_col = normalized["timestamp"]
                value_col = normalized["value"]

                def iter_readings() -> Iterator[SensorReading]:
                    def register_error(row_number: int, reason: str) -> None:
                        logger.warning(
                            "Skipping row %d for file_id=%s (object key %s): %s",
                            row_number,
                            file_id,
                            key,
                            reason,
                            extra={
                                "file_id": file_id,
                                "object_key": key,
                                "row_number": row_number,
                                "reason": reason,
                            },
                        )
                        errors.append(
                            ProcessingError(row_number=row_number, reason=reason)
                        )

                    for row_number, row in enumerate(reader, start=2):
                        sensor_raw = (row.get(sensor_col) or "").strip()
                        timestamp_raw = (row.get(timestamp_col) or "").strip()
                        value_raw = (row.get(value_col) or "").strip()

                        if not sensor_raw:
                            register_error(row_number, "missing sensor_id")
                            continue

                        if not timestamp_raw:
                            register_error(row_number, "missing timestamp")
                            continue

                        try:
                            timestamp = self._parse_timestamp(timestamp_raw)
                        except ValueError:
                            register_error(row_number, "invalid timestamp")
                            continue

                        if not value_raw:
                            register_error(row_number, "missing value")
                            continue

                        try:
                            value = float(value_raw)
                        except ValueError:
                            register_error(row_number, "invalid numeric value")
                            continue

                        yield SensorReading(
                            sensor_id=sensor_raw, timestamp=timestamp, value=value
                        )

                summary = self.aggregator.aggregate(iter_readings())
            aggregates = Aggregates(
                row_count=summary.row_count,
                min_value=summary.min_value,
                max_value=summary.max_value,
                mean_value=summary.mean_value,
                per_sensor_count=dict(summary.per_sensor_count),
            )

            if summary.row_count == 0 and errors:
                status = ProcessingStatus.failed
                aggregates = None
            elif errors:
                status = ProcessingStatus.partial
            else:
                status = ProcessingStatus.processed
        except Exception as exc:  # pragma: no cover - defensive catch-all
            status = ProcessingStatus.failed
            errors.append(ProcessingError(row_number=1, reason=str(exc)))
            aggregates = None

        processed_at = datetime.now(timezone.utc)
        processing_ms = int((time.perf_counter() - start_time) * 1000)
        final_record = ProcessingResult(
            file_id=file_id,
            status=status,
            uploaded_at=uploaded_at,
            processed_at=processed_at,
            processing_ms=processing_ms,
            aggregates=aggregates,
            errors=errors,
        )
        self.table.put_item(final_record)

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        candidate = value.strip()
        if not candidate:
            raise ValueError("Timestamp is empty.")

        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"

        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise ValueError("Invalid timestamp format") from exc

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return parsed.astimezone(timezone.utc)


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
