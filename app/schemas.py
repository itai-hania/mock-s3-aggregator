"""Pydantic schemas for the HTTP API layer."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class ProcessingStatus(str, Enum):
    """Processing lifecycle states exposed via the API."""

    uploaded = "uploaded"
    processing = "processing"
    processed = "processed"
    partial = "partial"
    failed = "failed"


class FileUploadResponse(BaseModel):
    """Immediate response payload after accepting a file upload."""

    file_id: str = Field(..., description="Generated identifier for the uploaded file.")


class Aggregates(BaseModel):
    """Aggregate metrics computed for a processed CSV."""

    row_count: int = Field(..., ge=0)
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean_value: Optional[float] = None
    per_sensor_count: Dict[str, int] = Field(default_factory=dict)


class ProcessingError(BaseModel):
    """Details about a row that failed validation or parsing."""

    row_number: int = Field(..., ge=1)
    reason: str


class ProcessingResult(BaseModel):
    """Full record representing a processed file."""

    file_id: str
    status: ProcessingStatus
    uploaded_at: datetime
    processed_at: Optional[datetime] = None
    processing_ms: Optional[int] = Field(
        default=None, description="Duration in milliseconds from start to finish."
    )
    aggregates: Optional[Aggregates] = None
    errors: List[ProcessingError] = Field(default_factory=list)

