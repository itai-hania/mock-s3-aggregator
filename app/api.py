"""HTTP route definitions for the service."""

from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile, status

from app.schemas import FileUploadResponse, ProcessingResult
from services.processor import ProcessorService, build_default_processor

router = APIRouter()


def get_processor() -> ProcessorService:
    return build_default_processor()


@router.post(
    "/files",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=FileUploadResponse,
    summary="Upload a CSV file for asynchronous processing.",
)
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="CSV file containing sensor readings."),
    processor: ProcessorService = Depends(get_processor),
) -> FileUploadResponse:
    try:
        file_id = processor.enqueue_file(background_tasks, file)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except NotImplementedError as exc:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="CSV processing pipeline not implemented yet.",
        ) from exc
    return FileUploadResponse(file_id=file_id)


@router.get(
    "/files/{file_id}",
    response_model=ProcessingResult,
    summary="Fetch processing metadata and aggregates for a file.",
)
async def get_file_result(
    file_id: str,
    processor: ProcessorService = Depends(get_processor),
) -> ProcessingResult:
    try:
        result = processor.fetch_result(file_id)
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except NotImplementedError as exc:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Fetching processing results is not implemented yet.",
        ) from exc
    return result


@router.get(
    "/health",
    summary="Health check endpoint.",
    status_code=status.HTTP_200_OK,
)
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.get(
    "/",
    summary="Root endpoint mirrors health information.",
    status_code=status.HTTP_200_OK,
)
async def root() -> dict[str, str]:
    return {"status": "ok", "detail": "See /health for service status."}
