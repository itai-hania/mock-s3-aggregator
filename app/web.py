from __future__ import annotations

from pathlib import Path
from typing import Iterable

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.schemas import ProcessingResult, ProcessingStatus
from datastore.mock_dynamodb import MockDynamoDBTable, build_default_table
from services.processor import ProcessorService, build_default_processor


templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))


def get_processor() -> ProcessorService:
    return build_default_processor()


def get_table() -> MockDynamoDBTable:
    return build_default_table()


def _sort_results(results: Iterable[ProcessingResult]) -> list[ProcessingResult]:
    return sorted(results, key=lambda result: result.uploaded_at, reverse=True)


router = APIRouter(include_in_schema=False)


@router.get("/ui", name="ui_index", response_class=HTMLResponse)
async def ui_index(
    request: Request,
    table: MockDynamoDBTable = Depends(get_table),
) -> HTMLResponse:
    items = _sort_results(table.scan())
    return templates.TemplateResponse(
        "ui/index.html",
        {
            "request": request,
            "results": items,
        },
    )


@router.get("/ui/files/{file_id}", name="ui_file_detail", response_class=HTMLResponse)
async def ui_file_detail(
    request: Request,
    file_id: str,
    processor: ProcessorService = Depends(get_processor),
) -> HTMLResponse:
    try:
        result = processor.fetch_result(file_id)
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc

    pollable_statuses = {
        ProcessingStatus.uploaded,
        ProcessingStatus.processing,
    }
    should_poll = result.status in pollable_statuses

    return templates.TemplateResponse(
        "ui/detail.html",
        {
            "request": request,
            "result": result,
            "should_poll": should_poll,
        },
    )
