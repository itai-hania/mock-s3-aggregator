from __future__ import annotations
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from app.api import router
from logging_config import configure_logging
from services.processor import build_default_processor


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    processor = build_default_processor()
    try:
        yield
    finally:
        processor.shutdown()
        build_default_processor.cache_clear()


def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(
        title="Mock S3 Aggregator",
        description="Asynchronous CSV processing service backed by mocked cloud stores.",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(router)
    return app

app = create_app()
