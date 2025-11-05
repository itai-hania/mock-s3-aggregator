"""FastAPI application entry-point."""

from __future__ import annotations

from fastapi import FastAPI

from app.api import router
from services.processor import build_default_processor


def create_app() -> FastAPI:
    """Instantiate and configure the FastAPI application."""
    app = FastAPI(
        title="Mock S3 Aggregator",
        description="Asynchronous CSV processing service backed by mocked cloud stores.",
        version="0.1.0",
    )
    app.include_router(router)

    @app.on_event("shutdown")
    async def shutdown_event() -> None:
        processor = build_default_processor()
        processor.shutdown()

    return app


app = create_app()

