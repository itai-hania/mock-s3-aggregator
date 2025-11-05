from fastapi.testclient import TestClient

from app.main import create_app
from services.processor import build_default_processor


def test_lifespan_shuts_down_processor_and_clears_cache() -> None:
    app = create_app()

    with TestClient(app):
        processor_during = build_default_processor()
        assert processor_during.executor._shutdown is False

    processor_after = build_default_processor()
    try:
        assert processor_after is not processor_during
        assert processor_after.executor._shutdown is False
    finally:
        processor_after.shutdown()
        build_default_processor.cache_clear()
