"""Mock S3-style storage for raw CSV objects."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, Optional


class MockS3Bucket:
    """Minimal in-memory representation of a bucket with optional persistence."""

    def __init__(self, name: str, root_path: Optional[Path] = None) -> None:
        self.name = name
        self._objects: Dict[str, bytes] = {}
        self.root_path = root_path
        if root_path:
            root_path.mkdir(parents=True, exist_ok=True)

    def put_object(self, key: str, data: bytes) -> None:
        raise NotImplementedError("Mock S3 put_object not implemented yet.")

    def get_object(self, key: str) -> bytes:
        raise NotImplementedError("Mock S3 get_object not implemented yet.")

    def list_objects(self) -> Iterable[str]:
        return self._objects.keys()


@lru_cache
def build_default_bucket(
    name: str = "uploads",
    root_path: Optional[str] = "./tmp/mock_s3",
) -> MockS3Bucket:
    """Factory that builds the default bucket backed by configuration."""
    path = Path(root_path) if root_path else None
    return MockS3Bucket(name=name, root_path=path)

