from __future__ import annotations
from functools import lru_cache
from pathlib import Path
from threading import Lock
from typing import Dict, Iterable, Optional


class MockS3Bucket:

    def __init__(self, name: str, root_path: Optional[Path] = None) -> None:
        self.name = name
        self._objects: Dict[str, bytes] = {}
        self.root_path = root_path
        self._lock = Lock()
        if root_path:
            root_path.mkdir(parents=True, exist_ok=True)

    def put_object(self, key: str, data: bytes) -> None:
        with self._lock:
            self._objects[key] = data
        if self.root_path:
            path = self.root_path / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)

    def get_object(self, key: str) -> bytes:
        with self._lock:
            data = self._objects.get(key)
        if data is not None:
            return data

        if self.root_path:
            path = self.root_path / key
            if path.exists():
                data = path.read_bytes()
                with self._lock:
                    self._objects[key] = data
                return data

        raise KeyError(f"Object with key {key!r} not found in bucket {self.name!r}.")

    def list_objects(self) -> Iterable[str]:
        with self._lock:
            return list(self._objects.keys())


@lru_cache
def build_default_bucket(
    name: str = "uploads",
    root_path: Optional[str] = "./tmp/mock_s3",
) -> MockS3Bucket:
    path = Path(root_path) if root_path else None
    return MockS3Bucket(name=name, root_path=path)
