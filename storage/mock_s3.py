from __future__ import annotations
from functools import lru_cache
from pathlib import Path
from threading import Lock
from typing import Dict, Iterable, Optional, Set


class MockS3Bucket:

    def __init__(self, name: str, root_path: Optional[Path] = None) -> None:
        self.name = name
        self._objects: Dict[str, bytes] = {}
        self._known_keys: Set[str] = set()
        self.root_path = root_path
        self._lock = Lock()
        if root_path:
            root_path.mkdir(parents=True, exist_ok=True)
            self._load_existing_keys()

    def put_object(self, key: str, data: bytes) -> None:
        with self._lock:
            self._objects[key] = data
            self._known_keys.add(key)
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
                    self._known_keys.add(key)
                return data

        raise KeyError(f"Object with key {key!r} not found in bucket {self.name!r}.")

    def list_objects(self) -> Iterable[str]:
        with self._lock:
            keys = set(self._known_keys)

        if self.root_path:
            for path in self.root_path.rglob("*"):
                if path.is_file():
                    keys.add(path.relative_to(self.root_path).as_posix())

        with self._lock:
            keys.update(self._objects.keys())

        return sorted(keys)

    def _load_existing_keys(self) -> None:
        assert self.root_path is not None
        for path in self.root_path.rglob("*"):
            if path.is_file():
                key = path.relative_to(self.root_path).as_posix()
                self._known_keys.add(key)


@lru_cache
def build_default_bucket(
    name: str = "uploads",
    root_path: Optional[str] = "./tmp/mock_s3",
) -> MockS3Bucket:
    path = Path(root_path) if root_path else None
    return MockS3Bucket(name=name, root_path=path)
