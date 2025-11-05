from __future__ import annotations
import json
from functools import lru_cache
from pathlib import Path
from threading import Lock
from typing import Dict, Optional

from app.schemas import ProcessingResult
from settings import get_settings


class MockDynamoDBTable:

    def __init__(self, name: str, persistence_path: Optional[Path] = None) -> None:
        self.name = name
        self._items: Dict[str, ProcessingResult] = {}
        self.persistence_path = persistence_path
        self._lock = Lock()
        if persistence_path:
            persistence_path.parent.mkdir(parents=True, exist_ok=True)
            self._load_from_disk()

    def put_item(self, item: ProcessingResult) -> None:
        with self._lock:
            self._items[item.file_id] = item.model_copy(deep=True)
            self._persist()

    def get_item(self, key: str) -> Optional[ProcessingResult]:
        with self._lock:
            item = self._items.get(key)
            if item is None:
                return None
            return item.model_copy(deep=True)

    def scan(self) -> list[ProcessingResult]:
        """Return deep copies of all stored processing results."""

        with self._lock:
            return [item.model_copy(deep=True) for item in self._items.values()]

    def _persist(self) -> None:
        if not self.persistence_path:
            return
        payload = {
            file_id: item.model_dump(mode="json") for file_id, item in self._items.items()
        }
        self.persistence_path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    def _load_from_disk(self) -> None:
        if not self.persistence_path or not self.persistence_path.exists():
            return

        try:
            raw = self.persistence_path.read_text() or "{}"
            data = json.loads(raw)
        except (OSError, json.JSONDecodeError):
            data = {}

        for file_id, payload in data.items():
            self._items[file_id] = ProcessingResult.model_validate(payload)


@lru_cache
def build_default_table(
    name: Optional[str] = None,
    path: Optional[str] = None,
) -> MockDynamoDBTable:
    settings = get_settings()
    table_name = settings.table_name if name is None else name
    table_path = settings.table_persistence_path if path is None else path
    persistence = Path(table_path) if table_path else None
    return MockDynamoDBTable(name=table_name, persistence_path=persistence)
