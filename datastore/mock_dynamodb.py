"""Mock DynamoDB-style datastore for processing results."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

from app.schemas import ProcessingResult


class MockDynamoDBTable:
    """In-memory table abstraction mirroring a subset of DynamoDB semantics."""

    def __init__(self, name: str, persistence_path: Optional[Path] = None) -> None:
        self.name = name
        self._items: Dict[str, ProcessingResult] = {}
        self.persistence_path = persistence_path
        if persistence_path:
            persistence_path.parent.mkdir(parents=True, exist_ok=True)

    def put_item(self, item: ProcessingResult) -> None:
        raise NotImplementedError("Mock DynamoDB put_item not implemented yet.")

    def get_item(self, key: str) -> Optional[ProcessingResult]:
        raise NotImplementedError("Mock DynamoDB get_item not implemented yet.")


@lru_cache
def build_default_table(
    name: str = "processing_results",
    path: Optional[str] = "./tmp/mock_db.json",
) -> MockDynamoDBTable:
    persistence = Path(path) if path else None
    return MockDynamoDBTable(name=name, persistence_path=persistence)

