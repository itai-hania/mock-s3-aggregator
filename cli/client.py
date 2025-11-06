from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict

import httpx
import typer

from cli.config import CLIConfig


class ApiClient:
    """Minimal HTTP client for the aggregator service."""

    def __init__(self, config: CLIConfig) -> None:
        self._config = config
        self._client = httpx.Client(base_url=config.base_url, timeout=30.0)

    def close(self) -> None:
        self._client.close()

    def upload_file(self, path: Path) -> str:
        if not path.exists():
            raise typer.BadParameter(f"File {path} does not exist.")
        if not path.is_file():
            raise typer.BadParameter(f"Path {path} is not a file.")

        try:
            with path.open("rb") as handle:
                response = self._client.post(
                    "/files",
                    files={"file": (path.name, handle, "text/csv")},
                )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            self._handle_http_error(exc)
        payload = response.json()
        file_id = payload.get("file_id")
        if not isinstance(file_id, str):
            raise typer.BadParameter("Unexpected response payload when uploading file.")
        return file_id

    def get_result(self, file_id: str) -> Dict[str, Any]:
        try:
            response = self._client.get(f"/files/{file_id}")
            if response.status_code == 404:
                raise typer.BadParameter(f"File {file_id} was not found.")
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            self._handle_http_error(exc)
        return response.json()

    def poll_result(self, file_id: str, interval: float, timeout: float) -> Dict[str, Any]:
        deadline = time.monotonic() + timeout
        last_payload: Dict[str, Any] | None = None
        while time.monotonic() <= deadline:
            last_payload = self.get_result(file_id)
            status = last_payload.get("status")
            if status not in {"uploaded", "processing"}:
                return last_payload
            time.sleep(interval)
        typer.secho(
            (
                f"Timed out waiting for processing of {file_id}. "
                f"Last status: {last_payload.get('status') if last_payload else 'unknown'}"
            ),
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)

    @staticmethod
    def _handle_http_error(exc: httpx.HTTPStatusError) -> None:
        detail: str | None = None
        try:
            data = exc.response.json()
            detail = data.get("detail")
        except Exception:  # noqa: BLE001 - best effort parsing
            detail = exc.response.text.strip()
        message = (
            f"Request failed with status {exc.response.status_code}: {detail or 'no detail provided.'}"
        )
        typer.secho(message, fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)
