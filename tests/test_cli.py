from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pytest
from typer.testing import CliRunner

from cli.app import app


class StubClient:
    def __init__(self, config, upload_response: str = "file-123") -> None:
        self.config = config
        self.upload_response = upload_response
        self.uploaded_path: Path | None = None
        self.poll_calls: List[tuple[str, float, float]] = []
        self.result_payload: Dict[str, Any] = {
            "file_id": upload_response,
            "status": "processed",
            "uploaded_at": "2024-01-01T00:00:00Z",
            "processed_at": "2024-01-01T00:00:01Z",
            "processing_ms": 123,
            "aggregates": {
                "row_count": 2,
                "min_value": 1.0,
                "max_value": 2.0,
                "mean_value": 1.5,
                "per_sensor_count": {"sensor-a": 1, "sensor-b": 1},
            },
            "errors": [],
        }
        self.closed = False

    def upload_file(self, path: Path) -> str:
        self.uploaded_path = path
        return self.upload_response

    def poll_result(self, file_id: str, interval: float, timeout: float) -> Dict[str, Any]:
        self.poll_calls.append((file_id, interval, timeout))
        return self.result_payload

    def get_result(self, file_id: str) -> Dict[str, Any]:
        payload = self.result_payload.copy()
        payload["file_id"] = file_id
        return payload

    def close(self) -> None:
        self.closed = True


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _install_stub(monkeypatch, stub: StubClient) -> None:
    def factory(config):
        stub.config = config
        return stub

    monkeypatch.setattr("cli.app.ApiClient", factory)


def test_upload_without_wait(monkeypatch, runner: CliRunner, tmp_path) -> None:
    stub = StubClient(config=None)
    _install_stub(monkeypatch, stub)
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("sensor_id,timestamp,value\ns,2024-01-01T00:00:00Z,1.0\n")

    result = runner.invoke(app, ["upload", str(csv_path)])

    assert result.exit_code == 0
    assert "Upload accepted" in result.stdout
    assert stub.uploaded_path == csv_path
    assert not stub.poll_calls
    assert stub.closed is True


def test_upload_with_wait(monkeypatch, runner: CliRunner, tmp_path) -> None:
    stub = StubClient(config=None)
    _install_stub(monkeypatch, stub)
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("sensor_id,timestamp,value\ns,2024-01-01T00:00:00Z,1.0\n")

    result = runner.invoke(app, ["upload", str(csv_path), "--wait", "--poll-interval", "0.1", "--timeout", "5"])

    assert result.exit_code == 0
    assert "Processing Result" in result.stdout
    assert stub.poll_calls == [("file-123", 0.1, 5.0)]
    assert stub.closed is True


def test_result_command(monkeypatch, runner: CliRunner) -> None:
    stub = StubClient(config=None)
    _install_stub(monkeypatch, stub)

    result = runner.invoke(app, ["result", "file-999"])

    assert result.exit_code == 0
    assert "file_id: file-999" in result.stdout
    assert "Aggregates" in result.stdout
    assert stub.closed is True
