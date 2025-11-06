from __future__ import annotations

from typing import Any, Dict, Iterable

import typer


def echo_heading(text: str) -> None:
    typer.secho(text, bold=True)


def echo_key_values(pairs: Iterable[tuple[str, Any]]) -> None:
    for key, value in pairs:
        typer.echo(f"{key}: {value}")


def render_result(payload: Dict[str, Any]) -> None:
    echo_heading("Processing Result")
    meta_pairs = [
        ("file_id", payload.get("file_id")),
        ("status", payload.get("status")),
        ("uploaded_at", payload.get("uploaded_at")),
        ("processed_at", payload.get("processed_at")),
        ("processing_ms", payload.get("processing_ms")),
    ]
    echo_key_values(meta_pairs)

    aggregates = payload.get("aggregates") or {}
    typer.echo()
    echo_heading("Aggregates")
    if aggregates:
        echo_key_values(
            [
                ("row_count", aggregates.get("row_count")),
                ("min_value", aggregates.get("min_value")),
                ("max_value", aggregates.get("max_value")),
                ("mean_value", aggregates.get("mean_value")),
            ]
        )
        per_sensor = aggregates.get("per_sensor_count") or {}
        if per_sensor:
            typer.echo("per_sensor_count:")
            for sensor_id, count in per_sensor.items():
                typer.echo(f"  - {sensor_id}: {count}")
    else:
        typer.echo("No aggregates available.")

    errors = payload.get("errors") or []
    typer.echo()
    echo_heading("Errors")
    if errors:
        for error in errors:
            typer.echo(
                f"  - row {error.get('row_number')}: {error.get('reason')}"
            )
    else:
        typer.echo("No errors recorded.")
