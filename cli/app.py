from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import typer

from cli.client import ApiClient
from cli.config import CLIConfig, load_config
from cli.render import render_result


@dataclass
class CLIState:
    config: CLIConfig
    client: ApiClient


app = typer.Typer(
    help="Utilities for interacting with the mock S3 aggregator service.",
    context_settings={"help_option_names": ["-h", "--help"]},
)


def _get_state(ctx: typer.Context) -> CLIState:
    state = ctx.obj
    if not isinstance(state, CLIState):
        raise typer.Exit(code=1, message="CLI state is uninitialized.")
    return state


@app.callback()
def main(
    ctx: typer.Context,
    base_url: Optional[str] = typer.Option(
        None,
        "--base-url",
        "-b",
        help="Aggregator API base URL (defaults to API_BASE_URL env or http://localhost:8000).",
    ),
    poll_interval: Optional[float] = typer.Option(
        None,
        "--poll-interval",
        help="Seconds between status checks when waiting for completion.",
    ),
    timeout: Optional[float] = typer.Option(
        None,
        "--timeout",
        help="Maximum seconds to wait when polling for results.",
    ),
) -> None:
    """Entry point for the CLI."""
    config = load_config(
        base_url=base_url,
        poll_interval=poll_interval,
        poll_timeout=timeout,
    )
    client = ApiClient(config)
    ctx.obj = CLIState(config=config, client=client)
    ctx.call_on_close(client.close)


@app.command("upload")
def upload_command(
    ctx: typer.Context,
    file: Path = typer.Argument(..., exists=True, dir_okay=False, readable=True, help="Path to CSV file."),
    wait: bool = typer.Option(
        False,
        "--wait/--no-wait",
        help="Wait for processing to finish and display the result.",
    ),
    poll_interval: Optional[float] = typer.Option(
        None,
        "--poll-interval",
        help="Override poll interval while waiting.",
    ),
    timeout: Optional[float] = typer.Option(
        None,
        "--timeout",
        help="Override timeout while waiting.",
    ),
) -> None:
    """Upload a CSV file for asynchronous processing."""
    state = _get_state(ctx)
    typer.echo(f"Uploading {file} to {state.config.base_url} ...")
    file_id = state.client.upload_file(file)
    typer.secho(f"Upload accepted. file_id={file_id}", fg=typer.colors.GREEN)

    if not wait:
        return

    interval = poll_interval if poll_interval is not None else state.config.poll_interval
    poll_timeout = timeout if timeout is not None else state.config.poll_timeout
    typer.echo(f"Waiting for processing (interval={interval}s, timeout={poll_timeout}s)...")
    result = state.client.poll_result(file_id, interval=interval, timeout=poll_timeout)
    typer.echo()
    render_result(result)


@app.command("result")
def result_command(
    ctx: typer.Context,
    file_id: str = typer.Argument(..., help="Identifier returned from the upload command."),
) -> None:
    """Fetch processing status and aggregates for a file."""
    state = _get_state(ctx)
    payload = state.client.get_result(file_id)
    render_result(payload)
