# Mock S3 Aggregator

Python service that accepts sensor CSV uploads, stores them in a mocked S3 bucket, processes the files asynchronously, and exposes aggregated results through an HTTP API backed by a mocked cloud datastore.

## Architecture

```
Client -> FastAPI upload endpoint
         |-- stores raw CSV in Mock S3 (bucket/key)
         |-- schedules background job
Background processor
         |-- downloads CSV from Mock S3
         |-- parses + aggregates sensor metrics
         |-- writes results & metadata to Mock DynamoDB
Client -> FastAPI results endpoint -> Mock DynamoDB lookup
```

- **Service layer:** FastAPI application served by Uvicorn (ASGI).
- **Storage mocks:** In-memory S3-style bucket for raw objects; DynamoDB-style table for processed metadata.
- **Processing pipeline:** CSV parsing, validation, aggregation, and error reporting in a dedicated worker pool.
- **Concurrency model:** Thread-based background executor to keep the HTTP layer responsive while CPU-light, I/O-bound processing runs in parallel.

## Key Decisions

- **FastAPI** chosen for async-friendly request handling, automatic OpenAPI docs, and easy background task integration.
- **ThreadPoolExecutor** leveraged for simplicity with Python’s GIL given the workload is I/O heavy (file reads/writes). Scales horizontally by increasing worker count or running multiple replicas.
- **Mocked AWS services** (S3 & DynamoDB) selected to mirror production intent while keeping the exercise self-contained.
- **Structured logging & status tracking** to surface partial failures without blocking successful rows.

## Setup

| Requirement | Notes |
| ----------- | ----- |
| Python      | 3.11+ recommended |
| Virtualenv  | `python -m venv .venv && source .venv/bin/activate` |
| Dependencies| `pip install -r requirements.txt` |

Environment variables (defaults provided in code):

- `MOCK_S3_BUCKET_NAME=uploads`
- `MOCK_S3_ROOT_PATH=./tmp/mock_s3`
- `MOCK_DYNAMODB_TABLE_NAME=processing_results`
- `MOCK_DYNAMODB_PERSISTENCE_PATH=./tmp/mock_db.json`
- `PROCESSOR_WORKER_COUNT=4`
- `LOG_LEVEL=INFO`
- `API_BASE_URL=http://localhost:8000` (CLI default target)
- `CLI_POLL_INTERVAL=0.5`
- `CLI_POLL_TIMEOUT=60`

## Running the Service

```bash
uvicorn app.main:app --reload
```

OpenAPI docs available at `http://localhost:8000/docs`. Redoc at `/redoc`.

## Web UI

- Navigate to `http://localhost:8000/ui` to access the server-rendered dashboard.
- Upload CSV files via the form and review processing progress from the same page.
- Click a file identifier to open `/ui/files/{file_id}` for detailed aggregates, error summaries, and live status polling.

The UI rides on the existing API surface, making it a companion to the CLI for quick manual verification. It is intentionally lightweight—no authentication, suited for local demos and smoke testing.
## Optional Docker Workflow

Container images are available for teams that prefer running the mock stack without a local Python toolchain. The existing
`uvicorn` instructions remain valid—Docker is entirely optional.

### Build & Run with Docker Compose

```bash
docker compose up --build
```

This starts the API on <http://localhost:8000> and persists mock S3/DynamoDB state under `./tmp` on the host so data
survives container restarts. The same environment variables listed above can be overridden when invoking Compose, e.g.:

```bash
MOCK_S3_ROOT_PATH=/data/mock_s3 PROCESSOR_WORKER_COUNT=8 docker compose up --build
```

### Containerized CLI (optional)

A companion `cli` service is defined for ad-hoc usage. It shares the same image and network so you can exercise the API end
to end:

```bash
docker compose run --rm cli --help
docker compose run --rm cli upload ./samples/readings.csv --wait
```

The CLI targets `http://api:8000` by default inside the Docker network. Override `API_BASE_URL` if you need to reach a
different host.

### Troubleshooting Containers

- Reset the persisted mock data by stopping Compose and deleting the `./tmp` directory before restarting.
- If a build fails due to stale dependencies, run `docker compose build --no-cache` to force a clean image.

## API Reference

### `POST /files`
- Accepts `multipart/form-data` with field `file` (CSV).
- Returns immediately with `202 Accepted` payload:

```json
{ "file_id": "f8d10a10-..." }
```

### `GET /files/{file_id}`
- Returns processing metadata, aggregates, and any row-level errors:

```json
{
  "file_id": "f8d10a10-...",
  "status": "processed",
  "uploaded_at": "2025-10-01T08:12:10Z",
  "processed_at": "2025-10-01T08:12:11Z",
  "processing_ms": 835,
  "aggregates": {
    "row_count": 5,
    "min_value": 19.87,
    "max_value": 42.0,
    "mean_value": 25.02,
    "per_sensor_count": {
      "sensor-a": 2,
      "sensor-b": 2,
      "sensor-c": 1
    }
  },
  "errors": [
    {
      "row_number": 6,
      "reason": "missing timestamp"
    }
  ]
}
```

### `GET /health`
- Lightweight health probe that verifies mock dependencies are reachable.

## Processing Pipeline

1. Validate CSV headers (`sensor_id`, `timestamp`, `value`), normalize casing.
2. Stream rows via `csv.DictReader`; parse ISO 8601 timestamps and floats.
3. Skip invalid rows, append error metadata, and continue processing.
4. Aggregate metrics: row count, min, max, mean, per-sensor counts.
5. Persist summary + errors to mock datastore with status:
   - `uploaded` -> `processing` -> `processed`
   - `partial` if at least one row failed but others succeeded
   - `failed` on unhandled exceptions (with message)

## Mock Services

- **Mock S3**
  - API: `put_object(bucket, key, data)`, `get_object(bucket, key)`, `list_objects(bucket)`.
  - Storage: in-memory index with optional persistence to `MOCK_S3_ROOT_PATH` for inspection.
  - Real-world counterpart: Amazon S3 standard bucket.

- **Mock DynamoDB**
  - API: `put_item(table, item)`, `get_item(table, key)`, `scan`.
  - Storage: in-memory dict with periodic write-through JSON file (`MOCK_DYNAMODB_PERSISTENCE_PATH`).
  - Mirrors DynamoDB partition key + item semantics.

## Concurrency Notes

- Upload endpoint immediately persists the file and submits a job to a shared thread pool (`PROCESSOR_WORKER_COUNT` size).
- Thread pool chosen over async coroutines to simplify CPU-friendly CSV parsing without complex event loops.
- Scaling strategy: increase worker count, run multiple service instances, or replace with distributed task queue (Celery/SQS) in production.

## Logging

- Centralized configuration applies a contextual formatter that appends `file_id`, `object_key`, `status`, `row_count`, and other metadata when present.
- Use `LOG_LEVEL` to control verbosity (`INFO` by default); set to `DEBUG` to surface detailed lifecycle messages.
- Row-level validation failures emit warnings with structured context so skipped records can be traced without interrupting successful processing.

## CLI

- Install requirements and then invoke `python -m cli --help` for command details.
- Upload a file: `python -m cli upload ./samples/readings.csv`
- Upload and wait for completion: `python -m cli upload ./samples/readings.csv --wait`
- Fetch a result later: `python -m cli result <file_id>`
- Override the target API via `API_BASE_URL` or the `--base-url` option; tweak polling behaviour with `CLI_POLL_INTERVAL` / `CLI_POLL_TIMEOUT` or per-command flags.

## Testing

```bash
pytest
```

Coverage highlights:
- Aggregator edge cases (invalid rows, min/max/mean math).
- Mock S3 and DynamoDB persistence/streaming semantics.
- FastAPI integration (upload + polling lifecycle, error paths).
- Processor concurrency (parallel worker test) and row-level error logging.
- CLI flows (upload, wait, result rendering) and settings overrides.

## Design Notes & Future Enhancements

- Swap mocks with real AWS clients behind shared interfaces.
- Add authentication/authorization to endpoints.
- CLI helper (upload/process/results) implemented with Typer.
- Optional Dockerfile + docker-compose for local orchestration.
- Web UI to visualize uploaded files and aggregates.
- Metrics/observability (Prometheus, structured logs, tracing).

## Project Structure

```
app/
  main.py          # FastAPI wiring & static mounts
  api.py           # JSON API routes
  web.py           # UI routes + Jinja templates
  schemas.py       # Pydantic models
cli/
  app.py           # Typer entrypoint
  client.py        # HTTP client
  render.py        # Console output helpers
services/
  processor.py     # Upload orchestration + threading
  aggregator.py    # Pure aggregation logic
storage/
  mock_s3.py       # S3-style object store
datastore/
  mock_dynamodb.py # Dynamo-style metadata store
models/
  records.py       # SensorReading dataclass
static/
  css/, js/        # UI assets (polling & multi-upload helpers)
tests/
  test_app.py, test_processor.py, test_cli.py, ...
```

## Troubleshooting

- **`ValueError` during parsing:** Ensure the CSV headers match exactly and timestamps are ISO 8601.
- **Results stuck in `processing`:** Check background worker logs; increase `PROCESSOR_WORKERS` if backlog accumulates.
- **Mock storage not persisting:** Verify `MOCK_S3_ROOT` and `MOCK_DB_PATH` directories exist and are writable.

---

Reach out or open an issue if anything here is unclear or you hit a setup snag. Happy hacking!
