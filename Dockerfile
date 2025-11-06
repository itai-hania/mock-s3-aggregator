# syntax=docker/dockerfile:1
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

ENV MOCK_S3_BUCKET_NAME=uploads \
    MOCK_S3_ROOT_PATH=./tmp/mock_s3 \
    MOCK_DYNAMODB_TABLE_NAME=processing_results \
    MOCK_DYNAMODB_PERSISTENCE_PATH=./tmp/mock_db.json \
    PROCESSOR_WORKER_COUNT=4 \
    LOG_LEVEL=INFO

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
