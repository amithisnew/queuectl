#!/bin/bash
# Integration test for QueueCTL
set -e

echo "=== Initializing QueueCTL Database ==="
python -m src.cli init

echo "=== Enqueue test jobs ==="
python -m src.cli enqueue --file examples/job_success.json
python -m src.cli enqueue --file examples/job_failure.json

echo "=== Starting workers (limit 3 jobs) ==="
python -m src.cli worker start --count 2 --limit 3

echo "=== Checking status ==="
python -m src.cli status

echo "=== Listing DLQ ==="
python -m src.cli dlq list || true

chmod +x scripts/integration_test.sh
