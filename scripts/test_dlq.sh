#!/bin/bash
# ==============================================================
# QueueCTL DLQ Lifecycle Demo Script
# Demonstrates job -> fail -> DLQ -> retry -> DLQ -> delete
# ==============================================================

set -e  # Exit on first error
echo "🚀 Starting QueueCTL DLQ demo..."
echo "========================================"

# Activate virtual environment if it exists
if [ -d "venv" ]; then
  source venv/bin/activate
  echo "✅ Virtual environment activated."
else
  echo "⚠️ No virtual environment found. Make sure dependencies are installed."
fi

# Clean up old DB
rm -f queuectl.db
echo "🧹 Old database removed (if any)."

# Initialize new DB
python -m src.cli init
echo "✅ Database initialized."

# Enqueue a failing job
python -m src.cli enqueue '{"id":"dlq-demo","command":"bash -c '\''exit 1'\''","max_retries":1}'
echo "✅ Failing job enqueued (will always exit 1)."

# Start a worker to process the job
echo "👷 Running worker..."
python -m src.cli worker start --count 1 --limit 1

# Show job states
echo "📋 Listing all jobs:"
python -m src.cli list

# Show DLQ contents
echo "🗃️ DLQ contents:"
python -m src.cli dlq list

# Retry DLQ job
echo "🔁 Retrying DLQ job..."
python -m src.cli dlq retry dlq-demo

# Run worker again
echo "👷 Running worker again (retry)..."
python -m src.cli worker start --count 1 --limit 1

# Show DLQ again
echo "🗃️ DLQ contents after retry:"
python -m src.cli dlq list

# Delete DLQ job
echo "🗑️ Deleting DLQ job..."
python -m src.cli dlq delete dlq-demo

# Final DLQ check
echo "✅ Final DLQ status:"
python -m src.cli dlq list

echo "========================================"
echo "🎉 DLQ lifecycle demo complete!"
