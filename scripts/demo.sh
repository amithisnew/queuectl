#!/bin/bash
set -e
echo "🚀 QueueCTL Quick Demo Starting..."

if [ -d "venv" ]; then
  source venv/bin/activate
  echo "✅ Virtual environment activated."
fi

rm -f queuectl.db
echo "🧹 Old database removed."

python -m src.cli init
echo "✅ Database initialized."

echo "📦 Enqueueing success job..."
python -m src.cli enqueue --file examples/job_success.json

echo "📦 Enqueueing failing job..."
python -m src.cli enqueue '{"id":"fail-demo","command":"bash -c '\''exit 1'\''","max_retries":1}'

echo "👷 Running worker..."
python -m src.cli worker start --count 1 --limit 2

echo "📊 System Status:"
python -m src.cli status

echo "🪦 DLQ contents:"
python -m src.cli dlq list

echo "🎉 Demo completed!"
