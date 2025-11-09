# QueueCTL Architecture
QueueCTL is a Python-based job queue system using SQLite for persistent storage.

## 🧩 Core Modules
| Module | Responsibility |
|---------|----------------|
| `cli.py` | Command-line interface using Click |
| `storage.py` | SQLite database operations, job persistence, and DLQ handling |
| `worker.py` | Worker management, job acquisition, and execution |
| `executor.py` | Safe command execution and output capture |
| `config.py` | Configuration storage and retrieval |
| `utils.py` | Logging and utilities |

## 🧠 Workflow
1. Enqueue Job → CLI adds new job to the database.  
2. Worker Acquisition → Worker locks a pending job.  
3. Execution → Job runs as a subprocess.  
4. Retry or Success → Job retries on failure or marks completed.  
5. DLQ Handling → Exceeded retries → moved to DLQ.  
6. DLQ Management → Jobs can be listed, retried, or deleted.

## ⚙️ Data Model
- **jobs**: id, command, state, attempts, max_retries, next_run_at, last_error, locked_by, locked_at  
- **workers**: active worker tracking  
- **config**: key-value settings  

## 🔐 Concurrency
SQLite WAL mode ensures atomic locks via `BEGIN IMMEDIATE` transactions.

## 🧾 Logging
Configurable via:
```bash
python -m src.cli config set log_level DEBUG
```

## 🧪 Testing
Unit tests validate enqueue, retry, DLQ, and concurrency behavior.
