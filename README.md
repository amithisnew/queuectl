# QueueCTL

QueueCTL is a **CLI-based background job queue system** built in Python using SQLite.  
It supports asynchronous task execution, retries with exponential backoff, and a  
**Dead Letter Queue (DLQ)** for failed jobs.

---

## 📦 Features

- Persistent storage via SQLite  
- Multi-worker parallel execution  
- Exponential backoff retry logic  
- Dead Letter Queue (DLQ) management  
- Configurable runtime parameters  
- Detailed logging and test coverage  

---

## 🧰 Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

🧩 Usage Overview

python -m src.cli init
python -m src.cli enqueue --file examples/job_success.json
python -m src.cli worker start --count 1 --limit 1
python -m src.cli status

Dead Letter Queue

python -m src.cli dlq list
python -m src.cli dlq retry <job_id>
python -m src.cli dlq delete <job_id>

🧪 DLQ Lifecycle Demo

./scripts/test_dlq.sh
Performs: Init → Enqueue → Fail → DLQ → Retry → Fail → DLQ → Delete

⚙️ Quick Demo

./scripts/demo.sh

🧾 Testing

pytest -v src/tests/

🏗️ Project Structure

queuectl/
├── src/
│   ├── cli.py
│   ├── storage.py
│   ├── worker.py
│   ├── executor.py
│   ├── config.py
│   ├── utils.py
│   └── tests/
├── scripts/
│   ├── demo.sh
│   └── test_dlq.sh
├── examples/
│   ├── job_success.json
│   └── job_failure.json
├── README.md
├── ARCHITECTURE.md
├── requirements.txt
└── setup.py