"""
Basic functionality tests for QueueCTL.
"""

import pytest
import tempfile
import os
import time
from pathlib import Path
from src.storage import Storage
from src.executor import Executor


@pytest.fixture
def temp_db():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    storage = Storage(db_path)
    storage.init_db()
    yield storage
    Path(db_path).unlink(missing_ok=True)


def test_database_initialization(temp_db):
    """Ensure all required tables exist."""
    with temp_db.get_connection() as conn:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row['name'] for row in cursor.fetchall()]
    assert 'jobs' in tables
    assert 'config' in tables
    assert 'workers' in tables


def test_enqueue_and_get_job(temp_db):
    """Test job enqueueing and retrieval."""
    job = {'id': 'job1', 'command': 'echo "Hi"', 'max_retries': 3}
    assert temp_db.enqueue_job(job)
    j = temp_db.get_job('job1')
    assert j is not None
    assert j['state'] == 'pending'


def test_duplicate_job_id(temp_db):
    """Ensure duplicate job IDs are rejected."""
    job = {'id': 'dup', 'command': 'echo 1'}
    assert temp_db.enqueue_job(job)
    assert not temp_db.enqueue_job(job)


def test_acquire_job_and_processing_state(temp_db):
    """Verify atomic job acquisition."""
    job = {'id': 'a1', 'command': 'echo 123'}
    temp_db.enqueue_job(job)
    job_acquired = temp_db.acquire_job('worker-1')
    assert job_acquired is not None
    assert job_acquired['state'] == 'processing'
    assert job_acquired['locked_by'] == 'worker-1'
    j = temp_db.get_job('a1')
    assert j['attempts'] == 1


def test_update_job_success(temp_db):
    """Verify job moves to completed after success."""
    job = {'id': 's1', 'command': 'echo done'}
    temp_db.enqueue_job(job)
    temp_db.acquire_job('worker-x')
    temp_db.update_job_success('s1')
    j = temp_db.get_job('s1')
    assert j['state'] == 'completed'
    assert j['locked_by'] is None


def test_update_job_failure_and_backoff(temp_db):
    """Ensure job failure sets retry and next_run_at."""
    job = {'id': 'f1', 'command': 'exit 1', 'max_retries': 3}
    temp_db.enqueue_job(job)
    temp_db.acquire_job('worker-x')
    temp_db.update_job_failure('f1', "failed test", backoff_base=2)
    j = temp_db.get_job('f1')
    assert j['state'] in ('failed', 'dead')
    assert j['last_error'] is not None
    assert 'failed' in j['last_error'] or j['state'] == 'dead'


def test_dlq_retry_and_delete(temp_db):
    """Test DLQ retry and delete flow."""
    job = {'id': 'dlq1', 'command': 'fail', 'max_retries': 1}
    temp_db.enqueue_job(job)
    temp_db.acquire_job('w1')
    # Force move to DLQ
    temp_db.update_job_failure('dlq1', "fail", backoff_base=2)
    temp_db.update_job_failure('dlq1', "fail again", backoff_base=2)
    j = temp_db.get_job('dlq1')
    assert j['state'] == 'dead'
    # Retry from DLQ
    assert temp_db.retry_dlq_job('dlq1')
    j2 = temp_db.get_job('dlq1')
    assert j2['state'] == 'pending'
    # Delete from DLQ
    temp_db.acquire_job('w2')
    temp_db.update_job_failure('dlq1', "final fail", backoff_base=2)
    temp_db.update_job_failure('dlq1', "final fail2", backoff_base=2)
    assert temp_db.delete_dlq_job('dlq1') is True


def test_executor_success_and_fail():
    """Verify Executor behavior for success/failure."""
    ex = Executor()
    res1 = ex.execute('echo "OK"')
    assert res1.returncode == 0
    res2 = ex.execute('exit 1')
    assert res2.returncode != 0
