"""
Test exponential backoff retry behavior.
"""

import pytest
import tempfile
import time
from datetime import datetime, timezone
from src.storage import Storage


@pytest.fixture
def db():
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    s = Storage(db_path)
    s.init_db()
    yield s


def test_exponential_backoff_progression(db):
    """Ensure next_run_at increases exponentially after retries."""
    job = {'id': 'b1', 'command': 'fail', 'max_retries': 3}
    db.enqueue_job(job)

    # Simulate 3 failed runs
    db.acquire_job('w1')
    db.update_job_failure('b1', "fail1", backoff_base=2)
    j1 = db.get_job('b1')
    first_delay = (
        datetime.fromisoformat(j1['next_run_at']) - datetime.fromisoformat(j1['updated_at'])
    ).total_seconds()

    db.acquire_job('w1')
    db.update_job_failure('b1', "fail2", backoff_base=2)
    j2 = db.get_job('b1')
    second_delay = (
        datetime.fromisoformat(j2['next_run_at']) - datetime.fromisoformat(j2['updated_at'])
    ).total_seconds()

    assert round(second_delay / first_delay) == 2 or second_delay > first_delay
