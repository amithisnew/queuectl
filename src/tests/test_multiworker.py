"""
Concurrent worker processing tests (simulated multi-worker non-overlap).
"""

import pytest
import tempfile
import time
import multiprocessing as mp
from src.storage import Storage


def worker_func(db_path, worker_id, processed_ids):
    s = Storage(db_path)
    job = s.acquire_job(worker_id)
    if job:
        processed_ids.append(job['id'])
        s.update_job_success(job['id'])


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "multi.db")


def test_multiworker_no_overlap(db_path):
    """Ensure multiple workers don't pick same job concurrently."""
    s = Storage(db_path)
    s.init_db()
    for i in range(5):
        s.enqueue_job({'id': f'job{i}', 'command': 'echo', 'max_retries': 1})

    manager = mp.Manager()
    processed = manager.list()

    procs = [
        mp.Process(target=worker_func, args=(db_path, f'w{i}', processed))
        for i in range(5)
    ]
    [p.start() for p in procs]
    [p.join() for p in procs]

    # Each job should be processed exactly once
    assert len(processed) == 5
    assert len(set(processed)) == 5
