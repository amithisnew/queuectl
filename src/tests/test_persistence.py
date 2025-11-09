"""
Persistence tests for QueueCTL.
"""

import tempfile
import os
from src.storage import Storage


def test_persistence_across_restart(tmp_path):
    """Jobs must persist after reloading the DB."""
    db_path = str(tmp_path / "persist.db")
    s1 = Storage(db_path)
    s1.init_db()
    job = {'id': 'p1', 'command': 'echo 1'}
    s1.enqueue_job(job)

    # New instance simulating restart
    s2 = Storage(db_path)
    j = s2.get_job('p1')
    assert j is not None
    assert j['state'] == 'pending'
