"""
Storage module for QueueCTL - handles all database operations with SQLite.
Implements atomic locking for multi-worker job acquisition.
"""

import sqlite3
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class Storage:
    """
    SQLite-based storage for job queue with atomic locking support.
    """
    
    def __init__(self, db_path: str = "queuectl.db"):
        self.db_path = db_path
        self.init_db()
    
    @contextmanager
    def get_connection(self):
        """Get a database connection with row factory."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        # Enable WAL mode for better concurrency
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
        finally:
            conn.close()
    
    def init_db(self):
        """Initialize database schema."""
        with self.get_connection() as conn:
            # Jobs table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    command TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempts INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT,
                    locked_by TEXT,
                    locked_at TEXT
                )
            """)
            
            # Indexes for performance
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_state_next_run 
                ON jobs(state, next_run_at)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_locked_by 
                ON jobs(locked_by)
            """)
            
            # Config table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            
            # Workers table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    pid INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    last_heartbeat TEXT NOT NULL
                )
            """)
            
            conn.commit()
            logger.info(f"Database initialized: {self.db_path}")
    
    def enqueue_job(self, job: Dict[str, Any]) -> bool:
        """
        Enqueue a new job.
        
        Args:
            job: Job dictionary with id, command, and optional fields
            
        Returns:
            True if successful, False if job ID already exists
        """
        try:
            with self.get_connection() as conn:
                now = datetime.now(timezone.utc).isoformat()
                
                conn.execute("""
                    INSERT INTO jobs (
                        id, command, state, attempts, max_retries,
                        created_at, updated_at, next_run_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    job['id'],
                    job['command'],
                    'pending',
                    0,
                    job.get('max_retries', 3),
                    now,
                    now,
                    job.get('next_run_at', now)
                ))
                conn.commit()
                logger.info(f"Job enqueued: {job['id']}")
                return True
        except sqlite3.IntegrityError:
            logger.error(f"Job ID already exists: {job['id']}")
            return False
    
    def acquire_job(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """
        Atomically acquire the next available job for processing.
        
        Args:
            worker_id: Unique identifier for the worker
            
        Returns:
            Job dictionary or None if no jobs available
        """
        with self.get_connection() as conn:
            try:
                # BEGIN IMMEDIATE to get exclusive write lock
                conn.execute("BEGIN IMMEDIATE")
                
                now = datetime.now(timezone.utc).isoformat()
                
                # Find next available job
                cursor = conn.execute("""
                    SELECT * FROM jobs
                    WHERE state = 'pending'
                    AND next_run_at <= ?
                    ORDER BY created_at
                    LIMIT 1
                """, (now,))
                
                row = cursor.fetchone()
                
                if row:
                    job_id = row['id']
                    
                    # Atomically lock the job
                    conn.execute("""
                        UPDATE jobs
                        SET state = 'processing',
                            locked_by = ?,
                            locked_at = ?,
                            attempts = attempts + 1,
                            updated_at = ?
                        WHERE id = ?
                    """, (worker_id, now, now, job_id))
                    
                    conn.commit()
                    
                    # Fetch the updated job
                    cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
                    job = dict(cursor.fetchone())
                    logger.info(f"Job acquired: {job_id} by worker {worker_id}")
                    return job
                else:
                    conn.commit()
                    return None
                    
            except sqlite3.OperationalError as e:
                conn.rollback()
                logger.warning(f"Failed to acquire job: {e}")
                return None
    
    def update_job_success(self, job_id: str):
        """Mark job as completed successfully."""
        with self.get_connection() as conn:
            now = datetime.now(timezone.utc).isoformat()
            conn.execute("""
                UPDATE jobs
                SET state = 'completed',
                    updated_at = ?,
                    locked_by = NULL,
                    locked_at = NULL
                WHERE id = ?
            """, (now, job_id))
            conn.commit()
            logger.info(f"Job completed: {job_id}")
    
    def update_job_failure(self, job_id: str, error: str, backoff_base: int = 2):
        """
        Update job after failure with retry logic.
        
        Args:
            job_id: Job identifier
            error: Error message
            backoff_base: Base for exponential backoff calculation
        """
        with self.get_connection() as conn:
            now = datetime.now(timezone.utc)
            
            # Get current job state
            cursor = conn.execute("""
                SELECT attempts, max_retries FROM jobs WHERE id = ?
            """, (job_id,))
            row = cursor.fetchone()
            
            if not row:
                logger.error(f"Job not found: {job_id}")
                return
            
            attempts = row['attempts']
            max_retries = row['max_retries']
            
            if attempts >= max_retries:
                # Move to DLQ
                conn.execute("""
                    UPDATE jobs
                    SET state = 'dead',
                        updated_at = ?,
                        last_error = ?,
                        locked_by = NULL,
                        locked_at = NULL
                    WHERE id = ?
                """, (now.isoformat(), error, job_id))
                logger.info(f"Job moved to DLQ: {job_id}")
            else:
                # Calculate exponential backoff
                delay_seconds = backoff_base ** attempts
                next_run = now + timedelta(seconds=delay_seconds)
                
                conn.execute("""
                    UPDATE jobs
                    SET state = 'failed',
                        updated_at = ?,
                        next_run_at = ?,
                        last_error = ?,
                        locked_by = NULL,
                        locked_at = NULL
                    WHERE id = ?
                """, (now.isoformat(), next_run.isoformat(), error, job_id))
                logger.info(f"Job failed, will retry in {delay_seconds}s: {job_id}")
            
            conn.commit()
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job by ID."""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def list_jobs(self, state: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        List jobs with optional filtering.
        
        Args:
            state: Filter by job state (optional)
            limit: Maximum number of jobs to return
            
        Returns:
            List of job dictionaries
        """
        with self.get_connection() as conn:
            if state:
                cursor = conn.execute("""
                    SELECT * FROM jobs 
                    WHERE state = ? 
                    ORDER BY created_at DESC 
                    LIMIT ?
                """, (state, limit))
            else:
                cursor = conn.execute("""
                    SELECT * FROM jobs 
                    ORDER BY created_at DESC 
                    LIMIT ?
                """, (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_job_counts(self) -> Dict[str, int]:
        """Get count of jobs by state."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT state, COUNT(*) as count 
                FROM jobs 
                GROUP BY state
            """)
            counts = {row['state']: row['count'] for row in cursor.fetchall()}
            
            # Ensure all states are present
            for state in ['pending', 'processing', 'completed', 'failed', 'dead']:
                counts.setdefault(state, 0)
            
            return counts
    
    def retry_dlq_job(self, job_id: str, reset_attempts: bool = True):
        """
        Retry a job from the Dead Letter Queue.
        
        Args:
            job_id: Job identifier
            reset_attempts: Whether to reset attempt counter
        """
        with self.get_connection() as conn:
            now = datetime.now(timezone.utc).isoformat()
            
            if reset_attempts:
                conn.execute("""
                    UPDATE jobs
                    SET state = 'pending',
                        attempts = 0,
                        next_run_at = ?,
                        updated_at = ?,
                        last_error = NULL
                    WHERE id = ? AND state = 'dead'
                """, (now, now, job_id))
            else:
                conn.execute("""
                    UPDATE jobs
                    SET state = 'pending',
                        next_run_at = ?,
                        updated_at = ?,
                        last_error = NULL
                    WHERE id = ? AND state = 'dead'
                """, (now, now, job_id))
            
            if conn.total_changes > 0:
                conn.commit()
                logger.info(f"Job retried from DLQ: {job_id}")
                return True
            else:
                logger.warning(f"Job not found in DLQ: {job_id}")
                return False
    
    def delete_dlq_job(self, job_id: str) -> bool:
        """Delete a job from the Dead Letter Queue."""
        with self.get_connection() as conn:
            conn.execute("""
                DELETE FROM jobs WHERE id = ? AND state = 'dead'
            """, (job_id,))
            
            if conn.total_changes > 0:
                conn.commit()
                logger.info(f"Job deleted from DLQ: {job_id}")
                return True
            else:
                logger.warning(f"Job not found in DLQ: {job_id}")
                return False
    
    def recover_abandoned_jobs(self, threshold_seconds: int = 3600):
        """
        Recover jobs that have been processing for too long (likely abandoned).
        
        Args:
            threshold_seconds: Time threshold for considering a job abandoned
        """
        with self.get_connection() as conn:
            cutoff = (datetime.now(timezone.utc) - 
                     timedelta(seconds=threshold_seconds)).isoformat()
            
            cursor = conn.execute("""
                SELECT id, locked_by FROM jobs
                WHERE state = 'processing'
                AND locked_at < ?
            """, (cutoff,))
            
            abandoned_jobs = cursor.fetchall()
            
            if abandoned_jobs:
                conn.execute("""
                    UPDATE jobs
                    SET state = 'pending',
                        locked_by = NULL,
                        locked_at = NULL
                    WHERE state = 'processing'
                    AND locked_at < ?
                """, (cutoff,))
                conn.commit()
                
                logger.warning(f"Recovered {len(abandoned_jobs)} abandoned jobs")
                for job in abandoned_jobs:
                    logger.info(f"  - Job {job['id']} (was locked by {job['locked_by']})")
    
    # Configuration methods
    
    def set_config(self, key: str, value: str):
        """Set a configuration value."""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO config (key, value)
                VALUES (?, ?)
            """, (key, value))
            conn.commit()
            logger.info(f"Config set: {key} = {value}")
    
    def get_config(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get a configuration value."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT value FROM config WHERE key = ?
            """, (key,))
            row = cursor.fetchone()
            return row['value'] if row else default
    
    def get_all_config(self) -> Dict[str, str]:
        """Get all configuration values."""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT key, value FROM config")
            return {row['key']: row['value'] for row in cursor.fetchall()}
    
    # Worker management methods
    
    def register_worker(self, worker_id: str, pid: int):
        """Register a worker."""
        with self.get_connection() as conn:
            now = datetime.now(timezone.utc).isoformat()
            conn.execute("""
                INSERT OR REPLACE INTO workers (worker_id, pid, started_at, last_heartbeat)
                VALUES (?, ?, ?, ?)
            """, (worker_id, pid, now, now))
            conn.commit()
            logger.info(f"Worker registered: {worker_id} (PID: {pid})")
    
    def unregister_worker(self, worker_id: str):
        """Unregister a worker."""
        with self.get_connection() as conn:
            conn.execute("DELETE FROM workers WHERE worker_id = ?", (worker_id,))
            conn.commit()
            logger.info(f"Worker unregistered: {worker_id}")
    
    def update_worker_heartbeat(self, worker_id: str):
        """Update worker heartbeat."""
        with self.get_connection() as conn:
            now = datetime.now(timezone.utc).isoformat()
            conn.execute("""
                UPDATE workers SET last_heartbeat = ? WHERE worker_id = ?
            """, (now, worker_id))
            conn.commit()
    
    def get_active_workers(self) -> List[Dict[str, Any]]:
        """Get list of active workers."""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM workers ORDER BY started_at")
            return [dict(row) for row in cursor.fetchall()]