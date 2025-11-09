"""
Worker process management for QueueCTL.
Handles job acquisition, execution, and graceful shutdown.
"""

import multiprocessing as mp
import signal
import time
import uuid
import os
import logging
from typing import Optional
from pathlib import Path
from .storage import Storage
from .executor import Executor
from .config import Config

logger = logging.getLogger(__name__)


class Worker:
    """Individual worker process that executes jobs."""
    
    def __init__(self, worker_id: str, db_path: str, backoff_base: int, job_limit: Optional[int] = None):
        self.worker_id = worker_id
        self.db_path = db_path
        self.backoff_base = backoff_base
        self.job_limit = job_limit
        self.shutdown_requested = False
        self.jobs_processed = 0
    
    def setup_signal_handlers(self):
        """Setup graceful shutdown handlers."""
        def shutdown_handler(signum, frame):
            logger.info(f"Worker {self.worker_id}: Shutdown signal received")
            self.shutdown_requested = True
        
        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
    
    def run(self):
        """Main worker loop."""
        self.setup_signal_handlers()
        
        storage = Storage(self.db_path)
        config = Config(storage)
        executor = Executor()
        
        # Register worker
        storage.register_worker(self.worker_id, os.getpid())
        logger.info(f"Worker {self.worker_id} started (PID: {os.getpid()})")
        
        try:
            while not self.shutdown_requested:
                # Check job limit
                if self.job_limit and self.jobs_processed >= self.job_limit:
                    logger.info(f"Worker {self.worker_id}: Job limit reached ({self.job_limit})")
                    break
                
                # Acquire next job
                job = storage.acquire_job(self.worker_id)
                
                if job:
                    logger.info(f"Worker {self.worker_id}: Processing job {job['id']}")
                    logger.info(f"  Command: {job['command']}")
                    logger.info(f"  Attempt: {job['attempts']}/{job['max_retries']}")
                    
                    # Execute the job
                    start_time = time.time()
                    result = executor.execute(job['command'])
                    elapsed = time.time() - start_time
                    
                    logger.info(f"  Exit code: {result.returncode}")
                    logger.info(f"  Duration: {elapsed:.2f}s")
                    
                    # Update job based on result
                    if result.returncode == 0:
                        storage.update_job_success(job['id'])
                        logger.info(f"Worker {self.worker_id}: Job {job['id']} completed successfully")
                    else:
                        error_msg = result.stderr[:500] if result.stderr else f"Exit code: {result.returncode}"
                        storage.update_job_failure(job['id'], error_msg, self.backoff_base)
                        logger.warning(f"Worker {self.worker_id}: Job {job['id']} failed")
                        if result.stderr:
                            logger.warning(f"  Error: {result.stderr[:200]}")
                    
                    self.jobs_processed += 1
                    
                    # Update heartbeat
                    storage.update_worker_heartbeat(self.worker_id)
                else:
                    # No jobs available, sleep briefly
                    poll_interval = config.get_float('poll_interval', 1.0)
                    time.sleep(poll_interval)
                    
                    # Update heartbeat
                    storage.update_worker_heartbeat(self.worker_id)
        
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Fatal error: {e}", exc_info=True)
        finally:
            # Unregister worker
            storage.unregister_worker(self.worker_id)
            logger.info(f"Worker {self.worker_id}: Stopped (processed {self.jobs_processed} jobs)")


class WorkerManager:
    """Manages multiple worker processes."""
    
    def __init__(self, db_path: str, worker_count: int = 1, backoff_base: int = 2, job_limit: Optional[int] = None):
        self.db_path = db_path
        self.worker_count = worker_count
        self.backoff_base = backoff_base
        self.job_limit = job_limit
        self.processes = []
        self.shutdown_requested = False
    
    def setup_signal_handlers(self):
        """Setup graceful shutdown handlers."""
        def shutdown_handler(signum, frame):
            logger.info("WorkerManager: Shutdown signal received")
            self.shutdown_requested = True
            self.stop()
        
        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
    
    def start(self):
        """Start worker processes."""
        self.setup_signal_handlers()
        
        # Write PID file for worker stop command
        pidfile = Path('.queuectl.pid')
        with open(pidfile, 'w') as f:
            f.write(str(os.getpid()))
        
        # Recover any abandoned jobs
        storage = Storage(self.db_path)
        config = Config(storage)
        abandoned_threshold = config.get_int('abandoned_threshold', 3600)
        storage.recover_abandoned_jobs(abandoned_threshold)
        
        # Start worker processes
        for i in range(self.worker_count):
            worker_id = f"worker-{uuid.uuid4().hex[:8]}"
            process = mp.Process(
                target=self._run_worker,
                args=(worker_id,)
            )
            process.start()
            self.processes.append(process)
            logger.info(f"Started worker process: {worker_id} (PID: {process.pid})")
        
        try:
            # Wait for all processes to complete
            for process in self.processes:
                process.join()
        finally:
            # Clean up PID file
            if pidfile.exists():
                pidfile.unlink()
    
    def _run_worker(self, worker_id: str):
        """Run a single worker (called in subprocess)."""
        worker = Worker(worker_id, self.db_path, self.backoff_base, self.job_limit)
        worker.run()
    
    def stop(self):
        """Stop all worker processes gracefully."""
        logger.info("Stopping all workers...")
        
        for process in self.processes:
            if process.is_alive():
                process.terminate()
        
        # Wait for graceful shutdown
        timeout = 30
        for process in self.processes:
            process.join(timeout=timeout)
            if process.is_alive():
                logger.warning(f"Worker {process.pid} did not stop gracefully, forcing...")
                process.kill()
        
        logger.info("All workers stopped")