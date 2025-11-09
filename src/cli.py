"""
CLI interface for QueueCTL using Click framework.
"""

import click
import json
import sys
from pathlib import Path
from tabulate import tabulate
from .storage import Storage
from .worker import WorkerManager
from .config import Config
from .utils import setup_logging
import logging

logger = logging.getLogger(__name__)


@click.group()
@click.option('--db', default='queuectl.db', help='Database file path')
@click.option('--log-level', default='INFO', help='Logging level')
@click.pass_context
def cli(ctx, db, log_level):
    """QueueCTL - Production-grade job queue system"""
    ctx.ensure_object(dict)
    ctx.obj['db_path'] = db
    ctx.obj['log_level'] = log_level
    setup_logging(log_level)


@cli.command()
@click.pass_context
def init(ctx):
    """Initialize the database"""
    db_path = ctx.obj['db_path']
    storage = Storage(db_path)
    storage.init_db()
    click.echo(f"✓ Database initialized: {db_path}")


@cli.command()
@click.argument('job_json', required=False)
@click.option('--file', '-f', type=click.Path(exists=True), help='Job JSON file')
@click.pass_context
def enqueue(ctx, job_json, file):
    """Enqueue a job from JSON string or file"""
    db_path = ctx.obj['db_path']
    storage = Storage(db_path)
    config = Config(storage)
    
    # Load job data
    if file:
        with open(file, 'r') as f:
            job_data = json.load(f)
    elif job_json:
        try:
            job_data = json.loads(job_json)
        except json.JSONDecodeError as e:
            click.echo(f"✗ Invalid JSON: {e}", err=True)
            sys.exit(1)
    else:
        click.echo("✗ Provide either job JSON string or --file option", err=True)
        sys.exit(1)
    
    # Validate required fields
    if 'id' not in job_data:
        click.echo("✗ Job must have 'id' field", err=True)
        sys.exit(1)
    
    if 'command' not in job_data:
        click.echo("✗ Job must have 'command' field", err=True)
        sys.exit(1)
    
    # Apply defaults from config
    if 'max_retries' not in job_data:
        job_data['max_retries'] = config.get_int('max_retries', 3)
    
    # Validate max_retries
    if not isinstance(job_data['max_retries'], int) or job_data['max_retries'] < 0:
        click.echo("✗ max_retries must be a non-negative integer", err=True)
        sys.exit(1)
    
    # Enqueue the job
    success = storage.enqueue_job(job_data)
    
    if success:
        click.echo(f"✓ Job enqueued: {job_data['id']}")
        click.echo(f"  Command: {job_data['command']}")
        click.echo(f"  Max retries: {job_data['max_retries']}")
    else:
        click.echo(f"✗ Failed to enqueue job: ID '{job_data['id']}' already exists", err=True)
        sys.exit(1)


@cli.group()
def worker():
    """Worker management commands"""
    pass


@worker.command()
@click.option('--count', default=1, help='Number of worker processes')
@click.option('--base', default=None, type=int, help='Backoff base for retries')
@click.option('--limit', default=None, type=int, help='Max jobs to process (for testing)')
@click.pass_context
def start(ctx, count, base, limit):
    """Start worker processes"""
    db_path = ctx.obj['db_path']
    storage = Storage(db_path)
    config = Config(storage)
    
    # Apply config defaults if not provided
    if base is None:
        base = config.get_int('backoff_base', 2)
    
    if count is None:
        count = config.get_int('worker_default_count', 1)
    
    click.echo(f"Starting {count} worker(s)...")
    click.echo(f"  Database: {db_path}")
    click.echo(f"  Backoff base: {base}")
    if limit:
        click.echo(f"  Job limit: {limit}")
    
    try:
        manager = WorkerManager(db_path, count, base, limit)
        manager.start()
    except KeyboardInterrupt:
        click.echo("\n✓ Workers stopped")
    except Exception as e:
        click.echo(f"✗ Error: {e}", err=True)
        sys.exit(1)


@worker.command()
@click.pass_context
def stop(ctx):
    """Stop all workers"""
    db_path = ctx.obj['db_path']
    
    # Read PID file and send signal
    pidfile = Path('.queuectl.pid')
    if not pidfile.exists():
        click.echo("✗ No workers running (PID file not found)", err=True)
        sys.exit(1)
    
    try:
        import os
        import signal
        
        with open(pidfile, 'r') as f:
            pid = int(f.read().strip())
        
        os.kill(pid, signal.SIGTERM)
        click.echo(f"✓ Sent stop signal to worker manager (PID: {pid})")
        
    except (ValueError, OSError) as e:
        click.echo(f"✗ Failed to stop workers: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def status(ctx):
    """Show system status"""
    db_path = ctx.obj['db_path']
    storage = Storage(db_path)
    
    # Get job counts
    counts = storage.get_job_counts()
    
    # Get active workers
    workers = storage.get_active_workers()
    
    click.echo("=== QueueCTL Status ===\n")
    
    # Job statistics
    click.echo("Job Statistics:")
    job_table = [
        ["Pending", counts['pending']],
        ["Processing", counts['processing']],
        ["Completed", counts['completed']],
        ["Failed", counts['failed']],
        ["Dead (DLQ)", counts['dead']],
        ["Total", sum(counts.values())]
    ]
    click.echo(tabulate(job_table, headers=["State", "Count"], tablefmt="simple"))
    
    # Worker information
    click.echo(f"\nActive Workers: {len(workers)}")
    if workers:
        worker_table = []
        for w in workers:
            worker_table.append([
                w['worker_id'][:8],
                w['pid'],
                w['started_at'],
                w['last_heartbeat']
            ])
        click.echo(tabulate(
            worker_table,
            headers=["Worker ID", "PID", "Started", "Last Heartbeat"],
            tablefmt="simple"
        ))


@cli.command()
@click.option('--state', help='Filter by job state')
@click.option('--limit', default=100, help='Maximum number of jobs to list')
@click.pass_context
def list(ctx, state, limit):
    """List jobs"""
    db_path = ctx.obj['db_path']
    storage = Storage(db_path)
    
    jobs = storage.list_jobs(state=state, limit=limit)
    
    if not jobs:
        click.echo("No jobs found")
        return
    
    # Format job table
    job_table = []
    for job in jobs:
        job_table.append([
            job['id'][:16],
            job['command'][:40],
            job['state'],
            f"{job['attempts']}/{job['max_retries']}",
            job['next_run_at'][:19],
            job['updated_at'][:19]
        ])
    
    click.echo(tabulate(
        job_table,
        headers=["Job ID", "Command", "State", "Attempts", "Next Run", "Updated"],
        tablefmt="simple"
    ))
    click.echo(f"\nTotal: {len(jobs)} job(s)")


@cli.group()
def dlq():
    """Dead Letter Queue commands"""
    pass


@dlq.command('list')
@click.pass_context
def dlq_list(ctx):
    """List dead jobs"""
    db_path = ctx.obj['db_path']
    storage = Storage(db_path)
    
    jobs = storage.list_jobs(state='dead', limit=1000)
    
    if not jobs:
        click.echo("No dead jobs in DLQ")
        return
    
    # Format job table
    job_table = []
    for job in jobs:
        error_preview = (job['last_error'] or '')[:50]
        job_table.append([
            job['id'][:16],
            job['command'][:30],
            job['attempts'],
            error_preview,
            job['updated_at'][:19]
        ])
    
    click.echo(tabulate(
        job_table,
        headers=["Job ID", "Command", "Attempts", "Last Error", "Updated"],
        tablefmt="simple"
    ))
    click.echo(f"\nTotal: {len(jobs)} dead job(s)")


@dlq.command('retry')
@click.argument('job_id')
@click.option('--no-reset-attempts', is_flag=True, help='Keep attempt counter')
@click.pass_context
def dlq_retry(ctx, job_id, no_reset_attempts):
    """Retry a dead job"""
    db_path = ctx.obj['db_path']
    storage = Storage(db_path)
    
    success = storage.retry_dlq_job(job_id, reset_attempts=not no_reset_attempts)
    
    if success:
        click.echo(f"✓ Job moved from DLQ to pending: {job_id}")
        if not no_reset_attempts:
            click.echo("  Attempts reset to 0")
    else:
        click.echo(f"✗ Job not found in DLQ: {job_id}", err=True)
        sys.exit(1)


@dlq.command('delete')
@click.argument('job_id')
@click.pass_context
def dlq_delete(ctx, job_id):
    """Delete a dead job"""
    db_path = ctx.obj['db_path']
    storage = Storage(db_path)
    
    success = storage.delete_dlq_job(job_id)
    
    if success:
        click.echo(f"✓ Job deleted from DLQ: {job_id}")
    else:
        click.echo(f"✗ Job not found in DLQ: {job_id}", err=True)
        sys.exit(1)


@cli.group()
def config():
    """Configuration management"""
    pass


@config.command('set')
@click.argument('key')
@click.argument('value')
@click.pass_context
def config_set(ctx, key, value):
    """Set configuration value"""
    db_path = ctx.obj['db_path']
    storage = Storage(db_path)
    cfg = Config(storage)
    
    cfg.set(key, value)
    click.echo(f"✓ Config set: {key} = {value}")


@config.command('get')
@click.argument('key')
@click.pass_context
def config_get(ctx, key):
    """Get configuration value"""
    db_path = ctx.obj['db_path']
    storage = Storage(db_path)
    cfg = Config(storage)
    
    value = cfg.get(key)
    if value is not None:
        click.echo(f"{key} = {value}")
    else:
        click.echo(f"✗ Config key not found: {key}", err=True)
        sys.exit(1)


@config.command('show')
@click.pass_context
def config_show(ctx):
    """Show all configuration"""
    db_path = ctx.obj['db_path']
    storage = Storage(db_path)
    cfg = Config(storage)
    
    all_config = cfg.get_all()
    
    if not all_config:
        click.echo("No configuration set (using defaults)")
        return
    
    config_table = [[k, v] for k, v in all_config.items()]
    click.echo(tabulate(config_table, headers=["Key", "Value"], tablefmt="simple"))


if __name__ == '__main__':
    cli(obj={})