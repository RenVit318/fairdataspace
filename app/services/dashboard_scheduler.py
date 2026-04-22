"""Dashboard scheduler — periodic refresh of aggregate statistics."""

import atexit
import fcntl
import logging
import os
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger(__name__)

# Keep reference so the lock file stays open for the process lifetime.
_lock_file = None


def _run_refresh(app):
    """Execute dashboard refresh (called by scheduler)."""
    from app.services.dashboard_service import refresh_all
    try:
        with app.app_context():
            refresh_all()
    except Exception as e:
        logger.error(f'Scheduled dashboard refresh failed: {e}')


def init_scheduler(app):
    """Start the dashboard background scheduler.

    Uses a file lock so that only one Gunicorn worker runs the scheduler.
    Other workers silently skip initialization.
    """
    global _lock_file

    data_dir = os.path.join(app.root_path, 'data', 'dashboard')
    os.makedirs(data_dir, exist_ok=True)
    lock_path = os.path.join(data_dir, '.scheduler.lock')

    _lock_file = open(lock_path, 'w')
    try:
        fcntl.flock(_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError):
        logger.debug('Another worker owns the dashboard scheduler, skipping')
        return

    interval = app.config.get('DASHBOARD_REFRESH_INTERVAL', 86400)

    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(
        _run_refresh,
        'interval',
        seconds=interval,
        id='dashboard_refresh',
        replace_existing=True,
        next_run_time=datetime.now(),  # run immediately on startup
        args=[app],
    )
    scheduler.start()
    logger.info(f'Dashboard scheduler started (interval: {interval}s)')

    atexit.register(lambda: scheduler.shutdown(wait=False))
