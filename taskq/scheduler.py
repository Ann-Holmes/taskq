"""
scheduler.py

TaskQ scheduler module. Implements the task scheduling loop, status management,
and task execution with environment restoration.

This module provides functions to start, stop, and query the status of the
scheduler. The scheduler reads pending tasks from the database, restores the
submission environment, and executes each task in order of priority and creation time.

Author: ender
"""

import os
import time
import subprocess
from .resources import is_system_overloaded
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from .db import (
    init_db,
    get_tasks,
    update_task_status,
    update_task_pid,
    update_task_start_time,
    update_task_end_time,
)
from .utils import get_taskq_config_dir, setup_logging
from loguru import logger

SCHEDULER_STATUS_FILE = os.path.join(get_taskq_config_dir(), "scheduler.status")

# Initialize logging
setup_logging()


def set_scheduler_status(status: str):
    """
    Set the scheduler status.

    Parameters
    ----------
    status : str
        The status to set ('running' or 'stopped').
    """
    logger.info(f"Setting scheduler status to {status}")
    with open(SCHEDULER_STATUS_FILE, "w") as f:
        f.write(status)


def get_scheduler_status():
    """
    Get the current scheduler status.

    Returns
    -------
    str
        The current status ('running' or 'stopped').
    """
    if not os.path.exists(SCHEDULER_STATUS_FILE):
        print("Scheduler status file not found. Returning 'stopped'.")
        logger.info("Scheduler status file not found. Returning 'stopped'.")
        return "stopped"
    with open(SCHEDULER_STATUS_FILE, "r") as f:
        status = f.read().strip()
        print(f"Retrieved scheduler status: {status}")
        logger.info(f"Retrieved scheduler status: {status}")
        return status


def scheduler_loop():
    """
    Main scheduling loop.

    Continuously polls the database for pending tasks, restores their
    submission environment and working directory, and executes them one by one.
    The loop runs until the scheduler status is set to 'stopped'.
    """
    set_scheduler_status("running")
    print("Scheduler started.")
    logger.info("Scheduler started.")
    # Dynamically adjust max_workers based on system load
    max_workers = 2 if is_system_overloaded() else 5
    # Initialize sleep_interval for exponential backoff
    sleep_interval = 2
    executor = ProcessPoolExecutor(max_workers=max_workers)  # Maximum parallel tasks
    try:
        # Initialize the database once at the start
        init_db()
        while get_scheduler_status() == "running":
            # Select all pending tasks
            pending = get_tasks(status=["pending"])
            if is_system_overloaded():
                logger.info("System is overloaded. Pausing task scheduling.")
                time.sleep(30)  # Wait before next poll, because system is overloaded
                continue

            if pending:
                for task in pending[:5]:  # Limit to 5 tasks at a time
                    logger.info(f"Submitting task {task.id}: {task.name}")
                    executor.submit(execute_task, task)
                    time.sleep(10)  # Wait for task initialization
            else:
                # No pending tasks, sleep before next poll
                # Implement exponential backoff for sleep intervals
                sleep_interval = min(sleep_interval * 2, 60) if not pending else 5
                time.sleep(sleep_interval)
    finally:
        executor.shutdown(wait=True)
        set_scheduler_status("stopped")
        logger.info("Scheduler stopped.")


def execute_task(task):
    """
    Execute a single task in a separate process.

    Parameters
    ----------
    task : Task
        The task object to execute.
    """
    try:
        update_task_status(task.id, "running")
        update_task_start_time(task.id, datetime.now().isoformat())
        env = task.environment if isinstance(task.environment, dict) else None
        cwd = task.cwd if task.cwd and os.path.isdir(task.cwd) else None
        with open(task.stdout_file, "a") as fout, open(task.stderr_file, "a") as ferr:
            proc = subprocess.Popen(
                task.command,
                shell=True,
                env=env,
                cwd=cwd,
                stdout=fout,
                stderr=ferr,
                text=True,
            )
            update_task_pid(task.id, proc.pid)
            timeout = task.timeout
            if timeout is None or timeout == 0:
                proc.wait()
            else:
                proc.wait(timeout=timeout)
        update_task_status(task.id, "completed")
        update_task_end_time(task.id, datetime.now().isoformat())
        logger.info(f"Task {task.id} completed.")
    except Exception as e:
        logger.error(f"Task execution failed: {e}")
        update_task_status(task.id, "failed")
        update_task_end_time(task.id, datetime.now().isoformat())


def start_scheduler():
    """
    Start the scheduler if not already running.

    Ensures only one scheduler instance is running by checking the status file.
    Runs the scheduling loop in the foreground.
    """
    if get_scheduler_status() == "running":
        print("Scheduler is already running.")
        logger.info("Scheduler already running.")
        return
    scheduler_loop()


def stop_scheduler():
    """
    Stop the scheduler by setting its status to 'stopped'.
    """
    if get_scheduler_status() != "running":
        print("Scheduler is not running.")
        logger.info("Scheduler is not running.")
        return
    set_scheduler_status("stopped")
    print("Scheduler stopped.")
    logger.info("Scheduler stopped.")


def status_scheduler():
    """
    Print the current scheduler status.
    """
    status = get_scheduler_status()
    print(f"Scheduler status: {status}")
    logger.info(f"Scheduler status: {status}")
