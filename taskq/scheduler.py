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
import json
import time
import subprocess
from datetime import datetime
from .db import (
    init_db,
    get_tasks,
    update_task_status,
    update_task_pid,
    update_task_start_time,
    update_task_end_time,
)
from .utils import get_taskq_config_dir

SCHEDULER_STATUS_FILE = os.path.join(get_taskq_config_dir(), "scheduler.status")


def set_scheduler_status(status: str):
    """
    Set the scheduler status.

    Parameters
    ----------
    status : str
        The status to set ('running' or 'stopped').
    """
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
        return "stopped"
    with open(SCHEDULER_STATUS_FILE, "r") as f:
        return f.read().strip()


def scheduler_loop():
    """
    Main scheduling loop.

    Continuously polls the database for pending tasks, restores their
    submission environment and working directory, and executes them one by one.
    The loop runs until the scheduler status is set to 'stopped'.
    """
    set_scheduler_status("running")
    print("Scheduler started.")
    try:
        while get_scheduler_status() == "running":
            init_db()
            # Select all pending tasks
            pending = get_tasks(status=["pending"])
            if pending:
                task = pending[0]
                print(f"Running task {task[0]}: {task[1]}")
                update_task_status(task[0], "running")
                update_task_start_time(task[0], datetime.now().isoformat())
                # Parse environment variables and working directory
                env = None
                cwd = None
                try:
                    if task[5]:
                        env = json.loads(task[5])
                    if task[6]:
                        cwd = task[6]
                except Exception as e:
                    print(f"Failed to parse environment/cwd: {e}")
                # Execute the task command in the restored environment
                try:
                    # task[7]: stdout_file, task[8]: stderr_file
                    with open(task[7], "a") as fout, open(task[8], "a") as ferr:
                        proc = subprocess.Popen(
                            task[1],
                            shell=True,
                            env=env,
                            cwd=cwd,
                            stdout=fout,
                            stderr=ferr,
                            text=True,
                        )
                        update_task_pid(task[0], proc.pid)
                        # task[10]: timeout
                        timeout = task[10]
                        if timeout is None or timeout == 0:
                            proc.wait()
                        else:
                            proc.wait(timeout=timeout)
                    print(f"Task output redirected to: {task[7]}")
                    print(f"Task error output redirected to: {task[8]}")
                    update_task_status(task[0], "completed")
                    update_task_end_time(task[0], datetime.now().isoformat())
                    print(f"Task {task[0]} completed.")
                except Exception as e:
                    print(f"Task execution failed: {e}")
                    update_task_status(task[0], "failed")
                    update_task_end_time(task[0], datetime.now().isoformat())
            else:
                # No pending tasks, sleep before next poll
                time.sleep(1)
    finally:
        set_scheduler_status("stopped")
        print("Scheduler stopped.")


def start_scheduler():
    """
    Start the scheduler if not already running.

    Ensures only one scheduler instance is running by checking the status file.
    Runs the scheduling loop in the foreground.
    """
    if get_scheduler_status() == "running":
        print("Scheduler already running.")
        return
    scheduler_loop()


def stop_scheduler():
    """
    Stop the scheduler by setting its status to 'stopped'.
    """
    if get_scheduler_status() != "running":
        print("Scheduler is not running.")
        return
    set_scheduler_status("stopped")
    print("Stopping scheduler...")


def status_scheduler():
    """
    Print the current scheduler status.
    """
    status = get_scheduler_status()
    print(f"Scheduler status: {status}")
