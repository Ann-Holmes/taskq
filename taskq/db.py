"""
db.py

TaskQ database module (SQLAlchemy ORM version).

Implements all database operations for the task queue, including initialization,
task creation, status updates, and task retrieval. Uses SQLAlchemy ORM.

Author: ender
"""

import os
from datetime import datetime
from .utils import get_taskq_config_dir, setup_logging
from .models import Task, get_session
from loguru import logger

DB_PATH = os.path.join(get_taskq_config_dir(), "taskq.db")
setup_logging()


def init_db():
    """
    Initialize the database and create tables if not exist.

    This function ensures the database schema is up-to-date.
    """
    # SQLAlchemy自动建表，无需手写DDL
    logger.info(f"Initializing database at {DB_PATH}")
    get_session(DB_PATH).close()
    logger.info("Database initialized successfully")


def add_task(
    name: str,
    command: str,
    priority: int,
    environment=None,
    cwd=None,
    stdout_file=None,
    stderr_file=None,
    timeout=None,
):
    """
    Add a new task to the database.

    Parameters
    ----------
    name : str
        Task name for display.
    command : str
        The actual shell command to execute.
    priority : int
        Task priority (lower value means higher priority).
    environment : dict or None
        Environment variables to serialize and store (optional).
    cwd : str or None
        Working directory to store (optional).
    stdout_file : str or None
        Absolute path to stdout log file.
    stderr_file : str or None
        Absolute path to stderr log file.
    timeout : int or None
        Timeout in seconds (None or 0 means unlimited).
    """
    logger.info(f"Adding task: {name}, command: {command}, priority: {priority}")
    session = get_session(DB_PATH)
    task = Task(
        name=name,
        command=command,
        priority=priority,
        created_at=datetime.now(),
        status="pending",
        environment=environment if environment is not None else {},
        cwd=cwd,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        timeout=timeout,
    )
    session.add(task)
    session.commit()
    logger.info(f"Task added successfully: {task}")
    session.close()
    return task


def get_tasks(status: list = None):
    """
    Retrieve tasks from the database, optionally filtered by status, ordered by priority and
    creation time.

    Parameters
    ----------
    status : list of str or None
        List of status values to filter by (e.g., ["pending", "running"]). If None, return all
        tasks.

    Returns
    -------
    list of Task
        List of Task ORM objects.
    """
    logger.info(f"Retrieving tasks with status: {status}")
    session = get_session(DB_PATH)
    q = session.query(Task)
    if status:
        q = q.filter(Task.status.in_(status))
    q = q.order_by(Task.priority.asc(), Task.created_at.asc())
    tasks = q.all()
    session.close()
    logger.info(f"Retrieved {len(tasks)} tasks")
    return tasks


def get_task_by_id(task_id: int):
    """
    Retrieve a single task by its ID.

    Parameters
    ----------
    task_id : int
        Task ID.

    Returns
    -------
    Task or None
        Task ORM object, or None if not found.
    """
    logger.info(f"Retrieving task by ID: {task_id}")
    session = get_session(DB_PATH)
    t = session.query(Task).filter(Task.id == task_id).first()
    session.close()
    logger.info(f"Task retrieved: {t}")
    return t


def update_task_status(task_id: int, status: str):
    """
    Update the status of a task.

    Parameters
    ----------
    task_id : int
        Task ID.
    status : str
        New status ('pending', 'running', 'completed', 'cancelled', 'failed').
    """
    logger.info(f"Updating status for task ID {task_id} to {status}")
    session = get_session(DB_PATH)
    t = session.query(Task).filter(Task.id == task_id).first()
    if t:
        t.status = status
        session.commit()
    session.close()


def update_task_pid(task_id: int, pid: int):
    """
    Update the pid of a task.

    Parameters
    ----------
    task_id : int
        Task ID.
    pid : int
        Process ID to record.
    """
    logger.info(f"Updating PID for task ID {task_id} to {pid}")
    session = get_session(DB_PATH)
    t = session.query(Task).filter(Task.id == task_id).first()
    if t:
        t.pid = pid
        session.commit()
    session.close()


def update_task_start_time(task_id: int, start_time: str):
    """
    Update the start_time of a task.

    Parameters
    ----------
    task_id : int
        Task ID.
    start_time : str
        ISO format datetime string.
    """
    logger.info(f"Updating start time for task ID {task_id} to {start_time}")
    session = get_session(DB_PATH)
    t = session.query(Task).filter(Task.id == task_id).first()
    if t:
        t.start_time = datetime.fromisoformat(start_time)
        session.commit()
    session.close()


def update_task_end_time(task_id: int, end_time: str):
    """
    Update the end_time of a task.

    Parameters
    ----------
    task_id : int
        Task ID.
    end_time : str
        ISO format datetime string.
    """
    logger.info(f"Updating end time for task ID {task_id} to {end_time}")
    session = get_session(DB_PATH)
    t = session.query(Task).filter(Task.id == task_id).first()
    if t:
        t.end_time = datetime.fromisoformat(end_time)
        session.commit()
    session.close()
