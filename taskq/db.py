"""
db.py

TaskQ database module.

Implements all database operations for the task queue, including initialization,
task creation, status updates, and task retrieval. Uses SQLite as the backend.

Author: ender
"""

import os
import sqlite3
from datetime import datetime
import json
from .utils import get_taskq_config_dir

DB_PATH = os.path.join(get_taskq_config_dir(), "taskq.db")


def get_connection():
    """
    Create and return a new SQLite connection.

    Returns
    -------
    sqlite3.Connection
        SQLite connection object.
    """
    conn = sqlite3.connect(DB_PATH)
    return conn


def init_db():
    """
    Initialize the tasks table in the database if it does not exist.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            priority INTEGER NOT NULL,
            created_at DATETIME NOT NULL,
            status TEXT NOT NULL,
            environment TEXT,
            cwd TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def add_task(name: str, priority: int, environment=None, cwd=None):
    """
    Add a new task to the database.

    Parameters
    ----------
    name : str
        Task name or command to execute.
    priority : int
        Task priority (lower value means higher priority).
    environment : dict or None
        Environment variables to serialize and store (optional).
    cwd : str or None
        Working directory to store (optional).
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO tasks (name, priority, created_at, status, environment, cwd)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            name,
            priority,
            datetime.now().isoformat(),
            "pending",
            json.dumps(environment) if environment else None,
            cwd,
        ),
    )
    conn.commit()
    conn.close()


def get_tasks():
    """
    Retrieve all tasks from the database, ordered by priority and creation time.

    Returns
    -------
    list of tuple
        List of task records, each as a tuple:
        (id, name, priority, created_at, status, environment, cwd)
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, name, priority, created_at, status, environment, cwd
        FROM tasks
        ORDER BY priority ASC, created_at ASC
        """
    )
    tasks = cursor.fetchall()
    conn.close()
    return tasks


def update_task_status(task_id: int, status: str):
    """
    Update the status of a task.

    Parameters
    ----------
    task_id : int
        Task ID.
    status : str
        New status ('pending', 'running', 'completed', 'cancelled').
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE tasks SET status = ? WHERE id = ?
        """,
        (status, task_id),
    )
    conn.commit()
    conn.close()


def get_task_by_id(task_id: int):
    """
    Retrieve a single task by its ID.

    Parameters
    ----------
    task_id : int
        Task ID.

    Returns
    -------
    tuple or None
        Task record as a tuple, or None if not found.
        (id, name, priority, created_at, status, environment, cwd)
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, name, priority, created_at, status, environment, cwd
        FROM tasks
        WHERE id = ?
        """,
        (task_id,),
    )
    task = cursor.fetchone()
    conn.close()
    return task
