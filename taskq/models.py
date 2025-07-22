"""
models.py

TaskQ ORM models using SQLAlchemy.

This module defines the core ORM data models for TaskQ, based on SQLAlchemy.
It provides the Task model for structured task storage and database mapping,
as well as utility functions for engine and session management.

Contents:
- Task: ORM mapping for the task table, including all task metadata fields.
- get_engine: Obtain a SQLAlchemy database engine.
- get_session: Obtain a SQLAlchemy session for database operations.

Dependencies:
- SQLAlchemy >= 1.4
- Python >= 3.7

Author: ender
"""

from sqlalchemy import Column, Integer, String, DateTime, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.types import JSON
from datetime import datetime

Base = declarative_base()


class Task(Base):
    """
    ORM model for the 'tasks' table.

    Fields:
    - id: Primary key, auto-incremented task ID.
    - name: Task name, for display and search.
    - command: The actual shell command to execute.
    - priority: Task priority, integer, lower means higher priority.
    - created_at: Task creation timestamp, auto-generated.
    - status: Task status ('pending', 'running', 'completed', 'cancelled', 'failed').
    - environment: Environment variables at submission (JSON dict).
    - cwd: Working directory at submission.
    - stdout_file: Path to stdout log file.
    - stderr_file: Path to stderr log file.
    - pid: Process ID when running.
    - timeout: Timeout in seconds, None/0 means unlimited.
    - start_time: Actual start time of the task.
    - end_time: End/failure/cancel time of the task.
    """

    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True, autoincrement=True)  # Primary key
    name = Column(String(128), nullable=False)  # Task name
    command = Column(Text, nullable=False)  # Shell command to execute
    priority = Column(Integer, nullable=False, default=0)  # Priority
    created_at = Column(DateTime, nullable=False, default=datetime.now)  # Creation time
    status = Column(String(32), nullable=False, default="pending")  # Status
    environment = Column(JSON, nullable=True)  # Environment variables (JSON)
    cwd = Column(String(256), nullable=True)  # Working directory
    stdout_file = Column(String(256), nullable=True)  # Stdout log file path
    stderr_file = Column(String(256), nullable=True)  # Stderr log file path
    pid = Column(Integer, nullable=True)  # Process ID
    timeout = Column(Integer, nullable=True)  # Timeout (seconds)
    start_time = Column(DateTime, nullable=True)  # Start time
    end_time = Column(DateTime, nullable=True)  # End time


def get_engine(db_path):
    """
    Get a SQLAlchemy database engine.

    Parameters
    ----------
    db_path : str
        Path to the SQLite database file.

    Returns
    -------
    engine : sqlalchemy.Engine
        SQLAlchemy database engine object.
    """
    return create_engine(f"sqlite:///{db_path}", echo=False, future=True)


def get_session(db_path):
    """
    Get a SQLAlchemy ORM session for database operations.

    Parameters
    ----------
    db_path : str
        Path to the SQLite database file.

    Returns
    -------
    session : sqlalchemy.orm.Session
        SQLAlchemy ORM session object.
    """
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    return Session()
