"""
utils.py

Utility functions for TaskQ.

Author: ender
"""

import os
from loguru import logger


def get_taskq_config_dir():
    """
    Get the path to the taskq configuration directory (~/.taskq), creating it if necessary.

    Returns
    -------
    str
        Absolute path to the ~/.taskq directory.
    """
    home = os.path.expanduser("~/.taskq")
    if not os.path.exists(home):
        os.makedirs(home, exist_ok=True)
    return home


def resolve_path(path, cwd=None):
    """
    Resolve a file path to absolute, using cwd as base if relative.

    Parameters
    ----------
    path : str
        The file path to resolve.
    cwd : str or None
        The base directory for relative paths. If None, uses os.getcwd().

    Returns
    -------
    str
        Absolute file path.
    """
    if os.path.isabs(path):
        return path
    if cwd is None:
        cwd = os.getcwd()
    return os.path.abspath(os.path.join(cwd, path))


def validate_priority(priority):
    """
    Validate task priority.

    Parameters
    ----------
    priority : int

    Returns
    -------
    bool
        True if valid, False otherwise.
    """
    return isinstance(priority, int) and 0 <= priority <= 9


def validate_timeout(timeout):
    """
    Validate task timeout.

    Parameters
    ----------
    timeout : int or None

    Returns
    -------
    bool
        True if valid, False otherwise.
    """
    return timeout is None or (isinstance(timeout, int) and timeout >= 0)


def setup_logging():
    """
    Configure the loguru logger to write logs to the ~/.taskq/taskq.log file.

    Logs are rotated when they reach 10MB, and the last 5 log files are retained.
    """
    log_dir = get_taskq_config_dir()
    log_file = os.path.join(log_dir, "taskq.log")
    logger.remove()
    logger.add(
        log_file, rotation="10 MB", retention=5, level="DEBUG", format="{time} {level} {message}"
    )
