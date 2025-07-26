"""
resources.py

This module provides system resource monitoring functionality for TaskQ.
It uses the psutil library to monitor CPU and memory usage in real-time.

Dependencies:
- psutil >= 5.0

Author: ender
"""

import psutil


def get_system_load():
    """
    Get the current system load, including CPU and memory usage.

    Returns
    -------
    dict
        A dictionary containing 'cpu_usage' (percentage) and 'memory_usage' (percentage).
    """
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_usage = psutil.virtual_memory().percent
    return {"cpu_usage": cpu_usage, "memory_usage": memory_usage}


def is_system_overloaded(cpu_threshold=80, memory_threshold=75):
    """
    Check if the system is overloaded based on CPU and memory thresholds.

    Parameters
    ----------
    cpu_threshold : int, optional
        The CPU usage percentage threshold (default is 80).
    memory_threshold : int, optional
        The memory usage percentage threshold (default is 75).

    Returns
    -------
    bool
        True if the system is overloaded, False otherwise.
    """
    load = get_system_load()
    return load["cpu_usage"] > cpu_threshold or load["memory_usage"] > memory_threshold
