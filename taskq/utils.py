"""
utils.py

Utility functions for TaskQ.

Author: ender
"""

import os


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
