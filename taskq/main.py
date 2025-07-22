"""
main.py

TaskQ command-line interface module.

Provides CLI commands for initializing the database, submitting, listing,
cancelling tasks, and controlling the scheduler. All commands are accessible
via the 'taskq' CLI entry point.

Author: ender
"""

import argparse
import os
from .db import init_db, add_task, get_tasks


def cmd_init(args):
    """
    Initialize the task database.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments (unused).
    """
    init_db()
    print("Database initialized.")


def cmd_submit(args):
    """
    Submit a new task to the queue.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments with 'name' and 'priority' attributes.
    """
    init_db()
    add_task(args.name, args.priority, environment=dict(os.environ), cwd=os.getcwd())
    print(f"Task submitted: {args.name} (priority={args.priority})")


def cmd_list(args):
    """
    List all tasks in the queue.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments (unused).
    """
    init_db()
    tasks = get_tasks()
    print("ID | Name | Priority | Created At | Status")
    for t in tasks:
        print(f"{t[0]} | {t[1]} | {t[2]} | {t[3]} | {t[4]}")


def cmd_cancel(args):
    """
    Cancel a pending or running task by ID.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments with 'id' attribute.
    """
    from .db import get_task_by_id, update_task_status

    init_db()
    task = get_task_by_id(args.id)
    if not task:
        print(f"Task {args.id} not found.")
        return
    if task[4] not in ("pending", "running"):
        print(f"Task {args.id} cannot be cancelled (status: {task[4]}).")
        return
    update_task_status(args.id, "cancelled")
    print(f"Task {args.id} cancelled.")


def cmd_start(args):
    """
    Start the scheduler in the foreground.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments (unused).
    """
    from .scheduler import start_scheduler

    start_scheduler()


def cmd_stop(args):
    """
    Stop the running scheduler.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments (unused).
    """
    from .scheduler import stop_scheduler

    stop_scheduler()


def cmd_status(args):
    """
    Show the current scheduler status.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments (unused).
    """
    from .scheduler import status_scheduler

    status_scheduler()


def main():
    """
    Main entry point for the taskq CLI.

    Parses command-line arguments and dispatches to the appropriate command handler.
    """
    parser = argparse.ArgumentParser(prog="taskq", description="Lightweight queue system")
    subparsers = parser.add_subparsers(dest="command")

    parser_init = subparsers.add_parser("init", help="Initialize database")
    parser_init.set_defaults(func=cmd_init)

    parser_submit = subparsers.add_parser("submit", help="Submit a new task")
    parser_submit.add_argument("name", type=str, help="Task name")
    parser_submit.add_argument("priority", type=int, help="Task priority (lower is higher)")
    parser_submit.set_defaults(func=cmd_submit)

    parser_list = subparsers.add_parser("list", help="List all tasks")
    parser_list.set_defaults(func=cmd_list)

    parser_cancel = subparsers.add_parser("cancel", help="Cancel a task")
    parser_cancel.add_argument("id", type=int, help="Task ID")
    parser_cancel.set_defaults(func=cmd_cancel)

    parser_start = subparsers.add_parser("start", help="Start the scheduler")
    parser_start.set_defaults(func=cmd_start)

    parser_stop = subparsers.add_parser("stop", help="Stop the scheduler")
    parser_stop.set_defaults(func=cmd_stop)

    parser_status = subparsers.add_parser("status", help="Show scheduler status")
    parser_status.set_defaults(func=cmd_status)

    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
