"""
main.py

TaskQ command-line interface module.

This module provides the main CLI entry point for TaskQ, including commands for:
- Initializing the database
- Submitting new tasks
- Listing tasks with filtering and formatting
- Cancelling tasks (with process termination)
- Starting/stopping the scheduler
- Querying scheduler status

All commands are accessible via the 'taskq' CLI entry point.

Author: ender
"""

import argparse
import signal
import os
from .db import init_db, add_task, get_tasks
from .utils import resolve_path, validate_priority, validate_timeout, setup_logging
from loguru import logger

# Initialize logging
setup_logging()


def cmd_init(args):
    print("Initializing the database.")
    logger.info("Initializing the database.")
    """
    Initialize the task database.

    This command ensures the database schema is up-to-date and ready for use.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments (unused).
    """
    init_db()
    print("Database initialized successfully.")
    logger.info("Database initialized successfully.")


def cmd_submit(args):
    """
    Submit a new task to the queue.

    This command validates input parameters, resolves file paths, and creates a new task
    in the database with all required metadata. If validation fails, an error is printed.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments with 'name', 'priority', 'stdout', 'stderr' attributes.
    """
    try:
        init_db()
        cwd = os.getcwd()
        # Validate and resolve stdout/stderr file paths
        stdout_file = args.stdout if args.stdout else "stdout.log"
        stderr_file = args.stderr if args.stderr else "stderr.log"
        if not isinstance(stdout_file, str) or not isinstance(stderr_file, str):
            print("Error: stdout and stderr file paths must be strings.")
            logger.error("Error: stdout and stderr file paths must be strings.")
            return
        stdout_file = resolve_path(stdout_file, cwd)
        stderr_file = resolve_path(stderr_file, cwd)
        # Validate priority
        if not validate_priority(args.priority):
            print("Error: priority must be between 0 and 9.")
            logger.error("Error: priority must be between 0 and 9.")
            return
        # Validate timeout
        if not validate_timeout(args.timeout):
            print("Error: timeout must be a non-negative integer or None.")
            logger.error("Error: timeout must be a non-negative integer or None.")
            return
        # Determine task name
        task_name = args.name
        if not task_name:
            # Use first 12 chars of command + ... if too long
            task_name = args.command[:12] + ("..." if len(args.command) > 12 else "")
        add_task(
            task_name,
            args.command,
            args.priority,
            environment=dict(os.environ),
            cwd=cwd,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            timeout=args.timeout,
        )
        print(f"Task submitted: {task_name} (priority={args.priority})")
        logger.info(f"Task submitted: {task_name} (priority={args.priority})")
    except Exception as e:
        print(f"Failed to submit task: {e}")
        logger.error(f"Failed to submit task: {e}")


def cmd_list(args):
    """
    List all tasks in the queue, optionally filtered by status.

    This command retrieves all tasks from the database (optionally filtered by status),
    formats them as a table, and prints to the console. Duration is calculated for running
    and completed/failed tasks.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments, may include 'status' attribute.
    """
    from datetime import datetime

    init_db()
    allowed_status = {"pending", "running", "completed", "cancelled", "failed"}
    status = args.status if hasattr(args, "status") and args.status else None
    if status:
        invalid = [s for s in status if s not in allowed_status]
        if invalid:
            logger.error(f"Invalid status: {', '.join(invalid)}")
            logger.info(f"Allowed status: {', '.join(sorted(allowed_status))}")
            return
    tasks = get_tasks(status)
    # Table columns: ID, Name, Priority, Date, Time, Status, PID, Duration
    headers = ["ID", "Name", "Priority", "Date", "Time", "Status", "PID", "Duration"]
    col_widths = [6, 18, 10, 12, 10, 12, 8, 12]
    # Prepare rows
    rows = []
    now = datetime.now()
    for t in tasks:
        # t: Task ORM object
        try:
            dt = t.created_at
            date_str = dt.strftime("%Y-%m-%d")
            time_str = dt.strftime("%H:%M:%S")
        except Exception:
            date_str = str(t.created_at)[:10]
            time_str = str(t.created_at)[11:19]
        name = t.name
        if len(name) > col_widths[1]:
            name = name[: col_widths[1] - 3] + "..."
        pid_str = str(t.pid) if t.pid is not None else "-"
        # Duration logic
        duration_str = "-"
        try:
            if t.status == "running" and t.start_time:
                start = t.start_time
                duration = now - start
                duration_str = str(duration).split(".")[0]
            elif t.status in ("completed", "failed") and t.start_time and t.end_time:
                start = t.start_time
                end = t.end_time
                duration = end - start
                duration_str = str(duration).split(".")[0]
        except Exception:
            duration_str = "-"
        row = [
            str(t.id).ljust(col_widths[0]),
            name.ljust(col_widths[1]),
            str(t.priority).ljust(col_widths[2]),
            date_str.ljust(col_widths[3]),
            time_str.ljust(col_widths[4]),
            t.status.ljust(col_widths[5]),
            pid_str.ljust(col_widths[6]),
            duration_str.ljust(col_widths[7]),
        ]
        rows.append(row)
    # Print header
    print("Listing tasks:")
    logger.info("Listing tasks:")
    print(" ".join(h.ljust(w) for h, w in zip(headers, col_widths)))
    logger.info(" ".join(h.ljust(w) for h, w in zip(headers, col_widths)))
    print("-" * (sum(col_widths) + len(col_widths) - 1))
    logger.info("-" * (sum(col_widths) + len(col_widths) - 1))
    for row in rows:
        print(" ".join(row))
        logger.info(" ".join(row))


def cmd_cancel(args):
    """
    Cancel a pending or running task by ID.

    This command cancels a task by updating its status in the database. If the task is
    currently running and has a valid PID, it will attempt to send SIGTERM to the process.

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
        logger.error(f"Task {args.id} not found.")
        return
    # Use ORM attribute access
    if task.status not in ("pending", "running"):
        print(f"Task {args.id} cannot be cancelled (status: {task.status}).")
        logger.error(f"Task {args.id} cannot be cancelled (status: {task.status}).")
        return
    # If running, try to terminate the process
    if task.status == "running" and task.pid:
        try:
            os.kill(task.pid, signal.SIGTERM)
            print(f"Sent SIGTERM to process {task.pid}.")
            logger.info(f"Sent SIGTERM to process {task.pid}.")
        except Exception as e:
            print(f"Failed to terminate process {task.pid}: {e}")
            logger.error(f"Failed to terminate process {task.pid}: {e}")
    update_task_status(args.id, "cancelled")
    print(f"Task {args.id} cancelled.")
    logger.info(f"Task {args.id} cancelled.")


def cmd_start(args):
    """
    Start the scheduler in the foreground.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments (unused).
    """
    from .scheduler import start_scheduler

    print("Starting the scheduler.")
    logger.info("Starting the scheduler.")
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

    print("Stopping the scheduler.")
    logger.info("Stopping the scheduler.")
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

    print("Querying the scheduler status.")
    logger.info("Querying the scheduler status.")
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
    parser_submit.add_argument("command", type=str, help="Task command to execute")
    parser_submit.add_argument(
        "--name",
        type=str,
        default=None,
        help="Task name (default: first 12 chars of command + ... if too long)",
    )
    parser_submit.add_argument(
        "-p",
        "--priority",
        type=int,
        choices=range(0, 10),
        default=0,
        help="Task priority (0-9, lower is higher, default: 0)",
    )
    parser_submit.add_argument(
        "--stdout",
        type=str,
        default=None,
        help="Path to stdout log file (default: ./stdout.log in cwd)",
    )
    parser_submit.add_argument(
        "--stderr",
        type=str,
        default=None,
        help="Path to stderr log file (default: ./stderr.log in cwd)",
    )
    parser_submit.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Timeout for this task in seconds (default: unlimited)",
    )
    parser_submit.set_defaults(func=cmd_submit)

    parser_list = subparsers.add_parser("list", help="List all tasks")
    parser_list.add_argument(
        "-s",
        "--status",
        action="append",
        help=(
            "Filter by task status: pending, running, completed, cancelled, failed. "
            "Note: can specify multiple"
        ),
    )
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
