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
        Parsed command-line arguments with 'name', 'priority', 'stdout', 'stderr' attributes.
    """
    init_db()
    cwd = os.getcwd()
    # Resolve stdout/stderr file paths to absolute paths
    stdout_file = args.stdout if args.stdout else "stdout.log"
    stderr_file = args.stderr if args.stderr else "stderr.log"
    if not os.path.isabs(stdout_file):
        stdout_file = os.path.abspath(os.path.join(cwd, stdout_file))
    if not os.path.isabs(stderr_file):
        stderr_file = os.path.abspath(os.path.join(cwd, stderr_file))
    # Determine task name
    task_name = args.name
    if not task_name:
        # Use first 12 chars of command + ... if too long
        task_name = args.command[:12] + ("..." if len(args.command) > 12 else "")
    add_task(
        task_name,
        args.priority,
        environment=dict(os.environ),
        cwd=cwd,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        timeout=args.timeout,
    )
    print(f"Task submitted: {task_name} (priority={args.priority})")


def cmd_list(args):
    """
    List all tasks in the queue, optionally filtered by status.

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
            print(f"Invalid status: {', '.join(invalid)}")
            print(f"Allowed status: {', '.join(sorted(allowed_status))}")
            return
    tasks = get_tasks(status)
    # Table columns: ID, Name, Priority, Date, Time, Status
    headers = ["ID", "Name", "Priority", "Date", "Time", "Status", "PID", "Duration"]
    col_widths = [6, 18, 10, 12, 10, 12, 8, 12]
    # Prepare rows
    rows = []
    now = datetime.now()
    for t in tasks:
        # t[0]: id, t[1]: name, t[2]: priority, t[3]: created_at, t[4]: status, ..., t[9]: pid, t[11]: start_time, t[12]: end_time
        try:
            dt = datetime.fromisoformat(t[3])
            date_str = dt.strftime("%Y-%m-%d")
            time_str = dt.strftime("%H:%M:%S")
        except Exception:
            date_str = t[3][:10]
            time_str = t[3][11:19]
        name = t[1]
        if len(name) > col_widths[1]:
            name = name[: col_widths[1] - 3] + "..."
        pid_str = str(t[9]) if t[9] is not None else "-"
        # Duration logic
        duration_str = "-"
        try:
            if t[4] == "running" and t[11]:
                start = datetime.fromisoformat(t[11])
                duration = now - start
                duration_str = str(duration).split(".")[0]
            elif t[4] in ("completed", "failed") and t[11] and t[12]:
                start = datetime.fromisoformat(t[11])
                end = datetime.fromisoformat(t[12])
                duration = end - start
                duration_str = str(duration).split(".")[0]
        except Exception:
            duration_str = "-"
        row = [
            str(t[0]).ljust(col_widths[0]),
            name.ljust(col_widths[1]),
            str(t[2]).ljust(col_widths[2]),
            date_str.ljust(col_widths[3]),
            time_str.ljust(col_widths[4]),
            t[4].ljust(col_widths[5]),
            pid_str.ljust(col_widths[6]),
            duration_str.ljust(col_widths[7]),
        ]
        rows.append(row)
    # Print header
    print(" ".join(h.ljust(w) for h, w in zip(headers, col_widths)))
    print("-" * (sum(col_widths) + len(col_widths) - 1))
    for row in rows:
        print(" ".join(row))


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
