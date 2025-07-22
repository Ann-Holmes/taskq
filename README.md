# TaskQ

TaskQ is a lightweight, extensible, and robust task queue system for local and small-scale distributed task management. It features a simple CLI, persistent task database (SQLite + SQLAlchemy ORM), and a flexible scheduler.

## Features

- Submit shell command tasks with priority, timeout, environment, working directory, and output redirection
- Persistent task storage with SQLAlchemy ORM
- List, filter, and inspect tasks with aligned table output
- Cancel tasks (including sending SIGTERM to running processes)
- Track task status, PID, start/end time, and duration
- Extensible and well-documented codebase
- Robust error handling and parameter validation

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/yourname/taskq.git
cd taskq
```

You can use `uv run` to run CLI commands directly without manual activation:

```bash
uv run taskq init
uv run taskq submit "sleep 10"
uv run taskq list
```

### Usage

#### Initialize Database

```bash
taskq init
```

#### Submit a Task

```bash
taskq submit "sleep 10; echo Hello" --priority 1 --stdout myout.log --stderr myerr.log --timeout 60
```

#### List Tasks

```bash
taskq list
taskq list --status running --status pending
```

#### Cancel a Task

```bash
taskq cancel 3
```

#### Start/Stop Scheduler

```bash
taskq start
taskq stop
```

## Command Line Reference

- `taskq init` : Initialize the database
- `taskq submit <command> [options]` : Submit a new task
- `taskq list [--status ...]` : List tasks, optionally filter by status
- `taskq cancel <id>` : Cancel a task by ID
- `taskq start` : Start the scheduler
- `taskq stop` : Stop the scheduler
- `taskq status` : Show scheduler status

## Task Fields

- **ID**: Task ID (auto-increment)
- **Name**: Task name (auto or user-defined)
- **Command**: Shell command to execute
- **Priority**: 0 (highest) to 9 (lowest)
- **Created At**: Submission timestamp
- **Status**: pending, running, completed, cancelled, failed
- **PID**: Process ID (if running)
- **Timeout**: Max seconds to run (0/unset = unlimited)
- **Start/End Time**: Actual execution timestamps
- **Duration**: Calculated from start/end time
- **Environment**: Environment variables at submission
- **CWD**: Working directory at submission
- **Stdout/Stderr File**: Output log file paths

## Development

- Codebase uses SQLAlchemy ORM for all database operations.
- All modules are documented with doc-strings and inline comments.
- Utilities for path resolution and parameter validation are in `taskq/utils.py`.
- Scheduler and CLI logic are separated for maintainability.

## License

MIT License
