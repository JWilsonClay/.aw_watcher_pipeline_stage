# aw-watcher-pipeline-stage
<!-- Last Major Change: Stage 7 Documentation & Maintainability complete; project fully documented and maintainable -->

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) [![Tests](https://github.com/yourusername/aw-watcher-pipeline-stage/actions/workflows/python-test.yml/badge.svg)](https://github.com/yourusername/aw-watcher-pipeline-stage/actions/workflows/python-test.yml) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

ActivityWatch watcher for development pipeline stages.

This watcher monitors a local `current_task.json` file and automatically logs your current development stage and task to ActivityWatch. It is designed to be lightweight, offline-first, and privacy-focused.

## Features
*   **Real-time Tracking**: Instantly detects changes in your pipeline state.
*   **Offline Resilience**: Queues events locally if ActivityWatch is unavailable.
*   **Privacy-First**: Runs 100% locally, sanitizes file paths, and supports metadata filtering.
*   **Low Resource Usage**: Optimized for < 1% CPU and < 50MB RAM.
*   **Robust**: Handles file rotation, deletion, and malformed input gracefully.

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Logging](#logging)
- [JSON Schema](#json-schema)
- [Architecture](#architecture)
- [Troubleshooting](#troubleshooting)
- [Security Notes](#security-notes)
- [Performance](#performance)
- [Testing](#testing)
- [Contributing](#contributing)

## Installation

### Prerequisites
- Python 3.8 or higher
- ActivityWatch running locally

### Using Poetry (Recommended)
```bash
git clone https://github.com/yourusername/aw-watcher-pipeline-stage.git
cd aw-watcher-pipeline-stage
poetry install
```

### Using Pip
```bash
git clone https://github.com/yourusername/aw-watcher-pipeline-stage.git
cd aw-watcher-pipeline-stage
pip install .
# Or for editable mode during development:
pip install -e .
```

## Usage

Start the watcher by running:

```bash
aw-watcher-pipeline-stage
```

By default, it looks for `current_task.json` in the current directory or the project root (detected via `.git`).

The watcher runs in the background with minimal resource usage (see Performance and Non-Functional Compliance in ARCHITECTURE.md).

### CLI Options

You can configure the watcher using command-line arguments. CLI arguments have the highest priority (overriding config files and environment variables). Below is an example using all available flags:

```bash
aw-watcher-pipeline-stage \
  --watch-path /path/to/current_task.json \
  --port 5600 \
  --testing \
  --log-file watcher.log \
  --log-level DEBUG \
  --pulsetime 60.0 \
  --debounce-seconds 1.0 \
  --metadata-allowlist "priority,tags"
```

| Flag | Description | Default |
|------|-------------|---------|
| `--watch-path PATH` | Path to the `current_task.json` file to monitor. | `.` (Current dir or git root) |
| `--port PORT` | Port of the ActivityWatch server. | `5600` |
| `--testing` | Run in testing mode (uses mock client, no server required). | `False` |
| `--log-file PATH` | Path to a file to write logs to. | `None` (Console only) |
| `--log-level LEVEL` | Logging verbosity (DEBUG, INFO, WARNING, ERROR). | `INFO` |
| `--pulsetime SECONDS` | Window to merge consecutive heartbeats. | `120.0` |
| `--debounce-seconds SECONDS` | Time to wait for file modifications to settle. | `1.0` |
| `--metadata-allowlist LIST` | Comma-separated list of allowed metadata keys (e.g., "priority,tags"). | `None` (All allowed) |

## Configuration

The watcher can be configured via CLI arguments, environment variables, or a configuration file.

### Priority
Configuration is loaded in the following order (highest priority first):
1. **CLI Arguments** (e.g., `--port 5600`)
2. **Environment Variables** (e.g., `AW_WATCHER_PORT=5600`)
3. **Config File** (`config.ini`)
4. **Defaults**

### Environment Variables
The following environment variables are supported:

| Variable | Description | Default |
|----------|-------------|---------|
| `PIPELINE_WATCHER_PATH` | Path to the `current_task.json` file. (Alias: `AW_WATCHER_WATCH_PATH`) | `.` (or git root) |
| `AW_WATCHER_PORT` | Port of the ActivityWatch server. | `5600` |
| `AW_WATCHER_TESTING` | Enable testing mode (`true`/`false`). | `False` |
| `AW_WATCHER_LOG_FILE` | Path to a file to write logs to. | `None` |
| `AW_WATCHER_LOG_LEVEL` | Logging verbosity (DEBUG, INFO, WARNING, ERROR). | `INFO` |
| `AW_WATCHER_PULSETIME` | Window to merge consecutive heartbeats (seconds). | `120.0` |
| `AW_WATCHER_DEBOUNCE_SECONDS` | Time to wait for file modifications to settle. | `1.0` |
| `AW_WATCHER_METADATA_ALLOWLIST` | Comma-separated list of allowed metadata keys. | `None` (All) |

### Configuration File
You can use a `config.ini` file for persistent configuration.

**Locations (checked in order):**
1. `config.ini` in the current working directory.
2. `$XDG_CONFIG_HOME/aw-watcher-pipeline-stage/config.ini` (Linux/macOS)
3. `%APPDATA%\aw-watcher-pipeline-stage\config.ini` (Windows)
4. `~/.config/aw-watcher-pipeline-stage/config.ini` (Fallback)

**Format:**
The file must be in INI format and contain an `[aw-watcher-pipeline-stage]` section.

```ini
[aw-watcher-pipeline-stage]
watch_path = ~/projects/my-pipeline/current_task.json
port = 5600
testing = false
log_level = INFO
debounce_seconds = 2.0
pulsetime = 120.0
metadata_allowlist = priority, tags
```

## JSON Schema

The watcher monitors a JSON file (default `current_task.json`) that defines your current activity.

### Concrete Example JSON

```json
{
  "current_stage": "Stage 4 - Core Feature Implementation",
  "current_task": "Implement database schema migrations",
  "project_id": "my-development-pipeline",
  "start_time": "2026-02-04T20:30:00Z",
  "status": "in_progress",
  "metadata": {
    "priority": "high",
    "estimated_hours_remaining": 3.5,
    "tags": ["backend", "database"],
    "notes": "Focus on PostgreSQL compatibility"
  }
}
```

### Field Reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `current_stage` | String | **Yes** | The high-level stage of your pipeline (e.g., "Planning", "Coding"). |
| `current_task` | String | **Yes** | The specific task you are working on. |
| `status` | String | No | Status of the task. Use `"in_progress"` to track time, `"paused"` or `"completed"` to stop heartbeats. |
| `project_id` | String | No | Identifier for the project. |
| `start_time` | String | No | ISO 8601 UTC timestamp of when the task started. |
| `metadata` | Object | No | Arbitrary key-value pairs (flattened into event data). |

## Architecture

For a detailed overview of the system design, component interactions, and architectural decisions (including offline resilience, debounce logic, and non-functional compliance), please refer to ARCHITECTURE.md.

## Troubleshooting

### Common Issues

**1. "File not found" warning**
- **Cause**: The watcher cannot find `current_task.json` at the specified or default path.
- **Fix**: Ensure the file exists or provide the correct path via `--watch-path`. The watcher will automatically detect the file if it is created later.

**2. "Malformed JSON" error**
- **Cause**: The JSON file has syntax errors (e.g., missing commas, unclosed braces).
- **Fix**: Validate your JSON syntax. The watcher will skip updates until the file is valid.

**3. "Connection refused" / Server unavailable**
- **Cause**: ActivityWatch server (`aw-server`) is not running or is on a different port.
- **Fix**: Start ActivityWatch. If using a non-standard port, specify it with `--port`. The watcher buffers events locally (`queued=True`) and automatically flushes them when the server becomes available.

**4. Permission errors**
- **Cause**: The watcher does not have read access to the file or directory.
- **Fix**: Check file permissions (`chmod`) or run the watcher with appropriate user privileges.

**5. "Target is a symlink" warning**
- **Cause**: The watcher detected that `current_task.json` is a symbolic link.
- **Fix**: For security reasons, the watcher does not follow symlinks. Replace the symlink with a regular file or point `--watch-path` directly to the real file.

**6. Missing Metadata**
- **Cause**: Metadata keys might be filtered by `metadata_allowlist` or truncated due to size limits.
- **Fix**: Check `metadata_allowlist` in config or reduce metadata size.

For deeper insight into how the watcher handles errors and offline states, see the System Flow in ARCHITECTURE.md.

## Security Notes

This watcher is designed with a "Local-Only" and "Privacy-First" architecture. See [SECURITY_AUDIT.md](SECURITY_AUDIT.md) for a full threat model.

*   **Path Validation**: All paths are resolved to their absolute form. The watcher refuses to follow symlinks for the target file to prevent arbitrary file read attacks (e.g., replacing `current_task.json` with a symlink to `/etc/passwd`).
*   **Symlink Handling**: If the target file is replaced by a symlink at runtime, the watcher detects this before reading and logs a security warning.
*   **Size Limits**:
    *   **File Size**: The watcher enforces a strict **10KB** limit on `current_task.json` to prevent memory exhaustion (DoS). Files larger than this are rejected.
    *   **Metadata Size**: Metadata entries are truncated if the total size exceeds **1KB** to keep heartbeat payloads lightweight.
*   **Metadata Privacy**: Keys in the `metadata` object are flattened into the event data. Use `metadata_allowlist` to restrict sensitive keys.

## Performance

This watcher is designed to be lightweight and performant. See [PERFORMANCE.md](PERFORMANCE.md) for detailed profiling reports.

### Key Benchmarks
*   **Idle CPU**: < 1% (Target met)
*   **Memory Usage**: < 50 MB RSS (Target met)
*   **Stability**: Verified stable under stress (1-hour run with rapid updates).

## Testing

This project uses `pytest` for testing and `pytest-cov` for coverage reporting. For a detailed testing strategy, coverage goals, and scenarios, see TESTING.md.

### Running Tests
To run the full test suite with coverage report:

```bash
poetry run pytest --cov=aw_watcher_pipeline_stage --cov-report=term-missing
```

### Viewing Coverage
To generate an HTML coverage report:

```bash
poetry run coverage html
# Open htmlcov/index.html in your browser
```

### CI/CD
Tests are automatically run via GitHub Actions on every push and pull request. See `.github/workflows/python-test.yml` for the configuration.

## Contributing

Contributions are welcome! Please read CONTRIBUTING.md for details on our code of conduct and the full process.

### Development Setup
1.  Install dependencies: `poetry install`
2.  Install pre-commit hooks: `pre-commit install`

### Development Process
1.  **Fork** the repository and **Clone** it locally.
2.  Create a new **Branch** for your feature or fix.
3.  Submit a **Pull Request** (PR) for review.

### Style Guidelines
We enforce strict code style to ensure maintainability:
*   **Formatting**: `black` and `isort`.
*   **Type Checking**: `mypy` (strict mode).

```bash
# Run style checks manually
poetry run black .
poetry run isort .
poetry run mypy .
```

### Testing
Ensure all tests pass before submitting a PR.

```bash
poetry run pytest --cov
```