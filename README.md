# aw-watcher-pipeline-stage

ActivityWatch watcher for development pipeline stages.

This watcher monitors a local `current_task.json` file and automatically logs your current development stage and task to ActivityWatch. It is designed to be lightweight, offline-first, and privacy-focused.

## Status
Last Major Change: Stage 5 Testing Strategy complete; comprehensive coverage achieved.

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

### CLI Options

You can configure the watcher using command-line arguments. Below is an example using all available flags:

```bash
aw-watcher-pipeline-stage \
  --watch-path /path/to/current_task.json \
  --port 5600 \
  --testing \
  --log-file watcher.log \
  --log-level DEBUG \
  --pulsetime 60.0 \
  --debounce-seconds 1.0
```

| Flag | Description | Default |
|------|-------------|---------|
| `--watch-path PATH` | Path to the `current_task.json` file to monitor. | `.` (Current dir or git root) |
| `--port PORT` | Port of the ActivityWatch server. | `5600` |
| `--testing` | Run in testing mode (uses mock client, no server required). | `False` |
| `--log-file PATH` | Path to a file to write logs to. | `None` (Console only) |
| `--log-level LEVEL` | Logging verbosity (DEBUG, INFO, WARNING, ERROR). | `INFO` |
| `--pulsetime SECONDS` | Window to merge consecutive heartbeats. | `120.0` |

### Configuration Priority
Configuration is loaded in the following order (highest priority first):
1. CLI Arguments
2. Environment Variables (e.g., `PIPELINE_WATCHER_PATH`, `AW_WATCHER_PORT`)
3. Config File (`config.ini` in `~/.config/aw-watcher-pipeline-stage/` or `XDG_CONFIG_HOME`)
4. Defaults

## JSON Configuration

The watcher expects a JSON file (default `current_task.json`) with the following structure.

### Example `current_task.json`

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
    "tags": ["backend", "database"]
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
- **Fix**: Start ActivityWatch. If using a non-standard port, specify it with `--port`. The watcher buffers events locally (`queued=True`) and retries until the server is available.

**4. Permission errors**
- **Cause**: The watcher does not have read access to the file or directory.
- **Fix**: Check file permissions (`chmod`) or run the watcher with appropriate user privileges.

## Testing

This project uses `pytest` for testing and `pytest-cov` for coverage reporting.

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

Contributions are welcome! Please read CONTRIBUTING.md for details on our code of conduct, and the process for submitting pull requests.

### Development Setup
1. Install dependencies: `poetry install`
2. Run tests: `poetry run pytest`
3. Install pre-commit hooks: `pre-commit install`