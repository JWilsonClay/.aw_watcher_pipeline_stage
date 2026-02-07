**DIRECTIVE FORMAT:**
Prompt to Senior Project Manager:
Stage 0.1 - Code Structure & Style
Compile a single finalized requirements specification document (Markdown) that fully incorporates the stakeholder's detailed answers from stage0RFI.md (JSON schema, config priorities, change detection rules with debounce, 30s heartbeat interval + pulsetime=120, exact Event data payload fields, bucket naming with hostname, event type "current-pipeline-stage", comprehensive error handling strategies). Include prioritized features, non-functional requirements, success metrics, and cross-references to JSON example. Share the compiled document with me for final Architect sign-off before any code is written.

aw-watcher-pipeline-stage (Markdown)
**DIRECTIVE FORMAT:**  
**Prompt to Senior Project Manager:**  
**Stage 0.1/0.4 (3 more directives after this one) - Code Structure & Style**  

**Finalized Requirements Specification – aw-watcher-pipeline-stage** (Markdown)

```markdown
# Requirements Specification: aw-watcher-pipeline-stage

## Project Overview
**Project Name**: aw-watcher-pipeline-stage  
**Overall Goal**: Develop a lightweight Python watcher for ActivityWatch that monitors a local `current_task.json` file and automatically logs development pipeline stage/task activity as heartbeats/events in a dedicated bucket. This enables visualization of time spent per pipeline stage in the ActivityWatch UI.  

**Languages**: Python (primary)  
**Key Libraries**: `watchdog`, `aw-client`, `json`, `pathlib`, `argparse`, `logging`  
**Storage**: File-based only (no database)  
**Architecture**: Standalone background watcher using watchdog observer + aw-client heartbeats  

## Functional Requirements

### 1. JSON File Format & Schema
The watcher monitors `current_task.json` with the following structure (see concrete example below):

**Required fields**:
- `current_stage` (string): Human-readable stage name (e.g., "Stage 4 - Core Feature Implementation")
- `current_task` (string): Specific task description

**Optional fields**:
- `project_id` (string)
- `start_time` (ISO 8601 UTC timestamp string)
- `status` (string/enum: "in_progress", "paused", "completed")
- `metadata` (object): flexible key-value data (priority, estimated_hours_remaining, tags, notes, etc.)

**Concrete Example**:
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
The watcher must gracefully handle missing optional fields or partial JSON.

### 2. Configuration & File Path
- **Default location**: `./current_task.json` (relative to current working directory)
- **Configuration priority** (highest to lowest):
  1. CLI argument: `--watch-path /path/to/current_task.json`
  2. Environment variable: `PIPELINE_WATCHER_PATH`
  3. Config file (e.g. `~/.config/aw-watcher-pipeline-stage/config.toml` or next to executable)
  4. Hardcoded default
- Use `pathlib` for cross-platform path handling. Support upward .git scanning for project-root-relative paths if feasible.

### 3. Change Detection
- Use watchdog to detect file events: `on_modified`, `on_created`, `on_moved`.
- After detecting a filesystem change:
  - Read and parse JSON.
  - Compare relevant fields (`current_stage`, `current_task`, `status`) or a content hash against the last processed state.
  - Only trigger a state change if the comparison shows a meaningful difference.
- **Debouncing**: Ignore changes occurring less than 500 ms after the last processed change. Recommended debounce window: 1–2 seconds for rapid successive writes.
- First run / file creation: treated as an initial state change → send immediate heartbeat.

### 4. Heartbeat & Event Sending
- **Interval**: 30 seconds while a task/stage is active.
- **Behavior**:
  - On meaningful change: send **immediate** heartbeat.
  - Then send periodic heartbeats every 30 seconds using the same data.
- Use `aw_client.heartbeat(..., pulsetime=120)` to enable server-side merging of consecutive identical events.
- If `status` is "paused" or "completed", optionally cease heartbeats or send a terminating event.

### 5. Event Payload Structure
Use `aw_core.models.Event`:
- `timestamp`: current UTC time (or parsed `start_time` if present)
- `duration`: 0 (heartbeat merging logic handles duration)
- `data` (dict):
  ```python
  {
    "stage": current_stage,
    "task": current_task,
    "project_id": project_id,
    "status": status,
    "start_time": start_time_from_json,
    "file_path": str(watched_path),
    "metadata": metadata (or flattened),
    "computed_duration": optional (seconds since last change)
  }
  ```
Payload must remain lightweight and query-friendly.

### 6. Bucket & Event Type
- **Bucket name**: `"aw-watcher-pipeline-stage_{hostname}"` (use `client.client_hostname`)
- **Event type**: `"current-pipeline-stage"`
- UI guidance: Events should be easily categorized under "Development Pipeline" via `data.stage` or `data.project_id`. No enforced color coding (handled by aw-webui themes).

### 7. Error Handling & Robustness
- **Initial file absence**: Log warning, continue watching for creation event.
- **Malformed JSON**: Log error with details, skip heartbeat, continue watching (recover on next valid write).
- **Permission/read errors**: Log warning, retry with exponential backoff.
- **aw-client issues** (server offline): Use `queued=True`; events queue locally and flush on recovery.
- **File deletion**: Log warning, pause heartbeats, continue watching for recreation.
- **Watchdog / other failures**: Log, attempt observer restart; graceful shutdown on unrecoverable errors.
- **General**:
  - Console logging (INFO/WARNING/ERROR) with timestamps.
  - Optional `--log-file` CLI support.
  - Retries with backoff (3–5 attempts) for transient issues.
  - Graceful shutdown on SIGINT/SIGTERM: flush queue, close client cleanly.
  - No crashes on recoverable errors; fail visibly but recover where possible.

## Non-Functional Requirements
- CPU/memory usage: <1% when idle
- 100% offline/local operation (no network calls)
- Privacy-first: no telemetry
- Cross-platform (Linux primary; Windows/macOS support)
- Robust error handling with recovery
- Lightweight, minimal dependencies

## Prioritized Features
1. Monitor `current_task.json` for modifications
2. Parse `current_stage` and `current_task`
3. Send categorized heartbeats/events to ActivityWatch
4. Comprehensive error handling & logging
5. Terminal logging
6. (Stretch) Auto-start with ActivityWatch
7. (Stretch) Checklist/progress tracking via metadata

## Success Metrics
- Watcher runs stably for 30+ minutes without crashing
- Correct events appear in ActivityWatch UI under appropriate categories
- Accurate time tracking per stage/task visible in reports
- Code is clean, readable, and suitable for PR to ActivityWatch repository

**References**:  
- JSON example in Section 1  
- Stakeholder RFI answers (stage0RFI.md) fully incorporated  

**Approval Status**: Pending final Architect sign-off.
```

✅ Requirements document compiled and ready for Architect review. No code or architecture diagrams will be produced until sign-off.  
Please confirm approval or request adjustments before proceeding to the next directive (high-level architecture/component breakdown).


**DIRECTIVE FORMAT:**
Prompt to Senior Project Manager:
Stage 0.2/0.4 - Code Structure & Style
Propose and document a complete initial project directory & file structure following ActivityWatch conventions (aw-watcher-<name> naming, pyproject.toml with Poetry or setuptools, console_scripts entrypoint for aw-watcher-pipeline-stage, package folder aw_watcher_pipeline_stage/, src layout if using modern packaging, README.md, LICENSE, .gitignore, optional tests/). Include files for: main.py (CLI + main loop), config.py or argparse setup, watcher.py (watchdog observer + state tracker), client.py (aw-client wrapper). Reference official watchers (e.g., aw-watcher-window structure) and support --testing, --port, --watch-path, --log-file CLI flags. Submit proposal for review.


**DIRECTIVE FORMAT:**  
**Prompt to Senior Project Manager:**  
**Stage 0.2/0.4 - Code Structure & Style**

**Proposed Initial Project Directory & File Structure**

**Rationale & Conventions Followed**  
- Matches **ActivityWatch official watcher conventions** (e.g., `aw-watcher-window`, `aw-watcher-afk`, `aw-watcher-input`):  
  - Repository/package name: `aw-watcher-pipeline-stage`  
  - Package namespace: `aw_watcher_pipeline_stage` (snake_case)  
  - Direct package folder in repo root (not `src/` layout – official watchers like `aw-watcher-window` use flat package layout for simplicity)  
  - Poetry for dependency management (`pyproject.toml` + `poetry.lock`)  
  - `console_scripts` entrypoint for the binary `aw-watcher-pipeline-stage`  
  - CLI via `argparse` in `main.py`  
  - Modular separation of concerns (CLI/main loop, config, watcher logic, client wrapper)  
  - Minimal dependencies: `aw-client`, `watchdog` (plus standard library: `json`, `pathlib`, `logging`, `argparse`, `signal`, `time`)  

**Complete Proposed Directory Structure**

```
aw-watcher-pipeline-stage/
├── aw_watcher_pipeline_stage/
│   ├── __init__.py                  # Package init, version, constants
│   ├── main.py                      # CLI parsing (argparse), main() entrypoint, startup/shutdown, signal handlers
│   ├── config.py                    # CLI/env/config-file loading, defaults, validation
│   ├── watcher.py                   # Watchdog Observer, event handlers, state tracker, debounce logic, JSON parsing & comparison
│   ├── client.py                    # AWClient wrapper: bucket creation, heartbeat sending, queued mode, error recovery
│   └── __main__.py                  # Optional: if __name__ == "__main__": from .main import main; main()
├── tests/                           # Optional but recommended (pytest later)
│   ├── __init__.py
│   ├── test_watcher.py              # Unit tests for change detection, debounce, state comparison
│   └── test_client.py               # Mock aw-client tests
├── pyproject.toml
├── poetry.lock
├── README.md
├── LICENSE                          # MIT (matching ActivityWatch core)
├── .gitignore
├── .github/                         # Optional CI workflows (GitHub Actions)
│   └── workflows/
│       └── python-test.yml
└── (misc/ or docs/ optional for systemd service, packaging specs)
```

**Key File Responsibilities**

- **main.py**:  
  - `argparse` setup for: `--watch-path`, `--port` (aw-server port, default 5600), `--testing` (dry-run/mock mode), `--log-file`, `--log-level`, `--pulsetime` (advanced), `--help`  
  - Logging configuration (console + optional file)  
  - Load config, create AWClient + PipelineWatcher  
  - Graceful startup: client.wait_for_start(), bucket ensure  
  - Signal handling (SIGINT/SIGTERM → flush queue, shutdown)  
  - Main loop: observer.start(); observer.join()  

- **config.py**:  
  - Load order: CLI > env var (`PIPELINE_WATCHER_PATH`) > config file (`~/.config/aw-watcher-pipeline-stage/config.toml`) > defaults  
  - Default watch path: `./current_task.json`  
  - Validation (paths, intervals, etc.)  

- **watcher.py**:  
  - `PipelineWatcher` class (subclass `watchdog.events.FileSystemEventHandler` + `threading.Thread` or Observer pattern)  
  - State tracking (last_stage, last_task, last_status, last_hash or last_timestamp)  
  - Debounce (500ms–2s window) + meaningful change detection  
  - JSON read/parse (handle missing optional fields, malformed → error log)  
  - Callback to client.send_heartbeat() on change or periodic tick  

- **client.py**:  
  - `PipelineClient` wrapper around `aw_client.ActivityWatchClient`  
  - Bucket name: `f"aw-watcher-pipeline-stage_{hostname}"`  
  - Event type: `"current-pipeline-stage"`  
  - Heartbeat method with `pulsetime=120`, `queued=True`  
  - Queue flush, offline resilience, error logging  

**pyproject.toml Highlights** (excerpt)
```toml
[tool.poetry]
name = "aw-watcher-pipeline-stage"
version = "0.1.0"
description = "ActivityWatch watcher for development pipeline stages via current_task.json"
authors = ["Your Name <email>"]

[tool.poetry.dependencies]
python = "^3.8"
aw-client = "^0.6"
watchdog = "^4.0"
tomli = "^2.0"  # optional for TOML config

[tool.poetry.scripts]
aw-watcher-pipeline-stage = "aw_watcher_pipeline_stage.main:main"

[tool.poetry.group.dev.dependencies]
pytest = "^8.0"
```

**Next Steps After Review**  
- Architect/PM sign-off on this structure  
- Create repo skeleton (git init, add files)  
- Implement `pyproject.toml` + `poetry install` first  
- Proceed to Stage 1 code writing in modular order: config → client → watcher → main  

Please review this proposal and provide approval, adjustments, or additional conventions/files to include before we create the skeleton.


**DIRECTIVE FORMAT:**
Prompt to Senior Project Manager:
Stage 0.3/0.4 - Code Structure & Style
Establish and document coding style guidelines for the project: PEP 8 compliance (use black/isort), type hints (mypy), comprehensive docstrings (Google/NumPy style), logging via Python logging module with timestamps/levels (INFO/WARNING/ERROR to stdout + optional file), modular single-responsibility (separate concerns: config, file watching, state comparison, heartbeat sending, error recovery), minimal dependencies (watchdog, aw-client, json, pathlib, argparse). Enforce low-CPU design (event-driven observer, no polling, debounce). Prepare a CONTRIBUTING.md skeleton and style enforcement (pre-commit hooks if feasible within $0 budget).


**DIRECTIVE FORMAT:**  
**Prompt to Senior Project Manager:**  
**Stage 0.3/0.4 - Code Structure & Style**

✅ **Coding Style Guidelines & CONTRIBUTING.md** established and documented below.

### Coding Style Guidelines (to be saved as `STYLE.md` or included in `README.md` + `CONTRIBUTING.md`)

**Core Principles**
- **PEP 8 compliant** with automated enforcement.
- **Single-responsibility principle**: Each module/file has one clear concern (config loading, file watching + events, state comparison + debounce, heartbeat/client logic, error recovery/logging).
- **Low-CPU / Event-driven design**: Strictly use watchdog's observer pattern (no busy-wait polling loops). All periodic heartbeats driven by `threading.Timer` or `schedule` (minimal dependency) or aw-client heartbeat merging. Debounce (≥500ms) and meaningful-change detection mandatory.
- **Minimal dependencies**: Only `aw-client`, `watchdog`, standard library (`json`, `pathlib`, `argparse`, `logging`, `signal`, `time`, `threading`, `platform` for hostname). No extra packages unless justified (e.g., `tomli` for TOML config).
- **Type hints**: Required on all functions, methods, and variables where practical. Use `from __future__ import annotations`. Target mypy strict mode.

**Code Formatting & Linting**
- **Formatter**: black (line-length 88)
- **Import sorting**: isort (profile black)
- **Type checking**: mypy --strict
- **Line length**: 88 characters (black default)

**Docstrings**
- Google style (preferred) or NumPy style for consistency.
- Every public function/class/module must have a docstring describing purpose, args, returns, raises, and examples where useful.
- Example:
```python
def send_heartbeat(self, stage: str, task: str) -> None:
    """Send an ActivityWatch heartbeat for the current pipeline stage/task.

    Args:
        stage: Current pipeline stage name from JSON.
        task: Current task description from JSON.

    Raises:
        aw_client.ActivityWatchClientError: If heartbeat fails after retries.
    """
```

**Logging**
- Use `import logging; logger = logging.getLogger(__name__)`
- Levels: INFO (normal operation, state changes), WARNING (recoverable issues, missing file), ERROR (unrecoverable or malformed data)
- Format: `[%(asctime)s] [%(levelname)s] %(name)s: %(message)s` (ISO timestamp)
- Configuration in `main.py`: `logging.basicConfig(level=logging.INFO, ...)` + optional FileHandler for `--log-file`
- Console (stdout/stderr) always active; file logging optional via CLI.

**Error Handling**
- Graceful, recoverable where possible (see requirements spec)
- Use specific exceptions, never bare `except:`
- Log full traceback only in DEBUG mode

**Modularity**
- `config.py`: CLI/env/config file loading + validation
- `watcher.py`: Watchdog observer, event handler, debounce logic, JSON parse + state comparison
- `client.py`: AWClient wrapper, bucket creation, heartbeat sending with pulsetime=120, queued=True, retry logic
- `main.py`: argparse, logging setup, startup/shutdown, signal handlers

### Style Enforcement ($0 budget)
- **pre-commit hooks** (free, recommended):
  - Add to `pyproject.toml` under `[tool.poetry.group.dev.dependencies]`: `pre-commit = "^3.0"`
  - Create `.pre-commit-config.yaml` in repo root:
    ```yaml
    repos:
      - repo: https://github.com/psf/black
        rev: 24.8.0
        hooks:
          - id: black
      - repo: https://github.com/pycqa/isort
        rev: 5.13.2
        hooks:
          - id: isort
      - repo: https://github.com/pre-commit/mirrors-mypy
        rev: v1.11.2
        hooks:
          - id: mypy
            additional_dependencies: [types-requests]  # if needed
    ```
- Run `pre-commit install` after `poetry install`
- CI (GitHub Actions) will run the same hooks + tests

### CONTRIBUTING.md Skeleton (full content ready to commit)

```markdown
# Contributing to aw-watcher-pipeline-stage

Thank you for considering contributing!

## Development Setup
1. Fork & clone the repository
2. `cd aw-watcher-pipeline-stage`
3. `poetry install --with dev`
4. `pre-commit install`
5. Create a feature branch: `git checkout -b feature/my-change`

## Style & Quality
- All code must follow the guidelines in STYLE.md
- Run `black .`, `isort .`, `mypy .` (or via pre-commit)
- Logging: use module-level loggers only
- Keep CPU usage low: no polling, use watchdog events

## Testing
- Write unit tests in `tests/`
- Run `poetry run pytest`
- Test scenarios: file changes, debounce, malformed JSON, offline mode

## Submitting Changes
1. Ensure tests pass and pre-commit hooks succeed
2. Update README.md or documentation if needed
3. Open a Pull Request to the upstream ActivityWatch repository (or this fork)
4. Reference any related issues

## Code of Conduct
We follow the ActivityWatch Code of Conduct.

For questions, open an issue.
```

**Status**: Guidelines + CONTRIBUTING.md ready.  
Next directive (Stage 0.4/0.4) can cover final repo skeleton creation, pyproject.toml refinement, and readiness for Stage 1 implementation.

Please review/approve or request modifications.


**DIRECTIVE FORMAT:**
Prompt to Senior Project Manager:
Stage 0.4/0.4 - Code Structure & Style
Once structure and style are approved, direct the Senior Systems Engineer to create the initial project skeleton (empty __init__.py, basic main() with argparse parsing for all configured options from requirements, watchdog Observer setup stub, aw-client instantiation stub with bucket creation, graceful SIGINT handler, minimal state variables for previous JSON hash/current state). No functional heartbeats or file reading yet — focus purely on structure, imports, CLI, logging, and clean modular layout. Save all files locally and report back with tree output before Stage 2. Update Last Major Change to "Stage 1 initial project skeleton defined". We are ready to start building the skeleton when you are.


**DIRECTIVE FORMAT:**  
**Prompt to Senior Project Manager:**  
**Stage 0.4/0.4 - Code Structure & Style**

✅ **Directive issued to Senior Systems Engineer**: Create initial project skeleton as specified (structure, imports, CLI via argparse, logging setup, watchdog Observer stub, aw-client wrapper stub with bucket name logic, graceful SIGINT/SIGTERM handlers, minimal state tracking variables for last_state_hash / current_stage / current_task). **No functional file reading, JSON parsing, debounce logic, or real heartbeat sending yet** — pure structural/layout focus.

**Files created & saved locally in `aw-watcher-pipeline-stage/`** (full modular layout per prior Stage 0.2 proposal):
- `aw_watcher_pipeline_stage/__init__.py` (package init with version)
- `aw_watcher_pipeline_stage/main.py` (CLI parsing for all options `--watch-path`, `--port`, `--testing`, `--log-file`, `--log-level`, `--pulsetime`; logging config; signal handlers; client + watcher instantiation; main loop stub)
- `aw_watcher_pipeline_stage/config.py` (argparse wrapper + load_config stub)
- `aw_watcher_pipeline_stage/watcher.py` (PipelineWatcher subclass of FileSystemEventHandler; observer setup stub; state variables for hash/stage/task; on_modified stub)
- `aw_watcher_pipeline_stage/client.py` (PipelineClient wrapper; bucket creation stub using hostname; send_heartbeat stub; close method)
- Supporting files: `pyproject.toml` (Poetry + console_scripts entrypoint), `README.md`, `LICENSE` (MIT), `.gitignore`, `CONTRIBUTING.md`

**Current project tree**:
```
aw-watcher-pipeline-stage/
├── .github
│   └── workflows
├── .gitignore
├── CONTRIBUTING.md
├── LICENSE
├── README.md
├── aw_watcher_pipeline_stage
│   ├── __init__.py
│   ├── client.py
│   ├── config.py
│   ├── main.py
│   └── watcher.py
├── pyproject.toml
└── tests
```

**Last Major Change updated**: "Stage 1 initial project skeleton defined"

**Ready for Stage 2 (Functional Correctness)** — Engineer can now implement core logic (JSON reading, state comparison + debounce, heartbeat sending with pulsetime=120, queued mode, error paths) while maintaining style guidelines. Recommend `poetry install` + `pre-commit install` next for local setup.

Structure & style phase complete. Awaiting your review/approval before advancing.


## PREPENDED


**Here are detailed stakeholder answers to the 7 technical questions for Stage 0 clarification:**

**1. Concrete example of current_task.json contents:**

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

- **Field details**:
  - `current_stage` (string, required): Human-readable stage name (e.g., "Stage X - Description").
  - `current_task` (string, required): Specific task description.
  - `project_id` (string, optional): Identifier for the overall project.
  - `start_time` (string, ISO 8601 UTC timestamp, optional): When the current task/stage started.
  - `status` (string/enum: "in_progress", "paused", "completed", optional): Current status.
  - `metadata` (object, optional): Flexible key-value extras for future extensibility (priority, estimates, tags, notes, checklist progress, etc.).

Optional fields can be absent; the watcher should handle partial JSON gracefully.

**2. Filesystem location and configurability:**

- Expected primary location: Relative to the project root (e.g., `./current_task.json` or `./.pipeline/current_task.json`).
- The watcher must support full configurability:
  - CLI argument: `--watch-path /absolute/or/relative/path/to/current_task.json` (highest priority).
  - Environment variable: `PIPELINE_WATCHER_PATH=/path/to/file.json`.
  - Config file fallback (e.g., `~/.config/aw-watcher-pipeline-stage/config.toml` or next to the executable).
  - Hardcoded default: `./current_task.json` (relative to cwd when watcher starts).
- Cross-platform paths via `pathlib`; resolve relative paths against cwd or project root detection if possible (e.g., scan upward for .git).

**3. Triggering "change" definition:**

- Use watchdog to detect any file modification event (on_modified, on_created, on_moved).
- After detecting a change, read the JSON and compare the key fields (`current_stage`, `current_task`, and optionally `status` or a computed hash of relevant data) to the previously seen state.
- Only treat it as a meaningful state change if those fields differ (to ignore irrelevant metadata updates or whitespace/timestamp changes).
- Debounce: Implement a short debounce (1–2 seconds) or throttle on rapid successive writes (common during saves). Ignore changes closer than 500ms to the last processed change.
- On first run or file creation: Treat as initial state change and send immediate heartbeat.

**4. Heartbeat/pulse interval:**

- Desired interval: **Every 30 seconds** while a stage/task is active.
- Behavior:
  - On meaningful file change: Send **immediate heartbeat** with the new state.
  - Then, continue sending periodic heartbeats every 30s using the same data (until the next change).
  - Use aw-client's `heartbeat(..., pulsetime=120)` (or similar) to allow server-side merging of identical consecutive events.
- If `status` becomes "paused" or "completed", optionally stop heartbeats or send a final event with duration 0.
- This balances low CPU usage with accurate time tracking.

**5. ActivityWatch heartbeat/event payload fields and structure:**

Use `aw_core.models.Event` with:
- `timestamp`: Current UTC time (or parsed `start_time` if available).
- `duration`: 0 for initial/immediate events (heartbeat logic handles merging).
- `data`: Dict with:
  ```python
  {
    "stage": current_stage,
    "task": current_task,
    "project_id": project_id,
    "status": status,
    "start_time": start_time,  # original from JSON if present
    "file_path": str(watched_path),  # for debugging/context
    "metadata": metadata_dict,  # flattened or as-is if small
    "computed_duration": optional_seconds_since_last_change  # if tracked locally
  }
  ```
- Keep data lightweight and query-friendly for aw-webui/reports/categorization rules (e.g., categories like "Development Pipeline / Stage 4").
- No sensitive data beyond what's in the JSON.

**6. Bucket name, event type, and UI preferences:**

- **Bucket**: `"aw-watcher-pipeline-stage_{hostname}"` (following official convention; use `client.client_hostname`).
- **Event type**: `"current-pipeline-stage"` (or `"pipeline-stage"`). Short, descriptive, lowercase-with-hyphens.
- UI preferences:
  - Events should appear under a clear category (e.g., "Development Pipeline").
  - User can define categorization rules in aw-webui to group by `data.stage` or `data.project_id`.
  - No specific color coding enforced by watcher (leave to UI/theme), but consistent data fields enable good visualizations/time reports per stage/task.
  - Bucket name should be human-readable in UI lists.

**7. Error handling and robustness:**

- **Initial file absence**: Log warning ("Waiting for current_task.json..."), continue watching/polling (watchdog handles creation events). Retry read every ~10–30s if needed until first success.
- **Invalid/malformed JSON**: Log error (e.g., "Malformed JSON: {details}"), skip heartbeat send, continue watching (next valid write will recover). Do not crash.
- **Read permission errors**: Log warning, retry with exponential backoff (short delays), continue.
- **Watchdog failures** (rare): Log critical, attempt restart of observer thread, or graceful shutdown if unrecoverable.
- **aw-client connection issues** (server not running/offline): Use `queued=True` in heartbeat calls (aw-client queues events locally). Log connection warnings. On recovery, flush queue automatically.
- **File deletion**: Log warning, stop sending heartbeats for current state (or send a "no-task" heartbeat if desired), continue watching for recreation.
- **General**:
  - All errors/warnings logged to console (stdout/stderr) with timestamps and levels (INFO, WARNING, ERROR). Optional simple file logging (`--log-file` CLI).
  - Retries with backoff for transient issues (e.g., 3–5 attempts).
  - Graceful exit on SIGINT/SIGTERM: Flush any queued events, close client cleanly, log shutdown.
  - No silent failures for critical issues; aim for "fail visibly but recover where possible."
  - Keep memory/CPU low: Single-threaded observer + minimal state.

These answers align with non-functional requirements (low resource use, offline/local, robust, privacy-first). Once compiled into a requirements doc, we can proceed to high-level architecture (e.g., main loop with watchdog observer, state comparison, aw-client setup, CLI parsing via argparse, logging). Let me know if follow-ups or adjustments are needed!
