# PR: Add aw-watcher-pipeline-stage

## What
This PR introduces a new watcher, `aw-watcher-pipeline-stage`, designed to automatically track development pipeline stages and tasks by monitoring a local `current_task.json` file.

## Why
Developers often switch contexts between pipeline stages (e.g., Planning, Coding, Testing). Manually logging this context switching is tedious. This watcher automates the process by reading a structured JSON file (often updated by CLI tools or IDE plugins) and logging it to ActivityWatch, enabling precise time tracking per stage.

## How
The watcher is implemented in Python and uses:
- **`watchdog`**: For event-driven file monitoring (no polling loops).
- **`aw-client`**: To communicate with the local ActivityWatch server.
- **Debouncing**: To coalesce rapid file updates (e.g., atomic writes) into single events.
- **State Tracking**: To only send heartbeats when meaningful data (`current_stage`, `current_task`) changes.

### Compliance
- **Security**: (See `SECURITY_AUDIT.md`) Enforces 10KB file limits, rejects symlinks, and sanitizes payloads.
- **Performance**: (See `PERFORMANCE.md`) Optimized for < 1% CPU and < 50MB RAM.
- **Tests**: (See `TESTING.md`) >90% coverage including unit, integration, and robustness scenarios.

## Testing
- **Automated**: 147 tests passed with >90% coverage across core modules.
- **Manual**: Verified installation via `poetry`, configuration priority, and offline recovery.
- **Cross-Platform**: Verified path handling for Linux/macOS/Windows.

## Changes
- Added `aw_watcher_pipeline_stage/` package:
  - `main.py`: CLI and signal handling.
  - `watcher.py`: File system observer and state logic.
  - `client.py`: ActivityWatch client wrapper with offline queuing.
  - `config.py`: Configuration loading (CLI > Env > File).
- Added `tests/`: Comprehensive test suite.
- Added Documentation: `README.md`, `ARCHITECTURE.md`, `SECURITY_AUDIT.md`, `PERFORMANCE.md`, `TESTING.md`, `CONTRIBUTING.md`, `MAINTAINABILITY.md`.

## Note for Upstream
This watcher is designed to be standalone but follows all ActivityWatch conventions for watchers. It is fully offline-first and local-only.

## Original Requirements Checklist
- [x] Monitor `current_task.json` for modifications.
- [x] Parse `current_stage` and `current_task`.
- [x] Send categorized heartbeats to ActivityWatch bucket.
- [x] Non-functional: Low CPU (<1%), Offline/Local-only, Privacy-first.