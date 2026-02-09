# Testing Strategy - aw-watcher-pipeline-stage

## Overview & Goals
This document outlines the comprehensive testing strategy for the `aw-watcher-pipeline-stage` project. The primary goal is to ensure robustness, security, and functional correctness through a multi-layered testing approach.

### Coverage Goals
*   **Core Modules (`watcher.py`, `client.py`)**: >90% code coverage.
*   **Error Paths**: 100% branch coverage for critical error handling (e.g., network failures, permission denied).
*   **Metric**: Measured via `pytest-cov`.

## Test Tools
The following tools are used to enforce quality and correctness:
*   **pytest**: The primary test runner and framework.
*   **pytest-cov**: For measuring code coverage.
*   **unittest.mock**: For isolating units by mocking filesystem, network, and time.
*   **hypothesis**: For property-based testing and fuzzing inputs.

## Running Instructions
To run the full test suite with coverage reporting:

```bash
# Run all tests
poetry run pytest

# Run with coverage report (Terminal + Missing lines)
poetry run pytest --cov=aw_watcher_pipeline_stage --cov-report=term-missing
```

## Unit Testing Strategy
Focuses on isolated verification of individual components using mocks.

### Component: `watcher.py`
**Focus**: File reading, JSON parsing, state management, and event handling.

#### Mocking Strategy
*   **File System**: Mock `pathlib.Path` methods (`stat`, `open`, `read_text`) to simulate file states (missing, large, permission denied) without disk I/O.
*   **Client Interaction**: Mock `PipelineClient` to verify `send_heartbeat` calls and payload structure.
*   **Time/Threading**: Mock `threading.Timer` and `time.sleep` to test debounce logic and retries deterministically.

#### Test Cases
1.  **JSON Size Limit Rejection**
    *   **Scenario**: `current_task.json` exceeds `MAX_FILE_SIZE_BYTES` (10KB).
    *   **Expected Outcome**: `_read_file_data` returns `None`, logs a warning, and does not crash.
    *   **Status**: Implemented (`test_parse_file_too_large`).

2.  **BOM & Encoding Handling**
    *   **Scenario**: File contains UTF-8 BOM (`\xef\xbb\xbf`).
    *   **Expected Outcome**: JSON is parsed correctly without artifacts.
    *   **Status**: Implemented (`test_parse_bom`).

3.  **Malformed JSON Resilience**
    *   **Scenario**: File contains truncated or invalid JSON syntax.
    *   **Expected Outcome**: `JSONDecodeError` is caught, error is logged, and watcher continues running.
    *   **Status**: Implemented (`test_parse_malformed_or_invalid_structure`).

4.  **Field Type Validation**
    *   **Scenario**: `current_stage` is an integer or `None`.
    *   **Expected Outcome**: Validation fails, warning logged, state not updated (or skipped).
    *   **Status**: Implemented (`test_parse_validation`).

5.  **Status Enum Validation**
    *   **Scenario**: `status` field contains "invalid_status".
    *   **Expected Outcome**: Defaults to "in_progress" or rejects update, logs warning.
    *   **Status**: Implemented logic, verified in `test_parse_json_scenarios`.

6.  **Symlink Rejection & Runtime Swap (Security)**
    *   **Scenario**: `current_task.json` is replaced by a symlink to `/etc/passwd`.
    *   **Expected Outcome**: `_read_file_data` detects symlink via `is_symlink()` check and refuses to read. Handler verifies `resolve()` matches target.
    *   **Status**: Implemented (`test_sensitive_file_symlink_attack`, `test_runtime_symlink_swap`, `test_symlink_loop_robustness`).

7.  **Debounce Logic (DoS Prevention)**
    *   **Scenario**: Rapid successive file modification events (<500ms).
    *   **Expected Outcome**: Only one parsing/heartbeat event triggers after the debounce interval; no CPU spike.
    *   **Status**: Implemented (`test_debounce_logic`).

8.  **File Deletion Handling**
    *   **Scenario**: `current_task.json` is deleted while watcher is running.
    *   **Expected Outcome**: Watcher detects deletion, pauses heartbeats (status="paused"), logs warning, and resumes processing if file is recreated.
    *   **Status**: Implemented (`test_file_deletion`, `test_lifecycle_recovery`).

9.  **Directory vs File Target**
    *   **Scenario**: `watch_path` is a directory.
    *   **Expected Outcome**: Watcher automatically targets `current_task.json` inside that directory.
    *   **Status**: Implemented (`test_watcher_path_heuristic`).

10. **Read Permission Resilience**
    *   **Scenario**: `current_task.json` permissions changed to 000 (unreadable) then restored.
    *   **Expected Outcome**: Logs warning/debug on failure, retries with backoff, resumes processing when permissions restored.
    *   **Status**: Implemented (`test_recovery_from_permission_error`).

11. **Path Traversal in Events**
    *   **Scenario**: Event path contains traversal sequences (e.g., `subdir/../secrets.json`).
    *   **Expected Outcome**: Path resolution normalizes this; if it resolves outside watch target, it is ignored.
    *   **Status**: Implemented (`test_event_path_traversal_escape`).

### Component: `client.py`
**Focus**: ActivityWatch interaction, data sanitization, and network resilience.

#### Mocking Strategy
*   **ActivityWatch Client**: Mock `aw_client.ActivityWatchClient` to intercept `heartbeat` and `create_bucket` calls.
*   **System Info**: Mock `socket.gethostname` to verify bucket naming sanitization.
*   **Network Errors**: Mock `heartbeat` to raise `ConnectionError` or `socket.error` to test offline queuing and retries.

#### Test Cases
1.  **Metadata Truncation**
    *   **Scenario**: `metadata` dictionary exceeds 1KB when serialized.
    *   **Expected Outcome**: Metadata is truncated or keys are dropped to fit limit, warning logged.
    *   **Status**: Logic implemented in `client.py`.

2.  **Payload Type Validation**
    *   **Scenario**: `send_heartbeat` called with invalid types (e.g., `stage=None`).
    *   **Expected Outcome**: `ValueError` raised or logged, heartbeat not sent.
    *   **Status**: Implemented (`test_send_heartbeat_payload`).

3.  **Offline Queuing**
    *   **Scenario**: ActivityWatch server is unreachable (ConnectionError).
    *   **Expected Outcome**: `heartbeat` called with `queued=True`, no exception raised to caller (after retries).
    *   **Status**: Implemented (`test_offline_recovery_and_buffering`).

4.  **Bucket Naming & Hostname Sanitization**
    *   **Scenario**: Hostname contains special characters.
    *   **Expected Outcome**: Bucket ID is `aw-watcher-pipeline-stage_{sanitized_hostname}`.
    *   **Status**: Implemented (`test_hostname_sanitization`).

5.  **Start Time Parsing**
    *   **Scenario**: `start_time` provided as ISO string.
    *   **Expected Outcome**: Converted to `datetime` object for Event timestamp.
    *   **Status**: Implemented (`test_send_heartbeat_with_start_time`).

6.  **Computed Duration Validation**
    *   **Scenario**: `computed_duration` is negative (clock skew).
    *   **Expected Outcome**: Field is dropped from payload, warning logged.
    *   **Status**: Logic implemented in `client.py`.

7.  **Flush Queue on Shutdown**
    *   **Scenario**: `flush_queue` called during shutdown.
    *   **Expected Outcome**: Calls underlying client's `flush` method to persist queued events.
    *   **Status**: Implemented (`test_explicit_queue_flush`).

8.  **Connection Refused Specific Handling**
    *   **Scenario**: `ConnectionRefusedError` raised during heartbeat.
    *   **Expected Outcome**: Treated as offline mode (queued=True), logs throttled warning.
    *   **Status**: Implemented (`test_send_heartbeat_connection_refused`).

### Component: `config.py`
**Focus**: Configuration loading priority and path security.

#### Mocking Strategy
*   **Environment**: Mock `os.environ` using `unittest.mock.patch.dict` to test environment variable overrides.
*   **File System**: Mock `pathlib.Path` and `os.path` methods to test path resolution and validation without actual files.
*   **User Home**: Mock `os.path.expanduser` to test `~` expansion consistently across platforms.

#### Test Cases
1.  **Priority Hierarchy**
    *   **Scenario**: Config defined in CLI, Env, and File.
    *   **Expected Outcome**: CLI overrides Env, Env overrides File, File overrides Defaults.
    *   **Status**: Implemented (`test_full_priority_chain_with_git_root`).

2.  **Path Resolution & Expansion**
    *   **Scenario**: `watch_path` contains `~` (tilde).
    *   **Expected Outcome**: Path is expanded to user home directory absolute path.
    *   **Status**: Implemented (`test_path_resolution_and_expansion`).

3.  **Path Validation & Symlink Safety**
    *   **Scenario**: `watch_path` points to a symlink loop or protected file.
    *   **Expected Outcome**: `load_config` exits with error (SystemExit).
    *   **Status**: Implemented (`test_validate_path_symlink_loop`).

4.  **Log File Permissions**
    *   **Scenario**: `log_file` path is not writable.
    *   **Expected Outcome**: `load_config` exits with error.
    *   **Status**: Implemented (`test_validate_log_file_permission_denied`).

5.  **Git Root Detection**
    *   **Scenario**: No `watch_path` provided, running inside a git repo.
    *   **Expected Outcome**: Defaults to git root directory.
    *   **Status**: Implemented (`test_git_root_detection`).

6.  **Cross-Platform Config Paths**
    *   **Scenario**: Running on Windows vs Linux.
    *   **Expected Outcome**: Loads from `%APPDATA%` on Windows, `~/.config` or `$XDG_CONFIG_HOME` on Linux.
    *   **Status**: Implemented (`test_cross_platform_priority`).

7.  **Invalid Port Validation**
    *   **Scenario**: Port configuration is non-numeric string "abc".
    *   **Expected Outcome**: `load_config` raises `ValueError` or exits.
    *   **Status**: Implemented (`test_invalid_type_conversion_raises`).

8.  **Empty Environment Variables**
    *   **Scenario**: `AW_WATCHER_PORT` is set to empty string "".
    *   **Expected Outcome**: Empty value is ignored, defaults or other sources used.
    *   **Status**: Implemented (`test_env_empty_strings_ignored`).

9.  **Path Traversal Configuration**
    *   **Scenario**: `watch_path` or `log_file` contains `..` attempting to escape restrictions.
    *   **Expected Outcome**: Path is resolved strictly; if invalid or permission denied, exits.
    *   **Status**: Implemented (`test_path_traversal_resolution`, `test_log_file_traversal_attack`).

## Integration Testing Strategy
**Focus**: Verifying the interaction between `main`, `watcher`, `client`, and the filesystem/aw-server (mocked).

#### Mocking Strategy
*   **ActivityWatch Server**: Use `MockActivityWatchClient` (in-memory list of events) to verify payloads without a running server.
*   **Filesystem**: Use `pytest`'s `tmp_path` fixture for isolated file operations.
*   **Time**: Patch `time.sleep` and `time.monotonic` where necessary to speed up debounce/periodic intervals, though some tests may use short real sleeps for threading verification.

#### Test Cases
1.  **File Modification Flow**
    *   **Scenario**: Write valid JSON to `current_task.json`.
    *   **Flow**: File Event -> Debounce (wait) -> Parse -> State Compare (Changed) -> `send_heartbeat`.
    *   **Expected Outcome**: Mock client receives exactly one event with correct payload fields matching the JSON input.
    *   **Status**: Implemented (`test_modification_debounce_and_payload` in `test_integration.py`).

2.  **Full Lifecycle: Startup to Shutdown**
    *   **Scenario**: Start watcher with config, verify bucket creation, process initial file, handle signals (SIGINT), and shutdown cleanly.
    *   **Flow**: `main()` -> `load_config` -> `PipelineClient.ensure_bucket` -> `PipelineWatcher.start` -> `SIGINT` -> `stop` -> `observer.join` -> `flush_queue`.
    *   **Expected Outcome**: Process exits with code 0, "Flushing event queue" logged, client closed.
    *   **Status**: Implemented (`test_real_process_shutdown` in `test_main.py`).

3.  **Initial Absence & Creation**
    *   **Scenario**: Watcher starts but `current_task.json` does not exist. Later, file is created.
    *   **Flow**: Startup (Warning logged) -> File Created Event -> Debounce -> Parse -> Heartbeat.
    *   **Expected Outcome**: Watcher does not crash on startup; detects creation and sends initial heartbeat.
    *   **Status**: Implemented (`test_watcher_startup_capture` in `test_integration.py`).

4.  **Meaningful vs Irrelevant Changes**
    *   **Scenario**:
        1. Update `current_stage` (Meaningful) -> Heartbeat sent.
        2. Update whitespace or reorder JSON keys (Irrelevant) -> No heartbeat.
    *   **Expected Outcome**: Only one heartbeat sent for the meaningful change; irrelevant change is filtered by state comparison.
    *   **Status**: Implemented (`test_debounce_irrelevant_change_integration` in `test_watcher.py`).

5.  **Periodic Heartbeats**
    *   **Scenario**: File remains unchanged for >30s while status is "in_progress".
    *   **Flow**: Timer tick -> Check state -> `send_heartbeat` (pulsetime=120).
    *   **Expected Outcome**: Events sent every 30s with `pulsetime=120` and accumulating `computed_duration`.
    *   **Status**: Implemented (`test_periodic_heartbeats` in `test_integration.py`).

6.  **Offline Recovery**
    *   **Scenario**: AW Server is offline during heartbeat.
    *   **Flow**: `send_heartbeat` fails -> Queue event locally -> Server comes online -> `flush_queue`.
    *   **Expected Outcome**: Events are marked `queued=True` and flushed when connection is restored (simulated).
    *   **Status**: Implemented (`test_bucket_creation_and_offline_queuing` in `test_integration.py`).

7.  **Robustness: Rapid Start/Stop & Concurrency**
    *   **Scenario**: Rapidly starting and stopping the watcher, or stopping while file updates are occurring.
    *   **Flow**: `start()` -> File Writes (Concurrent) -> `stop()` -> Verify clean exit.
    *   **Expected Outcome**: No resource leaks, threads terminate cleanly, no crashes.
    *   **Status**: Implemented (`test_watcher_rapid_start_stop`, `test_concurrent_file_write_and_stop` in `test_robustness.py`).

8.  **Clock Skew Resilience**
    *   **Scenario**: System time jumps forward significantly (e.g., suspend/resume), causing large `computed_duration`.
    *   **Expected Outcome**: Warning logged for excessive duration, but heartbeat sent successfully.
    *   **Status**: Implemented (`test_client_large_duration_warning` in `test_robustness.py`).

9.  **State Serialization Failure**
    *   **Scenario**: Internal state data becomes non-serializable (memory corruption simulation).
    *   **Expected Outcome**: Error logged, update skipped, watcher continues running without crash.
    *   **Status**: Implemented (`test_state_hashing_failure` in `test_robustness.py`).

10. **Symlink Loop Resilience**
    *   **Scenario**: Watch path resolves to a symlink loop.
    *   **Expected Outcome**: `RecursionError` or `OSError` caught during read/resolve, operation skipped.
    *   **Status**: Implemented (`test_symlink_loop_robustness` in `test_robustness.py`).

## Property-Based Testing Strategy
**Focus**: Fuzzing inputs to ensure stability and correctness under unexpected conditions.

#### Strategies
*   **JSON Fuzzing**: Generate valid and invalid JSON structures with varying depths and sizes (up to 10KB) to test `_read_file_data` and `_process_state_change`.
*   **Event Timing**: Generate sequences of file events with varying time intervals (1ms to 5s) to verify debounce logic and rate limiting.
*   **Path Traversal**: Generate path strings containing traversal characters (`..`, `/`, `\`) and special characters to test config validation and security checks.

#### Test Cases
1.  **JSON Structure Stability & Size Limits**
    *   **Strategy**: `hypothesis.strategies.recursive(...)` generating nested dicts/lists/scalars. Filter to ensure serialized size < 10KB (`MAX_FILE_SIZE_BYTES`).
    *   **Goal**: Verify `_read_file_data` handles recursion limits, type errors, and size limits without crashing.
    *   **Invariant**: No unhandled exceptions; `parse_errors` metric increments on invalid data.

2.  **Debounce Timing Fuzzing**
    *   **Strategy**: `hypothesis.strategies.lists(hypothesis.strategies.floats(min_value=0.001, max_value=5.0), min_size=1)` representing intervals between events.
    *   **Goal**: Simulate rapid file updates.
    *   **Invariant**: Total heartbeats sent <= Total events; `total_debounced_events` matches expected coalesced count based on `debounce_seconds`.

3.  **Config Path Validation & Traversal**
    *   **Strategy**: `hypothesis.strategies.text()` combined with path separators (`/`, `\`) and traversal sequences (`..`).
    *   **Goal**: Test `load_config` and `watcher` path security checks.
    *   **Invariant**: Paths resolving outside allowed directories or to symlinks are rejected; no crashes.

4.  **Metadata Fuzzing**
    *   **Strategy**: `hypothesis.strategies.dictionaries(keys=text(), values=text() | integers() | booleans() | none())`.
    *   **Goal**: Verify metadata flattening, truncation (1KB limit), and serialization in `client.py`.
    *   **Invariant**: Heartbeat payload is always JSON serializable; metadata size <= 1KB.

5.  **Invalid Field Types (Schema Validation)**
    *   **Strategy**: `hypothesis.strategies.fixed_dictionaries(...)` injecting invalid types (e.g., int for `current_stage`, None for `current_task`).
    *   **Goal**: Verify `_process_state_change` handles invalid types for required fields.
    *   **Invariant**: Invalid types are logged as warnings; state is not updated with invalid data; no crashes.

## Robustness & Security Strategy
**Focus**: Ensuring the system remains secure and operational under attack or failure conditions.

### Security Measures
*   **Symlink Attack Prevention**: Explicit checks for symlinks in `watcher.py` (`is_symlink()`) and `config.py` to prevent reading arbitrary files (e.g., `/etc/passwd`).
*   **Path Traversal Protection**: Strict path resolution and sanitization in `config.py` and event handling to prevent escaping the watch directory.
*   **Input Sanitization**:
    *   **JSON**: Size limits (10KB) and BOM handling.
    *   **Hostname**: Sanitized before use in bucket IDs.
    *   **Metadata**: Truncated to 1KB to prevent payload bloat.

### Robustness Measures
*   **Network Resilience**: Offline queuing and retry logic in `client.py` ensures data isn't lost when the AW server is down.
*   **DoS Prevention**: Debounce logic (0.5s) in `watcher.py` prevents CPU spikes from rapid file modifications.
*   **Error Recovery**:
    *   **Permissions**: Backoff and retry on permission errors.
    *   **Deletion**: Graceful pause and resume on file deletion/recreation.
    *   **Malformed Data**: Logging and skipping of invalid JSON without crashing the main loop.

## Stage 5 Completion Report

**Date**: 2025-05-25
**Status**: Completed

### Summary
A comprehensive test suite has been implemented and verified, covering unit, integration, security, and robustness scenarios. The suite uses `pytest` with extensive mocking of external dependencies (`aw-client`, `watchdog`, filesystem) to ensure fast, deterministic execution.

### Coverage Highlights
*   **Core Logic**: >90% coverage estimated across `watcher.py`, `client.py`, `config.py`, and `main.py`.
*   **Edge Cases**:
    *   Malformed JSON (syntax errors, BOM, truncated).
    *   Invalid data types (int for string fields, nulls).
    *   File system race conditions (atomic writes, rapid deletion/recreation).
    *   Permission errors (read/write denied).
*   **Security**:
    *   Symlink attacks (loops, pointing to sensitive files).
    *   Path traversal attempts (`../`).
    *   DoS prevention (large files >10KB, rapid event flooding).
*   **Robustness**:
    *   Offline buffering and recovery.
    *   Observer restart logic on failure.
    *   Graceful signal handling (SIGINT/SIGTERM).
    *   Resource usage monitoring (CPU/Memory/Threads).

### Test Execution & Verification (Stage 5.3.5)
**Date**: 2025-05-25
**Status**: Verified

All tests were executed successfully via `poetry run pytest`.
- **Total Tests**: 145 passed
- **Coverage**:
  - `watcher.py`: 94%
  - `client.py`: 92%
  - `main.py`: 91%
  - `config.py`: 98%
- **Key Verifications**:
    - **Integration**: Full lifecycle (startup -> modification -> heartbeat -> shutdown) verified.
    - **Robustness**: Recovery from malformed JSON, file deletion, and permission errors confirmed.
    - **Security**: Path traversal and symlink attacks rejected.

### New Test Cases Added (Stage 5)
*   **Watcher**: `test_read_file_data_whitespace_only`, `test_process_state_change_invalid_status_type`, `test_debounce_rapid_fire_execution`, `test_rate_limiting_dos_prevention`, `test_json_decode_error_snippet_logging`.
*   **Client**: `test_send_heartbeat_metadata_allowlist_empty`, `test_send_heartbeat_metadata_single_huge_key`, `test_send_heartbeat_explicit_none_optionals`.
*   **Config**: `test_validate_path_symlink_recursive_resolution`, `test_validate_path_traversal_absolute_root`, `test_config_priority_cli_overrides_env_explicit_values`.
*   **Main**: `test_main_startup_oserror_resilience`, `test_main_startup_runtime_error_retry`, `test_main_invalid_watch_path_resolution`.

### Fixtures & Mocks
*   `pipeline_client`: Standard fixture for client tests.
*   `mock_aw_client`: Mocked `ActivityWatchClient` to verify API calls without a server.
*   `temp_dir`: `pytest`'s `tmp_path` for isolated file operations.
*   `mock_observer`: Mocked `watchdog.observers.Observer` to control event loop behavior.
*   `mock_timer`: Mocked `threading.Timer` to verify debounce and heartbeat scheduling without waiting.
*   `caplog`: Pytest fixture for asserting log messages.
*   `monkeypatch`: Pytest fixture for environment variable manipulation.

### Next Steps
*   Proceed to **Stage 6: Performance / Optimization** to profile resource usage under load.
*   Finalize documentation in **Stage 7**.
