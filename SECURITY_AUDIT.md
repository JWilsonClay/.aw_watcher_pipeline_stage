# Security Audit & Threat Model

**Project**: aw-watcher-pipeline-stage  
**Date**: 2025-05-24  
**Version**: 1.9  
**Status**: Implemented (Stage 4.5.1 - Security Audit Finalized)

## 1. System Overview
`aw-watcher-pipeline-stage` is a local Python application that monitors a specific JSON file (`current_task.json`) for changes. It parses the file content and sends "heartbeat" events to a local ActivityWatch server via HTTP (aw-client). It runs with the privileges of the logged-in user.

## 2. Trust Boundaries & Assumptions
*   **File Operations**: The watcher is designed to be read-only regarding the monitored file. It does not create or modify `current_task.json`. The only write operation is to the optional log file.
*   **Local Execution**: The watcher runs entirely on the local machine with standard user privileges.
*   **Local Input**: The input (`current_task.json`) is a local file. We assume the user has write access to this file, but other malicious processes running as the user might also modify it.
*   **No Remote Surface**: The watcher does not open network ports or accept remote connections.
*   **Trusted Destination**: The local ActivityWatch server (localhost:5600) is assumed to be trusted.

## 3. Threat Analysis

### 3.1 Denial of Service (DoS)
**Vector**: Rapid File Events  
*   **Description**: A malicious process or script updates `current_task.json` at a very high frequency (e.g., every millisecond) to spike CPU usage and exhaust system resources.
*   **Severity**: **High**
*   **Mitigation Status**: **Implemented**. The watcher uses a configurable debounce interval (default 1.0s) and `threading.Timer` to coalesce rapid events.

**Vector**: Large JSON Payloads  
*   **Description**: An attacker replaces `current_task.json` with a multi-gigabyte file to cause Memory Exhaustion (OOM) when the watcher attempts to read/parse it.
*   **Severity**: **High**
*   **Mitigation Status**: **Implemented**. The watcher enforces a `MAX_FILE_SIZE_BYTES` (10KB) limit and reads with a buffer cap.

**Vector**: Recursive JSON (Billion Laughs / Deep Nesting)  
*   **Description**: A JSON file with deeply nested structures causing stack overflow or excessive CPU during parsing.
*   **Severity**: **Medium**
*   **Mitigation Status**: **Implemented**. The watcher catches `RecursionError` during JSON parsing.

**Vector**: Oversized Metadata
*   **Description**: Metadata field populated with excessive data to exhaust memory or storage.
*   **Severity**: **Low**
*   **Mitigation Status**: **Implemented**. Implicitly covered by `MAX_FILE_SIZE_BYTES` (10KB) limit on the entire file.

**Vector**: Blocking I/O on Non-Regular Files
*   **Description**: The watcher attempts to read from a named pipe or socket placed at `current_task.json`, causing the thread to hang indefinitely.
*   **Severity**: **Medium**
*   **Mitigation Status**: **Implemented**. `watcher.py` checks `stat.S_ISREG` before opening the file.

### 3.2 Information Disclosure
**Vector**: Data Leakage via Metadata  
*   **Description**: Sensitive data (API keys, passwords) inadvertently placed in the `metadata` field of `current_task.json` is sent to the ActivityWatch server and stored in its database.
*   **Severity**: **Medium**
*   **Mitigation Status**: **Partial/User Responsibility**. The watcher flattens metadata but does not filter keys. Users are responsible for the content of the JSON file.

**Vector**: Arbitrary File Read via Symlinks  
*   **Description**: An attacker replaces `current_task.json` with a symlink to a sensitive system file (e.g., `/etc/shadow` or `~/.ssh/id_rsa`). The watcher attempts to read it.
*   **Severity**: **Medium**
*   **Mitigation Status**: **Implemented**. Path resolution in `config.py` ensures the watcher starts with a canonical path, preventing confusion if the input path is a symlink. Runtime checks in `watcher.py` (`event_path.resolve() != target_file.resolve()`) provide additional protection against symlink swapping.

### 3.3 Tampering
**Vector**: Malformed JSON Injection  
*   **Description**: Injecting invalid JSON, binary data, or BOM markers to crash the watcher.
*   **Severity**: **Medium**
*   **Mitigation Status**: **Implemented**. Robust exception handling for `json.JSONDecodeError`, `UnicodeDecodeError`, and `OSError` prevents crashes.

**Vector**: Invalid Status Enum
*   **Description**: Injecting invalid status values (e.g., "hacked") to corrupt data or confuse the UI.
*   **Severity**: **Low**
*   **Mitigation Status**: **Implemented**. `watcher.py` validates status against allowed enums (`in_progress`, `paused`, `completed`).

**Vector**: Arbitrary File Write via Log Configuration
*   **Description**: If the user provides an untrusted path to `--log-file`, the watcher might overwrite or append to sensitive user files.
*   **Severity**: **Low**
*   **Mitigation Status**: **Accepted Risk**. The tool runs with user privileges; the user can already modify their own files.

**Vector**: Arbitrary File Write (Watch Path)
*   **Description**: Malicious actor attempts to trick watcher into writing to the watched file.
*   **Severity**: **None**
*   **Mitigation Status**: **Implemented**. Audit confirmed that `watcher.py` opens files in read-only mode (`"r"`) and performs no write operations to `watch_path`.

### 3.4 Supply Chain
**Vector**: Dependency Vulnerabilities
*   **Description**: Vulnerabilities in third-party libraries (`watchdog`, `aw-client`) could be exploited.
*   **Severity**: **Low**
*   **Mitigation Status**: **Implemented**. Automated `pip-audit` check in CI pipeline.

### 3.5 Dependency Vulnerabilities
**Audit Date**: 2025-05-24
**Scope**: `aw-client` (v0.8+), `watchdog` (^4.0), `json`, `pathlib`, `argparse`, `logging`.

*   **aw-client (v0.8+)**:
    *   **Status**: **Clean**.
    *   **Findings**: No published CVEs found in NVD or GitHub Security Advisories for the Python client library versions v0.8+.
    *   **Note**: Security relies on the local ActivityWatch server (aw-server) being secure (CORS/DNS rebinding protections).

*   **watchdog (^4.0)**:
    *   **Status**: **Clean**.
    *   **Findings**: No recent high-severity CVEs affecting the library usage on Linux/macOS/Windows in version 4.0+.
    *   **Note**: Historical issues (e.g., CVE-2019-25063) related to `yaml.load` in CLI tools do not affect this library usage as PyYAML is not a dependency and `watchmedo` is not used.

*   **Standard Library (`json`)**:
    *   **Risk**: Recursive JSON (DoS).
    *   **Mitigation**: Python 3.8+ has improved recursion limits. `watcher.py` explicitly catches `RecursionError`.

*   **Standard Library (`logging`, `argparse`, `pathlib`)**:
    *   **Status**: **Clean**.
    *   **Findings**: No relevant CVEs for the usage patterns in this project.

### 3.6 Telemetry & Privacy Audit
**Audit Date**: 2025-05-24
**Objective**: Verify compliance with privacy-first requirements (local-only, no external tracking).

*   **aw-client**:
    *   **Status**: **Clean**.
    *   **Findings**: Source code review and documentation confirm `aw-client` is designed to communicate exclusively with the user-configured ActivityWatch server (default: `localhost:5600`). It contains no built-in analytics, telemetry, or "phone-home" mechanisms.
    *   **Verification**: The library uses `requests` to hit the API endpoints defined in the client initialization. No hardcoded external domains were found in the source.

*   **watchdog**:
    *   **Status**: **Clean**.
    *   **Findings**: `watchdog` is a low-level filesystem event monitoring library. It operates strictly on local OS APIs (inotify, FSEvents, kqueue, ReadDirectoryChangesW). It has no network functionality for reporting usage statistics or telemetry.

*   **Conclusion**: The dependency chain aligns with the project's "100% local" and "privacy-first" requirements. No data leaves the machine via these libraries.

## 4. Threat Model & Risk Assessment

| Threat | Impact | Likelihood | Mitigation |
| :--- | :--- | :--- | :--- |
| **DoS: Rapid File Events**<br>Malicious process updates `current_task.json` at high frequency. | High CPU usage, System resource exhaustion. | Low | **Implemented**: Configurable debounce interval (default 1.0s) coalesces events. Warning logged if < 0.1s. |
| **DoS: Large JSON Payloads**<br>Input file replaced with multi-GB content. | Memory Exhaustion (OOM), Application Crash. | Low | **Implemented**: `MAX_FILE_SIZE_BYTES` (10KB) limit and buffer cap. |
| **DoS: Recursive JSON**<br>Deeply nested structures (Billion Laughs). | Stack overflow, High CPU. | Low | **Implemented**: `RecursionError` handling during JSON parsing. |
| **DoS: Oversized Metadata**<br>Metadata field contains excessive data. | Memory Exhaustion. | Low | **Implemented**: Covered by `MAX_FILE_SIZE_BYTES` (10KB) limit. |
| **DoS: Blocking I/O**<br>Input file is a named pipe/socket. | Application Hang / DoS. | Low | **Implemented**: `stat.S_ISREG` check in `watcher.py`. |
| **Info Disclosure: Metadata Leakage**<br>Sensitive keys (API tokens) in `metadata` sent to server. | Sensitive data stored in local AW database. | Medium | **Partial**: Metadata flattening. *Recommendation*: Allowlist/Blocklist config. |
| **Info Disclosure: Symlink Read**<br>Symlink to sensitive file (e.g., `/etc/shadow`) parsed as input. | Content snippet leakage in error logs. | Low | **Implemented**: Path resolution in `config.py` and runtime symlink checks prevent reading symlink targets. |
| **Info Disclosure: Path Traversal**<br>Attacker uses '..' to access files outside watch dir. | Information Leakage. | Low | **Implemented**: Strict path resolution in `config.py` and runtime checks in `watcher.py`. |
| **Tampering: Malformed Injection**<br>Invalid JSON, binary data, or BOM markers. | Application Crash, Denial of Service. | Medium | **Implemented**: Robust exception handling (`JSONDecodeError`, `UnicodeDecodeError`). Handles UTF-8 BOM. |
| **Tampering: Invalid Status Enum**<br>Invalid status values injected. | Data Corruption. | Low | **Implemented**: Validated against allowlist in `watcher.py`. |
| **Tampering: Log File Write**<br>User configures log path to sensitive location. | Overwrite/Corruption of user files. | Low | **Accepted Risk**: Tool runs with user privileges; user controls config. |
| **Tampering: Watch Path Write**<br>Watcher writes to input file. | Data Corruption. | None | **Implemented**: Code is strictly read-only for `watch_path`. |
| **Tampering: Runtime Symlink Swap**<br>File replaced by symlink during execution. | Arbitrary File Read. | Low | **Implemented**: Runtime `is_symlink()` check in `watcher.py` before read. |
| **Supply Chain: Dependencies**<br>Vulnerabilities in `watchdog` or `aw-client`. | Variable (RCE, DoS, Info Leak). | Low | **Audited**: No known CVEs as of 2025-05-24. |
| **DoS: Symlink Loops**<br>Watch path points to a symlink loop. | Application Crash / Hang. | Low | **Implemented**: Path resolution in `config.py` catches recursion errors and exits. |
| **DoS: Device Files**<br>Watch path points to `/dev/zero` or similar. | Application Hang / Resource Exhaustion. | Low | **Implemented**: `is_file()` check in `config.py` and `watcher.py` rejects non-regular files. |
| **Input Validation: Directory as File**<br>User provides directory path where file expected (log file). | Application Confusion / Crash. | Low | **Implemented**: `config.py` rejects directories for `log_file`. |

## 5. Recommendations
1.  **Log Sanitization**: [COMPLETED] `watcher.py` suppresses file content snippets unless `log_level` is DEBUG.
2.  **Dependency Audit**: [COMPLETED] Vulnerability scan performed. No issues found.
3.  **Metadata Filtering**: Consider adding a configuration option to allowlist/blocklist specific metadata keys if strict privacy is required.

## 6. Safeguard Implementation Plan (Stage 4.2.3)

### 6.1 Denial of Service (DoS)
*   **Rapid File Events**:
    *   *Safeguard*: Debounce logic (default 1.0s) and `threading.Timer` usage.
    *   *Status*: **Implemented** in `watcher.py`.
    *   *Refinement*: Ensure warning logs for high-frequency events are rate-limited to prevent log flooding (currently logs every 50 events).
*   **Large JSON Payloads**:
    *   *Safeguard*: `MAX_FILE_SIZE_BYTES` (10KB) limit during read.
    *   *Status*: **Implemented** in `watcher.py`.
*   **Recursive JSON**:
    *   *Safeguard*: `RecursionError` catching.
    *   *Status*: **Implemented** in `watcher.py`.
*   **Oversized Metadata**:
    *   *Safeguard*: `MAX_FILE_SIZE_BYTES` (10KB) limit.
    *   *Status*: **Implemented**.
*   **Blocking I/O (Non-Regular Files)**:
    *   **Safeguard**: `stat.S_ISREG` check.
    *   **Status**: **Implemented** in `watcher.py`.

### 6.2 Information Disclosure
*   **Metadata Leakage**:
    *   *Safeguard*: Metadata Allowlist/Blocklist.
    *   *Status*: **Pending**.
    *   *Proposal*: Add `metadata_allowlist` to configuration. In `client.py`, filter `metadata` keys before flattening.
*   **Symlink/Arbitrary File Read**:
    *   *Safeguard*: Log Sanitization.
    *   *Status*: **Implemented**.
    *   *Note*: `watcher.py` only logs file content snippets if `log_level` is DEBUG.
*   **Path Traversal**:
    *   *Safeguard*: Strict path resolution.
    *   *Status*: **Implemented**.
    *   *Note*: `config.py` resolves paths strictly; `watcher.py` validates event paths against target.

### 6.3 Tampering
*   **Malformed Injection**:
    *   *Safeguard*: Exception handling (`JSONDecodeError`, `UnicodeDecodeError`).
    *   *Status*: **Implemented** in `watcher.py`.
*   **Invalid Status Enum**:
    *   *Safeguard*: Validation against allowlist in `watcher.py`.
    *   *Status*: **Implemented**.
*   **Log File Write**:
    *   *Safeguard*: User permissions.
    *   *Status*: **Accepted Risk**.

### 6.4 Supply Chain Mitigations
*   **Dependency Vulnerabilities**:
    *   *Safeguard*: Automated vulnerability scanning (`pip-audit`) in CI.
    *   *Status*: **Implemented** in `.github/workflows/python-test.yml`.

## 7. Verification (Stage 4.3.3)
New tests added to verify malicious paths and robustness:
- `tests/test_config.py`:
  - `test_validate_path_symlink_to_protected_file`
  - `test_validate_log_file_is_directory`
  - `test_watch_path_is_directory_with_matching_name`
  - `test_validate_path_permission_denied`
  - `test_validate_log_file_symlink_to_protected_file`
  - `test_log_file_traversal_attack`
  - `test_path_traversal_resolution`
  - `test_validate_path_not_regular_file`
  - `test_validate_path_symlink_loop`
- `tests/test_main.py`:
  - `test_main_watch_path_traversal_exit`
  - `test_main_watch_path_symlink_exit`
  - `test_main_watch_path_permission_exit`
  - `test_main_log_file_symlink_exit`
  - `test_main_log_file_parent_failure`
  - `test_main_log_file_directory_failure`
- `tests/test_watcher.py`:
  - `test_runtime_symlink_loop_read`
  - `test_watcher_init_with_symlink_refusal`
  - `test_sensitive_file_symlink_attack`
  - `test_event_path_traversal_escape`
  - `test_runtime_symlink_swap`
  - `test_read_file_data_checks_symlink_before_stat`

## 8. Audit Findings (Stage 4.4.4)
**Date**: 2025-05-24
**Focus**: Secure Heartbeat Payload

*   **Finding**: `file_path` was sending absolute paths, potentially revealing usernames (e.g., `/home/alice/...`).
    *   **Mitigation**: Implemented path anonymization in `client.py`. Paths starting with the user's home directory now use `~` prefix.
*   **Finding**: Metadata size limit required verification.
    *   **Mitigation**: Verified existing 1KB truncation logic in `client.py`.
*   **Finding**: Large file paths could cause issues.
    *   **Mitigation**: Confirmed 4096 character limit on `file_path`.
*   **Finding**: Environment variable leakage check.
    *   **Mitigation**: Verified `client.py` does not access `os.environ`. Payload is strictly constructed from arguments.
*   **Finding**: Computed duration sanity check.
    *   **Mitigation**: Implemented validation in `client.py` to drop negative values or warn on excessive duration (>24h).

## 9. Audit Findings (Stage 4.4.5)
**Date**: 2025-05-24
**Focus**: Supply Chain Security

*   **Finding**: CI pipeline required automated vulnerability scanning.
    *   **Mitigation**: Verified and refined `pip-audit` step in `.github/workflows/python-test.yml` to include vulnerability descriptions.

## 10. Stage 4 Summary (Final Report)
**Date**: 2025-05-24
**Status**: Audit Complete & Mitigations Verified

### 10.1 Modeled Threats & Mitigations
*   **Denial of Service (DoS)**:
    *   *Vectors*: Rapid file updates, large payloads (multi-GB), recursive JSON (Billion Laughs), blocking I/O (pipes).
    *   *Mitigations*: Implemented configurable debounce (default 1.0s) in `watcher.py`, strict `MAX_FILE_SIZE_BYTES` (10KB) limit, `RecursionError` handling, and `stat.S_ISREG` checks.
*   **Information Disclosure**:
    *   *Vectors*: Metadata leakage, arbitrary file reads via symlinks, path traversal.
    *   *Mitigations*: Path resolution (`resolve()`) and runtime `is_symlink()` checks in `watcher.py` prevent reading unintended targets. `file_path` in payloads is anonymized (home dir replaced with `~`) in `client.py`.
*   **Tampering**:
    *   *Vectors*: Malformed JSON injection, invalid status enums.
    *   *Mitigations*: Robust exception handling for JSON parsing in `watcher.py`; strict enum validation (`in_progress`, `paused`, `completed`).
*   **Supply Chain**:
    *   *Vectors*: Vulnerable dependencies.
    *   *Mitigations*: Automated `pip-audit` in CI; no high-severity CVEs found in `watchdog` or `aw-client`.

### 10.2 Vulnerabilities Fixed
1.  **Input Size**: Enforced 10KB limit on `current_task.json` read to prevent OOM (`watcher.py`).
2.  **Path Safety**: Added `is_symlink()` checks before read to prevent symlink attacks (`watcher.py`).
3.  **Data Validation**: Implemented strict enum validation for `status` field (`watcher.py`).
4.  **Log Sanitization**: File content is excluded from logs unless `log_level` is DEBUG (`watcher.py`).
5.  **Privacy**: `file_path` sent to server is anonymized (`/home/user` -> `~`) (`client.py`).
6.  **Metadata Safety**: Metadata keys/values truncated to ensure total size < 1KB; non-serializable values dropped (`client.py`).
7.  **Recursion**: Explicit handling of `RecursionError` during JSON parsing (`watcher.py`).

### 10.3 Remaining Risks
*   **Log File Write**: The watcher runs with user privileges; if configured to write to a sensitive log path by the user, it will do so. *Mitigation*: Accepted risk (user responsibility).
*   **Metadata Content**: While size is limited, specific keys are not allowlisted. Sensitive data placed in `metadata` by the user will be sent to the local server. *Mitigation*: User responsibility; documentation warning.

### 10.4 Compliance Verification
*   **Privacy / Local-Only**: Confirmed no telemetry or external network calls. Communication is strictly with `localhost:5600` (or configured port).
*   **Robustness**: Integration tests verify recovery from file deletion, permission errors, malformed JSON, and server outages (offline queuing).

## 11. Verification & Regression Check (Stage 4.5.3)
**Date**: 2025-05-25
**Status**: Passed

*   **Linters & Formatters**:
    *   Ran `black`, `isort`, and `mypy --strict` on all source files.
    *   **Fixes Applied**:
        *   Sorted imports in `client.py`.
        *   Fixed indentation inconsistencies in `client.py`, `config.py`, and `watcher.py`.
        *   Applied line wrapping for PEP 8 compliance (max line length) in `client.py`, `watcher.py`, and `main.py`.
    *   Verified code style compliance.
*   **Regression Testing**:
    *   Executed full `pytest` suite (unit, integration, robustness, security).
    *   **Result**: All tests passed.
    *   **Coverage**: Verified that new security tests are active and passing:
        *   **Config Security**: `test_validate_path_symlink_to_protected_file`, `test_log_file_traversal_attack`, `test_validate_path_symlink_loop`.
        *   **Main Entry Security**: `test_main_watch_path_traversal_exit`, `test_main_log_file_symlink_exit`.
        *   **Watcher Runtime Security**: `test_runtime_symlink_swap`, `test_sensitive_file_symlink_attack`, `test_read_file_data_checks_symlink_before_stat`.

## 12. Audit Closure & Next Steps (Stage 4.5.4)
**Date**: 2025-05-25
**Status**: Closed

The security audit is considered closed. All identified high-priority risks have been mitigated or accepted.

**Next Steps**:
1.  **Stage 5 (Testing Strategy)**: Expand unit, integration, and edge case tests to ensure comprehensive coverage of new security logic and existing functionality.
2.  **Stage 7 (Documentation)**: Enhance README with security notes and configuration details.