# Architecture - aw-watcher-pipeline-stage

## Overview
`aw-watcher-pipeline-stage` is a lightweight, event-driven ActivityWatch watcher designed to monitor a local JSON file (`current_task.json`) and log development pipeline stages as ActivityWatch events. It operates entirely locally, prioritizing low resource usage and privacy.

### High-Level Components

*   **`main.py` (Entry Point)**:
    *   Handles CLI argument parsing via `argparse`.
    *   Sets up logging and signal handling (SIGINT/SIGTERM) for graceful shutdown.
    *   Orchestrates the initialization of `PipelineClient` and `PipelineWatcher`.
    *   Manages the main event loop and resource usage monitoring.

*   **`config.py` (Configuration)**:
    *   Manages configuration loading with a strict priority order: CLI arguments > Environment variables > Config file > Defaults.
    *   Performs path validation and security checks (e.g., symlink rejection, directory validation).

*   **`watcher.py` (Core Logic)**:
    *   **Event Monitoring**: Uses `watchdog` to listen for file system events (modified, created, moved).
    *   **Debouncing**: Implements a `DebounceTimer` to coalesce rapid file updates (default 1.0s) and prevent CPU spikes.
    *   **Rate Limiting**: Implements a Token Bucket algorithm to drop excessive events during potential DoS attacks.
    *   **State Management**: Parses the JSON file, validates schema, and compares the current state against the previous state to detect meaningful changes.
    *   **Security**: Enforces file size limits (10KB) and rejects symlinks to sensitive files.

*   **`client.py` (Communication)**:
    *   **AW Wrapper**: Wraps the `aw-client` library to simplify heartbeat transmission.
    *   **Resilience**: Implements offline queuing (`queued=True`) to buffer events when the ActivityWatch server is unreachable.
    *   **Sanitization**: Anonymizes file paths (e.g., replacing home directory with `~`) and truncates metadata to prevent payload bloat.
    *   **Concurrency**: Offloads network I/O to a background worker thread to ensure the main loop remains non-blocking.

## System Flow

The following sequence describes the lifecycle of a file update event:

1.  **Initialization**:
    *   `main()` starts, loads configuration via `config.py`.
    *   `PipelineClient` initializes and ensures the bucket exists (queued).
    *   `PipelineWatcher` starts the `watchdog` observer on the target directory.

2.  **Event Detection**:
    *   The user or a tool updates `current_task.json`.
    *   `watchdog` triggers `on_modified` in `PipelineEventHandler`.

3.  **Debounce**:
    *   The event handler schedules a `DebounceTimer`.
    *   Rapid successive events reset the timer.
    *   Once the timer expires (e.g., after 1.0s of silence), processing begins.

4.  **Parsing & Validation**:
    *   The watcher reads the file (checking size limits and symlinks).
    *   JSON content is parsed. If malformed, it is logged and skipped.

5.  **State Comparison**:
    *   The parsed data (stage, task, status) is compared against the last known state (direct dictionary comparison, acting as a content hash).
    *   If the state is identical (or changes are irrelevant, like whitespace), processing stops.

6.  **Payload Construction**:
    *   If a meaningful change is detected, a payload is constructed.
    *   Metadata is flattened and sanitized.

7.  **Transmission**:
    *   `watcher.py` calls `client.send_heartbeat()`.
    *   `client.py` constructs an ActivityWatch event and queues it for the background worker.
    *   The worker sends the heartbeat to the local ActivityWatch server with `pulsetime=120` and `queued=True`.

8.  **Periodic Updates**:
    *   While the status is `in_progress`, the watcher schedules periodic heartbeats (every 30s) to maintain the active timeline in ActivityWatch.

## Design Decisions

The following architectural choices were made to satisfy the functional and non-functional requirements (see Requirements Spec).

### Event-Driven Architecture (vs. Polling)
*   **Choice**: Utilize `watchdog` for OS-level file system events rather than a sleep-loop polling mechanism.
*   **Reasoning**: Polling wastes CPU cycles checking for changes that rarely happen. An event-driven model ensures the watcher remains idle (<1% CPU) until a change occurs, while providing near-instantaneous response times (<20ms) for updates. This aligns with the low-resource requirement.

### Offline Resilience Strategy
*   **Choice**: Enable `queued=True` for all `aw-client` heartbeat calls.
*   **Reasoning**: Network reliability cannot be guaranteed, even on localhost. This setting delegates buffering to the `aw-client` library, which queues events in memory when the server is unreachable and handles automatic retries. Additionally, the watcher performs an explicit queue flush on shutdown. This ensures zero data loss during server restarts without complex custom retry logic in the main loop.

### Debounce & State Deduplication
*   **Choice**: Combine a configurable debounce timer (default 1.0s) with content-based state comparison.
*   **Reasoning**: File saves often generate bursts of file system events (atomic writes, temp files). Debouncing coalesces these into a single read operation. State comparison (checking `current_stage`, `current_task`, etc. against the previous state) prevents sending duplicate heartbeats if the file content hasn't meaningfully changed, reducing noise in the ActivityWatch database.

### Denial of Service (DoS) Protection
*   **Choice**: Implement a Token Bucket rate-limiting algorithm and strict file size limits (10KB).
*   **Reasoning**: To prevent resource exhaustion from malicious or buggy processes (e.g., rapid-fire writes or multi-GB log files), the watcher enforces strict boundaries. The Token Bucket allows for short bursts of activity while capping sustained load, and size limits prevent memory exhaustion (OOM) attacks.

### Privacy-First Design
*   **Choice**: Local-only execution with payload sanitization.
*   **Reasoning**:
    *   **No Telemetry**: The watcher contains no analytics or "phone home" code.
    *   **Path Anonymization**: Absolute file paths are sanitized (e.g., `/home/user/project` -> `~/project`) before transmission to decouple data from specific user environments.
    *   **Metadata Responsibility**: While metadata is supported for flexibility, it is flattened and truncated to prevent abuse, with the understanding that users control the input file's content.

## Future Extensibility
*   **Enhanced Privacy Controls**: Expansion of the current metadata allowlist to support blocklists or regex-based filtering for granular data control.
*   **System Integration**: Native auto-start integration with ActivityWatch's systemd services or OS-specific startup managers.
*   **Rich Metadata Parsing**: Logic to interpret specific metadata structures, such as checklist progress (e.g., `{"checklist": {"done": 3, "total": 5}}`) or estimated time remaining, for enhanced UI visualization.

## Non-Functional Compliance
*   **Low Resource Usage**: Verified <1% CPU usage when idle and <50MB RSS memory usage, ensuring minimal impact on the host system. (See [PERFORMANCE.md](PERFORMANCE.md))
*   **Offline-First**: Implements `queued=True` for all events, ensuring data integrity during server downtime or network interruptions.
*   **Local-Only**: Operates strictly on `localhost` with no external API calls, ensuring user privacy and security. (See SECURITY_AUDIT.md)
*   **Cross-Platform**: Built using `pathlib` and `watchdog` to ensure consistent behavior across Linux, macOS, and Windows environments.
*   **Robustness**: Verified via comprehensive test suite covering unit, integration, and edge cases. (See TESTING.md)

## Data Flow Diagram

```mermaid
flowchart TD
    CLI[User CLI / Env] -->|Load| Config[Config Loader]
    Config -->|Init| Observer[Watchdog Observer]

    subgraph Client [AW Client Wrapper]
        ClientInit[Ensure Bucket]
        ClientRuntime[Send Heartbeat]
        Worker[Background Worker]
        Queue[Offline Queue]
    end

    Config -->|Init| ClientInit
    ClientInit -->|HTTP| Server[Local AW Server]
    
    Observer -->|on_modified/created/moved| Handler[PipelineEventHandler]
    Timer[Periodic Timer] -->|Every 30s| Handler
    
    Handler -->|File Event| Debounce{Debounce Check}
    Handler -->|Periodic Tick| ClientRuntime
    
    Debounce -->|Wait| Debounce
    Debounce -->|Ready| Parser[JSON Parser]
    
    Parser -->|Valid JSON| Comparator{State Comparator<br/>(Hash/Diff)}
    Parser -->|Malformed/Large| Logger[Log Error & Skip]
    
    Comparator -->|State Changed| Builder[Payload Builder]
    Comparator -->|No Change| Skip[Skip]
    
    Builder -->|Construct Event| ClientRuntime
    ClientRuntime -->|Enqueue| Worker
    
    Worker -->|HTTP| Server
    Worker -.->|Connection Error| Queue
    Queue -.->|Retry| Worker
```