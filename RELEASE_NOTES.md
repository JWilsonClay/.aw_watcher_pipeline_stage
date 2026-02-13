# Release Notes

## v0.1.0 (2026-02-11)

Initial release of **aw-watcher-pipeline-stage**, a lightweight ActivityWatch watcher for tracking development pipeline stages.

### Features
*   **Real-time Tracking**: Monitors `current_task.json` for changes and logs stage/task duration.
*   **Offline Resilience**: Queues events locally when the ActivityWatch server is unavailable.
*   **Privacy-First**: Runs 100% locally with path sanitization and metadata filtering.
*   **Low Resource Usage**: Optimized for < 1% CPU and < 50MB RAM.
*   **Cross-Platform**: Compatible with Linux, macOS, and Windows.

### Installation
```bash
pip install aw-watcher-pipeline-stage
```

### Usage
```bash
aw-watcher-pipeline-stage --watch-path ./current_task.json
```

### Artifacts
*   `aw_watcher_pipeline_stage-0.1.0-py3-none-any.whl`
*   `aw_watcher_pipeline_stage-0.1.0.tar.gz`