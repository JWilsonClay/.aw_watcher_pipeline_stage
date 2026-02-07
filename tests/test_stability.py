"""Stability tests for aw-watcher-pipeline-stage."""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from aw_watcher_pipeline_stage.client import PipelineClient
from aw_watcher_pipeline_stage.watcher import PipelineWatcher


def test_simulated_long_run(tmp_path: Path) -> None:
    """Simulate a long running session (1 hour) with periodic heartbeats."""
    f = tmp_path / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))

    mock_aw_client = MagicMock()
    client = PipelineClient(f, client=mock_aw_client, testing=True)
    # Disable debounce for simulation simplicity
    watcher = PipelineWatcher(f, client, pulsetime=120.0, debounce_seconds=0.0)

    # Mock time
    start_time = 1000000.0
    current_time = start_time

    def get_time() -> float:
        return current_time

    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", side_effect=get_time):
        # Initial read
        watcher.start()

        # Verify initial heartbeat
        assert mock_aw_client.heartbeat.call_count == 1

        # Simulate 1 hour loop (3600 seconds)
        # Expected periodics = 3600 / 30 = 120

        for i in range(1, 3601):
            current_time += 1.0
            watcher.check_periodic_heartbeat()

            # Inject a change at 30 minutes (1800s)
            if i == 1800:
                f.write_text(json.dumps({"current_stage": "Mid", "current_task": "Run"}))
                # Manually trigger parse since we are mocking time and not waiting for observer
                watcher.handler._parse_file(str(f))

        watcher.stop()

        # Count heartbeats
        # Initial: 1
        # Periodics 0-1800s (Init): ~59 (at 30, 60, ... 1770. 1800 is change)
        # Change at 1800s: 1
        # Periodics 1800-3600s (Mid): ~60

        # Total approx 120-122
        assert 115 < mock_aw_client.heartbeat.call_count < 125


def test_resilience_to_client_errors(tmp_path: Path) -> None:
    """Test that the watcher survives repeated client errors."""
    f = tmp_path / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))

    mock_aw_client = MagicMock()
    # Simulate client failure
    mock_aw_client.heartbeat.side_effect = Exception("Connection refused")

    client = PipelineClient(f, client=mock_aw_client, testing=True)
    watcher = PipelineWatcher(f, client, pulsetime=120.0, debounce_seconds=0.0)

    watcher.start()

    # Simulate loop
    for _ in range(10):
        watcher.check_periodic_heartbeat()
        # Should catch exception and continue

    watcher.stop()

    # Verify we tried multiple times
    assert mock_aw_client.heartbeat.call_count >= 1


def test_thread_leak_check(tmp_path: Path) -> None:
    """Check for thread leaks during rapid updates."""
    f = tmp_path / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))

    mock_aw_client = MagicMock()
    client = PipelineClient(f, client=mock_aw_client, testing=True)
    # Use real debounce to test Timer threads
    watcher = PipelineWatcher(f, client, pulsetime=120.0, debounce_seconds=0.01)

    watcher.start()

    initial_threads = threading.active_count()

    # Trigger many updates
    for i in range(50):
        f.write_text(json.dumps({"current_stage": "Run", "current_task": f"Task {i}"}))
        event = MagicMock()
        event.is_directory = False
        event.src_path = str(f)
        watcher.handler.on_modified(event)
        time.sleep(0.001)

    # Wait for debouncers to finish
    time.sleep(0.2)

    final_threads = threading.active_count()
    watcher.stop()

    # Should be roughly same number of threads (allow small variance for internal python threads)
    assert final_threads <= initial_threads + 1