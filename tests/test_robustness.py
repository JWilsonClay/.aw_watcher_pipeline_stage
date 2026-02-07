"""Robustness tests for aw-watcher-pipeline-stage."""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from aw_watcher_pipeline_stage.client import PipelineClient
from aw_watcher_pipeline_stage.watcher import PipelineEventHandler, PipelineWatcher

def test_client_send_heartbeat_none_duration(pipeline_client: PipelineClient, mock_aw_client: MagicMock) -> None:
    """Test that send_heartbeat handles None duration gracefully."""
    # Should not raise TypeError
    pipeline_client.send_heartbeat("Stage", "Task", computed_duration=None) # type: ignore
    
    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]
    assert "computed_duration" not in event.data

def test_watcher_symlink_resolution(pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock) -> None:
    """Test that watcher correctly handles symlinked target files."""
    real_file = temp_dir / "real_task.json"
    real_file.write_text(json.dumps({"current_stage": "Real", "current_task": "Task"}))
    
    link_file = temp_dir / "link_task.json"
    try:
        link_file.symlink_to(real_file)
    except OSError:
        pytest.skip("Symlinks not supported on this platform")
        
    watcher = PipelineWatcher(link_file, pipeline_client, 120.0)
    
    # The watcher should resolve the link to the real file
    assert watcher.handler.target_file.resolve() == real_file.resolve()
    
    # Update real file
    real_file.write_text(json.dumps({"current_stage": "Updated", "current_task": "Task"}))
    
    # Trigger parse on the LINK path (simulating event on link)
    watcher.handler._parse_file(str(link_file))
    
    # Should detect change
    assert watcher.handler.last_stage == "Updated"

def test_large_metadata_robustness(pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock) -> None:
    """Test handling of very large metadata payloads."""
    f = temp_dir / "large.json"
    
    # Create 1MB of metadata
    large_meta = {"data": "x" * 1024 * 1024}
    f.write_text(json.dumps({"current_stage": "Large", "current_task": "Test", "metadata": large_meta}))
    
    watcher = PipelineWatcher(f, pipeline_client, 120.0)
    watcher.handler._parse_file(str(f))
    
    # Should process without crashing
    assert watcher.handler.last_stage == "Large"
    mock_aw_client.heartbeat.assert_called_once()

def test_watcher_rapid_start_stop(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test rapid start/stop cycles to ensure no resource leaks or crashes."""
    f = temp_dir / "test.json"
    f.touch()
    watcher = PipelineWatcher(f, pipeline_client, 120.0)
    for _ in range(10):
        watcher.start()
        watcher.stop()

def test_serialized_heartbeats_slow_client(pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock) -> None:
    """Test that heartbeats are serialized when client is slow."""
    f = temp_dir / "current_task.json"
    f.touch()
    
    # Use 0 debounce to trigger immediately
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.0)
    
    # Mock send_heartbeat to be slow and track order
    call_order = []
    
    def slow_heartbeat(*args, **kwargs):
        # Extract stage from args or kwargs
        stage = kwargs.get("stage")
        call_order.append(f"start_{stage}")
        time.sleep(0.1)
        call_order.append(f"end_{stage}")
        
    # Patch the client's send_heartbeat method
    pipeline_client.send_heartbeat = slow_heartbeat # type: ignore
    
    # Trigger two updates rapidly in separate threads to simulate concurrency
    def trigger_update(stage: str) -> None:
        data = {"current_stage": stage, "current_task": "T"}
        handler.on_state_changed(data, 0.0)
        
    t1 = threading.Thread(target=trigger_update, args=("1",))
    t2 = threading.Thread(target=trigger_update, args=("2",))
    
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()
    
    # Verify serialization: start -> end, start -> end (no interleaving)
    assert len(call_order) == 4
    assert call_order[1].startswith("end_")
    assert call_order[3].startswith("end_")

def test_file_too_large_limit(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that files exceeding the size limit are skipped."""
    f = temp_dir / "too_large.json"
    # Create a file slightly larger than 5MB
    f.write_text(" " * (5 * 1024 * 1024 + 1024))

    watcher = PipelineWatcher(f, pipeline_client, 120.0)

    with patch.object(watcher.handler.logger, "warning") as mock_warn:
        watcher.handler._read_file_data(str(f))

        mock_warn.assert_called()
        assert "too large" in mock_warn.call_args[0][0]

def test_restricted_directory_robustness(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test robustness when watch directory has restricted permissions."""
    if os.name == "nt":
        pytest.skip("Skipping permission test on Windows")
        
    restricted_dir = temp_dir / "restricted"
    restricted_dir.mkdir()
    f = restricted_dir / "task.json"
    f.touch()
    
    # Remove read permissions
    restricted_dir.chmod(0o000)
    
    try:
        watcher = PipelineWatcher(f, pipeline_client, 120.0)
        # Start should fail (OSError/PermissionError caught and re-raised as RuntimeError after retries)
        with pytest.raises(RuntimeError, match="Failed to start watchdog observer"):
            watcher.start()
    finally:
        # Restore permissions for cleanup
        restricted_dir.chmod(0o755)

def test_concurrent_file_write_and_stop(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test stopping the watcher while file updates are happening concurrently."""
    f = temp_dir / "current_task.json"
    f.touch()
    
    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.01)
    watcher.start()
    
    stop_event = threading.Event()
    
    def writer() -> None:
        i = 0
        while not stop_event.is_set():
            try:
                f.write_text(json.dumps({"current_stage": "Stress", "current_task": f"Task {i}"}))
                i += 1
                time.sleep(0.005)
            except Exception:
                pass
                
    t = threading.Thread(target=writer)
    t.start()
    
    time.sleep(0.1)
    watcher.stop()
    stop_event.set()
    t.join()
    
    assert not watcher.observer.is_alive()

def test_periodic_and_event_concurrency(pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock) -> None:
    """Test that periodic heartbeats and event-driven heartbeats are serialized."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Concurrency", "current_task": "Test"}))
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    # Initialize state
    handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)
    
    # Mock send_heartbeat to be slow
    call_log = []
    def slow_send(*args, **kwargs):
        call_log.append("start")
        time.sleep(0.1)
        call_log.append("end")
        
    pipeline_client.send_heartbeat = slow_send # type: ignore
    
    # Trigger periodic check in one thread
    t1 = threading.Thread(target=handler.check_periodic_heartbeat)
    
    # Trigger event update in another
    t2 = threading.Thread(target=lambda: handler.on_state_changed(handler.current_data, 0.0))
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    # Verify no interleaving (start -> end -> start -> end)
    assert len(call_log) == 4
    assert call_log[1] == "end"
    assert call_log[3] == "end"

def test_symlink_loop_robustness(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test robustness against symlink loops."""
    link1 = temp_dir / "link1"
    link2 = temp_dir / "link2"
    try:
        link1.symlink_to(link2)
        link2.symlink_to(link1)
    except OSError:
        pytest.skip("Symlinks not supported")

    # Should not crash on init
    watcher = PipelineWatcher(link1, pipeline_client, 120.0)
    
    # Should not crash on event processing
    event = MagicMock()
    event.is_directory = False
    event.src_path = str(link1)
    
    # Mock resolve to raise RuntimeError (recursion)
    with patch("pathlib.Path.resolve", side_effect=RuntimeError("Symlink loop")):
        watcher.handler._process_event(event)
        watcher.handler.on_moved(event)
        watcher.handler.on_deleted(event)

def test_client_invalid_start_time_fallback(pipeline_client: PipelineClient, mock_aw_client: MagicMock) -> None:
    """Test that invalid start_time falls back to current time."""
    pipeline_client.ensure_bucket()
    
    # Send heartbeat with invalid start_time
    pipeline_client.send_heartbeat("Stage", "Task", start_time="invalid-date")
    
    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]
    
    # Should use current time (approx)
    assert event.timestamp.year >= 2023
    # Invalid field should be dropped
    assert "start_time" not in event.data

def test_client_large_duration_warning(pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: pytest.LogCaptureFixture) -> None:
    """Test that a warning is logged if computed_duration is excessively large."""
    pipeline_client.ensure_bucket()
    
    # Send heartbeat with huge duration (e.g. clock jump)
    pipeline_client.send_heartbeat("Stage", "Task", computed_duration=90000.0)
    
    mock_aw_client.heartbeat.assert_called_once()
    assert "Computed duration is very large" in caplog.text

def test_watcher_stat_failure_resilience(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that watcher handles OSError during stat() gracefully."""
    f = temp_dir / "stat_fail.json"
    f.touch()
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    with patch("time.sleep"): # Skip backoff
        with patch.object(Path, "stat", side_effect=OSError("IO Error")):
            with patch.object(handler.logger, "warning") as mock_warn:
                data = handler._read_file_data(str(f))
                
                assert data is None
                mock_warn.assert_called()
                assert "Failed to stat file" in mock_warn.call_args[0][0]

def test_client_port_validation(temp_dir: Path) -> None:
    """Test that PipelineClient validates the port argument."""
    f = temp_dir / "test.json"
    
    # Valid integer string
    client = PipelineClient(f, port="5600", testing=True) # type: ignore
    assert client.port == 5600
    
    # Invalid string
    with pytest.raises(ValueError, match="Port must be an integer"):
        PipelineClient(f, port="invalid", testing=True) # type: ignore

def test_handler_repr(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test the __repr__ method of PipelineEventHandler."""
    f = temp_dir / "test.json"
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    assert f"target={f.absolute()}" in repr(handler)

def test_state_hashing_failure(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test resilience against serialization failures during state hashing."""
    f = temp_dir / "test.json"
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    # Create data that is not JSON serializable (e.g. contains a set)
    # This simulates a case where internal data corruption or weird inputs might occur
    bad_data = {
        "current_stage": "Test",
        "current_task": "Task",
        "metadata": {"bad": {1, 2, 3}} # Sets are not JSON serializable
    }
    
    # Should log error and return None, not crash
    with patch.object(handler.logger, "error") as mock_error:
        result = handler._process_state_change(bad_data, str(f))
        assert result is None
        mock_error.assert_called()
        assert "Failed to serialize state" in mock_error.call_args[0][0]