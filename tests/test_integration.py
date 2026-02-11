"""Integration tests for aw-watcher-pipeline-stage.

Covers:
- Normal lifecycle (startup, updates, shutdown)
- Robustness (offline, deletion, permissions, moves, malformed data)
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Generator, TYPE_CHECKING
from unittest.mock import MagicMock, patch, ANY

import pytest

from aw_watcher_pipeline_stage.client import MockActivityWatchClient, PipelineClient
from aw_watcher_pipeline_stage.config import load_config
from aw_watcher_pipeline_stage.main import main
from aw_watcher_pipeline_stage.watcher import PipelineWatcher

if TYPE_CHECKING:
    from pytest import CaptureFixture, LogCaptureFixture


@pytest.fixture
def integration_temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for integration tests."""
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield Path(tmpdirname)


@pytest.fixture
def example_json_data() -> dict[str, Any]:
    """Return the example JSON data with all optional fields."""
    return {
        "current_stage": "Stage 3.2.2 - Testing & Validation",
        "current_task": "Integration Test Execution",
        "project_id": "aw-watcher-pipeline-stage",
        "status": "in_progress",
        "start_time": "2023-10-27T10:00:00Z",
        "metadata": {
            "environment": "test",
            "runner": "pytest",
            "priority": "high"
        }
    }


@pytest.fixture
def populated_task_file(integration_temp_dir: Path, example_json_data: dict[str, Any]) -> Path:
    """Create the current_task.json file populated with example data."""
    task_file = integration_temp_dir / "current_task.json"
    task_file.write_text(json.dumps(example_json_data), encoding="utf-8")
    return task_file


def test_watcher_startup_capture(
    populated_task_file: Path, example_json_data: dict[str, Any]
) -> None:
    """Test that the watcher captures the initial state on startup.

    Stage 5.3.2: Test initial file presence triggers immediate heartbeat.
    """
    # Initialize Mock Client explicitly to inspect events
    mock_aw_client = MockActivityWatchClient()
    
    # Initialize PipelineClient in testing mode
    client = PipelineClient(
        watch_path=populated_task_file,
        client=mock_aw_client,
        testing=True
    )

    # Initialize Watcher with a short debounce for testing speed
    watcher = PipelineWatcher(
        populated_task_file,
        client,
        debounce_seconds=0.1
    )

    try:
        # Start the watcher
        watcher.start()

        # Wait for startup and initial file read (debounce + processing)
        timeout = 2.0
        start_wait = time.time()
        while time.time() - start_wait < timeout:
            if len(mock_aw_client.events) > 0:
                break
            time.sleep(0.1)

        # Confirm initial state is captured
        assert len(mock_aw_client.events) > 0, "No heartbeat events captured"
        
        last_event = mock_aw_client.events[-1]
        data = last_event["data"]

        # Verify core fields
        assert data["stage"] == example_json_data["current_stage"]
        assert data["task"] == example_json_data["current_task"]
        assert data["status"] == example_json_data["status"]
        
        # Verify optional fields
        assert data["project_id"] == example_json_data["project_id"]
        assert data["start_time"] == example_json_data["start_time"]
        
        # Verify metadata flattening
        for key, value in example_json_data["metadata"].items():
            assert data[key] == value

    finally:
        watcher.stop()


def test_status_paused_completed_logic(populated_task_file: Path) -> None:
    """Test that paused/completed status sends final event then stops periodic heartbeats.

    Directive 5: Test paused/completed status -> stop periodic heartbeats or send final terminating event.
    """
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(
        watch_path=populated_task_file,
        client=mock_aw_client,
        testing=True
    )
    # Short debounce
    watcher = PipelineWatcher(
        populated_task_file,
        client,
        debounce_seconds=0.1
    )

    try:
        watcher.start()
        time.sleep(0.3)
        mock_aw_client.events.clear()

        # 1. Change to "paused"
        paused_data = {
            "current_stage": "Stage X",
            "current_task": "Paused Task",
            "status": "paused"
        }
        populated_task_file.write_text(json.dumps(paused_data), encoding="utf-8")
        time.sleep(0.3)  # Wait for debounce

        # Verify final event sent
        assert len(mock_aw_client.events) == 1
        event = mock_aw_client.events[0]
        assert event["data"]["status"] == "paused"
        assert event["data"].get("computed_duration", 0.0) < 1.0
        mock_aw_client.events.clear()

        # 2. Verify NO periodic heartbeat
        # Simulate time passing > 30s
        last_change = watcher.handler.last_change_time
        simulated_now = last_change + 35.0

        with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", return_value=simulated_now):
            watcher.handler._periodic_heartbeat_task()
            assert len(mock_aw_client.events) == 0, "Should not send periodic heartbeat when paused"

        # 3. Change to "completed"
        completed_data = {
            "current_stage": "Stage X",
            "current_task": "Completed Task",
            "status": "completed"
        }
        populated_task_file.write_text(json.dumps(completed_data), encoding="utf-8")
        time.sleep(0.3)

        # Verify final event sent
        assert len(mock_aw_client.events) == 1
        event = mock_aw_client.events[0]
        assert event["data"]["status"] == "completed"
        assert event["data"].get("computed_duration", 0.0) < 1.0
        mock_aw_client.events.clear()

        # 4. Verify NO periodic heartbeat
        simulated_now_2 = simulated_now + 40.0
        with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", return_value=simulated_now_2):
            watcher.handler._periodic_heartbeat_task()
            assert len(mock_aw_client.events) == 0, "Should not send periodic heartbeat when completed"

    finally:
        watcher.stop()


def test_bucket_creation_and_offline_queuing(populated_task_file: Path) -> None:
    """Test bucket creation (mocked name with hostname) and offline queuing logic.

    Stage 5.3.2: Test bucket creation (name/type) and offline queuing.
    """
    mock_aw_client = MockActivityWatchClient()
    # Ensure hostname is set for deterministic bucket ID
    mock_aw_client.client_hostname = "test-integration-host"

    client = PipelineClient(
        watch_path=populated_task_file,
        client=mock_aw_client,
        testing=True
    )

    # 1. Verify bucket creation
    client.ensure_bucket()
    expected_bucket_id = "aw-watcher-pipeline-stage_test-integration-host"

    assert expected_bucket_id in mock_aw_client.buckets
    bucket_info = mock_aw_client.buckets[expected_bucket_id]
    assert bucket_info["event_type"] == "current-pipeline-stage"
    assert bucket_info["queued"] is True  # Verify queued creation

    # 2. Verify heartbeat queuing
    client.send_heartbeat(stage="Offline Test", task="Queuing")

    assert len(mock_aw_client.events) == 1
    event = mock_aw_client.events[0]
    assert event["queued"] is True
    assert event["bucket_id"] == expected_bucket_id

    # 3. Verify flush
    # This simulates the recovery phase where queued events are flushed to the server
    # Mock the flush method to verify it's called
    with patch.object(mock_aw_client, "flush", wraps=mock_aw_client.flush) as mock_flush:
        client.flush_queue()
        mock_flush.assert_called_once()


def test_periodic_heartbeats(populated_task_file: Path) -> None:
    """Test periodic heartbeats are sent every 30s with correct parameters.

    Directive 4: Validate 30s periodic heartbeats and pulsetime=120 behavior.
    """
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(
        watch_path=populated_task_file,
        client=mock_aw_client,
        testing=True
    )

    # Use short debounce
    watcher = PipelineWatcher(
        populated_task_file,
        client,
        debounce_seconds=0.1
    )

    try:
        watcher.start()

        # Wait for initial heartbeat
        time.sleep(0.5)
        assert len(mock_aw_client.events) == 1
        initial_event = mock_aw_client.events[0]
        mock_aw_client.events.clear()

        # Capture the real time of the last change/heartbeat
        last_change_real = watcher.handler.last_change_time

        # --- Simulate 35s passing (Period 1) ---
        # We mock time.time to return a future time.
        # This affects both the check interval calculation and the computed_duration.
        simulated_now = last_change_real + 35.0

        with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", return_value=simulated_now):
            # Trigger the check (simulating the main loop)
            watcher.handler._periodic_heartbeat_task()

            # Verify periodic heartbeat sent
            assert len(mock_aw_client.events) == 1
            periodic_event = mock_aw_client.events[0]

            # Verify payload consistency
            assert periodic_event["data"]["stage"] == initial_event["data"]["stage"]
            assert periodic_event["data"]["task"] == initial_event["data"]["task"]
            assert periodic_event["data"]["status"] == "in_progress"
            
            # Verify duration increased (approx 35s)
            assert 34.0 < periodic_event["data"]["computed_duration"] < 36.0

            # Verify Directive 4 specific requirements
            assert periodic_event["pulsetime"] == 120.0
            assert periodic_event["queued"] is True

        # --- Verify silence when interval not met ---
        mock_aw_client.events.clear()

        # Simulate only 10s passed since the LAST heartbeat (which was at simulated_now)
        simulated_now_2 = simulated_now + 10.0
        
        with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", return_value=simulated_now_2):
            watcher.handler._periodic_heartbeat_task()
            assert len(mock_aw_client.events) == 0, "Should not send heartbeat before interval"

        # --- Simulate another 30s passing (Period 2) ---
        # Time = simulated_now + 31s
        simulated_now_3 = simulated_now + 31.0
        mock_aw_client.events.clear()

        with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", return_value=simulated_now_3):
            watcher.handler._periodic_heartbeat_task()

            assert len(mock_aw_client.events) == 1
            periodic_event_2 = mock_aw_client.events[0]
            
            # Verify duration continues to accumulate (35 + 31 = 66s)
            assert 65.0 < periodic_event_2["data"]["computed_duration"] < 67.0
            assert periodic_event_2["pulsetime"] == 120.0

    finally:
        watcher.stop()


def test_modification_debounce_and_payload(
    populated_task_file: Path,
) -> None:
    """Test immediate heartbeat on meaningful JSON change + debounce.

    Stage 5.3.2: Verify exactly one immediate heartbeat is sent within 1s,
    confirm debounce ignores rapid successive writes (<500ms).
    Assert recorded event data matches exact payload spec.
    """
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(
        watch_path=populated_task_file,
        client=mock_aw_client,
        testing=True
    )

    # Use 0.5s debounce to satisfy "ignores rapid successive writes (<500ms)"
    debounce_interval = 0.5
    watcher = PipelineWatcher(
        populated_task_file,
        client,
        debounce_seconds=debounce_interval
    )

    try:
        watcher.start()

        # Wait for startup event and clear it
        time.sleep(debounce_interval + 0.3)
        mock_aw_client.events.clear()

        # Rapid successive writes
        updates = [
            {"current_stage": "Rapid 1", "current_task": "Task 1"},
            {"current_stage": "Rapid 2", "current_task": "Task 2"},
            {
                "current_stage": "Final Stage",
                "current_task": "Final Task",
                "project_id": "debounce-test",
                "status": "in_progress",
                "start_time": "2023-10-27T12:00:00Z",
                "metadata": {"check": "debounce"}
            }
        ]

        for update in updates:
            populated_task_file.write_text(json.dumps(update), encoding="utf-8")
            # Sleep less than debounce interval (e.g. 0.1s)
            time.sleep(0.1)

        # Wait for debounce to trigger (0.5s) + processing buffer
        time.sleep(debounce_interval + 0.5)

        # Verify exactly one heartbeat
        assert len(mock_aw_client.events) == 1, f"Expected 1 event, got {len(mock_aw_client.events)}"
        
        event = mock_aw_client.events[0]
        data = event["data"]

        # Verify payload matches final update
        final_update = updates[-1]
        assert data["stage"] == final_update["current_stage"]
        assert data["task"] == final_update["current_task"]
        assert data["project_id"] == final_update["project_id"]
        assert data["status"] == final_update["status"]
        assert data["start_time"] == final_update["start_time"]
        assert data["check"] == "debounce" # Metadata flattened
        assert "metadata" not in data
        assert data["file_path"] == str(populated_task_file)
        
        # Verify Directive 5.3.2 specific fields
        assert event["pulsetime"] == 120.0
        # Duration should be 0 for event-driven updates (client handles merging)
        assert event.get("duration") == 0.0 or event.get("duration") == 0
        
        # Verify timestamp is approx now (within last minute)
        ts = event["timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        assert ts > datetime.now(timezone.utc) - timedelta(minutes=1)

    finally:
        watcher.stop()


def test_creation_triggers_heartbeat(integration_temp_dir: Path) -> None:
    """Test that creating the file after watcher start triggers a heartbeat.

    Stage 5.3.2: Test initial file absence -> creation triggers immediate heartbeat.
    """
    task_file = integration_temp_dir / "delayed_task.json"
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(
        watch_path=task_file,
        client=mock_aw_client,
        testing=True
    )
    
    watcher = PipelineWatcher(
        task_file,
        client,
        debounce_seconds=0.1
    )

    try:
        watcher.start()
        time.sleep(0.2)
        # Should be no events yet
        assert len(mock_aw_client.events) == 0

        # Create file
        data = {
            "current_stage": "Created",
            "current_task": "Just Now",
            "status": "in_progress"
        }
        task_file.write_text(json.dumps(data), encoding="utf-8")
        
        # Wait for debounce
        time.sleep(0.5)
        
        assert len(mock_aw_client.events) == 1
        event = mock_aw_client.events[0]
        assert event["data"]["stage"] == "Created"
        assert event["pulsetime"] == 120.0

    finally:
        watcher.stop()


def test_irrelevant_change_skips_heartbeat(populated_task_file: Path) -> None:
    """Test that irrelevant changes (whitespace, key order) do not trigger heartbeats.

    Stage 5.3.2: Test irrelevant change (e.g., only metadata update/reorder) skips heartbeat.
    """
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(
        watch_path=populated_task_file,
        client=mock_aw_client,
        testing=True
    )
    
    watcher = PipelineWatcher(
        populated_task_file,
        client,
        debounce_seconds=0.1
    )

    try:
        watcher.start()
        time.sleep(0.5)
        assert len(mock_aw_client.events) == 1
        mock_aw_client.events.clear()

        # Read current content
        content = json.loads(populated_task_file.read_text(encoding="utf-8"))
        
        # Write same content with different formatting (whitespace)
        populated_task_file.write_text(json.dumps(content, indent=4), encoding="utf-8")
        
        time.sleep(0.5)
        assert len(mock_aw_client.events) == 0, "Whitespace change should not trigger heartbeat"

        # Write same content with different key order
        original_json = '{"current_stage": "S", "current_task": "T"}'
        swapped_json = '{"current_task": "T", "current_stage": "S"}'
        
        populated_task_file.write_text(original_json, encoding="utf-8")
        time.sleep(0.5)
        mock_aw_client.events.clear() # Clear the event from original_json write
        
        populated_task_file.write_text(swapped_json, encoding="utf-8")
        time.sleep(0.5)
        assert len(mock_aw_client.events) == 0, "Key order change should not trigger heartbeat"

        # Write same content with extra top-level field (should be ignored)
        extra_field_json = '{"current_stage": "S", "current_task": "T", "extra": "ignored"}'
        populated_task_file.write_text(extra_field_json, encoding="utf-8")
        time.sleep(0.5)
        assert len(mock_aw_client.events) == 0, "Extra top-level field should not trigger heartbeat"

    finally:
        watcher.stop()


def test_e2e_lifecycle(integration_temp_dir: Path) -> None:
    """
    Comprehensive end-to-end test covering the full lifecycle:
    1. Startup & Bucket Creation
    2. Initial file absence -> Creation
    3. Valid modification -> Heartbeat with full payload check
    4. Irrelevant change -> No heartbeat
    """
    task_file = integration_temp_dir / "e2e_task.json"
    
    # Setup client
    mock_aw_client = MockActivityWatchClient()
    mock_aw_client.client_hostname = "e2e-host"
    
    client = PipelineClient(
        watch_path=task_file,
        client=mock_aw_client,
        testing=True
    )
    
    # Setup watcher with 0.5s debounce
    watcher = PipelineWatcher(
        task_file,
        client,
        debounce_seconds=0.5
    )
    
    try:
        # 1. Startup
        watcher.start()
        
        # Verify bucket creation
        client.ensure_bucket()
        
        expected_bucket_id = "aw-watcher-pipeline-stage_e2e-host"
        assert expected_bucket_id in mock_aw_client.buckets
        assert mock_aw_client.buckets[expected_bucket_id]["event_type"] == "current-pipeline-stage"
        
        # 2. File Creation
        time.sleep(0.2)
        mock_aw_client.events.clear()
        
        initial_data = {
            "current_stage": "Setup",
            "current_task": "Initialization",
            "status": "in_progress",
            "start_time": datetime.now(timezone.utc).isoformat()
        }
        task_file.write_text(json.dumps(initial_data), encoding="utf-8")
        
        # Wait for debounce (0.5s) + processing
        time.sleep(1.0)
        
        assert len(mock_aw_client.events) == 1
        event = mock_aw_client.events[0]
        assert event["data"]["stage"] == "Setup"
        mock_aw_client.events.clear()
        
        # 3. Valid Modification
        new_data = {
            "current_stage": "Development",
            "current_task": "Coding",
            "project_id": "E2E-Project",
            "status": "in_progress",
            "start_time": datetime.now(timezone.utc).isoformat(),
            "metadata": {"priority": "high"}
        }
        task_file.write_text(json.dumps(new_data), encoding="utf-8")
        
        # Wait > 1s (debounce 0.5s)
        time.sleep(1.2)
        
        assert len(mock_aw_client.events) == 1
        event = mock_aw_client.events[0]
        data = event["data"]
        
        # Verify exact payload
        assert data["stage"] == "Development"
        assert data["task"] == "Coding"
        assert data["project_id"] == "E2E-Project"
        assert data["status"] == "in_progress"
        assert data["start_time"] == new_data["start_time"]
        assert data["priority"] == "high" # Flattened metadata
        assert data["file_path"] == str(task_file)
        
        # Verify event fields
        assert event["pulsetime"] == 120.0
        assert event.get("duration") == 0.0 or event.get("duration") == 0
        
        # Verify timestamp approx now
        ts = datetime.fromisoformat(event["timestamp"])
        assert (datetime.now(timezone.utc) - ts).total_seconds() < 10.0
        
        mock_aw_client.events.clear()
        
        # 4. Irrelevant Change (Whitespace)
        json_str = json.dumps(new_data, indent=4)
        task_file.write_text(json_str, encoding="utf-8")
        
        time.sleep(1.0)
        
        assert len(mock_aw_client.events) == 0, "Irrelevant change should not trigger heartbeat"
        
    finally:
        watcher.stop()


def test_strict_interface_compliance(integration_temp_dir: Path) -> None:
    """
    Test end-to-end flow verifying strict client interface usage via assert_called_with.
    
    Stage 5.3.2: Verify heartbeat sent with exact payload via mock client.assert_called_with.
    """
    task_file = integration_temp_dir / "interface_test.json"
    
    # Use a MagicMock for the client to enable assert_called_with
    mock_client_instance = MagicMock()
    # Setup return values to avoid errors
    mock_client_instance.get_info.return_value = {"version": "test"}
    
    # We need to mock the hostname for bucket creation check
    with patch("socket.gethostname", return_value="interface-host"):
        client = PipelineClient(
            watch_path=task_file,
            client=mock_client_instance,
            testing=True
        )
        
        # Verify bucket creation call
        client.ensure_bucket()
        mock_client_instance.create_bucket.assert_called_with(
            "aw-watcher-pipeline-stage_interface-host",
            event_type="current-pipeline-stage",
            queued=True
        )
        
        watcher = PipelineWatcher(
            task_file,
            client,
            debounce_seconds=0.1
        )
        
        try:
            watcher.start()
            
            # Write valid JSON
            now = datetime.now(timezone.utc)
            start_time_str = now.isoformat()
            data = {
                "current_stage": "Interface Stage",
                "current_task": "Interface Task",
                "project_id": "P_Interface",
                "status": "in_progress",
                "start_time": start_time_str,
                "metadata": {"meta": "data"}
            }
            task_file.write_text(json.dumps(data), encoding="utf-8")
            
            # Wait for debounce
            time.sleep(0.5)
            
            # Verify heartbeat call
            assert mock_client_instance.heartbeat.called
            call_args = mock_client_instance.heartbeat.call_args
            
            # Call signature: heartbeat(bucket_id, event, pulsetime=..., queued=...)
            bucket_id = call_args[0][0]
            event = call_args[0][1]
            kwargs = call_args[1]
            
            assert bucket_id == "aw-watcher-pipeline-stage_interface-host"
            assert kwargs["pulsetime"] == 120.0
            assert kwargs["queued"] is True
            
            # Verify Event object content
            assert event.data["stage"] == "Interface Stage"
            assert event.data["task"] == "Interface Task"
            assert event.data["project_id"] == "P_Interface"
            assert event.data["status"] == "in_progress"
            assert event.data["start_time"] == start_time_str
            assert event.data["file_path"] == str(task_file)
            assert event.data["meta"] == "data"
            
            # Verify timestamp is approx now
            # Note: event.timestamp is a datetime object
            assert (event.timestamp - now).total_seconds() < 10.0
            
            # Verify duration is 0 (handled by server merging)
            assert event.duration == 0 or event.duration == 0.0

        finally:
            watcher.stop()


def test_metadata_reordering_skips_heartbeat(populated_task_file: Path) -> None:
    """Test that reordering keys within metadata does not trigger a heartbeat.

    Stage 5.3.2: Test irrelevant change (metadata reordering) skips heartbeat.
    """
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(
        watch_path=populated_task_file,
        client=mock_aw_client,
        testing=True
    )
    
    watcher = PipelineWatcher(
        populated_task_file,
        client,
        debounce_seconds=0.1
    )

    try:
        watcher.start()
        time.sleep(0.2)
        mock_aw_client.events.clear()

        # 1. Write initial metadata
        data = {
            "current_stage": "S",
            "current_task": "T",
            "metadata": {"a": 1, "b": 2}
        }
        populated_task_file.write_text(json.dumps(data), encoding="utf-8")
        time.sleep(0.5)
        
        # Should trigger heartbeat
        assert len(mock_aw_client.events) == 1
        mock_aw_client.events.clear()

        # 2. Write reordered metadata
        data_reordered = {
            "current_stage": "S",
            "current_task": "T",
            "metadata": {"b": 2, "a": 1}
        }
        populated_task_file.write_text(json.dumps(data_reordered), encoding="utf-8")
        time.sleep(0.5)

        # Should NOT trigger heartbeat (irrelevant change)
        assert len(mock_aw_client.events) == 0

    finally:
        watcher.stop()


def test_full_flow_strict_timing(integration_temp_dir: Path) -> None:
    """
    Test end-to-end flow with strict timing as requested:
    startup -> write -> debounce (>1s) -> verify heartbeat.
    
    Stage 5.3.2: Verify debounce timing > 1s.
    """
    task_file = integration_temp_dir / "strict_timing.json"
    task_file.touch()
    
    mock_aw_client = MockActivityWatchClient()
    mock_aw_client.client_hostname = "strict-host"
    
    client = PipelineClient(
        watch_path=task_file,
        client=mock_aw_client,
        testing=True
    )
    
    # Debounce 1.0s
    watcher = PipelineWatcher(
        task_file,
        client,
        debounce_seconds=1.0
    )
    
    try:
        watcher.start()
        client.ensure_bucket()
        
        # Verify bucket
        expected_bucket = "aw-watcher-pipeline-stage_strict-host"
        assert expected_bucket in mock_aw_client.buckets
        assert mock_aw_client.buckets[expected_bucket]["event_type"] == "current-pipeline-stage"
        
        # Write valid JSON
        data = {
            "current_stage": "Strict Stage",
            "current_task": "Strict Task",
            "project_id": "P1",
            "status": "in_progress",
            "start_time": datetime.now(timezone.utc).isoformat(),
            "metadata": {"check": "strict"}
        }
        task_file.write_text(json.dumps(data), encoding="utf-8")
        
        # Debounce wait > 1s (1.2s)
        time.sleep(1.5)
        
        # Verify heartbeat
        assert len(mock_aw_client.events) == 1
        event = mock_aw_client.events[0]
        payload = event["data"]
        
        # Exact payload check
        assert payload["stage"] == "Strict Stage"
        assert payload["task"] == "Strict Task"
        assert payload["project_id"] == "P1"
        assert payload["status"] == "in_progress"
        assert payload["start_time"] == data["start_time"]
        assert payload["file_path"] == str(task_file)
        assert payload["check"] == "strict"
        
        # Fields check
        assert event["pulsetime"] == 120.0
        assert event.get("duration") == 0.0 or event.get("duration") == 0
        
        # Timestamp approx now
        ts = datetime.fromisoformat(event["timestamp"])
        assert (datetime.now(timezone.utc) - ts).total_seconds() < 10.0

    finally:
        watcher.stop()


def test_startup_flow_from_config(integration_temp_dir: Path) -> None:
    """
    Test the full startup and event flow using configuration loading.

    Covers:
    1. Startup with valid config (load_config).
    2. Observer start & Bucket creation.
    3. Write valid JSON (Creation) -> Debounce.
    4. Verify meaningful change detection & Heartbeat payload.
    5. Irrelevant change (whitespace) skips heartbeat.
    """
    task_file = integration_temp_dir / "config_flow.json"
    
    # 1. Startup with valid config
    cli_args = {
        "watch_path": str(task_file),
        "testing": True,
        "pulsetime": 120.0,
        "debounce_seconds": 0.5
    }
    
    # Load config
    config = load_config(cli_args)
    
    mock_aw_client = MockActivityWatchClient()
    # Mock hostname for deterministic bucket ID
    mock_aw_client.client_hostname = "config-host"
    
    client = PipelineClient(
        watch_path=config.watch_path,
        client=mock_aw_client,
        testing=config.testing
    )
    
    watcher = PipelineWatcher(
        config.watch_path,
        client,
        debounce_seconds=config.debounce_seconds
    )
    
    try:
        # 2. Observer start & Bucket creation
        client.ensure_bucket()
        watcher.start()
        
        # Verify bucket creation
        expected_bucket = "aw-watcher-pipeline-stage_config-host"
        assert expected_bucket in mock_aw_client.buckets
        assert mock_aw_client.buckets[expected_bucket]["event_type"] == "current-pipeline-stage"
        
        # 3. Write valid JSON (Creation)
        data = {
            "current_stage": "Config Stage",
            "current_task": "Config Task",
            "project_id": "ConfigProject",
            "status": "in_progress",
            "start_time": datetime.now(timezone.utc).isoformat(),
            "metadata": {"source": "config_test"}
        }
        task_file.write_text(json.dumps(data), encoding="utf-8")
        
        # 4. Debounce wait (sleep > debounce)
        time.sleep(config.debounce_seconds + 0.5)
        
        # Verify heartbeat sent
        assert len(mock_aw_client.events) == 1
        event = mock_aw_client.events[0]
        payload = event["data"]
        
        # Verify exact payload
        assert payload["stage"] == "Config Stage"
        assert payload["task"] == "Config Task"
        assert payload["project_id"] == "ConfigProject"
        assert payload["status"] == "in_progress"
        assert payload["start_time"] == data["start_time"]
        assert payload["source"] == "config_test"  # Flattened metadata
        assert payload["file_path"] == str(task_file)
        
        # Verify event fields
        assert event["pulsetime"] == 120.0
        assert event.get("duration") == 0.0 or event.get("duration") == 0
        
        # Verify timestamp approx now
        ts = datetime.fromisoformat(event["timestamp"])
        assert ts > datetime.now(timezone.utc) - timedelta(seconds=10)
        
        # 5. Irrelevant change (whitespace)
        mock_aw_client.events.clear()
        # Write same data with different indentation
        task_file.write_text(json.dumps(data, indent=4), encoding="utf-8")
        
        time.sleep(config.debounce_seconds + 0.5)
        
        # Should skip heartbeat
        assert len(mock_aw_client.events) == 0
        
    finally:
        watcher.stop()


def test_metadata_allowlist_integration(integration_temp_dir: Path) -> None:
    """Test that changes to non-allowlisted metadata are ignored (irrelevant).

    Stage 5.3.2: Test irrelevant change (filtered metadata) skips heartbeat.
    """
    task_file = integration_temp_dir / "allowlist_task.json"
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(
        watch_path=task_file,
        client=mock_aw_client,
        testing=True,
        metadata_allowlist=["allowed"]
    )
    
    watcher = PipelineWatcher(
        task_file,
        client,
        debounce_seconds=0.1
    )
    
    try:
        watcher.start()
        
        # 1. Initial write
        data = {
            "current_stage": "S",
            "current_task": "T",
            "metadata": {"allowed": "yes", "ignored": "no"}
        }
        task_file.write_text(json.dumps(data), encoding="utf-8")
        time.sleep(0.5)
        
        assert len(mock_aw_client.events) == 1
        event = mock_aw_client.events[0]
        assert event["data"]["allowed"] == "yes"
        assert "ignored" not in event["data"]
        mock_aw_client.events.clear()
        
        # 2. Update ONLY ignored metadata
        data["metadata"]["ignored"] = "changed"
        task_file.write_text(json.dumps(data), encoding="utf-8")
        time.sleep(0.5)
        
        # Should be skipped as irrelevant (state didn't change after filtering)
        assert len(mock_aw_client.events) == 0
        
        # 3. Update allowed metadata
        data["metadata"]["allowed"] = "changed"
        task_file.write_text(json.dumps(data), encoding="utf-8")
        time.sleep(0.5)
        
        # Should trigger
        assert len(mock_aw_client.events) == 1
        assert mock_aw_client.events[0]["data"]["allowed"] == "changed"
        
    finally:
        watcher.stop()


def test_malformed_json_recovery_integration(integration_temp_dir: Path, caplog: LogCaptureFixture) -> None:
    """Test malformed JSON mid-run: write invalid -> skip -> recover on valid write."""
    task_file = integration_temp_dir / "malformed_recovery.json"
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(
        watch_path=task_file,
        client=mock_aw_client,
        testing=True
    )
    
    watcher = PipelineWatcher(
        task_file,
        client,
        debounce_seconds=0.1
    )
    
    try:
        watcher.start()
        
        # 1. Valid
        task_file.write_text(json.dumps({"current_stage": "Valid", "current_task": "1"}), encoding="utf-8")
        time.sleep(0.5)
        assert len(mock_aw_client.events) == 1
        mock_aw_client.events.clear()
        
        # 2. Malformed
        # Patch sleep to skip backoff retries
        with patch("aw_watcher_pipeline_stage.watcher.time.sleep"):
            task_file.write_text("{ broken", encoding="utf-8")
            time.sleep(0.5)
            assert len(mock_aw_client.events) == 0
            assert "Malformed JSON" in caplog.text
        
        # 3. Valid again
        task_file.write_text(json.dumps({"current_stage": "Valid", "current_task": "2"}), encoding="utf-8")
        time.sleep(0.5)
        assert len(mock_aw_client.events) == 1
        assert mock_aw_client.events[0]["data"]["task"] == "2"
        
    finally:
        watcher.stop()


def test_core_integration_flow(integration_temp_dir: Path) -> None:
    """
    Core integration test covering end-to-end flow as per Stage 5.3.2 requirements.
    
    Flow:
    1. Startup with valid config (file initially absent).
    2. Verify bucket creation (name/type).
    3. Create file (valid JSON) -> Debounce (>1s) -> Verify Heartbeat 1.
    4. Verify exact payload fields via mock client.assert_called_with.
    5. Modify file (meaningful change) -> Debounce -> Verify Heartbeat 2.
    6. Irrelevant change (metadata reorder) -> Debounce -> Verify NO Heartbeat.
    """
    task_file = integration_temp_dir / "core_flow.json"
    
    # 1. Startup with valid config
    config_args = {
        "watch_path": str(task_file),
        "testing": True,
        "pulsetime": 120.0,
        "debounce_seconds": 1.1
    }
    config = load_config(config_args)
    
    # Mock the AW client
    mock_client_instance = MagicMock()
    mock_client_instance.get_info.return_value = {"version": "test"}
    
    # Mock hostname for bucket name verification
    with patch("socket.gethostname", return_value="core-host"):
        client = PipelineClient(
            watch_path=config.watch_path,
            client=mock_client_instance,
            testing=config.testing
        )
        
        # 2. Observer start & Bucket creation
        client.ensure_bucket()
        
        # Verify bucket creation
        mock_client_instance.create_bucket.assert_called_with(
            "aw-watcher-pipeline-stage_core-host",
            event_type="current-pipeline-stage",
            queued=True
        )
        
        watcher = PipelineWatcher(
            config.watch_path,
            client,
            debounce_seconds=config.debounce_seconds
        )
        
        try:
            watcher.start()
            
            # 3. Write valid JSON (Creation)
            # Ensure file didn't exist (Initial absence)
            assert not task_file.exists()
            
            now_utc = datetime.now(timezone.utc)
            start_time_str = now_utc.isoformat()
            
            data = {
                "current_stage": "Integration Stage",
                "current_task": "Integration Task",
                "project_id": "PROJ-123",
                "status": "in_progress",
                "start_time": start_time_str,
                "metadata": {"env": "prod", "version": 1}
            }
            task_file.write_text(json.dumps(data), encoding="utf-8")
            
            # 4. Debounce (sleep > 1s)
            time.sleep(config.debounce_seconds + 0.5)
            
            # 5. Verify meaningful change detection & heartbeat sent with exact payload
            mock_client_instance.heartbeat.assert_called_with(
                "aw-watcher-pipeline-stage_core-host",
                ANY,
                pulsetime=120.0,
                queued=True
            )
            event = mock_client_instance.heartbeat.call_args[0][1]
            
            # Verify Event data
            assert event.data["stage"] == "Integration Stage"
            assert event.data["task"] == "Integration Task"
            assert event.data["project_id"] == "PROJ-123"
            assert event.data["status"] == "in_progress"
            assert event.data["start_time"] == start_time_str
            assert event.data["file_path"] == str(task_file)
            # Metadata flattened
            assert event.data["env"] == "prod"
            assert event.data["version"] == 1
            
            # Verify timestamp approx now
            assert (event.timestamp - now_utc).total_seconds() < 10.0
            # Verify duration
            assert event.duration == 0
            
            mock_client_instance.heartbeat.reset_mock()

            # 6. Modify file (Meaningful change)
            data["current_task"] = "Updated Task"
            task_file.write_text(json.dumps(data), encoding="utf-8")
            
            time.sleep(config.debounce_seconds + 0.5)
            
            mock_client_instance.heartbeat.assert_called_with(
                "aw-watcher-pipeline-stage_core-host",
                ANY,
                pulsetime=120.0,
                queued=True
            )
            event = mock_client_instance.heartbeat.call_args[0][1]
            assert event.data["task"] == "Updated Task"
            mock_client_instance.heartbeat.reset_mock()
            
            # 7. Test irrelevant change skips heartbeat
            # Write same data with reordered metadata keys (irrelevant change)
            data_reordered = data.copy()
            data_reordered["metadata"] = {"version": 1, "env": "prod"} # Swapped order
            task_file.write_text(json.dumps(data_reordered), encoding="utf-8")
            
            time.sleep(config.debounce_seconds + 0.5)
            
            mock_client_instance.heartbeat.assert_not_called()
            
        finally:
            watcher.stop()


def test_system_main_flow_and_shutdown(integration_temp_dir: Path, caplog: LogCaptureFixture) -> None:
    """
    Test main() end-to-end flow: startup, config load, component init, and graceful shutdown.
    Simulates SIGINT via KeyboardInterrupt in the main loop.
    """
    task_file = integration_temp_dir / "system_flow.json"
    task_file.write_text("{}")
    
    # Patch setup_logging to avoid configuring root logger during tests
    with patch("aw_watcher_pipeline_stage.main.setup_logging"):
        # Mock time.sleep to raise KeyboardInterrupt, simulating a signal to stop
        with patch("aw_watcher_pipeline_stage.main.time.sleep", side_effect=KeyboardInterrupt):
            # Use --testing to use MockActivityWatchClient
            argv = ["aw-watcher-pipeline-stage", "--watch-path", str(task_file), "--testing", "--log-level", "DEBUG"]
            with patch.object(sys, "argv", argv):
                main()
                
    # Verify startup
    assert f"Starting watcher on path: {task_file.absolute()}" in caplog.text
    
    # Verify graceful shutdown sequence (cleanup in finally block)
    assert "Watcher stopped." in caplog.text
    assert "Flushing event queue..." in caplog.text
    assert "Client closed." in caplog.text


def test_system_main_startup_failure_invalid_path(integration_temp_dir: Path, caplog: LogCaptureFixture) -> None:
    """Test that main() exits with code 1 if watch path is invalid."""
    invalid_path = integration_temp_dir / "nonexistent_dir" / "file.json"
    
    argv = ["aw-watcher-pipeline-stage", "--watch-path", str(invalid_path)]
    with patch.object(sys, "argv", argv):
        # Should exit due to config validation failure
        with pytest.raises(SystemExit) as excinfo:
            main()
        assert excinfo.value.code == 1
        # Verify critical/error log
        assert "Path not found" in caplog.text or "Failed to parse config" in caplog.text or "Watch directory not found" in caplog.text


def test_system_main_cross_platform_startup(integration_temp_dir: Path) -> None:
    """
    Test main() startup logic on simulated platforms (Windows/Darwin).
    Verifies that path resolution and config loading don't crash on different OSs.
    """
    task_file = integration_temp_dir / "platform.json"
    task_file.touch()
    
    # Custom exception to break out of main() after successful startup but before loop
    class StartupSuccess(Exception): pass

    # Mock PipelineWatcher to avoid starting real threads and to inject our exit exception
    with patch("aw_watcher_pipeline_stage.main.PipelineWatcher") as MockWatcher:
        MockWatcher.return_value.start.side_effect = StartupSuccess("Started")
        MockWatcher.return_value.observer.is_alive.return_value = True
        
        # Mock setup_logging
        with patch("aw_watcher_pipeline_stage.main.setup_logging"):
            argv = ["aw-watcher-pipeline-stage", "--watch-path", str(task_file), "--testing"]
            
            # 1. Windows
            with patch("sys.platform", "win32"), patch("os.name", "nt"):
                 with patch("pathlib.Path.resolve", return_value=task_file), patch("pathlib.Path.absolute", return_value=task_file):
                     with patch.object(sys, "argv", argv):
                         with pytest.raises(StartupSuccess):
                             main()
                         
            # 2. Darwin (macOS)
            with patch("sys.platform", "darwin"), patch("os.name", "posix"):
                 with patch("pathlib.Path.resolve", return_value=task_file), patch("pathlib.Path.absolute", return_value=task_file):
                     with patch.object(sys, "argv", argv):
                         with pytest.raises(StartupSuccess):
                             main()

            # 3. Linux
            with patch("sys.platform", "linux"), patch("os.name", "posix"):
                 with patch("pathlib.Path.resolve", return_value=task_file), patch("pathlib.Path.absolute", return_value=task_file):
                     with patch.object(sys, "argv", argv):
                         with pytest.raises(StartupSuccess):
                             main()


def test_offline_server_recovery_integration(integration_temp_dir: Path) -> None:
    """Test error recovery: offline server (ConnectionError) -> buffer (queued=True) -> reconnect flush."""
    task_file = integration_temp_dir / "offline_test.json"
    task_file.write_text(json.dumps({"current_stage": "Offline", "current_task": "Start"}), encoding="utf-8")

    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    
    watcher = PipelineWatcher(task_file, client, pulsetime=120.0, debounce_seconds=0.1)
    
    try:
        watcher.start()
        time.sleep(0.2)
        assert len(mock_aw_client.events) == 1
        mock_aw_client.events.clear()

        # 1. Simulate Offline (ConnectionError)
        with patch.object(mock_aw_client, "heartbeat", side_effect=ConnectionError("Server down")) as mock_hb:
            # Trigger update
            task_file.write_text(json.dumps({"current_stage": "Offline", "current_task": "Update 1"}), encoding="utf-8")
            time.sleep(0.5)
            
            # Verify it tried to send
            assert mock_hb.called
            # Verify queued=True was passed (PipelineClient handles this)
            _, kwargs = mock_hb.call_args
            assert kwargs.get("queued") is True

        # 2. Simulate Recovery (No error)
        # Trigger update
        task_file.write_text(json.dumps({"current_stage": "Offline", "current_task": "Update 2"}), encoding="utf-8")
        time.sleep(0.5)
        
        # Should succeed now
        assert len(mock_aw_client.events) == 1
        assert mock_aw_client.events[0]["data"]["task"] == "Update 2"
        
        # 3. Verify Flush
        with patch.object(mock_aw_client, "flush") as mock_flush:
            client.flush_queue()
            mock_flush.assert_called_once()
            
    finally:
        watcher.stop()


def test_file_deletion_recovery_integration(integration_temp_dir: Path, caplog: LogCaptureFixture) -> None:
    """Test file deletion -> pause heartbeats -> log warning -> recover on recreate."""
    task_file = integration_temp_dir / "delete_test.json"
    task_file.write_text(json.dumps({"current_stage": "Alive", "current_task": "1"}), encoding="utf-8")
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    watcher = PipelineWatcher(task_file, client, debounce_seconds=0.1)
    
    try:
        watcher.start()
        time.sleep(0.2)
        assert len(mock_aw_client.events) == 1
        mock_aw_client.events.clear()
        
        # Delete
        os.remove(task_file)
        time.sleep(0.5)
        
        # Should send paused event
        assert len(mock_aw_client.events) == 1
        assert "File deleted" in caplog.text
        assert mock_aw_client.events[0]["data"]["status"] == "paused"
        mock_aw_client.events.clear()
        
        # Recreate
        task_file.write_text(json.dumps({"current_stage": "Alive", "current_task": "2"}), encoding="utf-8")
        time.sleep(0.5)
        
        # Should recover
        assert len(mock_aw_client.events) == 1
        assert mock_aw_client.events[0]["data"]["task"] == "2"
        
    finally:
        watcher.stop()


def test_permission_recovery_integration(integration_temp_dir: Path, caplog: LogCaptureFixture) -> None:
    """Test permission change (chmod 000) -> log error -> skip read -> retry on chmod 644."""
    if os.name == "nt":
        pytest.skip("Skipping permission test on Windows")
        
    task_file = integration_temp_dir / "perm_test.json"
    task_file.write_text(json.dumps({"current_stage": "Perm", "current_task": "1"}), encoding="utf-8")
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    watcher = PipelineWatcher(task_file, client, debounce_seconds=0.1)
    
    try:
        watcher.start()
        time.sleep(0.2)
        mock_aw_client.events.clear()
        
        # 1. Make unreadable
        task_file.chmod(0o000)
        
        # Patch sleep to skip backoff retries
        with patch("aw_watcher_pipeline_stage.watcher.time.sleep"):
            # 2. Trigger event manually (since we can't write to it easily)
            # Trigger event manually as chmod might not trigger modification event on all platforms/filesystems in test env
            event = MagicMock()
            event.is_directory = False
            event.src_path = str(task_file)
            watcher.handler.on_modified(event)
            time.sleep(0.5)
            
            # Should be no new events (read failed)
            assert len(mock_aw_client.events) == 0
            assert "Permission denied" in caplog.text
        
        # 3. Restore and update
        task_file.chmod(0o644)
        task_file.write_text(json.dumps({"current_stage": "Perm", "current_task": "2"}), encoding="utf-8")
        time.sleep(0.5)
        
        # Should recover
        assert len(mock_aw_client.events) == 1
        assert mock_aw_client.events[0]["data"]["task"] == "2"
        
    finally:
        if task_file.exists():
            task_file.chmod(0o644)
        watcher.stop()


def test_file_move_recovery_integration(integration_temp_dir: Path, caplog: LogCaptureFixture) -> None:
    """Test file move/rename: move away (pause) -> move back (recover)."""
    task_file = integration_temp_dir / "move_test.json"
    task_file.write_text(json.dumps({"current_stage": "Move", "current_task": "1"}), encoding="utf-8")
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    watcher = PipelineWatcher(task_file, client, debounce_seconds=0.1)
    
    try:
        watcher.start()
        time.sleep(0.2)
        mock_aw_client.events.clear()
        
        with caplog.at_level(logging.INFO):
            # Rename file (Move Away)
            new_path = integration_temp_dir / "moved_file.json"
            os.rename(task_file, new_path)
            time.sleep(0.5)
            
            # Should detect as deletion/move-away -> pause
            assert len(mock_aw_client.events) >= 1
            assert "File moved away" in caplog.text
        assert mock_aw_client.events[-1]["data"]["status"] == "paused"
        mock_aw_client.events.clear()
        
        # Rename back (Move To)
        os.rename(new_path, task_file)
        time.sleep(0.5)
        
        # Should recover
        assert len(mock_aw_client.events) >= 1
        assert mock_aw_client.events[-1]["data"]["task"] == "1"
        
    finally:
        watcher.stop()


def test_initial_presence_startup(integration_temp_dir: Path) -> None:
    """Test startup with file already present triggers immediate heartbeat."""
    task_file = integration_temp_dir / "initial.json"
    data = {
        "current_stage": "Initial",
        "current_task": "Startup",
        "status": "in_progress"
    }
    task_file.write_text(json.dumps(data), encoding="utf-8")
    
    mock_client_instance = MagicMock()
    mock_client_instance.get_info.return_value = {"version": "test"}
    
    with patch("socket.gethostname", return_value="init-host"):
        client = PipelineClient(
            watch_path=task_file,
            client=mock_client_instance,
            testing=True
        )
        
        watcher = PipelineWatcher(
            task_file,
            client,
            debounce_seconds=0.1
        )
        
        try:
            watcher.start()
            # Wait for initial read
            time.sleep(0.5)
            
            assert mock_client_instance.heartbeat.called
            event = mock_client_instance.heartbeat.call_args[0][1]
            assert event.data["stage"] == "Initial"
            
        finally:
            watcher.stop()


@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
def test_system_subprocess_shutdown(integration_temp_dir: Path, sig: int) -> None:
    """
    System-level test: Start process via subprocess, send signal, verify graceful exit.
    
    Verifies:
    - Startup with CLI args (argparse)
    - Graceful shutdown on SIGINT/SIGTERM (os.kill)
    - Queue flush and client close
    """
    task_file = integration_temp_dir / "subprocess_task.json"
    task_file.write_text(json.dumps({"current_stage": "Subprocess", "current_task": "Test"}))

    # Construct command
    cmd = [
        sys.executable,
        "-m",
        "aw_watcher_pipeline_stage.main",
        "--watch-path",
        str(task_file),
        "--testing",
        "--log-level",
        "DEBUG"
    ]

    # Set PYTHONPATH to include project root
    env = os.environ.copy()
    root_dir = Path(__file__).resolve().parents[1]
    env["PYTHONPATH"] = f"{str(root_dir)}{os.pathsep}{env.get('PYTHONPATH', '')}"

    # Start process
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
        bufsize=1
    )

    captured_stdout = []
    
    try:
        # Wait for startup
        start_wait = time.time()
        started = False
        while time.time() - start_wait < 10.0:
            if process.poll() is not None:
                break
            line = process.stdout.readline()
            if line:
                captured_stdout.append(line)
                if "Observer started" in line:
                    started = True
                    break
        
        if not started:
            rest, _ = process.communicate(timeout=1)
            captured_stdout.append(rest)
            pytest.fail(f"Process failed to start: {''.join(captured_stdout)}")

        # Send signal
        process.send_signal(sig)
        
        # Wait for exit
        try:
            rest, _ = process.communicate(timeout=5)
            captured_stdout.append(rest)
        except subprocess.TimeoutExpired:
            process.kill()
            pytest.fail(f"Process did not exit after signal {sig}")

        assert process.returncode == 0
        
        output = "".join(captured_stdout)
        sig_name = signal.Signals(sig).name
        assert f"Received signal {sig_name}" in output
        assert "Watcher stopped." in output
        assert "Flushing event queue..." in output
        assert "Client closed." in output

    finally:
        if process.poll() is None:
            process.kill()


def test_system_main_args_propagation(integration_temp_dir: Path) -> None:
    """Test that CLI arguments are correctly propagated to components via main()."""
    task_file = integration_temp_dir / "args.json"
    task_file.touch()
    log_file = integration_temp_dir / "args.log"

    argv = [
        "aw-watcher-pipeline-stage",
        "--watch-path", str(task_file),
        "--port", "5699",
        "--pulsetime", "60.0",
        "--debounce-seconds", "2.5",
        "--log-level", "DEBUG",
        "--log-file", str(log_file),
        "--testing",
        "--batch-size-limit", "10"
    ]

    class StartupSuccess(Exception):
        pass

    with patch("aw_watcher_pipeline_stage.main.PipelineWatcher") as MockWatcher, \
         patch("aw_watcher_pipeline_stage.main.PipelineClient") as MockClient, \
         patch("aw_watcher_pipeline_stage.main.setup_logging") as MockSetupLogging, \
         patch.object(sys, "argv", argv):

        # Make start raise exception to exit main loop immediately after init
        MockWatcher.return_value.start.side_effect = StartupSuccess()
        MockWatcher.return_value.observer.is_alive.return_value = True
        MockWatcher.return_value.watch_dir.exists.return_value = True

        with pytest.raises(StartupSuccess):
            main()

        # Verify Client init args
        MockClient.assert_called_once()
        client_kwargs = MockClient.call_args[1]
        assert client_kwargs["port"] == 5699
        assert client_kwargs["pulsetime"] == 60.0
        assert client_kwargs["testing"] is True

        # Verify Watcher init args
        MockWatcher.assert_called_once()
        watcher_args = MockWatcher.call_args[0]
        watcher_kwargs = MockWatcher.call_args[1]
        # args[0] is path
        assert str(watcher_args[0]) == str(task_file)
        assert watcher_kwargs["debounce_seconds"] == 2.5
        assert watcher_kwargs["batch_size_limit"] == 10

        # Verify Logging setup
        MockSetupLogging.assert_called_once_with("DEBUG", str(log_file))


def test_system_main_startup_failure_observer(integration_temp_dir: Path, caplog: LogCaptureFixture) -> None:
    """Test that main() exits with code 1 if observer fails to start."""
    task_file = integration_temp_dir / "observer_fail.json"
    task_file.touch()

    with patch("aw_watcher_pipeline_stage.main.PipelineWatcher") as MockWatcher:
        # Simulate observer start failure
        MockWatcher.return_value.start.side_effect = RuntimeError("Observer failed to start")
        MockWatcher.return_value.watch_dir.exists.return_value = True

        argv = ["aw-watcher-pipeline-stage", "--watch-path", str(task_file)]
        with patch.object(sys, "argv", argv):
            with pytest.raises(SystemExit) as excinfo:
                main()
            assert excinfo.value.code == 1

    # Verify error log
    assert "Observer failed to start" in caplog.text


def test_system_main_startup_failure_client(integration_temp_dir: Path, caplog: LogCaptureFixture) -> None:
    """Test that main() exits with code 1 if client fails to initialize."""
    task_file = integration_temp_dir / "client_fail.json"
    task_file.touch()

    with patch("aw_watcher_pipeline_stage.main.PipelineClient") as MockClient:
        # Simulate client init failure
        MockClient.side_effect = RuntimeError("Client init failed")

        # Mock Watcher to ensure we don't fail on import or other checks
        with patch("aw_watcher_pipeline_stage.main.PipelineWatcher"):
            argv = ["aw-watcher-pipeline-stage", "--watch-path", str(task_file)]
            with patch.object(sys, "argv", argv):
                with pytest.raises(SystemExit) as excinfo:
                    main()
                assert excinfo.value.code == 1

    assert "Client init failed" in caplog.text


def test_system_main_invalid_cli_args() -> None:
    """Test that main() exits with code 2 on invalid CLI args (argparse behavior)."""
    argv = ["aw-watcher-pipeline-stage", "--port", "invalid_port"]
    with patch.object(sys, "argv", argv):
        with pytest.raises(SystemExit) as excinfo:
            main()
        assert excinfo.value.code == 2


def test_system_main_help(capsys: CaptureFixture[str]) -> None:
    """Test that main() prints help and exits with code 0."""
    argv = ["aw-watcher-pipeline-stage", "--help"]
    with patch.object(sys, "argv", argv):
        with pytest.raises(SystemExit) as excinfo:
            main()
        assert excinfo.value.code == 0

    captured = capsys.readouterr()
    assert "usage: aw-watcher-pipeline-stage" in captured.out


def test_main_cleanup_on_system_exit(integration_temp_dir: Path) -> None:
    """Test that main() performs cleanup (queue flush, close) on SystemExit (e.g. from signal handler)."""
    task_file = integration_temp_dir / "cleanup.json"
    task_file.touch()

    with patch("aw_watcher_pipeline_stage.main.PipelineWatcher") as MockWatcher, \
         patch("aw_watcher_pipeline_stage.main.PipelineClient") as MockClient, \
         patch("aw_watcher_pipeline_stage.main.setup_logging"), \
         patch("aw_watcher_pipeline_stage.main.load_config") as mock_load_config, \
         patch("aw_watcher_pipeline_stage.main.time.sleep", side_effect=SystemExit(0)):

        # Setup config
        mock_config = MagicMock()
        mock_config.watch_path = str(task_file)
        mock_config.port = 5600
        mock_config.testing = True
        mock_config.pulsetime = 120.0
        mock_config.debounce_seconds = 1.0
        mock_config.log_level = "INFO"
        mock_config.log_file = None
        mock_load_config.return_value = mock_config

        # Setup Watcher to be alive so main loop starts, then sleep raises SystemExit
        MockWatcher.return_value.observer.is_alive.return_value = True
        MockWatcher.return_value.watch_dir.exists.return_value = True

        with patch.object(sys, "argv", ["prog"]):
            with pytest.raises(SystemExit):
                main()

        # Verify cleanup
        MockClient.return_value.flush_queue.assert_called_once()
        MockClient.return_value.close.assert_called_once()
        MockWatcher.return_value.stop.assert_called_once()


def test_system_main_shutdown_during_startup(integration_temp_dir: Path, caplog: LogCaptureFixture) -> None:
    """Test graceful shutdown if SIGINT is received during startup (e.g. waiting for client)."""
    task_file = integration_temp_dir / "startup_shutdown.json"
    task_file.touch()

    with patch("aw_watcher_pipeline_stage.main.PipelineClient") as MockClient:
        # Simulate KeyboardInterrupt during wait_for_start
        MockClient.return_value.wait_for_start.side_effect = KeyboardInterrupt

        with patch("aw_watcher_pipeline_stage.main.setup_logging"):
            argv = ["aw-watcher-pipeline-stage", "--watch-path", str(task_file), "--testing"]
            with patch.object(sys, "argv", argv):
                main()

    # Verify cleanup logs
    assert "Watcher stopped." in caplog.text
    assert "Client closed." in caplog.text


def test_system_main_argparse_namespace_handling(integration_temp_dir: Path) -> None:
    """Test main() flow when argparse returns a populated Namespace."""
    from argparse import Namespace
    
    task_file = integration_temp_dir / "namespace.json"
    task_file.touch()
    
    args = Namespace(
        watch_path=str(task_file),
        port=5600,
        testing=True,
        pulsetime=120.0,
        debounce_seconds=1.0,
        log_level="INFO",
        log_file=None
    )
    
    class StartupSuccess(Exception): pass

    with patch("argparse.ArgumentParser.parse_args", return_value=args):
        with patch("aw_watcher_pipeline_stage.main.PipelineWatcher") as MockWatcher:
            MockWatcher.return_value.start.side_effect = StartupSuccess()
            MockWatcher.return_value.observer.is_alive.return_value = True
            MockWatcher.return_value.watch_dir.exists.return_value = True
            
            with patch("aw_watcher_pipeline_stage.main.PipelineClient") as MockClient:
                with patch("aw_watcher_pipeline_stage.main.setup_logging"):
                    with pytest.raises(StartupSuccess):
                        main()
                        
                # Verify Client init
                MockClient.assert_called_with(
                    watch_path=str(task_file),
                    port=5600,
                    testing=True,
                    pulsetime=120.0
                )


def test_mixed_config_propagation_integration(integration_temp_dir: Path) -> None:
    """
    Test that configuration propagates correctly from mixed sources (Env + CLI)
    through main() to the components (Client, Watcher).
    """
    task_file = integration_temp_dir / "mixed_config.json"
    task_file.touch()
    
    # Env sets port (to be overridden), pulsetime (to be used), and batch_size_limit
    env = {
        "AW_WATCHER_PORT": "5611",
        "AW_WATCHER_PULSETIME": "60.0",
        "AW_WATCHER_METADATA_ALLOWLIST": "env_key",
        "AW_WATCHER_BATCH_SIZE_LIMIT": "20",
        "AW_WATCHER_DEBOUNCE_SECONDS": "0.1"
    }
    
    # CLI sets watch_path, overrides port, sets testing, overrides metadata_allowlist
    argv = [
        "aw-watcher-pipeline-stage",
        "--watch-path", str(task_file),
        "--port", "5622",
        "--testing",
        "--metadata-allowlist", "cli_key1,cli_key2",
        "--debounce-seconds", "2.5",
        "--log-level", "DEBUG",
        "--log-file", "test.log"
    ]
    
    class StartupSuccess(Exception): pass

    with patch.dict(os.environ, env, clear=True):
        with patch("aw_watcher_pipeline_stage.main.PipelineWatcher") as MockWatcher, \
             patch("aw_watcher_pipeline_stage.main.PipelineClient") as MockClient, \
             patch("aw_watcher_pipeline_stage.main.setup_logging"), \
             patch.object(sys, "argv", argv):
            
            # Stop main after init
            MockWatcher.return_value.start.side_effect = StartupSuccess()
            MockWatcher.return_value.observer.is_alive.return_value = True
            MockWatcher.return_value.watch_dir.exists.return_value = True
            
            with pytest.raises(StartupSuccess):
                from aw_watcher_pipeline_stage.main import main
                main()
                
            # Verify Client init
            MockClient.assert_called_once()
            client_kwargs = MockClient.call_args[1]
            
            # CLI override (5622) should win over Env (5611)
            assert client_kwargs["port"] == 5622
            
            # Env value (60.0) should be used as CLI didn't specify it
            assert client_kwargs["pulsetime"] == 60.0
            
            # CLI flag
            assert client_kwargs["testing"] is True
            
            # CLI override for metadata allowlist
            assert client_kwargs["metadata_allowlist"] == ["cli_key1", "cli_key2"]
            
            # Verify Watcher init
            MockWatcher.assert_called_once()
            watcher_args = MockWatcher.call_args[0]
            watcher_kwargs = MockWatcher.call_args[1]
            assert str(watcher_args[0]) == str(task_file)
            
            # Env value for batch_size_limit should be used
            assert watcher_kwargs["batch_size_limit"] == 20
            
            # CLI value for debounce (overrides Env)
            assert watcher_kwargs["debounce_seconds"] == 2.5
            
            # Verify Logging setup
            MockSetupLogging.assert_called_with("DEBUG", "test.log")


def test_testing_flag_propagation_from_env(integration_temp_dir: Path) -> None:
    """Test that testing mode is enabled via Env when CLI flag is absent."""
    task_file = integration_temp_dir / "env_testing.json"
    task_file.touch()
    
    env = {"AW_WATCHER_TESTING": "true"}
    argv = ["aw-watcher-pipeline-stage", "--watch-path", str(task_file)]
    
    class StartupSuccess(Exception): pass

    with patch.dict(os.environ, env, clear=True):
        with patch("aw_watcher_pipeline_stage.main.PipelineWatcher") as MockWatcher, \
             patch("aw_watcher_pipeline_stage.main.PipelineClient") as MockClient, \
             patch("aw_watcher_pipeline_stage.main.setup_logging"), \
             patch.object(sys, "argv", argv):
            
            MockWatcher.return_value.start.side_effect = StartupSuccess()
            MockWatcher.return_value.observer.is_alive.return_value = True
            MockWatcher.return_value.watch_dir.exists.return_value = True
            
            with pytest.raises(StartupSuccess):
                from aw_watcher_pipeline_stage.main import main
                main()
            
            # Verify Client init received testing=True
            client_kwargs = MockClient.call_args[1]
            assert client_kwargs["testing"] is True


def test_batch_size_limit_trigger(integration_temp_dir: Path) -> None:
    """Test that hitting batch_size_limit triggers immediate processing (bypassing long debounce)."""
    task_file = integration_temp_dir / "batch_limit.json"
    task_file.touch()

    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(
        watch_path=task_file,
        client=mock_aw_client,
        testing=True
    )

    # Set long debounce (10s) but small batch limit (2)
    watcher = PipelineWatcher(
        task_file,
        client,
        debounce_seconds=10.0,
        batch_size_limit=2
    )

    try:
        watcher.start()
        client.ensure_bucket()

        # 1. First write
        task_file.write_text(json.dumps({"current_stage": "1", "current_task": "1"}), encoding="utf-8")
        time.sleep(0.1)
        # Should be queued, not processed yet (debounce 10s)
        assert len(mock_aw_client.events) == 0

        # 2. Second write (Hits limit 2)
        task_file.write_text(json.dumps({"current_stage": "2", "current_task": "2"}), encoding="utf-8")

        # Wait short time (much less than 10s)
        time.sleep(0.5)

        # Should have triggered
        assert len(mock_aw_client.events) >= 1
        assert mock_aw_client.events[-1]["data"]["task"] == "2"

    finally:
        watcher.stop()


def test_main_propagation_metadata_allowlist(integration_temp_dir: Path) -> None:
    """Test that metadata_allowlist propagates from Env to Client via main()."""
    task_file = integration_temp_dir / "metadata_prop.json"
    task_file.touch()
    
    env = {
        "AW_WATCHER_METADATA_ALLOWLIST": "allowed_key",
        "AW_WATCHER_TESTING": "true"
    }
    
    argv = ["aw-watcher-pipeline-stage", "--watch-path", str(task_file)]
    
    class StartupSuccess(Exception): pass

    with patch.dict(os.environ, env, clear=True):
        with patch("aw_watcher_pipeline_stage.main.PipelineWatcher") as MockWatcher, \
             patch("aw_watcher_pipeline_stage.main.PipelineClient") as MockClient, \
             patch("aw_watcher_pipeline_stage.main.setup_logging"), \
             patch.object(sys, "argv", argv):
            
            MockWatcher.return_value.start.side_effect = StartupSuccess()
            MockWatcher.return_value.observer.is_alive.return_value = True
            MockWatcher.return_value.watch_dir.exists.return_value = True
            
            with pytest.raises(StartupSuccess):
                from aw_watcher_pipeline_stage.main import main
                main()
            
            # Verify Client init
            MockClient.assert_called_once()
            client_kwargs = MockClient.call_args[1]
            assert client_kwargs["metadata_allowlist"] == ["allowed_key"]


def test_main_propagation_port(integration_temp_dir: Path) -> None:
    """Test that port propagates from CLI to Client via main()."""
    task_file = integration_temp_dir / "port_prop.json"
    task_file.touch()
    
    argv = ["aw-watcher-pipeline-stage", "--watch-path", str(task_file), "--port", "1234", "--testing"]
    
    class StartupSuccess(Exception): pass

    with patch("aw_watcher_pipeline_stage.main.PipelineWatcher") as MockWatcher, \
         patch("aw_watcher_pipeline_stage.main.PipelineClient") as MockClient, \
         patch("aw_watcher_pipeline_stage.main.setup_logging"), \
         patch.object(sys, "argv", argv):
        
        MockWatcher.return_value.start.side_effect = StartupSuccess()
        MockWatcher.return_value.observer.is_alive.return_value = True
        MockWatcher.return_value.watch_dir.exists.return_value = True
        
        with pytest.raises(StartupSuccess):
            from aw_watcher_pipeline_stage.main import main
            main()
        
        # Verify Client init
        MockClient.assert_called_once()
        client_kwargs = MockClient.call_args[1]
        assert client_kwargs["port"] == 1234


def test_main_propagation_debounce(integration_temp_dir: Path) -> None:
    """Test that debounce_seconds propagates from Config to Watcher via main()."""
    task_file = integration_temp_dir / "debounce_prop.json"
    task_file.touch()
    
    # Set via Env
    env = {
        "AW_WATCHER_DEBOUNCE_SECONDS": "2.5",
        "AW_WATCHER_TESTING": "true"
    }
    
    argv = ["aw-watcher-pipeline-stage", "--watch-path", str(task_file)]
    
    class StartupSuccess(Exception): pass

    with patch.dict(os.environ, env, clear=True):
        with patch("aw_watcher_pipeline_stage.main.PipelineWatcher") as MockWatcher, \
             patch("aw_watcher_pipeline_stage.main.PipelineClient"), \
             patch("aw_watcher_pipeline_stage.main.setup_logging"), \
             patch.object(sys, "argv", argv):
            
            MockWatcher.return_value.start.side_effect = StartupSuccess()
            MockWatcher.return_value.observer.is_alive.return_value = True
            MockWatcher.return_value.watch_dir.exists.return_value = True
            
            with pytest.raises(StartupSuccess):
                from aw_watcher_pipeline_stage.main import main
                main()
            
            # Verify Watcher init
            MockWatcher.assert_called_once()
            watcher_kwargs = MockWatcher.call_args[1]
            assert watcher_kwargs["debounce_seconds"] == 2.5


def test_main_propagation_batch_size(integration_temp_dir: Path) -> None:
    """Test that batch_size_limit propagates from CLI to Watcher via main()."""
    task_file = integration_temp_dir / "batch_prop.json"
    task_file.touch()
    
    argv = ["aw-watcher-pipeline-stage", "--watch-path", str(task_file), "--batch-size-limit", "50", "--testing"]
    
    class StartupSuccess(Exception): pass

    with patch("aw_watcher_pipeline_stage.main.PipelineWatcher") as MockWatcher, \
         patch("aw_watcher_pipeline_stage.main.PipelineClient"), \
         patch("aw_watcher_pipeline_stage.main.setup_logging"), \
         patch.object(sys, "argv", argv):
        
        MockWatcher.return_value.start.side_effect = StartupSuccess()
        MockWatcher.return_value.observer.is_alive.return_value = True
        MockWatcher.return_value.watch_dir.exists.return_value = True
        
        with pytest.raises(StartupSuccess):
            from aw_watcher_pipeline_stage.main import main
            main()
        
        # Verify Watcher init
        MockWatcher.assert_called_once()
        watcher_kwargs = MockWatcher.call_args[1]
        assert watcher_kwargs["batch_size_limit"] == 50
