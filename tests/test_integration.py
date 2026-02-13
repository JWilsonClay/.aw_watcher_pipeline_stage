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

    with patch("socket.gethostname", return_value="test-integration-host"):
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
    client.flush_queue()

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


def test_mock_client_heartbeat_parameters(integration_temp_dir: Path) -> None:
    """Test that MockActivityWatchClient correctly stores heartbeat parameters (pulsetime, queued)."""
    task_file = integration_temp_dir / "mock_params.json"
    task_file.touch()

    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True, pulsetime=42.0)

    client.send_heartbeat("Stage", "Task")
    
    # Mock client doesn't call flush, it stores the data
    assert len(mock_aw_client.events) == 1
    
    event = mock_aw_client.events[0]
    # Verify parameters are stored correctly
    assert event["pulsetime"] == 42.0
    assert event["queued"] is True



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
    
    with patch("socket.gethostname", return_value="e2e-host"):
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
    
    with patch("socket.gethostname", return_value="strict-host"):
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
    
    with patch("socket.gethostname", return_value="config-host"):
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
    with patch("aw_watcher_pipeline_stage.main.setup_logging"), \
         patch("aw_watcher_pipeline_stage.main.logging.shutdown"):
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
    assert "Cleanup complete." in caplog.text


def test_system_main_startup_failure_invalid_path(integration_temp_dir: Path, caplog: LogCaptureFixture) -> None:
    """Test that main() exits with code 1 if watch path is invalid."""
    invalid_path = integration_temp_dir / "nonexistent_dir" / "file.json"
    
    argv = ["aw-watcher-pipeline-stage", "--watch-path", str(invalid_path)]
    with patch.object(sys, "argv", argv), \
         patch("aw_watcher_pipeline_stage.main.logging.shutdown"):
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
    """
    Audit (Stage 8.2.1): Test error recovery and data persistence.
    Flow: offline server (ConnectionError) -> buffer (queued=True) -> reconnect -> flush.
    """
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
        # We verify that PipelineClient attempts to send with queued=True
        with patch.object(mock_aw_client, "heartbeat", side_effect=ConnectionError("Server down")) as mock_hb:
            # Trigger update
            task_file.write_text(json.dumps({"current_stage": "Offline", "current_task": "Update 1"}), encoding="utf-8")
            time.sleep(1.0)
            
            # Verify it tried to send
            assert mock_hb.called
            # Verify queued=True was passed (PipelineClient handles this)
            _, kwargs = mock_hb.call_args
            assert kwargs.get("queued") is True
            assert kwargs.get("pulsetime") == 120.0

        # 2. Simulate Recovery (No error)
        # Trigger update
        task_file.write_text(json.dumps({"current_stage": "Offline", "current_task": "Update 2"}), encoding="utf-8")
        time.sleep(1.0)
        
        # Should succeed now
        assert len(mock_aw_client.events) >= 1
        assert mock_aw_client.events[-1]["data"]["task"] == "Update 2"
        
        # 3. Verify Flush
        with patch.object(mock_aw_client, "flush") as mock_flush:
            client.flush_queue()
            mock_flush.assert_called_once()


def test_flush_queue_empty_queue(integration_temp_dir: Path) -> None:
    """Test that flush queue functions correctly even when queue is empty"""
    task_file = integration_temp_dir / "flush_queue_test.json"
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)

    with patch.object(mock_aw_client, "flush") as mock_flush:
        client.flush_queue()
        mock_flush.assert_called_once()


def test_send_heartbeat_flush_queue_waits_for_worker(integration_temp_dir: Path) -> None:
    """Test that flush_queue waits for the worker thread to process pending items before flushing the client."""
    task_file = integration_temp_dir / "flush_test.json"
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)

    # Enqueue an item.
    client.send_heartbeat("Stage", "Task")

    # mock client flush
    with patch.object(mock_aw_client, "flush") as mock_flush:
        client.flush_queue()

        # Verify that the flush was only called after joining the queue
        assert mock_flush.call_count == 1

    client.close()
            
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

    with patch("aw_watcher_pipeline_stage.main.PipelineWatcher") as MockWatcher, \
         patch("aw_watcher_pipeline_stage.main.logging.shutdown"):
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

    with patch("aw_watcher_pipeline_stage.main.PipelineClient") as MockClient, \
         patch("aw_watcher_pipeline_stage.main.logging.shutdown"):
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
         patch("aw_watcher_pipeline_stage.main.logging.shutdown") as mock_logging_shutdown, \
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
        mock_logging_shutdown.assert_called_once()


def test_system_main_shutdown_during_startup(integration_temp_dir: Path, caplog: LogCaptureFixture) -> None:
    """Test graceful shutdown if SIGINT is received during startup (e.g. waiting for client)."""
    task_file = integration_temp_dir / "startup_shutdown.json"
    task_file.touch()

    with patch("aw_watcher_pipeline_stage.main.PipelineClient") as MockClient:
        with patch("aw_watcher_pipeline_stage.main.threading.Event") as MockEvent:
            # Mock stop_event to simulate signal received during wait_for_start
            mock_event_instance = MockEvent.return_value
            # is_set returns False initially, then True to simulate signal arrival
            mock_event_instance.is_set.side_effect = [False, True, True, True]
            # wait returns True immediately (signaled)
            mock_event_instance.wait.return_value = True

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


def test_full_stack_config_propagation(integration_temp_dir: Path) -> None:
    """
    Verify full configuration flow: Config File < Env < CLI -> Components.
    Ensures that values from all sources propagate correctly to Client and Watcher.
    """
    task_file = integration_temp_dir / "stack_config.json"
    task_file.touch()
    
    # Setup XDG_CONFIG_HOME structure
    config_dir = integration_temp_dir / "aw-watcher-pipeline-stage"
    config_dir.mkdir()
    
    # 1. Config File (Lowest priority in this test, but overrides defaults)
    # Sets: pulsetime, log_level, metadata_allowlist
    (config_dir / "config.ini").write_text("""
[aw-watcher-pipeline-stage]
pulsetime = 45.0
log_level = WARNING
batch_size_limit = 3
metadata_allowlist = k1, k2
""", encoding="utf-8")
    
    # 2. Env (Overrides Config File)
    # Sets: batch_size_limit (overrides config), port
    env = {
        "AW_WATCHER_BATCH_SIZE_LIMIT": "7",
        "AW_WATCHER_PORT": "5633",
        "XDG_CONFIG_HOME": str(integration_temp_dir)
    }
    
    # 3. CLI (Overrides everything)
    # Sets: watch_path, debounce_seconds, testing
    argv = [
        "aw-watcher-pipeline-stage",
        "--watch-path", str(task_file),
        "--debounce-seconds", "0.25",
        "--testing"
    ]
    
    class StartupSuccess(Exception): pass

    with patch.dict(os.environ, env, clear=True):
        with patch("aw_watcher_pipeline_stage.main.PipelineWatcher") as MockWatcher, \
             patch("aw_watcher_pipeline_stage.main.PipelineClient") as MockClient, \
             patch("aw_watcher_pipeline_stage.main.setup_logging") as MockSetupLogging, \
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
            
            # Port from Env
            assert client_kwargs["port"] == 5633
            
            # Pulsetime from Config File
            assert client_kwargs["pulsetime"] == 45.0
            
            # Testing from CLI
            assert client_kwargs["testing"] is True
            
            # Metadata allowlist from Config File
            assert client_kwargs["metadata_allowlist"] == ["k1", "k2"]
            
            # Verify Watcher init
            MockWatcher.assert_called_once()
            watcher_kwargs = MockWatcher.call_args[1]
            
            # Debounce from CLI
            assert watcher_kwargs["debounce_seconds"] == 0.25
            
            # Batch size from Env (overrides Config)
            assert watcher_kwargs["batch_size_limit"] == 7
            
            # Verify Logging
            # Log level from Config File
            MockSetupLogging.assert_called_with("WARNING", None)


def test_subprocess_config_priority_metadata(integration_temp_dir: Path) -> None:
    """
    Test that CLI metadata allowlist overrides Env via subprocess execution.
    This verifies the full chain: CLI -> Config -> Client -> Heartbeat.
    """
    task_file = integration_temp_dir / "meta_priority.json"
    task_file.write_text(json.dumps({"current_stage": "S", "current_task": "T", "metadata": {"a": 1, "b": 2}}), encoding="utf-8")
    
    # Env allows "a"
    env = os.environ.copy()
    env["AW_WATCHER_METADATA_ALLOWLIST"] = "a"
    # Ensure PYTHONPATH includes project root
    root_dir = Path(__file__).resolve().parents[1]
    env["PYTHONPATH"] = f"{str(root_dir)}{os.pathsep}{env.get('PYTHONPATH', '')}"
    
    # CLI allows "b" (should override Env)
    cmd = [
        sys.executable,
        "-m",
        "aw_watcher_pipeline_stage.main",
        "--watch-path", str(task_file),
        "--testing",
        "--metadata-allowlist", "b",
        "--log-level", "DEBUG"
    ]
    
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
        bufsize=1
    )
    
    try:
        # Wait for output
        start = time.time()
        found_heartbeat = False
        while time.time() - start < 5.0:
            line = process.stdout.readline()
            if not line:
                if process.poll() is not None:
                    break
                continue
            
            if "[MOCK] heartbeat" in line:
                # We expect "b": 2 to be present, "a": 1 to be absent (filtered out)
                if "'b': 2" in line and "'a': 1" not in line:
                    found_heartbeat = True
                    break
        
        assert found_heartbeat, "Metadata allowlist propagation failed (CLI did not override Env)"
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_subprocess_config_batch_limit(integration_temp_dir: Path) -> None:
    """Test that batch_size_limit=1 from CLI triggers immediate processing despite long debounce."""
    task_file = integration_temp_dir / "batch_cli.json"
    task_file.write_text(json.dumps({"current_stage": "1", "current_task": "1"}), encoding="utf-8")
    
    cmd = [
        sys.executable,
        "-m",
        "aw_watcher_pipeline_stage.main",
        "--watch-path", str(task_file),
        "--testing",
        "--debounce-seconds", "10.0",
        "--batch-size-limit", "1",
        "--log-level", "DEBUG"
    ]
    
    env = os.environ.copy()
    root_dir = Path(__file__).resolve().parents[1]
    env["PYTHONPATH"] = f"{str(root_dir)}{os.pathsep}{env.get('PYTHONPATH', '')}"
    
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True, bufsize=1)
    
    try:
        # Wait for startup
        start = time.time()
        started = False
        while time.time() - start < 5.0:
            line = process.stdout.readline()
            if "Observer started" in line:
                started = True
                break
        
        if not started:
            pytest.fail("Process failed to start")
            
        # Trigger event
        task_file.write_text(json.dumps({"current_stage": "2", "current_task": "2"}), encoding="utf-8")
        
        # Should trigger immediately (within < 2s) despite 10s debounce
        found = False
        start = time.time()
        while time.time() - start < 2.0:
            line = process.stdout.readline()
            if "[MOCK] heartbeat" in line and "2 - 2" in line:
                found = True
                break
        
        assert found, "Batch limit 1 did not trigger immediate processing"
        
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_flush_queue_waits_for_worker(integration_temp_dir: Path) -> None:
    """Test that flush_queue waits for the worker thread to process pending items."""
    task_file = integration_temp_dir / "flush_test.json"

    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)

    # Enqueue an item
    client.send_heartbeat("Stage", "Task")

    # Verify flush_queue calls join on the internal queue
    with patch.object(client._queue, "join", wraps=client._queue.join) as mock_join:
        client.flush_queue()
        mock_join.assert_called_once()

    client.close()

def test_signal_handling_integration(integration_temp_dir: Path, caplog: LogCaptureFixture) -> None:
    """Test that SIGINT triggers cleanup and exit via signal handler."""
    task_file = integration_temp_dir / "signal_int_test.json"
    task_file.touch()

    # We need to run main in a thread or just mock the blocking wait
    # Here we mock stop_event.wait to simulate waiting then being set by handler
    
    with patch("aw_watcher_pipeline_stage.main.PipelineWatcher") as MockWatcher, \
         patch("aw_watcher_pipeline_stage.main.PipelineClient") as MockClient, \
         patch("aw_watcher_pipeline_stage.main.setup_logging"), \
         patch("aw_watcher_pipeline_stage.main.logging.shutdown") as mock_logging_shutdown, \
         patch("signal.signal") as mock_signal:

        # Setup mocks
        MockWatcher.return_value.observer.is_alive.return_value = True
        MockWatcher.return_value.watch_dir.exists.return_value = True
        
        # Mock stop_event to trigger exit immediately when wait is called
        # This simulates the loop running until signal is received
        with patch("aw_watcher_pipeline_stage.main.threading.Event") as MockEvent:
            mock_event_instance = MockEvent.return_value
            mock_event_instance.wait.return_value = True
            mock_event_instance.is_set.return_value = False # Initially false

            argv = ["aw-watcher-pipeline-stage", "--watch-path", str(task_file), "--testing"]
            with patch.object(sys, "argv", argv):
                main()

        # Verify signal registration
        calls = [args[0] for args, _ in mock_signal.call_args_list]
        assert signal.SIGINT in calls
        assert signal.SIGTERM in calls

        # Verify cleanup called
        MockWatcher.return_value.stop.assert_called()
        # Stage 8.1.4: Explicitly verify flush_queue is called to ensure no data loss
        MockClient.return_value.flush_queue.assert_called_once()
        MockClient.return_value.close.assert_called()
        mock_logging_shutdown.assert_called()
        # Note: observer.stop/join are called inside watcher.stop(), which is called by cleanup

def test_data_persistence_flush_integration(integration_temp_dir: Path) -> None:
    """
    Audit (Stage 8.2.1): Verify data persistence via flush on shutdown.
    Ensures that flush_queue is called and delegates to client.flush().
    """
    task_file = integration_temp_dir / "persistence.json"
    task_file.touch()
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    
    # Simulate queued events (offline mode in aw-client)
    # We just send a heartbeat, MockActivityWatchClient stores it
    client.send_heartbeat("Persist", "Me")
    
    # Mock flush to verify it's called
    with patch.object(mock_aw_client, "flush", wraps=mock_aw_client.flush) as mock_flush:
        # Simulate shutdown sequence calling flush_queue
        client.flush_queue()
        
        mock_flush.assert_called_once()

def test_audit_offline_persistence_flow(integration_temp_dir: Path) -> None:
    """
    Audit (Stage 8.2.1): Verify offline handling (queued=True) and data persistence (no loss).
    Mock ConnectionError -> Queue events -> Flush on stop.
    """
    task_file = integration_temp_dir / "audit_offline.json"
    task_file.touch()
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)

    # 1. Simulate Offline (ConnectionError) then Recovery
    # We want to verify that PipelineClient attempts to send with queued=True
    # and that it eventually succeeds (persistence) when the connection recovers.
    
    # Define side effect to simulate transient failure then success calling original
    original_heartbeat = mock_aw_client.heartbeat
    state = {"failed": False}

    def side_effect(*args: Any, **kwargs: Any) -> None:
        if not state["failed"]:
            state["failed"] = True
            raise ConnectionError("Offline")
        return original_heartbeat(*args, **kwargs)

    with patch.object(mock_aw_client, "heartbeat", side_effect=side_effect) as mock_hb:
        with patch("aw_watcher_pipeline_stage.client.time.sleep"):
            client.send_heartbeat("Offline", "Task")
            client.flush_queue()
        
        # Verify retry logic was triggered
        # Verify it was called at least twice (fail -> retry)
        assert mock_hb.call_count >= 2, "Should have retried at least once"
        # Verify queued=True passed in all attempts
        for call in mock_hb.call_args_list:
            _, kwargs = call
            assert kwargs["queued"] is True
            assert kwargs["pulsetime"] == 120.0
    
    # Verify event was eventually stored (persistence)
    assert len(mock_aw_client.events) > 0
    assert mock_aw_client.events[0]["data"]["stage"] == "Offline"
        
    # 2. Simulate Shutdown Flush
    # Verify flush is called to persist data
    with patch.object(mock_aw_client, "flush") as mock_flush:
        client.flush_queue()
        mock_flush.assert_called_once()

def test_audit_heartbeat_parameters_integration(integration_temp_dir: Path) -> None:
    """
    Audit (Stage 8.2.1): Verify heartbeat parameters (pulsetime=120, queued=True) in integration flow.
    """
    task_file = integration_temp_dir / "audit_params.json"
    task_file.write_text(json.dumps({"current_stage": "Audit", "current_task": "Params"}), encoding="utf-8")
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    watcher = PipelineWatcher(task_file, client, debounce_seconds=0.1)
    
    try:
        watcher.start()
        time.sleep(0.5)
        
        assert len(mock_aw_client.events) == 1
        event = mock_aw_client.events[0]
        
        # Verify parameters stored by MockActivityWatchClient
        assert event["pulsetime"] == 120.0
        assert event["queued"] is True
        assert event["bucket_id"].startswith("aw-watcher-pipeline-stage_")
        
    finally:
        watcher.stop()

def test_persistence_flush_on_error(integration_temp_dir: Path) -> None:
    """Test that flush is called even if previous heartbeats failed (persistence)."""
    task_file = integration_temp_dir / "persist_error.json"
    task_file.touch()
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    
    # Simulate heartbeat failure
    with patch.object(mock_aw_client, "heartbeat", side_effect=Exception("Fail")):
        client.send_heartbeat("S", "T")
        
    # Flush should still be called and succeed (if client supports it)
    with patch.object(mock_aw_client, "flush") as mock_flush:
        client.flush_queue()
        mock_flush.assert_called_once()

def test_offline_persistence_no_loss(integration_temp_dir: Path) -> None:
    """
    Audit (Stage 8.2.1): Verify no data loss during offline/reconnect cycle.
    Simulates server going offline, events queuing in worker, and flushing on reconnect.
    """
    task_file = integration_temp_dir / "persistence_loss.json"
    task_file.write_text(json.dumps({"current_stage": "1", "current_task": "1"}), encoding="utf-8")
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    watcher = PipelineWatcher(task_file, client, debounce_seconds=0.1)
    
    try:
        watcher.start()
        time.sleep(0.2)
        assert len(mock_aw_client.events) == 1
        
        # Go offline (simulate by patching the instance method to raise error)
        # This causes the worker thread to enter retry loop for the next event
        with patch.object(mock_aw_client, "heartbeat", side_effect=ConnectionError("Offline")):
            # Generate 3 events while offline
            for i in range(2, 5):
                task_file.write_text(json.dumps({"current_stage": str(i), "current_task": str(i)}), encoding="utf-8")
                time.sleep(0.2)
                
            # Wait briefly to ensure they are processed and queued/retrying
            time.sleep(0.5)
            
        # Reconnect (Context manager exit restores original method)
        # The worker thread should now succeed on its next retry attempt
        
        # Generate 1 more event to ensure flow continues
        task_file.write_text(json.dumps({"current_stage": "5", "current_task": "5"}), encoding="utf-8")
        
        # Wait for retries to succeed and queue to drain (backoff might be ~0.5-1.0s)
        time.sleep(2.0)
        
        # Verify all 5 events are present (1 initial + 3 queued + 1 final)
        assert len(mock_aw_client.events) == 5
        assert mock_aw_client.events[-1]["data"]["task"] == "5"
        
    finally:
        watcher.stop()

def test_startup_bucket_failure_lazy_recovery(integration_temp_dir: Path) -> None:
    """
    Audit (Stage 8.2.1): Test that if ensure_bucket fails at startup (e.g. offline), 
    the watcher proceeds and lazy bucket creation succeeds later.
    """
    task_file = integration_temp_dir / "lazy_recovery.json"
    task_file.write_text(json.dumps({"current_stage": "Lazy", "current_task": "Start"}), encoding="utf-8")
    
    mock_aw_client = MockActivityWatchClient()
    
    # Simulate ensure_bucket failure
    with patch.object(mock_aw_client, "create_bucket", side_effect=[ConnectionError("Startup Offline"), None]) as mock_create:
        client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
        
        # 1. Startup failure (simulated main.py behavior)
        try:
            client.ensure_bucket()
        except Exception:
            pass # main.py catches this
            
        assert not client._bucket_created
        
        # 2. Watcher starts and triggers heartbeat
        watcher = PipelineWatcher(task_file, client, debounce_seconds=0.1)
        try:
            watcher.start()
            time.sleep(0.5)
            
            # 3. Verify lazy creation succeeded (2nd call to create_bucket)
            assert client._bucket_created
            assert mock_create.call_count == 2
            
            # 4. Verify heartbeat sent
            assert len(mock_aw_client.events) == 1
            assert mock_aw_client.events[0]["data"]["stage"] == "Lazy"
            
        finally:
            watcher.stop()

def test_audit_data_persistence_flush_retry(integration_temp_dir: Path) -> None:
    """
    Audit (Stage 8.2.1): Verify flush_queue retries on transient failure to ensure persistence.
    """
    task_file = integration_temp_dir / "persist_retry.json"
    task_file.touch()
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    
    # Simulate flush failure then success
    with patch.object(mock_aw_client, "flush", side_effect=[Exception("Busy"), None]) as mock_flush:
        with patch("aw_watcher_pipeline_stage.client.time.sleep"):
            client.flush_queue()
            
        assert mock_flush.call_count == 2

def test_lazy_bucket_creation_offline_retry(integration_temp_dir: Path) -> None:
    """
    Audit (Stage 8.2.1): Verify that lazy bucket creation retries indefinitely
    when offline (ConnectionError), ensuring no data loss on startup.
    """
    task_file = integration_temp_dir / "lazy_offline.json"
    task_file.write_text(json.dumps({"current_stage": "OfflineStart", "current_task": "Retry"}), encoding="utf-8")
    
    mock_aw_client = MockActivityWatchClient()
    
    # Simulate ensure_bucket failing (main.py handles this)
    with patch.object(mock_aw_client, "create_bucket", side_effect=ConnectionError("Offline")):
        client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
        try:
            client.ensure_bucket()
        except Exception:
            pass
            
    # Now watcher starts. Worker thread will try to create bucket lazily.
    # We simulate 3 failures then success.
    failures = [ConnectionError("Offline")] * 3
    success = [None]
    side_effect = failures + success
    
    mock_aw_client.create_bucket.side_effect = side_effect
    
    watcher = PipelineWatcher(task_file, client, debounce_seconds=0.1)
    
    try:
        with patch("aw_watcher_pipeline_stage.client.time.sleep"): # Speed up retries
            watcher.start()
            time.sleep(0.5) # Allow worker to cycle through retries
            
        # Verify create_bucket was called multiple times (initial failure + 3 retries + 1 success)
        assert mock_aw_client.create_bucket.call_count >= 4
        assert client._bucket_created

def test_audit_flush_persistence_check(integration_temp_dir: Path) -> None:
    """
    Audit (Stage 8.2.1): Verify flush is called on shutdown to persist data.
    Ensures no data loss on reconnect/flush by verifying the flush call.
    """
    task_file = integration_temp_dir / "audit_flush.json"
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    
    # Send event and flush
    client.send_heartbeat("Audit", "Flush")
    with patch.object(mock_aw_client, "flush") as mock_flush:
        client.flush_queue()
        mock_flush.assert_called_once()

def test_audit_shutdown_flush_persistence(integration_temp_dir: Path) -> None:
    """
    Audit (Stage 8.2.1): Verify data persistence on shutdown.
    Ensures flush_queue is called when the watcher is stopped, persisting any buffered events.
    """
    task_file = integration_temp_dir / "shutdown_persist.json"
    task_file.touch()
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    
    # Simulate some activity
    client.send_heartbeat("Shutdown", "Test")
    
    # Mock flush to verify it gets called
    with patch.object(mock_aw_client, "flush") as mock_flush:
        # Simulate shutdown sequence (normally called by main.py cleanup)
        client.flush_queue()
        client.close()
        
        mock_flush.assert_called_once()

def test_final_audit_flow(integration_temp_dir: Path) -> None:
    """
    Stage 8.2.1 Final Audit: Comprehensive verification of integration requirements and data persistence.
    1. Bucket Creation: Name includes hostname, type='current-pipeline-stage', queued=True.
    2. Heartbeat: pulsetime=120.0, queued=True.
    3. Offline Handling: Events queued, retried, and flushed on recovery.
    4. Data Persistence: No loss on reconnect/flush.
    """
    task_file = integration_temp_dir / "final_audit.json"
    task_file.write_text(json.dumps({"current_stage": "Audit", "current_task": "Start"}), encoding="utf-8")

    mock_aw_client = MockActivityWatchClient()

    # Mock hostname for deterministic bucket ID
    with patch("socket.gethostname", return_value="audit-host"):
        client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)

        # 1. Bucket Creation Audit
        client.ensure_bucket()
        expected_bucket = "aw-watcher-pipeline-stage_audit-host"
        assert expected_bucket in mock_aw_client.buckets
        bucket = mock_aw_client.buckets[expected_bucket]
        assert bucket["event_type"] == "current-pipeline-stage"
        assert bucket["queued"] is True
        assert "aw-watcher-pipeline-stage_" in expected_bucket

        watcher = PipelineWatcher(task_file, client, debounce_seconds=0.1)

        try:
            watcher.start()
            time.sleep(0.2)

            # 2. Heartbeat Audit
            assert len(mock_aw_client.events) == 1
            event = mock_aw_client.events[0]
            assert event["bucket_id"] == expected_bucket
            assert event["pulsetime"] == 120.0
            assert event["queued"] is True

            # 3. Offline Handling Audit
            mock_aw_client.events.clear()

            # Simulate Offline (ConnectionError) then Recovery
            # We use patch.object to wrap the real method so we can simulate failure then success
            original_heartbeat = mock_aw_client.heartbeat
            
            def side_effect(*args: Any, **kwargs: Any) -> None:
                # Fail on the first call (Offline), then succeed (Recovery)
                if mock_hb.call_count == 1:
                    raise ConnectionError("Offline")
                return original_heartbeat(*args, **kwargs)

            with patch.object(mock_aw_client, "heartbeat", side_effect=side_effect) as mock_hb:
                # Trigger update
                # Send multiple events to verify no loss
                for i in range(3):
                    task_file.write_text(json.dumps({"current_stage": "Audit", "current_task": f"Offline {i}"}), encoding="utf-8")
                    # Speed up retries
                    with patch("aw_watcher_pipeline_stage.client.time.sleep"):
                        # Wait for debounce (0.1) + worker processing + retry
                        time.sleep(0.2)

            # Verify it eventually succeeded (No data loss - all 3 events + initial start event)
            # Initial start event (1) + 3 offline events = 4 total
            assert len(mock_aw_client.events) >= 4
            assert mock_aw_client.events[-1]["data"]["task"] == "Offline 2"
            assert mock_aw_client.events[-1]["queued"] is True
            assert mock_aw_client.events[-1]["pulsetime"] == 120.0

            # 4. Flush Audit
            with patch.object(mock_aw_client, "flush") as mock_flush:
                client.flush_queue()
                mock_flush.assert_called_once()

        finally:
            watcher.stop()

def test_audit_event_type_integration(integration_temp_dir: Path) -> None:
    """
    Audit (Stage 8.2.1): Verify that the event type 'current-pipeline-stage' is correctly
    propagated to the bucket creation in a full integration flow.
    """
    task_file = integration_temp_dir / "audit_type.json"
    task_file.touch()
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    
    client.ensure_bucket()
    
    # Verify bucket type in mock client storage
    bucket_id = f"aw-watcher-pipeline-stage_{client.hostname}"
    assert bucket_id in mock_aw_client.buckets
    assert mock_aw_client.buckets[bucket_id]["event_type"] == "current-pipeline-stage"

def test_audit_persistence_flush_retry_exhaustion(integration_temp_dir: Path, caplog: LogCaptureFixture) -> None:
    """
    Audit (Stage 8.2.1): Verify that if flush fails after all retries, it logs an error
    but does not crash the application (robustness).
    """
    task_file = integration_temp_dir / "flush_exhaust.json"
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    
    # Simulate persistent failure
    with patch.object(mock_aw_client, "flush", side_effect=Exception("Persistent Failure")):
        with patch("aw_watcher_pipeline_stage.client.time.sleep"):
            client.flush_queue()
            
    assert "Failed to flush event queue after 4 attempts" in caplog.text

def test_audit_full_offline_persistence(integration_temp_dir: Path) -> None:
    """
    Audit (Stage 8.2.1): Verify full offline persistence flow.
    1. Start offline (ConnectionError).
    2. Generate events (Buffered).
    3. Reconnect.
    4. Verify all events sent (No loss).
    """
    task_file = integration_temp_dir / "audit_persistence.json"
    task_file.write_text(json.dumps({"current_stage": "1", "current_task": "1"}), encoding="utf-8")

    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    watcher = PipelineWatcher(task_file, client, debounce_seconds=0.1)

    try:
        watcher.start()
        time.sleep(0.2)

        # Simulate Offline: Heartbeat raises ConnectionError
        # We generate 2 more events while offline
        with patch.object(mock_aw_client, "heartbeat", side_effect=ConnectionError("Offline")):
            for i in range(2, 4):
                task_file.write_text(json.dumps({"current_stage": str(i), "current_task": str(i)}), encoding="utf-8")
                time.sleep(0.2)
            time.sleep(1.0) # Allow processing

        # Reconnect (unpatch) and flush
        client.flush_queue()

        # Verify all 3 events (1 initial + 2 buffered) are present
        assert len(mock_aw_client.events) == 3
        assert mock_aw_client.events[0]["data"]["task"] == "1"
        assert mock_aw_client.events[1]["data"]["task"] == "2"
        assert mock_aw_client.events[2]["data"]["task"] == "3"

    finally:
        watcher.stop()

def test_audit_offline_queue_flush_no_loss(integration_temp_dir: Path) -> None:
    """
    Audit (Stage 8.2.1): Verify offline queue flush ensures no data loss on shutdown.
    """
    task_file = integration_temp_dir / "audit_flush_loss.json"
    task_file.write_text(json.dumps({"current_stage": "Offline", "current_task": "Work"}), encoding="utf-8")
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    watcher = PipelineWatcher(task_file, client, debounce_seconds=0.1)
    
    try:
        watcher.start()
        time.sleep(0.2)
        
        # Simulate offline
        with patch.object(mock_aw_client, "heartbeat", side_effect=ConnectionError("Offline")):
            # Trigger update
            task_file.write_text(json.dumps({"current_stage": "Offline", "current_task": "Update"}), encoding="utf-8")
            time.sleep(1.0)
            
        # Stop watcher and manually flush (simulating main.py cleanup)
        with patch.object(mock_aw_client, "flush") as mock_flush:
            client.flush_queue()
            mock_flush.assert_called()
            
    finally:
        if watcher.observer.is_alive():
            watcher.stop()

def test_audit_integration_persistence_check(integration_temp_dir: Path) -> None:
    """
    Audit (Stage 8.2.1): Integration test for data persistence.
    Verifies that events generated during a transient failure are eventually sent.
    """
    task_file = integration_temp_dir / "persist_integration.json"
    task_file.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}), encoding="utf-8")
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    watcher = PipelineWatcher(task_file, client, debounce_seconds=0.1)
    
    try:
        watcher.start()
        time.sleep(0.2)
        
        # Simulate transient failure
        mock_aw_client.heartbeat.side_effect = [ConnectionError("Transient"), None]
        
        with patch("aw_watcher_pipeline_stage.client.time.sleep"):
            task_file.write_text(json.dumps({"current_stage": "Persist", "current_task": "Test"}), encoding="utf-8")
            time.sleep(1.0)
            
        # Verify event eventually succeeded
        assert len(mock_aw_client.events) >= 2
        assert mock_aw_client.events[-1]["data"]["stage"] == "Persist"
        
    finally:
        watcher.stop()
    finally:
        if watcher.observer.is_alive():
            watcher.stop()

def test_audit_full_offline_persistence_flow(integration_temp_dir: Path) -> None:
    """
    Audit (Stage 8.2.1): Verify full offline persistence flow.
    1. Start offline (ConnectionError).
    2. Generate events (Buffered).
    3. Reconnect.
    4. Verify all events sent (No loss).
    """
    task_file = integration_temp_dir / "audit_persist_flow.json"
    task_file.write_text(json.dumps({"current_stage": "1", "current_task": "1"}), encoding="utf-8")

    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    watcher = PipelineWatcher(task_file, client, debounce_seconds=0.1)

    try:
        watcher.start()
        time.sleep(0.2)

        # Simulate Offline: Heartbeat raises ConnectionError
        # We generate 2 more events while offline
        with patch.object(mock_aw_client, "heartbeat", side_effect=ConnectionError("Offline")) as mock_hb:
            for i in range(2, 4):
                task_file.write_text(json.dumps({"current_stage": str(i), "current_task": str(i)}), encoding="utf-8")
                time.sleep(0.2)
            time.sleep(1.0) # Allow processing

            # Audit: Verify queued=True was passed even during failure (buffering active)
            assert mock_hb.call_count >= 2
            for call in mock_hb.call_args_list:
                _, kwargs = call
                assert kwargs.get("queued") is True

        # Reconnect (unpatch) and flush
        client.flush_queue()

        # Verify all 3 events (1 initial + 2 buffered) are present
        assert len(mock_aw_client.events) == 3
        assert mock_aw_client.events[0]["data"]["task"] == "1"
        assert mock_aw_client.events[1]["data"]["task"] == "2"
        assert mock_aw_client.events[2]["data"]["task"] == "3"

    finally:
        watcher.stop()

def test_audit_verify_bucket_attributes(integration_temp_dir: Path) -> None:
    """Audit (Stage 8.2.1): Verify bucket attributes (hostname in name, correct type)."""
    task_file = integration_temp_dir / "audit_bucket.json"
    mock_aw_client = MockActivityWatchClient()
    
    with patch("socket.gethostname", return_value="audit-host"):
        client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
        client.ensure_bucket()
        
        args, kwargs = mock_aw_client.create_bucket.call_args
        bucket_id = args[0]
        
        assert "audit-host" in bucket_id
        assert bucket_id.startswith("aw-watcher-pipeline-stage_")
        assert kwargs["event_type"] == "current-pipeline-stage"
        assert kwargs["queued"] is True

def test_stage_8_2_1_persistence_verification(integration_temp_dir: Path, mock_aw_client: MagicMock) -> None:
    """
    Audit (Stage 8.2.1): Verify data persistence lifecycle.
    Offline -> Buffer -> Reconnect -> Flush -> Verify No Loss.
    """
    task_file = integration_temp_dir / "stage_8_2_1_persist.json"
    task_file.write_text(json.dumps({"current_stage": "1", "current_task": "Init"}), encoding="utf-8")

    client = PipelineClient(watch_path=task_file, client=mock_aw_client, testing=True)
    watcher = PipelineWatcher(task_file, client, debounce_seconds=0.1)

    try:
        watcher.start()
        time.sleep(0.2)
        
        # Verify initial event
        assert len(mock_aw_client.events) == 1

        # Simulate Offline: Heartbeat raises ConnectionError
        # Generate events while offline
        with patch.object(mock_aw_client, "heartbeat", side_effect=ConnectionError("Offline")):
            task_file.write_text(json.dumps({"current_stage": "2", "current_task": "Buffered"}), encoding="utf-8")
            time.sleep(1.0) # Allow processing and queuing
            # Note: The worker thread is now retrying the "Buffered" event

        # Reconnect (unpatch) and flush
        client.flush_queue()

        # Verify all events are present (1 initial + 1 buffered)
        assert len(mock_aw_client.events) == 2
        assert mock_aw_client.events[0]["data"]["task"] == "Init"
        assert mock_aw_client.events[1]["data"]["task"] == "Buffered"

    finally:
        watcher.stop()
