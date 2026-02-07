"""Integration tests for aw-watcher-pipeline-stage."""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from typing import Any, Generator
from unittest.mock import patch

import pytest

from aw_watcher_pipeline_stage.client import MockActivityWatchClient, PipelineClient
from aw_watcher_pipeline_stage.watcher import PipelineWatcher


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

    Directive 2: Verify integration test skeleton with temporary JSON simulation.
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
        pulsetime=120.0,
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
        pulsetime=120.0,
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
            watcher.check_periodic_heartbeat()
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
            watcher.check_periodic_heartbeat()
            assert len(mock_aw_client.events) == 0, "Should not send periodic heartbeat when completed"

    finally:
        watcher.stop()


def test_bucket_creation_and_offline_queuing(populated_task_file: Path) -> None:
    """Test bucket creation (mocked name with hostname) and offline queuing logic.

    Directive 5: Test bucket creation, offline mode with queued=True events flushed on recovery.
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
        pulsetime=120.0,
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
            watcher.check_periodic_heartbeat()

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
            watcher.check_periodic_heartbeat()
            assert len(mock_aw_client.events) == 0, "Should not send heartbeat before interval"

        # --- Simulate another 30s passing (Period 2) ---
        # Time = simulated_now + 31s
        simulated_now_3 = simulated_now + 31.0
        mock_aw_client.events.clear()

        with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", return_value=simulated_now_3):
            watcher.check_periodic_heartbeat()

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

    Directive 3: Verify exactly one immediate heartbeat is sent within 1s,
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
        pulsetime=120.0,
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
        time.sleep(debounce_interval + 0.3)

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

    finally:
        watcher.stop()
