"""Tests for the PipelineClient."""

from __future__ import annotations

import logging
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest
from aw_core.models import Event

from aw_watcher_pipeline_stage.client import MockActivityWatchClient, PipelineClient

if TYPE_CHECKING:
    from pytest import LogCaptureFixture


@pytest.fixture
def mock_aw_client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def pipeline_client(tmp_path: Path, mock_aw_client: MagicMock) -> PipelineClient:
    with patch("socket.gethostname", return_value="test-host"):
        return PipelineClient(watch_path=tmp_path / "test.json", client=mock_aw_client, testing=True)

def test_hostname_failure(tmp_path: Path, mock_aw_client: MagicMock, caplog: LogCaptureFixture) -> None:
    """Test that client handles hostname retrieval failure."""
    with patch("socket.gethostname", side_effect=Exception("DNS Error")):
        client = PipelineClient(watch_path=tmp_path / "test.json", client=mock_aw_client, testing=True)
        assert client.hostname == "unknown-host"
        assert "aw-watcher-pipeline-stage_unknown-host" in client.bucket_id
        assert "Failed to get hostname" in caplog.text

def test_hostname_empty(tmp_path: Path, mock_aw_client: MagicMock, caplog: LogCaptureFixture) -> None:
    """Test that client handles empty hostname string."""
    with patch("socket.gethostname", return_value=""):
        client = PipelineClient(watch_path=tmp_path / "test.json", client=mock_aw_client, testing=True)
        assert client.hostname == "unknown-host"
        assert "aw-watcher-pipeline-stage_unknown-host" in client.bucket_id
        assert "Failed to get hostname" in caplog.text

def test_hostname_sanitization(tmp_path: Path, mock_aw_client: MagicMock) -> None:
    """Test that hostname is sanitized."""
    with patch("socket.gethostname", return_value="bad/host:name"):
        client = PipelineClient(watch_path=tmp_path / "test.json", client=mock_aw_client, testing=True)
        assert client.hostname == "bad_host_name"
        assert "aw-watcher-pipeline-stage_bad_host_name" in client.bucket_id

def test_hostname_sanitization_spaces(tmp_path: Path, mock_aw_client: MagicMock) -> None:
    """Test that hostname with spaces is sanitized."""
    with patch("socket.gethostname", return_value="My Host Name"):
        client = PipelineClient(watch_path=tmp_path / "test.json", client=mock_aw_client, testing=True)
        assert client.hostname == "My_Host_Name"
        assert "aw-watcher-pipeline-stage_My_Host_Name" in client.bucket_id

def test_hostname_sanitization_all_special_chars(tmp_path: Path, mock_aw_client: MagicMock) -> None:
    """Test that hostname sanitization falls back to unknown-host if all chars are sanitized."""
    with patch("socket.gethostname", return_value="!@#$%^"):
        client = PipelineClient(watch_path=tmp_path / "test.json", client=mock_aw_client, testing=True)
        assert client.hostname == "unknown-host"
        assert "aw-watcher-pipeline-stage_unknown-host" in client.bucket_id

def test_ensure_bucket(pipeline_client: PipelineClient, mock_aw_client: MagicMock) -> None:
    pipeline_client.ensure_bucket()
    mock_aw_client.create_bucket.assert_called_once()
    args = mock_aw_client.create_bucket.call_args
    assert args[0][0] == "aw-watcher-pipeline-stage_test-host"
    assert args[1]["event_type"] == "current-pipeline-stage"
    assert args[1]["queued"] is True


def test_send_heartbeat_payload(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    pipeline_client.ensure_bucket()
    pipeline_client.send_heartbeat(
        stage="Build",
        task="Compiling",
        project_id="proj1",
        metadata={"priority": "high", "nested": {"a": 1}},
        computed_duration=10.5,
    )

    mock_aw_client.heartbeat.assert_called_once()
    call_args = mock_aw_client.heartbeat.call_args
    event = call_args[0][1]

    assert isinstance(event, Event)
    assert event.data["stage"] == "Build"
    assert event.data["task"] == "Compiling"
    assert event.data["project_id"] == "proj1"
    assert event.data["priority"] == "high"  # Flattened
    assert event.data["nested"] == {"a": 1}  # Flattened
    assert event.data["computed_duration"] == 10.5

def test_send_heartbeat_with_start_time(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test that start_time is correctly parsed and used as event timestamp."""
    pipeline_client.ensure_bucket()
    # Use a fixed time string
    start_time_str = "2023-10-27T10:00:00Z"
    
    pipeline_client.send_heartbeat("Stage", "Task", start_time=start_time_str)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]
    
    # Verify timestamp matches (converted to datetime)
    # Timestamp should be start_time
    assert event.timestamp.isoformat() == "2023-10-27T10:00:00+00:00"
    assert event.data["start_time"] == start_time_str

def test_send_heartbeat_no_duration(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    pipeline_client.ensure_bucket()
    pipeline_client.send_heartbeat("Stage", "Task")

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]
    assert "computed_duration" not in event.data


def test_send_heartbeat_retry_logic(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    # Mock heartbeat to fail twice then succeed
    mock_aw_client.heartbeat.side_effect = [Exception("Fail 1"), Exception("Fail 2"), None]

    with patch("time.sleep") as mock_sleep:
        pipeline_client.send_heartbeat("Stage", "Task")
        assert mock_sleep.call_count == 2

    assert mock_aw_client.heartbeat.call_count == 3
    # Verify generic errors do NOT trigger "Server unavailable" log
    assert "Server unavailable" not in caplog.text
    assert "Heartbeat failed (Fail 1)" in caplog.text


def test_send_heartbeat_max_retries_failure(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    # Mock heartbeat to fail always
    mock_aw_client.heartbeat.side_effect = Exception("Fail Always")

    with patch("time.sleep"):
        with pytest.raises(Exception):
            pipeline_client.send_heartbeat("Stage", "Task")

    assert mock_aw_client.heartbeat.call_count == 4  # Max retries


def test_offline_failure_max_retries(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test offline failure after max retries (Directive 7)."""
    # Simulate ConnectionError (offline scenario) always
    mock_aw_client.heartbeat.side_effect = ConnectionError("Connection refused")

    with patch("time.sleep"):
        with pytest.raises(ConnectionError):
            pipeline_client.send_heartbeat("Stage", "Task")

    assert mock_aw_client.heartbeat.call_count == 4  # Initial + 3 retries
    assert "Server unavailable (buffering enabled)" in caplog.text
    assert "Heartbeat failed after 3 retries" in caplog.text


def test_send_heartbeat_connection_refused(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test specific handling of ConnectionRefusedError (Directive 7)."""
    mock_aw_client.heartbeat.side_effect = ConnectionRefusedError("Connection refused")

    with patch("time.sleep"):
        with pytest.raises(ConnectionRefusedError):
            pipeline_client.send_heartbeat("Stage", "Task")

    assert mock_aw_client.heartbeat.call_count == 4
    assert "Server unavailable (buffering enabled)" in caplog.text

def test_send_heartbeat_socket_error(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test handling of socket.error (Directive 7)."""
    mock_aw_client.heartbeat.side_effect = socket.error("Socket error")

    with patch("time.sleep"):
        with pytest.raises(socket.error):
            pipeline_client.send_heartbeat("Stage", "Task")

    assert mock_aw_client.heartbeat.call_count == 4
    assert "Server unavailable (buffering enabled)" in caplog.text

def test_send_heartbeat_offline_buffering(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """
    Test offline buffering behavior (Directive 7).
    
    Verify that send_heartbeat passes queued=True, allowing aw-client to buffer events
    without raising exceptions when offline (assuming aw-client handles queuing).
    """
    # Simulate aw-client accepting the event (queuing it) without error
    mock_aw_client.heartbeat.return_value = None

    pipeline_client.send_heartbeat("Stage", "Task")

    mock_aw_client.heartbeat.assert_called_once()
    _, kwargs = mock_aw_client.heartbeat.call_args
    assert kwargs.get("queued") is True


def test_send_heartbeat_metadata_conflict(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    pipeline_client.ensure_bucket()
    # "stage" is a core field. Metadata "stage" should be ignored/warned.
    with caplog.at_level(logging.WARNING):
        pipeline_client.send_heartbeat(
            stage="Core",
            task="Task",
            metadata={"stage": "Malicious", "safe": "value"}
        )

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]
    
    assert event.data["stage"] == "Core"
    assert event.data["safe"] == "value"
    assert "Metadata key 'stage' conflicts" in caplog.text


def test_wait_for_start_success(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    mock_aw_client.get_info.return_value = {"version": "test"}
    pipeline_client.wait_for_start()
    mock_aw_client.get_info.assert_called()


def test_wait_for_start_retry(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    # Fail once then succeed
    mock_aw_client.get_info.side_effect = [Exception("Not ready"), {"version": "test"}]

    with patch("time.sleep") as mock_sleep:
        pipeline_client.wait_for_start()

    assert mock_aw_client.get_info.call_count == 2
    mock_sleep.assert_called_once()

def test_wait_for_start_timeout(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that wait_for_start respects the timeout and proceeds offline."""
    # Always fail connection
    mock_aw_client.get_info.side_effect = Exception("Connection refused")

    with patch("time.sleep"):
        # Mock time.monotonic to simulate timeout expiration
        # Sequence: start_time, check 1 (0s), check 2 (timeout exceeded)
        with patch("time.monotonic", side_effect=[0.0, 0.0, 10.0]):
            pipeline_client.wait_for_start(timeout=5.0)

    assert "Proceeding in offline mode" in caplog.text

def test_close(pipeline_client: PipelineClient, mock_aw_client: MagicMock) -> None:
    pipeline_client.close()
    mock_aw_client.disconnect.assert_called_once()


def test_init_real_client(temp_dir: Path) -> None:
    with patch("aw_watcher_pipeline_stage.client.ActivityWatchClient") as mock_aw_cls:
        client = PipelineClient(watch_path=temp_dir / "test.json", port=1234, testing=False)
        mock_aw_cls.assert_called_once_with(
            "aw-watcher-pipeline-stage", port=1234, testing=False
        )
        assert client.bucket_id.startswith("aw-watcher-pipeline-stage_")


def test_init_testing_client(temp_dir: Path) -> None:
    client = PipelineClient(watch_path=temp_dir / "test.json", testing=True)
    assert isinstance(client.client, MockActivityWatchClient)


def test_offline_recovery_and_buffering(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """
    Test offline recovery and buffering (Directive 7):
    - Server unavailable (ConnectionError) triggers retries
    - queued=True behavior (passed to client for buffering)
    - Reconnection recovery (retries succeed)
    - Automatic queue flush on recovery (implicit via queued=True usage)
    """
    # Simulate server unavailable (ConnectionError) for first 2 attempts, then success
    mock_aw_client.heartbeat.side_effect = [
        ConnectionError("Server unavailable"),
        ConnectionError("Server still unavailable"),
        None
    ]

    with patch("time.sleep") as mock_sleep:
        pipeline_client.send_heartbeat("Stage", "Task")
        
        # Verify retries occurred
        assert mock_sleep.call_count == 2
        assert mock_aw_client.heartbeat.call_count == 3
        
        # Verify queued=True was passed in all attempts (buffering intent)
        for call in mock_aw_client.heartbeat.call_args_list:
            _, kwargs = call
            assert kwargs.get("queued") is True

    assert "Server unavailable (buffering enabled)" in caplog.text
    assert "Connection recovered" in caplog.text


def test_send_heartbeat_normal_no_recovery_log(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that normal success does not log recovery message."""
    mock_aw_client.heartbeat.return_value = None

    with caplog.at_level(logging.INFO):
        pipeline_client.send_heartbeat("Stage", "Task")

    assert "Connection recovered" not in caplog.text


def test_ensure_bucket_offline_support(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """
    Test offline bucket creation (Directive 7).
    Test that ensure_bucket uses queued=True, allowing bucket creation
    to be buffered if the server is offline.
    """
    pipeline_client.ensure_bucket()
    mock_aw_client.create_bucket.assert_called_once()
    _, kwargs = mock_aw_client.create_bucket.call_args
    assert kwargs.get("queued") is True


def test_flush_queue_behavior(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test explicit queue flush behavior and logging."""
    # Success case
    with caplog.at_level(logging.INFO):
        pipeline_client.flush_queue()
    mock_aw_client.flush.assert_called_once()
    assert "Flushing event queue..." in caplog.text
    assert "Queue flushed successfully." in caplog.text

    # Error case
    mock_aw_client.flush.reset_mock()
    mock_aw_client.flush.side_effect = Exception("Flush failed")

    with pytest.raises(Exception, match="Flush failed"):
        pipeline_client.flush_queue()

    assert "Failed to flush event queue" in caplog.text


def test_flush_queue_connection_error(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test flush_queue handling of connection errors."""
    mock_aw_client.flush.side_effect = ConnectionError("Flush failed")

    with pytest.raises(ConnectionError):
        pipeline_client.flush_queue()

    assert "Failed to flush event queue" in caplog.text


def test_mock_client_buffering(temp_dir: Path) -> None:
    """
    Test that events are correctly buffered when using the mock client (simulating offline queue).
    """
    client = PipelineClient(watch_path=temp_dir / "test.json", testing=True)
    client.ensure_bucket()
    
    # Send multiple heartbeats
    client.send_heartbeat("Stage 1", "Task 1")
    client.send_heartbeat("Stage 2", "Task 2")
    
    assert isinstance(client.client, MockActivityWatchClient)
    assert len(client.client.events) == 2
    
    # Verify events are queued
    assert client.client.events[0]["queued"] is True
    assert client.client.events[0]["data"]["stage"] == "Stage 1"
    assert client.client.events[1]["queued"] is True
    assert client.client.events[1]["data"]["stage"] == "Stage 2"


def test_send_heartbeat_general_exception(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test handling of generic exceptions during heartbeat."""
    mock_aw_client.heartbeat.side_effect = Exception("Generic Error")

    with patch("time.sleep"):
        with pytest.raises(Exception, match="Generic Error"):
            pipeline_client.send_heartbeat("Stage", "Task")

    assert "Heartbeat failed" in caplog.text
    assert mock_aw_client.heartbeat.call_count == 4


def test_flush_queue_fallback(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test flush_queue when client has no flush method but has disconnect."""
    del mock_aw_client.flush

    with caplog.at_level(logging.INFO):
        pipeline_client.flush_queue()

    assert "Client has no flush method" in caplog.text


def test_flush_queue_no_method(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test flush_queue when client has neither flush nor disconnect."""
    del mock_aw_client.flush
    del mock_aw_client.disconnect

    with caplog.at_level(logging.WARNING):
        pipeline_client.flush_queue()

    assert "Client has no flush or disconnect method" in caplog.text


def test_missing_aw_client_dependency() -> None:
    """Test that missing aw-client raises ImportError in production mode."""
    with patch("aw_watcher_pipeline_stage.client.ActivityWatchClient", None):
        with pytest.raises(ImportError, match="aw-client is not installed"):
            PipelineClient(watch_path=Path("."), testing=False)


def test_send_heartbeat_invalid_start_time_type(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that non-string start_time is handled gracefully."""
    pipeline_client.ensure_bucket()

    # Pass an integer as start_time
    pipeline_client.send_heartbeat("Stage", "Task", start_time=12345)  # type: ignore

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    assert "start_time" not in event.data
    assert "Invalid start_time format" in caplog.text


def test_send_heartbeat_serialization_error(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that serialization errors (TypeError) are fatal and do not retry."""
    # Simulate a serialization error (e.g. non-serializable object in metadata)
    mock_aw_client.heartbeat.side_effect = TypeError("Object of type set is not JSON serializable")

    pipeline_client.send_heartbeat("Stage", "Task")

    # Should only call once (no retries)
    assert mock_aw_client.heartbeat.call_count == 1
    assert "Failed to serialize heartbeat event" in caplog.text

def test_wait_for_start_sleep_cap(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test that sleep is capped by remaining timeout."""
    mock_aw_client.get_info.side_effect = Exception("Conn err")
    
    with patch("time.sleep") as mock_sleep:
        # Sequence of monotonic times:
        # 1. start_time = 0.0
        # 2. First failure elapsed = 0.0. Timeout 2.0. Sleep min(1.0, 2.0) = 1.0.
        # 3. Second failure elapsed = 1.5. Timeout 2.0. Remaining 0.5. Retry delay 2.0. Sleep min(2.0, 0.5) = 0.5.
        # 4. Third failure elapsed = 3.0. Timeout exceeded. Break.
        with patch("time.monotonic", side_effect=[0.0, 0.0, 1.5, 3.0]): 
            pipeline_client.wait_for_start(timeout=2.0)
            
    assert mock_sleep.call_count == 2
    assert mock_sleep.call_args_list[0][0][0] == 1.0
    assert mock_sleep.call_args_list[1][0][0] == 0.5

def test_send_heartbeat_invalid_metadata_type(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that non-dict metadata is ignored and logged."""
    pipeline_client.ensure_bucket()
    
    # Pass a list instead of a dict
    pipeline_client.send_heartbeat("Stage", "Task", metadata=["invalid", "list"]) # type: ignore
    
    mock_aw_client.heartbeat.assert_called_once()
    # Verify warning
    assert "Metadata is not a dictionary" in caplog.text

def test_send_heartbeat_throttled_logging(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that connection error logs are throttled."""
    mock_aw_client.heartbeat.side_effect = ConnectionError("Fail")

    # First call: Should log warning (at least once)
    with patch("time.sleep"):
        with pytest.raises(ConnectionError):
            pipeline_client.send_heartbeat("Stage", "Task")

    assert "Server unavailable" in caplog.text
    # Count warnings
    warning_count = len([r for r in caplog.records if r.levelname == "WARNING" and "Server unavailable" in r.message])
    assert warning_count == 1

    caplog.clear()

    # Second call immediately: Should log debug only (throttled)
    with patch("time.sleep"):
        with pytest.raises(ConnectionError):
            pipeline_client.send_heartbeat("Stage", "Task")

def test_send_heartbeat_metadata_non_string_keys(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test that non-string metadata keys are converted to strings."""
    pipeline_client.ensure_bucket()
    
    pipeline_client.send_heartbeat("Stage", "Task", metadata={123: "value", "key": "val"}) # type: ignore
    
    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]
    assert event.data["123"] == "value"
    assert event.data["key"] == "val"


def test_send_heartbeat_oversized_metadata(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that metadata exceeding 1KB is truncated."""
    pipeline_client.ensure_bucket()

    # Create metadata that exceeds 1KB
    # 100 keys of length 20 (key+val+overhead) -> ~2000 bytes
    metadata = {f"key_{i}": "x" * 10 for i in range(100)}

    pipeline_client.send_heartbeat("Stage", "Task", metadata=metadata)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    # Check that not all keys are present (truncation happened)
    # Flattened metadata is mixed with core fields.
    # We expect significantly fewer than 100 keys due to 1KB limit.
    assert len(event.data) < 100
    assert "Metadata exceeds 1KB limit" in caplog.text


def test_send_heartbeat_long_strings_truncation(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that long strings are truncated."""
    pipeline_client.ensure_bucket()

    long_stage = "a" * 300
    long_task = "b" * 600
    long_project = "c" * 300
    long_path = "d" * 5000

    pipeline_client.send_heartbeat(
        stage=long_stage, task=long_task, project_id=long_project, file_path=long_path
    )

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    assert len(event.data["stage"]) == 256
    assert len(event.data["task"]) == 512
    assert len(event.data["project_id"]) == 256
    assert len(event.data["file_path"]) == 4096

    assert "Stage name too long" in caplog.text
    assert "Task name too long" in caplog.text


def test_offline_flush_sequence(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test sequence of offline buffering followed by explicit flush."""
    # 1. Send heartbeat (queued=True)
    pipeline_client.send_heartbeat("Stage", "Task")
    mock_aw_client.heartbeat.assert_called_once()
    assert mock_aw_client.heartbeat.call_args[1]["queued"] is True

    # 2. Explicit flush (simulating reconnect/shutdown)
    pipeline_client.flush_queue()
    mock_aw_client.flush.assert_called_once()

    warning_count = len([r for r in caplog.records if r.levelname == "WARNING" and "Server unavailable" in r.message])
    assert warning_count == 0


def test_file_path_anonymization(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test that file_path is anonymized if it starts with user home."""
    pipeline_client.ensure_bucket()

    # Mock Path.home() to return a specific path
    with patch("pathlib.Path.home", return_value=Path("/home/user")):
        # Case 1: Path inside home
        pipeline_client.send_heartbeat(
            "Stage", "Task", file_path="/home/user/project/file.json"
        )

        mock_aw_client.heartbeat.assert_called()
        args = mock_aw_client.heartbeat.call_args
        event = args[0][1]
        assert event.data["file_path"] == "~/project/file.json"

        # Case 2: Path outside home
        pipeline_client.send_heartbeat(
            "Stage", "Task", file_path="/opt/project/file.json"
        )
        args = mock_aw_client.heartbeat.call_args
        event = args[0][1]
        assert event.data["file_path"] == "/opt/project/file.json"


def test_computed_duration_sanity_checks(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test validation of computed_duration (negative or excessive)."""
    pipeline_client.ensure_bucket()

    # Case 1: Negative duration (clock skew)
    pipeline_client.send_heartbeat("Stage", "Task", computed_duration=-5.0)

    mock_aw_client.heartbeat.assert_called()
    args = mock_aw_client.heartbeat.call_args
    event = args[0][1]
    
    # Should be dropped
    assert "computed_duration" not in event.data
    assert "Computed duration is negative" in caplog.text

    caplog.clear()

    # Case 2: Excessive duration (> 24h)
    pipeline_client.send_heartbeat("Stage", "Task", computed_duration=90000.0)

    args = mock_aw_client.heartbeat.call_args
    event = args[0][1]
    # Should be kept but warned
    assert event.data["computed_duration"] == 90000.0
    assert "Computed duration is very large" in caplog.text


@pytest.mark.parametrize(
    "invalid_status",
    ["hacked", "unknown", "123", "", "  ", "PENDING"],
)
def test_send_heartbeat_invalid_status_parametrized(
    pipeline_client: PipelineClient,
    mock_aw_client: MagicMock,
    caplog: LogCaptureFixture,
    invalid_status: str,
) -> None:
    """Test that various invalid status values are ignored (Directive 5.2.2)."""
    pipeline_client.ensure_bucket()

    pipeline_client.send_heartbeat("Stage", "Task", status=invalid_status)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    # Status should be absent (None)
    assert "status" not in event.data
    assert f"Invalid status '{invalid_status}'" in caplog.text


def test_metadata_serialization_filtering(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that non-serializable metadata values are filtered out individually."""
    pipeline_client.ensure_bucket()

    # 'set' is not JSON serializable
    metadata = {
        "valid_key": "valid_value",
        "invalid_key": {1, 2, 3},
        "another_valid": 123,
    }

    pipeline_client.send_heartbeat("Stage", "Task", metadata=metadata)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    assert event.data["valid_key"] == "valid_value"
    assert event.data["another_valid"] == 123
    assert "invalid_key" not in event.data

    assert "Metadata value for key 'invalid_key' is not JSON serializable" in caplog.text


def test_send_heartbeat_invalid_start_time_format(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that invalid start_time string format is handled gracefully."""
    pipeline_client.ensure_bucket()

    invalid_time = "not-an-iso-date"
    pipeline_client.send_heartbeat("Stage", "Task", start_time=invalid_time)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    # Should use current time (default) and drop start_time field from data
    assert "start_time" not in event.data
    assert f"Invalid start_time format: {invalid_time}" in caplog.text


@pytest.mark.parametrize(
    "field,value",
    [
        ("stage", 123),
        ("stage", None),
        ("task", 456),
        ("task", None),
        ("stage", ["list"]),
        ("task", {"dict": 1}),
    ],
)
def test_send_heartbeat_invalid_core_types_parametrized(
    pipeline_client: PipelineClient,
    mock_aw_client: MagicMock,
    field: str,
    value: Any,
) -> None:
    """Test that invalid types for core fields raise ValueError (Directive 5.2.2)."""
    kwargs = {"stage": "Stage", "task": "Task"}
    kwargs[field] = value

    with pytest.raises(ValueError, match=f"Invalid {field} type"):
        pipeline_client.send_heartbeat(**kwargs)  # type: ignore


def test_metadata_allowlist_filtering(
    tmp_path: Path, mock_aw_client: MagicMock
) -> None:
    """Test that metadata is filtered based on allowlist."""
    # Initialize client with allowlist
    with patch("socket.gethostname", return_value="test-host"):
        client = PipelineClient(
            watch_path=tmp_path / "test.json",
            client=mock_aw_client,
            testing=True,
            metadata_allowlist=["allowed_key", "also_allowed"]
        )

    metadata = {
        "allowed_key": "value1",
        "also_allowed": "value2",
        "blocked_key": "value3",
        "other": "value4"
    }

    client.send_heartbeat("Stage", "Task", metadata=metadata)

    mock_aw_client.heartbeat.assert_called()
    args = mock_aw_client.heartbeat.call_args
    event = args[0][1]

    assert "allowed_key" in event.data
    assert "also_allowed" in event.data
    assert "blocked_key" not in event.data
    assert "other" not in event.data
    assert event.data["allowed_key"] == "value1"


def test_project_id_invalid_type(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that non-string project_id is dropped."""
    pipeline_client.ensure_bucket()

    # Pass integer as project_id
    pipeline_client.send_heartbeat("Stage", "Task", project_id=12345)  # type: ignore

    mock_aw_client.heartbeat.assert_called()
    args = mock_aw_client.heartbeat.call_args
    event = args[0][1]

    assert "project_id" not in event.data
    assert "Invalid project_id type" in caplog.text


def test_metadata_size_limit_boundary(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test metadata size limit boundary conditions."""
    pipeline_client.ensure_bucket()
    
    # Overhead per item is len(key) + len(json.dumps(val)) + 6
    # Target 1024 bytes.
    # Key "k1" (2) + Quotes (2) + Colon (1) + Space (1) = 6 overhead
    # Value "x" * 1012 -> 1012 + 2 (quotes) = 1014
    # Total = 2 + 1014 + 6 = 1022 bytes. Fits.
    
    val_ok = "x" * 1012
    metadata_ok = {"k1": val_ok}
    
    pipeline_client.send_heartbeat("Stage", "Task", metadata=metadata_ok)
    event = mock_aw_client.heartbeat.call_args[0][1]
    assert "k1" in event.data
    
    # Add a second item. Even a small one will exceed 1024 total.
    metadata_over = {"k1": val_ok, "k2": "a"}
    pipeline_client.send_heartbeat("Stage", "Task", metadata=metadata_over)
    event = mock_aw_client.heartbeat.call_args[0][1]
    assert "k1" in event.data
    assert "k2" not in event.data
    assert "Metadata exceeds 1KB limit" in caplog.text


def test_status_case_sensitivity(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that status validation is case-sensitive (client expects normalized input)."""
    pipeline_client.ensure_bucket()
    
    pipeline_client.send_heartbeat("Stage", "Task", status="In_Progress")
    
    event = mock_aw_client.heartbeat.call_args[0][1]
    assert "status" not in event.data
    assert "Invalid status 'In_Progress'" in caplog.text


def test_offline_queuing_multiple_events_flush(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test queuing multiple events while offline and flushing (Directive 5.2.2)."""
    # Simulate offline
    mock_aw_client.heartbeat.side_effect = ConnectionError("Offline")

    with patch("time.sleep"):
        pipeline_client.send_heartbeat("Stage 1", "Task 1")
        pipeline_client.send_heartbeat("Stage 2", "Task 2")
        pipeline_client.send_heartbeat("Stage 3", "Task 3")

    # Verify all attempts tried to queue
    assert mock_aw_client.heartbeat.call_count >= 3
    # Check args for queued=True
    for call in mock_aw_client.heartbeat.call_args_list:
        _, kwargs = call
        assert kwargs.get("queued") is True

    # Flush
    pipeline_client.flush_queue()
    mock_aw_client.flush.assert_called_once()


def test_metadata_truncation_stops_processing(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that metadata processing stops after limit is reached (Directive 5.2.2)."""
    pipeline_client.ensure_bucket()

    # Create metadata where k2 causes overflow
    # k1 fits, k2 overflows, k3 is skipped due to break
    val_large = "x" * 1010  # ~1020 bytes with overhead
    metadata = {
        "k1": "small",  # ~15 bytes
        "k2": val_large,  # Causes total > 1024
        "k3": "small",  # Should be skipped due to break
    }

    pipeline_client.send_heartbeat("Stage", "Task", metadata=metadata)

    event = mock_aw_client.heartbeat.call_args[0][1]
    assert "k1" in event.data
    assert "k2" not in event.data
    assert "k3" not in event.data
    assert "Metadata exceeds 1KB limit" in caplog.text


def test_send_heartbeat_explicit_none_optionals(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test that explicit None values for optional fields result in omitted keys."""
    pipeline_client.ensure_bucket()

    pipeline_client.send_heartbeat(
        "Stage", "Task", project_id=None, status=None, file_path=None, start_time=None, computed_duration=None
    )

    event = mock_aw_client.heartbeat.call_args[0][1]
    assert "project_id" not in event.data
    assert "status" not in event.data
    assert "file_path" not in event.data
    assert "start_time" not in event.data
    assert "computed_duration" not in event.data


def test_send_heartbeat_empty_strings_omitted(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test that empty strings are omitted for optional fields."""
    pipeline_client.ensure_bucket()

    pipeline_client.send_heartbeat("Stage", "Task", project_id="", file_path="")

    event = mock_aw_client.heartbeat.call_args[0][1]
    assert "project_id" not in event.data
    assert "file_path" not in event.data


def test_send_heartbeat_start_time_naive(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test that naive start_time strings are assumed to be UTC."""
    pipeline_client.ensure_bucket()
    # Naive ISO string (no Z or offset)
    start_time_str = "2023-10-27T10:00:00"

    pipeline_client.send_heartbeat("Stage", "Task", start_time=start_time_str)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    # Verify timestamp matches and has UTC timezone
    assert event.timestamp.isoformat() == "2023-10-27T10:00:00+00:00"
    assert event.data["start_time"] == start_time_str


def test_send_heartbeat_computed_duration_invalid_type(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that non-numeric computed_duration is dropped."""
    pipeline_client.ensure_bucket()

    pipeline_client.send_heartbeat("Stage", "Task", computed_duration="invalid")  # type: ignore

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    assert "computed_duration" not in event.data
    assert "Invalid computed_duration type" in caplog.text


def test_send_heartbeat_file_path_invalid_type(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that non-string file_path is dropped."""
    pipeline_client.ensure_bucket()

    pipeline_client.send_heartbeat("Stage", "Task", file_path=12345)  # type: ignore

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    assert "file_path" not in event.data
    assert "Invalid file_path type" in caplog.text


def test_send_heartbeat_metadata_nested_structure(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test that nested metadata structures are preserved if they fit in size limit."""
    pipeline_client.ensure_bucket()

    nested_meta = {"config": {"nested": True, "list": [1, 2, 3]}, "tags": ["a", "b"]}

    pipeline_client.send_heartbeat("Stage", "Task", metadata=nested_meta)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    # Metadata is flattened into data, so keys "config" and "tags" should be at top level of data
    assert event.data["config"] == nested_meta["config"]
    assert event.data["tags"] == nested_meta["tags"]


def test_send_heartbeat_metadata_empty_keys(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test that metadata with empty keys is handled correctly."""
    pipeline_client.ensure_bucket()

    metadata = {"": "empty_key_value", "normal": "val"}

    pipeline_client.send_heartbeat("Stage", "Task", metadata=metadata)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    assert event.data[""] == "empty_key_value"
    assert event.data["normal"] == "val"


def test_metadata_allowlist_prevents_truncation(
    tmp_path: Path, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that blocked metadata keys do not contribute to size limit."""
    # Allowlist only 'small'
    with patch("socket.gethostname", return_value="test-host"):
        client = PipelineClient(
            watch_path=tmp_path / "test.json",
            client=mock_aw_client,
            testing=True,
            metadata_allowlist=["small"]
        )

    # 'huge' would exceed limit if processed, but should be filtered first
    metadata = {
        "small": "value",
        "huge": "x" * 2000
    }

    client.send_heartbeat("Stage", "Task", metadata=metadata)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    assert "small" in event.data
    assert "huge" not in event.data
    # Should NOT warn about truncation because 'huge' was filtered before size check
    assert "Metadata exceeds 1KB limit" not in caplog.text


def test_send_heartbeat_overflow_error(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that OverflowError is treated as fatal (no retries)."""
    mock_aw_client.heartbeat.side_effect = OverflowError("Math overflow")

    pipeline_client.send_heartbeat("Stage", "Task")

    assert mock_aw_client.heartbeat.call_count == 1
    assert "Failed to serialize heartbeat event" in caplog.text


def test_start_time_timezone_preservation(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test that timezone offset in start_time is preserved."""
    pipeline_client.ensure_bucket()
    # ISO string with offset +02:00
    start_time_str = "2023-10-27T12:00:00+02:00"

    pipeline_client.send_heartbeat("Stage", "Task", start_time=start_time_str)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    # Verify offset is preserved (not forcibly converted to UTC Z)
    assert event.timestamp.isoformat() == "2023-10-27T12:00:00+02:00"
    assert event.data["start_time"] == start_time_str


def test_send_heartbeat_retry_backoff_timing(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test that retry backoff increases exponentially."""
    mock_aw_client.heartbeat.side_effect = [Exception("Fail 1"), Exception("Fail 2"), None]

    with patch("time.sleep") as mock_sleep:
        pipeline_client.send_heartbeat("Stage", "Task")

        assert mock_sleep.call_count == 2
        # First sleep 0.5, second sleep 1.0
        assert mock_sleep.call_args_list[0][0][0] == 0.5
        assert mock_sleep.call_args_list[1][0][0] == 1.0


def test_send_heartbeat_empty_stage_task(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test that empty strings for stage/task are accepted."""
    pipeline_client.send_heartbeat("", "")

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]
    assert event.data["stage"] == ""
    assert event.data["task"] == ""


def test_send_heartbeat_metadata_value_none(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test that None values in metadata are preserved as null."""
    pipeline_client.ensure_bucket()

    metadata = {"key": None, "other": "value"}
    pipeline_client.send_heartbeat("Stage", "Task", metadata=metadata)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    assert event.data["key"] is None
    assert event.data["other"] == "value"


def test_ensure_bucket_failure_reraises(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that ensure_bucket logs and re-raises exceptions."""
    mock_aw_client.create_bucket.side_effect = Exception("Bucket creation failed")

    with pytest.raises(Exception, match="Bucket creation failed"):
        pipeline_client.ensure_bucket()

    assert "Failed to create bucket" in caplog.text


def test_send_heartbeat_file_path_home_dir_exact(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test anonymization when file_path is exactly the home directory."""
    pipeline_client.ensure_bucket()

    with patch("pathlib.Path.home", return_value=Path("/home/user")):
        pipeline_client.send_heartbeat("Stage", "Task", file_path="/home/user")

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]
    assert event.data["file_path"] == "~"


def test_send_heartbeat_computed_duration_zero(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock
) -> None:
    """Test that computed_duration of 0.0 is preserved."""
    pipeline_client.ensure_bucket()

    pipeline_client.send_heartbeat("Stage", "Task", computed_duration=0.0)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]
    assert event.data["computed_duration"] == 0.0


def test_send_heartbeat_metadata_allowlist_empty(
    tmp_path: Path, mock_aw_client: MagicMock
) -> None:
    """Test that an empty allowlist filters all metadata."""
    with patch("socket.gethostname", return_value="test-host"):
        client = PipelineClient(
            watch_path=tmp_path / "test.json",
            client=mock_aw_client,
            testing=True,
            metadata_allowlist=[]
        )

    metadata = {"key": "value", "other": 123}
    client.send_heartbeat("Stage", "Task", metadata=metadata)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    # Core fields present
    assert event.data["stage"] == "Stage"
    # Metadata fields absent
    assert "key" not in event.data
    assert "other" not in event.data


def test_send_heartbeat_metadata_single_huge_key(
    pipeline_client: PipelineClient, mock_aw_client: MagicMock, caplog: LogCaptureFixture
) -> None:
    """Test that a single metadata item exceeding the limit is dropped."""
    pipeline_client.ensure_bucket()

    # Key "huge" (4) + Value (1020 chars) + Overhead (6) = 1030 > 1024
    metadata = {"huge": "x" * 1020}

    pipeline_client.send_heartbeat("Stage", "Task", metadata=metadata)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    assert "huge" not in event.data
    assert "Metadata exceeds 1KB limit" in caplog.text
