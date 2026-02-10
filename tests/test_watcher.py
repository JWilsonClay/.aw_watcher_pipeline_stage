from __future__ import annotations

import json
import logging
import random
import shutil
import stat
import string
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any
import sys
from unittest.mock import MagicMock, patch

import pytest
from watchdog.events import FileMovedEvent

from aw_watcher_pipeline_stage.watcher import (
    MAX_FILE_SIZE_BYTES,
    PipelineEventHandler,
    PipelineWatcher,
)

if TYPE_CHECKING:
    from aw_watcher_pipeline_stage.client import PipelineClient


@pytest.mark.parametrize(
    "json_data,expected_stage,expected_task,expected_extras,expected_paused",
    [
        (
            {
                "current_stage": "Test",
                "current_task": "Unit Testing",
                "status": "in_progress",
                "project_id": "p1",
                "start_time": "2023-01-01T12:00:00Z",
                "metadata": {"foo": "bar"},
            },
            "Test",
            "Unit Testing",
            {"project_id": "p1", "start_time": "2023-01-01T12:00:00Z", "metadata": {"foo": "bar"}, "status": "in_progress"},
            False,
        ),
        (
            {"current_stage": "Minimal", "current_task": "Task"},
            "Minimal",
            "Task",
            {"project_id": None, "start_time": None, "status": None, "metadata": {}},
            False,
        ),
        (
            {"current_stage": "Extra", "current_task": "Fields", "unknown": "ignored"},
            "Extra",
            "Fields",
            {"project_id": None},
            False,
        ),
        (
            {"current_stage": "Paused", "current_task": "Task", "status": "paused"},
            "Paused",
            "Task",
            {"status": "paused"},
            True,
        ),
        (
            {"current_stage": "Numeric", "current_task": "Task", "project_id": 123},
            "Numeric",
            "Task",
            {"project_id": 123},
            False,
        ),
    ],
    ids=["Full JSON", "Minimal JSON", "Extra Fields", "Paused Status", "Numeric Project ID"],
)
def test_parse_json_scenarios(
    pipeline_client: PipelineClient,
    temp_dir: Path,
    mock_aw_client: MagicMock,
    json_data: dict[str, Any],
    expected_stage: str,
    expected_task: str,
    expected_extras: dict[str, Any],
    expected_paused: bool,
) -> None:
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps(json_data))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    data = handler._read_file_data(str(f))
    if data:
        data = handler._process_state_change(data, str(f))
    handler.on_state_changed(data, 0.0)

    assert handler.last_stage == expected_stage
    assert handler.last_task == expected_task
    assert handler.is_paused == expected_paused

    # Verify stage and task are in current_data
    assert handler.current_data.get("current_stage") == expected_stage
    assert handler.current_data.get("current_task") == expected_task

    # Verify unknown fields are filtered out
    if "unknown" in json_data:
        assert "unknown" not in handler.current_data

    for k, v in expected_extras.items():
        assert handler.current_data.get(k) == v

    mock_aw_client.heartbeat.assert_called_once()


@pytest.mark.parametrize(
    "file_content,expected_log_fragment",
    [
        ("{ invalid json", "Malformed JSON"),
        ("[]", "JSON root is not a dictionary"),
        ("123", "JSON root is not a dictionary"),
        ('"string"', "JSON root is not a dictionary"),
        ("null", "JSON root is not a dictionary"),
    ],
    ids=["Invalid Syntax", "List", "Integer", "String", "Null"],
)
def test_parse_malformed_or_invalid_structure(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock, file_content: str, expected_log_fragment: str
) -> None:
    f = temp_dir / "current_task.json"
    f.write_text(file_content)

    # Mock sleep to skip backoff delays
    with patch("time.sleep"):
        handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
        # Should not raise, just log error after retries
        with patch.object(handler.logger, "error") as mock_error:
            assert handler._read_file_data(str(f)) is None
        mock_error.assert_called_once()
        assert expected_log_fragment in mock_error.call_args[0][0]

    assert handler.last_stage is None
    mock_aw_client.heartbeat.assert_not_called()


@pytest.mark.parametrize("exception_cls", [PermissionError, OSError])
def test_parse_read_error_retries(
    pipeline_client: PipelineClient, temp_dir: Path, exception_cls: type[Exception]
) -> None:
    f = temp_dir / "current_task.json"
    f.write_text("{}")  # Ensure file is not empty to bypass size check

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch("time.sleep"):
        with patch.object(Path, "open", side_effect=exception_cls("Access denied")) as mock_open:
            with patch.object(handler.logger, "error") as mock_error:
                with patch.object(handler.logger, "debug") as mock_debug:
                    handler._read_file_data(str(f))
                    
                    # Should retry 5 times (initial + 4 retries)
                    assert mock_open.call_count == 5
                    assert mock_debug.call_count == 4
                    mock_error.assert_called_once()
                    if exception_cls is PermissionError:
                        assert "Permission denied" in mock_error.call_args[0][0]
                    else:
                        assert "Error processing" in mock_error.call_args[0][0]

def test_read_file_data_custom_retries(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that _read_file_data respects the max_attempts parameter."""
    f = temp_dir / "current_task.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Mock stat to raise OSError every time
    with patch("time.sleep") as mock_sleep:
        with patch.object(Path, "stat", side_effect=OSError("Fail")):
            with patch.object(handler.logger, "error"): # Suppress error log
                with patch.object(handler.logger, "debug"):
                    # Try with 2 attempts
                    handler._read_file_data(str(f), max_attempts=2)
                    # Should have slept once (between attempt 1 and 2)
                    assert mock_sleep.call_count == 1


def test_parse_is_a_directory(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that IsADirectoryError is handled gracefully without retries."""
    # Create a directory where the file should be
    d = temp_dir / "dir_conflict"
    d.mkdir()
    
    handler = PipelineEventHandler(d, pipeline_client, pulsetime=120.0)
    
    with patch.object(handler.logger, "warning") as mock_warn:
        handler._read_file_data(str(d))
        mock_warn.assert_called_once()
        assert "Target path is a directory" in mock_warn.call_args[0][0]

def test_parse_non_regular_file(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that non-regular files (e.g. sockets/pipes) are skipped."""
    f = temp_dir / "socket"
    f.touch()
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    # Mock stat to return a mode that isn't S_IFREG (e.g. S_IFSOCK)
    with patch.object(Path, "stat") as mock_stat:
        mock_stat.return_value.st_mode = stat.S_IFSOCK
        
        with patch.object(handler.logger, "warning") as mock_warn:
            assert handler._read_file_data(str(f)) is None
            assert "not a regular file" in mock_warn.call_args[0][0]

def test_parse_stat_error(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    f = temp_dir / "current_task.json"
    f.touch()

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch("time.sleep"):
        with patch.object(Path, "stat", side_effect=OSError("Disk error")) as mock_stat:
            with patch.object(handler.logger, "error") as mock_error:
                with patch.object(handler.logger, "debug") as mock_debug:
                    handler._read_file_data(str(f))
                    
                    # Should retry 5 times
                    assert mock_stat.call_count == 5
                    assert mock_debug.call_count == 4
                    mock_error.assert_called_once()
                    assert "Error processing" in mock_error.call_args[0][0]

def test_read_permission_error_logging(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that PermissionError logs a specific warning."""
    f = temp_dir / "current_task.json"
    f.touch()
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    with patch("time.sleep"):
        with patch.object(Path, "stat", side_effect=PermissionError("Access denied")):
            with patch.object(handler.logger, "debug") as mock_debug:
                with patch.object(handler.logger, "error") as mock_error:
                    handler._read_file_data(str(f))
                    
                    # Should have logged specific permission debug message
                    assert mock_debug.call_count >= 1
                    assert any("Permission denied" in str(c) for c in mock_debug.call_args_list)
                    
                    # Should log error on final attempt
                    mock_error.assert_called_once()
                    assert "Permission denied" in str(mock_error.call_args[0][0])

def test_recovery_from_permission_error(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    f = temp_dir / "current_task.json"
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # 1. Valid write
    f.write_text(json.dumps({"current_stage": "S1", "current_task": "T1"}))
    handler.on_state_changed(handler._parse_file(str(f)), 0.0)
    assert handler.last_stage == "S1"
    mock_aw_client.heartbeat.assert_called_once()
    mock_aw_client.heartbeat.reset_mock()

    # 2. Permission Error (simulated)
    with patch("time.sleep"):
        with patch.object(Path, "open", side_effect=PermissionError("Locked")):
            with patch.object(handler.logger, "error") as mock_error:
                handler._read_file_data(str(f))
                mock_error.assert_called_once()

    # 3. Valid write with NEW content
    f.write_text(json.dumps({"current_stage": "S2", "current_task": "T2"}))
    handler.on_state_changed(handler._parse_file(str(f)), 0.0)

    # Should recover and update
    assert handler.last_stage == "S2"
    mock_aw_client.heartbeat.assert_called_once()


@pytest.mark.parametrize(
    "json_data,should_parse",
    [
        ({"current_stage": "S", "current_task": "T"}, True),
        ({"current_stage": "", "current_task": "T"}, False),  # Empty stage
        ({"current_stage": "S", "current_task": ""}, False),  # Empty task
        ({"current_stage": None, "current_task": "T"}, False),  # Null stage
        ({"current_stage": "S", "current_task": None}, False),  # Null task
        ({"current_stage": "S"}, False),  # Missing task
        ({"current_task": "T"}, False),  # Missing stage
        ({}, False),  # Empty dict
    ],
    ids=["Valid", "Empty Stage", "Empty Task", "Null Stage", "Null Task", "Missing Task", "Missing Stage", "Empty Dict"],
)
def test_parse_validation(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock, json_data: dict[str, Any], should_parse: bool
) -> None:
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps(json_data))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    with patch.object(handler.logger, "error") as mock_error:
        data = handler._read_file_data(str(f))
        if data: data = handler._process_state_change(data, str(f))

        if should_parse:
            assert handler.last_stage is not None
            handler.on_state_changed(data, 0.0)
            mock_aw_client.heartbeat.assert_called_once()
            mock_error.assert_not_called()
        else:
            assert handler.last_stage is None
            mock_aw_client.heartbeat.assert_not_called()
            mock_error.assert_called_once()
            assert "Missing required fields" in mock_error.call_args[0][0]


def test_debounce_logic(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    f = temp_dir / "current_task.json"
    f.touch()

    # Mock Timer to verify calls
    with patch("threading.Timer") as mock_timer:
        handler = PipelineEventHandler(
            f, pipeline_client, pulsetime=120.0, debounce_seconds=0.5
        )

        event = MagicMock()
        event.is_directory = False
        event.src_path = str(f)

        # Trigger multiple times
        # This verifies the 500ms+ debounce requirement (debounce_seconds=0.5 here)
        handler.on_modified(event)
        handler.on_modified(event)

        # Timer should be cancelled and restarted
        assert mock_timer.return_value.cancel.call_count >= 1
        assert mock_timer.return_value.start.call_count >= 1
        mock_timer.assert_called_with(0.5, handler._parse_file_wrapper, args=[str(f)])


def test_file_deletion(pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock) -> None:
    f = temp_dir / "current_task.json"
    f.touch()  # Ensure file exists initially

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Simulate valid state first
    handler.current_data = {"current_stage": "S", "current_task": "T"}

    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)

    # Actually delete the file to test resolution of non-existent path
    f.unlink()

    with patch.object(handler.logger, "warning") as mock_warn:
        handler.on_deleted(event)
        mock_warn.assert_called_once()
        assert "File deleted" in mock_warn.call_args[0][0]

    assert handler.is_paused
    assert handler.current_data["status"] == "paused"

    mock_aw_client.heartbeat.assert_called_once()
    assert mock_aw_client.heartbeat.call_args[0][1].data["status"] == "paused"


def test_integration_flow(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Integration test simulating file changes."""
    watch_file = temp_dir / "current_task.json"
    # Ensure file exists before start
    watch_file.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))

    # Use short debounce for test
    watcher = PipelineWatcher(
        watch_file, pipeline_client, pulsetime=120.0, debounce_seconds=0.1
    )
    watcher.start()

    try:
        # Give it a moment to process initial file
        time.sleep(0.5)
        mock_aw_client.heartbeat.assert_called()
        mock_aw_client.heartbeat.reset_mock()

        # Update file
        new_data = {
            "current_stage": "Integration",
            "current_task": "Writing Tests",
            "status": "in_progress",
        }
        watch_file.write_text(json.dumps(new_data))

        # Wait for debounce (0.1s) + processing
        time.sleep(0.5)

        # Verify new heartbeat
        mock_aw_client.heartbeat.assert_called()
        args = mock_aw_client.heartbeat.call_args[0]
        assert args[1].data["stage"] == "Integration"
        
        # Verify computed_duration is present and small for immediate events
        assert args[1].data.get("computed_duration", 0.0) < 1.0

    finally:
        watcher.stop()


def test_empty_file(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    f = temp_dir / "current_task.json"
    f.touch()

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    with patch("time.sleep"):
        assert handler._read_file_data(str(f)) is None

    assert handler.last_stage is None


def test_initial_missing_file(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    f = temp_dir / "missing.json"

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch.object(handler.logger, "warning") as mock_warn:
        handler._read_file_data(str(f))
        mock_warn.assert_called_once()
        assert "File not found" in mock_warn.call_args[0][0]

    assert handler.last_stage is None


def test_read_file_data_empty_retry(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that _read_file_data retries on empty file."""
    f = temp_dir / "current_task.json"
    f.touch()  # Empty

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch("time.sleep") as mock_sleep:
        # Mock stat to return size 0 twice, then size 10
        with patch.object(Path, "stat") as mock_stat:
            stat_0 = MagicMock(st_size=0)
            stat_valid = MagicMock(st_size=10)
            mock_stat.side_effect = [stat_0, stat_0, stat_valid]

            with patch.object(Path, "open", side_effect=[MagicMock(), MagicMock(), MagicMock()]) as mock_open:
                # We mock json.load to return data on the successful read
                with patch("aw_watcher_pipeline_stage.watcher.json.load", return_value={"a": 1}):
                    data = handler._read_file_data(str(f))
                    assert data == {"a": 1}
                    assert mock_sleep.call_count == 2

    assert handler.last_stage is None


def test_parse_race_condition_deletion(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    f = temp_dir / "current_task.json"
    f.write_text("{}")

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Simulate race condition: file exists during stat check (real file)
    # but disappears before open (mocked failure)
    # With retry logic, it should try 5 times (initial + 4 retries)
    with patch("time.sleep"):
        with patch.object(Path, "open", side_effect=FileNotFoundError):
            with patch.object(handler.logger, "warning") as mock_warn:
                handler._read_file_data(str(f))
                mock_warn.assert_called_once()
                assert "File not found" in mock_warn.call_args[0][0]


def test_transient_file_not_found(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test recovery from transient FileNotFoundError (e.g. atomic write race)."""
    f = temp_dir / "current_task.json"
    f.touch()

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Mock successful file context
    mock_file_cm = MagicMock()
    mock_file_cm.__enter__.return_value = MagicMock()

    with patch("time.sleep") as mock_sleep:
        # Fail twice with FileNotFoundError, then succeed
        with patch.object(Path, "open", side_effect=[FileNotFoundError, FileNotFoundError, mock_file_cm]):
            with patch("aw_watcher_pipeline_stage.watcher.json.load", return_value={"current_stage": "Transient"}):
                data = handler._read_file_data(str(f))

                assert data is not None
                assert data["current_stage"] == "Transient"
                assert mock_sleep.call_count == 2


@pytest.mark.parametrize(
    "initial_data,new_data,should_trigger",
    [
        (
            {"current_stage": "A", "current_task": "1"},
            {"current_stage": "A", "current_task": "1"},
            False,
        ),
        (
            {"current_stage": "A", "current_task": "1"},
            {"current_stage": "B", "current_task": "1"},
            True,
        ),
        (
            {"current_stage": "A", "current_task": "1"},
            {"current_stage": "A", "current_task": "2"},
            True,
        ),
        (
            {"current_stage": "A", "current_task": "1"},
            {"current_stage": "A", "current_task": "1", "status": "paused"},
            True,
        ),
        (
            {"current_stage": "A", "current_task": "1"},
            {"current_stage": "A", "current_task": "1", "metadata": {"a": 1}},
            True,
        ),
        (
            {"current_stage": "A", "current_task": "1", "metadata": {"a": 1}},
            {"current_stage": "A", "current_task": "1", "metadata": {"a": 1}},
            False,
        ),
        (
            {"current_stage": "A", "current_task": "1", "metadata": {"a": 1, "b": 2}},
            {"current_stage": "A", "current_task": "1", "metadata": {"b": 2, "a": 1}},
            False,
        ),
        (
            {"current_stage": "A", "current_task": "1"},
            {"current_stage": "A", "current_task": "1", "project_id": "p1"},
            True,
        ),
        (
            {"current_stage": "A", "current_task": "1"},
            {"current_stage": "A", "current_task": "1", "status": "in_progress"},
            True,
        ),
        (
            {"current_stage": "A", "current_task": "1", "metadata": {"a": 1}},
            {"current_stage": "A", "current_task": "1"},
            True,
        ),
        (
            {"current_stage": "A", "current_task": "1", "project_id": "p1"},
            {"current_stage": "A", "current_task": "1"},
            True,
        ),
        (
            {"current_stage": "A", "current_task": "1", "status": "in_progress"},
            {"current_stage": "A", "current_task": "1"},
            True,
        ),
        (
            {"current_stage": "A", "current_task": "1", "metadata": {"n": {"v": 1}}},
            {"current_stage": "A", "current_task": "1", "metadata": {"n": {"v": 2}}},
            True,
        ),
        (
            {"current_stage": "A", "current_task": "1", "project_id": "p1"},
            {"current_stage": "A", "current_task": "1", "project_id": "p2"},
            True,
        ),
        (
            {"current_stage": "A", "current_task": "1"},
            {"current_stage": "A", "current_task": "1", "start_time": "2023-01-01T10:00:00Z"},
            True,
        ),
        (
            {"current_stage": "A", "current_task": "1", "start_time": "2023-01-01T10:00:00Z"},
            {"current_stage": "A", "current_task": "1"},
            True,
        ),
        (
            {"current_stage": "A", "current_task": "1", "start_time": "2023-01-01T10:00:00Z"},
            {"current_stage": "A", "current_task": "1", "start_time": "2023-01-01T11:00:00Z"},
            True,
        ),
        (
            {"current_stage": "A", "current_task": "1", "status": "in_progress"},
            {"status": "in_progress", "current_task": "1", "current_stage": "A"},
            False,
        ),
    ],
    ids=[
        "No Change",
        "Stage Change",
        "Task Change",
        "Status Change",
        "Metadata Add",
        "Metadata Same",
        "Metadata Reorder",
        "Project ID Add",
        "Status Add",
        "Metadata Remove",
        "Project ID Remove",
        "Status Remove",
        "Nested Metadata Change",
        "Project ID Change",
        "Start Time Add",
        "Start Time Remove",
        "Start Time Change",
        "Top Level Key Reorder",
    ],
)
def test_state_comparison(
    pipeline_client: PipelineClient,
    temp_dir: Path,
    mock_aw_client: MagicMock,
    initial_data: dict[str, Any],
    new_data: dict[str, Any],
    should_trigger: bool,
) -> None:
    f = temp_dir / "current_task.json"
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Initial write
    f.write_text(json.dumps(initial_data))
    handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)
    mock_aw_client.heartbeat.reset_mock()

    # New write
    f.write_text(json.dumps(new_data))
    data = handler._process_state_change(handler._read_file_data(str(f)), str(f))

    if should_trigger:
        handler.on_state_changed(data, 0.0)
        mock_aw_client.heartbeat.assert_called_once()
    else:
        mock_aw_client.heartbeat.assert_not_called()


def test_periodic_heartbeat(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    f = temp_dir / "current_task.json"
    data = {
        "current_stage": "Periodic",
        "current_task": "Check",
        "status": "in_progress",
    }
    f.write_text(json.dumps(data))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)

    # Reset mock to clear the immediate heartbeat
    mock_aw_client.heartbeat.reset_mock()

    # Simulate time passing (31 seconds)
    current_time = 1000.0
    handler.last_heartbeat_time = current_time - 31.0
    # Set last change time further back so computed_duration is significant
    handler.last_change_time = current_time - 60.0

    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", return_value=current_time):
        handler._periodic_heartbeat_task()

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]
    # computed_duration should be approx 60s
    assert event.data["computed_duration"] >= 60.0


@pytest.mark.parametrize("status", ["paused", "completed"])
def test_periodic_heartbeat_paused_or_completed(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock, status: str
) -> None:
    f = temp_dir / "current_task.json"
    data = {
        "current_stage": "Periodic",
        "current_task": "Check",
        "status": status,
    }
    f.write_text(json.dumps(data))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)

    mock_aw_client.heartbeat.reset_mock()

    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", return_value=1000.0):
        handler.last_heartbeat_time = 1000.0 - 31.0
        handler._periodic_heartbeat_task()

    mock_aw_client.heartbeat.assert_not_called()


def test_parse_invalid_timestamp(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    f = temp_dir / "current_task.json"
    data = {
        "current_stage": "S",
        "current_task": "T",
        "start_time": "invalid-time",
    }
    f.write_text(json.dumps(data))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch.object(handler.logger, "warning") as mock_warn:
        handler._read_file_data(str(f))
        # Check that warning was logged
        args = mock_warn.call_args[0]
        assert "Invalid timestamp format" in args[0]
    
    assert handler.current_data["start_time"] is None

def test_parse_invalid_metadata(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    f = temp_dir / "current_task.json"
    data = {
        "current_stage": "S",
        "current_task": "T",
        "metadata": "not-a-dict",
    }
    f.write_text(json.dumps(data))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch.object(handler.logger, "warning") as mock_warn:
        handler._read_file_data(str(f))

        # Check that warning was logged and metadata ignored
        args = mock_warn.call_args[0]
        assert "Metadata field" in args[0]
        assert handler.current_data["metadata"] == {}


def test_status_pause_resume(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    f = temp_dir / "current_task.json"
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # 1. Start in progress
    f.write_text(json.dumps({"current_stage": "S", "current_task": "T", "status": "in_progress"}))
    handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)
    assert not handler.is_paused

    # 2. Pause
    f.write_text(json.dumps({"current_stage": "S", "current_task": "T", "status": "paused"}))
    handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)
    assert handler.is_paused

    # 3. Resume
    f.write_text(json.dumps({"current_stage": "S", "current_task": "T", "status": "in_progress"}))
    handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)
    assert not handler.is_paused


def test_file_moved_away(pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock) -> None:
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "S", "current_task": "T"}))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)

    mock_aw_client.heartbeat.reset_mock()

    # Simulate move away (rename to something else)
    event = MagicMock(spec=FileMovedEvent)
    event.is_directory = False
    event.src_path = str(f)
    event.dest_path = str(temp_dir / "archived.json")

    # on_moved checks if src_path == target_file
    handler.on_moved(event)

    assert handler.is_paused
    assert handler.current_data.get("status") == "paused"

    mock_aw_client.heartbeat.assert_called_once()
    assert mock_aw_client.heartbeat.call_args[0][1].data["status"] == "paused"


def test_file_moved_to(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    f = temp_dir / "current_task.json"
    # File doesn't exist initially

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Simulate move TO target (rename from something else)
    event = MagicMock(spec=FileMovedEvent)
    event.is_directory = False
    event.src_path = str(temp_dir / "temp.json")
    event.dest_path = str(f)
    event.event_type = "moved"

    # Create the file so parse works
    f.write_text(json.dumps({"current_stage": "Moved", "current_task": "Here"}))

    with patch("threading.Timer") as mock_timer:
        handler.on_moved(event)

        # Should trigger debounce timer
        mock_timer.assert_called_once()
        mock_timer.return_value.start.assert_called_once()


def test_parse_file_wrapper_execution(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that the wrapper correctly resets debounce counters and calls parse."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Wrapper", "current_task": "Test"}))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    handler._debounce_counter = 5

    with patch.object(handler, "_read_file_data", return_value={"stage": "test"}) as mock_read:
        with patch.object(handler, "on_state_changed") as mock_on_change:
            # Simulate that this call is coming from the active timer
            handler._timer = threading.current_thread()  # type: ignore

            handler._parse_file_wrapper(str(f))

            mock_read.assert_called_once_with(str(f))
            mock_on_change.assert_called_once()
            assert handler._debounce_counter == 0
            assert handler._timer is None


def test_stale_timer_execution_skipped(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that if the timer has been replaced, execution is skipped."""
    f = temp_dir / "current_task.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Set _timer to something else (simulating a new timer took over)
    handler._timer = MagicMock()

    with patch.object(handler, "_read_file_data") as mock_read:
        handler._parse_file_wrapper(str(f))
        mock_read.assert_not_called() # Read should be skipped due to stale timer check


def test_on_created(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that file creation triggers the debounce logic."""
    f = temp_dir / "current_task.json"
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.1)

    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)

    with patch("threading.Timer") as mock_timer:
        handler.on_created(event)
        mock_timer.assert_called_once()
        mock_timer.return_value.start.assert_called_once()


def test_directory_event_ignored(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    handler = PipelineEventHandler(temp_dir, pipeline_client, pulsetime=120.0)
    event = MagicMock()
    event.is_directory = True
    event.src_path = str(temp_dir)

    with patch.object(handler, "_parse_file_wrapper") as mock_parse:
        handler.on_modified(event)
        mock_parse.assert_not_called()


def test_lifecycle_recovery(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Comprehensive integration test: Init -> Malformed -> Recover -> Delete -> Recreate."""
    watch_file = temp_dir / "current_task.json"
    # 1. Init
    watch_file.write_text(json.dumps({"current_stage": "1", "current_task": "A"}))

    # Patch logger to verify logs
    with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
        # Use short debounce for test speed
        watcher = PipelineWatcher(
            watch_file, pipeline_client, pulsetime=120.0, debounce_seconds=0.1
        )
        watcher.start()

        try:
            # Allow time for observer to pick up event + debounce (0.1s) + processing
            time.sleep(0.5)
            pipeline_client.client.heartbeat.assert_called()
            assert pipeline_client.client.heartbeat.call_args[0][1].data["stage"] == "1"
            pipeline_client.client.heartbeat.reset_mock()

            # 2. Malformed - should NOT trigger heartbeat (Error Handling)
            watch_file.write_text("{ broken json")
            time.sleep(0.5)
            pipeline_client.client.heartbeat.assert_not_called()

            # Verify error log
            assert mock_logger.error.call_count >= 1
            assert any("Malformed JSON" in str(call) for call in mock_logger.error.call_args_list)
            mock_logger.error.reset_mock()

            # 3. Recover - should trigger heartbeat
            watch_file.write_text(json.dumps({"current_stage": "2", "current_task": "B"}))
            time.sleep(0.5)
            pipeline_client.client.heartbeat.assert_called()
            assert pipeline_client.client.heartbeat.call_args[0][1].data["stage"] == "2"
            pipeline_client.client.heartbeat.reset_mock()

            # 4. Delete - should trigger pause heartbeat
            watch_file.unlink()
            time.sleep(0.5)
            pipeline_client.client.heartbeat.assert_called()
            assert pipeline_client.client.heartbeat.call_args[0][1].data["status"] == "paused"
            pipeline_client.client.heartbeat.reset_mock()

            # Verify warning log
            assert mock_logger.warning.call_count >= 1
            assert any("File deleted" in str(call) for call in mock_logger.warning.call_args_list)
            mock_logger.warning.reset_mock()

            # 5. Recreate - should trigger heartbeat
            watch_file.write_text(json.dumps({"current_stage": "3", "current_task": "C"}))
            time.sleep(0.5)
            pipeline_client.client.heartbeat.assert_called()
            assert pipeline_client.client.heartbeat.call_args[0][1].data["stage"] == "3"

        finally:
            watcher.stop()


def test_stop_cancels_timer(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    f = temp_dir / "current_task.json"
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Mock the timer
    mock_timer = MagicMock()
    handler._timer = mock_timer  # type: ignore

    handler.stop()

    mock_timer.cancel.assert_called_once()
    assert handler._timer is None


@pytest.mark.parametrize("debounce_sec", [1.0, 2.0])
def test_rapid_updates_debounce_integration(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock, debounce_sec: float
) -> None:
    """Integration test: Rapid updates should trigger only one heartbeat after debounce.

    Directive 2:
    - Simulate multiple rapid file modifications (under 500ms intervals).
    - Verify only one state change is processed after debounce window.
    - Test with 1-2s debounce window.
    """
    watch_file = temp_dir / "current_task.json"
    watch_file.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))

    # Test with specified debounce window
    watcher = PipelineWatcher(
        watch_file, pipeline_client, pulsetime=120.0, debounce_seconds=debounce_sec
    )
    watcher.start()

    try:
        # Initial read
        time.sleep(0.2)
        mock_aw_client.heartbeat.reset_mock()

        # Rapid fire writes
        for i in range(3):
            watch_file.write_text(json.dumps({
                "current_stage": "Rapid",
                "current_task": f"Update {i}",
                "status": "in_progress"
            }))
            # Sleep less than debounce (simulating <500ms intervals)
            time.sleep(0.1)

        # Wait for debounce to settle + processing buffer
        time.sleep(debounce_sec + 0.5)

        # Should have sent exactly one heartbeat (the last one)
        assert mock_aw_client.heartbeat.call_count == 1
        args = mock_aw_client.heartbeat.call_args[0]
        assert args[1].data["task"] == "Update 2"

    finally:
        watcher.stop()


def test_debounce_mixed_sequence(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """
    Test a sequence of rapid updates including meaningful and irrelevant changes.

    Directive 2: Test meaningful field changes vs irrelevant metadata updates.

    Sequence:
    1. Initial: Stage A
    2. Update 1: Stage B (Meaningful)
    3. Update 2: Stage B + reordered metadata (Irrelevant vs Update 1)

    Expectation:
    - Debounce coalesces Update 1 and 2.
    - Final state (Update 2) is compared against Initial (Stage A).
    - One heartbeat sent for Stage B.
    """
    watch_file = temp_dir / "current_task.json"
    initial_data = {"current_stage": "Stage A", "current_task": "Task A"}
    watch_file.write_text(json.dumps(initial_data))

    watcher = PipelineWatcher(
        watch_file, pipeline_client, pulsetime=120.0, debounce_seconds=1.0
    )
    watcher.start()

    try:
        # Initial read
        time.sleep(0.2)
        mock_aw_client.heartbeat.reset_mock()

        # Rapid updates
        # 1. Meaningful change
        watch_file.write_text(json.dumps({
            "current_stage": "Stage B",
            "current_task": "Task B",
            "metadata": {"a": 1, "b": 2}
        }))
        time.sleep(0.1)

        # 2. Irrelevant change (reordered metadata)
        watch_file.write_text(json.dumps({
            "current_stage": "Stage B",
            "current_task": "Task B",
            "metadata": {"b": 2, "a": 1}
        }))

        # Wait for debounce (1.0s) + buffer
        time.sleep(1.5)

        # Verify exactly one heartbeat
        assert mock_aw_client.heartbeat.call_count == 1
        args = mock_aw_client.heartbeat.call_args[0]
        assert args[1].data["stage"] == "Stage B"
        assert args[1].data["a"] == 1  # Flattened metadata check

    finally:
        watcher.stop()


def test_start_missing_directory(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that starting watcher on non-existent directory raises FileNotFoundError."""
    non_existent = temp_dir / "non_existent_dir" / "file.json"
    watcher = PipelineWatcher(non_existent, pipeline_client, pulsetime=120.0)
    
    with pytest.raises(FileNotFoundError):
        watcher.start()


def test_parse_valid_timestamp(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that valid ISO 8601 timestamps are accepted without warning."""
    f = temp_dir / "current_task.json"
    data = {
        "current_stage": "S",
        "current_task": "T",
        "start_time": "2023-01-01T12:00:00Z",
    }
    f.write_text(json.dumps(data))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch.object(handler.logger, "warning") as mock_warn:
        handler._parse_file(str(f))
        mock_warn.assert_not_called()

    assert handler.current_data["start_time"] == "2023-01-01T12:00:00Z"


def test_missing_optional_fields_defaults(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test that missing optional fields result in correct defaults in heartbeat."""
    f = temp_dir / "current_task.json"
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Missing status, project_id, start_time
    data = {"current_stage": "Stage 1", "current_task": "Task 1"}
    f.write_text(json.dumps(data))
    handler.on_state_changed(handler._parse_file(str(f)), 0.0)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    # Check defaults
    # Current implementation converts None to "None" string
    assert event.data["status"] == "None"
    assert event.data.get("project_id") is None
    assert event.data.get("start_time") is None


def test_empty_strings_edge_case(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test handling of empty strings in optional fields."""
    f = temp_dir / "current_task.json"
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    data = {
        "current_stage": "Stage 1",
        "current_task": "Task 1",
        "project_id": "",
        "status": "",  # Should default to in_progress if treated as falsy
        "start_time": "",
    }
    f.write_text(json.dumps(data))
    handler.on_state_changed(handler._parse_file(str(f)), 0.0)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    assert event.data["project_id"] == ""
    # Current implementation preserves empty string
    assert event.data["status"] == ""
    assert event.data["start_time"] == ""


def test_explicit_null_fields(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test that explicit nulls in JSON are handled same as missing fields."""
    f = temp_dir / "current_task.json"
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    data = {
        "current_stage": "Stage 1",
        "current_task": "Task 1",
        "project_id": None,
        "status": None,
        "start_time": None,
    }
    f.write_text(json.dumps(data))
    handler.on_state_changed(handler._parse_file(str(f)), 0.0)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    assert event.data["status"] == "None"
    assert event.data.get("project_id") is None


def test_initialization_triggers_heartbeat(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test that the first valid parse triggers an immediate heartbeat."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Start"}))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    handler.on_state_changed(handler._parse_file(str(f)), 0.0)

    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]
    assert event.data["stage"] == "Init"
    assert event.data["task"] == "Start"


def test_transition_full_to_partial(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test transitioning from a full state to a partial state (fields removed)."""
    f = temp_dir / "current_task.json"
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # 1. Full state
    f.write_text(json.dumps({
        "current_stage": "S1",
        "current_task": "T1",
        "project_id": "P1",
        "metadata": {"key": "val"}
    }))
    handler.on_state_changed(handler._parse_file(str(f)), 0.0)
    mock_aw_client.heartbeat.reset_mock()

    # 2. Partial state (removed project_id and metadata)
    # Note: We keep stage/task same, but removing optional fields changes state hash
    f.write_text(json.dumps({
        "current_stage": "S1",
        "current_task": "T1"
    }))
    handler.on_state_changed(handler._parse_file(str(f)), 0.0)

    # Should trigger heartbeat because state hash changed
    mock_aw_client.heartbeat.assert_called_once()
    event = mock_aw_client.heartbeat.call_args[0][1]

    # Verify optional fields are gone
    assert "project_id" not in event.data
    assert "key" not in event.data
    assert event.data["stage"] == "S1"


def test_invalid_to_valid_transition(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test transition from invalid JSON (missing required fields) to valid JSON."""
    f = temp_dir / "current_task.json"
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # 1. Invalid (missing task)
    f.write_text(json.dumps({"current_stage": "Stage 1"}))
    handler._parse_file(str(f))
    mock_aw_client.heartbeat.assert_not_called()
    assert handler.last_comparison_data is None

    # 2. Valid
    f.write_text(json.dumps({"current_stage": "Stage 1", "current_task": "Task 1"}))
    handler.on_state_changed(handler._parse_file(str(f)), 0.0)
    mock_aw_client.heartbeat.assert_called_once()
    assert handler.last_state_hash is not None
    assert handler.last_comparison_data is not None


def test_internal_state_tracking(
    pipeline_client: PipelineClient, temp_dir: Path
) -> None:
    """Test that internal state variables (hash, timestamps) update correctly."""
    f = temp_dir / "current_task.json"
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Setup time mock
    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic") as mock_time:
        mock_time.return_value = 1000.0

        # 1. Initial Parse
        data1 = {"current_stage": "S1", "current_task": "T1"}
        f.write_text(json.dumps(data1))
        handler.on_state_changed(handler._parse_file(str(f)), 0.0)

        comp1 = handler.last_comparison_data
        time1 = handler.last_change_time
        assert comp1 is not None
        assert time1 == 1000.0

        # 2. No Change (same content)
        mock_time.return_value = 1010.0
        handler._parse_file(str(f))
        assert handler.last_comparison_data == comp1
        assert handler.last_change_time == 1000.0  # Should not update

        # 3. Meaningful Change
        mock_time.return_value = 1020.0
        data2 = {"current_stage": "S1", "current_task": "T2"}
        f.write_text(json.dumps(data2))
        handler.on_state_changed(handler._parse_file(str(f)), 0.0)

        comp2 = handler.last_comparison_data
        time2 = handler.last_change_time
        assert comp2 is not None
        assert comp2 != comp1
        assert time2 == 1020.0

        # 4. Key Reordering (No meaningful change)
        mock_time.return_value = 1030.0
        data3 = {"current_task": "T2", "current_stage": "S1"}  # Same as data2
        f.write_text(json.dumps(data3))
        handler._parse_file(str(f))

        assert handler.last_comparison_data == comp2
        assert handler.last_change_time == 1020.0  # Should not update


def test_parse_unicode_content(pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock) -> None:
    """Test that JSON with Unicode characters is parsed correctly."""
    f = temp_dir / "current_task.json"
    data = {"current_stage": "Stage 🚀", "current_task": "Task 🐛"}
    f.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)

    assert handler.last_stage == "Stage 🚀"
    assert handler.last_task == "Task 🐛"
    mock_aw_client.heartbeat.assert_called_once()


def test_watcher_is_event_driven(pipeline_client: PipelineClient, temp_dir: Path, mock_observer: MagicMock) -> None:
    """Verify that the watcher uses watchdog Observer and is event-driven."""
    f = temp_dir / "current_task.json"
    f.touch()

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    mock_observer.assert_called_once()
    observer_instance = mock_observer.return_value
    observer_instance.schedule.assert_called()
    observer_instance.start.assert_called_once()

    watcher.stop()
    observer_instance.stop.assert_called_once()
    observer_instance.join.assert_called_once()


def test_debounce_flapping_updates(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test that rapid changes A -> B -> A result in no event if settled on A."""
    watch_file = temp_dir / "current_task.json"
    initial_data = {"current_stage": "Stable", "current_task": "A"}
    watch_file.write_text(json.dumps(initial_data))

    watcher = PipelineWatcher(
        watch_file, pipeline_client, pulsetime=120.0, debounce_seconds=1.0
    )
    watcher.start()

    try:
        # Wait for initial read
        time.sleep(0.2)
        mock_aw_client.heartbeat.reset_mock()

        # Rapid flap: A -> B -> A
        # Write B
        watch_file.write_text(json.dumps({"current_stage": "Stable", "current_task": "B"}))
        time.sleep(0.1)
        # Write A
        watch_file.write_text(json.dumps(initial_data))
        
        # Wait for debounce
        time.sleep(1.5)

        # Since the file on disk is A when the timer fires, and last state was A, 
        # no heartbeat should be sent.
        mock_aw_client.heartbeat.assert_not_called()

    finally:
        watcher.stop()


def test_debounce_irrelevant_change_integration(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test that a debounced update that is semantically equivalent results in no event."""
    watch_file = temp_dir / "current_task.json"
    # Initial state
    initial_data = {
        "current_stage": "Stage A",
        "current_task": "Task A",
        "metadata": {"key1": "val1", "key2": "val2"}
    }
    watch_file.write_text(json.dumps(initial_data))

    watcher = PipelineWatcher(
        watch_file, pipeline_client, pulsetime=120.0, debounce_seconds=1.0
    )
    watcher.start()

    try:
        # Wait for initial read
        time.sleep(0.2)
        mock_aw_client.heartbeat.reset_mock()

        # Write semantically identical data (reordered metadata)
        # This triggers a file modification event -> debounce timer -> parse -> hash check
        new_data = {
            "current_stage": "Stage A",
            "current_task": "Task A",
            "metadata": {"key2": "val2", "key1": "val1"}
        }
        watch_file.write_text(json.dumps(new_data))

        # Wait for debounce
        time.sleep(1.5)

        # Should NOT trigger heartbeat
        mock_aw_client.heartbeat.assert_not_called()

    finally:
        watcher.stop()


def test_creation_debounce_integration(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test that file creation is debounced.

    Directive 2: Test initial file creation event.
    """
    watch_file = temp_dir / "created.json"
    # File does not exist yet

    watcher = PipelineWatcher(
        watch_file, pipeline_client, pulsetime=120.0, debounce_seconds=1.0
    )
    watcher.start()

    try:
        time.sleep(0.2)
        mock_aw_client.heartbeat.assert_not_called()

        # Create file
        watch_file.write_text(json.dumps({"current_stage": "Created", "current_task": "Task"}))

        # Wait less than debounce
        time.sleep(0.5)
        mock_aw_client.heartbeat.assert_not_called()

        # Wait rest of debounce
        time.sleep(1.2)
        mock_aw_client.heartbeat.assert_called_once()
        assert mock_aw_client.heartbeat.call_args[0][1].data["stage"] == "Created"

    finally:
        watcher.stop()


def test_parse_encoding_error(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    f = temp_dir / "current_task.json"
    # Write invalid utf-8 bytes
    with open(f, "wb") as binary_file:
        binary_file.write(b"\x80\x81") # Invalid start byte

    # Mock sleep to skip backoff
    with patch("time.sleep"):
        handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

        with patch.object(handler.logger, "warning") as mock_warn:
            handler._read_file_data(str(f))
            mock_warn.assert_called_once()
            # The error message might vary slightly depending on where it was caught, but should be logged
            assert "Encoding error" in mock_warn.call_args[0][0]


def test_on_deleted_oserror(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    f = temp_dir / "current_task.json"
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)

    # Simulate OSError when resolving path
    with patch.object(Path, "absolute", side_effect=OSError("Path error")):
        # Should simply return without error/warning/crash
        handler.on_deleted(event)


def test_parse_unexpected_error(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that unexpected exceptions are caught and logged without crashing."""
    f = temp_dir / "current_task.json"
    f.write_text("{}")
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch.object(Path, "open", side_effect=RuntimeError("Unexpected boom")):
        with patch.object(handler.logger, "error") as mock_error:
            handler._read_file_data(str(f))
            mock_error.assert_called_once()
            assert "Unexpected error" in mock_error.call_args[0][0]


def test_parse_flaky_read_recovery(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test recovery from transient read errors (retry logic)."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Flaky", "current_task": "Success"}))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch("time.sleep") as mock_sleep:
        # Patch json.load to simulate a read error followed by success
        with patch("aw_watcher_pipeline_stage.watcher.json.load", side_effect=[OSError("Disk glitch"), {"current_stage": "Flaky", "current_task": "Success"}]) as mock_load:
            with patch.object(handler.logger, "debug") as mock_debug:
                data = handler._read_file_data(str(f))
                
                # Should have slept once (backoff)
                mock_sleep.assert_called_once()
                # Should have called load twice (retry)
                assert mock_load.call_count == 2
                # Should have logged debug for first failure
                mock_debug.assert_called_once()
                assert "Read error" in mock_debug.call_args[0][0]
                
                # Should have succeeded eventually
                assert handler.last_stage == "Flaky"
                assert data is not None
                assert data["current_stage"] == "Flaky"
                
                # Verify heartbeat was NOT called (since we didn't call on_state_changed)
                mock_aw_client.heartbeat.assert_not_called()


def test_observer_restart_logic(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that the observer restarts if it dies."""
    f = temp_dir / "current_task.json"
    f.touch()

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    # Verify started
    assert watcher.observer.is_alive()
    original_observer = watcher.observer

    # Simulate crash by stopping manually
    original_observer.stop()
    original_observer.join()
    assert not original_observer.is_alive()

    # Accessing observer should trigger restart
    with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
        new_observer = watcher.observer
        
        # Verify restart happened
        assert new_observer is not original_observer
        assert new_observer.is_alive()
        
        # Verify log message
        assert mock_logger.error.call_count >= 1
        assert "Watchdog observer found dead" in str(mock_logger.error.call_args)

    watcher.stop()


def test_state_preserved_across_restart(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test that handler state is preserved across observer restarts."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "S1", "current_task": "T1"}))

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.1)
    watcher.start()

    # Wait for initial read
    time.sleep(0.2)
    assert watcher.handler.last_stage == "S1"

    # Kill observer
    watcher.observer.stop()
    watcher.observer.join()

    # Trigger restart by accessing observer (simulating main loop check)
    assert watcher.observer.is_alive()

    # Write new data
    f.write_text(json.dumps({"current_stage": "S2", "current_task": "T2"}))

    # Wait for processing
    time.sleep(0.5)

    assert watcher.handler.last_stage == "S2"
    
    # Verify heartbeats: 1 for S1, 1 for S2
    assert mock_aw_client.heartbeat.call_count >= 2
    
    watcher.stop()


def test_periodic_heartbeat_restarts_observer(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that check_periodic_heartbeat triggers observer restart if dead."""
    f = temp_dir / "current_task.json"
    f.touch()

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    original_observer = watcher.observer
    assert original_observer.is_alive()

    # Simulate crash
    original_observer.stop()
    original_observer.join()
    assert not original_observer.is_alive()

    # Trigger via periodic check (simulating main loop)
    with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
        watcher.check_health()

        # Should have logged warning
        assert mock_logger.error.call_count >= 1
        assert "Watchdog observer found dead" in str(mock_logger.error.call_args)

    # Verify restarted
    assert watcher.observer is not original_observer
    assert watcher.observer.is_alive()

    watcher.stop()


def test_observer_start_failure_recovery(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test recovery when observer fails to start initially (with retries)."""
    f = temp_dir / "current_task.json"
    f.touch()

    with patch("aw_watcher_pipeline_stage.watcher.Observer") as mock_observer_cls:
        # First observer fails start, Second succeeds
        obs1 = MagicMock()
        obs1.start.side_effect = OSError("Fail 1")
        obs1.is_alive.return_value = False
        
        obs2 = MagicMock()
        obs2.start.return_value = None
        # is_alive sequence: 
        # 1. Inside _start_observer loop (check before start): False
        # 2. After start (when accessed later): True
        obs2.is_alive.side_effect = [False, True, True]
        
        mock_observer_cls.side_effect = [obs1, obs2]
        
        watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
        
        with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
            with patch("time.sleep"):
                # Start should succeed internally after retry
                watcher.start()
            
            # Should have logged error for first failure
            assert mock_logger.error.call_count >= 1
            assert "Failed to start observer" in str(mock_logger.error.call_args_list[0])
            
            # Should have created two observers
            assert mock_observer_cls.call_count == 2
            obs1.start.assert_called_once()
            obs2.start.assert_called_once()
            
            # Watcher should hold the working observer
            assert watcher._observer is obs2

    watcher.stop()


def test_observer_schedule_failure_recovery(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test recovery when observer fails to schedule watch (with retries)."""
    f = temp_dir / "current_task.json"
    f.touch()

    with patch("aw_watcher_pipeline_stage.watcher.Observer") as mock_observer_cls:
        obs1 = MagicMock()
        obs1.schedule.side_effect = OSError("No space left")
        obs1.is_alive.return_value = False
        
        obs2 = MagicMock()
        obs2.schedule.return_value = None
        obs2.is_alive.side_effect = [False, True, True]
        
        mock_observer_cls.side_effect = [obs1, obs2]
        
        watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
        
        with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
            with patch("time.sleep"):
                watcher.start()
            
            assert obs1.schedule.call_count == 1
            assert mock_logger.error.call_count >= 1
            
            obs2.schedule.assert_called_once()
            obs2.start.assert_called_once()

    watcher.stop()


def test_observer_persistent_failure(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that observer gives up after max retries."""
    f = temp_dir / "current_task.json"
    f.touch()

    with patch("aw_watcher_pipeline_stage.watcher.Observer") as mock_observer_cls:
        obs = MagicMock()
        obs.start.side_effect = OSError("Persistent Fail")
        obs.is_alive.return_value = False
        
        mock_observer_cls.return_value = obs
        
        watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
        
        with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
            with patch("time.sleep"):
                with pytest.raises(RuntimeError, match="Failed to start watchdog observer"):
                    watcher.start()
            
            # Should have logged final error
            assert any("Could not start observer" in str(c) for c in mock_logger.error.call_args_list)
            # Should have retried max times (3)
            assert obs.start.call_count == 3


def test_observer_restart_failure_recovery(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test recovery when observer fails to restart initially (with retries)."""
    f = temp_dir / "current_task.json"
    f.touch()

    # Start with a working observer
    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    original_observer = watcher.observer
    assert original_observer.is_alive()

    # Kill it
    original_observer.stop()
    original_observer.join()

    # Now mock Observer class to fail once then succeed
    with patch("aw_watcher_pipeline_stage.watcher.Observer") as mock_observer_cls:
        obs_fail = MagicMock()
        obs_fail.start.side_effect = OSError("Restart Fail")
        obs_fail.is_alive.return_value = False

        obs_success = MagicMock()
        obs_success.start.return_value = None
        # is_alive sequence:
        # 1. Inside _start_observer loop (check before start): False
        # 2. After start (when accessed later): True
        obs_success.is_alive.side_effect = [False, True, True]

        mock_observer_cls.side_effect = [obs_fail, obs_success]

        with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
            with patch("time.sleep"):
                # Trigger restart via property
                new_observer = watcher.observer

                # Should have logged error for the dead observer
                assert any("Watchdog observer found dead" in str(c) for c in mock_logger.error.call_args_list)
                # Should have logged error for the failed restart attempt
                assert any("Failed to start observer" in str(c) for c in mock_logger.error.call_args_list)

                # Should have eventually succeeded
                assert new_observer is obs_success
                assert new_observer.is_alive()

    watcher.stop()


def test_observer_restart_persistent_failure(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that if restart fails persistently, the watcher reports it."""
    f = temp_dir / "current_task.json"
    f.touch()

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    # Kill original
    watcher.observer.stop()
    watcher.observer.join()

    # Mock persistent failure on restart
    with patch("aw_watcher_pipeline_stage.watcher.Observer") as mock_observer_cls:
        obs_fail = MagicMock()
        obs_fail.start.side_effect = OSError("Persistent Restart Fail")
        obs_fail.is_alive.return_value = False

        mock_observer_cls.return_value = obs_fail

        with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
            with patch("time.sleep"):
                # Trigger restart
                current_obs = watcher.observer

                # Should have retried 3 times
                assert obs_fail.start.call_count == 3
                # Should have logged error
                assert any("Could not start observer" in str(c) for c in mock_logger.error.call_args_list)
                # Returned observer should be dead
                assert not current_obs.is_alive()

    watcher.stop()


def test_observer_cleanup_on_restart(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that the old observer is joined before creating a new one."""
    f = temp_dir / "current_task.json"
    f.touch()

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    old_observer = watcher.observer
    # Simulate death
    old_observer.stop()
    old_observer.join()

    # Mock join to verify it's called during restart logic
    with patch.object(old_observer, "join", wraps=old_observer.join) as mock_join:
        # Trigger restart by accessing property
        _ = watcher.observer

        # Verify join was called
        mock_join.assert_called()

    watcher.stop()


def test_observer_cleanup_error_handling(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that errors during observer cleanup (join) are swallowed."""
    f = temp_dir / "current_task.json"
    f.touch()

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    old_observer = watcher.observer
    # Simulate death
    old_observer.stop()
    old_observer.join()

    # Mock join to raise exception
    with patch.object(old_observer, "join", side_effect=RuntimeError("Join failed")):
        # Trigger restart
        new_observer = watcher.observer
        
        # Should succeed despite join error
        assert new_observer is not old_observer
        assert new_observer.is_alive()

    watcher.stop()


def test_observer_constructor_failure_recovery(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test recovery when Observer constructor fails (e.g. inotify limit)."""
    f = temp_dir / "current_task.json"
    f.touch()

    with patch("aw_watcher_pipeline_stage.watcher.Observer") as mock_observer_cls:
        # First attempt: Constructor raises OSError
        # Second attempt: Constructor succeeds
        obs_success = MagicMock()
        obs_success.start.return_value = None
        obs_success.is_alive.return_value = True

        mock_observer_cls.side_effect = [OSError("Inotify limit reached"), obs_success]

        watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)

        with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
            with patch("time.sleep"):
                watcher.start()

            # Should have logged error for first failure
            assert mock_logger.error.call_count >= 1
            assert "Failed to start observer" in str(mock_logger.error.call_args_list[0])

            # Should have called constructor twice
            assert mock_observer_cls.call_count == 2

            # Watcher should hold the working observer
            assert watcher._observer is obs_success
            obs_success.start.assert_called_once()

    watcher.stop()


def test_observer_subsequent_recovery(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that subsequent access to observer property triggers new restart attempts."""
    f = temp_dir / "current_task.json"
    f.touch()

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    # Kill observer
    watcher.observer.stop()
    watcher.observer.join()

    with patch("aw_watcher_pipeline_stage.watcher.Observer") as mock_observer_cls:
        # 1. First access: Fails 3 times (persistent failure)
        obs_fail = MagicMock()
        obs_fail.start.side_effect = OSError("Fail")
        obs_fail.is_alive.return_value = False

        # 2. Second access: Succeeds
        obs_success = MagicMock()
        obs_success.start.return_value = None
        obs_success.is_alive.side_effect = [False, True, True]

        # Sequence:
        # Access 1: _start_observer calls Observer() -> obs_fail.
        #           Retries 3 times.
        # Access 2: _start_observer calls Observer() -> obs_success.
        mock_observer_cls.side_effect = [obs_fail, obs_fail, obs_fail, obs_success]

        with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
            # Mock time to bypass cooldown
            with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", side_effect=[100.0, 200.0]):
                # First access - fails
                obs = watcher.observer
                assert not obs.is_alive()
                assert obs_fail.start.call_count == 3

                # Second access - succeeds
                # We simulated time passing (100.0 -> 200.0) so cooldown of 10s is satisfied
                obs = watcher.observer
                assert obs.is_alive()
                assert obs_success.start.call_count == 1

    watcher.stop()


def test_observer_join_before_start_recovery(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test recovery when observer fails to schedule and cleanup of unstarted observer is needed."""
    f = temp_dir / "current_task.json"
    f.touch()

    with patch("aw_watcher_pipeline_stage.watcher.Observer") as mock_observer_cls:
        # 1. First attempt: schedule fails
        obs1 = MagicMock()
        obs1.schedule.side_effect = OSError("No space")
        obs1.is_alive.return_value = False
        # Mock join to raise RuntimeError if called on unstarted thread
        obs1.join.side_effect = RuntimeError("cannot join thread before it is started")

        # 2. Second attempt: succeeds
        obs2 = MagicMock()
        obs2.schedule.return_value = None
        obs2.is_alive.side_effect = [False, True, True]

        mock_observer_cls.side_effect = [obs1, obs2]

        watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)

        with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
            with patch("time.sleep"):
                watcher.start()

            # Verify obs1.join was called and exception swallowed
            obs1.join.assert_called()

            # Verify obs2 started
            obs2.start.assert_called_once()
            assert watcher.observer is obs2

    watcher.stop()


def test_restart_fails_missing_directory(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that observer restart fails gracefully if directory is missing."""
    # Create a subdirectory to watch so we can delete it
    watch_dir = temp_dir / "subdir"
    watch_dir.mkdir()
    f = watch_dir / "current_task.json"
    f.touch()

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    assert watcher.observer.is_alive()

    # Kill observer
    watcher.observer.stop()
    watcher.observer.join()

    # Delete directory
    shutil.rmtree(watch_dir)

    # Trigger restart
    with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
        with patch("time.sleep"):  # Skip sleep delays
            obs = watcher.observer

            # Should have logged errors
            assert mock_logger.error.call_count >= 1
            assert "Could not start observer" in str(mock_logger.error.call_args_list[-1])

            # Observer should be present but not alive
            assert not obs.is_alive()

    watcher.stop()


def test_recovery_scenario_via_heartbeat_check(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """
    Scenario:
    1. Watcher running with state.
    2. Observer dies.
    3. Main loop calls check_periodic_heartbeat.
    4. Observer restarts.
    5. State is preserved.
    """
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "S1", "current_task": "T1"}))

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.1)
    watcher.start()

    # 1. Initial state
    time.sleep(0.2)
    assert watcher.handler.last_stage == "S1"
    original_observer = watcher.observer
    assert original_observer.is_alive()

    # 2. Kill observer
    original_observer.stop()
    original_observer.join()
    assert not original_observer.is_alive()

    # 3. Trigger check (simulating main loop)
    with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
        watcher.check_health()
        
        # Verify log
        assert any("Watchdog observer found dead" in str(c) for c in mock_logger.error.call_args_list)

    # 4. Verify restart
    new_observer = watcher.observer
    assert new_observer is not original_observer
    assert new_observer.is_alive()

    # 5. Verify state preserved (handler instance is same)
    assert watcher.handler.last_stage == "S1"
    
    # Verify we can still process events
    mock_aw_client.heartbeat.reset_mock()
    f.write_text(json.dumps({"current_stage": "S2", "current_task": "T2"}))
    time.sleep(0.5) # Wait for debounce/processing
    
    assert watcher.handler.last_stage == "S2"
    mock_aw_client.heartbeat.assert_called_once()
    
    watcher.stop()


def test_restart_after_filesystem_error_via_heartbeat(
    pipeline_client: PipelineClient, temp_dir: Path
) -> None:
    """Test observer restart recovery after filesystem error during restart, triggered by heartbeat check."""
    f = temp_dir / "current_task.json"
    f.touch()

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()
    
    # Kill it
    watcher.observer.stop()
    watcher.observer.join()

    # Mock Observer to fail once then succeed
    with patch("aw_watcher_pipeline_stage.watcher.Observer") as mock_observer_cls:
        obs_fail = MagicMock()
        obs_fail.start.side_effect = OSError("Filesystem error")
        obs_fail.is_alive.return_value = False

        obs_success = MagicMock()
        obs_success.start.return_value = None
        obs_success.is_alive.side_effect = [False, True, True]

        mock_observer_cls.side_effect = [obs_fail, obs_success]

        with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
            with patch("time.sleep"):
                # Trigger via heartbeat check
                watcher.check_health()

                # Should have logged error
                assert any("Failed to start observer" in str(c) for c in mock_logger.error.call_args_list)
                
                # Should have eventually succeeded
                assert watcher.observer.is_alive()

    watcher.stop()


def test_repeated_start_stop_stability(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that starting and stopping the watcher repeatedly works correctly."""
    f = temp_dir / "current_task.json"
    f.touch()

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)

    for _ in range(3):
        watcher.start()
        assert watcher.observer.is_alive()
        assert watcher._started
        assert not watcher._stopping

        watcher.stop()
        assert not watcher.observer.is_alive()
        assert watcher._stopping

    watcher.stop()


def test_stability_periodic_checks(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Verify stability over many periodic checks (simulated long run)."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Stable", "current_task": "Check"}))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    handler.on_state_changed(handler._parse_file(str(f)), 0.0)
    mock_aw_client.heartbeat.reset_mock()

    # Simulate 1000 periodic checks (approx 8 hours if every 30s)
    start_time = time.time()

    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic") as mock_time:
        current_time = start_time
        mock_time.return_value = current_time

        # Sync handler time
        handler.last_heartbeat_time = current_time

        for i in range(1000):
            # Advance time
            current_time += 31.0
            mock_time.return_value = current_time

            handler._periodic_heartbeat_task()

            assert mock_aw_client.heartbeat.call_count == i + 1
            assert handler.last_heartbeat_time == current_time


def test_long_run_state_stability(
    pipeline_client: PipelineClient, temp_dir: Path
) -> None:
    """Simulate a long run and verify state size stability (proxy for memory leaks)."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Start"}))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)

    # Initial state size check (number of keys in current_data)
    initial_keys = len(handler.current_data)

    # Simulate 1000 iterations of updates and heartbeats
    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic") as mock_time:
        start_time = 1000.0
        mock_time.return_value = start_time

        for i in range(1000):
            # Advance time
            current_time = start_time + (i * 30.0)
            mock_time.return_value = current_time

            # Update file every 10 iterations
            if i % 10 == 0:
                f.write_text(json.dumps({
                    "current_stage": f"Stage {i}",
                    "current_task": f"Task {i}",
                    "metadata": {"iter": i}
                }))
                data = handler._process_state_change(handler._read_file_data(str(f)), str(f))
                handler.on_state_changed(data, 0.0)

            # Check periodic
            handler._periodic_heartbeat_task()

            # Verify state hasn't exploded
            # current_data should strictly contain the keys from the last update
            # It shouldn't accumulate history. Allow small variance for metadata/file_path.
            assert len(handler.current_data) <= initial_keys + 2

    # Verify handler attributes are stable
    assert isinstance(handler.current_data, dict)
    assert len(handler.current_data) > 0


def test_thread_leak_simulation(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that rapid updates do not leak threads."""
    f = temp_dir / "current_task.json"
    f.touch()

    # Use a real debounce to trigger Timer creation
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.01)
    
    # Baseline threads
    start_threads = threading.active_count()
    
    # Trigger many events
    for i in range(50):
        event = MagicMock()
        event.is_directory = False
        event.src_path = str(f)
        handler.on_modified(event)
        # Sleep slightly less than debounce to trigger cancel/restart logic
        time.sleep(0.002)
            
    # Wait for final debounce to settle
    time.sleep(0.1)
    
    end_threads = threading.active_count()
    
    handler.stop()
    
    # Should be roughly same number of threads (allow small variance)
    assert end_threads <= start_threads + 2


def test_watcher_long_running_simulation(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Simulate a long running watcher session (e.g. 60 mins) to verify stability."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))

    # Mock time to control the simulation
    start_time = 1000000.0
    current_time = start_time

    # We need to patch time.monotonic in the watcher module
    # Also patch Observer so we don't have real threads interfering
    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", side_effect=lambda: current_time):
        with patch("aw_watcher_pipeline_stage.watcher.Observer") as mock_observer_cls:
            mock_observer = mock_observer_cls.return_value
            mock_observer.is_alive.return_value = True

            watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.1)
            watcher.start()
            
            # Initial state
            # Manually trigger parse since observer is mocked
            watcher.handler.on_state_changed(watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f)), 0.0)
            
            assert watcher.handler.last_stage == "Init"
            mock_aw_client.heartbeat.assert_called()
            mock_aw_client.heartbeat.reset_mock()

            # Simulate 60 minutes (3600 seconds)
            for i in range(3600):
                current_time += 1.0
                
                # Every 30s, a heartbeat should be sent
                watcher.handler._periodic_heartbeat_task()
                
                # Every 5 minutes (300s), change the file
                if i > 0 and i % 300 == 0:
                    f.write_text(json.dumps({
                        "current_stage": "Running",
                        "current_task": f"Task {i}",
                        "status": "in_progress"
                    }))
                    # Manually trigger parse
                    watcher.handler.on_state_changed(watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f)), 0.0)

            watcher.stop()

            # Verification
            # 60 mins = 3600s.
            # Periodic heartbeats: roughly 3600/30 = 120.
            # File changes: 3600/300 = 12 (at 300, 600, ... 3300).
            # Each file change triggers immediate heartbeat.
            # Total heartbeats should be roughly 120 + 12 = 132.
            # Allow some margin.
            assert 125 <= mock_aw_client.heartbeat.call_count <= 140
            
            # Verify no memory leak in state (should be small constant size)
            assert len(watcher.handler.current_data) < 20

            # Verify statistics match
            stats = watcher.get_statistics()
            # Initial heartbeat (1) + loop heartbeats (call_count)
            assert stats["heartbeats_sent"] == mock_aw_client.heartbeat.call_count + 1
            assert stats["uptime"] >= 3600.0


def test_resource_usage_logging() -> None:
    """Test the resource usage logging function."""
    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            # Setup mock return values
            mock_usage = MagicMock()
            mock_usage.ru_maxrss = 50 * 1024  # 50 MB (assuming Linux KB)
            mock_usage.ru_utime = 10.0
            mock_usage.ru_stime = 5.0
            mock_resource.getrusage.return_value = mock_usage
            mock_resource.RUSAGE_SELF = 0

            # Import module to test
            import aw_watcher_pipeline_stage.main as main_mod
            
            # Reset globals
            main_mod._last_rusage = None
            main_mod._last_rusage_time = 0.0
            main_mod._last_info_log_time = 0.0

            # 1. First call
            with patch("time.monotonic", return_value=1000.0):
                mock_logger.isEnabledFor.return_value = True
                main_mod.log_resource_usage()
                
                # Should log INFO on first call (initial status)
                mock_logger.info.assert_called()
                assert "Resource Usage" in mock_logger.info.call_args[0][0]
                assert "PID=" in mock_logger.info.call_args[0][0]
                assert "Threads=" in mock_logger.info.call_args[0][0]
                assert "Target <1%" in mock_logger.info.call_args[0][0]

            # 2. Second call shortly after (DEBUG)
            with patch("time.monotonic", return_value=1002.0):
                main_mod.log_resource_usage()
                mock_logger.debug.assert_called()
                # Should not log INFO again so soon
                assert mock_logger.info.call_count == 1

            # 3. High memory warning
            mock_usage_high = MagicMock()
            mock_usage_high.ru_maxrss = 150 * 1024 # 150 MB
            mock_resource.getrusage.return_value = mock_usage_high
            
            with patch("time.monotonic", return_value=1002.0):
                main_mod.log_resource_usage()
                mock_logger.warning.assert_called()
                assert "High resource usage" in mock_logger.warning.call_args[0][0]

            # 4. High CPU warning
            # Reset globals to simulate a new interval
            main_mod._last_rusage = mock_usage
            main_mod._last_rusage_time = 1000.0
            
            mock_usage_cpu = MagicMock()
            mock_usage_cpu.ru_maxrss = 50 * 1024
            # Delta time = 2.0s, Delta CPU = 0.4s (20%)
            mock_usage_cpu.ru_utime = 10.4
            mock_usage_cpu.ru_stime = 5.0
            mock_resource.getrusage.return_value = mock_usage_cpu
            
            with patch("time.monotonic", return_value=1002.0):
                mock_logger.warning.reset_mock()
                main_mod.log_resource_usage()
                mock_logger.warning.assert_called()
                assert "High resource usage" in mock_logger.warning.call_args[0][0]


def test_resource_usage_normal_behavior() -> None:
    """Test that normal resource usage (<1% CPU) does not trigger warnings."""
    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            import aw_watcher_pipeline_stage.main as main_mod
            
            # Setup baseline
            mock_usage_1 = MagicMock()
            mock_usage_1.ru_maxrss = 20 * 1024 # 20 MB
            mock_usage_1.ru_utime = 10.0
            mock_usage_1.ru_stime = 5.0
            mock_resource.getrusage.return_value = mock_usage_1
            mock_resource.RUSAGE_SELF = 0
            
            main_mod._last_rusage = None
            main_mod._last_rusage_time = 0.0
            
            # 1. Initial call
            with patch("time.monotonic", return_value=1000.0):
                main_mod.log_resource_usage()
            
            # 2. Second call after 60s with low CPU usage
            # Delta time = 60s
            # CPU used = 0.05s (approx 0.08%)
            mock_usage_2 = MagicMock()
            mock_usage_2.ru_maxrss = 20 * 1024
            mock_usage_2.ru_utime = 10.05
            mock_usage_2.ru_stime = 5.0
            mock_resource.getrusage.return_value = mock_usage_2
            
            mock_logger.isEnabledFor.return_value = True
            with patch("time.monotonic", return_value=1060.0):
                main_mod.log_resource_usage()
                
                # Should NOT log warning
                mock_logger.warning.assert_not_called()
                # Should log debug (since it's not 5 min yet)
                mock_logger.debug.assert_called()
                args = mock_logger.debug.call_args[0][0]
                assert "CPU=" in args


def test_stability_simulated_load(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test stability when heartbeat sending is slow (simulating network load)."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Load", "current_task": "Test"}))

    # Use 0 debounce to trigger immediate sends
    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.0)
    watcher.start()

    # Mock send_heartbeat to be slow
    with patch.object(pipeline_client, "send_heartbeat", side_effect=lambda **kwargs: time.sleep(0.1)) as mock_send:
        try:
            # Trigger multiple updates
            for i in range(5):
                f.write_text(json.dumps({"current_stage": "Load", "current_task": f"Step {i}"}))
                watcher.handler.on_state_changed(watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f)), 0.0)
                # No sleep, blast updates
            
            # Wait for processing (0.1s per send * 5 = 0.5s) + buffer
            time.sleep(1.0)
            
            # Verify calls were made (should be 5 + initial 1 = 6)
            assert mock_send.call_count >= 5
            
            # Verify watcher is still alive
            assert watcher.observer.is_alive()
            
        finally:
            watcher.stop()


def test_memory_stability_simulation(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Verify that state does not grow indefinitely over many updates (memory leak check)."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    # Simulate 100 updates with different metadata keys
    for i in range(100):
        data = {"current_stage": "Run", "current_task": "Test", "metadata": {f"key_{i}": "value"}}
        f.write_text(json.dumps(data))
        handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)
        
        # Check that current_data only has the keys from the LATEST update
        assert f"key_{i}" in handler.current_data["metadata"]
        if i > 0:
            assert f"key_{i-1}" not in handler.current_data["metadata"]
            
    # Verify overall size is small (no accumulation)
    assert len(handler.current_data) < 10


def test_metrics_tracking(pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock) -> None:
    """Test that internal metrics (events, heartbeats) are tracked correctly."""
    f = temp_dir / "current_task.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    # Trigger event
    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)
    
    handler.on_modified(event)
    assert handler.events_detected == 1
    assert handler.last_event_time > 0
    
    # Trigger heartbeat
    f.write_text(json.dumps({"current_stage": "S", "current_task": "T"}))
    handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)
    assert handler.heartbeats_sent == 1
    
    # Trigger debounce metric update
    handler._debounce_counter = 5
    # Mock timer identity to allow wrapper execution
    handler._timer = threading.current_thread()  # type: ignore
    handler._parse_file_wrapper(str(f))

    stats = handler.get_statistics()
    assert stats["events_detected"] == 1
    assert stats["heartbeats_sent"] == 1
    assert stats["total_debounced_events"] == 5
    assert "last_heartbeat_time" in stats
    assert "last_event_time" in stats
    assert "state_keys" in stats
    assert "processing_latency" in stats


def test_parse_file_too_large(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    f = temp_dir / "current_task.json"
    f.touch()

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Mock stat to return large size
    with patch.object(Path, "stat") as mock_stat:
        mock_stat.return_value.st_size = MAX_FILE_SIZE_BYTES + 1024

        with patch.object(handler.logger, "warning") as mock_warn:
            handler._read_file_data(str(f))

            mock_warn.assert_called_once()
            assert "too large" in mock_warn.call_args[0][0]

    assert handler.last_stage is None


def test_stability_continuous_updates(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Simulate continuous updates to verify debounce and resource stability."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))

    # 100ms debounce
    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.1)
    # We don't start the real observer to avoid race conditions with manual triggers

    try:
        # Simulate 50 updates in 0.5 second (10ms interval)
        for i in range(50):
            f.write_text(json.dumps({"current_stage": "Stress", "current_task": f"Update {i}"}))

            # Manually trigger event
            event = MagicMock()
            event.is_directory = False
            event.src_path = str(f)
            watcher.handler.on_modified(event)

            time.sleep(0.01)

        # Wait for final debounce
        time.sleep(0.5)

        # Should be well below 50 heartbeats (expecting ~1-5 depending on timing)
        assert mock_aw_client.heartbeat.call_count < 10

        # Verify final state
        assert watcher.handler.last_task == "Update 49"

    finally:
        watcher.stop()

def test_comprehensive_stability(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """
    Simulate a comprehensive 1-hour run with mixed events to verify stability.
    """
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))

    # Use short debounce for simulation
    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.1)
    
    # Mock time.monotonic
    start_time = 1000.0
    current_time = start_time
    
    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", side_effect=lambda: current_time):
        with patch("aw_watcher_pipeline_stage.watcher.Observer") as mock_observer_cls:
            mock_observer = mock_observer_cls.return_value
            mock_observer.is_alive.return_value = True
            
            watcher.start()
            
            # Initial parse
            watcher.handler.on_state_changed(watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f)), 0.0)
            mock_aw_client.heartbeat.reset_mock()
            
            # Simulate 1 hour (3600s)
            for i in range(3600):
                current_time += 1.0
                
                # Periodic check
                watcher.handler._periodic_heartbeat_task()
                
                # Events at specific times
                if i == 600: # 10 mins
                    f.write_text(json.dumps({"current_stage": "Dev", "current_task": "Coding"}))
                    watcher.handler.on_state_changed(watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f)), 0.0)
                elif i == 1200: # 20 mins - Malformed
                    f.write_text("{ broken")
                    watcher.handler._read_file_data(str(f))
                elif i == 1800: # 30 mins - Recovery
                    f.write_text(json.dumps({"current_stage": "Dev", "current_task": "Fixing"}))
                    watcher.handler.on_state_changed(watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f)), 0.0)
                elif i == 2400: # 40 mins - Delete
                    if f.exists(): f.unlink()
                    event = MagicMock()
                    event.is_directory = False
                    event.src_path = str(f)
                    watcher.handler.on_deleted(event)
                elif i == 3000: # 50 mins - Recreate
                    f.write_text(json.dumps({"current_stage": "Dev", "current_task": "Done"}))
                    watcher.handler.on_state_changed(watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f)), 0.0)

            watcher.stop()
            
            # Verify heartbeats
            # Periodics: roughly 3600/30 = 120
            # Events: Init(1) + Dev(1) + Recovery(1) + Delete(1) + Recreate(1) = 5
            # Total ~ 125
            assert 110 < mock_aw_client.heartbeat.call_count < 140
            
            # Verify statistics consistency
            stats = watcher.get_statistics()
            assert stats["heartbeats_sent"] == mock_aw_client.heartbeat.call_count
            assert stats["events_detected"] >= 5
            assert stats["parse_errors"] >= 1  # From the malformed JSON step
            
            # Verify state
            assert watcher.handler.last_stage == "Dev"
            assert watcher.handler.last_task == "Done"


def test_long_running_stability_simulation_24h(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Simulate a 24-hour run to verify long-term stability."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))

    # Mock time
    start_time = 1000000.0
    current_time = start_time

    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", side_effect=lambda: current_time):
        with patch("aw_watcher_pipeline_stage.watcher.Observer") as mock_observer_cls:
            mock_observer = mock_observer_cls.return_value
            mock_observer.is_alive.return_value = True

            watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.1)
            watcher.start()
            
            # Initial parse
            watcher.handler.on_state_changed(watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f)), 0.0)
            mock_aw_client.heartbeat.reset_mock()
            
            # Simulate 24 hours (86400s)
            # We simulate checking every 30s
            steps = 86400 // 30
            for i in range(steps):
                current_time += 30.0
                watcher.handler._periodic_heartbeat_task()
                
                # Every hour (120 steps), change file
                if i % 120 == 0:
                    f.write_text(json.dumps({
                        "current_stage": "Running",
                        "current_task": f"Hour {i // 120}",
                        "status": "in_progress"
                    }))
                    watcher.handler.on_state_changed(watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f)), 0.0)

            watcher.stop()
            
            # Verify heartbeats
            # Periodics: steps (2880)
            # Changes: 24
            # Total approx 2900
            assert 2800 <= mock_aw_client.heartbeat.call_count <= 3000
            
            # Verify state size is stable
            assert len(watcher.handler.current_data) < 20


def test_heartbeat_interval_consistency(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Verify that heartbeats are sent with consistent intervals over time."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Consistency", "current_task": "Test"}))

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    # Initial parse
    watcher.handler.on_state_changed(watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f)), 0.0)
    mock_aw_client.heartbeat.reset_mock()

    # Mock time
    start_time = 1000.0
    current_time = start_time
    watcher.handler.last_heartbeat_time = start_time

    timestamps = []

    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", side_effect=lambda: current_time):
        # Run for 300 seconds (5 minutes)
        for _ in range(300):
            current_time += 1.0
            watcher.handler._periodic_heartbeat_task()
            if mock_aw_client.heartbeat.called:
                timestamps.append(current_time)
                mock_aw_client.heartbeat.reset_mock()

    watcher.stop()

    # Verify intervals are roughly 30s
    # We expect timestamps at 1030, 1060, 1090... (approx 10 heartbeats)
    assert len(timestamps) >= 9

    for i in range(1, len(timestamps)):
        interval = timestamps[i] - timestamps[i - 1]
        # Should be exactly 30s given our simulation step of 1s and logic >= 30.0
        assert 29.0 <= interval <= 31.0


def test_fuzz_stability(pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock) -> None:
    """Fuzz testing with random data to ensure stability."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.01)
    watcher.start()

    try:
        # Run for a short burst of intense fuzzing
        for i in range(50):
            # Generate random data
            stage = ''.join(random.choices(string.ascii_letters, k=10))
            task = ''.join(random.choices(string.ascii_letters, k=20))
            metadata = {
                ''.join(random.choices(string.ascii_letters, k=5)): ''.join(random.choices(string.ascii_letters, k=5))
                for _ in range(random.randint(0, 5))
            }
            
            data = {
                "current_stage": stage,
                "current_task": task,
                "metadata": metadata,
                "status": random.choice(["in_progress", "paused", "completed", None])
            }
            
            # Occasionally write malformed JSON
            if random.random() < 0.1:
                f.write_text("{ broken json")
            else:
                f.write_text(json.dumps(data))
                
            # Trigger parse manually to speed up test (bypassing observer wait)
            data = watcher.handler._read_file_data(str(f))
            if data: data = watcher.handler._process_state_change(data, str(f))
            if data: watcher.handler.on_state_changed(data, 0.0)
            
        # Verify it didn't crash and stats are reasonable
        stats = watcher.get_statistics()
        assert stats["parse_errors"] >= 0
        assert stats["heartbeats_sent"] > 0
        assert stats["uptime"] >= 0.0
    finally:
        watcher.stop()

def test_resource_usage_logging_no_resource_module() -> None:
    """Test log_resource_usage when resource module is not available."""
    with patch("aw_watcher_pipeline_stage.main.resource", None):
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            import aw_watcher_pipeline_stage.main as main_mod
            
            mock_logger.isEnabledFor.return_value = True
            main_mod.log_resource_usage()
            
            mock_logger.debug.assert_called()
            assert "not available" in mock_logger.debug.call_args[0][0]


def test_stop_without_start(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that stop() can be called safely without start()."""
    f = temp_dir / "current_task.json"
    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    
    # Should not raise
    watcher.stop()
    assert watcher._stopping


def test_rapid_cycling_stability(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test stability under rapid start/stop and file event cycling."""
    f = temp_dir / "current_task.json"
    f.touch()

    # Use very short debounce to stress timer creation/cancellation
    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.01)

    try:
        for i in range(10):
            watcher.start()
            f.write_text(json.dumps({"current_stage": f"Cycle {i}", "current_task": "Test"}))
            # Give it a tiny bit of time to spawn threads/timers
            time.sleep(0.05)
            watcher.stop()

        # Should not have crashed and threads should be cleaned up
        assert not watcher.observer.is_alive()
        assert watcher.handler._timer is None or not watcher.handler._timer.is_alive()
    finally:
        if watcher.observer.is_alive():
            watcher.stop()


def test_processing_latency_tracking(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that processing latency is tracked."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "S", "current_task": "T"}))
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    # Mock time to simulate duration
    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", side_effect=[100.0, 100.1]):
        handler._parse_file(str(f))
        
    assert handler.processing_latency >= 0.099  # Approx 0.1
    assert handler.max_processing_latency >= 0.099
    stats = handler.get_statistics()
    assert stats["processing_latency"] >= 0.099
    assert stats["max_processing_latency"] >= 0.099


def test_heartbeat_latency_tracking(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that heartbeat latency is tracked."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "S", "current_task": "T"}))
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    # Mock client.send_heartbeat to simulate delay
    with patch.object(pipeline_client, "send_heartbeat", side_effect=lambda **kwargs: time.sleep(0.1)):
        handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)
        
    assert handler.heartbeat_latency >= 0.09
    assert handler.max_heartbeat_latency >= 0.09


def test_resource_usage_logging_with_watcher_stats() -> None:
    """Test that resource usage logging includes watcher statistics."""
    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            # Setup mock return values
            mock_usage = MagicMock()
            mock_usage.ru_maxrss = 50 * 1024  # 50 MB
            mock_usage.ru_utime = 10.0
            mock_usage.ru_stime = 5.0
            mock_resource.getrusage.return_value = mock_usage
            mock_resource.RUSAGE_SELF = 0

            import aw_watcher_pipeline_stage.main as main_mod
            
            # Reset globals
            main_mod._last_rusage = None
            main_mod._last_rusage_time = 0.0
            main_mod._last_info_log_time = 0.0

            # Mock watcher
            mock_watcher = MagicMock()
            mock_watcher.get_statistics.return_value = {
                "events_detected": 42,
                "heartbeats_sent": 10,
                "total_debounced_events": 5,
                "parse_errors": 2,
                "last_error_time": 950.0,
                "last_heartbeat_time": 970.0,  # 30s ago
                "last_event_time": 990.0,      # 10s ago
                "state_keys": 5,
                "uptime": 120.0,
                "processing_latency": 0.123,
                "max_processing_latency": 0.456,
                "heartbeat_latency": 0.050,
                "max_heartbeat_latency": 0.100
            }

            with patch("time.monotonic", return_value=1000.0):
                mock_logger.isEnabledFor.return_value = True
                main_mod.log_resource_usage(mock_watcher)
                
                # Should log INFO on first call
                mock_logger.info.assert_called()
                log_msg = mock_logger.info.call_args[0][0]
                
                assert "Resource Usage" in log_msg
                assert "Events=42" in log_msg
                assert "Keys=" in log_msg
                assert "Heartbeats=10" in log_msg
                assert "Debounced=5" in log_msg
                assert "Errors=2" in log_msg
                assert "LastErr=50.0s" in log_msg
                assert "LastHB=30.0s" in log_msg
                assert "LastEvt=10.0s" in log_msg
                assert "Uptime=00:02:00" in log_msg
                assert "Latency=0.123s (Max=0.456s)" in log_msg
                assert "HBLatency=0.050s (Max=0.100s)" in log_msg


def test_long_running_resource_usage_sequence() -> None:
    """Test resource usage logging frequency over a simulated long run."""
    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            import aw_watcher_pipeline_stage.main as main_mod
            
            # Reset globals
            main_mod._last_rusage = None
            main_mod._last_rusage_time = 0.0
            main_mod._last_info_log_time = 0.0
            
            mock_usage = MagicMock()
            mock_usage.ru_maxrss = 50 * 1024
            mock_usage.ru_utime = 0.0
            mock_usage.ru_stime = 0.0
            mock_resource.getrusage.return_value = mock_usage
            mock_resource.RUSAGE_SELF = 0
            
            mock_logger.isEnabledFor.return_value = True
            
            # Simulate 20 minutes (1200s) with checks every 60s
            start_time = 1000.0
            
            with patch("time.monotonic") as mock_time:
                for i in range(21): # 0 to 20 minutes
                    current_time = start_time + (i * 60.0)
                    mock_time.return_value = current_time
                    
                    # Update CPU time slightly to avoid 0 division or static
                    mock_usage.ru_utime += 0.01
                    
                    main_mod.log_resource_usage()
                    
                # Count INFO calls: 0m, 5m, 10m, 15m, 20m -> 5 calls
                assert mock_logger.info.call_count == 5


def test_metrics_tracking_errors(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that parse errors are tracked in statistics."""
    f = temp_dir / "current_task.json"
    f.write_text("{ invalid json")
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    with patch.object(handler.logger, "error"):
        handler._parse_file(str(f))
    
    stats = handler.get_statistics()
    assert stats["parse_errors"] == 1


def test_long_running_stability_with_resource_checks(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Simulate a long run and verify resource logging and heartbeat consistency."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    # Mock resource module
    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            import aw_watcher_pipeline_stage.main as main_mod
            
            # Setup mock resource usage
            mock_usage = MagicMock()
            mock_usage.ru_maxrss = 50 * 1024
            mock_usage.ru_utime = 10.0
            mock_usage.ru_stime = 5.0
            mock_resource.getrusage.return_value = mock_usage
            mock_resource.RUSAGE_SELF = 0
            
            mock_logger.isEnabledFor.return_value = True
            
            # Reset globals
            main_mod._last_rusage = None
            main_mod._last_rusage_time = 0.0
            main_mod._last_info_log_time = 0.0

            # Simulate 30 minutes (1800s)
            start_time = 1000.0
            current_time = start_time
            
            with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", side_effect=lambda: current_time):
                with patch("aw_watcher_pipeline_stage.main.time.monotonic", side_effect=lambda: current_time):
                    
                    # Initial parse
                    watcher.handler.on_state_changed(watcher.handler._parse_file(str(f)), 0.0)
                    
                    # Loop
                    for i in range(1800):
                        current_time += 1.0
                        
                        # Watcher periodic check
                        watcher.handler._periodic_heartbeat_task()
                        
                        # Main loop resource check (every 60s)
                        if i % 60 == 0:
                            main_mod.log_resource_usage(watcher)
                            
            # Verify heartbeats sent (approx 60)
            assert mock_aw_client.heartbeat.call_count >= 58
            
            # Verify max heartbeat interval is around 30s (consistency check)
            stats = watcher.get_statistics()
            assert 29.0 <= stats["max_heartbeat_interval"] <= 31.0
            
            # Verify resource logging happened
            assert mock_logger.info.call_count >= 1
            
    watcher.stop()


def test_periodic_heartbeat_timing_accuracy(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Verify heartbeats are sent exactly every 30s (within tolerance) during a loop."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Timing", "current_task": "Test"}))

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    # Initial parse
    watcher.handler.on_state_changed(watcher.handler._parse_file(str(f)), 0.0)
    mock_aw_client.heartbeat.reset_mock()

    # Sync times
    start_time = 1000.0
    current_time = start_time
    watcher.handler.last_heartbeat_time = start_time

    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", side_effect=lambda: current_time):
        # Simulate 300 seconds, stepping by 1s
        for i in range(1, 301):
            current_time += 1.0
            watcher.handler._periodic_heartbeat_task()

            # Expected heartbeats: at 30s, 60s, 90s...
            expected_heartbeats = i // 30
            assert mock_aw_client.heartbeat.call_count == expected_heartbeats

    watcher.stop()


def test_resource_usage_target_compliance() -> None:
    """Verify that resource usage logging correctly identifies compliance with <1% target."""
    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            import aw_watcher_pipeline_stage.main as main_mod

            # Setup baseline
            mock_usage = MagicMock()
            mock_usage.ru_maxrss = 20 * 1024
            mock_usage.ru_utime = 10.0
            mock_usage.ru_stime = 5.0
            mock_resource.getrusage.return_value = mock_usage
            mock_resource.RUSAGE_SELF = 0

            main_mod._last_rusage = None
            main_mod._last_rusage_time = 0.0

            # Init
            with patch("time.monotonic", return_value=1000.0):
                main_mod.log_resource_usage()

            # Case 1: <1% CPU (0.5s over 60s = 0.83%)
            mock_usage_low = MagicMock()
            mock_usage_low.ru_maxrss = 20 * 1024
            mock_usage_low.ru_utime = 10.4
            mock_usage_low.ru_stime = 5.1
            mock_resource.getrusage.return_value = mock_usage_low

            mock_logger.isEnabledFor.return_value = True
            with patch("time.monotonic", return_value=1060.0):
                main_mod.log_resource_usage()
                # Should be DEBUG or INFO (if 5m passed), NOT WARNING
                mock_logger.warning.assert_not_called()

            # Case 2: >10% CPU (7s over 60s = 11.6%)
            mock_usage_high = MagicMock()
            mock_usage_high.ru_maxrss = 20 * 1024
            mock_usage_high.ru_utime = 15.0
            mock_usage_high.ru_stime = 7.5  # +4.6 +2.4 = 7.0s
            mock_resource.getrusage.return_value = mock_usage_high

            # Reset time tracking for info log to ensure warning isn't blocked (it isn't, warning has priority)
            with patch("time.monotonic", return_value=1120.0):
                main_mod.log_resource_usage()
                mock_logger.warning.assert_called()
                assert "High resource usage" in mock_logger.warning.call_args[0][0]


def test_long_running_stability_simulation_with_anomalies(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Simulate a long run with resource anomalies to verify logging resilience."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            import aw_watcher_pipeline_stage.main as main_mod

            # Setup baseline
            mock_usage = MagicMock()
            mock_usage.ru_maxrss = 50 * 1024
            mock_usage.ru_utime = 10.0
            mock_usage.ru_stime = 5.0
            mock_resource.getrusage.return_value = mock_usage
            mock_resource.RUSAGE_SELF = 0

            mock_logger.isEnabledFor.return_value = True
            main_mod._last_rusage = None
            main_mod._last_rusage_time = 0.0

            start_time = 1000.0
            current_time = start_time

            with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", side_effect=lambda: current_time):
                with patch("aw_watcher_pipeline_stage.main.time.monotonic", side_effect=lambda: current_time):

                    # Loop for 10 minutes
                    for i in range(600):
                        current_time += 1.0
                        watcher.handler._periodic_heartbeat_task()

                        if i % 60 == 0:
                            # Inject anomaly at minute 5
                            if i == 300:
                                mock_usage.ru_maxrss = 200 * 1024  # 200MB
                            else:
                                mock_usage.ru_maxrss = 50 * 1024

                            main_mod.log_resource_usage(watcher)

            # Verify warning was called for anomaly
            assert mock_logger.warning.call_count >= 1
            assert "High resource usage" in str(mock_logger.warning.call_args_list)

    watcher.stop()


def test_shutdown_resource_logging_noise() -> None:
    """Test that rapid calls to log_resource_usage (e.g. shutdown) don't trigger CPU warnings."""
    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            import aw_watcher_pipeline_stage.main as main_mod
            
            # Setup baseline
            mock_usage = MagicMock()
            mock_usage.ru_maxrss = 20 * 1024
            mock_usage.ru_utime = 10.0
            mock_usage.ru_stime = 5.0
            mock_resource.getrusage.return_value = mock_usage
            mock_resource.RUSAGE_SELF = 0
            
            main_mod._last_rusage = None
            main_mod._last_rusage_time = 0.0
            
            # 1. Call at T=1000
            with patch("time.monotonic", return_value=1000.0):
                main_mod.log_resource_usage()
            
            # 2. Call at T=1000.5 (Shutdown shortly after)
            # CPU usage increased slightly (cleanup)
            mock_usage_2 = MagicMock()
            mock_usage_2.ru_maxrss = 20 * 1024
            mock_usage_2.ru_utime = 10.05 # +0.05s
            mock_usage_2.ru_stime = 5.0
            mock_resource.getrusage.return_value = mock_usage_2
            
            mock_logger.isEnabledFor.return_value = True
            
            # If we calculated CPU: (0.05 / 0.5) * 100 = 10% -> Warning
            # But with threshold > 1.0s, we should skip calculation/warning
            with patch("time.monotonic", return_value=1000.5):
                main_mod.log_resource_usage()
                
                # Should NOT log warning
                mock_logger.warning.assert_not_called()


def test_long_running_memory_stability_varying_content(
    pipeline_client: PipelineClient, temp_dir: Path
) -> None:
    """Verify memory stability when file content size varies significantly."""
    f = temp_dir / "current_task.json"
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    # Simulate 100 updates with varying metadata size
    for i in range(100):
        # Create metadata of varying size
        metadata_size = (i % 10) * 100 # 0 to 900 chars
        metadata = {"payload": "x" * metadata_size}
        
        data = {
            "current_stage": "Stability",
            "current_task": f"Task {i}",
            "metadata": metadata
        }
        f.write_text(json.dumps(data))
        handler._process_state_change(handler._read_file_data(str(f)), str(f))
        
        # Verify current_data holds only current state
        assert len(handler.current_data["metadata"]["payload"]) == metadata_size
        
    # Final check
    assert len(handler.current_data) < 10


def test_slow_processing_warning(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that slow processing triggers a warning."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "S", "current_task": "T"}))
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    # Mock time to simulate 1.5s duration
    # Sequence: start_proc, (change detected), (send heartbeat), (latency calc)
    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", side_effect=[100.0, 101.5]):
        with patch.object(handler.logger, "warning") as mock_warn:
            handler._read_file_data(str(f))
            
            mock_warn.assert_called()
            assert "Slow processing detected" in mock_warn.call_args[0][0]


def test_compliance_long_running_stability(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """
    Explicit compliance test for Directive 6:
    - Run for 30+ minutes (simulated)
    - Monitor CPU/memory usage (<1% idle implied by no warnings)
    - Heartbeat consistency
    - No memory leaks (stable state keys)
    - Stable operation under normal file changes
    """
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    # Mock resource usage to be low (compliant)
    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            import aw_watcher_pipeline_stage.main as main_mod
            
            mock_usage = MagicMock()
            mock_usage.ru_maxrss = 50 * 1024
            mock_usage.ru_utime = 10.0
            mock_usage.ru_stime = 5.0
            mock_resource.getrusage.return_value = mock_usage
            mock_resource.RUSAGE_SELF = 0
            
            mock_logger.isEnabledFor.return_value = True
            main_mod._last_rusage = None
            main_mod._last_rusage_time = 0.0

            start_time = 1000.0
            current_time = start_time
            
            with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", side_effect=lambda: current_time):
                with patch("aw_watcher_pipeline_stage.main.time.monotonic", side_effect=lambda: current_time):
                    
                    # Initial parse
                    watcher.handler.on_state_changed(watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f)), 0.0)
                    
                    # Simulate 35 minutes (2100s)
                    for i in range(2100):
                        current_time += 1.0
                        
                        # 1. Heartbeat consistency check (every 30s)
                        watcher.check_periodic_heartbeat()
                        
                        # 2. Resource monitoring (every 60s)
                        if i % 60 == 0:
                            # Update CPU time slightly to simulate <1% usage
                            # 1s elapsed, add 0.001s CPU -> 0.1% usage
                            mock_usage.ru_utime += 0.001
                            main_mod.log_resource_usage(watcher)
                            
                        # 3. Normal file changes (every 5 mins)
                        if i > 0 and i % 300 == 0:
                            f.write_text(json.dumps({
                                "current_stage": "Running",
                                "current_task": f"Minute {i // 60}",
                                "metadata": {"iteration": i}
                            }))
                            watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f))

            watcher.stop()

            # Verification
            
            # 1. Heartbeat consistency
            # 2100s / 30s = 70 periodics
            # + 7 changes (5, 10, 15, 20, 25, 30, 35 mins)
            # + 1 initial
            # Total approx 78
            assert 75 <= mock_aw_client.heartbeat.call_count <= 85
            
            # 2. No warnings (CPU < 1%, Memory stable)
            mock_logger.warning.assert_not_called()
            
            # 3. Info logs (every 5 mins) -> 7 calls (0, 5, 10, 15, 20, 25, 30, 35)
            assert mock_logger.info.call_count >= 7
            
            # 4. No memory leaks (state keys stable)
            # Should only have keys from last update
            assert len(watcher.handler.current_data) < 10
            assert watcher.handler.current_data["current_task"] == "Minute 35"

            # 5. Verify Stats Stability
            stats = watcher.get_statistics()
            # Uptime should be approx 2100s
            assert 2090.0 <= stats["uptime"] <= 2110.0
            # Latency should be tracked
            assert "processing_latency" in stats

    watcher.stop()


def test_statistics_accuracy(pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock) -> None:
    """Test that statistics accurately track intervals and latencies."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Stats", "current_task": "Test"}))

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    # Initial parse
    watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f))

    # Mock time to simulate intervals
    start_time = 1000.0
    current_time = start_time
    watcher.handler.last_heartbeat_time = start_time

    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", side_effect=lambda: current_time):
        # 1. Simulate 31s interval (Normal)
        current_time += 31.0
        watcher.handler._periodic_heartbeat_task()

        stats = watcher.get_statistics()
        assert 30.0 <= stats["max_heartbeat_interval"] <= 32.0

        # 2. Simulate 40s interval (Lag/Delay)
        current_time += 40.0
        watcher.handler._periodic_heartbeat_task()

        stats = watcher.get_statistics()
        assert 39.0 <= stats["max_heartbeat_interval"] <= 41.0

    watcher.stop()


def test_resource_usage_logging_real_integration(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test log_resource_usage with a real PipelineWatcher instance to ensure compatibility."""
    f = temp_dir / "current_task.json"
    f.touch()
    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)

    # Mock resource and logger
    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            import aw_watcher_pipeline_stage.main as main_mod

            mock_usage = MagicMock()
            mock_usage.ru_maxrss = 1024
            mock_usage.ru_utime = 1.0
            mock_usage.ru_stime = 1.0
            mock_resource.getrusage.return_value = mock_usage
            mock_resource.RUSAGE_SELF = 0

            mock_logger.isEnabledFor.return_value = True

            # Should not raise exception and should log info
            main_mod.log_resource_usage(watcher)

            mock_logger.info.assert_called()
            msg = mock_logger.info.call_args[0][0]
            # Verify some keys from watcher stats are present
            assert "Events=" in msg
            assert "Uptime=" in msg

    watcher.stop()


def test_resource_usage_logging_with_fds() -> None:
    """Test that resource usage logging includes FD count on supported systems."""
    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            with patch("os.path.exists", return_value=True):
                with patch("os.listdir", return_value=["0", "1", "2"]):
                    import aw_watcher_pipeline_stage.main as main_mod
                    
                    # Setup mocks
                    mock_usage = MagicMock()
                    mock_usage.ru_maxrss = 1024
                    mock_usage.ru_utime = 1.0
                    mock_usage.ru_stime = 1.0
                    mock_resource.getrusage.return_value = mock_usage
                    mock_resource.RUSAGE_SELF = 0
                    
                    mock_logger.isEnabledFor.return_value = True
                    
                    # Reset globals
                    main_mod._last_rusage = None
                    main_mod._last_rusage_time = 0.0
                    
                    with patch("time.monotonic", return_value=1000.0):
                        main_mod.log_resource_usage()
                    
                    mock_logger.info.assert_called()
                    msg = mock_logger.info.call_args[0][0]
                    assert "FDs=3" in msg


def test_resource_usage_logging_no_fds() -> None:
    """Test that resource usage logging handles missing /proc/self/fd gracefully."""
    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            with patch("os.path.exists", return_value=False):
                import aw_watcher_pipeline_stage.main as main_mod
                
                # Setup mocks
                mock_usage = MagicMock()
                mock_usage.ru_maxrss = 1024
                mock_usage.ru_utime = 1.0
                mock_usage.ru_stime = 1.0
                mock_resource.getrusage.return_value = mock_usage
                mock_resource.RUSAGE_SELF = 0
                
                mock_logger.isEnabledFor.return_value = True
                
                # Reset globals
                main_mod._last_rusage = None
                main_mod._last_rusage_time = 0.0
                
                with patch("time.monotonic", return_value=1000.0):
                    main_mod.log_resource_usage()
                
                mock_logger.info.assert_called()
                msg = mock_logger.info.call_args[0][0]
                assert "FDs=" not in msg


def test_atomic_write_pattern(pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock) -> None:
    """Test atomic write pattern (write to temp file, rename to target)."""
    target_file = temp_dir / "current_task.json"
    # Start with existing file
    target_file.write_text(json.dumps({"current_stage": "Initial", "current_task": "Task"}))
    
    watcher = PipelineWatcher(target_file, pipeline_client, pulsetime=120.0, debounce_seconds=0.1)
    watcher.start()
    
    try:
        # Initial read
        time.sleep(0.2)
        mock_aw_client.heartbeat.reset_mock()
        
        # Simulate atomic write
        temp_file = temp_dir / "temp_write.json"
        temp_file.write_text(json.dumps({"current_stage": "Atomic", "current_task": "Write"}))
        
        # Rename temp to target (atomic replace)
        # This triggers a MOVED_TO event for target_file (or MOVED_FROM for temp depending on observer)
        shutil.move(str(temp_file), str(target_file))
        
        # Wait for debounce
        time.sleep(0.5)
        
        # Verify update
        mock_aw_client.heartbeat.assert_called_once()
        assert mock_aw_client.heartbeat.call_args[0][1].data["stage"] == "Atomic"
        
    finally:
        watcher.stop()


def test_resource_usage_thread_anomaly() -> None:
    """Test that high thread count triggers anomaly warning."""
    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            import aw_watcher_pipeline_stage.main as main_mod
            
            mock_resource.getrusage.return_value.ru_maxrss = 20 * 1024
            
            # Mock threading.active_count to return 15 (threshold is 10)
            with patch("threading.active_count", return_value=15):
                with patch("time.monotonic", return_value=1000.0):
                    main_mod.log_resource_usage()
                    
                    mock_logger.warning.assert_called()
                    assert "High resource usage" in mock_logger.warning.call_args[0][0]


def test_long_running_idle_stability(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """
    Test strict idle stability: 1 hour run, no file changes.
    Verifies:
    - Exactly 120 periodic heartbeats (plus/minus small margin for start).
    - Resource usage logged periodically.
    - No warnings (CPU/Memory within limits).
    - No memory leaks (state keys constant).
    """
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Idle", "current_task": "Test"}))

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    # Mock resource/logger
    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            import aw_watcher_pipeline_stage.main as main_mod
            
            mock_usage = MagicMock()
            mock_usage.ru_maxrss = 20 * 1024
            mock_usage.ru_utime = 10.0
            mock_usage.ru_stime = 5.0
            mock_resource.getrusage.return_value = mock_usage
            mock_resource.RUSAGE_SELF = 0
            
            mock_logger.isEnabledFor.return_value = True
            # Reset globals for test isolation
            main_mod._last_rusage = None
            main_mod._last_rusage_time = 0.0
            main_mod._last_info_log_time = 0.0

            start_time = 1000.0
            current_time = start_time

            with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", side_effect=lambda: current_time):
                with patch("aw_watcher_pipeline_stage.main.time.monotonic", side_effect=lambda: current_time):
                    
                    # Initial parse
                    watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f))
                    mock_aw_client.heartbeat.reset_mock()
                    
                    # Simulate 1 hour (3600s)
                    for i in range(3600):
                        current_time += 1.0
                        
                        watcher.handler._periodic_heartbeat_task()
                        
                        # Main loop check every 60s
                        if i % 60 == 0:
                            main_mod.log_resource_usage(watcher)

            # Verify heartbeats: 3600 / 30 = 120
            # Allow +/- 2 for timing alignment
            assert 118 <= mock_aw_client.heartbeat.call_count <= 122
            
            # Verify resource logs
            # Should log at least every 6 mins (10 times in 60 mins) + initial
            assert mock_logger.info.call_count >= 10
            
            # Verify no warnings (CPU/Mem stable)
            mock_logger.warning.assert_not_called()
            
            # Verify state stability (no leaks)
            # current_data keys: current_stage, current_task, project_id, status, start_time, metadata, file_path
            assert len(watcher.handler.current_data) == 7

    watcher.stop()


def test_directory_deleted(pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock) -> None:
    """Test that deleting the watched directory triggers a pause."""
    # Create a subdirectory to watch
    watch_dir = temp_dir / "subdir"
    watch_dir.mkdir()
    f = watch_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "S", "current_task": "T"}))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)
    mock_aw_client.heartbeat.reset_mock()

    # Simulate directory deletion event
    event = MagicMock()
    event.is_directory = True
    event.src_path = str(watch_dir)

    with patch.object(handler.logger, "warning") as mock_warn:
        handler.on_deleted(event)
        mock_warn.assert_called_once()
        assert "Watch directory deleted" in mock_warn.call_args[0][0]

    assert handler.is_paused
    mock_aw_client.heartbeat.assert_called_once()
    assert mock_aw_client.heartbeat.call_args[0][1].data["status"] == "paused"


def test_directory_moved(pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock) -> None:
    """Test that moving the watched directory triggers a pause."""
    watch_dir = temp_dir / "subdir"
    watch_dir.mkdir()
    f = watch_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "S", "current_task": "T"}))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)
    mock_aw_client.heartbeat.reset_mock()

    # Simulate directory move event
    event = MagicMock(spec=FileMovedEvent)
    event.is_directory = True
    event.src_path = str(watch_dir)
    event.dest_path = str(temp_dir / "moved_subdir")

    with patch.object(handler.logger, "warning") as mock_warn:
        handler.on_moved(event)
        mock_warn.assert_called_once()
        assert "Watch directory moved" in mock_warn.call_args[0][0]

    assert handler.is_paused
    mock_aw_client.heartbeat.assert_called_once()
    assert mock_aw_client.heartbeat.call_args[0][1].data["status"] == "paused"


def test_directory_recreation_recovery_integration(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test that watcher recovers when the watched directory is deleted and recreated."""
    watch_dir = temp_dir / "subdir"
    watch_dir.mkdir()
    f = watch_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "S1", "current_task": "T1"}))

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    # 1. Initial state
    watcher.handler.on_state_changed(watcher.handler._process_state_change(watcher.handler._read_file_data(str(f)), str(f)), 0.0)
    assert watcher.handler.last_stage == "S1"

    # 2. Delete directory
    shutil.rmtree(watch_dir)
    
    # Trigger check (simulating loop) -> updates _watch_dir_existed to False
    watcher.check_health()
    assert not watcher._watch_dir_existed

    # 3. Recreate directory
    watch_dir.mkdir()
    f.write_text(json.dumps({"current_stage": "S2", "current_task": "T2"}))

    # Trigger check -> detects reappearance, restarts observer, parses file
    with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
        watcher.check_health()
        assert any("Watch directory reappeared" in str(c) for c in mock_logger.info.call_args_list)

    # Verify state updated
    assert watcher.handler.last_stage == "S2"
    
    watcher.stop()


def test_self_healing_empty_state(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test that the watcher recovers from an initial empty state via periodic check."""
    f = temp_dir / "current_task.json"
    # File exists but let's assume initial parse failed or wasn't triggered
    f.write_text(json.dumps({"current_stage": "Healed", "current_task": "State"}))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    # Simulate empty state (e.g. initial read failed)
    handler.current_data = {}
    handler.last_parse_attempt = 0.0  # Ensure throttle check passes

    # Trigger periodic check
    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", return_value=1000.0):
        handler._periodic_heartbeat_task()

    # Should have parsed and sent heartbeat
    assert handler.last_stage == "Healed"
    mock_aw_client.heartbeat.assert_called_once()
    
    # Verify throttle updated
    assert handler.last_parse_attempt == 1000.0


def test_parse_recursion_error(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that RecursionError during JSON parsing is handled gracefully."""
    f = temp_dir / "current_task.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch("aw_watcher_pipeline_stage.watcher.json.load", side_effect=RecursionError("Maximum recursion depth exceeded")):
        with patch("time.sleep"):  # Skip backoff
            with patch.object(handler.logger, "error") as mock_error:
                assert handler._read_file_data(str(f)) is None
                
                # Should have logged error after retries
                mock_error.assert_called_once()
                assert "Error processing" in mock_error.call_args[0][0]
                assert "Maximum recursion depth exceeded" in mock_error.call_args[0][0]


def test_check_health_oserror(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that check_health handles OSError when checking directory existence."""
    f = temp_dir / "current_task.json"
    f.touch()
    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    
    # Mock watch_dir.exists to raise OSError
    with patch.object(Path, "exists", side_effect=OSError("Disk error")):
        # Should not raise exception
        watcher.check_health()


def test_handler_check_self_healing_oserror(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that handler's check_self_healing handles OSError during file existence check."""
    f = temp_dir / "current_task.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    # Force self-healing check
    handler.last_parse_attempt = 0.0
    
    with patch.object(Path, "exists", side_effect=OSError("Disk error")):
        with patch.object(handler.logger, "warning") as mock_warn:
            handler.check_self_healing()
            mock_warn.assert_called()
            assert "Failed to check file existence" in mock_warn.call_args[0][0]


def test_start_initial_read_failure(pipeline_client: PipelineClient, temp_dir: Path, mock_observer: MagicMock) -> None:
    """Test that start() handles initial read failure (returning None) without crashing."""
    f = temp_dir / "current_task.json"
    f.touch()

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)

    # Mock _read_file_data to return None (simulating read error)
    with patch.object(watcher.handler, "_read_file_data", return_value=None):
        watcher.start()
        # Should not raise UnboundLocalError

    watcher.stop()


def test_check_health_recovery_read_failure(pipeline_client: PipelineClient, temp_dir: Path, mock_observer: MagicMock) -> None:
    """Test that check_health handles read failure during directory recovery."""
    f = temp_dir / "current_task.json"
    f.touch()

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    # Simulate directory disappearing and reappearing
    watcher._watch_dir_existed = False

    # Mock _read_file_data to return None
    with patch.object(watcher.handler, "_read_file_data", return_value=None):
        with patch("aw_watcher_pipeline_stage.watcher.logger"):
            watcher.check_health()
            # Should not raise UnboundLocalError

    watcher.stop()

def test_check_health_recovery_file_check_oserror(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that check_health handles OSError when checking target file during recovery."""
    f = temp_dir / "current_task.json"
    f.touch()

    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    watcher.start()

    # Simulate directory disappearing and reappearing
    watcher._watch_dir_existed = False

    # Mock target_file.exists to raise OSError
    with patch.object(watcher.watch_dir, "exists", return_value=True):
        with patch.object(watcher.handler.target_file, "exists", side_effect=OSError("Disk error")):
            with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
                watcher.check_health()
                
                # Should have logged warning
                assert any("Failed to check target file" in str(c) for c in mock_logger.warning.call_args_list)

    watcher.stop()

def test_parse_file_wrapper_exception_handling(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that exceptions in the timer wrapper are caught and logged."""
    f = temp_dir / "current_task.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Mock _read_file_data to raise unexpected exception
    with patch.object(handler, "_read_file_data", side_effect=RuntimeError("Unexpected boom")):
        with patch.object(handler.logger, "error") as mock_error:
            handler._parse_file_wrapper(str(f))
            
            mock_error.assert_called_once()
            assert "Unexpected error in debounce timer" in mock_error.call_args[0][0]


def test_parse_file_wrapper_stopped(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that _parse_file_wrapper aborts if stopped."""
    f = temp_dir / "current_task.json"
    f.write_text(json.dumps({"current_stage": "S", "current_task": "T"}))
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    handler._stopped = True
    
    with patch.object(handler, "_read_file_data") as mock_read:
        handler._parse_file_wrapper(str(f))
        mock_read.assert_not_called()


def test_parse_bom(pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock) -> None:
    """Test that files with UTF-8 BOM are parsed correctly."""
    f = temp_dir / "current_task.json"
    data = {"current_stage": "BOM", "current_task": "Test"}
    # Write BOM + JSON
    f.write_bytes(b'\xef\xbb\xbf' + json.dumps(data).encode('utf-8'))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)

    assert handler.last_stage == "BOM"
    mock_aw_client.heartbeat.assert_called_once()


def test_watcher_path_heuristic(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test heuristic for non-existent paths."""
    # Case 1: Non-existent file (has suffix)
    p1 = temp_dir / "missing.json"
    w1 = PipelineWatcher(p1, pipeline_client, 120.0)
    assert w1.watch_dir == temp_dir
    assert w1.handler.target_file == p1

    # Case 2: Non-existent dir (no suffix)
    p2 = temp_dir / "missing_dir"
    w2 = PipelineWatcher(p2, pipeline_client, 120.0)
    assert w2.watch_dir == p2
    assert w2.handler.target_file == p2 / "current_task.json"


def test_read_error_log_throttling(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that read errors are throttled and not logged every time."""
    f = temp_dir / "current_task.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch("time.sleep"):  # Skip backoff
        with patch.object(Path, "open", side_effect=OSError("Read fail")):
            with patch.object(handler.logger, "error") as mock_error:
                # First call: should log
                handler._read_file_data(str(f))
                assert mock_error.call_count == 1
                
                # Second call immediately: should NOT log
                handler._read_file_data(str(f))
                assert mock_error.call_count == 1
                
                # Third call after 61s: should log
                handler.last_read_error_time -= 61.0
                handler._read_file_data(str(f))
                assert mock_error.call_count == 2


def test_directory_check_throttling(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that directory existence check is throttled."""
    f = temp_dir / "current_task.json"
    f.touch()
    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    
    with patch.object(watcher.watch_dir, "exists", return_value=True) as mock_exists:
        # First call: checks
        watcher.check_periodic_heartbeat()
        assert mock_exists.call_count == 1
        
        # Second call immediately: skips check
        watcher.check_periodic_heartbeat()
        assert mock_exists.call_count == 1


def test_observer_property_instantiation_failure(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that observer property raises RuntimeError if Observer() fails."""
    f = temp_dir / "current_task.json"
    f.touch()
    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    
    with patch("aw_watcher_pipeline_stage.watcher.Observer", side_effect=OSError("Inotify limit")):
        with pytest.raises(RuntimeError, match="Observer is not available"):
            _ = watcher.observer


def test_computed_duration_accumulation(
    pipeline_client: PipelineClient, temp_dir: Path, mock_aw_client: MagicMock
) -> None:
    """Test that computed_duration accumulates across identical file updates."""
    f = temp_dir / "current_task.json"
    data = {"current_stage": "Accumulate", "current_task": "Test"}
    f.write_text(json.dumps(data))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    # Mock time
    start_time = 1000.0
    
    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", return_value=start_time):
        # Initial parse
        handler.on_state_changed(handler._process_state_change(handler._read_file_data(str(f)), str(f)), 0.0)
        
    # Simulate 10s passing, file touched but content same
    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", return_value=start_time + 10.0):
        # Trigger parse (simulating file event)
        # Should NOT trigger on_state_changed because hash matches
        handler._process_state_change(handler._read_file_data(str(f)), str(f))
        assert handler.last_change_time == start_time
        
    # Simulate 30s passing (total 30s from start), periodic check
    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", return_value=start_time + 30.0):
        mock_aw_client.heartbeat.reset_mock()
        handler._periodic_heartbeat_task()
        
        mock_aw_client.heartbeat.assert_called_once()
        event = mock_aw_client.heartbeat.call_args[0][1]
        # Duration should be 30.0
        assert event.data["computed_duration"] == 30.0

def test_observer_cooldown_logic(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that observer restart respects cooldown."""
    f = temp_dir / "current_task.json"
    f.touch()
    watcher = PipelineWatcher(f, pipeline_client, pulsetime=120.0)
    
    # Simulate a recent restart attempt
    watcher._started = True
    watcher._last_observer_restart_attempt = time.monotonic()
    
    # Should raise RuntimeError because it's in cooldown and _observer is None
    with pytest.raises(RuntimeError, match="Observer is not available"):
        _ = watcher.observer
        
    # Simulate cooldown passed
    watcher._last_observer_restart_attempt -= 11.0
    
    with patch.object(watcher, "_start_observer") as mock_start:
        _ = watcher.observer
        mock_start.assert_called_once()

def test_small_debounce_warning(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that small debounce interval triggers warning."""
    f = temp_dir / "current_task.json"
    f.touch()
    
    with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
        PipelineEventHandler(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.05)
        mock_logger.warning.assert_called()
        assert "Very small debounce" in mock_logger.warning.call_args[0][0]


def test_symlink_read_security_check(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that _read_file_data refuses to read symlinks."""
    target = temp_dir / "target.json"
    target.write_text("{}")
    link = temp_dir / "link.json"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("Symlinks not supported")

    handler = PipelineEventHandler(target, pipeline_client, 120.0)

    with patch.object(handler.logger, "warning") as mock_warn:
        data = handler._read_file_data(str(link))
        assert data is None
        assert "Target is a symlink" in mock_warn.call_args[0][0]


def test_process_event_path_mismatch(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that events for files other than the target are ignored."""
    target = temp_dir / "target.json"
    target.touch()
    other = temp_dir / "other.json"
    other.touch()

    handler = PipelineEventHandler(target, pipeline_client, 120.0)

    event = MagicMock()
    event.is_directory = False
    event.src_path = str(other)

    with patch.object(handler, "_read_file_data") as mock_read:
        handler._process_event(event)
        mock_read.assert_not_called()


def test_runtime_symlink_swap(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that replacing a file with a symlink at runtime is detected and rejected."""
    f = temp_dir / "swap_test.json"
    f.write_text("{}")
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    # 1. Valid read
    assert handler._read_file_data(str(f)) is not None
    
    # 2. Swap to symlink
    f.unlink()
    target = temp_dir / "target.json"
    target.write_text('{"hacked": true}')
    try:
        f.symlink_to(target)
    except OSError:
        pytest.skip("Symlinks not supported")
        
    # 3. Attempt read - should be rejected
    with patch.object(handler.logger, "warning") as mock_warn:
        data = handler._read_file_data(str(f))
        assert data is None
        assert "Target is a symlink" in mock_warn.call_args[0][0]


def test_event_path_traversal_resolution(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that event paths with traversal are correctly resolved and matched."""
    f = temp_dir / "target.json"
    f.write_text("{}")
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    # Construct a path with traversal that resolves to the target
    # e.g. /tmp/dir/../dir/target.json
    traversal_path = str(temp_dir / "subdir" / ".." / "target.json")
    
    event = MagicMock()
    event.is_directory = False
    event.src_path = traversal_path
    
    # Should resolve and process if it matches target
    with patch.object(handler, "_read_file_data") as mock_read:
        handler._process_event(event)
        mock_read.assert_called()


def test_parse_file_content_exceeds_limit_race_condition(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test rejection when file content exceeds limit despite stat reporting small size."""
    f = temp_dir / "race_large.json"
    # Write large content
    large_content = " " * (MAX_FILE_SIZE_BYTES + 100)
    f.write_text(large_content)

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Mock stat to return small size (simulating race condition or FS lag)
    with patch.object(Path, "stat") as mock_stat:
        mock_stat.return_value.st_size = 100
        mock_stat.return_value.st_mode = stat.S_IFREG

        with patch.object(handler.logger, "warning") as mock_warn:
            data = handler._read_file_data(str(f))
            assert data is None
            
            # Should warn about content exceeding limit
            mock_warn.assert_called()
            assert "content exceeds limit" in mock_warn.call_args[0][0]


def test_parse_invalid_types_for_required_fields(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that non-string types for required fields are rejected."""
    f = temp_dir / "invalid_types.json"
    
    scenarios = [
        {"current_stage": 123, "current_task": "Task"},
        {"current_stage": "Stage", "current_task": ["Task"]},
        {"current_stage": {"nested": "dict"}, "current_task": "Task"},
    ]
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    for data in scenarios:
        f.write_text(json.dumps(data))
        # Read data
        read_data = handler._read_file_data(str(f))
        assert read_data is not None
        
        # Process state change
        with patch.object(handler.logger, "warning") as mock_warn:
            result = handler._process_state_change(read_data, str(f))
            assert result is None
            mock_warn.assert_called()
            assert "must be a string" in mock_warn.call_args[0][0]


def test_debounce_timer_cancellation_metrics(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that rapid events correctly cancel previous timers."""
    f = temp_dir / "debounce.json"
    f.touch()
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0, debounce_seconds=1.0)
    
    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)
    
    with patch("threading.Timer") as mock_timer_cls:
        mock_timer_instance = MagicMock()
        mock_timer_cls.return_value = mock_timer_instance
        
        # Simulate 100 rapid events
        for _ in range(100):
            handler.on_modified(event)
            
        # Timer should be started 100 times
        assert mock_timer_cls.call_count == 100
        # Previous timers should be cancelled. 
        # On calls 2-100, self._timer is the previous mock (same instance), so cancel is called.
        # Total cancel calls should be 99.
        assert mock_timer_instance.cancel.call_count == 99
        assert mock_timer_instance.start.call_count == 100


def test_parse_value_error_handling(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test handling of ValueError during JSON parsing (e.g. limit exceeded in underlying lib)."""
    f = temp_dir / "value_error.json"
    f.write_text("{}")
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    with patch("time.sleep"): # Skip backoff
        with patch("json.loads", side_effect=ValueError("Limit exceeded")):
            with patch.object(handler.logger, "error") as mock_error:
                data = handler._read_file_data(str(f))
                assert data is None
                
                # Should eventually log error after retries
                mock_error.assert_called()
                assert "Value error processing" in mock_error.call_args[0][0]


def test_parse_os_error_during_read(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test handling of generic OSError during file read (not stat)."""
    f = temp_dir / "os_error.json"
    f.write_text("{}")
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    with patch("time.sleep"):
        # Mock open to succeed, but read to fail
        with patch.object(Path, "open") as mock_open:
            mock_file = MagicMock()
            mock_file.__enter__.return_value.read.side_effect = OSError("IO Error")
            mock_open.return_value = mock_file
            
            with patch.object(handler.logger, "error") as mock_error:
                data = handler._read_file_data(str(f))
                assert data is None
                
                mock_error.assert_called()
                assert "Error processing" in mock_error.call_args[0][0]


def test_event_path_traversal_escape(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that event paths traversing outside are ignored."""
    f = temp_dir / "target.json"
    f.touch()
    outside = temp_dir / "outside.json"
    outside.touch()
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    # Path that looks like it might be inside but traverses out
    traversal_path = str(temp_dir / "subdir" / ".." / "outside.json")
    
    event = MagicMock()
    event.is_directory = False
    event.src_path = traversal_path
    
    with patch.object(handler, "_read_file_data") as mock_read:
        handler._process_event(event)
        mock_read.assert_not_called()

def test_sensitive_file_symlink_attack(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that watcher refuses to read if target is a symlink to a sensitive file."""
    # Simulate a sensitive file (e.g. /etc/passwd)
    sensitive = temp_dir / "passwd"
    sensitive.write_text("root:x:0:0:root:/root:/bin/bash")
    
    link = temp_dir / "link_to_passwd"
    try:
        link.symlink_to(sensitive)
    except OSError:
        pytest.skip("Symlinks not supported")
        
    handler = PipelineEventHandler(link, pipeline_client, 120.0)
    
    # Verify _read_file_data rejects the symlink explicitly
    with patch.object(handler.logger, "warning") as mock_warn:
        data = handler._read_file_data(str(link))
        assert data is None
        assert "Target is a symlink" in mock_warn.call_args[0][0]


def test_runtime_symlink_loop_read(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that a runtime symlink loop is handled gracefully."""
    link1 = temp_dir / "loop1.json"
    link2 = temp_dir / "loop2.json"
    
    # Create initial valid file
    link1.write_text("{}")
    
    handler = PipelineEventHandler(link1, pipeline_client, 120.0)
    
    # 1. Valid read
    assert handler._read_file_data(str(link1)) is not None
    
    # 2. Create loop
    link1.unlink()
    try:
        link1.symlink_to(link2)
        link2.symlink_to(link1)
    except OSError:
        pytest.skip("Symlinks not supported")
        
    # 3. Attempt read
    # Should catch OSError (Too many levels of symbolic links) or RecursionError
    with patch("time.sleep"):
        data = handler._read_file_data(str(link1))
        assert data is None


def test_watcher_init_with_symlink_refusal(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that if Watcher is initialized with a symlink, it refuses to read."""
    target = temp_dir / "target.json"
    target.write_text("{}")
    link = temp_dir / "link.json"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("Symlinks not supported")

    watcher = PipelineWatcher(link, pipeline_client, 120.0)
    # The watcher itself starts, but the handler should refuse to read
    with patch.object(watcher.handler.logger, "warning") as mock_warn:
        data = watcher.handler._read_file_data(str(link))
        assert data is None
        assert "Target is a symlink" in mock_warn.call_args[0][0]


def test_read_file_data_checks_symlink_before_stat(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that _read_file_data checks is_symlink before performing stat (mitigation check)."""
    f = temp_dir / "test.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    # Verify rejection path: if is_symlink returns True, stat should NOT be called
    with patch.object(Path, "is_symlink", return_value=True) as mock_is_symlink:
        with patch.object(Path, "stat") as mock_stat:
            with patch.object(handler.logger, "warning") as mock_warn:
                data = handler._read_file_data(str(f))
                assert data is None
                mock_is_symlink.assert_called()
                mock_stat.assert_not_called()
                assert "Target is a symlink" in mock_warn.call_args[0][0]


def test_read_file_data_real_file_size_check(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that _read_file_data rejects large files using real stat check."""
    f = temp_dir / "real_large.json"
    # Write 11KB (MAX is 10KB)
    f.write_text("x" * (11 * 1024))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch.object(handler.logger, "warning") as mock_warn:
        data = handler._read_file_data(str(f))
        assert data is None
        mock_warn.assert_called()
        assert "too large" in mock_warn.call_args[0][0]


def test_parse_actual_deep_nesting(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that actual deep nesting triggers RecursionError handling."""
    f = temp_dir / "deep.json"

    # Create deep nesting: {"a": {"a": ...}}
    # Depth 1500 should trigger RecursionError in default Python (limit 1000)
    depth = 1500
    content = '{"a": ' * depth + '1' + '}' * depth
    f.write_text(content)

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Bypass size checks to test recursion specifically
    with patch.object(Path, "stat") as mock_stat:
        mock_stat.return_value.st_size = 100  # Fake small size
        mock_stat.return_value.st_mode = stat.S_IFREG

        # Patch the constant used in _read_file_data
        with patch("aw_watcher_pipeline_stage.watcher.MAX_FILE_SIZE_BYTES", 100000):
            with patch.object(handler.logger, "error") as mock_error:
                data = handler._read_file_data(str(f))
                assert data is None

                # Should catch RecursionError
                mock_error.assert_called()
                assert "recursion limit exceeded" in mock_error.call_args[0][0]


def test_parse_truncated_json(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test handling of truncated JSON (valid encoding but invalid syntax)."""
    f = temp_dir / "truncated.json"
    f.write_text('{"current_stage": "Incomplete"')

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch.object(handler.logger, "debug") as mock_debug:
        # Skip retries
        with patch("time.sleep"):
            data = handler._read_file_data(str(f))
            assert data is None

            # Verify it caught JSONDecodeError (logged as debug during retries)
            assert any("Malformed JSON" in str(c) for c in mock_debug.call_args_list)


def test_parse_invalid_optional_types(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that invalid types for optional fields are handled gracefully."""
    f = temp_dir / "optional_types.json"
    data = {
        "current_stage": "S",
        "current_task": "T",
        "project_id": 12345,  # Should be string
        "status": ["invalid"],  # Should be string
        "start_time": 99999,  # Should be string
    }
    f.write_text(json.dumps(data))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch.object(handler.logger, "warning") as mock_warn:
        result = handler._process_state_change(handler._read_file_data(str(f)), str(f))

        # Should succeed but drop invalid fields
        assert result is not None
        assert result["current_stage"] == "S"
        assert result["project_id"] is None
        assert result["status"] == "in_progress"  # Default
        assert result["start_time"] is None

        # Verify warnings
        warnings = [str(c) for c in mock_warn.call_args_list]
        assert any("project_id must be a string" in w for w in warnings)
        assert any("status must be a string" in w for w in warnings)
        assert any("start_time must be a string" in w for w in warnings)


def test_debounce_high_frequency_warning(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that high frequency events trigger a warning log."""
    f = temp_dir / "rapid.json"
    f.touch()

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0, debounce_seconds=1.0)
    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)

    with patch("threading.Timer"):
        with patch.object(handler.logger, "warning") as mock_warn:
            # Trigger 51 events (threshold is 50)
            for _ in range(51):
                handler.on_modified(event)
            
            mock_warn.assert_called()
            assert "High frequency file events detected" in mock_warn.call_args[0][0]


def test_read_file_data_boundary_size(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test reading files at exact size limit and just above."""
    f_ok = temp_dir / "ok.json"
    f_ok.write_text("x" * MAX_FILE_SIZE_BYTES)
    
    f_large = temp_dir / "large.json"
    f_large.write_text("x" * (MAX_FILE_SIZE_BYTES + 1))
    
    handler = PipelineEventHandler(f_ok, pipeline_client, 120.0)
    
    # 1. Exact limit should pass size check (though content might be invalid JSON)
    # We mock json.loads to avoid decode error since "x"*N isn't valid JSON
    with patch("json.loads", return_value={}):
        assert handler._read_file_data(str(f_ok)) is not None
        
    # 2. Just above limit should fail
    with patch.object(handler.logger, "warning") as mock_warn:
        assert handler._read_file_data(str(f_large)) is None
        assert "too large" in mock_warn.call_args[0][0]


def test_read_file_data_unicode_retries(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that UnicodeDecodeError triggers retries."""
    f = temp_dir / "bad_encoding.json"
    with open(f, "wb") as binary:
        binary.write(b"\x80\x81")
        
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    with patch("time.sleep") as mock_sleep:
        with patch.object(handler.logger, "debug") as mock_debug:
            with patch.object(handler.logger, "warning") as mock_warn:
                handler._read_file_data(str(f))
                
                # Should retry 4 times (total 5 attempts)
                assert mock_sleep.call_count == 4
                # Should log debug for retries
                assert mock_debug.call_count >= 4
                # Should log warning on final failure
                mock_warn.assert_called_once()
                assert "Encoding error" in mock_warn.call_args[0][0]


def test_process_state_change_robustness(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test _process_state_change with unexpected types directly."""
    f = temp_dir / "test.json"
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    # 1. Non-string stage
    data_bad_stage = {"current_stage": 123, "current_task": "T"}
    with patch.object(handler.logger, "warning") as mock_warn:
        assert handler._process_state_change(data_bad_stage, str(f)) is None
        assert "current_stage must be a string" in mock_warn.call_args[0][0]
        
    # 2. Non-string task
    data_bad_task = {"current_stage": "S", "current_task": ["Task"]}
    with patch.object(handler.logger, "warning") as mock_warn:
        assert handler._process_state_change(data_bad_task, str(f)) is None
        assert "current_task must be a string" in mock_warn.call_args[0][0]


def test_read_file_utf16_be(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that UTF-16 encoded files trigger UnicodeDecodeError and are handled."""
    f = temp_dir / "utf16.json"
    data = json.dumps({"current_stage": "S", "current_task": "T"})
    f.write_bytes(data.encode("utf-16-be"))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch("time.sleep"):  # Skip backoff
        with patch.object(handler.logger, "warning") as mock_warn:
            # Should fail to decode as utf-8-sig
            assert handler._read_file_data(str(f)) is None
            # Should eventually log warning after retries
            mock_warn.assert_called()
            assert "Encoding error" in mock_warn.call_args[0][0]


def test_parse_json_bool_values(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that boolean values for string fields are rejected."""
    f = temp_dir / "bools.json"
    f.write_text(json.dumps({"current_stage": True, "current_task": False}))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch.object(handler.logger, "warning") as mock_warn:
        data = handler._read_file_data(str(f))
        assert data is not None
        result = handler._process_state_change(data, str(f))
        assert result is None

        warnings = [str(c) for c in mock_warn.call_args_list]
        assert any("current_stage must be a string" in w for w in warnings)


def test_process_state_change_metadata_null(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that metadata set to null in JSON is handled as empty dict."""
    f = temp_dir / "null_meta.json"
    f.write_text(json.dumps({"current_stage": "S", "current_task": "T", "metadata": None}))

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch.object(handler.logger, "warning") as mock_warn:
        data = handler._read_file_data(str(f))
        assert data is not None
        result = handler._process_state_change(data, str(f))

        assert result is not None
        assert result["metadata"] == {}
        # Should warn about metadata not being a dict (NoneType)
        mock_warn.assert_called()
        assert "Metadata field" in mock_warn.call_args[0][0]


def test_debounce_timer_replacement_verification(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that a new event replaces the existing timer instance."""
    f = temp_dir / "timer.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0, debounce_seconds=1.0)

    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)

    with patch("threading.Timer") as mock_timer_cls:
        timer1 = MagicMock()
        timer2 = MagicMock()
        mock_timer_cls.side_effect = [timer1, timer2]

        # First event
        handler.on_modified(event)
        assert handler._timer is timer1
        timer1.start.assert_called_once()

        # Second event
        handler.on_modified(event)
        assert handler._timer is timer2

        # Verify first timer cancelled
        timer1.cancel.assert_called_once()
        timer2.start.assert_called_once()


def test_read_file_data_whitespace_only(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that a file containing only whitespace is retried/skipped."""
    f = temp_dir / "whitespace.json"
    f.write_text("   \n   \t   ")

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch("time.sleep") as mock_sleep:
        with patch.object(handler.logger, "debug") as mock_debug:
            data = handler._read_file_data(str(f))
            assert data is None

            # Should have retried
            assert mock_sleep.call_count >= 4
            assert any("whitespace only" in str(c) for c in mock_debug.call_args_list)


def test_process_state_change_invalid_status_type(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that invalid status type is handled gracefully."""
    f = temp_dir / "test.json"
    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    data_bad_status = {"current_stage": "S", "current_task": "T", "status": 123}
    with patch.object(handler.logger, "warning") as mock_warn:
        res = handler._process_state_change(data_bad_status, str(f))
        assert res is not None
        assert res["status"] == "in_progress" # Default
        assert "status must be a string" in mock_warn.call_args[0][0]


def test_debounce_rapid_fire_execution(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that rapid events result in correct debounce counter and single execution."""
    f = temp_dir / "debounce.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0, debounce_seconds=1.0)
    
    callbacks = []
    
    class MockTimer:
        def __init__(self, interval, function, args=None, kwargs=None):
            self.function = function
            self.args = args or []
            self.kwargs = kwargs or {}
            self.interval = interval
            self.cancelled = False
            self.daemon = False
            
        def cancel(self):
            self.cancelled = True
            
        def start(self):
            callbacks.append(self)
            
    with patch("threading.Timer", side_effect=MockTimer):
        event = MagicMock()
        event.is_directory = False
        event.src_path = str(f)
        
        # Fire 100 events
        for _ in range(100):
            handler.on_modified(event)
            
        assert len(callbacks) == 100
        
        # Verify cancellation
        for t in callbacks[:-1]:
            assert t.cancelled
        assert not callbacks[-1].cancelled
        
        last_timer = callbacks[-1]
        
        # Execute the last timer
        with patch.object(handler, "_read_file_data", return_value={"current_stage": "S", "current_task": "T"}) as mock_read:
            with patch.object(handler, "on_state_changed") as mock_change:
                # Mock threading.current_thread to match the timer instance
                with patch("threading.current_thread", return_value=last_timer):
                    last_timer.function(*last_timer.args, **last_timer.kwargs)
                    
                    mock_read.assert_called_once()
                    mock_change.assert_called_once()
                    
                    # Verify stats
                    assert handler.total_debounced_events == 99
                    assert handler._debounce_counter == 0


def test_batch_limit_trigger(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that reaching batch_size_limit triggers immediate processing."""
    f = temp_dir / "batch_limit.json"
    f.touch()
    
    # Set debounce high to ensure timer wouldn't fire naturally
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0, debounce_seconds=10.0)
    handler.batch_size_limit = 5
    
    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)
    
    with patch("threading.Timer") as mock_timer_cls:
        # 1. Add 4 events (below limit)
        for _ in range(4):
            handler.on_modified(event)
            
        # Should have scheduled normal debounce (10.0s)
        assert mock_timer_cls.call_count == 1
        args, _ = mock_timer_cls.call_args
        assert args[0] == 10.0
        
        # 2. Add 5th event (hit limit)
        handler.on_modified(event)
        
        # Should have scheduled immediate check (0.0s)
        assert mock_timer_cls.call_count == 2
        args, _ = mock_timer_cls.call_args
        assert args[0] == 0.0
        
        # Verify previous timer cancelled
        mock_timer_instance = mock_timer_cls.return_value
        mock_timer_instance.cancel.assert_called()


def test_rate_limiting_dos_prevention(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that events are dropped when rate limit is exceeded."""
    f = temp_dir / "rate_limit.json"
    f.touch()

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    # Reset tokens to allow controlled testing
    handler._rate_limit_tokens = 5.0
    handler._rate_limit_max = 5.0
    handler._rate_limit_last_update = 1000.0

    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)

    # Freeze time so tokens don't refill
    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", return_value=1000.0):
        # Process 10 events
        # First 5 should pass (tokens 5 -> 0)
        # Next 5 should be dropped
        for _ in range(10):
            handler._process_event(event)

    # Check dropped count
    assert handler._dropped_events >= 5


def test_json_decode_error_snippet_logging(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that a snippet of malformed JSON is logged in debug mode."""
    f = temp_dir / "malformed.json"
    f.write_text('{"broken": "json" ' + "x" * 100)  # Malformed

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    with patch("time.sleep"):  # Skip backoff
        with patch.object(handler.logger, "debug") as mock_debug:
            with patch.object(handler.logger, "isEnabledFor", return_value=True):
                handler._read_file_data(str(f))

                # Check for snippet log
                assert any("JSON Snippet" in str(c) for c in mock_debug.call_args_list)


def test_metadata_allowlist_filtering_in_watcher(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that metadata is filtered in watcher based on client allowlist."""
    f = temp_dir / "allowlist.json"

    # Configure client with allowlist
    pipeline_client.metadata_allowlist = {"allowed_key"}

    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    data = {
        "current_stage": "S",
        "current_task": "T",
        "metadata": {"allowed_key": "yes", "blocked_key": "no"},
    }

    processed = handler._process_state_change(data, str(f))
    assert processed is not None
    assert "allowed_key" in processed["metadata"]
    assert "blocked_key" not in processed["metadata"]


def test_start_time_validation_in_watcher(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test validation of start_time format in watcher."""
    f = temp_dir / "starttime.json"
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)

    # Invalid format
    data_invalid = {"current_stage": "S", "current_task": "T", "start_time": "not-a-date"}

    with patch.object(handler.logger, "warning") as mock_warn:
        processed = handler._process_state_change(data_invalid, str(f))
        assert processed is not None
        assert processed["start_time"] is None
        assert any("Invalid timestamp format" in str(c) for c in mock_warn.call_args_list)

    # Valid format
    data_valid = {
        "current_stage": "S",
        "current_task": "T",
        "start_time": "2023-01-01T12:00:00Z",
    }
    processed = handler._process_state_change(data_valid, str(f))
    assert processed is not None
    assert processed["start_time"] == "2023-01-01T12:00:00Z"


def test_read_file_enforces_read_limit_arg(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that file.read() is called with exactly MAX_FILE_SIZE_BYTES + 1."""
    f = temp_dir / "limit_check.json"
    f.write_text("{}")
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    # Mock the file object returned by open()
    with patch("pathlib.Path.open") as mock_open:
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        mock_file.read.return_value = "{}"
        
        handler._read_file_data(str(f))
        
        # Verify read was called with limit
        mock_file.read.assert_called_with(MAX_FILE_SIZE_BYTES + 1)


def test_parse_truncated_utf8(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test handling of truncated UTF-8 sequences (valid start byte, missing continuation)."""
    f = temp_dir / "truncated_utf8.json"
    # 0xE2 is start of 3-byte sequence, but we stop there
    with open(f, "wb") as binary:
        binary.write(b'{"key": "val\xE2')
        
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    with patch("time.sleep"): # Skip backoff
        with patch.object(handler.logger, "warning") as mock_warn:
            data = handler._read_file_data(str(f))
            assert data is None
            
            # Should eventually log warning about encoding
            mock_warn.assert_called()
            assert "Encoding error" in mock_warn.call_args[0][0]


def test_parse_deeply_nested_lists(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test RecursionError handling for deeply nested lists."""
    f = temp_dir / "deep_list.json"
    
    # Create deep nesting: [[[[...]]]]
    depth = 1500
    content = '[' * depth + '1' + ']' * depth
    f.write_text(content)
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    # Bypass size checks to hit recursion limit
    with patch("aw_watcher_pipeline_stage.watcher.MAX_FILE_SIZE_BYTES", 100000):
        with patch.object(handler.logger, "error") as mock_error:
            data = handler._read_file_data(str(f))
            assert data is None
            
            mock_error.assert_called()
            assert "recursion limit exceeded" in mock_error.call_args[0][0]


def test_process_state_change_multiple_invalid_fields(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test processing a state dict with multiple invalid fields simultaneously."""
    f = temp_dir / "multi_invalid.json"
    
    data = {
        "current_stage": 123, # Invalid
        "current_task": ["Task"], # Invalid
        "status": "unknown_status", # Invalid enum
        "project_id": 456 # Invalid type
    }
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    with patch.object(handler.logger, "warning") as mock_warn:
        result = handler._process_state_change(data, str(f))
        
        # Should return None because required fields are invalid
        assert result is None
        
        # Verify warnings for the first encountered issue (stage)
        warnings = [str(c) for c in mock_warn.call_args_list]
        assert any("current_stage must be a string" in w for w in warnings)


def test_debounce_metrics_update_on_fire(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that total_debounced_events metric is updated when the timer actually fires."""
    f = temp_dir / "metrics.json"
    f.touch()
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    handler._debounce_counter = 10
    
    # Mock read to return data so we proceed to metrics update
    with patch.object(handler, "_read_file_data", return_value={"current_stage": "S", "current_task": "T"}):
        with patch.object(handler, "on_state_changed"):
            # Mock current thread to match timer check
            handler._timer = threading.current_thread() # type: ignore
            
            handler._parse_file_wrapper(str(f))
            
            assert handler.total_debounced_events == 10
            assert handler._debounce_counter == 0


def test_parse_json_float_values(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that float values for string fields are rejected."""
    f = temp_dir / "floats.json"
    f.write_text(json.dumps({"current_stage": 1.5, "current_task": "Task"}))
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    with patch.object(handler.logger, "warning") as mock_warn:
        data = handler._read_file_data(str(f))
        assert data is not None
        result = handler._process_state_change(data, str(f))
        assert result is None
        
        assert "current_stage must be a string" in mock_warn.call_args[0][0]


def test_parse_large_file_incremental_growth(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test file growing from valid to invalid size."""
    f = temp_dir / "growing.json"
    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    # 1. Valid size
    f.write_text(json.dumps({"current_stage": "Small", "current_task": "Task"}))
    assert handler._read_file_data(str(f)) is not None

    # 2. Invalid size
    f.write_text("x" * (MAX_FILE_SIZE_BYTES + 100))
    with patch.object(handler.logger, "warning") as mock_warn:
        assert handler._read_file_data(str(f)) is None
        assert "too large" in mock_warn.call_args[0][0]


def test_utf8_bom_with_invalid_body(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test file with valid BOM but invalid body bytes."""
    f = temp_dir / "bom_invalid.json"
    # BOM + invalid byte
    f.write_bytes(b'\xef\xbb\xbf' + b'{"key": "\xff"}')

    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    with patch("time.sleep"): # Skip backoff
        with patch.object(handler.logger, "warning") as mock_warn:
            assert handler._read_file_data(str(f)) is None
            assert "Encoding error" in mock_warn.call_args[0][0]


def test_deep_nesting_mixed_structure(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test recursion error with mixed dict/list nesting."""
    f = temp_dir / "deep_mixed.json"
    
    # {"a": [{"a": ...}]}
    depth = 800 # 800 * 2 = 1600 depth approx
    content = '{"a": [' * depth + '1' + ']}' * depth
    f.write_text(content)
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    with patch("aw_watcher_pipeline_stage.watcher.MAX_FILE_SIZE_BYTES", 100000):
        with patch.object(handler.logger, "error") as mock_error:
            assert handler._read_file_data(str(f)) is None
            assert "recursion limit exceeded" in mock_error.call_args[0][0]


def test_type_validation_all_fields_invalid(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test rejection when all fields have invalid types."""
    f = temp_dir / "all_invalid.json"
    data = {
        "current_stage": 1,
        "current_task": 2,
        "project_id": 3,
        "status": 4,
        "start_time": 5,
        "metadata": 6
    }
    f.write_text(json.dumps(data))
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    with patch.object(handler.logger, "warning") as mock_warn:
        # Read succeeds (valid JSON)
        read_data = handler._read_file_data(str(f))
        assert read_data is not None
        
        # Process fails
        result = handler._process_state_change(read_data, str(f))
        assert result is None
        
        # Verify warnings for required fields
        warnings = [str(c) for c in mock_warn.call_args_list]
        assert any("current_stage must be a string" in w for w in warnings)


def test_debounce_flood_during_shutdown(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that events flooding during shutdown don't cause errors."""
    f = temp_dir / "shutdown_flood.json"
    f.touch()
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0, debounce_seconds=0.1)
    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)
    
    # Start flooding
    for _ in range(50):
        handler.on_modified(event)
        
    # Stop handler
    handler.stop()
    
    # Flood more
    for _ in range(50):
        handler.on_modified(event)
        
    # Should handle gracefully, timer should be cancelled
    assert handler._stopped
    if handler._timer:
        assert not handler._timer.is_alive()


def test_logging_of_large_skipped_files_throttled(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that large file warnings are throttled."""
    f = temp_dir / "large_throttled.json"
    f.write_text("x" * (MAX_FILE_SIZE_BYTES + 100))
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    with patch.object(handler.logger, "warning") as mock_warn:
        # 1. First read - logs warning
        handler._read_file_data(str(f))
        assert mock_warn.call_count == 1
        
        # 2. Second read immediately - no log
        handler._read_file_data(str(f))
        assert mock_warn.call_count == 1
        
        # 3. Third read after delay - logs warning
        handler._last_file_size_warning_time -= 61.0
        handler._read_file_data(str(f))
        assert mock_warn.call_count == 2


def test_parse_valid_json_exact_limit(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test parsing a valid JSON file that is exactly at the size limit."""
    # Construct valid JSON to hit exactly MAX_FILE_SIZE_BYTES
    # {"k": "..."}
    # Overhead: {"k": ""} is 9 bytes.
    # Padding needed: MAX - 9
    padding_len = MAX_FILE_SIZE_BYTES - 9
    data = {"k": "x" * padding_len}
    json_str = json.dumps(data)
    
    # Verify assumption
    assert len(json_str) == MAX_FILE_SIZE_BYTES
    
    f = temp_dir / "exact_limit.json"
    f.write_text(json_str)
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    # Should succeed without warning
    with patch.object(handler.logger, "warning") as mock_warn:
        data_read = handler._read_file_data(str(f))
        assert data_read is not None
        mock_warn.assert_not_called()


def test_parse_json_with_comments(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that JSON with comments (invalid in standard JSON) is handled gracefully."""
    f = temp_dir / "comments.json"
    f.write_text('{\n  "current_stage": "Test",\n  // This is a comment\n  "current_task": "Task"\n}')
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    with patch("time.sleep"): # Skip backoff
        with patch.object(handler.logger, "debug") as mock_debug:
            data = handler._read_file_data(str(f))
            assert data is None
            # Should catch JSONDecodeError
            assert any("Malformed JSON" in str(c) for c in mock_debug.call_args_list)


def test_parse_json_trailing_commas(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that JSON with trailing commas (invalid) is handled."""
    f = temp_dir / "trailing.json"
    f.write_text('{"current_stage": "Test", "current_task": "Task",}')
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    with patch("time.sleep"):
        with patch.object(handler.logger, "debug") as mock_debug:
            data = handler._read_file_data(str(f))
            assert data is None
            assert any("Malformed JSON" in str(c) for c in mock_debug.call_args_list)


def test_debounce_zero_seconds_immediate_execution(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that debounce_seconds=0.0 results in immediate execution."""
    f = temp_dir / "zero_debounce.json"
    f.touch()
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0, debounce_seconds=0.0)
    
    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)
    
    with patch("threading.Timer") as mock_timer_cls:
        handler.on_modified(event)
        
        # Should still use Timer with 0.0 seconds
        mock_timer_cls.assert_called_once()
        args = mock_timer_cls.call_args
        assert args[0][0] == 0.0
        mock_timer_cls.return_value.start.assert_called_once()


def test_parse_json_nan_handling(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that NaN values (valid in Python json, invalid in standard JSON) are handled."""
    f = temp_dir / "nan.json"
    f.write_text('{"current_stage": NaN, "current_task": "Task"}')
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    # json.loads accepts NaN by default
    data = handler._read_file_data(str(f))
    assert data is not None
    
    # But _process_state_change expects strings
    with patch.object(handler.logger, "warning") as mock_warn:
        result = handler._process_state_change(data, str(f))
        assert result is None
        assert "current_stage must be a string" in mock_warn.call_args[0][0]


def test_parse_json_recursion_small_file(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test recursion error on a file that is small in bytes but deep in nesting."""
    f = temp_dir / "deep_small.json"
    # Create a file that is small enough to pass size check but deep enough to crash default recursion limit
    depth = 1200
    content = '[' * depth + ']' * depth
    f.write_text(content)
    
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    # Temporarily lower recursion limit to ensure we trigger the error
    original_limit = sys.getrecursionlimit()
    sys.setrecursionlimit(500)
    try:
        with patch.object(handler.logger, "error") as mock_error:
            # Should return None and log error
            assert handler._read_file_data(str(f)) is None
            
            # Verify it was RecursionError
            assert any("recursion limit exceeded" in str(c) for c in mock_error.call_args_list)
    finally:
        sys.setrecursionlimit(original_limit)


def test_read_file_data_io_error_on_close(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test handling of IOError when closing the file (context manager exit)."""
    f = temp_dir / "close_fail.json"
    f.write_text("{}")
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0)
    
    # Mock open to return a file whose __exit__ raises OSError
    with patch("pathlib.Path.open") as mock_open:
        mock_file = MagicMock()
        mock_file.__enter__.return_value.read.return_value = "{}"
        mock_file.__exit__.side_effect = OSError("Disk full on close")
        mock_open.return_value = mock_file
        
        with patch("time.sleep"): # Skip backoff
            with patch.object(handler.logger, "error") as mock_error:
                # Should catch the error
                assert handler._read_file_data(str(f)) is None
                assert any("Error processing" in str(c) for c in mock_error.call_args_list)


def test_large_valid_json_rejection(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that a valid JSON file exceeding the size limit is rejected."""
    f = temp_dir / "large_valid.json"
    # Create valid JSON > 10KB
    # {"key": "..."}
    # Overhead is ~10 chars. Content needs to be > 10240.
    data = {"key": "a" * (10 * 1024 + 100)}
    f.write_text(json.dumps(data))

    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    with patch.object(handler.logger, "warning") as mock_warn:
        assert handler._read_file_data(str(f)) is None
        # Should verify warning log about size
        assert any("too large" in str(c) for c in mock_warn.call_args_list)


def test_malformed_utf8_middle_of_file(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test handling of invalid UTF-8 bytes in the middle of the file."""
    f = temp_dir / "bad_utf8.json"
    # Valid start, bad byte 0xFF, valid end
    with open(f, "wb") as binary:
        binary.write(b'{"key": "val\xffue"}')

    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    with patch("time.sleep"):  # Skip backoff
        with patch.object(handler.logger, "warning") as mock_warn:
            assert handler._read_file_data(str(f)) is None
            assert any("Encoding error" in str(c) for c in mock_warn.call_args_list)


def test_unexpected_types_mixed_validity(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test state processing with mixed valid and invalid types."""
    f = temp_dir / "mixed_types.json"
    # Stage is invalid (int), Task is valid (str)
    data = {"current_stage": 12345, "current_task": "Valid Task"}
    f.write_text(json.dumps(data))

    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    # Read should succeed
    read_data = handler._read_file_data(str(f))
    assert read_data is not None

    # Process should fail
    with patch.object(handler.logger, "warning") as mock_warn:
        result = handler._process_state_change(read_data, str(f))
        assert result is None
        assert any("current_stage must be a string" in str(c) for c in mock_warn.call_args_list)


def test_debounce_rapid_events_timer_args(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Verify that the debounce timer is initialized with correct args."""
    f = temp_dir / "timer_args.json"
    f.touch()

    handler = PipelineEventHandler(f, pipeline_client, 120.0, debounce_seconds=0.5)
    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)

    with patch("threading.Timer") as mock_timer_cls:
        handler.on_modified(event)

        mock_timer_cls.assert_called_once()
        args, kwargs = mock_timer_cls.call_args
        assert args[0] == 0.5  # interval
        assert args[1] == handler._parse_file_wrapper  # function
        assert kwargs["args"] == [str(handler.target_file)]  # args


def test_debounce_rapid_events_timing_simulation(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """
    Simulate rapid events and verify timer reset logic using manual time advancement.
    Mock 100 events in <1s -> verify only 1 processed.
    """
    f = temp_dir / "rapid_timing.json"
    f.touch()
    
    # Debounce 2.0s
    handler = PipelineEventHandler(f, pipeline_client, pulsetime=120.0, debounce_seconds=2.0)
    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)
    
    # Mock Timer to capture instances
    timers = []
    class MockTimer:
        def __init__(self, interval, function, args=None, kwargs=None):
            self.interval = interval
            self.function = function
            self.args = args or []
            self.kwargs = kwargs or {}
            self.cancelled = False
            
        def cancel(self):
            self.cancelled = True
            
        def start(self):
            timers.append(self)
            
    with patch("threading.Timer", side_effect=MockTimer):
        with patch("aw_watcher_pipeline_stage.watcher.time.monotonic") as mock_time:
            start_time = 1000.0
            mock_time.return_value = start_time
            
            # Fire 100 events over 1 second (every 0.01s)
            for i in range(100):
                mock_time.return_value = start_time + (i * 0.01)
                handler.on_modified(event)
                
            # We should have created 100 timers
            assert len(timers) == 100
            
            # All but the last should be cancelled
            for t in timers[:-1]:
                assert t.cancelled
            assert not timers[-1].cancelled
            
            # Now simulate time passing to 1003.0 and execute the last timer
            mock_time.return_value = 1003.0
            last_timer = timers[-1]
            
            # Mock read/process
            with patch.object(handler, "_read_file_data", return_value={"current_stage": "Final", "current_task": "T"}):
                with patch.object(handler, "on_state_changed") as mock_change:
                    # Mock current_thread to match timer
                    with patch("threading.current_thread", return_value=last_timer):
                        last_timer.function(*last_timer.args, **last_timer.kwargs)
                        
                        mock_change.assert_called_once()
                        # Verify debounce counter
                        assert handler.total_debounced_events == 99


def test_process_state_change_strict_types_subclass(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that subclasses of str are accepted in state change."""
    f = temp_dir / "types.json"
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    class MyStr(str):
        pass
        
    data = {
        "current_stage": MyStr("Stage"),
        "current_task": MyStr("Task"),
        "status": MyStr("in_progress")
    }
    
    # Should be accepted
    result = handler._process_state_change(data, str(f))
    assert result is not None
    assert result["current_stage"] == "Stage"
    assert result["current_task"] == "Task"


def test_parse_json_unescaped_control_chars(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that unescaped control characters (invalid JSON) are handled."""
    f = temp_dir / "control.json"
    # Newline inside string is invalid in JSON
    f.write_text('{"key": "line\nbreak"}')
    
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    with patch("time.sleep"):
        with patch.object(handler.logger, "debug") as mock_debug:
            assert handler._read_file_data(str(f)) is None
            assert any("Malformed JSON" in str(c) for c in mock_debug.call_args_list)


def test_read_file_data_race_dir_conversion(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test handling of IsADirectoryError raised during open() (race condition)."""
    f = temp_dir / "race_dir.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    # Mock stat to succeed (return file), but open to raise IsADirectoryError
    with patch.object(Path, "stat") as mock_stat:
        mock_stat.return_value.st_mode = stat.S_IFREG
        mock_stat.return_value.st_size = 10
        
        with patch.object(Path, "open", side_effect=IsADirectoryError("Is a directory")):
            with patch.object(handler.logger, "warning") as mock_warn:
                data = handler._read_file_data(str(f))
                assert data is None
                mock_warn.assert_called_with(f"Target path is a directory, skipping parse: {f}")


def test_parse_file_wrapper_timer_cleanup_on_error(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that _timer is cleared even if an error occurs in the wrapper."""
    f = temp_dir / "error.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    # Set a dummy timer object to simulate active state
    handler._timer = threading.current_thread() # type: ignore
    
    # Mock _read_file_data to raise exception
    with patch.object(handler, "_read_file_data", side_effect=RuntimeError("Boom")):
        with patch.object(handler.logger, "error"): # Suppress log
            handler._parse_file_wrapper(str(f))
            
    # Timer should be cleared
    assert handler._timer is None


def test_process_state_change_project_id_non_string(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that non-string project_id is ignored and warned."""
    f = temp_dir / "test.json"
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    data = {"current_stage": "S", "current_task": "T", "project_id": 12345}
    
    with patch.object(handler.logger, "warning") as mock_warn:
        result = handler._process_state_change(data, str(f))
        
        assert result is not None
        assert result["project_id"] is None
        assert any("project_id must be a string" in str(c) for c in mock_warn.call_args_list)


def test_process_state_change_status_normalization(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that status is normalized to lowercase."""
    f = temp_dir / "test.json"
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    data = {"current_stage": "S", "current_task": "T", "status": "In_Progress"}
    
    result = handler._process_state_change(data, str(f))
    assert result is not None
    assert result["status"] == "in_progress"


def test_read_file_data_permission_error_throttling(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that PermissionError logging is throttled."""
    f = temp_dir / "perm.json"
    f.write_text("{}")
    handler = PipelineEventHandler(f, pipeline_client, 120.0)
    
    with patch("time.sleep"): # Skip backoff
        with patch.object(Path, "open", side_effect=PermissionError("Access denied")):
            with patch.object(handler.logger, "error") as mock_error:
                # 1. First call - logs error
                handler._read_file_data(str(f))
                assert mock_error.call_count == 1
                
                # 2. Second call immediately - no log
                handler._read_file_data(str(f))
                assert mock_error.call_count == 1
                
                # 3. Third call after delay - logs error
                handler.last_read_error_time -= 61.0
                handler._read_file_data(str(f))
                assert mock_error.call_count == 2


def test_large_file_rejection_metrics(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that large file rejection does not increment parse_errors (as it's a pre-check)."""
    f = temp_dir / "large_metrics.json"
    # Write content larger than 10KB
    f.write_text("x" * (10 * 1024 + 100))

    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    with patch.object(handler.logger, "warning") as mock_warn:
        handler._read_file_data(str(f))
        # Should verify warning log
        assert any("too large" in str(c) for c in mock_warn.call_args_list)

    # Should be 0 because it's skipped before parsing attempt
    assert handler.parse_errors == 0


def test_malformed_utf8_metrics(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that invalid UTF-8 increments parse_errors."""
    f = temp_dir / "utf8_metrics.json"
    with open(f, "wb") as binary:
        binary.write(b"\x80\x81")

    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    with patch("time.sleep"):  # Skip backoff
        with patch.object(handler.logger, "warning"):
            handler._read_file_data(str(f))

    assert handler.parse_errors == 1


def test_recursion_error_metrics(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that recursion error increments parse_errors."""
    f = temp_dir / "recursion_metrics.json"
    f.write_text("{}")  # Content doesn't matter as we mock json.loads

    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    with patch("aw_watcher_pipeline_stage.watcher.json.loads", side_effect=RecursionError("Boom")):
        with patch("time.sleep"):
            with patch.object(handler.logger, "error"):
                handler._read_file_data(str(f))

    assert handler.parse_errors == 1


def test_unexpected_types_metrics(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that invalid types for required fields increment parse_errors."""
    f = temp_dir / "types_metrics.json"
    data = {"current_stage": 123, "current_task": "Task"}

    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    with patch.object(handler.logger, "warning"):
        handler._process_state_change(data, str(f))

    assert handler.parse_errors == 1


def test_debounce_load_timer_usage(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that rapid events use threading.Timer and do not block (no sleep)."""
    f = temp_dir / "load.json"
    f.touch()

    handler = PipelineEventHandler(f, pipeline_client, 120.0, debounce_seconds=1.0)
    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)

    with patch("threading.Timer") as mock_timer_cls:
        with patch("time.sleep") as mock_sleep:
            # Fire 100 events
            for _ in range(100):
                handler.on_modified(event)

            # Should not sleep (blocking)
            mock_sleep.assert_not_called()

            # Should have created 100 timers (cancelled previous ones)
            assert mock_timer_cls.call_count == 100


def test_stress_large_json_boundary(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test exact boundary conditions for file size limit."""
    f = temp_dir / "boundary.json"
    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    # 1. Exactly MAX_FILE_SIZE_BYTES
    # We use a valid JSON structure to avoid JSONDecodeError
    # {"a": "..."} -> 10 bytes overhead.
    padding = MAX_FILE_SIZE_BYTES - 10
    content_exact = '{"a": "' + ("x" * padding) + '"}'
    assert len(content_exact) == MAX_FILE_SIZE_BYTES
    f.write_text(content_exact)

    with patch.object(handler.logger, "warning") as mock_warn:
        data = handler._read_file_data(str(f))
        assert data is not None
        # Should NOT log "too large"
        assert not any("too large" in str(c) for c in mock_warn.call_args_list)

    # 2. MAX_FILE_SIZE_BYTES + 1
    content_over = content_exact + " "
    f.write_text(content_over)

    with patch.object(handler.logger, "warning") as mock_warn:
        data = handler._read_file_data(str(f))
        assert data is None
        assert any("too large" in str(c) for c in mock_warn.call_args_list)


def test_stress_malformed_utf8_sequences(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test various malformed UTF-8 sequences."""
    f = temp_dir / "utf8_stress.json"
    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    sequences = [
        b"\xff",              # Invalid start byte
        b"\xc0\xaf",          # Overlong encoding
        b"\xe0\x80\x80",      # Overlong encoding
        b"\xf0\x80\x80\x80",  # Overlong encoding
        b"\xed\xa0\x80",      # Surrogate half
    ]

    with patch("time.sleep"):  # Skip backoff
        for seq in sequences:
            with open(f, "wb") as binary:
                binary.write(seq)

            # Reset error tracking to ensure log is emitted
            handler.last_read_error_time = 0.0

            with patch.object(handler.logger, "warning") as mock_warn:
                assert handler._read_file_data(str(f)) is None
                # Should eventually log warning
                assert any("Encoding error" in str(c) for c in mock_warn.call_args_list)


def test_stress_debounce_flood_drop(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that flooding events triggers rate limiting drops."""
    f = temp_dir / "flood.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)

    # Reset tokens to a known low value
    handler._rate_limit_tokens = 5.0
    handler._rate_limit_max = 100.0

    # Freeze time so tokens don't refill
    with patch("aw_watcher_pipeline_stage.watcher.time.monotonic", return_value=1000.0):
        # Fire 10 events. First 5 should pass (tokens 5->0). Next 5 should drop.
        for _ in range(10):
            handler.on_modified(event)

    assert handler._dropped_events >= 5


def test_parse_file_wrapper_state_consistency(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that state remains consistent if read fails inside wrapper."""
    f = temp_dir / "wrapper_fail.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    # Set initial state
    handler.last_stage = "Initial"

    # Mock read to return None (failure)
    with patch.object(handler, "_read_file_data", return_value=None):
        # Mock timer identity
        handler._timer = threading.current_thread()  # type: ignore

        handler._parse_file_wrapper(str(f))

        # State should be unchanged
        assert handler.last_stage == "Initial"
        # Timer should be cleared
        assert handler._timer is None


@pytest.mark.parametrize("mode", [stat.S_IFBLK, stat.S_IFCHR, stat.S_IFIFO])
def test_read_file_data_non_regular_files_parametrized(pipeline_client: PipelineClient, temp_dir: Path, mode: int) -> None:
    """Test that various non-regular files (Block, Char, FIFO) are skipped."""
    f = temp_dir / "special_file"
    f.touch()

    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    with patch.object(Path, "stat") as mock_stat:
        mock_stat.return_value.st_mode = mode

        with patch.object(handler.logger, "warning") as mock_warn:
            assert handler._read_file_data(str(f)) is None
            assert "not a regular file" in mock_warn.call_args[0][0]


def test_process_state_change_metadata_nested_preservation(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that nested metadata dictionaries are preserved as-is in watcher state."""
    f = temp_dir / "test.json"
    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    metadata = {"config": {"nested": {"deep": "value"}}}
    data = {"current_stage": "S", "current_task": "T", "metadata": metadata}

    result = handler._process_state_change(data, str(f))
    assert result is not None
    assert result["metadata"] == metadata


def test_process_state_change_project_id_whitespace(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that project_id whitespace is stripped."""
    f = temp_dir / "test.json"
    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    data = {"current_stage": "S", "current_task": "T", "project_id": "  my-project  "}

    result = handler._process_state_change(data, str(f))
    assert result is not None
    assert result["project_id"] == "my-project"


def test_process_state_change_start_time_whitespace(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that start_time whitespace is stripped."""
    f = temp_dir / "test.json"
    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    data = {"current_stage": "S", "current_task": "T", "start_time": "  2023-01-01T12:00:00Z  "}

    result = handler._process_state_change(data, str(f))
    assert result is not None
    assert result["start_time"] == "2023-01-01T12:00:00Z"


def test_debounce_timer_daemon_status(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that the debounce timer is set as a daemon thread."""
    f = temp_dir / "test.json"
    f.touch()
    handler = PipelineEventHandler(f, pipeline_client, 120.0, debounce_seconds=1.0)

    event = MagicMock()
    event.is_directory = False
    event.src_path = str(f)

    with patch("threading.Timer") as mock_timer_cls:
        handler.on_modified(event)

        mock_timer_instance = mock_timer_cls.return_value
        assert mock_timer_instance.daemon is True


def test_read_file_data_caching(pipeline_client: PipelineClient, temp_dir: Path) -> None:
    """Test that _read_file_data uses cache when mtime/size are unchanged."""
    f = temp_dir / "cache.json"
    f.write_text(json.dumps({"key": "val"}))

    handler = PipelineEventHandler(f, pipeline_client, 120.0)

    # 1. First read - should open file
    with patch.object(Path, "open", wraps=Path(f).open) as mock_open:
        data1 = handler._read_file_data(str(f))
        assert data1 == {"key": "val"}
        mock_open.assert_called_once()

    # 2. Second read - unchanged file - should skip open (cache hit)
    with patch.object(Path, "open", wraps=Path(f).open) as mock_open:
        data2 = handler._read_file_data(str(f))
        assert data2 == {"key": "val"}
        mock_open.assert_not_called()

    # 3. Modify file (mtime/size changes)
    # Ensure mtime changes by sleeping slightly if needed, but write usually updates it
    time.sleep(0.01)
    f.write_text(json.dumps({"key": "val2"}))

    # 4. Third read - changed file - should open
    with patch.object(Path, "open", wraps=Path(f).open) as mock_open:
        data3 = handler._read_file_data(str(f))
        assert data3 == {"key": "val2"}
        mock_open.assert_called_once()