from pathlib import Path
import sys
from typing import Any, Callable, Generator, Union, cast
from unittest.mock import MagicMock, patch
import tempfile

import pytest

from aw_client import ActivityWatchClient
from watchdog.events import FileSystemEvent

from aw_watcher_pipeline_stage.client import MockActivityWatchClient, PipelineClient
from aw_watcher_pipeline_stage.config import Config


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Fixture for a temporary directory using tempfile.TemporaryDirectory.

    Ensures automatic cleanup after test execution.
    """
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield Path(tmpdirname)


@pytest.fixture
def sample_task_file(temp_dir: Path) -> Path:
    """Fixture for a populated current_task.json file."""
    f = temp_dir / "current_task.json"
    f.write_text('{"current_stage": "Test", "current_task": "Task"}', encoding="utf-8")
    return f


@pytest.fixture
def mock_aw_client() -> MagicMock:
    client = MagicMock()
    client.client_hostname = "test-host"
    return client


@pytest.fixture
def pipeline_client(temp_dir: Path, mock_aw_client: MagicMock) -> PipelineClient:
    # Use cast to satisfy strict type checking for the mock injection
    client_mock = cast(Union[ActivityWatchClient, MockActivityWatchClient], mock_aw_client)
    return PipelineClient(
        watch_path=temp_dir / "current_task.json", client=client_mock, testing=True
    )


@pytest.fixture
def mock_filesystem_event() -> MagicMock:
    """Fixture for a generic watchdog FileSystemEvent."""
    event = MagicMock(spec=FileSystemEvent)
    event.is_directory = False
    event.src_path = "/tmp/test/current_task.json"
    event.event_type = "modified"
    return event


@pytest.fixture
def mock_sleep(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Mock time.sleep to skip delays."""
    mock = MagicMock()
    monkeypatch.setattr("time.sleep", mock)
    return mock


@pytest.fixture
def mock_time(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Mock time.time to control clock."""
    mock = MagicMock(return_value=1000.0)
    monkeypatch.setattr("time.time", mock)
    return mock


@pytest.fixture
def mock_monotonic(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Mock time.monotonic for deterministic timing."""
    mock = MagicMock(return_value=1000.0)
    monkeypatch.setattr("time.monotonic", mock)
    return mock


@pytest.fixture
def mock_config() -> Config:
    """Fixture for a default Config object."""
    return Config(
        watch_path=".",
        port=5600,
        testing=True,
        log_file=None,
        log_level="INFO",
        pulsetime=120.0,
        debounce_seconds=1.0,
    )


@pytest.fixture
def mock_observer() -> Generator[MagicMock, None, None]:
    """Fixture for mocking the watchdog Observer."""
    with patch("aw_watcher_pipeline_stage.watcher.Observer") as mock:
        yield mock


@pytest.fixture
def mock_signal() -> Generator[MagicMock, None, None]:
    """Fixture for mocking signal.signal."""
    with patch("signal.signal") as mock:
        yield mock


@pytest.fixture
def cli_args(monkeypatch: pytest.MonkeyPatch) -> Callable[[list[str]], None]:
    """Fixture to mock command line arguments for CLI config overrides."""
    def _set_args(args: list[str]) -> None:
        monkeypatch.setattr("sys.argv", ["aw-watcher-pipeline-stage"] + args)
    return _set_args


@pytest.fixture
def mock_psutil(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Mock psutil for deterministic resource usage checks."""
    mock_psutil_mod = MagicMock()
    mock_psutil_mod.process_iter.return_value = []
    mock_psutil_mod.virtual_memory.return_value.percent = 15.5
    mock_psutil_mod.cpu_percent.return_value = 10.0

    # Mock Process class
    mock_process = MagicMock()
    mock_process.memory_info.return_value.rss = 1024 * 1024 * 50  # 50MB
    mock_psutil_mod.Process.return_value = mock_process

    monkeypatch.setitem(sys.modules, "psutil", mock_psutil_mod)
    return mock_psutil_mod


@pytest.fixture
def offline_aw_client(mock_aw_client: MagicMock) -> MagicMock:
    """Mock aw-client that simulates offline behavior (ConnectionError then success)."""
    # Fail 3 times, then succeed
    mock_aw_client.heartbeat.side_effect = [
        ConnectionError("Offline"),
        ConnectionError("Offline"),
        ConnectionError("Offline"),
        None
    ]
    return mock_aw_client


@pytest.fixture
def flood_events() -> Callable[[Any, int], None]:
    """Fixture to simulate event flooding on a handler."""
    def _flood(handler: Any, count: int) -> None:
        event = MagicMock()
        event.is_directory = False
        if hasattr(handler, "target_file"):
            event.src_path = str(handler.target_file)
        else:
            event.src_path = "test_file.json"
        for _ in range(count):
            handler.on_modified(event)
    return _flood
