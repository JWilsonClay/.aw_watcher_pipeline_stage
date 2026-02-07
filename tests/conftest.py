from pathlib import Path
from typing import Callable, Generator, Union, cast
from unittest.mock import MagicMock, patch

import pytest

from aw_client import ActivityWatchClient
from watchdog.events import FileSystemEvent

from aw_watcher_pipeline_stage.client import MockActivityWatchClient, PipelineClient
from aw_watcher_pipeline_stage.config import Config


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Fixture for a temporary directory using pytest's tmp_path."""
    return tmp_path


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
