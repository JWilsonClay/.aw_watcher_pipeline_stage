"""
Final robustness tests for aw-watcher-pipeline-stage.
"""
import logging
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest
from aw_watcher_pipeline_stage.client import PipelineClient
from aw_watcher_pipeline_stage.watcher import PipelineWatcher

def test_watcher_stop_exception_handling(tmp_path: Path) -> None:
    """Test that watcher.stop() handles exceptions from observer gracefully."""
    f = tmp_path / "test.json"
    f.touch()
    
    mock_client = MagicMock()
    watcher = PipelineWatcher(f, mock_client, 120.0)
    
    # Mock observer
    watcher._observer = MagicMock()
    watcher._observer.is_alive.return_value = True
    watcher._observer.stop.side_effect = RuntimeError("Observer stop failed")
    
    with patch("aw_watcher_pipeline_stage.watcher.logger") as mock_logger:
        # Should not raise exception
        watcher.stop()
        
        # Should log error
        mock_logger.error.assert_called_with("Error stopping observer: RuntimeError('Observer stop failed')")

def test_stat_race_condition(tmp_path: Path) -> None:
    """Test race condition where file disappears during stat."""
    f = tmp_path / "race.json"
    f.touch()
    
    mock_client = MagicMock()
    watcher = PipelineWatcher(f, mock_client, 120.0)
    
    # Mock path.stat to raise FileNotFoundError (simulating race)
    with patch("pathlib.Path.stat", side_effect=FileNotFoundError("File gone")):
        with patch("time.sleep"): # Skip backoff
             data = watcher.handler._read_file_data(str(f))
             assert data is None

def test_negative_duration_warning(tmp_path: Path) -> None:
    """Test that negative computed_duration logs a warning."""
    f = tmp_path / "test.json"
    mock_client = MagicMock()
    # We need to mock logger to check warning
    with patch("aw_watcher_pipeline_stage.client.logger") as mock_logger:
        client = PipelineClient(f, client=mock_client, testing=True)
        client.send_heartbeat("Stage", "Task", computed_duration=-5.0)
        
        mock_logger.warning.assert_called()
        assert "Computed duration is negative" in str(mock_logger.warning.call_args)