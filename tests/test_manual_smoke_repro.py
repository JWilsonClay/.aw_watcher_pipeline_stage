"""Codified manual smoke test for regression."""
import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
from aw_watcher_pipeline_stage.client import PipelineClient
from aw_watcher_pipeline_stage.watcher import PipelineWatcher

def test_manual_smoke_repro(tmp_path: Path) -> None:
    """
    Reproduce manual smoke test steps:
    1. Start watcher.
    2. Valid change -> Heartbeat.
    3. Offline -> Reconnect -> Flush.
    4. Verify Bucket/Event Spec.
    """
    f = tmp_path / "current_task.json"
    f.write_text(json.dumps({"current_stage": "Smoke", "current_task": "Start"}))
    
    mock_aw = MagicMock()
    # Mock hostname for deterministic bucket check
    with patch("socket.gethostname", return_value="smoke-host"):
        client = PipelineClient(f, client=mock_aw, testing=True)
        watcher = PipelineWatcher(f, client, debounce_seconds=0.1)
        
        watcher.start()
        # Ensure bucket created
        client.ensure_bucket()
        
        try:
            time.sleep(0.2)
            
            # Verify Bucket Spec
            mock_aw.create_bucket.assert_called()
            bucket_args = mock_aw.create_bucket.call_args
            assert bucket_args[0][0] == "aw-watcher-pipeline-stage_smoke-host"
            assert bucket_args[1]["event_type"] == "current-pipeline-stage"

            # 1. Valid Change
            f.write_text(json.dumps({"current_stage": "Smoke", "current_task": "Change"}))
            time.sleep(0.2)
            assert mock_aw.heartbeat.called
            event = mock_aw.heartbeat.call_args[0][1]
            _, kwargs = mock_aw.heartbeat.call_args
            assert event.data["task"] == "Change"
            assert event.data["stage"] == "Smoke"
            assert kwargs["queued"] is True
            
            # 2. Offline -> Reconnect
            # Simulate offline
            mock_aw.heartbeat.side_effect = ConnectionError("Offline")
            f.write_text(json.dumps({"current_stage": "Smoke", "current_task": "Offline"}))
            time.sleep(0.2) # Allow worker to fail and queue
            
            # Simulate reconnect (clear side effect)
            mock_aw.heartbeat.side_effect = None
            
            # Trigger flush (simulating shutdown or periodic flush)
            client.flush_queue()
            
            # Verify flush called
            assert mock_aw.flush.called
        finally:
            watcher.stop()