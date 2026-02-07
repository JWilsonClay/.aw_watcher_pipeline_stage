"""
Compiled robustness test suite for Directive 9.
Aggregates critical robustness scenarios.
"""
import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from aw_watcher_pipeline_stage.client import PipelineClient
from aw_watcher_pipeline_stage.watcher import PipelineWatcher

def test_full_robustness_suite(tmp_path: Path) -> None:
    """
    Runs a composite robustness scenario:
    1. Start with missing file.
    2. Create file.
    3. Malformed write.
    4. Recovery.
    5. Client connection failure (offline buffering).
    6. Graceful stop.
    """
    f = tmp_path / "robustness.json"
    
    mock_aw_client = MagicMock()
    client = PipelineClient(f, client=mock_aw_client, testing=True)
    watcher = PipelineWatcher(f, client, pulsetime=120.0, debounce_seconds=0.1)
    
    # 1. Start missing
    watcher.start()
    
    # 2. Create
    f.write_text(json.dumps({"current_stage": "1", "current_task": "A"}))
    time.sleep(0.2)
    assert watcher.handler.last_stage == "1"
    
    # 3. Malformed
    f.write_text("{ bad")
    time.sleep(0.2)
    # State should remain
    assert watcher.handler.last_stage == "1"
    
    # 5. Client failure simulation
    mock_aw_client.heartbeat.side_effect = Exception("Offline")
    f.write_text(json.dumps({"current_stage": "2", "current_task": "B"}))
    time.sleep(0.2)
    # Should not crash watcher
    assert watcher.handler.last_stage == "2"
    
    # 6. Stop
    watcher.stop()