"""
Edge Case Test Script for aw-watcher-pipeline-stage.

Verifies Directive 5 Requirements:
1. Initial Missing File: Warning + Wait.
2. Malformed JSON: Log ERROR, skip heartbeat, no crash.
3. File Deletion: Log WARNING, pause heartbeats, continue watching.
4. Recovery: Resume on valid write/recreation.
"""

import json
import logging
import shutil
import time
from pathlib import Path
from typing import Any

from aw_watcher_pipeline_stage.client import MockActivityWatchClient, PipelineClient
from aw_watcher_pipeline_stage.watcher import PipelineWatcher

# Setup logging
logging.basicConfig(level=logging.INFO, format="[TEST] %(message)s")
logger = logging.getLogger("test_edge_cases")

TEST_DIR = Path("./test_env_edge")
TEST_FILE = TEST_DIR / "current_task.json"


def setup() -> None:
    if TEST_DIR.exists():
        shutil.rmtree(TEST_DIR)
    TEST_DIR.mkdir()
    logger.info(f"Created test directory: {TEST_DIR}")


def cleanup() -> None:
    if TEST_DIR.exists():
        shutil.rmtree(TEST_DIR)
    logger.info("Cleaned up test directory.")


def write_file(content: str) -> None:
    with open(TEST_FILE, "w") as f:
        f.write(content)


def main() -> None:
    setup()
    
    logger.info(">>> STARTING EDGE CASE VERIFICATION <<<")
    
    # Initialize Client and Watcher
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=TEST_FILE, client=mock_aw_client, testing=True)
    
    # Use 0.5s debounce for faster testing
    watcher = PipelineWatcher(TEST_FILE, client, pulsetime=120.0, debounce_seconds=0.5)

    try:
        # --- TEST 1: Initial Missing File ---
        logger.info("\n[TEST 1] Initial Missing File")
        logger.info("Starting watcher while file does not exist...")
        watcher.start()
        logger.info(">>> VERIFY: Check logs above for 'Target file not found at startup... Waiting for creation'")
        time.sleep(1.0)

        # --- TEST 2: Creation & Valid Write ---
        logger.info("\n[TEST 2] File Creation (Recovery)")
        write_file(json.dumps({
            "current_stage": "Stage 1",
            "current_task": "Valid Task",
            "status": "in_progress"
        }))
        time.sleep(1.0)
        logger.info(">>> VERIFY: Heartbeat sent for 'Valid Task'")

        # --- TEST 3: Malformed JSON ---
        logger.info("\n[TEST 3] Malformed JSON")
        write_file("{ invalid_json: ")
        time.sleep(1.0)
        logger.info(">>> VERIFY: Log ERROR 'Malformed JSON', NO crash, NO heartbeat change.")

        # --- TEST 4: Recovery from Malformed ---
        logger.info("\n[TEST 4] Recovery from Malformed")
        write_file(json.dumps({
            "current_stage": "Stage 1",
            "current_task": "Recovered Task",
            "status": "in_progress"
        }))
        time.sleep(1.0)
        logger.info(">>> VERIFY: Heartbeat sent for 'Recovered Task'")

        # --- TEST 5: File Deletion ---
        logger.info("\n[TEST 5] File Deletion")
        TEST_FILE.unlink()
        time.sleep(1.0)
        logger.info(">>> VERIFY: Log WARNING 'File deleted', Status set to 'paused' (final heartbeat).")
        
        # Verify periodic heartbeats are paused
        logger.info("Simulating periodic check while deleted...")
        watcher.handler.last_heartbeat_time = time.time() - 31.0
        watcher.check_periodic_heartbeat()
        logger.info(">>> VERIFY: NO periodic heartbeat should appear above.")

        # --- TEST 6: Recreation ---
        logger.info("\n[TEST 6] File Recreation")
        write_file(json.dumps({
            "current_stage": "Stage 2",
            "current_task": "Recreated Task",
            "status": "in_progress"
        }))
        time.sleep(1.0)
        logger.info(">>> VERIFY: Heartbeat sent for 'Recreated Task'")

    finally:
        watcher.stop()
        cleanup()
        logger.info("\nTest complete.")

if __name__ == "__main__":
    main()