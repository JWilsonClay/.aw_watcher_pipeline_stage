"""
Manual test script for aw-watcher-pipeline-stage.

Executes Directive 4 Scenarios:
1. Normal Flow: Modify stage/task -> Verify immediate heartbeat.
2. Periodic: Simulate 30s wait -> Verify periodic heartbeat.
3. Rapid Writes: Write 3x quickly -> Verify debounce (1 event).
4. Status Changes: Set to paused -> Verify final event & stop.
5. Malformed JSON: Write invalid JSON -> Verify error log & no crash.
6. File Deletion: Delete file -> Verify warning & paused state.
7. Recreation: Recreate file -> Verify resumption.
"""

import argparse
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
logger = logging.getLogger("manual_test")

TEST_DIR = Path("./test_env")
TEST_FILE = TEST_DIR / "current_task.json"


def setup() -> None:
    """Prepare the test environment by creating the test directory."""
    if TEST_DIR.exists():
        shutil.rmtree(TEST_DIR)
    TEST_DIR.mkdir()
    logger.info(f"Created test directory: {TEST_DIR}")


def cleanup() -> None:
    """Clean up the test environment by removing the test directory."""
    if TEST_DIR.exists():
        shutil.rmtree(TEST_DIR)
    logger.info("Cleaned up test directory.")


def write_json(data: Any) -> None:
    """Write data to the test file.

    Args:
        data (Any): The data to write (dict or str).
    """
    if isinstance(data, str):
        with open(TEST_FILE, "w") as f:
            f.write(data)
        logger.info(f"Wrote raw data: {data}")
        return

    try:
        with open(TEST_FILE, "w") as f:
            json.dump(data, f)
        logger.info(f"Wrote to file: {data.get('current_task', 'UNKNOWN')}")
    except Exception as e:
        logger.error(f"Failed to write: {e}")

def run_stability_test() -> None:
    """Run a 30-minute stability test."""
    setup()
    logger.info(">>> STARTING STABILITY TEST (30 MINUTES) <<<")
    
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=TEST_FILE, client=mock_aw_client, testing=True)
    watcher = PipelineWatcher(TEST_FILE, client, pulsetime=120.0, debounce_seconds=1.0)
    
    try:
        write_json({"current_stage": "Stability", "current_task": "Start"})
        watcher.start()
        
        start_time = time.time()
        duration = 30 * 60  # 30 minutes
        
        while time.time() - start_time < duration:
            elapsed = time.time() - start_time
            remaining = duration - elapsed
            
            # Log stats every minute
            stats = watcher.get_statistics()
            logger.info(f"Elapsed: {elapsed/60:.1f}m | Heartbeats: {stats['heartbeats_sent']} | MaxHBInt: {stats.get('max_heartbeat_interval', 0):.1f}s | RSS: Check system monitor")
            
            # Simulate a file change every 5 minutes
            if int(elapsed) % 300 == 0 and int(elapsed) > 0:
                logger.info(">>> Simulating file update...")
                write_json({"current_stage": "Stability", "current_task": f"Update {int(elapsed)}s"})
            
            time.sleep(60)
            
        logger.info(">>> STABILITY TEST COMPLETE <<<")
        logger.info(f"Final Stats: {watcher.get_statistics()}")
        
    except KeyboardInterrupt:
        logger.info("Stability test interrupted.")
    finally:
        watcher.stop()
        cleanup()

def main() -> None:
    """Execute the manual test scenarios."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--stability", action="store_true", help="Run stability test (30m)")
    args = parser.parse_args()

    if args.stability:
        run_stability_test()
        return

    setup()

    logger.info(">>> INITIALIZING WATCHER (TESTING MODE) <<<")
    # Initialize Client and Watcher
    mock_aw_client = MockActivityWatchClient()
    client = PipelineClient(watch_path=TEST_FILE, client=mock_aw_client, testing=True)
    
    # Use 1.0s debounce as per requirements
    watcher = PipelineWatcher(TEST_FILE, client, pulsetime=120.0, debounce_seconds=1.0)

    try:
        # --- SCENARIO 0: Initial Missing File ---
        logger.info("\n[STEP 0] TEST INITIAL MISSING FILE: Starting watcher without file...")
        if TEST_FILE.exists():
            TEST_FILE.unlink()
        
        watcher.start()
        logger.info(">>> VERIFY: Warning logged above about missing file.")
        time.sleep(1.0)

        # --- PRE-REQUISITE: Create Sample File ---
        logger.info("\n[STEP 0.5] Creating initial sample file...")
        write_json({
            "current_stage": "Stage 1 - Setup",
            "current_task": "Initial Setup",
            "project_id": "demo-pipeline",
            "status": "in_progress",
            "start_time": "2026-02-04T20:30:00Z"
        })
        
        time.sleep(1.5) # Allow initial read + debounce

        # --- SCENARIO 1: Normal Flow (Modify stage/task) ---
        logger.info("\n[STEP 1] TEST NORMAL FLOW: Modifying task...")
        write_json({
            "current_stage": "Stage 2 - Development",
            "current_task": "Implement Core Logic",
            "project_id": "demo-pipeline",
            "status": "in_progress"
        })
        logger.info(">>> VERIFY: Immediate heartbeat sent below (Stage 2)")
        time.sleep(1.5)  # Wait for debounce (1.0s) + processing

        # --- SCENARIO 2: Periodic Heartbeat ---
        logger.info("\n[STEP 2] TEST PERIODIC HEARTBEAT: Simulating 30s passing...")
        # Manually age the last heartbeat time to trigger periodic check
        watcher.handler.last_heartbeat_time = time.time() - 31.0
        watcher.check_periodic_heartbeat()
        logger.info(">>> VERIFY: Periodic heartbeat sent above")

        # --- SCENARIO 3: Rapid Writes (Debounce) ---
        logger.info("\n[STEP 3] TEST RAPID WRITES: Writing 3 times in 200ms...")
        write_json({"current_stage": "Stage 2", "current_task": "Typing...", "status": "in_progress"})
        time.sleep(0.1)
        write_json({"current_stage": "Stage 2", "current_task": "Typing more...", "status": "in_progress"})
        time.sleep(0.1)
        write_json({"current_stage": "Stage 2", "current_task": "Final Task Description", "status": "in_progress"})
        
        logger.info("Waiting for debounce (1.5s)...")
        time.sleep(1.5) 
        logger.info(">>> VERIFY: Only ONE heartbeat above (Final Task Description)")

        # --- SCENARIO 4: Status Changes (Pause) ---
        logger.info("\n[STEP 4] TEST STATUS CHANGE: Setting to 'paused'...")
        write_json({
            "current_stage": "Stage 2",
            "current_task": "Final Task Description",
            "status": "paused"
        })
        time.sleep(1.5)
        logger.info(">>> VERIFY: Final heartbeat sent above. Checking periodic silence...")

        watcher.handler.last_heartbeat_time = time.time() - 31.0
        watcher.check_periodic_heartbeat()
        logger.info(">>> VERIFY: NO heartbeat should appear after this line.")

        # --- SCENARIO 5: Malformed JSON ---
        logger.info("\n[STEP 5] TEST MALFORMED JSON: Writing invalid JSON...")
        write_json("INVALID { JSON")
        time.sleep(1.5)
        logger.info(">>> VERIFY: Error logged above, NO heartbeat sent, NO crash.")

        # --- SCENARIO 6: File Deletion ---
        logger.info("\n[STEP 6] TEST FILE DELETION: Deleting file...")
        if TEST_FILE.exists():
            TEST_FILE.unlink()
        time.sleep(1.5)
        logger.info(">>> VERIFY: Warning logged, 'paused' heartbeat sent (if previous state active).")

        # --- SCENARIO 7: File Recreation ---
        logger.info("\n[STEP 7] TEST RECREATION: Recreating file...")
        write_json({
            "current_stage": "Stage 3 - Testing",
            "current_task": "Resumed Task",
            "status": "in_progress"
        })
        time.sleep(1.5)
        logger.info(">>> VERIFY: Heartbeat sent above (Resumed Task).")

    except KeyboardInterrupt:
        pass
    finally:
        watcher.stop()
        cleanup()
        logger.info("Test complete.")

if __name__ == "__main__":
    main()