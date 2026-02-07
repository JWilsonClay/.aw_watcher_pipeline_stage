"""
Performance validation script using psutil.
Measures CPU and Memory usage of the watcher process.
"""
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Dict

import psutil
import pytest

from aw_watcher_pipeline_stage.client import MockActivityWatchClient, PipelineClient
from aw_watcher_pipeline_stage.watcher import PipelineWatcher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_performance")

def measure_resources(duration: int, interval: float = 1.0) -> Dict[str, float]:
    """Measure CPU and Memory usage over a duration."""
    process = psutil.Process(os.getpid())
    cpu_readings = []
    rss_readings = []
    
    # Initial call to prime cpu_percent (returns 0.0)
    process.cpu_percent()
    
    steps = int(duration / interval)
    for _ in range(steps):
        # cpu_percent with interval blocks, so this acts as our sleep
        cpu = process.cpu_percent(interval=interval)
        mem = process.memory_info().rss / (1024 * 1024) # MB
        
        cpu_readings.append(cpu)
        rss_readings.append(mem)
        
    return {
        "cpu_avg": sum(cpu_readings) / len(cpu_readings) if cpu_readings else 0.0,
        "cpu_peak": max(cpu_readings) if cpu_readings else 0.0,
        "rss_avg": sum(rss_readings) / len(rss_readings) if rss_readings else 0.0,
        "rss_peak": max(rss_readings) if rss_readings else 0.0
    }

def simulate_activity(file_path: Path, stop_event: threading.Event, interval: float = 0.5) -> None:
    """Simulate file modifications."""
    counter = 0
    while not stop_event.is_set():
        try:
            data = {
                "current_stage": "Performance Test",
                "current_task": f"Task {counter}",
                "status": "in_progress",
                "metadata": {"ts": time.time()}
            }
            file_path.write_text(json.dumps(data))
            counter += 1
            # Sleep in small chunks to allow responsive stop
            start_sleep = time.time()
            while time.time() - start_sleep < interval and not stop_event.is_set():
                time.sleep(0.1)
        except Exception:
            pass

def simulate_main_loop(watcher: PipelineWatcher, stop_event: threading.Event) -> None:
    """Simulate the main loop's periodic checks to capture their CPU cost."""
    while not stop_event.is_set():
        try:
            watcher.check_periodic_heartbeat()
        except Exception:
            pass
        time.sleep(1.0)

@pytest.mark.skipif(os.environ.get("CI") == "true", reason="Skipping performance test in CI")
def test_performance_metrics(tmp_path: Path) -> None:
    """
    Run performance validation.
    
    Targets (Directive 3):
    - Idle CPU < 1% (over 5+ minutes)
    - Active CPU < 5%
    - RAM < 50MB
    """
    f = tmp_path / "perf_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))
    
    mock_client = MockActivityWatchClient()
    client = PipelineClient(f, client=mock_client, testing=True)
    # Use standard debounce
    watcher = PipelineWatcher(f, client, pulsetime=120.0, debounce_seconds=1.0)
    
    watcher.start()
    
    # Start main loop simulation
    main_loop_stop = threading.Event()
    main_loop_thread = threading.Thread(target=simulate_main_loop, args=(watcher, main_loop_stop))
    main_loop_thread.start()
    
    try:
        # 1. Idle Mode
        logger.info("Starting Idle Mode Measurement (10s sample for test)...")
        # Note: Using 10s for automated test speed, manual run can use 60s
        idle_stats = measure_resources(duration=10)
        
        logger.info(f"Idle Stats: CPU Avg={idle_stats['cpu_avg']:.2f}% (Target <1%), Peak={idle_stats['cpu_peak']:.2f}% | RSS Peak={idle_stats['rss_peak']:.2f}MB (Target <50MB)")
        
        # 2. Active Mode
        logger.info("Starting Active Mode Measurement (10s sample for test)...")
        stop_event = threading.Event()
        t = threading.Thread(target=simulate_activity, args=(f, stop_event, 0.5))
        t.start()
        
        active_stats = measure_resources(duration=10)
        
        stop_event.set()
        t.join()
        
        logger.info(f"Active Stats: CPU Avg={active_stats['cpu_avg']:.2f}%, Peak={active_stats['cpu_peak']:.2f}% | RSS Peak={active_stats['rss_peak']:.2f}MB")
        
        # Assertions
        # RSS should be stable
        assert idle_stats['rss_peak'] < 50.0
        assert active_stats['rss_peak'] < 50.0
        assert idle_stats['cpu_avg'] < 1.0
        assert active_stats['cpu_avg'] < 5.0
        
    finally:
        main_loop_stop.set()
        main_loop_thread.join()
        watcher.stop()

if __name__ == "__main__":
    # Manual execution block for full run (default 300s for idle validation)
    import shutil
    import tempfile
    import argparse
    import sys
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=300, help="Duration in seconds (Default: 300s for Directive 3)")
    parser.add_argument("--scenario", choices=["idle", "active", "pacing"], default="pacing", help="Test scenario: idle, active (rapid 0.5s), pacing (30s - Directive 3)")
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
    
    # Ensure package is in path for standalone execution
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    
    tmp_dir = Path(tempfile.mkdtemp())
    try:
        f = tmp_dir / "manual_perf.json"
        f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))
        print(f"Test PID: {os.getpid()}")
        print(f"Scenario: {args.scenario.upper()} | Duration: {args.duration}s")
        
        mock_client = MockActivityWatchClient()
        client = PipelineClient(f, client=mock_client, testing=True)
        watcher = PipelineWatcher(f, client, pulsetime=120.0, debounce_seconds=1.0)
        watcher.start()
        
        main_loop_stop = threading.Event()
        main_loop_thread = threading.Thread(target=simulate_main_loop, args=(watcher, main_loop_stop))
        main_loop_thread.start()
        
        activity_thread = None
        stop_activity = threading.Event()
        
        if args.scenario == "active":
            print("Starting rapid activity (0.5s interval)...")
            activity_thread = threading.Thread(target=simulate_activity, args=(f, stop_activity, 0.5))
            activity_thread.start()
        elif args.scenario == "pacing":
            print("Starting pacing activity (30s interval)...")
            activity_thread = threading.Thread(target=simulate_activity, args=(f, stop_activity, 30.0))
            activity_thread.start()
        
        stats = measure_resources(duration=args.duration)
        print(f"Results: {stats}")
        
        if activity_thread:
            stop_activity.set()
            activity_thread.join()
        
        # Directive 3: Verify Pulsetime and Heartbeats
        events = mock_client.events
        pulsetime_valid = True
        if events:
            pulsetimes = {e.get('pulsetime') for e in events}
            if not all(p == 120.0 for p in pulsetimes):
                pulsetime_valid = False

        # Check targets
        print("-" * 40)
        print(f"Performance Report ({args.scenario.upper()}):")
        print(f"  Duration: {args.duration}s")
        print(f"  Heartbeats: {len(events)}")
        print(f"  Pulsetime OK: {pulsetime_valid} (Values: {set(e.get('pulsetime') for e in events) if events else 'None'})")
        print(f"  CPU Avg:  {stats['cpu_avg']:.2f}% (Target: <5.0%)")
        print(f"  RSS Peak: {stats['rss_peak']:.2f}MB (Target: <50.0MB)")
        print("-" * 40)

        failures = []
        if stats['cpu_avg'] >= 5.0:
            failures.append(f"CPU Avg {stats['cpu_avg']:.2f}% >= 5.0%")
        if stats['rss_peak'] >= 50.0:
            failures.append(f"RSS Peak {stats['rss_peak']:.2f}MB >= 50.0MB")
        if not pulsetime_valid:
            failures.append(f"Pulsetime verification failed. Expected {{120.0}}, got {set(e.get('pulsetime') for e in events)}")
            
        if failures:
            print("FAILURES DETECTED:")
            for fail in failures:
                print(f"  - {fail}")
            sys.exit(1)
        else:
            print("SUCCESS: Performance targets met.")
            sys.exit(0)
        
        main_loop_stop.set()
        main_loop_thread.join()
        watcher.stop()
        
    finally:
        shutil.rmtree(tmp_dir)