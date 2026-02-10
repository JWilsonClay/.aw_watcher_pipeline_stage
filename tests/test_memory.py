#!/usr/bin/env python3
"""
Memory profiling script using tracemalloc.

Profiles key loops in watcher.py and client.py for memory leaks.
"""
import argparse
import gc
import json
import logging
import sys
import tracemalloc
import psutil
from pathlib import Path

# Ensure package is in path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from aw_watcher_pipeline_stage.client import MockActivityWatchClient, PipelineClient
from aw_watcher_pipeline_stage.watcher import PipelineWatcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_memory")

class MockEvent:
    __slots__ = ('src_path', 'is_directory', 'event_type', 'dest_path')
    def __init__(self, src_path: str, is_directory: bool = False):
        self.src_path = src_path
        self.is_directory = is_directory
        self.event_type = "modified"
        self.dest_path = None

def run_memory_profile(iterations: int = 1000) -> str:
    """Run memory profiling using tracemalloc over N iterations."""
    logger.info(f"Starting memory profiling ({iterations} iterations)...")

    tmp_file = Path("memory_profile_test.json")
    tmp_file.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))

    try:
        # Setup components
        mock_client = MockActivityWatchClient()
        client = PipelineClient(tmp_file, client=mock_client, testing=True)
        # Disable debounce to allow manual triggering
        watcher = PipelineWatcher(
            tmp_file, client, pulsetime=120.0, debounce_seconds=1.0
        )

        process = psutil.Process()
        start_rss = process.memory_info().rss / 1024 / 1024  # MB

        # Start tracemalloc
        tracemalloc.start()

        # Warmup to load modules, caches, and internal buffers
        logger.info("Warming up...")
        for _ in range(10):
            event = MockEvent(str(tmp_file))
            watcher.handler._process_event(event) # type: ignore
            watcher.handler._on_debounce_fired()
            client.send_heartbeat("Warmup", "Task")
            mock_client.events.clear()
            
        # Clear any lingering events/state from warmup
        watcher.handler._event_queue.clear()
        # Reset stats to baseline
        watcher.handler.events_detected = 0
        watcher.handler.heartbeats_sent = 0

        gc.collect()
        snapshot1 = tracemalloc.take_snapshot()

        # Run iterations
        logger.info("Running iterations...")
        for i in range(iterations):
            # Update file content to force cache miss and full parsing
            # This ensures we profile json.loads and dict creation overhead
            tmp_file.write_text(json.dumps({
                "current_stage": "Loop", 
                "current_task": f"Task {i}",
                "metadata": {"iteration": i}
            }))

            # 1. Watcher Event Processing Loop
            # Simulate event -> queue -> debounce fire -> read -> process
            event = MockEvent(str(tmp_file))
            watcher.handler._process_event(event) # type: ignore
            
            # Manually trigger debounce callback to process the queue immediately
            watcher.handler._on_debounce_fired()

            # 2. Client Heartbeat Loop
            client.send_heartbeat("Stage", f"Task {i}")

            # Clear mock accumulation to focus on application leaks, not mock storage
            mock_client.events.clear()

        gc.collect()
        snapshot2 = tracemalloc.take_snapshot()

        end_rss = process.memory_info().rss / 1024 / 1024  # MB

        # Analyze differences
        top_stats = snapshot2.compare_to(snapshot1, "lineno")

        report_lines = []
        report_lines.append(f"**Iterations**: {iterations}")
        report_lines.append("**Top Memory Growth Allocations**:")
        report_lines.append("| Size Diff | Count Diff | Location |")
        report_lines.append("| :--- | :--- | :--- |")

        total_growth = 0
        for stat in top_stats[:10]:
            total_growth += stat.size_diff
            frame = stat.traceback[0]
            filename = Path(frame.filename).name
            report_lines.append(
                f"| {stat.size_diff / 1024:.2f} KiB | {stat.count_diff} | `{filename}:{frame.lineno}` |"
            )

        report_lines.append(f"\n**Total Growth (Top 10)**: {total_growth / 1024:.2f} KiB")

        rss_growth = end_rss - start_rss
        report_lines.append(f"**RSS Start**: {start_rss:.2f} MB | **RSS End**: {end_rss:.2f} MB | **Growth**: {rss_growth:.2f} MB")

        # Threshold: 500KB growth over 1000 iterations is a warning sign
        # RSS threshold: < 50MB total, growth < 1MB
        if total_growth > 500 * 1024 or rss_growth > 1.0:
            report_lines.append("\n⚠️ **Status**: Potential leak detected (>500KB alloc growth or >1MB RSS growth).")
        else:
            report_lines.append("\n✅ **Status**: Memory usage stable.")

        return "\n".join(report_lines)

    finally:
        watcher.stop()
        tracemalloc.stop()
        if tmp_file.exists():
            tmp_file.unlink()


def update_performance_md(report_content: str, md_path: Path) -> None:
    """Update PERFORMANCE.md with the memory report."""
    if not md_path.exists():
        logger.error(f"{md_path} not found.")
        return

    content = md_path.read_text(encoding="utf-8")
    start_marker = "<!-- MEMORY_REPORT_START -->"
    end_marker = "<!-- MEMORY_REPORT_END -->"

    if start_marker not in content or end_marker not in content:
        logger.error("Memory report markers not found in PERFORMANCE.md")
        return

    pre = content.split(start_marker)[0]
    post = content.split(end_marker)[1]

    new_content = f"{pre}{start_marker}\n\n{report_content}\n{end_marker}{post}"
    md_path.write_text(new_content, encoding="utf-8")
    logger.info(f"Updated {md_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--update-report", action="store_true", help="Update PERFORMANCE.md"
    )
    parser.add_argument(
        "--iterations", type=int, default=1000, help="Number of iterations"
    )
    args = parser.parse_args()

    report = run_memory_profile(args.iterations)
    print("\n" + report)

    if args.update_report:
        md_path = Path(__file__).resolve().parents[1] / "PERFORMANCE.md"
        update_performance_md(report, md_path)