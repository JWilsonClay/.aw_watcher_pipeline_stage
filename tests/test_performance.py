#!/usr/bin/env python3
"""
Performance validation and profiling script.

Stage 6.1.4: Profile debounce and heartbeat paths with visualization support.

Modes:
1. Performance Metrics (pytest): Measures CPU/RAM during idle and active states.
   Run: poetry run pytest tests/test_performance.py

2. Profiling (Manual):
   - cProfile: poetry run python tests/test_performance.py --profile
   - Timeit:   poetry run python tests/test_performance.py --benchmark
   - LineProfiler: poetry run python tests/test_performance.py --line-profile
   - Resource Monitor: poetry run python tests/test_performance.py --scenario active

Prerequisites for Visualizations:
   poetry add --group dev matplotlib psutil line_profiler
"""
import cProfile
import io
import pstats
import platform
import json
import logging
import os
import random
import threading
import time
import timeit
from pathlib import Path
import sys
from typing import Dict, Optional, Tuple, List, Any
from unittest.mock import MagicMock, patch

import pytest

try:
    import psutil
except ImportError:
    psutil = None

try:
    from line_profiler import LineProfiler
except ImportError:
    LineProfiler = None

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
except ImportError:
    plt = None

from aw_watcher_pipeline_stage.client import MockActivityWatchClient, PipelineClient
from aw_watcher_pipeline_stage.watcher import PipelineWatcher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_performance")

def measure_resources(duration: int, interval: float = 1.0) -> Dict[str, float]:
    """Measure CPU and Memory usage over a duration."""
    if psutil is None:
        return {"cpu_avg": 0.0, "cpu_peak": 0.0, "rss_avg": 0.0, "rss_peak": 0.0}
        
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
    """Simulate file modifications with varying content."""
    counter = 0
    while not stop_event.is_set():
        try:
            # Vary payload size to stress memory/parsing (100B to 2KB)
            payload_size = random.randint(100, 2048)
            
            # Vary metadata keys to stress internal state dictionary
            meta_keys = {f"key_{random.randint(0, 100)}": random.random() for _ in range(5)}
            
            data = {
                "current_stage": "Performance Test",
                "current_task": f"Task {counter}",
                "status": "in_progress",
                "metadata": {
                    "ts": time.time(),
                    "iteration": counter,
                    "payload": "x" * payload_size,
                    "nested": {"level": counter % 10, "val": random.random()},
                    **meta_keys
                }
            }
            file_path.write_text(json.dumps(data))
            counter += 1
            # Sleep in small chunks to allow responsive stop
            start_sleep = time.time()
            while time.time() - start_sleep < interval and not stop_event.is_set():
                time.sleep(0.1)
        except Exception as e:
            logger.debug(f"Activity simulation error: {e}")

def simulate_burst(file_path: Path, count: int = 1000) -> None:
    """Simulate a burst of rapid file updates using low-level OS calls."""
    for i in range(count):
        data = {
            "current_stage": "Burst Test",
            "current_task": f"Task {i}",
            "status": "in_progress",
            "metadata": {"ts": time.time(), "padding": "x" * 200, "nested": {"level": 1}}
        }
        # Low-level write for speed (<1ms delay target)
        s = json.dumps(data).encode('utf-8')
        fd = os.open(str(file_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        try:
            os.write(fd, s)
        finally:
            os.close(fd)

def simulate_main_loop(watcher: PipelineWatcher, stop_event: threading.Event) -> None:
    """Simulate the main loop's periodic checks to capture their CPU cost."""
    while not stop_event.is_set():
        try:
            watcher.check_periodic_heartbeat()
        except Exception:
            pass
        time.sleep(1.0)

@pytest.mark.skipif(os.environ.get("CI") == "true", reason="Skipping performance test in CI")
@pytest.mark.skipif(psutil is None, reason="psutil not installed")
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

@pytest.mark.skipif(psutil is None, reason="psutil not installed")
@pytest.mark.skip(reason="Long running stress test (1h). Run manually or via CLI.")
def test_stress_1h(tmp_path: Path) -> None:
    """
    Run a 1-hour stress test to verify stability.
    
    Targets:
    - RSS < 50MB
    - RSS Growth < 5%
    - CPU Avg < 1%
    """
    metrics, _ = run_stress_test(tmp_path, duration=3600, sample_interval=300)
    
    assert metrics['peak_rss_mb'] < 50.0
    assert metrics['rss_growth_pct'] < 5.0
    assert metrics['avg_cpu_pct'] < 1.0

def ascii_bar_chart(data: Dict[str, float], title: str, width: int = 40) -> str:
    """Generate a simple ASCII bar chart."""
    if not data:
        return ""
    output = io.StringIO()
    output.write(f"\n#### {title}\n\n")
    output.write("```\n")
    max_val = max(data.values()) if data else 1.0
    if max_val == 0:
        max_val = 1.0
    max_len = max(len(k) for k in data.keys()) if data else 10
    max_label_len = min(max_len, 50)
    
    for label, value in sorted(data.items(), key=lambda x: x[1], reverse=True):
        display_label = label[:max_label_len]
        bar_len = int((value / max_val) * width)
        bar = "█" * bar_len
        output.write(f"{display_label.ljust(max_label_len)} | {bar} {value:.4f}s\n")
    output.write("```\n\n")
    return output.getvalue()

def print_hot_path_tree(ps: pstats.Stats, limit: int = 5) -> str:
    """Generate a tree view of the hottest path (highest cumulative time)."""
    output = io.StringIO()
    output.write("**Hot Path Tree (Flame Graph approximation):**\n")
    output.write("```text\n")
    
    if not hasattr(ps, 'all_callees'):
        ps.calc_callees()
        
    if not ps.stats:
        output.write("(No stats available)\n```\n\n")
        return output.getvalue()

    # Find root (function with max cumtime)
    root = max(ps.stats.keys(), key=lambda f: ps.stats[f][3])
    total_time = ps.total_tt
    
    def print_node(func, depth=0, prefix="", is_last=True, parent_ct=0.0):
        if depth > limit:
            output.write(f"{prefix}...\n")
            return
        
        cc, nc, tt, ct, callers = ps.stats[func]
        file, line, name = func
        filename = Path(file).name
        
        pct_total = (ct / total_time * 100) if total_time > 0 else 0.0
        pct_parent = (ct / parent_ct * 100) if parent_ct > 0 else 100.0
        
        marker = "└─ " if is_last else "├─ "
        if depth == 0: marker = ""
        
        # Show % of parent time to identify where the parent spends its time
        pct_str = f"{pct_total:.1f}%"
        if depth > 0:
            pct_str += f" / {pct_parent:.1f}% of parent"
            
        # ASCII Bar for visual weight (Flame graph style)
        bar_len = int(pct_total / 5)  # Max 20 chars for 100%
        bar = "█" * bar_len + " " if bar_len > 0 else ""
        
        output.write(f"{prefix}{marker}{bar}{filename}:{line}:{name} ({ct:.4f}s - {pct_str})\n")
        
        if func in ps.all_callees:
            children = ps.all_callees[func]
            sorted_children = sorted(children.keys(), key=lambda f: ps.stats[f][3], reverse=True)
            # Show top children that contribute significantly
            top_children = [c for c in sorted_children[:3] if ps.stats[c][3] > (ct * 0.05)]
            
            for i, child in enumerate(top_children):
                child_is_last = (i == len(top_children) - 1)
                new_prefix = prefix + ("   " if is_last else "│  ") if depth > 0 else ""
                print_node(child, depth + 1, new_prefix, child_is_last, parent_ct=ct)

    print_node(root, is_last=True, parent_ct=ps.stats[root][3])
    output.write("```\n\n")
    return output.getvalue()

def analyze_profile_stats(pr: cProfile.Profile, title: str, limit: int = 15) -> str:
    """Analyze profile stats and return a markdown report section with bottlenecks."""
    output = io.StringIO()
    ps = pstats.Stats(pr, stream=output).sort_stats('cumulative')
    
    output.write(f"\n### {title}\n\n")
    output.write(f"**Total Time**: {ps.total_tt:.4f}s\n\n")

    if ps.total_tt == 0:
        output.write("(Total time is 0s, skipping analysis)\n")
        return output.getvalue()
    
    # Quick Summary: Top Hotspot by Internal Time
    ps.sort_stats('tottime')
    if ps.fcn_list:
        top_func = ps.fcn_list[0]
        _, _, top_tt, _, _ = ps.stats[top_func]
        top_name = f"{Path(top_func[0]).name}:{top_func[1]}:{top_func[2]}"
        output.write(f"**Top Hotspot (Internal)**: `{top_name}` ({top_tt:.4f}s)\n\n")
    ps.sort_stats('cumulative')

    # Bottleneck Heuristics
    bottlenecks = []
    recommendations = set()
    for func, (cc, nc, tt, ct, callers) in ps.stats.items():
        file, line, name = func
        filename = Path(file).name
        file_path_str = str(file)
        ratio = ct / ps.total_tt if ps.total_tt > 0 else 0
        
        if ratio > 0.05: # > 5% of time
            severity = "High" if ratio > 0.3 else ("Medium" if ratio > 0.1 else "Low")
            desc = ""
            
            if "json" in filename or ("json" in file_path_str and name in ("loads", "load", "dumps", "dump", "decode", "raw_decode")):
                desc = f"**JSON Parsing** (`{name}`)"
                recommendations.add("- Optimization: Reduce JSON size or debounce frequency to lower parsing cost.")
            elif "aw_client" in file_path_str or "aw-client" in file_path_str or "requests" in file_path_str:
                desc = f"**ActivityWatch Client** (`{name}`)"
                recommendations.add("- Optimization: Check network latency, payload size, or queuing logic.")
            elif "watchdog" in file_path_str:
                desc = f"**Watchdog** (`{name}`)"
                recommendations.add("- Optimization: Check filesystem event frequency or observer efficiency.")
            elif "threading" in filename:
                if name in ("start", "_start_new_thread", "__init__"):
                    desc = f"**Thread Creation** (`{name}`)"
                    recommendations.add("- Optimization: High thread churn. Check debounce timer creation overhead.")
                elif name in ("wait", "acquire", "release", "Condition"):
                    desc = f"**Thread Sync** (`{name}`)"
                    recommendations.add("- Optimization: Check for lock contention or excessive waiting.")
                else:
                    desc = f"**Threading** (`{name}`)"
                    recommendations.add("- Optimization: Check threading overhead.")
            elif "builtins" in filename and "acquire" in name:
                desc = f"**Lock Contention** (`{name}`)"
                recommendations.add("- Optimization: Check for lock contention in threaded components.")
            elif ("builtins" in filename or "io" in filename) and "open" in name:
                desc = f"**File Open** (`{name}`)"
                recommendations.add("- Optimization: Ensure file opens are necessary. Keep handles open if possible?")
            elif "method 'read'" in name or (("builtins" in filename or "io" in filename) and "read" in name):
                desc = f"**File Read** (`{name}`)"
                recommendations.add("- Optimization: Large reads? Check buffer sizes or read frequency.")
            elif ("posix" in filename or "nt" in filename) and name in ("stat", "lstat"):
                desc = f"**File Metadata Check** (`{name}`)"
                recommendations.add("- Optimization: Reduce frequency of stat calls (check debounce or polling interval).")
            elif ("posix" in filename or "nt" in filename) and name in ("read", "write", "scandir", "listdir"):
                desc = f"**System Call** (`{name}`)"
                recommendations.add("- Optimization: Reduce frequency of system calls (batching, caching).")
            elif "socket" in filename:
                desc = f"**Network/Socket** (`{name}`)"
                recommendations.add("- Optimization: Verify offline queuing logic if network is slow.")
            elif "selectors" in filename or "select" in name:
                desc = f"**IO Wait** (`{name}`)"
                recommendations.add("- Optimization: IO Wait is expected, but ensure it doesn't block main loop unexpectedly.")
            elif "time" in filename and "sleep" in name:
                desc = f"**Sleep/Wait** (`{name}`)"
                recommendations.add("- Optimization: Verify if sleep is for debounce (good) or blocking retry (bad in hot path).")
            elif "pathlib" in filename:
                desc = f"**Pathlib/Path Resolution** (`{name}`)"
                recommendations.add("- Optimization: Cache Path objects or use os.path in hot paths.")
            elif "logging" in filename or "logging" in file_path_str:
                desc = f"**Logging Overhead** (`{name}`)"
                recommendations.add("- Optimization: Reduce log level or use lazy formatting in hot paths.")
            elif "watcher.py" in filename:
                desc = f"**Watcher Core** (`{name}`)"
                recommendations.add("- Optimization: Review core logic efficiency in watcher (state comparison, event processing).")
            elif "client.py" in filename:
                desc = f"**Client Core** (`{name}`)"
                recommendations.add("- Optimization: Review client interaction or data transformation (payload construction).")

            if desc:
                # Store tuple for sorting: (severity_rank, ratio, string)
                rank = 0 if severity == "High" else (1 if severity == "Medium" else 2)
                per_call = ct / nc if nc > 0 else 0
                bottlenecks.append((rank, ratio, f"- **[{severity}]** {desc}: {ct:.4f}s ({ratio*100:.1f}%) | Calls: {nc} | Avg: {per_call:.6f}s"))

    if bottlenecks:
        output.write("#### Prioritized Bottlenecks:\n")
        # Sort by rank (High < Medium) then by ratio descending
        for _, _, b_str in sorted(bottlenecks, key=lambda x: (x[0], -x[1])):
            output.write(f"{b_str}\n")
        output.write("\n")
    else:
        output.write("#### Prioritized Bottlenecks:\n")
        output.write("- No specific bottlenecks detected by heuristics (all < 5% of total time).\n\n")
        
    if recommendations:
        output.write("**Recommendations:**\n")
        for r in sorted(recommendations):
            output.write(f"{r}\n")
        output.write("\n")

    output.write(print_hot_path_tree(ps))

    output.write("#### Top 5 Hotspots & Callers:\n")
    output.write("```text\n")
    ps.print_callers(5)
    output.write("```\n\n")

    output.write("**Top Functions by Cumulative Time:**\n")
    output.write("| Calls | Cum Time (s) | % Cum | Total Time (s) | Per Call (s) | Function |\n")
    output.write("| --- | --- | --- | --- | --- | --- |\n")
    
    for func in ps.fcn_list[:limit]:
        cc, nc, tt, ct, callers = ps.stats[func]
        file, line, name = func
        filename = Path(file).name
        per_call = ct / nc if nc > 0 else 0
        pct = (ct / ps.total_tt * 100) if ps.total_tt > 0 else 0
        output.write(f"| {nc} | {ct:.4f} | {pct:.1f}% | {tt:.4f} | {per_call:.6f} | `{filename}:{line}:{name}` |\n")
    output.write("\n")

    # Add sorting by tottime (Internal Time)
    ps.sort_stats('tottime')
    output.write("**Top Functions by Internal Time (tottime):**\n")
    output.write("| Calls | Total Time (s) | % Int | Cum Time (s) | Per Call (s) | Function |\n")
    output.write("| --- | --- | --- | --- | --- | --- |\n")
    
    for func in ps.fcn_list[:limit]:
        cc, nc, tt, ct, callers = ps.stats[func]
        file, line, name = func
        filename = Path(file).name
        per_call = tt / nc if nc > 0 else 0
        pct = (tt / ps.total_tt * 100) if ps.total_tt > 0 else 0
        output.write(f"| {nc} | {tt:.4f} | {pct:.1f}% | {ct:.4f} | {per_call:.6f} | `{filename}:{line}:{name}` |\n")
    output.write("\n")

    # Add sorting by ncalls (Call Count)
    ps.sort_stats('ncalls')
    output.write("**Top Functions by Call Count:**\n")
    output.write("| Calls | Total Time (s) | Cum Time (s) | Per Call (s) | Function |\n")
    output.write("| --- | --- | --- | --- | --- |\n")
    
    for func in ps.fcn_list[:limit]:
        cc, nc, tt, ct, callers = ps.stats[func]
        file, line, name = func
        filename = Path(file).name
        per_call = ct / nc if nc > 0 else 0
        output.write(f"| {nc} | {tt:.4f} | {ct:.4f} | {per_call:.6f} | `{filename}:{line}:{name}` |\n")
    output.write("\n")
    return output.getvalue()

def generate_component_chart(data: Dict[str, float], output_path: Path) -> None:
    """Generate a bar chart of component CPU times using matplotlib."""
    if plt is None:
        return
    
    components = list(data.keys())
    times = list(data.values())
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(components, times, color='skyblue')
    plt.xlabel('Component')
    plt.ylabel('CPU Time (s)')
    plt.title('CPU Time Distribution by Component')
    plt.xticks(rotation=45, ha='right')
    
    # Add value labels
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                 f'{height:.4f}s',
                 ha='center', va='bottom')
                 
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

def generate_stats_chart(ps: pstats.Stats, output_path: Path, title: str, sort_by: str = 'tottime', limit: int = 10) -> None:
    """Generate a horizontal bar chart of top functions."""
    if plt is None:
        return
        
    # Ensure stats are sorted
    ps.sort_stats(sort_by)
    
    # Extract data
    func_list = ps.fcn_list[:limit] if hasattr(ps, 'fcn_list') else []
    
    if not func_list:
        return

    labels = []
    values = []
    
    for func in func_list:
        cc, nc, tt, ct, callers = ps.stats[func]
        file, line, name = func
        filename = Path(file).name
        labels.append(f"{filename}:{name}")
        val = tt if sort_by == 'tottime' else ct
        values.append(val)
        
    plt.figure(figsize=(10, 6))
    y_pos = range(len(labels))
    color = 'salmon' if sort_by == 'tottime' else 'lightgreen'
    plt.barh(y_pos, values, align='center', color=color)
    plt.yticks(y_pos, labels)
    plt.gca().invert_yaxis()  # labels read top-to-bottom
    metric_name = 'Internal Time' if sort_by == 'tottime' else 'Cumulative Time'
    plt.xlabel(f'{metric_name} (s)')
    plt.title(f'Top Hotspots ({sort_by}) - {title}')
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

def run_profiling(tmp_path: Path, profile_export_dir: Optional[Path] = None) -> Tuple[Dict[str, float], str]:
    """Run cProfile on key watcher methods during a burst."""
    print("Setting up profiling environment...")
    f = tmp_path / "profile_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))
    
    report = io.StringIO()
    mock_client = MockActivityWatchClient()
    client = PipelineClient(f, client=mock_client, testing=True)
    # Use standard debounce to see how it handles the burst (should coalesce)
    watcher = PipelineWatcher(f, client, pulsetime=120.0, debounce_seconds=1.0)
    
    # Disable rate limiting to stress debounce timer creation logic
    watcher.handler._rate_limit_max = 10000.0
    watcher.handler._rate_limit_tokens = 10000.0
    
    # Wrap methods for profiling
    pr_process = cProfile.Profile()
    orig_process = watcher.handler._process_event
    def wrapped_process(*args, **kwargs):
        pr_process.enable()
        try:
            return orig_process(*args, **kwargs)
        finally:
            pr_process.disable()
    watcher.handler._process_event = wrapped_process
    
    pr_read = cProfile.Profile()
    orig_read = watcher.handler._read_file_data
    def wrapped_read(*args, **kwargs):
        pr_read.enable()
        try:
            return orig_read(*args, **kwargs)
        finally:
            pr_read.disable()
    watcher.handler._read_file_data = wrapped_read
    
    pr_logic = cProfile.Profile()
    orig_logic = watcher.handler._process_state_change
    def wrapped_logic(*args, **kwargs):
        pr_logic.enable()
        try:
            return orig_logic(*args, **kwargs)
        finally:
            pr_logic.disable()
    watcher.handler._process_state_change = wrapped_logic

    pr_send = cProfile.Profile()
    orig_send = client.send_heartbeat
    def wrapped_send(*args, **kwargs):
        pr_send.enable()
        try:
            return orig_send(*args, **kwargs)
        finally:
            pr_send.disable()
    client.send_heartbeat = wrapped_send
    
    print("Starting watcher...")
    watcher.start()
    
    try:
        print("Simulating burst of 1000 updates...")
        start = time.perf_counter()
        simulate_burst(f, count=1000)
        duration = time.perf_counter() - start
        print(f"Burst completed in {duration:.4f}s")
        
        print("Waiting for processing (3s)...")
        time.sleep(3.0)
    finally:
        watcher_stats = watcher.get_statistics()
        watcher.stop()
        
    print("\n" + "="*50)
    print("PROFILING RESULTS")
    print("="*50)
    print(f"Total Burst Processing Time (1000 events): {duration:.4f}s")
    print(f"Avg Processing Time per Event: {(duration / 1000) * 1000:.4f}ms")

    report.write("## Burst Profiling Results\n\n")
    report.write(f"- **Total Burst Time**: {duration:.4f}s (1000 events)\n")
    report.write(f"- **Avg Time/Event**: {(duration / 1000) * 1000:.4f}ms\n\n")
    
    metrics = {
        "burst_duration_s": duration,
        "avg_event_ms": (duration / 1000) * 1000
    }

    metrics["max_processing_latency_s"] = watcher_stats.get("max_processing_latency", 0.0)
    report.write(f"- **Max Processing Latency (Profiled)**: {metrics['max_processing_latency_s']*1000:.2f}ms\n\n")
    
    print("\n" + "-"*50)
    print("CPU PER COMPONENT (cProfile)")
    print("-"*50)
    
    component_times = {}
    
    # Track call counts for debounce efficiency analysis
    call_counts = {}

    for component, name, pr in [
        ("Watcher (Debounce)", "PipelineEventHandler._process_event", pr_process), 
        ("Watcher (I/O)", "PipelineEventHandler._read_file_data", pr_read),
        ("Watcher (Logic)", "PipelineEventHandler._process_state_change", pr_logic),
        ("Client (Heartbeat)", "PipelineClient.send_heartbeat", pr_send)
    ]:
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        # Calculate total CPU time for this component safely
        total_tt = sum(stat[2] for stat in ps.stats.values())
        metrics[f"cpu_{component}"] = total_tt
        component_times[component] = total_tt
        
        # Extract call count for the wrapped function
        # We need to find the entry point in stats. Since we wrapped it, it's the root.
        # But ps.stats keys are (file, line, name). We search by name.
        func_name = name.split(".")[-1]
        for func, (cc, nc, tt, ct, callers) in ps.stats.items():
            if func[2] == func_name:
                call_counts[component] = nc
                break
        
        if profile_export_dir:
            profile_export_dir.mkdir(parents=True, exist_ok=True)
            safe_name = component.replace(" ", "_").replace("(", "").replace(")", "").lower()
            prof_file = profile_export_dir / f"{safe_name}.prof"
            pr.dump_stats(str(prof_file))
            report.write(f"- **Profile Saved**: `{prof_file}`\n")
            
            if plt:
                chart_file_tt = profile_export_dir / f"{safe_name}_hotspots_tottime.png"
                generate_stats_chart(ps, chart_file_tt, component, 'tottime')
                report.write(f"- **Hotspots (Internal)**:\n\n![{component} Internal](profiles/{chart_file_tt.name})\n")
                
                chart_file_cum = profile_export_dir / f"{safe_name}_hotspots_cumtime.png"
                generate_stats_chart(ps, chart_file_cum, component, 'cumulative')
                report.write(f"- **Hotspots (Cumulative)**:\n\n![{component} Cumulative](profiles/{chart_file_cum.name})\n")
            else:
                print(f"Warning: matplotlib not installed. Skipping chart generation for {component}.")
                report.write(f"- **Charts Skipped**: `matplotlib` not installed.\n")

        stats_str = analyze_profile_stats(pr, f"{component} ({total_tt:.4f}s CPU)", limit=10)
        print(stats_str)
        report.write(stats_str)

    # Debounce Efficiency Analysis
    events_detected = call_counts.get("Watcher (Debounce)", 0)
    reads_performed = call_counts.get("Watcher (I/O)", 0)
    
    if events_detected > 0:
        debounce_ratio = reads_performed / events_detected
        report.write("### Debounce Efficiency Analysis\n\n")
        report.write(f"- **Events Detected**: {events_detected}\n")
        report.write(f"- **File Reads**: {reads_performed}\n")
        report.write(f"- **Read/Event Ratio**: {debounce_ratio:.4f}\n\n")
        
        if debounce_ratio > 0.5 and events_detected > 100:
            report.write("⚠️ **CRITICAL**: Debounce logic is inefficient. Reads are > 50% of events during burst.\n")
            report.write("  - *Recommendation*: Check `debounce_seconds` or timer reset logic.\n\n")
        elif debounce_ratio > 0.1 and events_detected > 100:
            report.write("⚠️ **WARNING**: Debounce logic could be improved. Reads are > 10% of events.\n\n")
        else:
            report.write("✅ **PASS**: Debounce logic is working efficiently (Reads << Events).\n\n")

    chart_str = ascii_bar_chart(component_times, "CPU Time Distribution by Component")
    print(chart_str)
    report.write(chart_str)
    
    if profile_export_dir:
        if plt:
            try:
                chart_path = profile_export_dir / "cpu_distribution.png"
                generate_component_chart(component_times, chart_path)
                report.write(f"\n### CPU Distribution Chart\n\n!CPU Distribution\n")
            except Exception as e:
                print(f"Failed to generate matplotlib chart: {e}")
        else:
            report.write(f"- **Distribution Chart Skipped**: `matplotlib` not installed.\n")
    
    return metrics, report.getvalue()

def run_latency_profiling(tmp_path: Path) -> Tuple[Dict[str, float], str]:
    """Profile send_heartbeat latency in different modes (Online/Offline)."""
    metrics = {}
    report = io.StringIO()
    print("\n" + "="*50)
    print("LATENCY PROFILING (Online vs Offline vs Truncation)")
    print("="*50)
    report.write("## Latency Profiling\n\n")
    f = tmp_path / "latency_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))
    
    # Complex metadata to stress payload construction/flattening
    metadata = {
        "env": "profiling",
        "details": "x" * 500,  # Increased to stress truncation
        "nested": {"level1": {"level2": "value" * 10}},
        "list": ["item"] * 20
    }
    
    mock_aw = MockActivityWatchClient()
    client = PipelineClient(f, client=mock_aw, testing=True)
    
    # 1. Online (Fast Mock)
    print("1. Online (Fast Mock)...")
    latencies = []
    start = time.perf_counter()
    for i in range(1000):
        t0 = time.perf_counter()
        client.send_heartbeat("Stage", f"Task {i}")
        latencies.append(time.perf_counter() - t0)
    duration = time.perf_counter() - start
    avg_ms = (duration / 1000) * 1000
    max_ms = max(latencies) * 1000 if latencies else 0.0
    print(f"   Total: {duration:.4f}s | Avg: {avg_ms:.3f}ms | Max: {max_ms:.3f}ms")
    metrics["online_ms"] = avg_ms
    metrics["online_max_ms"] = max_ms
    
    
    # 2. Offline (ConnectionError -> Queued)
    print("2. Offline (ConnectionError -> Queued)...")
    # Mock heartbeat to raise ConnectionError
    original_heartbeat = mock_aw.heartbeat
    mock_aw.heartbeat = MagicMock(side_effect=ConnectionError("Offline"))
    
    # Patch sleep to measure queuing overhead without backoff delays
    with patch("time.sleep"):
        start = time.perf_counter()
        for i in range(1000):
            client.send_heartbeat("Stage", f"Task {i}", metadata=metadata)
        duration = time.perf_counter() - start
    
    avg_ms = (duration / 1000) * 1000
    print(f"   Total: {duration:.4f}s | Avg Heartbeat Latency: {avg_ms:.3f}ms")
    metrics["offline_ms"] = avg_ms
    
    # Calculate queuing impact
    queuing_impact = metrics["offline_ms"] - metrics["online_ms"]
    metrics["queuing_impact_ms"] = queuing_impact
    print(f"   Queuing Impact: {queuing_impact:.3f}ms")
    
    
    # Restore
    mock_aw.heartbeat = original_heartbeat

    # 3. Payload Truncation (Large Metadata)
    print("3. Payload Truncation (Large Metadata)...")
    # Suppress warnings to measure pure processing time without I/O spam
    logging.getLogger("aw_watcher_pipeline_stage.client").setLevel(logging.ERROR)
    
    # Create metadata that exceeds 1KB limit to force truncation logic
    # 50 keys * 100 chars = 5000 chars > 1024 limit
    large_metadata = {f"key_{i}": "x" * 100 for i in range(50)}
    
    start = time.perf_counter()
    for i in range(1000):
        client.send_heartbeat("Stage", f"Task {i}", metadata=large_metadata)
    duration = time.perf_counter() - start
    
    logging.getLogger("aw_watcher_pipeline_stage.client").setLevel(logging.INFO)
    
    avg_ms = (duration / 1000) * 1000
    print(f"   Total: {duration:.4f}s | Avg Heartbeat Latency: {avg_ms:.3f}ms")
    metrics["truncation_ms"] = avg_ms
    
    print("\n### Latency Summary Table\n")
    print("| Scenario | Total Time (s) | Avg Latency (ms) |")
    print("| --- | --- | --- |")
    print(f"| Online | {metrics['online_ms']/1000*1000:.4f} | {metrics['online_ms']:.3f} |")
    print(f"| Offline (Queued) | {metrics['offline_ms']/1000*1000:.4f} | {metrics['offline_ms']:.3f} |")
    print(f"| Large Payload | {metrics['truncation_ms']/1000*1000:.4f} | {metrics['truncation_ms']:.3f} |")
    print("\n")
    
    report.write("| Scenario | Avg Latency (ms) | Max Latency (ms) |\n| --- | --- | --- |\n")
    report.write(f"| Online | {metrics['online_ms']:.3f} | {metrics['online_max_ms']:.3f} |\n")
    report.write(f"| Offline (Queued) | {metrics['offline_ms']:.3f} | N/A |\n")
    report.write(f"| Large Payload | {metrics['truncation_ms']:.3f} | N/A |\n\n")
    
    return metrics, report.getvalue()

def run_benchmarking(tmp_path: Path) -> str:
    """Run timeit benchmarks on hot paths."""
    report = io.StringIO()
    report.write("## Micro-Benchmarks (timeit)\n\n")
    
    print("Setting up benchmarking environment...")
    f = tmp_path / "benchmark_task.json"
    # Create a representative file (approx 500 bytes)
    data = {
        "current_stage": "Benchmark",
        "current_task": "Parsing",
        "status": "in_progress",
        "metadata": {"key": "x" * 200}
    }
    f.write_text(json.dumps(data))
    
    mock_client = MockActivityWatchClient()
    client = PipelineClient(f, client=mock_client, testing=True)
    watcher = PipelineWatcher(f, client, pulsetime=120.0)
    
    print(f"Benchmarking _read_file_data with {f.stat().st_size} bytes file...")
    
    handler = watcher.handler
    file_path = str(f)
    
    # Benchmark _read_file_data
    number = 1000
    repeat = 5
    
    results = timeit.repeat(lambda: handler._read_file_data(file_path), number=number, repeat=repeat)
    
    best_time = min(results)
    avg_time = sum(results) / len(results)
    per_call_best = (best_time / number) * 1000 # ms
    per_call_avg = (avg_time / number) * 1000 # ms
    
    print(f"\n_read_file_data results ({number} loops, best of {repeat}):")
    print(f"  Best: {per_call_best:.4f} ms per call")
    print(f"  Avg:  {per_call_avg:.4f} ms per call")
    report.write(f"- **_read_file_data**: {per_call_best:.4f} ms/call (Avg: {per_call_avg:.4f} ms)\n")
    
    # Benchmark _process_state_change (pure logic, no I/O)
    data_dict = handler._read_file_data(file_path)
    if data_dict:
        print(f"\nBenchmarking _process_state_change...")
        results_process = timeit.repeat(lambda: handler._process_state_change(data_dict, file_path), number=number, repeat=repeat)
        
        best_time_proc = min(results_process)
        per_call_best_proc = (best_time_proc / number) * 1000 # ms
        
        print(f"  Best: {per_call_best_proc:.4f} ms per call")
        report.write(f"- **_process_state_change**: {per_call_best_proc:.4f} ms/call\n")
    
    report.write("\n")
    return report.getvalue()

def run_resource_benchmarks(tmp_path: Path) -> Tuple[Dict[str, float], str]:
    """Run resource usage benchmarks (Idle vs Active)."""
    print("\n" + "="*50)
    print("RESOURCE USAGE BENCHMARKS")
    print("="*50)
    
    report = io.StringIO()
    report.write("## Resource Usage Benchmarks\n\n")
    
    if psutil is None:
        print("Skipping resource benchmarks (psutil not installed)")
        report.write("(Skipped: psutil not installed)\n\n")
        return {}, report.getvalue()

    report.write("| Scenario | CPU Avg (%) | CPU Peak (%) | RSS Peak (MB) |\n")
    report.write("| --- | --- | --- | --- |\n")
    
    metrics = {}
    f = tmp_path / "resource_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))
    
    mock_client = MockActivityWatchClient()
    client = PipelineClient(f, client=mock_client, testing=True)
    watcher = PipelineWatcher(f, client, pulsetime=120.0, debounce_seconds=1.0)
    watcher.start()
    
    main_loop_stop = threading.Event()
    main_loop_thread = threading.Thread(target=simulate_main_loop, args=(watcher, main_loop_stop))
    main_loop_thread.start()
    
    try:
        # 1. Idle
        print("1. Idle Mode (10s)...")
        idle_stats = measure_resources(duration=10)
        print(f"   CPU: {idle_stats['cpu_avg']:.2f}% | RSS: {idle_stats['rss_peak']:.2f}MB")
        report.write(f"| Idle (10s) | {idle_stats['cpu_avg']:.2f} | {idle_stats['cpu_peak']:.2f} | {idle_stats['rss_peak']:.2f} |\n")
        metrics.update({f"idle_{k}": v for k, v in idle_stats.items()})
        
        # 2. Active
        print("2. Active Mode (10s, 0.5s interval)...")
        stop_activity = threading.Event()
        activity_thread = threading.Thread(target=simulate_activity, args=(f, stop_activity, 0.5))
        activity_thread.start()
        
        active_stats = measure_resources(duration=10)
        print(f"   CPU: {active_stats['cpu_avg']:.2f}% | RSS: {active_stats['rss_peak']:.2f}MB")
        
        stop_activity.set()
        activity_thread.join()
        
        report.write(f"| Active (Burst) | {active_stats['cpu_avg']:.2f} | {active_stats['cpu_peak']:.2f} | {active_stats['rss_peak']:.2f} |\n\n")
        metrics.update({f"active_{k}": v for k, v in active_stats.items()})
        
    finally:
        main_loop_stop.set()
        main_loop_thread.join()
        watcher.stop()
        
    return metrics, report.getvalue()

def run_stress_test(tmp_path: Path, duration: int = 60, sample_interval: int = 10, profile_export_dir: Optional[Path] = None) -> Tuple[Dict[str, Any], str]:
    """Run a long-duration stress test to monitor resource stability."""
    print("\n" + "="*50)
    print(f"STRESS TEST ({duration}s)")
    print("="*50)
    
    report = io.StringIO()
    report.write(f"### Stress Test Results ({duration}s)\n\n")
    report.write(f"**Methodology**: Runs the watcher for {duration}s with a file write every 1.0s (varying JSON content). Monitors RSS and CPU usage every {sample_interval}s to detect memory leaks or CPU creep.\n\n")
    
    if psutil is None:
        print("Skipping stress test (psutil not installed)")
        report.write("(Skipped: psutil not installed)\n\n")
        return {}, report.getvalue()

    metrics: Dict[str, Any] = {}
    f = tmp_path / "stress_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))
    
    mock_client = MockActivityWatchClient()
    client = PipelineClient(f, client=mock_client, testing=True)
    watcher = PipelineWatcher(f, client, pulsetime=120.0, debounce_seconds=1.0)
    watcher.start()
    
    main_loop_stop = threading.Event()
    main_loop_thread = threading.Thread(target=simulate_main_loop, args=(watcher, main_loop_stop))
    main_loop_thread.start()
    
    # Start activity thread (1s interval)
    stop_activity = threading.Event()
    activity_thread = threading.Thread(target=simulate_activity, args=(f, stop_activity, 1.0))
    activity_thread.start()
    
    process = psutil.Process(os.getpid())
    cpu_readings = []
    rss_readings = []
    vms_readings = []
    timestamps = []
    
    # Prime cpu_percent
    process.cpu_percent()
    
    try:
        # Warmup to stabilize initial memory usage (allow libraries to load/cache)
        print("Warming up for 5s to stabilize baseline...")
        time.sleep(5)
        
        # Capture baseline RSS after warmup
        baseline_rss = process.memory_info().rss / (1024 * 1024) # MB
        
        print(f"Running for {duration}s, sampling every {sample_interval}s...")
        
        # Adjust duration to account for warmup, ensure at least one sample
        remaining_duration = max(sample_interval, duration)
        num_samples = remaining_duration // sample_interval
        
        for i in range(num_samples):
            time.sleep(sample_interval)
            cpu = process.cpu_percent()
            mem = process.memory_info().rss / (1024 * 1024) # MB
            vms = process.memory_info().vms / (1024 * 1024) # MB
            
            cpu_readings.append(cpu)
            rss_readings.append(mem)
            vms_readings.append(vms)
            timestamps.append((i + 1) * sample_interval)
            print(f"  [{(i+1) * sample_interval:>4}s] CPU: {cpu:5.2f}%, RSS: {mem:.2f}MB, VMS: {vms:.2f}MB")
            
    finally:
        stop_activity.set()
        activity_thread.join()
        main_loop_stop.set()
        main_loop_thread.join()
        watcher.stop()
        
    # Analysis
    if not rss_readings:
        report.write("(No data collected)\n")
        return {}, report.getvalue()
        
    metrics['start_rss_mb'] = baseline_rss
    metrics['end_rss_mb'] = rss_readings[-1]
    metrics['peak_rss_mb'] = max(rss_readings)
    metrics['peak_vms_mb'] = max(vms_readings) if vms_readings else 0.0
    metrics['avg_cpu_pct'] = sum(cpu_readings) / len(cpu_readings) if cpu_readings else 0.0
    metrics['rss_growth_mb'] = metrics['end_rss_mb'] - metrics['start_rss_mb']
    metrics['rss_growth_pct'] = (metrics['rss_growth_mb'] / metrics['start_rss_mb']) * 100 if metrics['start_rss_mb'] > 0 else 0.0

    # Report Generation
    report.write("| Metric | Value | Target | Status |\n")
    report.write("| :--- | :--- | :--- | :--- |\n")
    
    rss_status = "✅" if metrics['peak_rss_mb'] < 50.0 else "❌"
    report.write(f"| **Peak RSS** | {metrics['peak_rss_mb']:.2f} MB | < 50 MB | {rss_status} |\n")
    report.write(f"| **Peak VMS** | {metrics['peak_vms_mb']:.2f} MB | Monitor | N/A |\n")
    
    growth_status = "✅" if metrics['rss_growth_pct'] < 5.0 else "❌"
    report.write(f"| **RSS Growth** | {metrics['rss_growth_mb']:.2f} MB ({metrics['rss_growth_pct']:.2f}%) | < 5% | {growth_status} |\n")
    
    cpu_status = "✅" if metrics['avg_cpu_pct'] < 1.0 else "❌"
    report.write(f"| **Avg CPU** | {metrics['avg_cpu_pct']:.2f}% | < 1% | {cpu_status} |\n")
    report.write("\n")
    
    # Chart
    if plt and profile_export_dir and timestamps:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
        
        # Memory Plot
        ax1.plot(timestamps, rss_readings, marker='o', linestyle='-', color='b', label='RSS')
        ax1.plot(timestamps, vms_readings, marker='x', linestyle='--', color='g', label='VMS')
        ax1.set_title(f'Memory Usage Over Time ({duration}s Stress Test)')
        ax1.set_ylabel('Memory (MB)')
        ax1.legend()
        ax1.grid(True)
        
        # CPU Plot
        ax2.plot(timestamps, cpu_readings, marker='o', linestyle='-', color='r')
        ax2.set_title(f'CPU Usage Over Time ({duration}s Stress Test)')
        ax2.set_xlabel('Time (s)')
        ax2.set_ylabel('CPU (%)')
        ax2.grid(True)
        
        plt.tight_layout()
        chart_path = profile_export_dir / "stress_test_usage.png"
        plt.savefig(chart_path)
        plt.close()
        
        report.write(f"#### Resource Usage Chart\n\n![Stress Test Usage](profiles/{chart_path.name})\n\n")
    
    return metrics, report.getvalue()

def run_line_profiling(tmp_path: Path) -> None:
    """Run line_profiler on key watcher methods."""
    if LineProfiler is None:
        print("line_profiler not installed. Install with 'pip install line_profiler'.")
        return

    print("Setting up line_profiling environment...")
    f = tmp_path / "line_profile_task.json"
    f.write_text(json.dumps({"current_stage": "Init", "current_task": "Init"}))
    
    mock_client = MockActivityWatchClient()
    client = PipelineClient(f, client=mock_client, testing=True)
    watcher = PipelineWatcher(f, client, pulsetime=120.0, debounce_seconds=1.0)
    
    lp = LineProfiler()
    
    # Wrap methods to profile
    # We wrap the bound methods on the instances
    watcher.handler._process_event = lp(watcher.handler._process_event)
    watcher.handler._read_file_data = lp(watcher.handler._read_file_data)
    watcher.handler._process_state_change = lp(watcher.handler._process_state_change)
    client.send_heartbeat = lp(client.send_heartbeat)
    
    print("Starting watcher...")
    watcher.start()
    
    try:
        print("Simulating burst of 1000 updates...")
        simulate_burst(f, count=1000)
        
        print("Waiting for processing (3s)...")
        time.sleep(3.0)
    finally:
        watcher.stop()
        
    print("\n" + "="*50)
    print("LINE PROFILER RESULTS")
    print("="*50)
    lp.print_stats()

def test_profiling_benchmarks(tmp_path: Path) -> None:
    """
    Run profiling benchmarks and verify performance targets.
    
    Targets:
    - Burst processing (1000 events) < 5.0s
    - Avg heartbeat latency < 20ms (Online)
    """
    # Run profiling
    metrics, _ = run_profiling(tmp_path)
    assert metrics["burst_duration_s"] < 5.0, f"Burst processing too slow: {metrics['burst_duration_s']}s"
    
    # Run latency profiling
    latency_metrics, _ = run_latency_profiling(tmp_path)
    assert latency_metrics["online_ms"] < 20.0, f"Heartbeat latency too high: {latency_metrics['online_ms']}ms"

def test_visualization_export(tmp_path: Path) -> None:
    """
    Test that the visualization export path works (generating .prof and .png files).
    This ensures the reporting tools don't crash, even if matplotlib is missing.
    """
    export_dir = tmp_path / "profiles"
    
    # Mock simulate_burst and sleep to make test fast
    with patch("tests.test_performance.simulate_burst"), \
         patch("time.sleep"):
        run_profiling(tmp_path, profile_export_dir=export_dir)
        
    # Check that .prof files were generated (pstats doesn't require matplotlib)
    prof_files = list(export_dir.glob("*.prof"))
    assert len(prof_files) >= 4
    
    # Check matplotlib charts if available
    if plt:
        png_files = list(export_dir.glob("*.png"))
        assert len(png_files) >= 1

def run_full_report(tmp_path: Path, save_path: Optional[Path] = None, profile_export_dir: Optional[Path] = None) -> str:
    """Run all profiling tasks and print a consolidated report."""
    print("Running full performance profile (Stage 6.1.4)...")
    m1, report1 = run_profiling(tmp_path, profile_export_dir)
    m2, report2 = run_latency_profiling(tmp_path)
    m3, report3 = run_resource_benchmarks(tmp_path)
    report4 = run_benchmarking(tmp_path)
    # For the automated report, run a short 2-minute stress test, sampling every 15s
    m_stress, report_stress = run_stress_test(tmp_path, duration=120, sample_interval=15, profile_export_dir=profile_export_dir)

    print("\n" + "="*60)
    print("PERFORMANCE METRICS SUMMARY")
    print("="*60)
    print(f"Total Burst Processing Time (1000 events): {m1['burst_duration_s']:.4f}s")
    print(f"  - Avg Time/Event: {m1['avg_event_ms']:.4f}ms")
    print(f"CPU per component:")
    print(f"  - Watcher (Debounce): {m1.get('cpu_Watcher (Debounce)', 0):.4f}s")
    print(f"  - Watcher (I/O):      {m1.get('cpu_Watcher (I/O)', 0):.4f}s")
    print(f"  - Watcher (Logic):    {m1.get('cpu_Watcher (Logic)', 0):.4f}s")
    print(f"  - Client (Heartbeat): {m1.get('cpu_Client (Heartbeat)', 0):.4f}s")
    print("-" * 30)
    print(f"Avg Heartbeat Latency (ms):")
    print(f"  - Online:         {m2['online_ms']:.3f}ms")
    print(f"  - Offline:        {m2['offline_ms']:.3f}ms")
    print(f"  - Queuing Impact: {m2.get('queuing_impact_ms', 0):.3f}ms")
    print(f"  - Large Payload:  {m2['truncation_ms']:.3f}ms")
    print("-" * 30)
    print(f"Resource Usage:")
    print(f"  - Idle CPU:       {m3.get('idle_cpu_avg', 0):.2f}%")
    print(f"  - Active CPU:     {m3.get('active_cpu_avg', 0):.2f}%")
    print("-" * 30)
    print(f"Stress Test (120s):")
    print(f"  - Avg CPU:        {m_stress.get('avg_cpu_pct', 0):.2f}%")
    print(f"  - Peak RSS:       {m_stress.get('peak_rss_mb', 0):.2f} MB")
    print(f"  - RSS Growth:     {m_stress.get('rss_growth_mb', 0):.2f} MB ({m_stress.get('rss_growth_pct', 0):.2f}%)")
    print("="*60)

    # Construct full report string
    full_report = io.StringIO()
    full_report.write(f"**Date**: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    full_report.write(f"**System**: {platform.system()} {platform.release()} ({platform.machine()})\n")
    full_report.write(f"**Python**: {sys.version.split()[0]}\n\n")
    
    # Executive Summary Table
    full_report.write("### Executive Summary\n\n")
    full_report.write("| Metric | Value | Target | Status |\n")
    full_report.write("| :--- | :--- | :--- | :--- |\n")
    
    # Burst
    burst_time = m1.get('burst_duration_s', 0.0)
    burst_status = "✅" if burst_time < 5.0 else "❌"
    full_report.write(f"| **Burst Time (1000 events)** | {burst_time:.4f}s | < 5.0s | {burst_status} |\n")
    
    # Latency
    lat_avg = m2.get('online_ms', 0.0)
    lat_max = m2.get('online_max_ms', 0.0)
    lat_status = "✅" if lat_avg < 20.0 else "❌"
    full_report.write(f"| **Heartbeat Latency** | Avg: {lat_avg:.3f}ms / Max: {lat_max:.3f}ms | < 20ms (Avg) | {lat_status} |\n")
    
    # CPU
    if m3:
        idle_cpu = m3.get('idle_cpu_avg', 0.0)
        cpu_status = "✅" if idle_cpu < 1.0 else "❌"
        full_report.write(f"| **Idle CPU Avg** | {idle_cpu:.2f}% | < 1% | {cpu_status} |\n")
    else:
        full_report.write(f"| **Idle CPU Avg** | N/A | < 1% | ❓ (psutil missing) |\n")
        
    full_report.write("\n")
    
    full_report.write(report1)
    full_report.write("\n")
    full_report.write(report2)
    full_report.write("\n")
    full_report.write(report3)
    full_report.write("\n")
    full_report.write(report4)
    full_report.write("\n")
    full_report.write(report_stress)
    
    report_content = full_report.getvalue()

    if save_path:
        with open(save_path, "w") as f:
            f.write(report_content)
        print(f"\nReport saved to: {save_path}")
        
    return report_content

def update_performance_md(report_content: str, md_path: Path, start_marker: str = "<!-- BENCHMARK_REPORT_START -->", end_marker: str = "<!-- BENCHMARK_REPORT_END -->") -> None:
    """Update the PERFORMANCE.md file with the latest report."""
    if not md_path.exists():
        print(f"Creating new {md_path}...")
        default_content = """# Performance & Optimization - aw-watcher-pipeline-stage

## 1. Overview
This document tracks the performance characteristics, benchmarks, and optimization targets for the watcher.

## Performance Targets (Directive 3 & 6)
*   **Idle CPU**: < 1% (over 5+ minutes)
*   **Active CPU**: < 5% (during burst updates)
*   **Memory (RSS)**: < 50 MB
*   **Heartbeat Latency**: < 20ms (Online)
*   **Burst Processing**: 1000 events processed in < 5.0s

## 2. Latest Benchmark Report
<!-- BENCHMARK_REPORT_START -->
<!-- BENCHMARK_REPORT_END -->

<!-- STRESS_REPORT_START -->
<!-- STRESS_REPORT_END -->

## 3. Bottleneck Analysis Guide
*Automated analysis via `tests/test_performance.py`*
"""
        md_path.write_text(default_content, encoding="utf-8")

    content = md_path.read_text(encoding="utf-8")

    if start_marker not in content or end_marker not in content:
        print(f"Error: Markers {start_marker} and/or {end_marker} not found in {md_path}.")
        return

    pre = content.split(start_marker)[0]
    post = content.split(end_marker)[1]
    
    new_content = f"{pre}{start_marker}\n\n{report_content}\n{end_marker}{post}"
    md_path.write_text(new_content, encoding="utf-8")
    print(f"Updated {md_path} with latest benchmark report.")

if __name__ == "__main__":
    # Manual execution block for full run (default 300s for idle validation)
    import shutil
    import tempfile
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=300, help="Duration in seconds (Default: 300s for Directive 3)")
    parser.add_argument("--scenario", choices=["idle", "active", "pacing"], default="pacing", help="Test scenario: idle, active (rapid 0.5s), pacing (30s - Directive 3)")
    parser.add_argument("--profile", action="store_true", help="Run cProfile on watcher methods with burst simulation")
    parser.add_argument("--benchmark", action="store_true", help="Run timeit benchmarks on hot paths")
    parser.add_argument("--line-profile", action="store_true", help="Run line_profiler on watcher methods")
    parser.add_argument("--stress-test", type=int, nargs='?', const=3600, default=None, help="Run stress test for specified duration in seconds (default: 3600s).")
    parser.add_argument("--report", action="store_true", help="Run full performance report (Profile + Latency + Benchmark)")
    parser.add_argument("--save-report", type=str, help="Save report to file (e.g. PERFORMANCE_REPORT.md)")
    parser.add_argument("--update-performance-md", type=str, help="Update PERFORMANCE.md with the report (path to file)")
    parser.add_argument("--save-profiles", type=str, help="Directory to save .prof files for external visualization")
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
    
    if psutil is None:
        print("Error: psutil is not installed. Cannot run performance tests.")
        sys.exit(1)

    # Ensure package is in path for standalone execution
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    
    tmp_dir = Path(tempfile.mkdtemp())
    profile_dir = Path(args.save_profiles) if args.save_profiles else None
    
    if args.stress_test:
        try:
            # For manual stress test, sample every 5 minutes (300s) for long runs
            sample_interval = 300 if args.stress_test >= 1800 else 60
            metrics, report_content = run_stress_test(tmp_dir, duration=args.stress_test, sample_interval=sample_interval, profile_export_dir=profile_dir)
            
            if args.update_performance_md:
                full_report = io.StringIO()
                full_report.write(f"**Date**: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                full_report.write(f"**Type**: Stress Test ({args.stress_test}s)\n\n")
                full_report.write(report_content)
                update_performance_md(full_report.getvalue(), Path(args.update_performance_md), start_marker="<!-- STRESS_REPORT_START -->", end_marker="<!-- STRESS_REPORT_END -->")
        finally:
            shutil.rmtree(tmp_dir)
        sys.exit(0)
    
    if args.report or args.update_performance_md:
        try:
            save_path = Path(args.save_report) if args.save_report else None
            report_content = run_full_report(tmp_dir, save_path, profile_dir)
            
            if args.update_performance_md:
                update_performance_md(report_content, Path(args.update_performance_md))
        finally:
            shutil.rmtree(tmp_dir)
        sys.exit(0)

    if args.profile:
        try:
            run_profiling(tmp_dir, profile_dir)
            run_latency_profiling(tmp_dir)
        finally:
            shutil.rmtree(tmp_dir)
        sys.exit(0)

    if args.benchmark:
        try:
            run_benchmarking(tmp_dir)
        finally:
            shutil.rmtree(tmp_dir)
        sys.exit(0)

    if args.line_profile:
        try:
            run_line_profiling(tmp_dir)
        finally:
            shutil.rmtree(tmp_dir)
        sys.exit(0)

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
