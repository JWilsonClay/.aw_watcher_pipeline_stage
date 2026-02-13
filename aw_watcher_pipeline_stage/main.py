"""Main entry point for aw-watcher-pipeline-stage.

This module handles the command-line interface (CLI), configuration loading,
logging setup, and the main event loop. It orchestrates the initialization
of the PipelineClient and PipelineWatcher components.

Key Responsibilities:
    - CLI Argument Parsing: Handles --watch-path, --port, --testing, etc.
    - Signal Handling: Registers handlers for SIGINT/SIGTERM to ensure graceful
      shutdown.
      Sets a stop event to break the main loop, triggering cleanup in the finally block.
      Force exits with os._exit(1) if a second signal is received (stuck shutdown).
    - Resource Management: Monitors CPU/Memory usage and logs anomalies.
    - Logging: Configures structured logging with rotation (10MB) and privacy sanitization.
    - Startup/Shutdown Invariants: Ensures graceful exit with resource cleanup
      (flushing event queue, closing client) via atexit and finally blocks.
    - Privacy: Logs a privacy notice on startup confirming local-only operation.
"""

from __future__ import annotations

import argparse
import atexit
import logging
import os
import signal
import sys
import threading
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from types import FrameType
from typing import Optional

try:
    import resource
except ImportError:
    resource = None  # type: ignore

from aw_watcher_pipeline_stage import __version__
from aw_watcher_pipeline_stage.client import PipelineClient
from aw_watcher_pipeline_stage.config import load_config
from aw_watcher_pipeline_stage.watcher import PipelineWatcher


__all__ = ["setup_logging", "log_resource_usage", "main"]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# Global state for resource usage tracking
_last_rusage = None
_last_rusage_time = 0.0
_last_info_log_time = 0.0

# Logging configuration constants
LOG_FORMAT = '[%(asctime)s] [%(levelname)s] %(name)s: %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'


def setup_logging(log_level: str, log_file: Optional[str]) -> None:
    """Configure the logging system.

    Sets up console logging (stdout) and optional file logging with rotation.

    Logging Practices:
        - **Levels**:
            - ``INFO``: Normal operations (state changes, startup).
            - ``WARNING``: Recoverable issues (file not found, permission denied).
            - ``ERROR``: Critical failures (unhandled exceptions, startup failure).
            - ``DEBUG``: Detailed diagnostics (payloads, raw events).
        - **Format**: ``[asctime] [levelname] name: message``
        - **Output**: Console is always active. File logging is optional via ``--log-file``.
        - **Rotation**: Log files are rotated at 10MB (keeping 5 backups) to prevent disk exhaustion.
        - **Privacy**: Logs are sanitized to exclude sensitive data (e.g., file content snippets are only logged in DEBUG; paths in payloads are anonymized).

    Args:
        log_level (str): The logging level (e.g., "DEBUG", "INFO", "WARNING", "ERROR").
        log_file (Optional[str]): Optional path to a log file. If provided, logs are written here.

    Returns:
        None

    Raises:
        ValueError: If the provided log_level is not a valid logging level.

    Example:
        >>> setup_logging("INFO", "/path/to/watcher.log")
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    handlers = []
    formatter = logging.Formatter(
        LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT  # ISO 8601 format
    )

    # Console handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)

    if log_file:
        try:
            # Rotate at 10MB, keep 5 backups
            file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)
        except Exception as e:
            # Fallback to console only, but print warning to stderr since logging isn't setup yet
            sys.stderr.write(f"Warning: Failed to setup log file '{log_file}': {e}\n")

    logging.basicConfig(level=numeric_level, handlers=handlers, force=True)



def log_resource_usage(watcher: Optional[PipelineWatcher] = None) -> None:
    """Log current resource usage (CPU, Memory, Threads) and watcher statistics.

    Monitors the process's resource consumption against defined targets (<1% CPU, <50MB RSS).
    Logs warnings if anomalies are detected.
    Updates global state (`_last_rusage`, `_last_rusage_time`) to track CPU usage over time intervals.

    Note:
        Resource usage monitoring requires the `resource` module (Unix-only).
        On non-Unix systems, only thread count and watcher stats are logged.

    Args:
        watcher (Optional[PipelineWatcher]): PipelineWatcher instance to retrieve internal statistics
            (e.g., event counts, latencies) for logging.

    Returns:
        None

    Example:
        >>> log_resource_usage(my_watcher_instance)
    """
    global _last_rusage, _last_rusage_time, _last_info_log_time

    now = time.monotonic()
    stats_msg = ""
    max_rss_mb = 0.0
    cpu_percent = 0.0

    if resource:
        try:
            usage = resource.getrusage(resource.RUSAGE_SELF)

            # ru_maxrss is in KB on Linux, bytes on macOS.
            max_rss = usage.ru_maxrss
            if sys.platform == "darwin":
                max_rss_mb = max_rss / (1024 * 1024)
            else:
                max_rss_mb = max_rss / 1024

            # Calculate CPU percentage since last check
            if _last_rusage and _last_rusage_time > 0:
                time_delta = now - _last_rusage_time
                # Avoid division by zero or extremely small intervals (e.g. rapid calls on shutdown)
                if time_delta > 1.0:
                    user_delta = usage.ru_utime - _last_rusage.ru_utime
                    sys_delta = usage.ru_stime - _last_rusage.ru_stime
                    cpu_percent = ((user_delta + sys_delta) / time_delta) * 100

                    # Only update baseline if we successfully calculated a new interval
                    _last_rusage = usage
                    _last_rusage_time = now

            if _last_rusage is None:
                _last_rusage = usage
                _last_rusage_time = now

            # Add FD count (Linux/Unix)
            try:
                if os.path.exists('/proc/self/fd'):
                    num_fds = len(os.listdir('/proc/self/fd'))
                    stats_msg += f", FDs={num_fds}"
            except Exception as e:
                logger.debug(f"Failed to count FDs: {e}")
                pass

            stats_msg += f", User Time={usage.ru_utime:.2f}s, Sys Time={usage.ru_stime:.2f}s"
        except Exception as e:
            logger.debug(f"Failed to get resource usage: {e}")

    if watcher:
        try:
            stats = watcher.get_statistics()
            stats_msg += (
                f", Events={stats.get('events_detected', 0)}, "
                f"Heartbeats={stats.get('heartbeats_sent', 0)}, "
                f"Debounced={stats.get('total_debounced_events', 0)}, "
                f"Errors={stats.get('parse_errors', 0)}, "
                f"Keys={stats.get('state_keys', 0)}"
            )

            uptime = stats.get("uptime", 0.0)
            m, s = divmod(int(uptime), 60)
            h, m = divmod(m, 60)
            stats_msg += f", Uptime={h:02d}:{m:02d}:{s:02d}"

            last_evt = stats.get("last_event_time", 0.0)
            if last_evt > 0:
                ago_evt = now - last_evt
                stats_msg += f", LastEvt={ago_evt:.1f}s"

            last_hb = stats.get("last_heartbeat_time", 0.0)
            if last_hb > 0:
                ago = now - last_hb
                stats_msg += f", LastHB={ago:.1f}s"

            max_hb_int = stats.get("max_heartbeat_interval", 0.0)
            if max_hb_int > 0:
                stats_msg += f", MaxHBInt={max_hb_int:.1f}s"

            latency = stats.get("processing_latency", 0.0)
            max_latency = stats.get("max_processing_latency", 0.0)
            if latency > 0 or max_latency > 0:
                stats_msg += f", Latency={latency:.3f}s (Max={max_latency:.3f}s)"

            hb_latency = stats.get("heartbeat_latency", 0.0)
            max_hb_latency = stats.get("max_heartbeat_latency", 0.0)
            if hb_latency > 0 or max_hb_latency > 0:
                stats_msg += f", HBLatency={hb_latency:.3f}s (Max={max_hb_latency:.3f}s)"

            last_err = stats.get("last_error_time", 0.0)
            if last_err > 0:
                ago_err = now - last_err
                stats_msg += f", LastErr={ago_err:.1f}s"
        except Exception as e:
            logger.debug(f"Failed to get watcher stats: {e}")

    # Thresholds: RSS > 50MB, CPU > 10%, Threads > 10 (target is <1% idle, 10% is anomaly)
    try:
        thread_count = threading.active_count()
    except Exception as e:
        logger.debug(f"Failed to get thread count: {e}")
        thread_count = -1
    is_anomaly = max_rss_mb > 50 or cpu_percent > 10.0 or thread_count > 10
    should_log_info = (now - _last_info_log_time) > 300.0  # Log INFO every 5 minutes

    status_label = "ANOMALY" if is_anomaly else "OK"

    if resource:
        msg = (
            f"Resource Usage (PID={os.getpid()}) [{status_label}]: "
            f"Max RSS={max_rss_mb:.2f}MB (Target <50MB), "
            f"CPU={cpu_percent:.2f}% (Target <1%), Threads={thread_count}{stats_msg}"
        )
    else:
        msg = (
            f"Resource Usage (PID={os.getpid()}) [{status_label}]: Max RSS=N/A, "
            f"CPU=N/A, Threads={thread_count}{stats_msg}"
        )

    if is_anomaly:
        logger.warning(f"High resource usage detected: {msg}")
        _last_info_log_time = now
    elif should_log_info:
        logger.info(msg)
        _last_info_log_time = now
    elif logger.isEnabledFor(logging.DEBUG):
        logger.debug(msg)



def main() -> None:
    """Execute the main application logic.

    Parse command-line arguments (from `sys.argv`) using `argparse`, load configuration, set up logging,
    and start the watcher loop. Handle the lifecycle of the application,
    including startup checks (bucket creation) and graceful shutdown on signals.
    Ensure graceful exit with resource cleanup.

    Command-line arguments handled:
        --watch-path (str): Path to the directory or file to watch.
        --port (int): Port of the ActivityWatch server (default: 5600).
        --testing (bool): Run in testing mode (mock client).
        --debug (bool): Enable debug logging (overrides --log-level).
        --log-file (str): Path to the log file.
        --log-level (str): Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.
        --pulsetime (float): Heartbeat merge window.
        --debounce-seconds (float): Time in seconds to debounce file events.
        --metadata-allowlist (str): Comma-separated list of allowed metadata keys.
        --batch-size-limit (int): Max events to queue before forcing a batch process.

    Returns:
        None: The function returns None but may exit the process with a status code.

    Raises:
        SystemExit: If configuration is invalid (code 2), dependencies are missing, or
            fatal errors occur during startup (code 1).

    Example:
        $ aw-watcher-pipeline-stage --watch-path ./my-project --log-level DEBUG
    """
    parser = argparse.ArgumentParser(
        description="ActivityWatch watcher for pipeline stages."
    )
    parser.add_argument(
        "--watch-path",
        type=str,
        default=None,
        help="Path to the file or directory to watch.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port of the ActivityWatch server (default: 5600).",
    )
    parser.add_argument(
        "--testing",
        action="store_const",
        const=True,
        default=None,
        help="Run in testing mode.",
    )
    parser.add_argument(
        "--no-testing",
        action="store_const",
        const=False,
        dest="testing",
        help="Disable testing mode (overrides config/env).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging (overrides --log-level).",
    )
    parser.add_argument(
        "--log-file", type=str, default=None, help="Path to the log file."
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO",
    )
    parser.add_argument(
        "--pulsetime",
        type=float,
        default=None,
        help="Time in seconds to wait before considering a task finished (default: 120.0).",
    )
    parser.add_argument(
        "--debounce-seconds",
        type=float,
        default=None,
        help="Time in seconds to debounce file events (default: 1.0).",
    )
    parser.add_argument(
        "--metadata-allowlist",
        type=str,
        default=None,
        help="Comma-separated list of allowed metadata keys (default: All).",
    )
    parser.add_argument(
        "--batch-size-limit",
        type=int,
        default=None,
        help="Max events to queue before forcing a batch process (1-1000) (default: 5).",
    )

    args = parser.parse_args()

    # Bootstrap logging to capture config loading events
    # Use the same format as the final setup for consistency
    bootstrap_formatter = logging.Formatter(
        LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT
    )
    bootstrap_handler = logging.StreamHandler(sys.stdout)
    bootstrap_handler.setFormatter(bootstrap_formatter)
    bootstrap_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=bootstrap_level, handlers=[bootstrap_handler], force=True)

    try:
        # Load configuration
        config = load_config(vars(args))
        logger.debug(f"Configuration loaded: {config}")

        # Setup logging
        setup_logging(config.log_level, config.log_file)
        if config.log_level == "DEBUG":
            logger.debug(f"Debug logging enabled. Log file: {config.log_file}")
    except ValueError as e:
        sys.exit(f"Configuration Error: {e}")
    except Exception as e:
        sys.exit(f"Startup Error: {e}")
    logger.info(f"Starting aw-watcher-pipeline-stage v{__version__} (PID: {os.getpid()})...")
    # Privacy Compliance: Explicitly state local-only operation on startup
    # Audit (Stage 8.4.5): Verified local-only notice.
    logger.info("Privacy Notice: This watcher runs 100% locally and sends no telemetry.")

    # Watch path is already resolved and validated in config (str -> Path)
    watch_path = Path(config.watch_path)

    # Initialize variables for cleanup safety
    client: Optional[PipelineClient] = None
    watcher: Optional[PipelineWatcher] = None
    resource_timer: Optional[threading.Timer] = None
    cleanup_done = False

    # Event to signal shutdown
    stop_event = threading.Event()

    # Define cleanup function
    def cleanup() -> None:
        """Perform resource cleanup on exit.

        Registered via `atexit` to ensure execution on normal interpreter termination.
        Order of operations:
        1. Stop Watcher (prevents new events).
        2. Flush Client Queue (sends pending events).
        3. Close Client (cleans up worker thread).
        4. Shutdown Logging.

        Flushes stdio (safely), cancels timers, logs final resource usage,
        stops the watcher (observer.stop/join),
        flushes the client queue, and closes the client connection.

        This function suppresses and logs any exceptions that occur during cleanup
        to ensure the process exits as cleanly as possible.

        Returns:
            None
        """
        nonlocal resource_timer, client, watcher, cleanup_done

        # Ensure any waiters are woken up (redundancy for atexit calls)
        stop_event.set()

        # Idempotency check
        if cleanup_done:
            return
        cleanup_done = True

        # Ensure streams are flushed early to capture any pending output
        try:
            if sys:
                if getattr(sys, "stdout", None):
                    sys.stdout.flush()
                if getattr(sys, "stderr", None):
                    sys.stderr.flush()
        except Exception:
            pass

        try:
            # We use a try-except block for logging in case it's already shut down
            if logger.isEnabledFor(logging.INFO):
                logger.info("Cleanup started...")
            except Exception:
                pass

            if resource_timer:
                try:
                    resource_timer.cancel()
                    # Join if it's a thread, though cancel is usually enough for Timer
                    if resource_timer.is_alive():
                        resource_timer.join(timeout=0.1)
                except Exception:
                    pass
                resource_timer = None

            # Log final resource usage
            if watcher:
                try:
                    log_resource_usage(watcher)
                except Exception:
                    pass

            if watcher:
                try:
                    logger.info("Stopping watcher...")
                    # watcher.stop() handles observer.stop() and observer.join()
                    watcher.stop()
                    logger.info("Watcher stopped.")
                    logger.debug("Watcher cleanup complete.")
                except Exception as e:
                    try:
                        logger.error(f"Error stopping watcher in cleanup: {e}")
                    except Exception:
                        pass
                finally:
                    watcher = None

            if client:
                try:
                    try:
                        # Flush queue before closing to ensure no data loss
                        # Note: This blocks until the queue is empty. If the worker is stuck,
                        # the process will hang until a second signal forces exit.
                        client.flush_queue()
                    except Exception as e:
                        try:
                            logger.error(f"Error flushing queue: {e}")
                        except Exception:
                            pass
                finally:
                    try:
                        # client.close() stops the worker thread and closes connection
                        client.close()
                        logger.debug("Client cleanup complete.")
                    except Exception as e:
                        try:
                            logger.error(f"Error closing client: {e}")
                        except Exception:
                            pass
                    finally:
                        client = None

            try:
                logger.info("Cleanup complete.")
            except Exception:
                pass

        finally:
            # Ensure logging is flushed and shut down
            # This must be the very last step to ensure previous logs are emitted.
            try:
                if logging:
                    try:
                        logger.info("Shutting down logging...")
                    except Exception:
                        pass
                    logging.shutdown()
            except Exception:
                pass

            # Final flush of stdio to ensure all logs/errors are out
            try:
                if sys:
                    if getattr(sys, "stdout", None):
                        sys.stdout.flush()
                    if getattr(sys, "stderr", None):
                        sys.stderr.flush()
            except Exception:
                pass

    atexit.register(cleanup)

    def signal_handler(sig: int, frame: Optional[FrameType]) -> None:
        """Handle system signals (SIGINT, SIGTERM) for graceful shutdown.

        Sets the stop event to trigger the main loop termination.
        The main loop will exit, triggering the finally block which calls cleanup().
        If called a second time (stuck shutdown), forces immediate exit.

        Args:
            sig (int): The signal number.
            frame (Optional[FrameType]): The current stack frame (unused).

        Returns:
            None
        """
        sig_name = str(sig)
        try:
            sig_name = signal.Signals(sig).name
        except Exception:
            pass

        _ = frame  # Unused argument
        if stop_event.is_set():
            try:
                msg = f"Received signal {sig_name} again, forcing immediate exit..."
                sys.stderr.write(f"\n{msg}\n")
                sys.stderr.flush()
                logger.critical(msg)
            except Exception:
                pass
            # Use os._exit to force termination if stuck, bypassing cleanup handlers
            os._exit(1)

        # Set event immediately to ensure responsiveness.
        # This breaks the main loop (stop_event.wait()), triggering the finally block
        # which calls cleanup() to stop observer, flush queue, and close client.
        stop_event.set()

        try:
            # Use _ = frame to avoid unused variable warning if linters are strict
            logger.debug(f"Signal handler caught {sig_name}")
            logger.info(f"Received signal {sig_name} ({sig}), shutting down...")
        except Exception:
            # Fallback if logging is broken/closed
            try:
                sys.stderr.write(
                    f"Received signal {sig_name}, shutting down... (logging unavailable)\n"
                )
                sys.stderr.flush()
                sys.stdout.flush()
            except Exception:
                pass

    # Signal Handling & Cleanup (Verified Stage 8.4.5 - Final Integration):
    # 1. Registers SIGINT/SIGTERM handlers.
    # 2. Sets stop_event to break main loop gracefully.
    # 3. cleanup() ensures observer stop, queue flush, client close, logging shutdown.
    # 4. atexit provides redundancy for unhandled exits.
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    logger.debug("Signal handlers registered.")

    try:
        # Initialize Client
        client = PipelineClient(
            watch_path=watch_path,
            port=config.port,
            testing=config.testing,
            pulsetime=config.pulsetime,
            metadata_allowlist=config.metadata_allowlist,
        )

        # Wait for server before starting watcher (with timeout for offline support)
        # We wait up to 5 seconds for the server to appear, otherwise we proceed.
        client.wait_for_start(timeout=5.0, stop_check=stop_event.is_set)

        if stop_event.is_set():
            logger.info("Stop signal received during startup, exiting...")
            return

        # Ensure bucket exists (or queue creation if offline)
        try:
            client.ensure_bucket()
        except Exception as e:
            logger.warning(f"Could not ensure bucket (proceeding in offline/queued mode): {e}")

        if stop_event.is_set():
            logger.info("Stop signal received during startup, exiting...")
            return

        # Initialize and Start Watcher
        logger.info(
            f"Initializing watcher for: {watch_path} (Pulsetime: {config.pulsetime}s)"
        )
        watcher = PipelineWatcher(
            watch_path,
            client,
            debounce_seconds=config.debounce_seconds,
            batch_size_limit=config.batch_size_limit,
        )

        # Robust startup: Wait for directory to exist AND start successfully
        while not stop_event.is_set():
            try:
                if not watcher.watch_dir.exists():
                    logger.warning(
                        f"Watch directory not found: {watcher.watch_dir}. Waiting for creation..."
                    )
                    if stop_event.wait(5.0):
                        break
                    continue

                watcher.start()
                break
            except FileNotFoundError:
                logger.warning(
                    f"Watch directory disappeared during startup: {watcher.watch_dir}. Retrying..."
                )
                if stop_event.wait(1.0):
                    break
                continue
            except OSError as e:
                logger.warning(
                    f"Error accessing watch directory during startup: {e}. Retrying..."
                )
                if stop_event.wait(5.0):
                    break
                continue
            except RuntimeError as e:
                logger.warning(
                    f"Failed to start watcher (observer error): {e}. Retrying..."
                )
                if stop_event.wait(5.0):
                    break
                continue

        if resource:
            logger.info("Resource usage monitoring enabled.")
        else:
            logger.warning("Resource usage monitoring not available (resource module missing).")

        # Log initial resource usage
        log_resource_usage(watcher)

        # Resource usage logging timer
        def run_resource_log() -> None:
            """Log resource usage periodically.

            Run log_resource_usage() and reschedule itself if the
            application is still running.

            Catches and logs exceptions to prevent the timer thread from crashing.

            Returns:
                None
            """
            nonlocal resource_timer
            if stop_event.is_set():
                return
            try:
                log_resource_usage(watcher)
            except Exception as e:
                logger.error(f"Error logging resource usage: {e}")
            finally:
                if not stop_event.is_set():
                    resource_timer = threading.Timer(60.0, run_resource_log)
                    resource_timer.daemon = True
                    resource_timer.start()

        resource_timer = threading.Timer(60.0, run_resource_log)
        resource_timer.daemon = True
        resource_timer.start()

        # Main loop: Wait for stop signal (pure event-driven)
        # This blocks the main thread efficiently without polling until the signal
        # handler sets the event.
        logger.debug("Entering main loop, waiting for stop signal...")
        stop_event.wait()
        logger.debug("Main loop stop_event set, exiting loop.")

    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        cleanup()
        atexit.unregister(cleanup)
    sys.exit(0)


if __name__ == "__main__":
    main()
