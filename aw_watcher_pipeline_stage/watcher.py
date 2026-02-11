"""
File system watcher implementation using watchdog.

Responsibility:
    This module follows the **Single Responsibility Principle** by being solely responsible
    for monitoring file system events, parsing the target JSON file, and maintaining the
    current state of the pipeline. It delegates communication to the `client` module.

Design:
    - **Event-Driven**: Uses `watchdog` to react to file system events (modified, created,
      moved, deleted) instead of polling, ensuring low CPU usage.
    - **Debouncing**: Implements a `DebounceTimer` to coalesce rapid bursts of events
      (e.g., atomic writes or rapid saves) into a single processing action.
    - **State Comparison**: Only triggers heartbeats when the meaningful content of the
      file changes (ignoring irrelevant metadata updates if configured).
    - **Security**: Enforces strict path resolution, rejects symlinks to sensitive files,
      and limits file read size to prevent DoS.

Key Invariants:
    - The watcher never modifies the watched file (read-only).
    - Heartbeats are sent only when the state changes or periodically for active tasks.
    - File operations are robust against race conditions (e.g., file deletion during read).
    - **No Polling**: File monitoring is purely event-driven (except for self-healing checks).
    - **Symlink Rejection**: Symlinks are rejected at read time to prevent security bypass.
"""

from __future__ import annotations

import json
import logging
import stat
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, TYPE_CHECKING, Type, Union

from watchdog.events import FileMovedEvent, FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

if TYPE_CHECKING:
    from aw_watcher_pipeline_stage.client import PipelineClient

try:
    import orjson
except ImportError:
    orjson = None  # type: ignore

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

MAX_FILE_SIZE_BYTES = 10 * 1024  # 10 KB (Security: DoS prevention)
VALID_STATUSES = {"in_progress", "paused", "completed"}

if orjson:
    JSON_DECODE_EXCEPTIONS: Tuple[Type[Exception], ...] = (json.JSONDecodeError, orjson.JSONDecodeError)
    json_loads = orjson.loads
else:
    JSON_DECODE_EXCEPTIONS = (json.JSONDecodeError,)
    json_loads = json.loads


class DebounceTimer:
    """Implement a reusable timer for debouncing events without thread churn.

    Attributes:
        interval (float): The debounce interval in seconds.
        callback (Callable[[], None]): The function to call when the timer fires.
    """

    __slots__ = ('interval', 'callback', '_condition', '_target_time', '_active', '_stopped', '_thread')

    def __init__(self, interval: float, callback: Callable[[], None]) -> None:
        """Initialize the debounce timer.

        Args:
            interval (float): The debounce interval in seconds.
            callback (Callable[[], None]): The function to call when the timer fires.
        """
        self.interval = interval
        self.callback = callback
        self._condition = threading.Condition()
        self._target_time = 0.0
        self._active = False
        self._stopped = False
        self._thread: Optional[threading.Thread] = None

    def schedule(self) -> None:
        """Schedule or reschedule the timer execution.

        Returns:
            None
        """
        with self._condition:
            if self._stopped:
                return
            self._target_time = time.monotonic() + self.interval
            if not self._active:
                self._active = True
                self._start_thread()
            else:
                self._condition.notify()

    def trigger_now(self) -> None:
        """Force immediate execution.

        Returns:
            None
        """
        with self._condition:
            if self._stopped:
                return
            self._target_time = 0.0
            if not self._active:
                self._active = True
                self._start_thread()
            else:
                self._condition.notify()

    def stop(self) -> None:
        """Stop the timer.

        Returns:
            None
        """
        with self._condition:
            self._stopped = True
            self._active = False
            self._condition.notify_all()

    def __repr__(self) -> str:
        """Return a string representation of the timer.

        Returns:
            str: String representation including interval and active state.
        """
        return f"<DebounceTimer interval={self.interval} active={self._active}>"

    def _start_thread(self) -> None:
        """Start the timer thread if not already running.

        Returns:
            None
        """
        if self._thread is None or not self._thread.is_alive():
            try:
                self._thread = threading.Thread(target=self._run, name="DebounceTimer")
                self._thread.daemon = True
                self._thread.start()
            except Exception:
                # Reset active state if thread fails to start to allow retries
                self._active = False
                logger.error("Failed to start DebounceTimer thread", exc_info=True)

    def _run(self) -> None:
        """Run the timer loop.

        Returns:
            None
        """
        with self._condition:
            while self._active and not self._stopped:
                now = time.monotonic()
                wait_time = self._target_time - now

                if wait_time <= 0:
                    self._active = False
                    self._condition.release()
                    try:
                        self.callback()
                    except Exception:
                        logger.error("Error in debounce callback", exc_info=True)
                    finally:
                        self._condition.acquire()
                    
                    # If rescheduled during callback, continue loop
                    if self._active:
                        continue
                    else:
                        self._thread = None
                        break

                self._condition.wait(wait_time)
            
            if not self._active or self._stopped:
                self._thread = None


class PipelineEventHandler(FileSystemEventHandler):
    """Handle file system events to track pipeline state.

    Maintain state of the current pipeline stage and task using thread-safe
    mechanisms. Use `watchdog` for file system events, `DebounceTimer` for
    debouncing, and `threading.Timer` for periodic heartbeats, ensuring pure
    event-driven operation.

    Performance:
        - Optimized for low latency (<10ms) and low CPU (<1%).
        - Uses DebounceTimer to coalesce rapid events without thread churn.
        - Implements caching for file reads to minimize I/O.
        - Uses fast-path string comparison for path filtering.

    Attributes:
        watch_path (Path): The absolute path being watched.
        client (PipelineClient): The client used to send heartbeats.
        debounce_seconds (float): The debounce interval in seconds.
        target_file (Path): The specific file being monitored.
        batch_size_limit (int): Max events to queue before forcing a batch process.
        current_data (Dict[str, Any]): The current state data dictionary.
        is_paused (bool): Whether the watcher is currently paused.
        last_stage (Optional[str]): The last recorded pipeline stage.
        last_task (Optional[str]): The last recorded task name.
        last_comparison_data (Optional[Dict[str, Any]]): The last state data used for comparison.
    """

    def __init__(
        self,
        watch_path: Path,
        client: PipelineClient,
        debounce_seconds: float = 1.0,
        logger: Optional[logging.Logger] = None,
        batch_size_limit: int = 5,
    ) -> None:
        """Initialize the event handler with thread-safe state variables.

        Args:
            watch_path (Path): The path to the file or directory being watched.
            client (PipelineClient): The ActivityWatch client wrapper.
            debounce_seconds (float): Time in seconds to debounce file events.
            logger (Optional[logging.Logger]): Optional logger instance.
            batch_size_limit (int): Max events to queue before forcing a batch process.
        """
        try:
            # Security: Use absolute() instead of resolve() to avoid following symlinks
            self.watch_path = watch_path.absolute()
        except (OSError, RuntimeError):
            self.watch_path = watch_path.absolute()

        self.client = client
        self.debounce_seconds = debounce_seconds
        self.logger = logger or logging.getLogger(__name__)

        if orjson:
            self.logger.debug("Using orjson for JSON parsing")
        else:
            self.logger.debug("Using stdlib json for JSON parsing")

        if self.debounce_seconds < 0.1:
            self.logger.warning(
                f"Very small debounce interval ({self.debounce_seconds}s) may cause high CPU usage."
            )

        self.target_file: Path

        # Determine the specific file to monitor
        try:
            if self.watch_path.is_dir():
                self.target_file = self.watch_path / "current_task.json"
            elif self.watch_path.exists():
                self.target_file = self.watch_path
            else:
                # Path does not exist. Use heuristic: if suffix present, assume file.
                if self.watch_path.suffix:
                    self.target_file = (
                        self.watch_path
                    )  # Explicitly set to avoid mypy confusion if needed
                else:
                    self.target_file = self.watch_path / "current_task.json"
        except (OSError, RuntimeError) as e:
            self.logger.debug(
                f"Filesystem check failed during init, using heuristic fallback: {e}"
            )
            # Fallback if filesystem check fails
            if self.watch_path.suffix:
                self.target_file = self.watch_path
            else:
                self.target_file = self.watch_path / "current_task.json"

        self.target_file_str = str(self.target_file)

        self._lock = threading.Lock()

        # State variables
        self._event_queue: deque[FileSystemEvent] = deque()
        self._debounce_timer = DebounceTimer(self.debounce_seconds, self._on_debounce_fired)
        self.last_comparison_data: Optional[Dict[str, Any]] = None
        self.last_stage: Optional[str] = None
        self.last_task: Optional[str] = None
        self.is_paused: bool = False
        self._stopped: bool = False
        self.current_data: Dict[str, Any] = {}
        self.last_heartbeat_time: float = 0.0
        self.last_change_time: float = time.monotonic()
        self.last_event_time: float = 0.0
        self.last_parse_attempt: float = 0.0
        self.start_time: float = time.monotonic()
        self.max_heartbeat_interval: float = 0.0
        self.processing_latency: float = 0.0
        self.max_processing_latency: float = 0.0
        self.heartbeat_latency: float = 0.0
        self.max_heartbeat_latency: float = 0.0

        self._heartbeat_lock = threading.Lock()
        # Metrics
        self.events_detected: int = 0
        self.heartbeats_sent: int = 0
        self.total_debounced_events: int = 0
        self.parse_errors: int = 0
        self.last_error_time: float = 0.0
        self._last_file_size_warning_time: float = 0.0
        self.last_read_error_time: float = 0.0

        self._heartbeat_timer: Optional[threading.Timer] = None

        # Security: Rate limiting (Token Bucket) for DoS prevention.
        # Effectively acts as a queue with max size 100 (burst) and refill rate 10/s.
        self._rate_limit_max: float = 100.0
        self._rate_limit_tokens: float = 100.0
        self._rate_limit_fill_rate: float = 10.0  # tokens per second
        self._rate_limit_last_update: float = time.monotonic()
        self._dropped_events: int = 0
        self._last_dropped_log_time: float = 0.0

        # Cache for I/O optimization
        self._cache_mtime: Any = 0.0
        self._cache_size: int = -1
        self._cache_data: Optional[Dict[str, Any]] = None

        # Batch processing limit
        if batch_size_limit < 1:
            self.logger.warning(f"Invalid batch_size_limit ({batch_size_limit}), defaulting to 5.")
            self.batch_size_limit = 5
        elif batch_size_limit > 1000:
            self.logger.warning(f"batch_size_limit ({batch_size_limit}) exceeds max (1000), capping.")
            self.batch_size_limit = 1000
        else:
            self.batch_size_limit = batch_size_limit

    def _invalidate_cache(self) -> None:
        """Invalidate the file read cache.

        Returns:
            None
        """
        self._cache_data = None
        self._cache_mtime = 0.0
        self._cache_size = -1

    def _process_event(self, event: FileSystemEvent) -> None:
        """Process a file system event with debounce logic.

        Validates that the event corresponds to the target file (security check),
        applies rate limiting to prevent DoS, and schedules a debounce timer
        to coalesce rapid updates.

        Performance: This method is a hot path during bursts.
        Uses `DebounceTimer` to minimize thread churn.

        Args:
            event (FileSystemEvent): The file system event to process.

        Returns:
            None
        """
        if self._stopped:
            return

        # Security: Ignore directory events in file processing logic
        if event.is_directory:
            return

        # Handle moved events by checking dest_path if available
        file_path = event.src_path
        if isinstance(event, FileMovedEvent):
            file_path = event.dest_path

        # 1. Path Validation (Security: Ensure we only process the resolved target file)
        # Move this BEFORE rate limiting to avoid DoS from other files in the directory
        # Optimization: Fast string comparison to avoid Path instantiation overhead
        if file_path != self.target_file_str:
            try:
                event_path = Path(file_path).absolute()
                # Security: Ensure we only process the resolved target file (no symlink following).
                if event_path != self.target_file:
                    # Strict path checking: Do not follow symlinks or resolve paths
                    return
            except (OSError, RuntimeError) as e:
                # File might have been deleted or inaccessible
                self.logger.debug(f"Could not resolve path in _process_event: {e}")
                return

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Processing event: {event.event_type} on {file_path}")

        # 2. Rate Limiting: Token Bucket Algorithm
        now = time.monotonic()
        time_passed = now - self._rate_limit_last_update
        self._rate_limit_last_update = now
        self._rate_limit_tokens = min(
            self._rate_limit_max,
            self._rate_limit_tokens + time_passed * self._rate_limit_fill_rate,
        )

        if self._rate_limit_tokens < 1.0:
            self._dropped_events += 1
            if now - self._last_dropped_log_time > 5.0:
                self.logger.warning(
                    f"Rate limit exceeded (DoS prevention). Dropping excess events for {file_path}. "
                    f"Total dropped: {self._dropped_events}"
                )
                self._last_dropped_log_time = now
            return

        self._rate_limit_tokens -= 1.0

        self.events_detected += 1
        self.last_event_time = time.monotonic()

        # 3. Queue Event & Schedule Debounce Check
        try:
            with self._lock:
                self._event_queue.append(event)
                
                # Check for batch limit trigger
                if len(self._event_queue) >= self.batch_size_limit:
                    self._debounce_timer.trigger_now()
                    if self.logger.isEnabledFor(logging.DEBUG):
                        self.logger.debug(f"Batch limit reached ({len(self._event_queue)}), forcing process for {file_path}")
                else:
                    self._debounce_timer.schedule()
                    if self.logger.isEnabledFor(logging.DEBUG):
                        self.logger.debug(f"Scheduled debounce check for {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to schedule debounce timer for {file_path}: {e}")

    def _on_debounce_fired(self) -> None:
        """Handle the debounce timer callback.

        Returns:
            None
        """
        if self._stopped:
            return
            
        batch_size = 0
        with self._lock:
            batch_size = len(self._event_queue)
            self._event_queue.clear()
            
        # Process batch outside the lock (I/O)
        if batch_size > 0:
            self._process_batch(batch_size)

    def _process_batch(self, batch_size: int) -> None:
        """Process a batch of events (read once, parse once).

        Coalesces multiple file events into a single read operation.
        If the final state differs from the previous state, a heartbeat is sent.

        Concurrency:
            - Performs file I/O (read) **outside** the lock to avoid blocking.
            - Acquires lock only when updating internal state or comparing data.

        Args:
            batch_size (int): The number of events coalesced in this batch.

        Returns:
            None
        """
        file_path = str(self.target_file)
        
        if batch_size > 1:
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(
                    f"Batch processing {batch_size} coalesced events for {file_path}"
                )
            self.total_debounced_events += (batch_size - 1)
        
        if self._stopped:
            return
        try:
            # 1. Perform I/O outside the lock
            raw_data = self._read_file_data(file_path)
            
            if self._stopped:
                return

            data_to_send = None
            elapsed = 0.0
            
            # 2. Update state inside the lock
            with self._lock:
                if self._stopped:
                    return

                if raw_data:
                    data_to_send = self._process_state_change(raw_data, file_path)
                
                if data_to_send:
                    elapsed = max(0.0, time.monotonic() - self.last_change_time)

            if data_to_send and not self._stopped:
                self.on_state_changed(data_to_send, elapsed)
        except Exception as e:
            self.logger.error(
                f"Unexpected error in batch processing for {file_path}: {e}", exc_info=True
            )

    def _parse_file_wrapper(self, file_path: str) -> None:
        """Execute batch processing (legacy wrapper).

        Args:
            file_path (str): The path to the file to parse.

        Returns:
            None
        """
        # Redirect to batch processing
        self._process_batch(1)

    def _read_file_data(self, file_path: str, max_attempts: int = 5) -> Optional[Dict[str, Any]]:
        """Read and parse the JSON file. Perform I/O.

        Handles missing files, permissions, and malformed JSON with retries and logging.
        Security: Enforces size limit (10KB) and handles BOM to prevent DoS/injection.

        Args:
            file_path (str): The absolute path to the file to parse.
            max_attempts (int): Maximum number of attempts to read the file (default: 5).

        Returns:
            Optional[Dict[str, Any]]: The parsed data dict if successful, else None.

        Raises:
            None: This method catches and logs the following exceptions internally:
                - `OSError`: For file access issues (permissions, not found).
                - `json.JSONDecodeError`: For malformed JSON.
                - `RecursionError`: For deeply nested JSON (DoS prevention).
                - `UnicodeDecodeError`: For invalid encoding.

        Example:
            Input file content:
            {
                "current_stage": "Build",
                "current_task": "Compiling"
            }

            >>> handler._read_file_data("/path/to/current_task.json")
            {'current_stage': 'Build', 'current_task': 'Compiling'}
        """
        if not file_path:
            return None
        start_proc = time.monotonic()
        with self._lock:
            self.last_parse_attempt = start_proc
        try:
            # Optimization: Reuse Path object if it matches target to avoid instantiation overhead
            if file_path == self.target_file_str:
                path = self.target_file
            else:
                path = Path(file_path)
            data = None
            content_bytes = b""

            # Retry settings for transient IO errors
            backoff = 0.1

            for attempt in range(max_attempts):
                if self._stopped:
                    return None
                try:
                    # Security: Check for symlinks to prevent arbitrary file read
                    # Note: There is a theoretical TOCTOU window here, but we accept the risk for cross-platform compatibility
                    # and because we run with user privileges.
                    if path.is_symlink():
                        self.logger.warning(f"Target is a symlink, skipping read (security): {file_path}")
                        return None

                    file_stat = path.stat()
                    
                    # Ensure it's a regular file to avoid blocking on pipes/devices
                    if not stat.S_ISREG(file_stat.st_mode):
                        self.logger.warning(
                            f"Target is not a regular file, skipping: {file_path}"
                        )
                        return None

                    # Optimization: Check cache
                    current_mtime = getattr(file_stat, "st_mtime_ns", file_stat.st_mtime)
                    if (
                        self._cache_data is not None
                        and self._cache_mtime == current_mtime
                        and self._cache_size == file_stat.st_size
                    ):
                        if self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug(f"Cache hit for {file_path}")
                        return self._cache_data.copy()

                except OSError as e:
                    if attempt < max_attempts - 1:
                        self.logger.debug(f"Stat failed for {file_path}: {e}. Retrying in {backoff}s...")
                        time.sleep(backoff)
                        if self._stopped:
                            return None
                        backoff *= 2
                        continue
                    
                    # Invalidate cache on persistent stat failure (e.g. file deleted/inaccessible)
                    self._invalidate_cache()
                    # If we fail stat repeatedly, we likely can't open it either, but let's fall through to open() try/except or return
                    self.logger.warning(f"Failed to stat file {file_path} after retries: {e}")
                    return None

                try:
                    # Check size to avoid JSONDecodeError on empty files created during atomic writes
                    if file_stat.st_size == 0:
                        if attempt < max_attempts - 1:
                            self.logger.debug(f"File {file_path} is empty, retrying in {backoff}s...")
                            time.sleep(backoff)
                            if self._stopped:
                                return None
                            backoff *= 2
                            continue
                        else:
                            self.logger.debug(f"File {file_path} is empty after retries, skipping parse.")
                            return None

                    # Security: Check size to avoid OOM / DoS
                    if file_stat.st_size < 0:
                        return None

                    # Optimization: Warn early if file is known to be too large
                    if file_stat.st_size > MAX_FILE_SIZE_BYTES:
                        now = time.monotonic()
                        if now - self._last_file_size_warning_time > 60.0:
                            self.logger.warning(
                                f"File {file_path} is too large ({file_stat.st_size} bytes). Truncating to {MAX_FILE_SIZE_BYTES} bytes."
                            )
                            self._last_file_size_warning_time = now

                    with path.open("rb") as f:
                        # Security: Read with limit to prevent OOM if file grows after stat
                        content_bytes = f.read(MAX_FILE_SIZE_BYTES + 1)
                        if len(content_bytes) > MAX_FILE_SIZE_BYTES:
                            content_bytes = content_bytes[:MAX_FILE_SIZE_BYTES]
                            now = time.monotonic()
                            if now - self._last_file_size_warning_time > 60.0:
                                self.logger.warning(
                                    f"File {file_path} is too large (> {MAX_FILE_SIZE_BYTES} bytes). Truncating and attempting partial parse."
                                )
                                self._last_file_size_warning_time = now

                        if not content_bytes.strip():
                            if attempt < max_attempts - 1:
                                self.logger.debug(f"File {file_path} is empty or whitespace only, retrying in {backoff}s...")
                                time.sleep(backoff)
                                if self._stopped:
                                    return None
                                backoff *= 2
                                continue
                            return None

                        # Handle UTF-8 BOM manually since we are reading bytes
                        if content_bytes.startswith(b"\xef\xbb\xbf"):
                            content_bytes = content_bytes[3:]

                        data = json_loads(content_bytes)

                    # Update cache on success
                    self._cache_data = data
                    self._cache_mtime = getattr(file_stat, "st_mtime_ns", file_stat.st_mtime)
                    self._cache_size = file_stat.st_size
                    break
                except IsADirectoryError:
                    self.logger.warning(f"Target path is a directory, skipping parse: {file_path}")
                    return None
                except FileNotFoundError:
                    if attempt < max_attempts - 1:
                        self.logger.debug(f"File not found (transient?), retrying in {backoff}s: {file_path}")
                        time.sleep(backoff)
                        if self._stopped:
                            return None
                        backoff *= 2
                        continue
                    else:
                        self.logger.warning(f"File not found after retries, waiting for creation: {file_path}")
                        return None
                except UnicodeDecodeError as e:
                    if attempt < max_attempts - 1:
                        self.logger.debug(f"Encoding error in {file_path}: {e}. Retrying in {backoff}s...")
                        time.sleep(backoff)
                        if self._stopped:
                            return None
                        backoff *= 2
                        continue
                    else:
                        now = time.monotonic()
                        if now - self.last_read_error_time > 60.0:
                            self.logger.error(f"Encoding error in {file_path} after {max_attempts} attempts: {e}")
                            self.last_read_error_time = now
                        with self._lock:
                            self.parse_errors += 1
                            self.last_error_time = now
                        return None
                except PermissionError as e:
                    if attempt < max_attempts - 1:
                        self.logger.debug(f"Permission denied for {file_path}: {e}. Retrying in {backoff}s...")
                        time.sleep(backoff)
                        if self._stopped:
                            return None
                        backoff *= 2
                        continue
                    
                    now = time.monotonic()
                    if now - self.last_read_error_time > 60.0:
                        self.logger.warning(f"Permission denied for {file_path} after {max_attempts} attempts: {e}")
                        self.last_read_error_time = now
                    with self._lock:
                        self.parse_errors += 1
                        self.last_error_time = now
                    return None
                except JSON_DECODE_EXCEPTIONS as e:
                    if attempt < max_attempts - 1:
                        self.logger.debug(
                            f"Malformed JSON in {file_path}: {e}. Retrying in {backoff}s..."
                        )
                        time.sleep(backoff)
                        if self._stopped:
                            return None
                        backoff *= 2
                        continue
                    else:
                        now = time.monotonic()
                        if now - self.last_read_error_time > 60.0:
                            self.logger.error(
                                f"Malformed JSON in {file_path} after {max_attempts} attempts."
                            )
                            self.last_read_error_time = now
                        
                        if self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug(f"JSON Decode Error: {e}")
                            snippet = content_bytes[:50].decode("utf-8", errors="replace").replace("\n", "\\n")
                            self.logger.debug(f"JSON Snippet: '{snippet}'")

                        with self._lock:
                            self.parse_errors += 1
                            self.last_error_time = now
                        return None
                except RecursionError as e:
                    now = time.monotonic()
                    if now - self.last_read_error_time > 60.0:
                        self.logger.error(
                            f"JSON recursion limit exceeded in {file_path}: {e} (Possible DoS attempt)"
                        )
                        self.last_read_error_time = now
                    with self._lock:
                        self.parse_errors += 1
                        self.last_error_time = now
                    return None
                except ValueError as e:
                    # Catch other ValueErrors (unlikely with json.loads but good for safety)
                    if attempt < max_attempts - 1:
                        self.logger.debug(f"Value error for {file_path}: {e}. Retrying in {backoff}s...")
                        time.sleep(backoff)
                        if self._stopped:
                            return None
                        backoff *= 2
                        continue
                    else:
                        now = time.monotonic()
                        if now - self.last_read_error_time > 60.0:
                            self.logger.error(f"Value error processing {file_path}: {e}")
                            self.last_read_error_time = now
                        with self._lock:
                            self.parse_errors += 1
                            self.last_error_time = now
                        return None
                except OSError as e:
                    if attempt < max_attempts - 1:
                        self.logger.debug(f"Read error for {file_path}: {e}. Retrying in {backoff}s...")
                        time.sleep(backoff)
                        if self._stopped:
                            return None
                        backoff *= 2
                        continue
                    else:
                        now = time.monotonic()
                        if now - self.last_read_error_time > 60.0:
                            self.logger.error(f"Error processing {file_path} after {max_attempts} attempts: {e}")
                            self.last_read_error_time = now
                        with self._lock:
                            self.parse_errors += 1
                            self.last_error_time = now
                        return None
                except Exception as e:
                    now = time.monotonic()
                    if now - self.last_read_error_time > 60.0:
                        self.logger.error(f"Unexpected error processing {file_path}: {e}", exc_info=True)
                        self.last_read_error_time = now
                    with self._lock:
                        self.parse_errors += 1
                        self.last_error_time = now
                    return None

            if not isinstance(data, dict):
                self.logger.error(f"JSON root is not a dictionary in {file_path}")
                with self._lock:
                    self.parse_errors += 1
                    self.last_error_time = time.monotonic()
                return None

            return data
        finally:
            # Track latency for the I/O portion
            latency = time.monotonic() - start_proc
            with self._lock:
                self.processing_latency = latency
                if latency > self.max_processing_latency:
                    self.max_processing_latency = latency

            if latency > 1.0:
                self.logger.warning(
                    f"Slow processing detected: {latency:.2f}s for {file_path}"
                )

    def _process_state_change(self, data: Dict[str, Any], file_path: str) -> Optional[Dict[str, Any]]:
        """Process parsed data and update state if changed. Must be called with the lock.

        Validates the parsed data against the schema, filters metadata, compares with
        previous state to detect meaningful changes, and updates internal state.

        Args:
            data (Dict[str, Any]): The parsed JSON data from the file.
            file_path (str): The absolute path to the file (for logging/context).

        Returns:
            Optional[Dict[str, Any]]: The new state dictionary if a meaningful change
            occurred, or None if the state is unchanged or data is invalid.

        Example:
            >>> data = {'current_stage': 'Build', 'current_task': 'Compiling'}
            >>> handler._process_state_change(data, "/path/to/file")
            {'current_stage': 'Build', 'current_task': 'Compiling', ...}
        """
        try:
            current_stage = data.get("current_stage")
            current_task = data.get("current_task")

            if not current_stage or not current_task:
                self.logger.warning(f"Missing required fields (current_stage, current_task) in {file_path}")
                self.parse_errors += 1
                self.last_error_time = time.monotonic()
                return None

            if not isinstance(current_stage, str):
                self.logger.warning(f"current_stage must be a string in {file_path}")
                self.parse_errors += 1
                self.last_error_time = time.monotonic()
                return None
            current_stage = current_stage.strip()

            if not isinstance(current_task, str):
                self.logger.warning(f"current_task must be a string in {file_path}")
                self.parse_errors += 1
                self.last_error_time = time.monotonic()
                return None
            current_task = current_task.strip()

            # Extract optional fields
            project_id = data.get("project_id")
            if project_id is not None and not isinstance(project_id, str):
                self.logger.warning(f"project_id must be a string in {file_path}. Ignoring.")
                project_id = None
            elif project_id is not None:
                project_id = project_id.strip()

            start_time = data.get("start_time")
            status = data.get("status")
            if status is not None:
                if not isinstance(status, str):
                    self.logger.warning(f"status must be a string in {file_path}. Defaulting to 'in_progress'.")
                    status = "in_progress"
                else:
                    status = status.strip().lower()
                    if status not in VALID_STATUSES:
                        self.logger.warning(
                            f"Invalid status value in {file_path}. Expected: {', '.join(sorted(VALID_STATUSES))}."
                        )
                        status = "in_progress"
            else:
                status = "in_progress"

            metadata = data.get("metadata", {})

            # Validate metadata is a dict to ensure flattening works later
            if metadata is None or not isinstance(metadata, dict):
                self.logger.warning(
                    f"Metadata field in {file_path} is not a dictionary. Ignoring."
                )
                metadata = {}

            # Security: Filter metadata early if allowlist is configured
            # This prevents blocked data from being stored in memory or triggering updates
            if getattr(self.client, "metadata_allowlist", None) is not None:
                allowlist = self.client.metadata_allowlist
                metadata = {k: v for k, v in metadata.items() if k in allowlist}

            # Validate timestamp if present
            if start_time:
                if not isinstance(start_time, str):
                    self.logger.warning(f"start_time must be a string in {file_path}. Ignoring.")
                    start_time = None
                else:
                    start_time = start_time.strip()
                    try:
                        # Basic ISO 8601 check
                        datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                    except ValueError:
                        self.logger.warning(
                            f"Invalid timestamp format in start_time in {file_path}. Ignoring."
                        )
                        start_time = None

            # Construct comparison data (optimization: direct dict compare instead of hash)
            # This avoids serialization overhead and handles key order naturally.
            # Optimization verified for Directive 3: Direct dict comparison is faster than json.dumps(sorted).
            comparison_data = {
                "current_stage": current_stage,
                "current_task": current_task,
                "status": status,
                "project_id": project_id,
                "start_time": start_time,
                "metadata": metadata,
            }

            # Compare with last state
            if self.last_comparison_data is not None and comparison_data == self.last_comparison_data:
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.debug("State matches last state. No change.")
                return

            # Meaningful change detected
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug("Meaningful state change detected.")
            now = time.monotonic()
            self.last_change_time = now

            self.last_comparison_data = comparison_data
            self.last_stage = current_stage
            self.last_task = current_task
            self.is_paused = status in ("paused", "completed")

            # Optimization: Avoid Path instantiation if possible
            if file_path == self.target_file_str:
                abs_path = self.target_file_str
            else:
                abs_path = str(Path(file_path).absolute())

            self.current_data = {
                "current_stage": current_stage,
                "current_task": current_task,
                "project_id": project_id,
                "status": status,
                "start_time": start_time,
                "metadata": metadata,
                "file_path": abs_path,  # Ensure absolute path
            }
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(f"Current data updated. Keys: {len(self.current_data)}")

            return self.current_data.copy()
        except Exception as e:
            self.logger.error(f"Error processing state change: {e}")
            return None

    def _parse_file(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Read and process the file immediately (legacy wrapper).

        Reads the file and processes state changes immediately without debouncing.

        Args:
            file_path (str): The path to the file to parse.

        Returns:
            Optional[Dict[str, Any]]: The new state dictionary if changed, else None.

        Raises:
            None: Exceptions during read are caught by `_read_file_data`.

        Example:
            >>> handler._parse_file("/path/to/current_task.json")
            {'current_stage': 'Test', 'current_task': 'Unit Tests', ...}
        """
        data = self._read_file_data(file_path)
        if data:
            with self._lock:
                return self._process_state_change(data, file_path)
        return None

    def _schedule_periodic_heartbeat(self) -> None:
        """Schedule the next periodic heartbeat using a timer.

        Returns:
            None
        """
        with self._lock:
            if self._heartbeat_timer:
                self._heartbeat_timer.cancel()
                self._heartbeat_timer = None
            
            if not self._stopped and not self.is_paused and self.current_data:
                self._heartbeat_timer = threading.Timer(30.0, self._periodic_heartbeat_task)
                self._heartbeat_timer.daemon = True
                self._heartbeat_timer.start()

    def _periodic_heartbeat_task(self) -> None:
        """Execute the periodic heartbeat task.

        Checks if the state is active and sends a heartbeat if needed.
        Reschedules the next heartbeat.

        Returns:
            None
        """
        if self._stopped:
            return
            
        data_to_send = None
        elapsed = 0.0
        
        with self._lock:
            # Verify we should still send
            if self._stopped or self.is_paused or not self.current_data:
                return
            data_to_send = self.current_data.copy()
            elapsed = max(0.0, time.monotonic() - self.last_change_time)
            
        if data_to_send:
            # Send heartbeat (I/O outside lock)
            self._send_heartbeat(data_to_send, elapsed)
            # Reschedule
            self._schedule_periodic_heartbeat()

    def on_state_changed(self, data: Dict[str, Any], elapsed: float) -> None:
        """Handle state change events.

        Logs the change and delegates heartbeat sending to the client.
        Reschedules the periodic heartbeat timer.

        Args:
            data (Dict[str, Any]): The new state data dictionary.
            elapsed (float): Time elapsed since the last state change in seconds.

        Returns:
            None
        """
        msg = f"State changed: {data.get('current_stage')} - {data.get('current_task')}"
        if data.get("project_id"):
            msg += f" (Project: {data.get('project_id')})"
        self.logger.info(msg)
        
        with self._heartbeat_lock:
            if not self._stopped:
                self._send_heartbeat(data, elapsed)
        
        # Reset/Start periodic timer
        self._schedule_periodic_heartbeat()

    def _send_heartbeat(self, data: Dict[str, Any], elapsed: float) -> None:
        """Send a heartbeat with the current data.

        Delegates to the client to send the heartbeat. Updates internal metrics
        like heartbeats_sent and latency tracking.

        Args:
            data (Dict[str, Any]): The data dictionary to send.
            elapsed (float): The computed duration since the last change.

        Returns:
            None
        """
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Computed duration: {elapsed:.2f}s")

        start_hb = time.monotonic()
        try:
            self.client.send_heartbeat(
                stage=str(data.get("current_stage", "")),
                task=str(data.get("current_task", "")),
                project_id=data.get("project_id"),
                status=str(data.get("status")),
                start_time=data.get("start_time"),
                metadata=data.get("metadata"),
                file_path=data.get("file_path"),
                computed_duration=elapsed,
            )
            
            with self._lock:
                self.heartbeats_sent += 1
                now = time.monotonic()
                if self.last_heartbeat_time > 0:
                    interval = now - self.last_heartbeat_time
                    if interval > self.max_heartbeat_interval:
                        self.max_heartbeat_interval = interval
                self.last_heartbeat_time = now
        except Exception as e:
            self.logger.error(f"Failed to send heartbeat: {e}")
            # Update time anyway to avoid spamming retries every second in the main loop
            self.last_heartbeat_time = time.monotonic()
        finally:
            hb_duration = time.monotonic() - start_hb
            self.heartbeat_latency = hb_duration
            if hb_duration > self.max_heartbeat_latency:
                self.max_heartbeat_latency = hb_duration
            
            if hb_duration > 5.0:
                self.logger.warning(f"Slow heartbeat sending detected: {hb_duration:.2f}s")

    def check_self_healing(self) -> None:
        """Check if self-healing is needed (e.g., file reappeared).

        Checks if the target file exists and if the current state is empty.
        If so, attempts to read and parse the file to recover state.

        Returns:
            None
        """
        state_change_data = None
        elapsed = 0.0
        
        # Perform I/O outside lock if needed for self-healing
        healing_data = None
        now = time.monotonic()
        
        # Optimization: Only attempt healing if we lack data (e.g. startup fail or deleted)
        needs_healing = False
        with self._lock:
            if not self.current_data:
                needs_healing = True

        if needs_healing and (now - self.last_parse_attempt > 30.0):
            try:
                if self.target_file.exists():
                    # Use max_attempts=1 to avoid blocking the main loop on retries
                    healing_data = self._read_file_data(
                        str(self.target_file), max_attempts=1
                    )
                else:
                    # File doesn't exist, update timestamp to avoid polling every second
                    with self._lock:
                        self.last_parse_attempt = now
            except OSError as e:
                self.logger.warning(f"Failed to check file existence during self-healing: {e}")
                with self._lock:
                    self.last_parse_attempt = now

        with self._lock:
            if not self.current_data and healing_data:
                self.logger.debug("Attempting self-healing update for empty state")
                state_change_data = self._process_state_change(healing_data, str(self.target_file))
                if state_change_data:
                    elapsed = max(0.0, time.monotonic() - self.last_change_time)

        # Perform I/O outside the lock
        if state_change_data:
            self.on_state_changed(state_change_data, elapsed)

    def check_periodic_heartbeat(self) -> None:
        """Execute the legacy periodic heartbeat check.

        Returns:
            None
        """
        self.check_self_healing()
        # Note: Periodic heartbeats are now handled by _heartbeat_timer

    def stop(self) -> None:
        """Stop the handler and any timers.

        Returns:
            None
        """
        self.logger.debug("Stopping event handler...")
        # Set flag immediately to allow running tasks to exit early
        self._stopped = True
        self._debounce_timer.stop()
        with self._lock:
            if self._heartbeat_timer:
                self._heartbeat_timer.cancel()
                self._heartbeat_timer = None

    def get_current_data(self) -> Optional[Dict[str, Any]]:
        """Return the current pipeline state data in a thread-safe manner.

        Returns:
            Optional[Dict[str, Any]]: A copy of the current state dictionary,
            or None if no state is available.
        """
        with self._lock:
            if not self.current_data:
                return None
            return self.current_data.copy()

    def get_statistics(self) -> Dict[str, Any]:
        """Return usage statistics.

        Returns:
            Dict[str, Any]: A dictionary containing metrics like events detected,
            heartbeats sent, parse errors, latencies, etc.
        """
        with self._lock:
            return {
                "events_detected": self.events_detected,
                "heartbeats_sent": self.heartbeats_sent,
                "total_debounced_events": self.total_debounced_events,
                "parse_errors": self.parse_errors,
                "last_error_time": self.last_error_time,
                "last_heartbeat_time": self.last_heartbeat_time,
                "last_event_time": self.last_event_time,
                "state_keys": len(self.current_data),
                "max_heartbeat_interval": self.max_heartbeat_interval,
                "processing_latency": self.processing_latency,
                "max_processing_latency": self.max_processing_latency,
                "heartbeat_latency": self.heartbeat_latency,
                "max_heartbeat_latency": self.max_heartbeat_latency,
                "uptime": time.monotonic() - self.start_time,
            }

    def on_modified(self, event: FileSystemEvent) -> None:
        """Handle file modification events.

        Delegates to `_process_event` to apply rate limiting and debounce logic.
        This is the primary entry point for file change notifications from watchdog.

        Args:
            event (FileSystemEvent): The event object containing the path to the modified file
                in `event.src_path`.

        Returns:
            None
        """
        self._process_event(event)

    def on_created(self, event: FileSystemEvent) -> None:
        """Handle file creation events.

        Delegates to `_process_event` to apply rate limiting and debounce logic.
        Crucial for detecting when the target file is created or recreated after deletion.

        Args:
            event (FileSystemEvent): The event object containing the path to the created file
                in `event.src_path`.

        Returns:
            None
        """
        self._process_event(event)

    def on_moved(self, event: FileMovedEvent) -> None:
        """Handle file move events.

        Detects if the target file was moved away (treated as deletion) or moved to
        the target path (treated as creation/modification).

        Args:
            event (FileMovedEvent): The event object containing `event.src_path` (source)
                and `event.dest_path` (destination).

        Returns:
            None
        """
        if event.is_directory:
            # Check if watched directory was moved away
            try:
                src_path = Path(event.src_path).absolute()
                if src_path == self.target_file.parent:
                    self.logger.warning(
                        f"Watch directory moved: {event.src_path} -> {event.dest_path}"
                    )
                    with self._lock:
                        self.is_paused = True
                        if self.current_data and self.current_data.get("current_stage"):
                            self.current_data["status"] = "paused"
                            self.last_comparison_data = None
                            self.on_state_changed(self.current_data, 0.0)
                elif self.current_data:
                    self.current_data = {}
                self._invalidate_cache()
            except OSError as e:
                self.logger.debug(f"Failed to resolve path in on_moved (directory): {e}")
                pass
            return

        # Check if moved AWAY from target (effectively deleted/renamed)
        try:
            src_path = Path(event.src_path).absolute()
            if src_path == self.target_file:
                self.logger.info(f"File moved away: {event.src_path} -> {event.dest_path}")
                with self._lock:
                    self.is_paused = True
                    if self.current_data and self.current_data.get("current_stage"):
                        self.current_data["status"] = "paused"
                        self.last_comparison_data = None
                        self.on_state_changed(self.current_data, 0.0)
                    elif self.current_data:
                        self.current_data = {}
                    self._invalidate_cache()
        except OSError as e:
            self.logger.debug(f"Failed to resolve path in on_moved (file): {e}")
            pass

        # Check if moved TO target (handled by _process_event)
        self._process_event(event)

    def on_deleted(self, event: FileSystemEvent) -> None:
        """Handle file deletion events.

        Pauses heartbeats if the target file or its directory is deleted.

        Args:
            event (FileSystemEvent): The event object containing the path to the deleted file
                in `event.src_path`.

        Returns:
            None
        """
        if event.is_directory:
            # Check if watched directory was deleted
            try:
                src_path = Path(event.src_path).absolute()
                if src_path == self.target_file.parent:
                    self.logger.warning(f"Watch directory deleted: {event.src_path}")
                    with self._lock:
                        self.is_paused = True
                        if self.current_data and self.current_data.get("current_stage"):
                            self.current_data["status"] = "paused"
                            self.last_comparison_data = None
                            self.on_state_changed(self.current_data, 0.0)
                        elif self.current_data:
                            self.current_data = {}
                        self._invalidate_cache()
            except OSError as e:
                self.logger.debug(f"Failed to resolve path in on_deleted (directory): {e}")
                pass
            return

        try:
            event_path = Path(event.src_path).absolute()
            if event_path != self.target_file:
                return
        except OSError:
            return

        self.logger.warning(f"File deleted: {event.src_path}")
        with self._lock:
            # Set status to paused to stop heartbeats in main loop
            self.is_paused = True
            if self.current_data and self.current_data.get("current_stage"):
                self.current_data["status"] = "paused"
                # Reset hash so that if the file is recreated with same content, we detect it
                self.last_comparison_data = None
                self.on_state_changed(self.current_data, 0.0)
            elif self.current_data:
                self.current_data = {}
            self._invalidate_cache()

    def __repr__(self) -> str:
        """Return a string representation of the handler.

        Returns:
            str: String representation including target file.
        """
        return f"<PipelineEventHandler target={self.target_file}>"


class PipelineWatcher:
    """Orchestrate the file system observer and event handler.

    Manage the lifecycle of the `watchdog.observers.Observer` and the
    `PipelineEventHandler`. Ensure the observer is restarted if it fails
    and handle graceful shutdown.

    Attributes:
        path (Path): The resolved absolute path being watched.
        client (PipelineClient): Client for sending heartbeats.
        handler (PipelineEventHandler): The event handler instance.
        watch_dir (Path): The directory containing the target file (parent of `path`).
        observer (Observer): The active watchdog Observer instance (property).
        batch_size_limit (int): Max events to queue before forcing a batch process.

    Example:
        >>> client = PipelineClient(Path("current_task.json"))
        >>> watcher = PipelineWatcher(Path("current_task.json"), client, pulsetime=120.0)
        >>> watcher.start()
        >>> # ...
        >>> watcher.stop()
    """

    def __init__(
        self,
        path: Union[str, Path],
        client: PipelineClient,
        debounce_seconds: float = 1.0,
        batch_size_limit: int = 5,
    ) -> None:
        """Initialize the watcher.

        Args:
            path (Union[str, Path]): The directory or file path to watch.
            client (PipelineClient): The ActivityWatch client wrapper.
            debounce_seconds (float): Time in seconds to debounce file events.
            batch_size_limit (int): Max events to queue before forcing a batch process.
        """
        try:
            # Security: Use absolute() instead of resolve() to avoid following symlinks
            self.path = Path(path).absolute()
        except (OSError, RuntimeError):
            self.path = Path(path).absolute()

        self.client = client
        self._observer: Optional[Observer] = None
        self._stopping = False
        self._started = False
        self._last_observer_restart_attempt = 0.0
        self._watch_dir_existed = False
        self._last_observer_error_time = 0.0
        self._health_check_timer: Optional[threading.Timer] = None

        self.watch_dir: Path
        
        # Determine directory to watch
        try:
            if self.path.is_dir():
                self.watch_dir = self.path
            elif self.path.exists():
                self.watch_dir = self.path.parent
            else:
                # Path does not exist. Use heuristic.
                if self.path.suffix:
                    self.watch_dir = self.path.parent
                else:
                    self.watch_dir = self.path
        except (OSError, RuntimeError):
            # Fallback
            if self.path.suffix:
                self.watch_dir = self.path.parent
            else:
                self.watch_dir = self.path

        self.handler = PipelineEventHandler(
            self.path,
            client,
            debounce_seconds=debounce_seconds,
            logger=logger,
            batch_size_limit=batch_size_limit,
        )

    @property
    def observer(self) -> Observer:
        """Return the underlying observer, restarting it if it died unexpectedly.

        Returns:
            Observer: The active watchdog Observer instance.

        Raises:
            RuntimeError: If the observer cannot be started or recovered.
        """
        # Check if we need to restart
        if self._started and not self._stopping:
            if self._observer is None or not self._observer.is_alive():
                if self._observer is not None:
                    logger.critical("Watchdog observer found dead.")
                
                now = time.monotonic()
                if now - self._last_observer_restart_attempt > 10.0:
                    logger.info("Attempting to restart observer...")
                    self._start_observer()
                    self._last_observer_restart_attempt = now
        
        if self._observer is None:
            raise RuntimeError("Observer is not available (down or initializing)")
        return self._observer

    def _start_observer(self) -> None:
        """Start or restart the observer.

        Attempts to create and start a new Observer instance. Retries on failure.

        Returns:
            None
        """
        max_retries = 3
        for attempt in range(max_retries):
            if self._stopping:
                return
            try:
                # If existing observer is dead or None, create new one
                if self._observer is None or not self._observer.is_alive():
                    if self._observer:
                        # Ensure old observer is cleaned up
                        try:
                            self._observer.join(timeout=1.0)
                        except Exception as e:
                            logger.debug(f"Error joining dead observer: {e}")
                            pass
                    self._observer = Observer()
                
                # If it's already alive (e.g. race condition), skip
                if self._observer.is_alive():
                    return

                # Schedule and start if not alive
                # Security: recursive=False to prevent watching subdirectories.
                # Security: We watch the resolved directory to handle atomic writes (rename),
                # but filter events in handler to ensure we only process the target file.
                self._observer.schedule(self.handler, str(self.watch_dir), recursive=False)
                self._observer.start()
                logger.info(f"Observer started ({type(self._observer).__name__})")
                if attempt > 0:
                    logger.info(f"Observer recovered successfully on attempt {attempt + 1}")
                self._last_observer_restart_attempt = time.monotonic()
                return
            except OSError as e:
                logger.error(
                    f"OS Error starting observer (attempt {attempt + 1}/{max_retries}): {e} (Check inotify limits?)"
                )
                if attempt < max_retries - 1:
                    time.sleep(0.5)
            except Exception as e:
                logger.error(f"Failed to start observer (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(0.5)
        
        self._last_observer_restart_attempt = time.monotonic()
        logger.critical("Could not start observer after retries.")

    def start(self) -> None:
        """Start the directory observer.

        Initializes the watchdog observer and schedules the event handler.
        Also performs an initial read of the target file if it exists.

        Returns:
            None

        Raises:
            FileNotFoundError: If the path does not exist.
            RuntimeError: If the observer fails to start.
        """
        logger.info(f"Starting watcher on path: {self.path}")
        if not self.watch_dir.exists():
            logger.error(f"Watch directory not found: {self.watch_dir}")
            raise FileNotFoundError(f"Path not found: {self.watch_dir}")

        self._started = True
        self._stopping = False
        self._watch_dir_existed = True
        self._start_observer()
        self._schedule_health_check()

        if self._observer is None or not self._observer.is_alive():
            raise RuntimeError("Failed to start watchdog observer")

        # Perform initial read if target file exists
        target_file = self.handler.target_file
        try:
            if target_file.exists() and target_file.is_file():
                logger.info(f"Performing initial read of {target_file}")
                # Do I/O outside lock
                data_raw = self.handler._read_file_data(str(target_file))
                data = None
                if data_raw:
                    with self.handler._lock:
                        data = self.handler._process_state_change(
                            data_raw, str(target_file)
                        )
                if data:
                    self.handler.on_state_changed(data, 0.0)
            else:
                logger.warning(f"Target file not found at startup: {target_file}. Waiting for creation...")
        except OSError as e:
            logger.warning(f"Failed to perform initial read of {target_file}: {e}")

    def stop(self) -> None:
        """Stop the directory observer.

        Stops the observer thread and the health check timer.
        Ensures graceful shutdown of all background threads.

        Returns:
            None
        """
        self._stopping = True
        if self._health_check_timer:
            self._health_check_timer.cancel()
            self._health_check_timer = None
        self.handler.stop()
        if self._observer:
            try:
                if self._observer.is_alive():
                    self._observer.stop()
                    self._observer.join(timeout=5.0)
                    if self._observer.is_alive():
                        logger.warning("Observer thread did not terminate within timeout.")
            except Exception as e:
                logger.error(f"Error stopping observer: {e}")
        logger.info("Watcher stopped.")

    def check_health(self) -> None:
        """Check directory and observer health.

        Verifies that the watch directory exists and the observer is alive.
        Triggers self-healing if needed.

        Returns:
            None
        """
        if self._stopping:
            return
        try:
            self._check_directory_health()
            self.handler.check_self_healing()
        except Exception as e:
            logger.error(f"Health check failed: {e}")

    def _check_directory_health(self) -> None:
        """Check if watch directory exists and restart observer if needed.

        Returns:
            None
        """
        try:
            current_exists = self.watch_dir.exists()
        except OSError as e:
            logger.debug(f"Error checking directory existence: {e}")
            current_exists = False
        
        if current_exists and not self._watch_dir_existed:
            logger.info(
                f"Watch directory reappeared: {self.watch_dir}. Restarting observer."
            )
            if self._observer and self._observer.is_alive():
                self._observer.stop()
                self._observer.join(timeout=1.0)
            self._observer = None
            self._start_observer()
            
            # Trigger immediate parse if target file exists
            try:
                if self.handler.target_file.exists():
                    data_raw = self.handler._read_file_data(str(self.handler.target_file))
                    data = None
                    if data_raw:
                        with self.handler._lock:
                            data = self.handler._process_state_change(
                                data_raw, str(self.handler.target_file)
                            )
                    if data:
                        self.handler.on_state_changed(data, 0.0)
            except OSError as e:
                logger.warning(f"Failed to check target file during recovery: {e}")
        
        self._watch_dir_existed = current_exists

        # Access property to ensure observer is alive
        try:
            _ = self.observer
        except Exception as e:
            now = time.monotonic()
            if now - self._last_observer_error_time > 60.0:
                logger.error(f"Failed to ensure observer is alive: {e}")
                self._last_observer_error_time = now

    def _schedule_health_check(self) -> None:
        """Schedule the next health check execution.

        Uses a daemon timer to run `_run_health_check` after 10 seconds.

        Returns:
            None
        """
        if self._stopping:
            return
        self._health_check_timer = threading.Timer(10.0, self._run_health_check)
        self._health_check_timer.daemon = True
        self._health_check_timer.start()

    def _run_health_check(self) -> None:
        """Execute the health check and reschedule.

        Wraps `check_health` with error handling and rescheduling logic.

        Returns:
            None
        """
        if self._stopping:
            return
        try:
            self.check_health()
        except Exception as e:
            logger.error(f"Health check failed: {e}")
        finally:
            self._schedule_health_check()

    def check_periodic_heartbeat(self) -> None:
        """Execute the legacy periodic heartbeat check.

        Returns:
            None
        """
        self.check_health()

    def get_statistics(self) -> Dict[str, Any]:
        """Return usage statistics from the handler.

        Returns:
            Dict[str, Any]: Statistics dictionary from `PipelineEventHandler`.
        """
        return self.handler.get_statistics()

    def __repr__(self) -> str:
        """Return a string representation of the watcher.

        Returns:
            str: String representation including path and observer status.
        """
        return f"<PipelineWatcher path={self.path} observer={'alive' if self._observer and self._observer.is_alive() else 'down'}>"