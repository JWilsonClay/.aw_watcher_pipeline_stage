"""
File system watcher implementation using watchdog.
"""

from __future__ import annotations

import json
import logging
import stat
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING, Union

from watchdog.events import FileMovedEvent, FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

if TYPE_CHECKING:
    from aw_watcher_pipeline_stage.client import PipelineClient

logger = logging.getLogger(__name__)

MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB


class PipelineEventHandler(FileSystemEventHandler):
    """Event handler for file system events.

    Maintains state of the current pipeline stage and task using thread-safe
    mechanisms. Uses watchdog for file system events and threading.Timer for
    debouncing and heartbeats, ensuring pure event-driven operation.
    """

    def __init__(
        self,
        watch_path: Path,
        client: PipelineClient,
        pulsetime: float,
        debounce_seconds: float = 1.0,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the event handler with thread-safe state variables.

        Args:
            watch_path: The path to the file or directory being watched.
            client: The ActivityWatch client wrapper.
            pulsetime: The pulsetime for heartbeats.
            debounce_seconds: Time in seconds to debounce file events.
            logger: Optional logger instance.
        """
        self.watch_path = watch_path.absolute()
        self.client = client
        self.pulsetime = pulsetime
        self.debounce_seconds = debounce_seconds
        self.logger = logger or logging.getLogger(__name__)

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
            self.logger.debug(f"Filesystem check failed during init, using heuristic fallback: {e}")
            # Fallback if filesystem check fails
            if self.watch_path.suffix:
                self.target_file = self.watch_path
            else:
                self.target_file = self.watch_path / "current_task.json"

        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None

        # State variables
        self.last_comparison_data: Optional[Dict[str, Any]] = None
        self.last_stage: Optional[str] = None
        self.last_task: Optional[str] = None
        self.is_paused: bool = False
        self._stopped: bool = False
        self.current_data: Dict[str, Any] = {}
        self.last_heartbeat_time: float = 0.0
        self.last_change_time: float = time.monotonic()
        self.last_event_time: float = 0.0
        self._debounce_counter: int = 0
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

    def _process_event(self, event: FileSystemEvent) -> None:
        """Process a file system event with debounce logic.

        Args:
            event: The file system event to process.
        """
        if self._stopped:
            return

        if event.is_directory:
            return

        # Handle moved events by checking dest_path if available
        file_path = event.src_path
        if isinstance(event, FileMovedEvent):
            file_path = event.dest_path

        self.logger.debug(f"Processing event: {event.event_type} on {file_path}")
        # Check if the event matches our target file
        try:
            event_path = Path(file_path).absolute()
            if event_path != self.target_file:
                # Fallback: check if resolved path matches (handles symlink targets)
                if event_path.resolve() != self.target_file.resolve():
                    return
        except (OSError, RuntimeError) as e:
            # File might have been deleted or inaccessible
            self.logger.debug(f"Could not resolve path in _process_event: {e}")
            return

        self.events_detected += 1
        self.last_event_time = time.monotonic()

        try:
            with self._lock:
                if self._timer:
                    self._timer.cancel()
                    self._debounce_counter += 1
                    if self._debounce_counter > 50 and self._debounce_counter % 50 == 0:
                        self.logger.warning(
                            f"High frequency file events detected: {self._debounce_counter} events debounced for {file_path}"
                        )
                    else:
                        self.logger.debug(
                            f"Debouncing rapid change for {file_path} (count={self._debounce_counter})"
                        )
                else:
                    self._debounce_counter = 0

                # Short delay to allow file write to complete before parsing
                self._timer = threading.Timer(
                    self.debounce_seconds,
                    self._parse_file_wrapper,
                    args=[str(self.target_file)],
                )
                self._timer.daemon = True
                self._timer.start()
        except Exception as e:
            self.logger.error(f"Failed to schedule debounce timer for {file_path}: {e}")

    def _parse_file_wrapper(self, file_path: str) -> None:
        """Wrapper to run _parse_file in a thread-safe manner from Timer."""
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

                # Verify this is the active timer executing
                if self._timer != threading.current_thread():
                    self.logger.debug("Skipping stale timer execution")
                    return

                if self._debounce_counter > 0:
                    self.logger.info(
                        f"Debounce coalesced {self._debounce_counter} rapid events for {file_path}"
                    )
                    self.total_debounced_events += self._debounce_counter
                self._debounce_counter = 0
                
                if raw_data:
                    data_to_send = self._process_state_change(raw_data, file_path)
                
                if data_to_send:
                    elapsed = max(0.0, time.monotonic() - self.last_change_time)
                self._timer = None

            if data_to_send and not self._stopped:
                self.on_state_changed(data_to_send, elapsed)
        except Exception as e:
            self.logger.error(
                f"Unexpected error in debounce timer for {file_path}: {e}", exc_info=True
            )

    def _read_file_data(self, file_path: str, max_attempts: int = 5) -> Optional[Dict[str, Any]]:
        """Read and parse the JSON file. Performs I/O.

        Handles missing files, permissions, and malformed JSON with retries and logging.

        Args:
            file_path: The path to the file to parse.
            max_attempts: Maximum number of attempts to read the file (default: 5).

        Returns:
            The parsed data dict if successful, else None.
        """
        if not file_path:
            return None
        start_proc = time.monotonic()
        with self._lock:
            self.last_parse_attempt = start_proc
        try:
            path = Path(file_path)
            data = None

            # Retry settings for transient IO errors
            backoff = 0.1

            for attempt in range(max_attempts):
                if self._stopped:
                    return
                try:
                    file_stat = path.stat()
                    
                    # Ensure it's a regular file to avoid blocking on pipes/devices
                    if not stat.S_ISREG(file_stat.st_mode):
                        self.logger.warning(
                            f"Target is not a regular file, skipping: {file_path}"
                        )
                        return None
                except OSError as e:
                    if attempt < max_attempts - 1:
                        self.logger.debug(f"Stat failed for {file_path}: {e}. Retrying in {backoff}s...")
                        time.sleep(backoff)
                        if self._stopped:
                            return None
                        backoff *= 2
                        continue
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

                    # Safety check for very large files (stability)
                    if file_stat.st_size > MAX_FILE_SIZE_BYTES:
                        now = time.monotonic()
                        if now - self._last_file_size_warning_time > 60.0:
                            self.logger.warning(
                                f"File {file_path} is too large ({file_stat.st_size} bytes), skipping parse."
                            )
                            self._last_file_size_warning_time = now
                        return None

                    with path.open("r", encoding="utf-8-sig") as f:
                        # Read with limit to prevent OOM if file grows after stat
                        content = f.read(MAX_FILE_SIZE_BYTES + 1)
                        if len(content) > MAX_FILE_SIZE_BYTES:
                            self.logger.warning(
                                f"File {file_path} content exceeds limit ({MAX_FILE_SIZE_BYTES} bytes), skipping parse."
                            )
                            return None
                        data = json.loads(content)
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
                        self.logger.error(f"Permission denied for {file_path} after {max_attempts} attempts: {e}")
                        self.last_read_error_time = now
                    with self._lock:
                        self.parse_errors += 1
                        self.last_error_time = now
                    return None
                except json.JSONDecodeError as e:
                    snippet = content[:50].replace("\n", "\\n")
                    if attempt < max_attempts - 1:
                        self.logger.debug(
                            f"Malformed JSON in {file_path}: {e}. Snippet: '{snippet}'. Retrying in {backoff}s..."
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
                                f"Malformed JSON in {file_path} after {max_attempts} attempts: {e}. Snippet: '{snippet}'"
                            )
                            self.last_read_error_time = now
                        with self._lock:
                            self.parse_errors += 1
                            self.last_error_time = now
                        return None
                except (OSError, ValueError, RecursionError) as e:
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
                        self.logger.error(f"Unexpected error processing {file_path}: {e}")
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
        """Process parsed data and update state if changed. Must be called with lock."""
        try:
            current_stage = data.get("current_stage")
            current_task = data.get("current_task")

            if not current_stage or not current_task:
                self.logger.error(f"Missing required fields (current_stage, current_task) in {file_path}")
                self.parse_errors += 1
                self.last_error_time = time.monotonic()
                return None

            # Extract optional fields
            project_id = data.get("project_id")
            start_time = data.get("start_time")
            status = data.get("status")
            metadata = data.get("metadata", {})

            # Validate metadata is a dict to ensure flattening works later
            if metadata is None or not isinstance(metadata, dict):
                self.logger.warning(
                    f"Metadata field in {file_path} is not a dictionary. Ignoring."
                )
                metadata = {}

            # Validate timestamp if present
            if start_time:
                try:
                    # Basic ISO 8601 check
                    datetime.fromisoformat(str(start_time).replace("Z", "+00:00"))
                except ValueError:
                    self.logger.warning(
                        f"Invalid timestamp format in start_time: {start_time}. Ignoring."
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
                self.logger.debug("State matches last state. No change.")
                return

            # Meaningful change detected
            self.logger.debug("Meaningful state change detected.")
            now = time.monotonic()
            self.last_change_time = now

            self.last_comparison_data = comparison_data
            self.last_stage = current_stage
            self.last_task = current_task
            self.is_paused = status in ("paused", "completed")

            self.current_data = {
                "current_stage": current_stage,
                "current_task": current_task,
                "project_id": project_id,
                "status": status,
                "start_time": start_time,
                "metadata": metadata,
                "file_path": str(Path(file_path).absolute()),
            }
            self.logger.debug(f"Current data updated. Keys: {len(self.current_data)}")

            return self.current_data.copy()
        except Exception as e:
            self.logger.error(f"Error processing state change: {e}")
            return None

    def _parse_file(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Legacy wrapper for backward compatibility with tests."""
        data = self._read_file_data(file_path)
        if data:
            with self._lock:
                return self._process_state_change(data, file_path)
        return None

    def _schedule_periodic_heartbeat(self) -> None:
        """Schedule the next periodic heartbeat using a timer."""
        with self._lock:
            if self._heartbeat_timer:
                self._heartbeat_timer.cancel()
                self._heartbeat_timer = None
            
            if not self._stopped and not self.is_paused and self.current_data:
                self._heartbeat_timer = threading.Timer(30.0, self._periodic_heartbeat_task)
                self._heartbeat_timer.daemon = True
                self._heartbeat_timer.start()

    def _periodic_heartbeat_task(self) -> None:
        """Execute periodic heartbeat task."""
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

        Args:
            data: The new state data.
            elapsed: Time elapsed since the last change.
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
        """Send a heartbeat with the current data."""
        self.logger.debug(f"Computed duration: {elapsed:.2f}s")

        start_hb = time.monotonic()
        try:
            self.client.send_heartbeat(
                stage=str(data.get("current_stage", "")),
                task=str(data.get("current_task", "")),
                project_id=data.get("project_id"),
                status=str(data.get("status", "in_progress")),
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
        """Check if self-healing is needed (e.g. file reappeared)."""
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
        """Legacy method for backward compatibility with tests."""
        self.check_self_healing()
        # Note: Periodic heartbeats are now handled by _heartbeat_timer

    def stop(self) -> None:
        """Stop the handler and any timers."""
        self.logger.debug("Stopping event handler...")
        # Set flag immediately to allow running tasks to exit early
        self._stopped = True
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None
            if self._heartbeat_timer:
                self._heartbeat_timer.cancel()
                self._heartbeat_timer = None

    def get_current_data(self) -> Optional[Dict[str, Any]]:
        """Return the current pipeline state data in a thread-safe manner."""
        with self._lock:
            if not self.current_data:
                return None
            return self.current_data.copy()

    def get_statistics(self) -> Dict[str, Any]:
        """Return usage statistics."""
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

        Args:
            event: The file system event to process.
        """
        self._process_event(event)

    def on_created(self, event: FileSystemEvent) -> None:
        """Handle file creation events.

        Args:
            event: The file system event to process.
        """
        self._process_event(event)

    def on_moved(self, event: FileMovedEvent) -> None:
        """Handle file move events.

        Args:
            event: The file system event to process.
        """
        if event.is_directory:
            # Check if watched directory was moved away
            try:
                src_path = Path(event.src_path).resolve()
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
            except OSError as e:
                self.logger.debug(f"Failed to resolve path in on_moved (directory): {e}")
                pass
            return

        # Check if moved AWAY from target (effectively deleted/renamed)
        try:
            src_path = Path(event.src_path).resolve()
        except (OSError, RuntimeError):
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

        # Check if moved TO target (handled by _process_event)
        self._process_event(event)

    def on_deleted(self, event: FileSystemEvent) -> None:
        """Handle file deletion events.

        Args:
            event: The file system event to process.
        """
        if event.is_directory:
            # Check if watched directory was deleted
            try:
                src_path = Path(event.src_path).resolve()
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
            except OSError as e:
                self.logger.debug(f"Failed to resolve path in on_deleted (directory): {e}")
                pass
            return

        try:
            event_path = Path(event.src_path).resolve()
        except (OSError, RuntimeError):
            event_path = Path(event.src_path).absolute()

        if event_path != self.target_file:
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

    def __repr__(self) -> str:
        return f"<PipelineEventHandler target={self.target_file}>"


class PipelineWatcher:
    """Orchestrates the file system observer."""

    def __init__(
        self,
        path: Union[str, Path],
        client: PipelineClient,
        pulsetime: float,
        debounce_seconds: float = 1.0,
    ) -> None:
        """Initialize the watcher.

        Args:
            path: The directory or file path to watch.
            client: The ActivityWatch client wrapper.
            pulsetime: The pulsetime for heartbeats.
            debounce_seconds: Time in seconds to debounce file events.
        """
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
            pulsetime=pulsetime,
            debounce_seconds=debounce_seconds,
            logger=logger,
        )

    @property
    def observer(self) -> Observer:
        """Return the underlying observer, restarting it if it died unexpectedly."""
        # Check if we need to restart
        if self._started and not self._stopping:
            if self._observer is None or not self._observer.is_alive():
                if self._observer is not None:
                    logger.error("Watchdog observer found dead.")
                
                now = time.monotonic()
                if now - self._last_observer_restart_attempt > 10.0:
                    logger.info("Attempting to restart observer...")
                    self._start_observer()
                    self._last_observer_restart_attempt = now
        
        if self._observer is None:
            raise RuntimeError("Observer is not available (down or initializing)")
        return self._observer

    def _start_observer(self) -> None:
        """Start or restart the observer."""
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
        logger.error("Could not start observer after retries.")

    def start(self) -> None:
        """Start the directory observer.

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
        """Stop the directory observer."""
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
        """Periodic task to check directory and observer health."""
        if self._stopping:
            return
        try:
            self._check_directory_health()
            self.handler.check_self_healing()
        except Exception as e:
            logger.error(f"Health check failed: {e}")

    def _check_directory_health(self) -> None:
        """Check if watch directory exists and restart observer if needed."""
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
        if self._stopping:
            return
        self._health_check_timer = threading.Timer(10.0, self._run_health_check)
        self._health_check_timer.daemon = True
        self._health_check_timer.start()

    def _run_health_check(self) -> None:
        if self._stopping:
            return
        try:
            self.check_health()
        except Exception as e:
            logger.error(f"Health check failed: {e}")
        finally:
            self._schedule_health_check()

    def check_periodic_heartbeat(self) -> None:
        """Legacy method for backward compatibility with tests."""
        self.check_health()

    def get_statistics(self) -> Dict[str, Any]:
        """Return usage statistics from the handler."""
        return self.handler.get_statistics()

    def __repr__(self) -> str:
        return f"<PipelineWatcher path={self.path} observer={'alive' if self._observer and self._observer.is_alive() else 'down'}>"