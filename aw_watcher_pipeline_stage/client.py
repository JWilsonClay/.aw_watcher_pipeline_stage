"""ActivityWatch client wrapper for the pipeline watcher.

This module encapsulates all communication with the ActivityWatch server (aw-server).
It handles bucket creation, heartbeat transmission, and connection management.
It implements a non-blocking, offline-first design using a background worker thread
and local queuing.

Key Responsibilities:
  * **Bucket Creation**: Ensuring the bucket exists with the correct type and hostname.
  * **Queued Heartbeats**: Buffering events locally when the server is offline.
  * **Offline Resilience**: Retrying connections and managing the event queue.

Design:
  * **Offline Resilience**: All operations use `queued=True` where possible. Heartbeats
    are buffered locally if the server is unreachable.
  * **Non-blocking**: The `send_heartbeat` method returns immediately, offloading
    serialization and network I/O to a background worker thread.
  * **Privacy**: Enforces local-only operation (no telemetry) and sanitizes payloads
    (e.g., anonymizing home directory paths).

Invariants:
  * **Pulsetime**: Defaults to 120s to allow server-side merging of events.
  * **No Telemetry**: No external network calls are made; only localhost communication.
  * **Thread Safety**: The client is thread-safe for sending heartbeats.
"""

from __future__ import annotations

import json
import logging
import queue
import re
import socket
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

try:
    from aw_core.models import Event
except ImportError:
    # Fallback for when aw-core is not installed
    class Event:  # type: ignore
        """Fallback Event class when aw-core is not available."""

        def __init__(self, timestamp: Any = None, data: Optional[Dict[str, Any]] = None) -> None:
            """Initialize the fallback Event.

            Args:
                timestamp: The event timestamp.
                data: The event data payload.
            """
            self.timestamp = timestamp
            self.data = data or {}

try:
    from aw_client import ActivityWatchClient
except ImportError:
    ActivityWatchClient = None  # type: ignore

try:
    import orjson
except ImportError:
    orjson = None  # type: ignore

if orjson:
    json_dumps = orjson.dumps
else:
    json_dumps = json.dumps

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class MockActivityWatchClient:
    """Mock the ActivityWatch client for testing without a running AW server.

    Simulate the behavior of the official ActivityWatchClient, storing buckets
    and events in memory for verification.

    Attributes:
        buckets (Dict[str, Any]): Dictionary of created buckets.
        events (List[Dict[str, Any]]): List of sent events.
        client_hostname (str): Hostname used for bucket creation.
        metadata_allowlist (Optional[Set[str]]): Set of allowed metadata keys.
    """
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the mock client.

        Args:
            *args: Variable length argument list passed to parent.
            **kwargs: Arbitrary keyword arguments passed to parent.
        """
        self.buckets: Dict[str, Any] = {}
        self.events: List[Dict[str, Any]] = []
        self.client_hostname = "test-host"
        self.metadata_allowlist: Optional[Set[str]] = None

    def connect(self) -> None:
        """Establish mock connection.

        Simulates connecting to the ActivityWatch server.

        Returns:
            None
        """
        pass

    def disconnect(self) -> None:
        """Disconnect mock client.

        Simulates disconnecting from the ActivityWatch server.

        Returns:
            None
        """
        pass

    def create_bucket(self, bucket_id: str, event_type: str, queued: bool = False) -> None:
        """Create mock bucket.

        Args:
            bucket_id (str): The bucket identifier.
            event_type (str): The event type.
            queued (bool): Whether to queue the creation if offline.

        Returns:
            None
        """
        self.buckets[bucket_id] = {"event_type": event_type, "queued": queued}

    def heartbeat(self, bucket_id: str, event: Event, pulsetime: float = 0.0, queued: bool = False) -> None:
        """Send mock heartbeat.

        Args:
            bucket_id (str): The bucket identifier.
            event (Event): The event to send.
            pulsetime (float): The pulsetime for the heartbeat.
            queued (bool): Whether to queue the heartbeat if offline.

        Returns:
            None
        """
        self.events.append({
            "bucket_id": bucket_id,
            "data": event.data,
            "timestamp": event.timestamp,
            "pulsetime": pulsetime,
            "queued": queued
        })

    def get_info(self) -> Dict[str, Any]:
        """Get mock server info.

        Returns:
            Mock server info dictionary.
        """
        return {"version": "mock"}

    def flush(self) -> None:
        """Flush mock queue.

        Simulates flushing the event queue.

        Returns:
            None
        """
        logger.info("[MOCK] flush: Queue flushed")


VALID_STATUSES = {"in_progress", "paused", "completed"}

class PipelineClient:
    """Wrap ActivityWatchClient to handle pipeline-specific logic.

    Manage the connection to the ActivityWatch server, ensuring robust handling
    of network issues and strict adherence to privacy requirements. Provide
    a simplified interface for sending pipeline stage events.

    This client operates in a non-blocking manner by offloading network I/O and
    serialization to a background worker thread. This ensures that the main
    application loop is never blocked by ActivityWatch latency or connection timeouts.

    Attributes:
        watch_path (Path): The path being watched (used for context/logging).
        port (Optional[int]): The port of the ActivityWatch server (default: 5600).
        testing (bool): Whether to use a mock client for testing.
        pulsetime (float): The heartbeat merge window in seconds (default: 120.0).
        metadata_allowlist (Optional[Set[str]]): Set of allowed metadata keys.
        hostname (str): The sanitized hostname used for bucket naming.
        bucket_id (str): The unique bucket identifier (aw-watcher-pipeline-stage_{hostname}).
        client (Any): The underlying ActivityWatchClient instance.
        last_connection_error_log_time (float): Timestamp of the last connection error log.
    """

    __slots__ = (
        'watch_path', 'port', 'testing', 'pulsetime', 'metadata_allowlist',
        'hostname', 'bucket_id', 'last_connection_error_log_time', 'client',
        '_home_path', '_closed', '_queue', '_worker_thread'
    )

    def __init__(
        self,
        watch_path: Path,
        port: Optional[int] = None,
        testing: bool = False,
        pulsetime: float = 120.0,
        client: Optional[Any] = None,
        metadata_allowlist: Optional[List[str]] = None,
    ) -> None:
        """Initialize the PipelineClient.

        Args:
            watch_path (Path): The path to the file being watched.
            port (Optional[int]): The ActivityWatch server port. Defaults to 5600 via aw-client.
            testing (bool): If True, uses a mock client instead of connecting to a real server.
            pulsetime (float): Time in seconds to merge consecutive heartbeats.
            client (Optional[Any]): Injected client instance (for testing/dependency injection).
            metadata_allowlist (Optional[List[str]]): List of allowed metadata keys to include in payloads.

        Raises:
            ValueError: If port is invalid or hostname cannot be determined.
            ImportError: If aw-client is not installed and testing is False.
        """
        self.watch_path = watch_path
        if port is not None:
            try:
                self.port = int(port)
            except (ValueError, TypeError):
                raise ValueError(f"Port must be an integer, got {port}")
        else:
            self.port = None
        self.testing = testing
        self.pulsetime = pulsetime
        self.metadata_allowlist: Optional[Set[str]] = (
            set(metadata_allowlist) if metadata_allowlist is not None else None
        )
        try:
            self.hostname = socket.gethostname()
            if not self.hostname:
                raise ValueError("Empty hostname")
        except Exception as e:
            logger.warning(f"Failed to get hostname: {e}. Using 'unknown-host'.")
            self.hostname = "unknown-host"

        # Sanitize hostname (keep alphanumeric, hyphens, underscores, dots)
        self.hostname = re.sub(r'[^a-zA-Z0-9\-_.]', '_', self.hostname)
        if not self.hostname or set(self.hostname) == {"_"}:
            self.hostname = "unknown-host"

        self.bucket_id = f"aw-watcher-pipeline-stage_{self.hostname}"
        self.last_connection_error_log_time = 0.0
        self._closed = False
        
        # Use a dedicated worker thread and queue for non-blocking heartbeats.
        # This avoids the overhead of ThreadPoolExecutor futures for fire-and-forget tasks.
        self._queue: queue.Queue[Any] = queue.Queue()
        self._worker_thread = threading.Thread(
            target=self._worker_loop, 
            name="PipelineClientWorker", 
            daemon=True
        )
        self._worker_thread.start()

        # Cache home path for performance in send_heartbeat
        try:
            self._home_path: Optional[str] = str(Path.home())
            if self._home_path == "/":
                self._home_path = None
        except Exception:
            self._home_path = None

        logger.info(
            f"PipelineClient initialized. Bucket: {self.bucket_id} (Privacy: Local-only, Offline-first)"
        )

        self.client: Any

        if client:
            self.client = client
        elif testing:
            self.client = MockActivityWatchClient()
        else:
            if ActivityWatchClient is None:
                raise ImportError("aw-client is not installed. Please install it to run in production mode.")
            try:
                self.client = ActivityWatchClient("aw-watcher-pipeline-stage", port=port, testing=testing)
            except Exception as e:
                logger.error(f"Failed to initialize ActivityWatchClient: {e}")
                raise

    def wait_for_start(self, timeout: Optional[float] = None, stop_check: Optional[Callable[[], bool]] = None) -> None:
        """Wait for the ActivityWatch server to start.

        Args:
            timeout (Optional[float]): Maximum time to wait in seconds. If None, wait indefinitely.
            stop_check (Optional[Callable[[], bool]]): Optional callable returning True if waiting
                should be aborted (e.g. shutdown).

        Returns:
            None
        """
        retry_delay = 1.0
        start_time = time.monotonic()
        while True:
            if stop_check and stop_check():
                logger.info("Wait for start aborted by stop signal.")
                break

            try:
                self.client.get_info()
                logger.info("Connected to ActivityWatch server.")
                break
            except Exception as e:
                elapsed = time.monotonic() - start_time
                if timeout is not None and elapsed >= timeout:
                    logger.warning(f"Could not connect to ActivityWatch server after {timeout}s. Proceeding in offline mode (queued).")
                    break

                logger.warning(f"Could not connect to ActivityWatch server: {e}. Retrying in {retry_delay}s...")
                
                sleep_time = retry_delay
                if timeout is not None:
                    remaining = timeout - elapsed
                    if remaining > 0:
                        sleep_time = min(retry_delay, remaining)
                
                # Sleep in small chunks to remain responsive to stop_check
                chunk = 0.1
                slept = 0.0
                while slept < sleep_time:
                    if stop_check and stop_check():
                        return
                    time.sleep(min(chunk, sleep_time - slept))
                    slept += chunk

                retry_delay = min(retry_delay * 2, 30.0)

    def ensure_bucket(self) -> None:
        """Create the bucket if it does not exist.

        Uses `queued=True` to ensure bucket creation happens eventually even if
        the server is currently offline. The bucket ID is constructed from the
        sanitized hostname to ensure uniqueness.

        Returns:
            None

        Raises:
            Exception: If bucket creation fails immediately (e.g. client configuration error).
                While connection errors are typically handled by the `queued=True` mechanism,
                fatal errors from the underlying client (e.g. `ActivityWatchClientError`) will be re-raised.
        """
        try:
            self.client.create_bucket(
                self.bucket_id,
                event_type="current-pipeline-stage",
                queued=True
            )
            logger.info(f"Bucket '{self.bucket_id}' ensured (queued).")
        except Exception as e:
            logger.error(f"Failed to create bucket: {e}")
            raise

    def send_heartbeat(
        self,
        stage: str,
        task: str,
        project_id: Optional[str] = None,
        status: Optional[str] = "in_progress",
        start_time: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        file_path: Optional[str] = None,
        computed_duration: Optional[float] = 0.0,
    ) -> None:
        """Send a heartbeat to the server (non-blocking).

        This method enqueues the heartbeat data to a background worker thread,
        ensuring the main thread is never blocked by network I/O or serialization.

        The worker thread constructs an ActivityWatch `Event` object with the provided
        data and sends it to the configured bucket. The arguments provided here
        are mapped to the `data` dictionary of the Event.

        Note:
            This method is non-blocking and does not raise exceptions to the caller.
            Connection errors or serialization failures are logged by the worker thread.
            Unlike the raw client, this wrapper suppresses `ActivityWatchClientError`
            to prevent crashing the watcher loop, relying on offline queuing instead.

        Args:
            stage (str): The current pipeline stage name.
            task (str): The current task description.
            project_id (Optional[str]): Identifier for the project.
            status (Optional[str]): Current status ('in_progress', 'paused', 'completed').
            start_time (Optional[str]): ISO 8601 timestamp string for when the task started.
            metadata (Optional[Dict[str, Any]]): Additional key-value metadata.
                These keys are flattened into the event data. Keys may be filtered
                if a metadata allowlist is configured. Metadata is truncated if it
                exceeds 1KB to prevent payload bloat.
            file_path (Optional[str]): Path to the file associated with the event.
                Paths within the user's home directory are anonymized.
            computed_duration (Optional[float]): Duration in seconds since the last update.

        Returns:
            None

        Raises:
            None: This method is non-blocking. Exceptions (including `ActivityWatchClientError`)
                are caught and logged by the worker thread to prevent crashing the main application loop.

        Example:
            >>> client.send_heartbeat(
            ...     stage="Build",
            ...     task="Compiling",
            ...     project_id="my-project",
            ...     status="in_progress",
            ...     metadata={"priority": "high"}
            ... )
            # Resulting Event payload (sent asynchronously):
            # {
            #     "timestamp": "...",
            #     "data": {
            #         "stage": "Build",
            #         "task": "Compiling",
            #         "project_id": "my-project",
            #         "status": "in_progress",
            #         "priority": "high"
            #     }
            # }
        """
        if self._closed:
            return

        # Capture timestamp immediately to avoid queue latency affecting event time
        # if start_time is not provided.
        current_time = datetime.now(timezone.utc)

        # Shallow copy metadata to prevent race conditions if caller modifies it later
        if metadata:
            metadata = metadata.copy()

        # Offload to queue to ensure strictly non-blocking behavior for the caller
        # Using a tuple is slightly faster than a dict or object
        self._queue.put(
            (stage, task, project_id, status, start_time, metadata, file_path, computed_duration, current_time)
        )

    def _worker_loop(self) -> None:
        """Run the worker thread loop to process heartbeats from the queue.

        Returns:
            None
        """
        while True:
            try:
                item = self._queue.get()
                if item is None:  # Sentinel for shutdown
                    break
                
                self._send_heartbeat_sync(*item)
            except Exception as e:
                logger.error(f"Error in heartbeat worker: {e}", exc_info=True)
            finally:
                self._queue.task_done()

    def _send_heartbeat_sync(
        self,
        stage: str,
        task: str,
        project_id: Optional[str],
        status: Optional[str],
        start_time: Optional[str],
        metadata: Optional[Dict[str, Any]],
        file_path: Optional[str],
        computed_duration: Optional[float],
        current_time: datetime,
    ) -> None:
        """Send a heartbeat synchronously (runs in the worker thread).

        Performs the following actions:
        1. Validates input types (logging errors for invalid types).
        2. Truncates strings that exceed length limits.
        3. Sanitizes file paths (anonymizing home directory).
        4. Flattens and filters metadata (enforcing size limits and allowlists).
        5. Constructs the Event object.
        6. Sends the heartbeat to ActivityWatch (with offline queuing).

        Args:
            stage (str): The current pipeline stage name.
            task (str): The current task description.
            project_id (Optional[str]): Identifier for the project.
            status (Optional[str]): Current status.
            start_time (Optional[str]): ISO 8601 timestamp string.
            metadata (Optional[Dict[str, Any]]): Additional metadata.
            file_path (Optional[str]): Path to the file.
            computed_duration (Optional[float]): Duration in seconds.
            current_time (datetime): Timestamp captured when heartbeat was enqueued.
        """
        # Validation: Types (Fail fast)
        if not isinstance(stage, str):
            logger.error(f"Invalid stage type: {type(stage)}. Skipping heartbeat.")
            return
        if not isinstance(task, str):
            logger.error(f"Invalid task type: {type(task)}. Skipping heartbeat.")
            return

        # Truncation
        if len(stage) > 256:
            logger.warning(f"Stage name too long ({len(stage)} chars). Truncating to 256.")
            stage = stage[:256]
        if len(task) > 512:
            logger.warning(f"Task name too long ({len(task)} chars). Truncating to 512.")
            task = task[:512]

        # Status handling
        final_status = "in_progress"
        if status:
            if status in VALID_STATUSES:
                final_status = status
            else:
                logger.warning(f"Invalid status '{status}'. Expected one of {VALID_STATUSES}.")

        # Pre-format dict with core fields
        data: Dict[str, Any] = {
            "stage": stage,
            "task": task,
            "status": final_status
        }

        # Optional fields
        if project_id:
            if not isinstance(project_id, str):
                logger.warning(f"Invalid project_id type: {type(project_id)}. Dropping.")
            elif len(project_id) > 256:
                logger.warning(f"project_id too long ({len(project_id)}). Truncating.")
                data["project_id"] = project_id[:256]
            else:
                data["project_id"] = project_id

        if file_path:
            if not isinstance(file_path, str):
                logger.warning(f"Invalid file_path type: {type(file_path)}. Dropping.")
            else:
                if len(file_path) > 4096:
                    logger.warning(f"file_path too long ({len(file_path)}). Truncating.")
                    file_path = file_path[:4096]

                # Privacy: Anonymize home directory
                if self._home_path and file_path.startswith(self._home_path):
                    data["file_path"] = file_path.replace(self._home_path, "~", 1)
                else:
                    data["file_path"] = file_path

        if computed_duration is not None:
            if not isinstance(computed_duration, (int, float)):
                logger.warning(f"Invalid computed_duration type: {type(computed_duration)}. Dropping.")
            elif computed_duration < 0:
                logger.warning(f"Computed duration is negative ({computed_duration}s).")
            elif computed_duration > 86400:
                logger.warning(f"Computed duration is very large ({computed_duration}s).")
            else:
                data["computed_duration"] = computed_duration

        timestamp = current_time
        if start_time:
            if not isinstance(start_time, str):
                logger.warning(f"Invalid start_time type: {type(start_time)}. Dropping.")
            else:
                try:
                    # Validate format and use as event timestamp
                    # Optimized replacement for Z
                    ts_str = start_time[:-1] + "+00:00" if start_time.endswith("Z") else start_time
                    parsed_ts = datetime.fromisoformat(ts_str)
                    if parsed_ts.tzinfo is None:
                        parsed_ts = parsed_ts.replace(tzinfo=timezone.utc)
                    timestamp = parsed_ts
                    data["start_time"] = start_time
                except (ValueError, AttributeError, TypeError):
                    logger.warning(f"Invalid start_time format: {start_time}. Dropping.")

        # Flatten metadata (Optimized)
        if metadata:
            if not isinstance(metadata, dict):
                logger.warning(f"Metadata is not a dictionary ({type(metadata).__name__}). Ignoring.")
            else:
                # Security: Filter keys based on allowlist if configured
                if self.metadata_allowlist is not None:
                    metadata = {k: v for k, v in metadata.items() if k in self.metadata_allowlist}

                # Fast Path: Try serializing the whole dict first
                try:
                    serialized = json_dumps(metadata)
                    if len(serialized) > 1024:
                        raise ValueError("Metadata too large")
                    # If successful, merge into data. Core fields will overwrite metadata keys.
                    metadata.update(data)
                    data = metadata
                except (TypeError, ValueError, OverflowError):
                    # Slow Path: Iterative check and filter for large or non-serializable metadata
                    logger.debug("Metadata fast-path failed, using slow-path for sanitization.")
                    safe_metadata: Dict[str, Any] = {}
                    current_size = 2  # {} overhead

                    for k, v in metadata.items():
                        try:
                            key_str = str(k)
                            val_json = json_dumps(v)
                            item_size = len(key_str) + len(val_json) + 6

                            if current_size + item_size > 1024:
                                logger.warning(
                                    "Metadata exceeds 1KB limit. Truncating remaining keys."
                                )
                                break

                            safe_metadata[key_str] = v
                            current_size += item_size
                        except (TypeError, ValueError):
                            logger.warning(f"Metadata value for key '{k}' is not JSON serializable. Skipping.")
                            continue
                    
                    # Merge the sanitized metadata. Core fields will overwrite metadata keys.
                    safe_metadata.update(data)
                    data = safe_metadata

        event = Event(timestamp=timestamp, data=data)

        # The original retry logic with time.sleep is removed to make this method
        # non-blocking, fully relying on aw-client's queuing mechanism.
        try:
            self.client.heartbeat(
                self.bucket_id,
                event,
                pulsetime=self.pulsetime,
                queued=True  # Ensures non-blocking and offline buffering
            )
            if self.testing and isinstance(self.client, MockActivityWatchClient):
                logger.info(f"[MOCK] heartbeat: {stage} - {task}")
        except (TypeError, OverflowError, ValueError) as e:
            # These are fatal serialization errors for this event, do not retry.
            logger.error(f"Failed to serialize heartbeat event (check metadata types): {e}")
            # We return instead of re-raising, as the watcher's loop should not crash
            # on a single bad event.
            return
        except Exception as e:
            # All other exceptions from aw-client are considered unexpected,
            # as queuing should prevent connection errors.
            logger.error(f"Failed to send heartbeat: {e}", exc_info=True)

    def flush_queue(self) -> None:
        """Flush the event queue.

        This is useful for ensuring all queued events are sent before shutdown.
        It delegates to the underlying client's flush method if available.

        Returns:
            None
        """
        try:
            if hasattr(self.client, "flush"):
                logger.info("Flushing event queue...")
                self.client.flush()
                logger.info("Queue flushed successfully.")
            elif hasattr(self.client, "disconnect"):
                logger.info("Client has no flush method, skipping explicit flush.")
            else:
                logger.warning("Client has no flush or disconnect method.")
        except Exception as e:
            logger.error(f"Failed to flush event queue: {e}")

    def close(self) -> None:
        """Close the client connection and stop the worker thread.

        Signals the worker thread to exit, waits for it to join, and closes
        the underlying ActivityWatch client connection.

        Returns:
            None
        """
        if self._closed:
            return
        self._closed = True
        
        # Signal worker to stop
        self._queue.put(None)
        
        # Wait for worker to finish (with timeout to avoid hanging)
        if self._worker_thread.is_alive():
            self._worker_thread.join(timeout=1.0)
        
        try:
            if hasattr(self.client, "disconnect"):
                self.client.disconnect()
                logger.info("Client closed.")
        except Exception as e:
            logger.error(f"Error closing client: {e}")

    def __enter__(self) -> PipelineClient:
        """Enter the context manager.

        Establishes connection if the underlying client supports it.

        Returns:
            PipelineClient: The client instance.
        """
        if hasattr(self.client, "connect"):
            self.client.connect()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Exit the context manager.

        Closes the client connection and stops the worker thread.

        Args:
            exc_type (Any): The exception type if an exception was raised, else None.
            exc_value (Any): The exception value, else None.
            traceback (Any): The traceback, else None.

        Returns:
            None
        """
        self.close()

    def __repr__(self) -> str:
        """Return a string representation of the client.

        Returns:
            str: String representation including bucket ID and hostname.
        """
        return f"<PipelineClient bucket={self.bucket_id} host={self.hostname}>"
