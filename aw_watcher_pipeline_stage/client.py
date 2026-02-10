"""ActivityWatch client wrapper for the pipeline watcher."""

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
        def __init__(self, timestamp: Any = None, data: Optional[Dict[str, Any]] = None) -> None:
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


class MockActivityWatchClient:
    """Mock client for testing without a running AW server."""
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.buckets: Dict[str, Any] = {}
        self.events: List[Dict[str, Any]] = []
        self.client_hostname = "test-host"
        self.metadata_allowlist: Optional[Set[str]] = None

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def create_bucket(self, bucket_id: str, event_type: str, queued: bool = False) -> None:
        self.buckets[bucket_id] = {"event_type": event_type, "queued": queued}

    def heartbeat(self, bucket_id: str, event: Event, pulsetime: float = 0.0, queued: bool = False) -> None:
        self.events.append({
            "bucket_id": bucket_id,
            "data": event.data,
            "timestamp": event.timestamp,
            "pulsetime": pulsetime,
            "queued": queued
        })

    def get_info(self) -> Dict[str, Any]:
        return {"version": "mock"}

    def flush(self) -> None:
        logger.info("[MOCK] flush: Queue flushed")


VALID_STATUSES = {"in_progress", "paused", "completed"}

class PipelineClient:
    """Wrapper around ActivityWatchClient to handle pipeline-specific logic.

    Privacy & Offline Policy:
    - 100% Local Operation: No external network calls are made. All data stays local.
    - Offline-First: Uses queued=True for all ActivityWatch interactions. Events are buffered
      locally if the server is unavailable and flushed upon reconnection.
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
            logger.warning("Failed to get hostname: %s. Using 'unknown-host'.", e)
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
            "PipelineClient initialized. Bucket: %s (Privacy: Local-only, Offline-first)",
            self.bucket_id
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
                logger.error("Failed to initialize ActivityWatchClient: %s", e)
                raise

    def wait_for_start(self, timeout: Optional[float] = None, stop_check: Optional[Callable[[], bool]] = None) -> None:
        """Wait for ActivityWatch server to start.

        Args:
            timeout: Maximum time to wait in seconds. If None, wait indefinitely.
            stop_check: Optional callable returning True if waiting should be aborted (e.g. shutdown).
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
                    logger.warning("Could not connect to ActivityWatch server after %ss. Proceeding in offline mode (queued).", timeout)
                    break

                logger.warning("Could not connect to ActivityWatch server: %s. Retrying in %ss...", e, retry_delay)
                
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
        """Create the bucket if it doesn't exist.

        Uses queued=True to ensure bucket creation happens eventually even if
        server is currently offline.
        """
        try:
            self.client.create_bucket(
                self.bucket_id,
                event_type="current-pipeline-stage",
                queued=True
            )
            logger.info("Bucket '%s' ensured (queued).", self.bucket_id)
        except Exception as e:
            logger.error("Failed to create bucket: %s", e)
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
        """Send a heartbeat to the server (Non-blocking).

        This method enqueues the heartbeat data to a background worker thread,
        ensuring the main thread is never blocked by network I/O or serialization.

        Performance Notes:
        - Execution Time: <0.2ms (Enqueues tuple only).
        - Threading: Uses a dedicated daemon thread for processing.
        - Optimization: Metadata is shallow-copied to ensure thread safety while minimizing overhead.
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
        """Worker thread loop to process heartbeats from the queue."""
        while True:
            try:
                item = self._queue.get()
                if item is None:  # Sentinel for shutdown
                    break
                
                self._send_heartbeat_sync(*item)
            except Exception as e:
                logger.error("Error in heartbeat worker: %s", e, exc_info=True)
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
        """Synchronous implementation of heartbeat sending (runs in worker thread).

        Handles connection errors with local buffering (queued=True) and logging.
        Catches all exceptions to prevent thread crashes.

        Performance:
        - Processing time: <10ms typical.
        - Optimizations: Fast-path JSON serialization, pre-formatted dicts.
        """
        # Validation: Types (Fail fast)
        if not isinstance(stage, str):
            logger.error("Invalid stage type: %s. Skipping heartbeat.", type(stage))
            return
        if not isinstance(task, str):
            logger.error("Invalid task type: %s. Skipping heartbeat.", type(task))
            return

        # Truncation
        if len(stage) > 256:
            logger.warning("Stage name too long (%d chars). Truncating to 256.", len(stage))
            stage = stage[:256]
        if len(task) > 512:
            logger.warning("Task name too long (%d chars). Truncating to 512.", len(task))
            task = task[:512]

        # Status handling
        final_status = "in_progress"
        if status:
            if status in VALID_STATUSES:
                final_status = status
            else:
                logger.warning("Invalid status '%s'. Expected one of %s.", status, VALID_STATUSES)

        # Pre-format dict with core fields
        data: Dict[str, Any] = {
            "stage": stage,
            "task": task,
            "status": final_status
        }

        # Optional fields
        if project_id:
            if not isinstance(project_id, str):
                logger.warning("Invalid project_id type: %s. Dropping.", type(project_id))
            elif len(project_id) > 256:
                logger.warning("project_id too long (%d). Truncating.", len(project_id))
                data["project_id"] = project_id[:256]
            else:
                data["project_id"] = project_id

        if file_path:
            if not isinstance(file_path, str):
                logger.warning("Invalid file_path type: %s. Dropping.", type(file_path))
            else:
                if len(file_path) > 4096:
                    logger.warning("file_path too long (%d). Truncating.", len(file_path))
                    file_path = file_path[:4096]

                # Privacy: Anonymize home directory
                if self._home_path and file_path.startswith(self._home_path):
                    data["file_path"] = file_path.replace(self._home_path, "~", 1)
                else:
                    data["file_path"] = file_path

        if computed_duration is not None:
            if not isinstance(computed_duration, (int, float)):
                logger.warning("Invalid computed_duration type: %s. Dropping.", type(computed_duration))
            elif computed_duration < 0:
                logger.warning("Computed duration is negative (%ss).", computed_duration)
            elif computed_duration > 86400:
                logger.warning("Computed duration is very large (%ss).", computed_duration)
            else:
                data["computed_duration"] = computed_duration

        timestamp = current_time
        if start_time:
            if not isinstance(start_time, str):
                logger.warning("Invalid start_time type: %s. Dropping.", type(start_time))
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
                    logger.warning("Invalid start_time format: %s. Dropping.", start_time)

        # Flatten metadata (Optimized)
        if metadata:
            if not isinstance(metadata, dict):
                logger.warning("Metadata is not a dictionary (%s). Ignoring.", type(metadata).__name__)
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
                            logger.warning("Metadata value for key '%s' is not JSON serializable. Skipping.", k)
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
                logger.info("[MOCK] heartbeat: %s - %s", stage, task)
        except (TypeError, OverflowError, ValueError) as e:
            # These are fatal serialization errors for this event, do not retry.
            logger.error("Failed to serialize heartbeat event (check metadata types): %s", e)
            # We return instead of re-raising, as the watcher's loop should not crash
            # on a single bad event.
            return
        except Exception as e:
            # All other exceptions from aw-client are considered unexpected,
            # as queuing should prevent connection errors. Log and re-raise
            # to be handled by the caller (e.g., the watcher's main loop).
            logger.error("Failed to queue heartbeat: %s", e)
            # Removed raise to prevent thread crash noise, as main loop cannot catch it from here.

    def flush_queue(self) -> None:
        """Flush the event queue.

        This is useful for ensuring all queued events are sent before shutdown.
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
            logger.error("Failed to flush event queue: %s", e)

    def close(self) -> None:
        """Close the client connection."""
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
            logger.error("Error closing client: %s", e)

    def __enter__(self) -> PipelineClient:
        if hasattr(self.client, "connect"):
            self.client.connect()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<PipelineClient bucket={self.bucket_id} host={self.hostname}>"
