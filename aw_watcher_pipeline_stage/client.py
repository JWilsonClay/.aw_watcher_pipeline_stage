"""ActivityWatch client wrapper for the pipeline watcher."""

from __future__ import annotations

import json
import logging
import re
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

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


class PipelineClient:
    """Wrapper around ActivityWatchClient to handle pipeline-specific logic.

    Privacy & Offline Policy:
    - 100% Local Operation: No external network calls are made. All data stays local.
    - Offline-First: Uses queued=True for all ActivityWatch interactions. Events are buffered
      locally if the server is unavailable and flushed upon reconnection.
    """

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
            logger.warning(f"Failed to get hostname: {e}. Using 'unknown-host'.")
            self.hostname = "unknown-host"

        # Sanitize hostname (keep alphanumeric, hyphens, underscores, dots)
        self.hostname = re.sub(r'[^a-zA-Z0-9\-_.]', '_', self.hostname)
        if not self.hostname or set(self.hostname) == {"_"}:
            self.hostname = "unknown-host"

        self.bucket_id = f"aw-watcher-pipeline-stage_{self.hostname}"
        self.last_connection_error_log_time = 0.0

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

    def wait_for_start(self, timeout: Optional[float] = None) -> None:
        """Wait for ActivityWatch server to start.

        Args:
            timeout: Maximum time to wait in seconds. If None, wait indefinitely.
        """
        retry_delay = 1.0
        start_time = time.monotonic()
        while True:
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
                
                time.sleep(sleep_time)
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
        """Send a heartbeat to the server.

        Handles connection errors with local buffering (queued=True) and logging.
        Retries transient errors with exponential backoff.

        Security & Validation (Stage 4.2.2):
        - Validates types for stage, task, project_id, file_path.
        - Enforces length limits on strings.
        - Validates status enum.
        - Validates start_time ISO format.
        - Flattens and truncates metadata to 1KB limit.
        """
        # Validation: Types
        if not isinstance(stage, str) or stage is None:
            msg = f"Invalid stage type: {type(stage)}. Skipping heartbeat."
            logger.error(msg)
            raise ValueError(msg)
        if len(stage) > 256:
            logger.warning(f"Stage name too long ({len(stage)} chars). Truncating to 256.")
            stage = stage[:256]

        if not isinstance(task, str) or task is None:
            msg = f"Invalid task type: {type(task)}. Skipping heartbeat."
            logger.error(msg)
            raise ValueError(msg)
        if len(task) > 512:
            logger.warning(f"Task name too long ({len(task)} chars). Truncating to 512.")
            task = task[:512]

        if project_id is not None:
            if not isinstance(project_id, str):
                logger.warning(f"Invalid project_id type: {type(project_id)}. Dropping.")
                project_id = None
            elif len(project_id) > 256:
                logger.warning(f"project_id too long ({len(project_id)}). Truncating.")
                project_id = project_id[:256]

        if file_path is not None:
            if not isinstance(file_path, str):
                logger.warning(f"Invalid file_path type: {type(file_path)}. Dropping.")
                file_path = None
            elif len(file_path) > 4096:
                logger.warning(f"file_path too long ({len(file_path)}). Truncating.")
                file_path = file_path[:4096]

            # Privacy: Anonymize home directory
            if file_path:
                try:
                    home = str(Path.home())
                    if home and home != "/" and file_path.startswith(home):
                        file_path = file_path.replace(home, "~", 1)
                except Exception:
                    pass

        if computed_duration is not None and not isinstance(computed_duration, (int, float)):
            logger.warning(f"Invalid computed_duration type: {type(computed_duration)}. Dropping.")
            computed_duration = None

        # Validation: Status Enum
        valid_statuses = {"in_progress", "paused", "completed"}
        if status is not None and (not isinstance(status, str) or status not in valid_statuses):
            logger.warning(f"Invalid status '{status}'. Expected one of {valid_statuses}. Skipping status field.")
            status = None

        data: Dict[str, Any] = {
            "stage": stage,
            "task": task,
        }
        if status:
            data["status"] = status
        if project_id:
            data["project_id"] = project_id
        if file_path:
            data["file_path"] = file_path
        if computed_duration is not None:
            if computed_duration < 0:
                logger.warning(
                    f"Computed duration is negative ({computed_duration}s). System clock may have moved backwards."
                )
            elif computed_duration >= 0:
                if computed_duration > 86400:  # 24 hours
                    logger.warning(
                        f"Computed duration is very large ({computed_duration}s). Possible system clock jump."
                    )
                data["computed_duration"] = computed_duration

        timestamp = datetime.now(timezone.utc)
        if start_time:
            if not isinstance(start_time, str):
                logger.warning(f"Invalid start_time type: {type(start_time)}. Expected ISO string. Dropping.")
            else:
                try:
                    # Validate format and use as event timestamp
                    ts_str = start_time.replace("Z", "+00:00")
                    parsed_ts = datetime.fromisoformat(ts_str)
                    if parsed_ts.tzinfo is None:
                        parsed_ts = parsed_ts.replace(tzinfo=timezone.utc)
                    timestamp = parsed_ts
                    data["start_time"] = start_time
                except (ValueError, AttributeError, TypeError):
                    logger.warning(f"Invalid start_time format: {start_time}. Dropping.")

        # Flatten metadata
        if metadata:
            if not isinstance(metadata, dict) or metadata is None:
                logger.warning(f"Metadata is not a dictionary ({type(metadata).__name__}). Ignoring.")
                metadata = {}
            else:
                # Security: Check size limit (1KB) and filter non-serializable values
                safe_metadata: Dict[str, Any] = {}
                current_size = 2  # {} overhead

                for k, v in metadata.items():
                    # Security: Filter keys based on allowlist if configured
                    if self.metadata_allowlist is not None:
                        if k not in self.metadata_allowlist:
                            continue

                    try:
                        key_str = str(k)
                        val_json = json.dumps(v)
                        # Key is string, value is dumped. Conservative overhead for JSON structure (quotes, colon, comma)
                        item_size = len(key_str) + len(val_json) + 6

                        if current_size + item_size > 1024:
                            logger.warning(
                                f"Metadata exceeds 1KB limit (current={current_size}, item={item_size}). "
                                "Truncating remaining keys."
                            )
                            break

                        # We store the original value, but we've verified it's serializable and fits
                        safe_metadata[key_str] = v
                        current_size += item_size
                    except (TypeError, ValueError):
                        logger.warning(f"Metadata value for key '{k}' is not JSON serializable. Skipping.")
                        continue

                metadata = safe_metadata

                for k, v in metadata.items():
                    if k in data:
                        logger.debug(f"Metadata key '{k}' conflicts with core field. Ignoring.")
                        continue
                    data[k] = v

        event = Event(timestamp=timestamp, data=data)

        # Retry logic
        max_retries = 3
        backoff = 0.5

        for attempt in range(max_retries + 1):
            try:
                self.client.heartbeat(
                    self.bucket_id,
                    event,
                    pulsetime=self.pulsetime,  # Verified for Directive 3: Reduces unnecessary sends via server-side merging
                    queued=True
                )
                if attempt > 0:
                    logger.info(f"Connection recovered after {attempt} retries.")

                if self.testing and isinstance(self.client, MockActivityWatchClient):
                    logger.info(f"[MOCK] heartbeat: {stage} - {task}")
                return
            except (TypeError, OverflowError, ValueError) as e:
                logger.error(f"Failed to serialize heartbeat event (check metadata types): {e}")
                return  # Fatal error for this event, do not retry
            except Exception as e:
                is_connection_error = (
                    isinstance(e, (ConnectionError, socket.error))
                    or "Connection" in str(e)
                    or "refused" in str(e)
                )

                if attempt < max_retries:
                    log_msg = f"Heartbeat failed ({type(e).__name__}: {e}). Retrying in {backoff}s..."
                    if is_connection_error:
                        now = time.monotonic()
                        if now - self.last_connection_error_log_time > 60.0:
                            logger.warning(f"Server unavailable (buffering enabled). {log_msg}")
                            self.last_connection_error_log_time = now
                        else:
                            logger.debug(f"Server unavailable (buffering enabled). {log_msg}")
                    else:
                        logger.warning(log_msg)
                    time.sleep(backoff)
                    backoff *= 2
                else:
                    logger.error(f"Heartbeat failed after {max_retries} retries: {e}")
                    raise

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
            logger.error(f"Failed to flush event queue: {e}")

    def close(self) -> None:
        """Close the client connection."""
        try:
            if hasattr(self.client, "disconnect"):
                self.client.disconnect()
                logger.info("Client closed.")
        except Exception as e:
            logger.error(f"Error closing client: {e}")

    def __repr__(self) -> str:
        return f"<PipelineClient bucket={self.bucket_id} host={self.hostname}>"
