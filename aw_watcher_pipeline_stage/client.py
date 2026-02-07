"""ActivityWatch client wrapper for the pipeline watcher."""

from __future__ import annotations

import logging
import re
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

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
        try:
            self.hostname = socket.gethostname()
            if not self.hostname:
                raise ValueError("Empty hostname")
        except Exception as e:
            logger.warning(f"Failed to get hostname: {e}. Using 'unknown-host'.")
            self.hostname = "unknown-host"

        # Sanitize hostname (keep alphanumeric, hyphens, underscores, dots)
        self.hostname = re.sub(r'[^a-zA-Z0-9\-_\.]', '_', self.hostname)
        if not self.hostname or set(self.hostname) == {"_"}:
            self.hostname = "unknown-host"

        self.bucket_id = f"aw-watcher-pipeline-stage_{self.hostname}"
        self.last_connection_error_log_time = 0.0

        logger.info(f"PipelineClient initialized. Bucket: {self.bucket_id} (Privacy: Local-only, Offline-first)")

        self.client: Any

        if client:
            self.client = client
        elif testing:
            self.client = MockActivityWatchClient()
        else:
            if ActivityWatchClient is None:
                raise ImportError("aw-client is not installed. Please install it to run in production mode.")
            self.client = ActivityWatchClient("aw-watcher-pipeline-stage", port=port, testing=testing)

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
        status: str = "in_progress",
        start_time: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        file_path: Optional[str] = None,
        computed_duration: Optional[float] = 0.0,
    ) -> None:
        """Send a heartbeat to the server.

        Handles connection errors with local buffering (queued=True) and logging.
        Retries transient errors with exponential backoff.

        Uses queued=True to allow buffering if the server is offline. Handles
        connection errors by logging warnings and retrying, assuming aw-client
        might raise errors if buffering fails or connection is refused.
        """
        data: Dict[str, Any] = {
            "stage": stage,
            "task": task,
            "status": status,
        }
        if project_id:
            data["project_id"] = project_id
        if start_time:
            data["start_time"] = start_time
        if file_path:
            data["file_path"] = file_path
        if computed_duration is not None:
            if computed_duration < 0:
                logger.warning(
                    f"Computed duration is negative ({computed_duration}s). System clock may have moved backwards."
                )
            elif computed_duration > 0:
                if computed_duration > 86400:  # 24 hours
                    logger.warning(
                        f"Computed duration is very large ({computed_duration}s). Possible system clock jump."
                    )
                data["computed_duration"] = computed_duration

        # Flatten metadata
        if metadata:
            if not isinstance(metadata, dict):
                logger.warning(f"Metadata is not a dictionary ({type(metadata).__name__}). Ignoring.")
            else:
                for k, v in metadata.items():
                    if k in data:
                        logger.warning(f"Metadata key '{k}' conflicts with core field. Ignoring.")
                        continue
                    data[str(k)] = v

        timestamp = datetime.now(timezone.utc)
        if start_time:
            try:
                # Validate format and use as event timestamp
                ts_str = str(start_time).replace("Z", "+00:00")
                parsed_ts = datetime.fromisoformat(ts_str)
                if parsed_ts.tzinfo is None:
                    parsed_ts = parsed_ts.replace(tzinfo=timezone.utc)
                timestamp = parsed_ts
            except (ValueError, AttributeError, TypeError):
                logger.warning(f"Invalid start_time format: {start_time}. Dropping from payload.")
                if "start_time" in data:
                    del data["start_time"]

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
            raise

    def close(self) -> None:
        """Close the client connection."""
        if hasattr(self.client, "disconnect"):
            self.client.disconnect()
            logger.info("Client closed.")

    def __repr__(self) -> str:
        return f"<PipelineClient bucket={self.bucket_id} host={self.hostname}>"
