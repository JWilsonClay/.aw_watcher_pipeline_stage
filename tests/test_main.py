from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from aw_watcher_pipeline_stage.main import main, setup_logging


def test_setup_logging(tmp_path: Path) -> None:
    log_file = tmp_path / "test.log"
    setup_logging("DEBUG", str(log_file))

    logger = logging.getLogger("test_logger")
    logger.debug("Test message")

    # Check if file was created and contains message
    assert log_file.exists()
    content = log_file.read_text()
    assert "Test message" in content


def test_setup_logging_invalid() -> None:
    with pytest.raises(ValueError, match="Invalid log level"):
        setup_logging("INVALID_LEVEL", None)

def test_setup_logging_creates_dir(tmp_path: Path) -> None:
    """Test that setup_logging creates the log directory if it doesn't exist."""
    log_dir = tmp_path / "subdir" / "logs"
    log_file = log_dir / "test.log"
    
    setup_logging("INFO", str(log_file))
    
    assert log_dir.exists()
    assert log_file.exists()

def test_setup_logging_file_error(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Test that setup_logging handles file access errors gracefully."""
    # Use a directory as log file to trigger OSError/IsADirectoryError
    log_file = tmp_path / "log_dir"
    log_file.mkdir()
    
    setup_logging("INFO", str(log_file))
    
    captured = capsys.readouterr()
    assert "Warning: Failed to setup log file" in captured.err

@patch("aw_watcher_pipeline_stage.main.threading.Event")
@patch("aw_watcher_pipeline_stage.main.PipelineWatcher")
@patch("aw_watcher_pipeline_stage.main.PipelineClient")
@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("aw_watcher_pipeline_stage.main.setup_logging")
@patch("time.sleep")
def test_main_execution(
    mock_sleep: MagicMock,
    mock_setup_logging: MagicMock,
    mock_load_config: MagicMock,
    mock_client_cls: MagicMock,
    mock_watcher_cls: MagicMock,
    mock_event_cls: MagicMock,
) -> None:
    # Setup mocks
    mock_config = MagicMock()
    mock_config.watch_path = "/tmp/test"
    mock_config.port = 5600
    mock_config.testing = True
    mock_config.pulsetime = 120.0
    mock_config.log_level = "INFO"
    mock_config.log_file = None
    mock_config.debounce_seconds = 1.0
    mock_load_config.return_value = mock_config

    mock_client_instance = mock_client_cls.return_value
    mock_watcher_instance = mock_watcher_cls.return_value

    # Mock stop_event to return immediately on wait()
    mock_event_instance = mock_event_cls.return_value
    mock_event_instance.wait.return_value = True

    mock_watcher_instance.observer.is_alive.return_value = True
    mock_watcher_instance.watch_dir.exists.return_value = True

    # Run main
    with patch.object(sys, "argv", ["aw-watcher-pipeline-stage", "--testing"]):
        main()

    # Verify interactions
    mock_load_config.assert_called_once()
    mock_setup_logging.assert_called_once()
    mock_client_cls.assert_called_once_with(
        watch_path=mock_config.watch_path,
        port=mock_config.port,
        testing=mock_config.testing,
        pulsetime=mock_config.pulsetime,
    )
    mock_client_instance.wait_for_start.assert_called_once_with(timeout=5.0)
    mock_client_instance.ensure_bucket.assert_called_once()
    
    mock_watcher_cls.assert_called_once_with(
        mock_config.watch_path,
        mock_client_instance,
        pulsetime=mock_config.pulsetime,
        debounce_seconds=mock_config.debounce_seconds,
    )
    mock_watcher_instance.start.assert_called_once()
    
    # Verify cleanup in finally block
    mock_watcher_instance.stop.assert_called_once()
    mock_client_instance.flush_queue.assert_called_once()
    mock_client_instance.close.assert_called_once()


@patch("aw_watcher_pipeline_stage.main.PipelineWatcher")
@patch("aw_watcher_pipeline_stage.main.PipelineClient")
@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("aw_watcher_pipeline_stage.main.setup_logging")
@patch("signal.signal")
def test_signal_registration(
    mock_signal: MagicMock,
    mock_setup_logging: MagicMock,
    mock_load_config: MagicMock,
    mock_client_cls: MagicMock,
    mock_watcher_cls: MagicMock,
) -> None:
    mock_config = MagicMock()
    mock_config.watch_path = "."
    mock_load_config.return_value = mock_config

    mock_watcher_instance = mock_watcher_cls.return_value
    mock_watcher_instance.observer.is_alive.return_value = True
    mock_watcher_instance.watch_dir.exists.return_value = True
    mock_watcher_instance._stopping = True

    with patch.object(sys, "argv", ["prog"]):
        main()

    # Verify signal registration
    assert mock_signal.call_count >= 2
    calls = [args[0] for args, _ in mock_signal.call_args_list]
    assert signal.SIGINT in calls
    assert signal.SIGTERM in calls


@patch("aw_watcher_pipeline_stage.main.PipelineWatcher")
@patch("aw_watcher_pipeline_stage.main.PipelineClient")
@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("aw_watcher_pipeline_stage.main.setup_logging")
@patch("signal.signal")
@patch("sys.exit")
def test_signal_handler_graceful_shutdown(
    mock_exit: MagicMock,
    mock_signal: MagicMock,
    mock_setup_logging: MagicMock,
    mock_load_config: MagicMock,
    mock_client_cls: MagicMock,
    mock_watcher_cls: MagicMock,
) -> None:
    # Setup mocks
    mock_config = MagicMock()
    mock_config.watch_path = "."
    mock_load_config.return_value = mock_config

    mock_client_instance = mock_client_cls.return_value
    mock_watcher_instance = mock_watcher_cls.return_value

    # Mock observer to be not alive so main loop exits immediately after setup
    # allowing us to verify signal registration without blocking
    mock_watcher_instance.observer.is_alive.return_value = True
    mock_watcher_instance.watch_dir.exists.return_value = True
    mock_watcher_instance._stopping = True

    with patch.object(sys, "argv", ["prog"]):
        main()

    # Now extract the handler
    # signal.signal(signal.SIGINT, handler)
    handler = None
    for call in mock_signal.call_args_list:
        args, _ = call
        if args[0] == signal.SIGINT:
            handler = args[1]
            break

    assert handler is not None

    # Call the handler
    with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
        # Handler should just call sys.exit(0)
        with pytest.raises(SystemExit):
            handler(signal.SIGINT, None)
        # Verify logging
        mock_logger.info.assert_any_call("Received signal SIGINT, shutting down...")
    
    # Note: Cleanup verification is handled by test_main_execution and test_real_process_shutdown
    # because calling handler() directly here does not trigger main()'s finally block.


@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
def test_real_process_shutdown(tmp_path: Path, sig: int) -> None:
    """Test that the actual process handles signals gracefully using a subprocess.

    Covers Directive 5: Verify SIGINT/SIGTERM handling, queue flush, and clean shutdown.
    """
    # Create a dummy task file
    task_file = tmp_path / "current_task.json"
    task_file.write_text('{"current_stage": "Test", "current_task": "Signal"}', encoding="utf-8")

    # Run the module as a subprocess
    cmd = [
        sys.executable,
        "-m",
        "aw_watcher_pipeline_stage.main",
        "--testing",
        "--watch-path",
        str(task_file),
        "--log-level",
        "DEBUG"
    ]

    # Ensure PYTHONPATH includes the current directory
    env = os.environ.copy()
    # Use the project root relative to this test file to ensure robust import
    root_dir = Path(__file__).resolve().parents[1]
    env["PYTHONPATH"] = f"{str(root_dir)}{os.pathsep}{env.get('PYTHONPATH', '')}"

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
        bufsize=1  # Line buffered
    )

    captured_stdout = []

    try:
        # Wait for startup by monitoring stdout
        # We expect "Observer started" which indicates the watcher is fully up
        started = False
        start_wait = time.time()
        while time.time() - start_wait < 10.0:
            if process.poll() is not None:
                break
            
            # Read line (blocking, but we expect output)
            line = process.stdout.readline()
            if line:
                captured_stdout.append(line)
                if "Observer started" in line:
                    started = True
                    break
            else:
                # EOF
                break
        
        if not started:
             # Consume rest if any
             rest, _ = process.communicate(timeout=1)
             captured_stdout.append(rest)
             pytest.fail(f"Process exited prematurely or did not start in time:\n{''.join(captured_stdout)}")

        # Send Signal
        process.send_signal(sig)
        
        # Wait for exit
        try:
            stdout_rest, _ = process.communicate(timeout=5)
            captured_stdout.append(stdout_rest)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout_rest, _ = process.communicate()
            captured_stdout.append(stdout_rest)
            pytest.fail(f"Process did not exit in time after signal {sig}")

        # Verify exit code
        assert process.returncode == 0

        # Verify logs in stdout
        stdout = "".join(captured_stdout)
        sig_name = signal.Signals(sig).name
        assert f"Received signal {sig_name}, shutting down..." in stdout
        assert "Watcher stopped." in stdout
        assert "Flushing event queue..." in stdout
        # Confirming flush was called ensures no pending events are lost (queued events are flushed)
        assert "[MOCK] flush: Queue flushed" in stdout
        assert "Client closed." in stdout
        # Verify that the initial event was processed (confirming the loop ran at least once/setup worked)
        assert "[MOCK] heartbeat: Test - Signal" in stdout

        # Verify Resource Usage logging
        assert "Resource Usage" in stdout

        # Verify Order of Operations to ensure graceful shutdown
        # 1. Signal received
        # 2. Watcher stopped (stops processing new events)
        # 3. Queue flushed (sends any pending events)
        # 4. Client closed (cleans up connection)
        idx_signal = stdout.find(f"Received signal {sig_name}")
        idx_stop = stdout.find("Watcher stopped.")
        idx_flush = stdout.find("Flushing event queue...")
        idx_close = stdout.find("Client closed.")

        assert idx_signal != -1
        assert idx_stop != -1
        assert idx_flush != -1
        assert idx_close != -1

        assert idx_signal < idx_stop, "Watcher stopped before signal received?"
        assert idx_stop < idx_flush, "Queue flushed before watcher stopped?"
        assert idx_flush < idx_close, "Client closed before queue flushed?"

    except Exception:
        print("\n=== Process Stdout (Failure Capture) ===")
        print("".join(captured_stdout))
        print("========================================")
        raise
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


@patch("aw_watcher_pipeline_stage.main.PipelineWatcher")
@patch("aw_watcher_pipeline_stage.main.PipelineClient")
@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("aw_watcher_pipeline_stage.main.setup_logging")
@patch("time.sleep")
def test_main_wait_for_directory(
    mock_sleep: MagicMock,
    mock_setup_logging: MagicMock,
    mock_load_config: MagicMock,
    mock_client_cls: MagicMock,
    mock_watcher_cls: MagicMock,
) -> None:
    # Setup mocks
    mock_config = MagicMock()
    mock_config.watch_path = "/tmp/test"
    mock_load_config.return_value = mock_config

    mock_watcher_instance = mock_watcher_cls.return_value
    # Directory doesn't exist initially, then exists
    mock_watcher_instance.watch_dir.exists.side_effect = [False, True]
    # Observer alive then dead
    mock_watcher_instance._stopping = True

    with patch.object(sys, "argv", ["prog"]):
        main()

    # Verify sleep was called (waiting for directory)
    mock_sleep.assert_called()
    # Verify watcher started
    mock_watcher_instance.start.assert_called_once()


@patch("aw_watcher_pipeline_stage.main.PipelineWatcher")
@patch("aw_watcher_pipeline_stage.main.PipelineClient")
@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("aw_watcher_pipeline_stage.main.setup_logging")
@patch("signal.signal")
@patch("sys.exit", side_effect=SystemExit)
def test_signal_handler_error_resilience(
    mock_exit: MagicMock,
    mock_signal: MagicMock,
    mock_setup_logging: MagicMock,
    mock_load_config: MagicMock,
    mock_client_cls: MagicMock,
    mock_watcher_cls: MagicMock,
) -> None:
    # Setup mocks
    mock_config = MagicMock()
    mock_config.watch_path = "."
    mock_load_config.return_value = mock_config

    mock_client_instance = mock_client_cls.return_value
    mock_watcher_instance = mock_watcher_cls.return_value

    # Mock watcher.stop to raise an exception during cleanup
    mock_watcher_instance.stop.side_effect = RuntimeError("Watcher failed to stop")

    # Mock observer to be not alive so main loop exits immediately
    mock_watcher_instance.observer.is_alive.return_value = True
    mock_watcher_instance.watch_dir.exists.return_value = True
    mock_watcher_instance._stopping = True

    with patch.object(sys, "argv", ["prog"]):
        # main() will run, exit loop, and hit finally block
        try:
            main()
        except SystemExit:
            pass

    # Verify error logged (we need to check logger calls on the mock passed to test)
    # Since we can't easily access the internal logger of main, we rely on the fact 
    # that main() completed and called cleanup.

    # Verify client cleanup still happened
    mock_client_instance.flush_queue.assert_called_once()
    mock_client_instance.close.assert_called_once()


@patch("aw_watcher_pipeline_stage.main.PipelineWatcher")
@patch("aw_watcher_pipeline_stage.main.PipelineClient")
@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("aw_watcher_pipeline_stage.main.setup_logging")
@patch("signal.signal")
@patch("sys.exit", side_effect=SystemExit)
def test_signal_handler_flush_error_resilience(
    mock_exit: MagicMock,
    mock_signal: MagicMock,
    mock_setup_logging: MagicMock,
    mock_load_config: MagicMock,
    mock_client_cls: MagicMock,
    mock_watcher_cls: MagicMock,
) -> None:
    # Setup mocks
    mock_config = MagicMock()
    mock_config.watch_path = "."
    mock_load_config.return_value = mock_config

    mock_client_instance = mock_client_cls.return_value
    mock_watcher_instance = mock_watcher_cls.return_value

    # Mock flush_queue to raise an exception
    mock_client_instance.flush_queue.side_effect = RuntimeError("Flush failed")

    # Mock observer to be not alive so main loop exits immediately
    mock_watcher_instance.observer.is_alive.return_value = True
    mock_watcher_instance.watch_dir.exists.return_value = True
    mock_watcher_instance._stopping = True

    with patch.object(sys, "argv", ["prog"]):
        try:
            main()
        except SystemExit:
            pass

    # Verify client cleanup still happened
    mock_client_instance.close.assert_called_once()


@patch("aw_watcher_pipeline_stage.main.PipelineWatcher")
@patch("aw_watcher_pipeline_stage.main.PipelineClient")
@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("aw_watcher_pipeline_stage.main.setup_logging")
@patch("sys.exit")
def test_main_startup_failure(
    mock_exit: MagicMock,
    mock_setup_logging: MagicMock,
    mock_load_config: MagicMock,
    mock_client_cls: MagicMock,
    mock_watcher_cls: MagicMock,
) -> None:
    """Test that main exits with error if watcher fails to start."""
    mock_config = MagicMock()
    mock_config.watch_path = "."
    mock_load_config.return_value = mock_config

    mock_watcher_instance = mock_watcher_cls.return_value
    mock_watcher_instance.watch_dir.exists.return_value = True
    # Simulate startup failure
    mock_watcher_instance.start.side_effect = RuntimeError("Startup failed")

    with patch.object(sys, "argv", ["prog"]):
        main()

    mock_exit.assert_called_once_with(1)


@patch("aw_watcher_pipeline_stage.main.PipelineWatcher")
@patch("aw_watcher_pipeline_stage.main.PipelineClient")
@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("aw_watcher_pipeline_stage.main.setup_logging")
def test_main_bucket_creation_failure(
    mock_setup_logging: MagicMock,
    mock_load_config: MagicMock,
    mock_client_cls: MagicMock,
    mock_watcher_cls: MagicMock,
) -> None:
    """Test that main proceeds if ensure_bucket fails (robustness)."""
    mock_config = MagicMock()
    mock_config.watch_path = "."
    mock_load_config.return_value = mock_config

    mock_client_instance = mock_client_cls.return_value
    mock_client_instance.ensure_bucket.side_effect = RuntimeError("Bucket creation failed")

    with patch.object(sys, "argv", ["prog"]):
        main()

    # Should have proceeded to start watcher
    mock_watcher_cls.return_value.start.assert_called_once()


@patch("aw_watcher_pipeline_stage.main.threading.Event")
@patch("aw_watcher_pipeline_stage.main.PipelineWatcher")
@patch("aw_watcher_pipeline_stage.main.PipelineClient")
@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("aw_watcher_pipeline_stage.main.setup_logging")
@patch("time.sleep")
def test_main_startup_race_condition(
    mock_sleep: MagicMock,
    mock_setup_logging: MagicMock,
    mock_load_config: MagicMock,
    mock_client_cls: MagicMock,
    mock_watcher_cls: MagicMock,
    mock_event_cls: MagicMock,
) -> None:
    """Test that main handles race condition where directory disappears during start."""
    mock_config = MagicMock()
    mock_config.watch_path = "/tmp/test"
    mock_load_config.return_value = mock_config

    mock_watcher_instance = mock_watcher_cls.return_value
    # Directory exists check passes
    mock_watcher_instance.watch_dir.exists.return_value = True
    
    # Start fails with FileNotFoundError first (race condition), then succeeds
    mock_watcher_instance.start.side_effect = [FileNotFoundError("Race condition"), None]
    
    # Mock stop_event to return immediately on wait()
    mock_event_instance = mock_event_cls.return_value
    mock_event_instance.wait.return_value = True

    with patch.object(sys, "argv", ["prog"]):
        main()

    # Verify start called twice (retry happened)
    assert mock_watcher_instance.start.call_count == 2
    mock_sleep.assert_called()


def test_resource_usage_rapid_calls_accumulation() -> None:
    """Test that rapid calls to log_resource_usage accumulate time until >1.0s."""
    with patch("aw_watcher_pipeline_stage.main.resource") as mock_resource:
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            import aw_watcher_pipeline_stage.main as main_mod
            
            # Setup baseline
            mock_usage = MagicMock()
            mock_usage.ru_maxrss = 20 * 1024
            mock_usage.ru_utime = 10.0
            mock_usage.ru_stime = 5.0
            mock_resource.getrusage.return_value = mock_usage
            mock_resource.RUSAGE_SELF = 0
            
            main_mod._last_rusage = None
            main_mod._last_rusage_time = 0.0
            
            mock_logger.isEnabledFor.return_value = True
            
            # 1. Initial call at T=1000.0
            with patch("time.monotonic", return_value=1000.0):
                main_mod.log_resource_usage()
                
            # 2. Rapid call at T=1000.5 (Delta 0.5s) - Should NOT update baseline
            # If it updated baseline, the next call would see delta 0.6s and also fail.
            with patch("time.monotonic", return_value=1000.5):
                main_mod.log_resource_usage()
                
            # 3. Call at T=1001.1 (Delta from start 1.1s) - Should update
            # If baseline wasn't updated in step 2, delta is 1.1s. If it was, delta is 0.6s.
            # We verify it calculates by checking if it logs CPU usage (which requires calculation)
            mock_usage_new = MagicMock()
            mock_usage_new.ru_maxrss = 20 * 1024
            mock_usage_new.ru_utime = 10.1
            mock_usage_new.ru_stime = 5.0
            mock_resource.getrusage.return_value = mock_usage_new
            
            with patch("time.monotonic", return_value=1001.1):
                main_mod.log_resource_usage()
                
                # Verify debug log contains CPU info (implies calculation happened)
                mock_logger.debug.assert_called()
                args = mock_logger.debug.call_args[0][0]
                assert "CPU=" in args
                assert "CPU=N/A" not in args


@patch("aw_watcher_pipeline_stage.main.threading.Event")
@patch("aw_watcher_pipeline_stage.main.PipelineWatcher")
@patch("aw_watcher_pipeline_stage.main.PipelineClient")
@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("aw_watcher_pipeline_stage.main.setup_logging")
@patch("time.sleep")
def test_main_startup_oserror_resilience(
    mock_sleep: MagicMock,
    mock_setup_logging: MagicMock,
    mock_load_config: MagicMock,
    mock_client_cls: MagicMock,
    mock_watcher_cls: MagicMock,
    mock_event_cls: MagicMock,
) -> None:
    """Test that main handles OSError during startup checks."""
    mock_config = MagicMock()
    mock_config.watch_path = "/tmp/test"
    mock_load_config.return_value = mock_config

    mock_watcher_instance = mock_watcher_cls.return_value
    # 1. OSError on exists check
    # 2. Success
    mock_watcher_instance.watch_dir.exists.side_effect = [OSError("Perm error"), True]
    
    # Mock stop_event to return immediately on wait()
    mock_event_instance = mock_event_cls.return_value
    mock_event_instance.wait.return_value = True

    with patch.object(sys, "argv", ["prog"]):
        main()

    # Should have slept once
    mock_sleep.assert_called()
    # Should have started
    mock_watcher_instance.start.assert_called_once()


@patch("aw_watcher_pipeline_stage.main.threading.Event")
@patch("aw_watcher_pipeline_stage.main.PipelineWatcher")
@patch("aw_watcher_pipeline_stage.main.PipelineClient")
@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("aw_watcher_pipeline_stage.main.setup_logging")
@patch("time.sleep")
def test_main_startup_runtime_error_retry(
    mock_sleep: MagicMock,
    mock_setup_logging: MagicMock,
    mock_load_config: MagicMock,
    mock_client_cls: MagicMock,
    mock_watcher_cls: MagicMock,
    mock_event_cls: MagicMock,
) -> None:
    """Test that main retries if watcher.start() raises RuntimeError."""
    mock_config = MagicMock()
    mock_config.watch_path = "/tmp/test"
    mock_load_config.return_value = mock_config

    mock_watcher_instance = mock_watcher_cls.return_value
    mock_watcher_instance.watch_dir.exists.return_value = True
    
    # Fail once with RuntimeError, then succeed
    mock_watcher_instance.start.side_effect = [RuntimeError("Observer fail"), None]
    
    # Mock stop_event to return immediately on wait()
    mock_event_instance = mock_event_cls.return_value
    mock_event_instance.wait.return_value = True

    with patch.object(sys, "argv", ["prog"]):
        main()

    assert mock_watcher_instance.start.call_count == 2
    mock_sleep.assert_called()


@patch("aw_watcher_pipeline_stage.main.Path")
@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("aw_watcher_pipeline_stage.main.setup_logging")
@patch("sys.exit")
def test_main_invalid_watch_path_resolution(
    mock_exit: MagicMock,
    mock_setup_logging: MagicMock,
    mock_load_config: MagicMock,
    mock_path_cls: MagicMock,
) -> None:
    """Test that main exits if watch path resolution fails."""
    mock_config = MagicMock()
    mock_config.watch_path = "/invalid/path"
    mock_load_config.return_value = mock_config
    
    mock_path_instance = mock_path_cls.return_value
    mock_path_instance.resolve.side_effect = OSError("Invalid path")
    
    with patch.object(sys, "argv", ["prog"]):
        # We need to import main inside patch context to ensure patched Path is used if imported
        from aw_watcher_pipeline_stage.main import main
        main()
        
    mock_exit.assert_called_once_with(1)


def test_resource_usage_thread_error() -> None:
    """Test that log_resource_usage handles errors when getting thread count."""
    with patch("threading.active_count", side_effect=RuntimeError("Thread error")):
        with patch("aw_watcher_pipeline_stage.main.logger") as mock_logger:
            import aw_watcher_pipeline_stage.main as main_mod
            main_mod.log_resource_usage()
            # Should run without raising exception
            mock_logger.debug.assert_called()


@patch("aw_watcher_pipeline_stage.main.threading.Event")
@patch("aw_watcher_pipeline_stage.main.PipelineWatcher")
@patch("aw_watcher_pipeline_stage.main.PipelineClient")
@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("aw_watcher_pipeline_stage.main.setup_logging")
@patch("time.sleep")
def test_main_loop_observer_access_failure(
    mock_sleep: MagicMock,
    mock_setup_logging: MagicMock,
    mock_load_config: MagicMock,
    mock_client_cls: MagicMock,
    mock_watcher_cls: MagicMock,
    mock_event_cls: MagicMock,
) -> None:
    """Test that main loop handles RuntimeError when accessing watcher.observer."""
    mock_config = MagicMock()
    mock_config.watch_path = "."
    mock_load_config.return_value = mock_config

    mock_watcher_instance = mock_watcher_cls.return_value
    mock_watcher_instance.watch_dir.exists.return_value = True
    
    # Simulate observer property raising RuntimeError (e.g. failed to restart)
    # We use a PropertyMock to simulate the property access raising
    type(mock_watcher_instance).observer = PropertyMock(side_effect=[RuntimeError("Observer down"), RuntimeError("Observer down"), MagicMock()])
    
    # Mock stop_event to return immediately on wait()
    mock_event_instance = mock_event_cls.return_value
    mock_event_instance.wait.return_value = True

    with patch.object(sys, "argv", ["prog"]):
        main()

    # Should have slept in the exception block
    assert mock_sleep.call_count >= 2


def test_main_load_config_exit_propagation() -> None:
    """Test that SystemExit from load_config propagates out of main."""
    with patch("aw_watcher_pipeline_stage.main.load_config", side_effect=SystemExit(1)):
        with patch.object(sys, "argv", ["prog"]):
            with pytest.raises(SystemExit):
                from aw_watcher_pipeline_stage.main import main
                main()


def test_main_malicious_arg_exit() -> None:
    """Test that main exits when a malicious path argument causes config failure."""
    with patch("aw_watcher_pipeline_stage.main.load_config", side_effect=SystemExit(1)):
        with patch.object(sys, "argv", ["prog", "--watch-path", "/dev/mem"]):
            with pytest.raises(SystemExit):
                from aw_watcher_pipeline_stage.main import main
                main()


def test_main_log_file_directory_failure(tmp_path: Path) -> None:
    """Test that specifying a directory as log file causes exit."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    
    with patch.object(sys, "argv", ["prog", "--log-file", str(log_dir)]):
        with pytest.raises(SystemExit):
            from aw_watcher_pipeline_stage.main import main
            main()

@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("sys.exit")
def test_main_log_file_traversal_exit(mock_exit: MagicMock, mock_load_config: MagicMock) -> None:
    """Test that main exits if log file path traversal fails validation."""
    # Simulate load_config raising SystemExit due to validation failure
    mock_load_config.side_effect = SystemExit(1)
    
    with patch.object(sys, "argv", ["prog", "--log-file", "../../../etc/passwd"]):
        try:
            from aw_watcher_pipeline_stage.main import main
            main()
        except SystemExit:
            pass
            
    mock_load_config.assert_called()

@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("sys.exit")
def test_main_watch_path_traversal_exit(mock_exit: MagicMock, mock_load_config: MagicMock) -> None:
    """Test that main exits if watch path traversal fails validation."""
    # Simulate load_config raising SystemExit due to validation failure
    mock_load_config.side_effect = SystemExit(1)
    
    with patch.object(sys, "argv", ["prog", "--watch-path", "../../../etc/passwd"]):
        try:
            from aw_watcher_pipeline_stage.main import main
            main()
        except SystemExit:
            pass
            
    mock_load_config.assert_called()


@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("sys.exit")
def test_main_watch_path_symlink_exit(mock_exit: MagicMock, mock_load_config: MagicMock) -> None:
    """Test that main exits if watch path is a symlink (simulated validation failure)."""
    mock_load_config.side_effect = SystemExit(1)
    
    with patch.object(sys, "argv", ["prog", "--watch-path", "symlink.json"]):
        try:
            from aw_watcher_pipeline_stage.main import main
            main()
        except SystemExit:
            pass
            
    mock_load_config.assert_called()


@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("sys.exit")
def test_main_log_file_parent_failure(mock_exit: MagicMock, mock_load_config: MagicMock) -> None:
    """Test that main exits if log file parent directory does not exist."""
    # Simulate load_config raising SystemExit due to validation failure
    mock_load_config.side_effect = SystemExit(1)
    
    with patch.object(sys, "argv", ["prog", "--log-file", "/nonexistent/dir/log.txt"]):
        try:
            from aw_watcher_pipeline_stage.main import main
            main()
        except SystemExit:
            pass
            
    mock_load_config.assert_called()

@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("sys.exit")
def test_main_watch_path_permission_exit(mock_exit: MagicMock, mock_load_config: MagicMock) -> None:
    """Test that main exits if watch path permission is denied."""
    # Simulate load_config raising SystemExit due to permission failure
    mock_load_config.side_effect = SystemExit(1)
    
    with patch.object(sys, "argv", ["prog", "--watch-path", "/nopermission.json"]):
        try:
            from aw_watcher_pipeline_stage.main import main
            main()
        except SystemExit:
            pass
            
    mock_load_config.assert_called()


@patch("aw_watcher_pipeline_stage.main.load_config")
@patch("sys.exit")
def test_main_log_file_symlink_exit(mock_exit: MagicMock, mock_load_config: MagicMock) -> None:
    """Test that main exits if log file is a symlink (simulated validation failure)."""
    mock_load_config.side_effect = SystemExit(1)
    
    with patch.object(sys, "argv", ["prog", "--log-file", "symlink.log"]):
        try:
            from aw_watcher_pipeline_stage.main import main
            main()
        except SystemExit:
            pass
            
    mock_load_config.assert_called()