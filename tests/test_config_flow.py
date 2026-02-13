"""Tests specifically for configuration flow and priority verification.

Verifies: CLI > Env > Config File > Defaults.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from aw_watcher_pipeline_stage.config import load_config


def test_defaults_propagation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that defaults are correctly propagated when no other sources are present."""
    monkeypatch.chdir(tmp_path)
    # Ensure no git root to trigger default "."
    with patch("aw_watcher_pipeline_stage.config._find_project_root", return_value=None):
        with patch.dict(os.environ, {}, clear=True):
            config = load_config({})
            
            assert config.watch_path == "."
            assert config.port == 5600
            assert config.testing is False
            assert config.pulsetime == 120.0
            assert config.debounce_seconds == 1.0
            assert config.metadata_allowlist is None
            assert config.batch_size_limit == 5
            assert config.log_level == "INFO"


def test_config_file_overrides_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that config file values override defaults."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("""
[aw-watcher-pipeline-stage]
port = 5601
debounce_seconds = 2.0
batch_size_limit = 10
""", encoding="utf-8")

    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        
        assert config.port == 5601
        assert config.debounce_seconds == 2.0
        assert config.batch_size_limit == 10
        # Defaults remain for others
        assert config.pulsetime == 120.0


def test_env_overrides_config_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that environment variables override config file values."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("""
[aw-watcher-pipeline-stage]
port = 5601
pulsetime = 60.0
""", encoding="utf-8")

    env = {
        "AW_WATCHER_PORT": "5602",
        "AW_WATCHER_PULSETIME": "30.0"
    }

    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        
        assert config.port == 5602  # Env wins
        assert config.pulsetime == 30.0  # Env wins


def test_cli_overrides_env_and_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that CLI arguments override both Env and Config file."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5601", encoding="utf-8")
    
    env = {"AW_WATCHER_PORT": "5602"}
    cli_args = {"port": 5603}

    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli_args)
        
        assert config.port == 5603  # CLI wins


def test_full_propagation_all_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that configuration propagates correctly from mixed sources for ALL fields.
    
    Layer 1 (Defaults): batch_size_limit (5)
    Layer 2 (Config File): watch_path, pulsetime
    Layer 3 (Env): port, log_level
    Layer 4 (CLI): testing, debounce_seconds, metadata_allowlist, log_file
    """
    monkeypatch.chdir(tmp_path)
    
    # 1. Config File
    (tmp_path / "config.ini").write_text("""
[aw-watcher-pipeline-stage]
watch_path = ./config.json
pulsetime = 10.0
""", encoding="utf-8")

    # 2. Env
    env = {
        "AW_WATCHER_PORT": "5002",
        "AW_WATCHER_LOG_LEVEL": "WARNING"
    }

    # 3. CLI
    cli_args = {
        "testing": True,
        "debounce_seconds": 0.5,
        "metadata_allowlist": "cli1, cli2",
        "log_file": "cli.log"
    }

    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli_args)

        # Config File
        assert config.watch_path == "./config.json"
        assert config.pulsetime == 10.0

        # Env
        assert config.port == 5002
        assert config.log_level == "WARNING"

        # CLI
        assert config.testing is True
        assert config.debounce_seconds == 0.5
        assert config.metadata_allowlist == ["cli1", "cli2"]
        assert config.log_file == "cli.log"
        
        # Default
        assert config.batch_size_limit == 5


def test_cli_disable_log_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that passing empty string for log_file via CLI disables it (overriding config)."""
    monkeypatch.chdir(tmp_path)
    
    # Config file enables logging
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nlog_file = config.log", encoding="utf-8")
    
    # CLI passes empty string to disable
    cli_args = {"log_file": ""}
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config(cli_args)
        # Should be empty string (falsy), which setup_logging treats as "no file"
        assert config.log_file == ""


def test_env_empty_string_ignored(tmp_path: Path) -> None:
    """Verify that empty environment variables are ignored and do not override defaults/config."""
    env = {
        "AW_WATCHER_PORT": "",
        "AW_WATCHER_PULSETIME": "",
    }
    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        
        # Should use defaults
        assert config.port == 5600
        assert config.pulsetime == 120.0


def test_batch_size_limit_propagation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify batch_size_limit propagation: CLI > Env > Config > Default."""
    monkeypatch.chdir(tmp_path)
    
    # 1. Default
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.batch_size_limit == 5

    # 2. Config File
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nbatch_size_limit = 8", encoding="utf-8")
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.batch_size_limit == 8

    # 3. Env
    with patch.dict(os.environ, {"AW_WATCHER_BATCH_SIZE_LIMIT": "10"}, clear=True):
        config = load_config({})
        assert config.batch_size_limit == 10

    # 4. CLI
    with patch.dict(os.environ, {"AW_WATCHER_BATCH_SIZE_LIMIT": "10"}, clear=True):
        config = load_config({"batch_size_limit": 20})
        assert config.batch_size_limit == 20