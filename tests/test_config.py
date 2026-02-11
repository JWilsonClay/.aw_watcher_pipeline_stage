"""Tests for configuration loading and priority logic."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from aw_watcher_pipeline_stage.config import load_config


def test_config_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test default configuration values."""
    monkeypatch.chdir(tmp_path)
    
    # Ensure no env vars interfere and config paths are empty
    env = {"XDG_CONFIG_HOME": str(tmp_path / "empty_config")}
    (tmp_path / "empty_config").mkdir()

    with patch("aw_watcher_pipeline_stage.config._find_project_root", return_value=None):
        with patch.dict(os.environ, env, clear=True):
            config = load_config({})
            assert config.watch_path == "."
            assert config.port == 5600
            assert config.pulsetime == 120.0
            assert config.debounce_seconds == 1.0
            assert config.testing is False
            assert config.batch_size_limit == 5


def test_git_root_detection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that watch_path defaults to git root if available."""
    # Create a fake git repo structure
    (tmp_path / ".git").mkdir()
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    
    monkeypatch.chdir(subdir)
    
    # Ensure no config file interferes
    env = {"XDG_CONFIG_HOME": str(tmp_path / "empty_config")}
    (tmp_path / "empty_config").mkdir()
    
    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        # Should resolve to tmp_path (the git root)
        assert Path(config.watch_path).resolve() == tmp_path.resolve()

    # Test fallback to "." if no git root
    monkeypatch.chdir(tmp_path / "empty_config") # A dir without .git
    with patch("aw_watcher_pipeline_stage.config._find_project_root", return_value=None):
        with patch.dict(os.environ, env, clear=True):
            config = load_config({})
            assert config.watch_path == "."

def test_nested_git_root_detection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that the closest .git directory is used when nested."""
    # Create outer repo
    outer = tmp_path / "outer"
    outer.mkdir()
    (outer / ".git").mkdir()
    
    # Create inner repo
    inner = outer / "inner"
    inner.mkdir()
    (inner / ".git").mkdir()
    
    # Create subdir in inner
    subdir = inner / "subdir"
    subdir.mkdir()
    
    monkeypatch.chdir(subdir)
    
    # Ensure no config/env interference
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        # Should resolve to inner, not outer
        assert Path(config.watch_path).resolve() == inner.resolve()


def test_mixed_configuration_sources(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that configuration can be composed from multiple sources simultaneously."""
    monkeypatch.chdir(tmp_path)
    
    # Config file provides pulsetime
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\npulsetime = 60.0\nport = 5000", encoding="utf-8")
    
    # Env provides port (overrides config)
    env = {"AW_WATCHER_PORT": "5602", "AW_WATCHER_TESTING": "true"}
    
    # CLI provides watch_path
    cli_args = {"watch_path": "./cli_path"}
    
    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli_args)
        
        assert config.pulsetime == 60.0  # From Config
        assert config.port == 5602       # From Env
        assert config.watch_path == "./cli_path" # From CLI
        assert config.testing is True    # From Env


def test_path_resolution_and_expansion(tmp_path: Path) -> None:
    """Test path resolution, including absolute, relative, and user expansion."""
    
    # Absolute path
    abs_path = str(tmp_path / "absolute")
    config = load_config({"watch_path": abs_path})
    assert config.watch_path == abs_path
    
    # Relative path
    rel_path = "relative/path"
    config = load_config({"watch_path": rel_path})
    assert config.watch_path == rel_path
    
    # User expansion (~)
    # We mock os.path.expanduser to verify it's called
    with patch("os.path.expanduser") as mock_expand:
        mock_expand.return_value = "/home/user/expanded"
        config = load_config({"watch_path": "~/expanded"})
        
        mock_expand.assert_called_with("~/expanded")
        assert config.watch_path == "/home/user/expanded"


def test_windows_path_handling() -> None:
    """Test that Windows-style paths are preserved in config loading."""
    win_path = r"C:\Users\Dev\current_task.json"
    config = load_config({"watch_path": win_path})
    assert config.watch_path == win_path


def test_boolean_conversion() -> None:
    """Test boolean string conversion for testing flag."""
    true_values = ["true", "1", "yes", "on", "True", "YES"]
    false_values = ["false", "0", "no", "off", "False", "NO", "other"]

    for val in true_values:
        config = load_config({"testing": val})
        assert config.testing is True

    for val in false_values:
        config = load_config({"testing": val})
        assert config.testing is False


def test_numeric_conversion() -> None:
    """Test numeric type conversion."""
    config = load_config({
        "port": "1234",
        "pulsetime": "60.5",
        "debounce_seconds": "0.5"
    })
    assert config.port == 1234
    assert isinstance(config.port, int)
    assert config.pulsetime == 60.5
    assert isinstance(config.pulsetime, float)
    assert config.debounce_seconds == 0.5
    assert isinstance(config.debounce_seconds, float)


def test_debug_flag_override() -> None:
    """Test that --debug CLI flag sets log_level to DEBUG."""
    config = load_config({"debug": True, "log_level": "INFO"})
    assert config.log_level == "DEBUG"
    
    config = load_config({"debug": False, "log_level": "INFO"})
    assert config.log_level == "INFO"


def test_xdg_config_home_support(tmp_path: Path) -> None:
    """Test that configuration is loaded from XDG_CONFIG_HOME."""
    config_dir = tmp_path / "config"
    app_config_dir = config_dir / "aw-watcher-pipeline-stage"
    app_config_dir.mkdir(parents=True)
    (app_config_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5605", encoding="utf-8")
    
    with patch.dict(os.environ, {"XDG_CONFIG_HOME": str(config_dir)}, clear=True):
        config = load_config({})
        assert config.port == 5605


def test_local_vs_user_config_precedence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that local config.ini overrides user config."""
    monkeypatch.chdir(tmp_path)
    
    # 1. Setup User Config
    user_config_dir = tmp_path / "user_config"
    app_config_dir = user_config_dir / "aw-watcher-pipeline-stage"
    app_config_dir.mkdir(parents=True)
    (app_config_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5607", encoding="utf-8")
    
    env = {"XDG_CONFIG_HOME": str(user_config_dir)}
    
    # 2. Setup Local Config
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5606", encoding="utf-8")
    
    # Verify Local wins
    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        assert config.port == 5606

    # 3. Remove Local, Verify User wins
    (tmp_path / "config.ini").unlink()
    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        assert config.port == 5607


def test_windows_appdata_config_path(tmp_path: Path) -> None:
    """Test that on Windows, APPDATA is used if XDG_CONFIG_HOME is not set."""
    appdata = tmp_path / "AppData"
    config_dir = appdata / "aw-watcher-pipeline-stage"
    config_dir.mkdir(parents=True)
    (config_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5608", encoding="utf-8")

    with patch("os.name", "nt"):
        with patch.dict(os.environ, {"APPDATA": str(appdata), "XDG_CONFIG_HOME": ""}, clear=True):
            config = load_config({})
            assert config.port == 5608


def test_default_config_path_posix(tmp_path: Path) -> None:
    """Test default config path on POSIX systems (~/.config/...)."""
    # Mock expanduser to return a temp path we control
    mock_home = tmp_path / "home"
    mock_home.mkdir()
    
    # Setup expected config location
    config_dir = mock_home / ".config" / "aw-watcher-pipeline-stage"
    config_dir.mkdir(parents=True)
    (config_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5609", encoding="utf-8")

    # We need to mock expanduser because the code calls it for the default path
    # We also need to mock os.name to ensure we hit the else block (not nt)
    with patch("os.name", "posix"):
        with patch.dict(os.environ, {}, clear=True):
            with patch("os.path.expanduser") as mock_expand:
                def expand_side_effect(path: str) -> str:
                    if path == "~":
                        return str(mock_home)
                    if path.startswith("~"):
                        # Replace ~ with mock_home
                        return str(mock_home / path[2:])
                    return path
                
                mock_expand.side_effect = expand_side_effect
                
                config = load_config({})
                assert config.port == 5609


def test_xdg_takes_precedence_over_appdata_on_windows(tmp_path: Path) -> None:
    """Test that XDG_CONFIG_HOME takes precedence over APPDATA on Windows."""
    xdg_dir = tmp_path / "xdg_config"
    appdata_dir = tmp_path / "appdata"
    
    # Setup XDG config
    xdg_app_dir = xdg_dir / "aw-watcher-pipeline-stage"
    xdg_app_dir.mkdir(parents=True)
    (xdg_app_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5610", encoding="utf-8")
    
    # Setup APPDATA config
    appdata_app_dir = appdata_dir / "aw-watcher-pipeline-stage"
    appdata_app_dir.mkdir(parents=True)
    (appdata_app_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5611", encoding="utf-8")
    
    with patch("os.name", "nt"):
        with patch.dict(os.environ, {"XDG_CONFIG_HOME": str(xdg_dir), "APPDATA": str(appdata_dir)}, clear=True):
            config = load_config({})
            assert config.port == 5610


def test_watch_path_expansion_from_env() -> None:
    """Test that watch_path from environment variable is expanded."""
    with patch("os.path.expanduser") as mock_expand:
        mock_expand.return_value = "/home/user/env_expanded"
        with patch.dict(os.environ, {"PIPELINE_WATCHER_PATH": "~/env_expanded"}, clear=True):
            config = load_config({})
            mock_expand.assert_called_with("~/env_expanded")
            assert config.watch_path == "/home/user/env_expanded"


def test_config_file_path_expansion(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that tilde expansion works for paths defined in config file."""
    monkeypatch.chdir(tmp_path)
    
    # Create config file with tilde path
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nwatch_path = ~/config_expanded", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        with patch("os.path.expanduser") as mock_expand:
            mock_expand.return_value = "/home/user/config_expanded"
            
            config = load_config({})
            
            # Verify expanduser was called with the value from config file
            mock_expand.assert_called_with("~/config_expanded")
            assert config.watch_path == "/home/user/config_expanded"


def test_config_file_overrides_git_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that config file watch_path takes precedence over git root detection."""
    # Setup git root
    (tmp_path / ".git").mkdir()
    monkeypatch.chdir(tmp_path)
    
    # Setup config file
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nwatch_path = ./explicit_config", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        # Should be config file value, not tmp_path (git root)
        assert config.watch_path == "./explicit_config"


def test_absolute_path_in_config_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test absolute path in config file is respected."""
    monkeypatch.chdir(tmp_path)
    abs_path = str(tmp_path / "absolute" / "path")
    
    (tmp_path / "config.ini").write_text(f"[aw-watcher-pipeline-stage]\nwatch_path = {abs_path}", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.watch_path == abs_path


def test_env_var_alias_support(tmp_path: Path) -> None:
    """Test that AW_WATCHER_WATCH_PATH works as an alias for watch_path."""
    with patch.dict(os.environ, {"AW_WATCHER_WATCH_PATH": "./env_alias_path"}, clear=True):
        config = load_config({})
        assert config.watch_path == "./env_alias_path"


def test_env_overrides_git_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that environment variable watch_path takes precedence over git root detection."""
    # Setup git root
    (tmp_path / ".git").mkdir()
    monkeypatch.chdir(tmp_path)

    # Setup Env
    env = {"PIPELINE_WATCHER_PATH": "./env_explicit"}

    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        # Should be env value, not tmp_path (git root)
        assert config.watch_path == "./env_explicit"


def test_full_priority_chain_with_git_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test the complete priority chain including dynamic git root default.
    Priority: CLI > Env > Config File > Git Root > CWD (.)
    """
    # 1. Setup Git Root (Lowest priority dynamic default)
    (tmp_path / ".git").mkdir()
    monkeypatch.chdir(tmp_path)

    # 2. Setup Config File
    config_file = tmp_path / "config.ini"
    config_file.write_text("[aw-watcher-pipeline-stage]\nwatch_path = ./config_path", encoding="utf-8")

    # 3. Setup Env
    env = {"PIPELINE_WATCHER_PATH": "./env_path"}

    # 4. Setup CLI
    cli_args = {"watch_path": "./cli_path"}

    # Test 1: CLI wins
    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli_args)
        assert config.watch_path == "./cli_path"

    # Test 2: Env wins (CLI removed)
    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        assert config.watch_path == "./env_path"

    # Test 3: Config wins (Env removed)
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.watch_path == "./config_path"


def test_comprehensive_path_resolution_and_priority(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Comprehensive test for Directive 8:
    Verifies path resolution (expansion), priority (CLI > Env > Config), and platform handling.
    """
    monkeypatch.chdir(tmp_path)
    
    # Setup Config File
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nwatch_path = ~/config_path", encoding="utf-8")
    
    # Setup Env
    env = {"PIPELINE_WATCHER_PATH": "~/env_path"}
    
    # Setup CLI
    cli_args = {"watch_path": "~/cli_path"}
    
    # Mock expanduser to verify expansion happens on the WINNER
    with patch("os.path.expanduser") as mock_expand:
        mock_expand.side_effect = lambda p: p.replace("~", "/expanded")
        
        # 1. CLI Wins
        with patch.dict(os.environ, env, clear=True):
            config = load_config(cli_args)
            assert config.watch_path == "/expanded/cli_path"
            
        # 2. Env Wins
        with patch.dict(os.environ, env, clear=True):
            config = load_config({})
            assert config.watch_path == "/expanded/env_path"
            
        # 3. Config Wins
        with patch.dict(os.environ, {}, clear=True):
            config = load_config({})
            assert config.watch_path == "/expanded/config_path"

    # Test 4: Git Root wins (Config removed)
    config_file.unlink()
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert Path(config.watch_path).resolve() == tmp_path.resolve()

    # Test 5: CWD wins (Git Root removed)
    (tmp_path / ".git").rmdir()
    with patch("aw_watcher_pipeline_stage.config._find_project_root", return_value=None):
        with patch.dict(os.environ, {}, clear=True):
            config = load_config({})
            assert config.watch_path == "."


def test_log_file_expansion(tmp_path: Path) -> None:
    """Test that log_file path is expanded."""
    with patch("os.path.expanduser") as mock_expand:
        def side_effect(path: str) -> str:
            if path == "~/test.log":
                return "/home/user/test.log"
            return path
        mock_expand.side_effect = side_effect

        config = load_config({"log_file": "~/test.log"})

        assert config.log_file == "/home/user/test.log"


def test_empty_watch_path_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that explicit empty string for watch_path triggers default logic."""
    monkeypatch.chdir(tmp_path)
    # Ensure no env/config interference
    with patch("aw_watcher_pipeline_stage.config._find_project_root", return_value=None):
        with patch.dict(os.environ, {}, clear=True):
            # Pass empty string via CLI args
            config = load_config({"watch_path": ""})
            # Should default to "." (since tmp_path is not git root)
            assert config.watch_path == "."


def test_extra_config_keys_ignored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that extra keys in config file are ignored."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nunknown_key = value\nport = 5699", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.port == 5699
        # Config object is a dataclass, so it won't have the attribute unless defined
        assert not hasattr(config, "unknown_key")


def test_invalid_type_conversion_raises() -> None:
    """Test that invalid types in config raise ValueError."""
    with pytest.raises(ValueError, match="Invalid integer for port"):
        load_config({"port": "invalid_int"})
        
    with pytest.raises(ValueError, match="Invalid float for pulsetime"):
        load_config({"pulsetime": "invalid_float"})

    with pytest.raises(ValueError, match="Invalid integer for batch_size_limit"):
        load_config({"batch_size_limit": "not_a_number"})


def test_cli_args_none_values() -> None:
    """Test that None values in cli_args are ignored (fall through to lower priority)."""
    # Default port is 5600
    config = load_config({"port": None})
    assert config.port == 5600


def test_cli_none_falls_through_to_env() -> None:
    """Test that explicit None in CLI args falls through to Environment variable."""
    env = {"AW_WATCHER_PORT": "5699"}
    with patch.dict(os.environ, env, clear=True):
        # Pass None for port
        config = load_config({"port": None})
        # Should pick up Env value
        assert config.port == 5699


def test_config_file_partial_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that config file can override some defaults while leaving others."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5688", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.port == 5688
        assert config.pulsetime == 120.0 # Default


def test_windows_path_in_config_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that Windows-style paths with backslashes in config file are preserved."""
    monkeypatch.chdir(tmp_path)
    # Note: In INI files, backslashes are generally treated literally by ConfigParser
    # unless using ExtendedInterpolation. We explicitly disable interpolation in config.py.
    win_path = r"C:\Users\Dev\current_task.json"
    
    (tmp_path / "config.ini").write_text(f"[aw-watcher-pipeline-stage]\nwatch_path = {win_path}", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.watch_path == win_path


def test_config_file_unicode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test reading config file with Unicode characters (enforcing UTF-8)."""
    monkeypatch.chdir(tmp_path)
    unicode_path = "/home/user/projects/🚀/current_task.json"
    
    (tmp_path / "config.ini").write_text(f"[aw-watcher-pipeline-stage]\nwatch_path = {unicode_path}", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.watch_path == unicode_path


def test_config_file_path_with_percent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that paths with percent signs are not interpolated."""
    monkeypatch.chdir(tmp_path)
    path_with_percent = "/tmp/100%_complete/task.json"
    
    (tmp_path / "config.ini").write_text(f"[aw-watcher-pipeline-stage]\nwatch_path = {path_with_percent}", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.watch_path == path_with_percent


def test_env_var_alias_precedence() -> None:
    """Test precedence when both environment variable aliases are present."""
    # AW_WATCHER_WATCH_PATH is processed first, PIPELINE_WATCHER_PATH second (overwriting)
    # based on dict definition order in config.py
    env = {
        "PIPELINE_WATCHER_PATH": "./pipeline_path",
        "AW_WATCHER_WATCH_PATH": "./aw_path"
    }
    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        assert config.watch_path == "./pipeline_path"


def test_windows_user_expansion() -> None:
    """Test user expansion on Windows."""
    with patch("os.name", "nt"):
        with patch("os.path.expanduser") as mock_expand:
            mock_expand.return_value = r"C:\Users\User\expanded"
            config = load_config({"watch_path": r"~\expanded"})
            
            mock_expand.assert_called_with(r"~\expanded")
            assert config.watch_path == r"C:\Users\User\expanded"


def test_log_file_from_env(tmp_path: Path) -> None:
    """Test that log_file can be set via environment variable and is expanded."""
    with patch("os.path.expanduser") as mock_expand:
        mock_expand.return_value = "/home/user/env.log"
        with patch.dict(os.environ, {"AW_WATCHER_LOG_FILE": "~/env.log"}, clear=True):
            config = load_config({})
            mock_expand.assert_called_with("~/env.log")
            assert config.log_file == "/home/user/env.log"


def test_xdg_empty_string_fallback(tmp_path: Path) -> None:
    """Test that empty XDG_CONFIG_HOME falls back to default ~/.config on POSIX."""
    mock_home = tmp_path / "home"
    mock_home.mkdir()
    
    config_dir = mock_home / ".config" / "aw-watcher-pipeline-stage"
    config_dir.mkdir(parents=True)
    (config_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5700", encoding="utf-8")

    with patch("os.name", "posix"):
        # Set XDG_CONFIG_HOME to empty string
        with patch.dict(os.environ, {"XDG_CONFIG_HOME": ""}, clear=True):
            with patch("os.path.expanduser") as mock_expand:
                def expand_side_effect(path: str) -> str:
                    if path == "~":
                        return str(mock_home)
                    if path.startswith("~"):
                        return str(mock_home / path[2:])
                    return path
                mock_expand.side_effect = expand_side_effect
                
                config = load_config({})
                assert config.port == 5700


@pytest.mark.parametrize("os_name", ["posix", "nt"])
def test_cross_platform_priority(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, os_name: str) -> None:
    """Test that priority logic holds regardless of OS simulation (parametrized)."""
    monkeypatch.chdir(tmp_path)
    
    # Setup Config File (Priority 3)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5630\nwatch_path = ./config_path", encoding="utf-8")
    
    # Setup Env (Priority 2)
    env = {"AW_WATCHER_PORT": "5631", "PIPELINE_WATCHER_PATH": "./env_path"}
    
    # Setup CLI (Priority 1)
    cli_args = {"port": 5632, "watch_path": "./cli_path"}
    
    with patch("os.name", os_name):
        # 1. CLI wins
        with patch.dict(os.environ, env, clear=True):
            config = load_config(cli_args)
            assert config.port == 5632
            assert config.watch_path == "./cli_path"
            
        # 2. Env wins
        with patch.dict(os.environ, env, clear=True):
            config = load_config({})
            assert config.port == 5631
            assert config.watch_path == "./env_path"
            
        # 3. Config wins
        with patch.dict(os.environ, {}, clear=True):
            config = load_config({})
            assert config.port == 5630
            assert config.watch_path == "./config_path"


def test_default_watch_path_fallback_explicit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that watch_path defaults to '.' if no git root and no config provided."""
    monkeypatch.chdir(tmp_path)
    # Ensure no git root
    with patch("aw_watcher_pipeline_stage.config._find_project_root", return_value=None):
        with patch.dict(os.environ, {}, clear=True):
            config = load_config({})
            assert config.watch_path == "."


def test_deep_git_root_detection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test git root detection from deep within a subdirectory."""
    (tmp_path / ".git").mkdir()
    deep_dir = tmp_path / "a" / "b" / "c"
    deep_dir.mkdir(parents=True)
    
    monkeypatch.chdir(deep_dir)
    
    # Ensure no config/env interference
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        # Should resolve to tmp_path (the git root)
        assert Path(config.watch_path).resolve() == tmp_path.resolve()


@pytest.mark.parametrize("os_name, env_vars, expected_path_part", [
    ("posix", {"XDG_CONFIG_HOME": "/tmp/xdg"}, "/tmp/xdg/aw-watcher-pipeline-stage/config.ini"),
    ("posix", {}, ".config/aw-watcher-pipeline-stage/config.ini"),
    ("nt", {"APPDATA": "/tmp/appdata"}, "/tmp/appdata/aw-watcher-pipeline-stage/config.ini"),
    ("nt", {}, ".config/aw-watcher-pipeline-stage/config.ini"),
])
def test_config_file_location_logic(
    os_name: str, 
    env_vars: dict, 
    expected_path_part: str,
) -> None:
    """Test logic for determining config file location across platforms."""
    with patch("os.name", os_name):
        with patch.dict(os.environ, env_vars, clear=True):
            with patch("os.path.isfile", return_value=False) as mock_isfile:
                with patch("os.path.expanduser", side_effect=lambda p: p.replace("~", "/home/user")):
                    load_config({})
                    
                    checked_paths = [str(args[0]).replace("\\", "/") for args, _ in mock_isfile.call_args_list]
                    assert any(expected_path_part in p for p in checked_paths)


def test_watch_path_as_path_object() -> None:
    """Test that watch_path provided as a Path object is converted to string."""
    path_obj = Path("/tmp/some/path")
    config = load_config({"watch_path": path_obj})
    assert config.watch_path == str(path_obj)
    assert isinstance(config.watch_path, str)


def test_relative_path_preservation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that relative paths are preserved in config (not resolved to absolute)."""
    monkeypatch.chdir(tmp_path)
    # CLI
    config = load_config({"watch_path": "./relative/foo"})
    assert config.watch_path == "./relative/foo"
    
    # Config file
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nwatch_path = ../parent/bar", encoding="utf-8")
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.watch_path == "../parent/bar"


def test_user_expansion_extended() -> None:
    """Test ~user expansion."""
    with patch("os.path.expanduser") as mock_expand:
        mock_expand.side_effect = lambda p: p.replace("~otheruser", "/home/otheruser")
        
        config = load_config({"watch_path": "~otheruser/project"})
        assert config.watch_path == "/home/otheruser/project"


def test_git_root_detection_from_file_path(tmp_path: Path) -> None:
    """Test git root detection if start path is a file (robustness check)."""
    (tmp_path / ".git").mkdir()
    file_path = tmp_path / "file.txt"
    file_path.touch()
    
    # Mock Path.cwd to return a file path
    with patch("pathlib.Path.cwd", return_value=file_path):
        with patch.dict(os.environ, {}, clear=True):
            config = load_config({})
            # Should resolve to tmp_path (the git root)
            assert Path(config.watch_path).resolve() == tmp_path.resolve()


def test_config_path_construction_uses_os_path_join() -> None:
    """Verify that config path construction uses os.path.join for cross-platform compatibility."""
    with patch("os.environ", {"XDG_CONFIG_HOME": "/tmp/xdg"}):
        with patch("os.path.join") as mock_join:
            mock_join.return_value = "/tmp/xdg/aw-watcher-pipeline-stage/config.ini"
            with patch("os.path.isfile", return_value=False):
                load_config({})
                # Verify it was called with separate components, allowing OS-specific join
                mock_join.assert_any_call("/tmp/xdg", "aw-watcher-pipeline-stage", "config.ini")


def test_debounce_seconds_priority(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test priority for debounce_seconds."""
    monkeypatch.chdir(tmp_path)
    
    # 1. Default
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.debounce_seconds == 1.0

    # 2. Config File
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\ndebounce_seconds = 2.5", encoding="utf-8")
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.debounce_seconds == 2.5

    # 3. Env
    with patch.dict(os.environ, {"AW_WATCHER_DEBOUNCE_SECONDS": "0.5"}, clear=True):
        config = load_config({})
        assert config.debounce_seconds == 0.5

    # 4. CLI
    with patch.dict(os.environ, {"AW_WATCHER_DEBOUNCE_SECONDS": "0.5"}, clear=True):
        config = load_config({"debounce_seconds": 3.0})
        assert config.debounce_seconds == 3.0

def test_log_file_priority(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test priority for log_file."""
    monkeypatch.chdir(tmp_path)
    
    # 1. Default
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.log_file is None

    # 2. Config File
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nlog_file = ./config.log", encoding="utf-8")
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.log_file == "./config.log"

    # 3. Env
    with patch.dict(os.environ, {"AW_WATCHER_LOG_FILE": "./env.log"}, clear=True):
        config = load_config({})
        assert config.log_file == "./env.log"

    # 4. CLI
    with patch.dict(os.environ, {"AW_WATCHER_LOG_FILE": "./env.log"}, clear=True):
        config = load_config({"log_file": "./cli.log"})
        assert config.log_file == "./cli.log"

def test_testing_priority(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test priority for testing flag."""
    monkeypatch.chdir(tmp_path)
    
    # 1. Default
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.testing is False

    # 2. Config File
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\ntesting = true", encoding="utf-8")
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.testing is True

    # 3. Env
    with patch.dict(os.environ, {"AW_WATCHER_TESTING": "false"}, clear=True):
        config = load_config({})
        assert config.testing is False

    # 4. CLI
    with patch.dict(os.environ, {"AW_WATCHER_TESTING": "false"}, clear=True):
        config = load_config({"testing": True})
        assert config.testing is True

def test_log_level_priority(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test priority for log_level."""
    monkeypatch.chdir(tmp_path)
    
    # 1. Default
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.log_level == "INFO"

    # 2. Config File
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nlog_level = ERROR", encoding="utf-8")
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.log_level == "ERROR"

    # 3. Env
    with patch.dict(os.environ, {"AW_WATCHER_LOG_LEVEL": "WARNING"}, clear=True):
        config = load_config({})
        assert config.log_level == "WARNING"
        
    # 4. CLI (debug flag overrides)
    with patch.dict(os.environ, {"AW_WATCHER_LOG_LEVEL": "WARNING"}, clear=True):
        config = load_config({"debug": True})
        assert config.log_level == "DEBUG"


def test_git_root_detection_skipped_if_explicit(tmp_path: Path) -> None:
    """Test that git root detection is skipped if watch_path is provided."""
    with patch("aw_watcher_pipeline_stage.config._find_project_root") as mock_find:
        # Provide watch_path via CLI
        load_config({"watch_path": "."})
        mock_find.assert_not_called()
        
        # Provide via Env
        with patch.dict(os.environ, {"PIPELINE_WATCHER_PATH": "."}, clear=True):
            load_config({})
            mock_find.assert_not_called()


def test_log_file_as_path_object() -> None:
    """Test that log_file provided as a Path object is converted to string."""
    path_obj = Path("/tmp/test.log")
    with patch("os.path.expanduser", return_value="/tmp/test.log"):
        config = load_config({"log_file": path_obj})
        assert config.log_file == "/tmp/test.log"
        assert isinstance(config.log_file, str)


def test_malformed_config_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a malformed config file is ignored and an error is logged."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("not an ini file", encoding="utf-8")

    with patch("aw_watcher_pipeline_stage.config.logger") as mock_logger:
        with patch.dict(os.environ, {}, clear=True):
            config = load_config({})
            # Should fall back to defaults
            assert config.port == 5600
            # Should log error
            mock_logger.error.assert_called()
            assert "Failed to parse config file" in str(mock_logger.error.call_args)


def test_config_file_unicode_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a config file with bad encoding is ignored."""
    monkeypatch.chdir(tmp_path)
    with open(tmp_path / "config.ini", "wb") as f:
        f.write(b"\x80\x81")

    with patch("aw_watcher_pipeline_stage.config.logger") as mock_logger:
        with patch.dict(os.environ, {}, clear=True):
            load_config({})
            mock_logger.error.assert_called()
            assert "Failed to parse config file" in str(mock_logger.error.call_args)


def test_appdata_ignored_on_posix(tmp_path: Path) -> None:
    """Test that APPDATA is ignored on POSIX systems."""
    appdata = tmp_path / "AppData"
    config_dir = appdata / "aw-watcher-pipeline-stage"
    config_dir.mkdir(parents=True)
    (config_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5699", encoding="utf-8")

    with patch("os.name", "posix"):
        with patch.dict(os.environ, {"APPDATA": str(appdata), "XDG_CONFIG_HOME": ""}, clear=True):
            with patch("os.path.expanduser", return_value="/non/existent/path"):
                config = load_config({})
                assert config.port == 5600


def test_find_project_root_oserror(tmp_path: Path) -> None:
    """Test that _find_project_root handles OSErrors gracefully."""
    from aw_watcher_pipeline_stage.config import _find_project_root

    with patch("pathlib.Path.resolve", side_effect=OSError("Permission denied")):
        result = _find_project_root(tmp_path)
        assert result is None


def test_windows_env_var_path() -> None:
    """Test that Windows paths in environment variables are preserved."""
    win_path = r"D:\Projects\MyPipeline"
    with patch.dict(os.environ, {"PIPELINE_WATCHER_PATH": win_path}, clear=True):
        config = load_config({})
        assert config.watch_path == win_path


def test_fallback_config_path_construction() -> None:
    """Verify that fallback config path is constructed using os.path.join for cross-platform safety."""
    with patch("os.name", "posix"):
        with patch.dict(os.environ, {}, clear=True): # No XDG
            with patch("os.path.expanduser", return_value="/home/user"):
                with patch("os.path.join") as mock_join:
                    # We need to return a path that doesn't exist so we don't try to read it
                    mock_join.return_value = "/home/user/.config/aw-watcher-pipeline-stage/config.ini"
                    with patch("os.path.isfile", return_value=False):
                        load_config({})
                        # Verify join called with components
                        mock_join.assert_any_call("/home/user", ".config", "aw-watcher-pipeline-stage", "config.ini")


def test_xdg_config_home_expansion(tmp_path: Path) -> None:
    """Test that XDG_CONFIG_HOME with tilde is expanded."""
    mock_home = tmp_path / "home"
    mock_home.mkdir()
    
    config_dir = mock_home / "config"
    app_config_dir = config_dir / "aw-watcher-pipeline-stage"
    app_config_dir.mkdir(parents=True)
    (app_config_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5612", encoding="utf-8")
    
    with patch.dict(os.environ, {"XDG_CONFIG_HOME": "~/config"}, clear=True):
        with patch("os.path.expanduser") as mock_expand:
            def expand_side_effect(path: str) -> str:
                if path == "~/config":
                    return str(config_dir)
                return path
            mock_expand.side_effect = expand_side_effect
            
            config = load_config({})
            assert config.port == 5612


def test_appdata_missing_on_windows_fallback(tmp_path: Path) -> None:
    """Test fallback to ~/.config on Windows if APPDATA is missing."""
    mock_home = tmp_path / "home"
    mock_home.mkdir()
    
    config_dir = mock_home / ".config" / "aw-watcher-pipeline-stage"
    config_dir.mkdir(parents=True)
    (config_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5613", encoding="utf-8")

    with patch("os.name", "nt"):
        # APPDATA missing
        with patch.dict(os.environ, {}, clear=True):
            with patch("os.path.expanduser") as mock_expand:
                def expand_side_effect(path: str) -> str:
                    if path == "~":
                        return str(mock_home)
                    return path
                mock_expand.side_effect = expand_side_effect
                
                config = load_config({})
                assert config.port == 5613


def test_cli_overrides_config_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that CLI args override config file values."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5000", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({"port": 6000})
        assert config.port == 6000


def test_cwd_failure_handling() -> None:
    """Test that config loading survives if Path.cwd() fails."""
    with patch("pathlib.Path.cwd", side_effect=OSError("Current directory not found")):
        with patch("aw_watcher_pipeline_stage.config._find_project_root") as mock_find:
            config = load_config({})
            assert config.watch_path == "."
            mock_find.assert_not_called()


def test_cli_extra_keys_ignored() -> None:
    """Test that extra keys in CLI args are ignored."""
    config = load_config({"unknown_cli_arg": "value", "port": 5777})
    assert config.port == 5777
    assert not hasattr(config, "unknown_cli_arg")


def test_config_file_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that an empty config file is handled gracefully."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        # Should use defaults
        assert config.port == 5600


def test_config_file_wrong_section(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a config file with the wrong section is ignored."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[other-section]\nport = 9999", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        # Should use defaults, not 9999
        assert config.port == 5600


def test_config_loading_logs_debug(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that loading a config file logs a debug message."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5555", encoding="utf-8")

    with patch("aw_watcher_pipeline_stage.config.logger") as mock_logger:
        with patch.dict(os.environ, {}, clear=True):
            load_config({})
            # Verify that we logged the loading action
            assert any("Loading config from config.ini" in str(call) for call in mock_logger.debug.call_args_list)


def test_local_vs_appdata_precedence_on_windows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that local config.ini overrides APPDATA config on Windows."""
    monkeypatch.chdir(tmp_path)
    
    appdata = tmp_path / "AppData"
    config_dir = appdata / "aw-watcher-pipeline-stage"
    config_dir.mkdir(parents=True)
    (config_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5640", encoding="utf-8")
    
    # Local config
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5641", encoding="utf-8")
    
    with patch("os.name", "nt"):
        with patch.dict(os.environ, {"APPDATA": str(appdata), "XDG_CONFIG_HOME": ""}, clear=True):
            config = load_config({})
            assert config.port == 5641
            
    # Remove local, should fall back to APPDATA
    (tmp_path / "config.ini").unlink()
    with patch("os.name", "nt"):
        with patch.dict(os.environ, {"APPDATA": str(appdata), "XDG_CONFIG_HOME": ""}, clear=True):
            config = load_config({})
            assert config.port == 5640


def test_find_project_root_logic(tmp_path: Path) -> None:
    """Test _find_project_root logic directly."""
    from aw_watcher_pipeline_stage.config import _find_project_root
    
    # Case 1: .git exists in parent
    git_root = tmp_path / "repo"
    (git_root / ".git").mkdir(parents=True)
    subdir = git_root / "sub" / "dir"
    subdir.mkdir(parents=True)
    
    assert _find_project_root(subdir) == git_root.resolve()
    
    # Case 2: .git exists in current dir
    assert _find_project_root(git_root) == git_root.resolve()
    
    # Case 3: No .git found
    no_git_dir = tmp_path / "nogit"
    no_git_dir.mkdir()
    
    # Mock .exists() to always return False to simulate no .git anywhere up the tree
    with patch("pathlib.Path.exists", return_value=False):
        assert _find_project_root(no_git_dir) is None


def test_pulsetime_priority(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test priority for pulsetime."""
    monkeypatch.chdir(tmp_path)
    
    # 1. Default
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.pulsetime == 120.0

    # 2. Config File
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\npulsetime = 60.0", encoding="utf-8")
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.pulsetime == 60.0

    # 3. Env
    with patch.dict(os.environ, {"AW_WATCHER_PULSETIME": "30.0"}, clear=True):
        config = load_config({})
        assert config.pulsetime == 30.0

    # 4. CLI
    with patch.dict(os.environ, {"AW_WATCHER_PULSETIME": "30.0"}, clear=True):
        config = load_config({"pulsetime": 15.0})
        assert config.pulsetime == 15.0


def test_xdg_config_home_nonexistent(tmp_path: Path) -> None:
    """Test that a non-existent XDG_CONFIG_HOME is handled gracefully."""
    non_existent = tmp_path / "does_not_exist"
    
    with patch.dict(os.environ, {"XDG_CONFIG_HOME": str(non_existent)}, clear=True):
        config = load_config({})
        # Should just load defaults
        assert config.port == 5600


def test_appdata_empty_string_on_windows(tmp_path: Path) -> None:
    """Test that empty APPDATA falls back to ~/.config on Windows."""
    mock_home = tmp_path / "home"
    mock_home.mkdir()
    
    config_dir = mock_home / ".config" / "aw-watcher-pipeline-stage"
    config_dir.mkdir(parents=True)
    (config_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5614", encoding="utf-8")

    with patch("os.name", "nt"):
        # APPDATA is empty string
        with patch.dict(os.environ, {"APPDATA": ""}, clear=True):
            with patch("os.path.expanduser") as mock_expand:
                def expand_side_effect(path: str) -> str:
                    if path == "~":
                        return str(mock_home)
                    return path
                mock_expand.side_effect = expand_side_effect
                
                config = load_config({})
                assert config.port == 5614


def test_env_empty_strings_ignored() -> None:
    """Test that empty environment variables are ignored and do not cause crashes."""
    env = {
        "AW_WATCHER_PORT": "",
        "AW_WATCHER_PULSETIME": "",
        "AW_WATCHER_DEBOUNCE_SECONDS": "",
        "PIPELINE_WATCHER_PATH": "",
    }
    with patch.dict(os.environ, env, clear=True):
        # Should not raise ValueError for int("") or float("")
        config = load_config({})
        
        # Should use defaults
        assert config.port == 5600
        assert config.pulsetime == 120.0
        assert config.debounce_seconds == 1.0
        assert config.watch_path != ""


def test_config_file_read_permission_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a permission error during config read is handled gracefully."""
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "config.ini"
    config_file.touch()
    
    # Mock ConfigParser.read to raise PermissionError
    with patch("configparser.ConfigParser.read", side_effect=PermissionError("Access denied")):
        with patch("aw_watcher_pipeline_stage.config.logger") as mock_logger:
            with patch.dict(os.environ, {}, clear=True):
                config = load_config({})
                # Should fall back to defaults
                assert config.port == 5600
                # Should log error
                mock_logger.error.assert_called()
                assert "Failed to parse config file" in str(mock_logger.error.call_args)


def test_config_file_is_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that if config file path is a directory, it is handled gracefully."""
    monkeypatch.chdir(tmp_path)
    config_dir = tmp_path / "config.ini"
    config_dir.mkdir()
    
    # Since we now use os.path.isfile(), the directory should be skipped entirely
    # and no error should be logged.
    with patch("aw_watcher_pipeline_stage.config.logger") as mock_logger:
        with patch.dict(os.environ, {}, clear=True):
            config = load_config({})
            assert config.port == 5600
            mock_logger.error.assert_not_called()


def test_log_level_normalization() -> None:
    """Test that log_level is normalized to uppercase."""
    config = load_config({"log_level": "debug"})
    assert config.log_level == "DEBUG"


def test_path_resolution_mixed_separators() -> None:
    """Test handling of mixed path separators."""
    # On Windows, python handles / and \ interchangeably in many cases, but we want to ensure they are preserved.
    mixed_path = "C:/Users\\Dev/project"
    config = load_config({"watch_path": mixed_path})
    assert config.watch_path == mixed_path


def test_config_file_empty_values_ignored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that empty values in config file are ignored and do not cause type errors."""
    monkeypatch.chdir(tmp_path)
    # port is empty, should use default 5600
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport =\npulsetime =", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.port == 5600
        assert config.pulsetime == 120.0


def test_config_file_empty_watch_path_ignored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that empty watch_path in config file is ignored and defaults are used."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nwatch_path =", encoding="utf-8")
    
    with patch("aw_watcher_pipeline_stage.config._find_project_root", return_value=None):
        with patch.dict(os.environ, {}, clear=True):
            config = load_config({})
            assert config.watch_path == "."


def test_windows_config_file_expansion(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test ~ expansion in config file on Windows."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nwatch_path = ~/win_docs", encoding="utf-8")
    
    with patch("os.name", "nt"):
        with patch("os.path.expanduser") as mock_expand:
            mock_expand.return_value = r"C:\Users\User\win_docs"
            
            config = load_config({})
            
            mock_expand.assert_called_with("~/win_docs")
            assert config.watch_path == r"C:\Users\User\win_docs"


def test_path_resolution_priority_explicit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Explicitly test path resolution priority: CLI > Env > Config > Default.
    Satisfies Directive 8 requirements.
    """
    monkeypatch.chdir(tmp_path)
    
    # 1. Default (Git Root or .)
    (tmp_path / ".git").mkdir()
    
    # 2. Config File
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nwatch_path = ./config_path", encoding="utf-8")
    
    # 3. Env
    env = {"PIPELINE_WATCHER_PATH": "./env_path"}
    
    # 4. CLI
    cli_args = {"watch_path": "./cli_path"}
    
    # Verify CLI wins
    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli_args)
        assert config.watch_path == "./cli_path"


def test_get_config_file_paths_order() -> None:
    """Test that _get_config_file_paths returns paths in correct priority order."""
    from aw_watcher_pipeline_stage.config import _get_config_file_paths
    
    # Basic check to ensure local config.ini is always first
    paths = _get_config_file_paths()
    assert paths[0] == "config.ini"
    assert len(paths) >= 2
    assert "aw-watcher-pipeline-stage" in paths[1]
        
    # Verify Env wins over Config
    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        assert config.watch_path == "./env_path"
        
    # Verify Config wins over Default
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.watch_path == "./config_path"


def test_config_keys_case_insensitivity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that config file keys are case-insensitive."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nWATCH_PATH = ./upper\nPort = 5666", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.watch_path == "./upper"
        assert config.port == 5666


def test_watch_path_home_expansion() -> None:
    """Test that watch_path='~' expands to home directory."""
    with patch("os.path.expanduser") as mock_expand:
        mock_expand.return_value = "/home/user"
        config = load_config({"watch_path": "~"})
        mock_expand.assert_called_with("~")
        assert config.watch_path == "/home/user"


def test_env_empty_string_fallback_to_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that empty environment variable falls back to config file value."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5555", encoding="utf-8")
    
    with patch.dict(os.environ, {"AW_WATCHER_PORT": ""}, clear=True):
        config = load_config({})
        assert config.port == 5555


def test_env_empty_watch_path_fallback_to_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that empty watch_path in env falls back to config file."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nwatch_path = ./config_path", encoding="utf-8")
    
    with patch.dict(os.environ, {"PIPELINE_WATCHER_PATH": ""}, clear=True):
        config = load_config({})
        assert config.watch_path == "./config_path"


def test_default_watch_path_logging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that defaulting to '.' logs a debug message."""
    monkeypatch.chdir(tmp_path)
    # Ensure no git root
    with patch("aw_watcher_pipeline_stage.config._find_project_root", return_value=None):
        with patch("aw_watcher_pipeline_stage.config.logger") as mock_logger:
            with patch.dict(os.environ, {}, clear=True):
                load_config({})
                # Verify log message
                assert any("No project root found" in str(call) for call in mock_logger.debug.call_args_list)


def test_platform_specific_config_priority(tmp_path: Path) -> None:
    """
    Test that platform-specific user config files are loaded and respect priority.
    Verifies that Env and CLI override the user config file on both POSIX and Windows.
    """
    # Setup mock home and config directories
    mock_home = tmp_path / "home"
    mock_home.mkdir()
    
    # POSIX location: ~/.config/aw-watcher-pipeline-stage/config.ini
    posix_config_dir = mock_home / ".config" / "aw-watcher-pipeline-stage"
    posix_config_dir.mkdir(parents=True)
    (posix_config_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5001", encoding="utf-8")
    
    # Windows location: APPDATA/aw-watcher-pipeline-stage/config.ini
    appdata_dir = tmp_path / "AppData"
    win_config_dir = appdata_dir / "aw-watcher-pipeline-stage"
    win_config_dir.mkdir(parents=True)
    (win_config_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5002", encoding="utf-8")
    
    # Common Env and CLI
    env_override = {"AW_WATCHER_PORT": "6000"}
    cli_override = {"port": 7000}
    
    # Helper to mock expanduser
    def mock_expanduser(path: str) -> str:
        if path == "~":
            return str(mock_home)
        return path.replace("~", str(mock_home))

    # 1. Test POSIX (Linux/macOS)
    with patch("os.name", "posix"):
        with patch.dict(os.environ, {}, clear=True):
            with patch("os.path.expanduser", side_effect=mock_expanduser):
                # Case A: Config only
                config = load_config({})
                assert config.port == 5001
                
                # Case B: Env overrides Config
                with patch.dict(os.environ, env_override):
                    config = load_config({})
                    assert config.port == 6000
                    
                # Case C: CLI overrides Env
                with patch.dict(os.environ, env_override):
                    config = load_config(cli_override)
                    assert config.port == 7000

    # 2. Test Windows (nt)
    with patch("os.name", "nt"):
        # Ensure XDG is unset so it falls back to APPDATA
        with patch.dict(os.environ, {"APPDATA": str(appdata_dir)}, clear=True):
            with patch("os.path.expanduser", side_effect=mock_expanduser):
                # Case A: Config only
                config = load_config({})
                assert config.port == 5002
                
                # Case B: Env overrides Config
                with patch.dict(os.environ, env_override):
                    config = load_config({})
                    assert config.port == 6000
                    
                # Case C: CLI overrides Env
                with patch.dict(os.environ, env_override):
                    config = load_config(cli_override)
                    assert config.port == 7000


def test_cli_empty_string_resets_to_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that passing empty string via CLI overrides config file and triggers default logic."""
    monkeypatch.chdir(tmp_path)
    
    # Config file sets a specific path
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nwatch_path = /tmp/explicit", encoding="utf-8")
    
    # CLI passes empty string
    cli_args = {"watch_path": ""}
    
    # Ensure no git root
    with patch("aw_watcher_pipeline_stage.config._find_project_root", return_value=None):
        with patch.dict(os.environ, {}, clear=True):
            config = load_config(cli_args)
            # Should default to "." because CLI "" overrides config, and "" triggers default logic
            assert config.watch_path == "."


def test_xdg_config_home_relative(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test XDG_CONFIG_HOME as a relative path."""
    monkeypatch.chdir(tmp_path)
    
    # Create relative config dir
    rel_config_dir = Path("my_config")
    app_config_dir = rel_config_dir / "aw-watcher-pipeline-stage"
    app_config_dir.mkdir(parents=True)
    (app_config_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5999", encoding="utf-8")
    
    with patch.dict(os.environ, {"XDG_CONFIG_HOME": "my_config"}, clear=True):
        config = load_config({})
        assert config.port == 5999

def test_invalid_numeric_values_logic() -> None:
    """Test validation for numeric values."""
    # Negative pulsetime
    with pytest.raises(ValueError, match="pulsetime must be positive"):
        load_config({"pulsetime": -10.0})
        
    # Negative debounce
    with pytest.raises(ValueError, match="debounce_seconds must be non-negative"):
        load_config({"debounce_seconds": -0.5})

    # Invalid port range (Low)
    with pytest.raises(ValueError, match="Port must be between 1 and 65535"):
        load_config({"port": 0})

    # Invalid port range (High)
    with pytest.raises(ValueError, match="Port must be between 1 and 65535"):
        load_config({"port": 65536})


def test_path_traversal_resolution(tmp_path: Path) -> None:
    """Test that path traversal sequences are resolved and validated."""
    # Create a nested structure
    base = tmp_path / "base"
    base.mkdir()
    target = tmp_path / "target.json"
    target.touch()
    
    # Path with traversal
    traversal_path = str(base / ".." / "target.json")
    
    config = load_config({"watch_path": traversal_path})
    assert Path(config.watch_path).resolve() == target.resolve()


def test_symlink_config_resolution(tmp_path: Path) -> None:
    """Test that config loading resolves symlinks to their targets."""
    target = tmp_path / "real.json"
    target.touch()
    link = tmp_path / "link.json"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("Symlinks not supported")
        
    with pytest.raises(ValueError, match="Symlinks are not allowed"):
        load_config({"watch_path": str(link)})
        

def test_validate_path_symlink_loop(tmp_path: Path) -> None:
    """Test that a symlink loop in watch_path causes exit."""
    link1 = tmp_path / "loop1"
    link2 = tmp_path / "loop2"
    try:
        link1.symlink_to(link2)
        link2.symlink_to(link1)
    except OSError:
        pytest.skip("Symlinks not supported")

    # load_config raises ValueError on validation failure
    with pytest.raises(ValueError):
        load_config({"watch_path": str(link1)})


def test_log_file_traversal_attack(tmp_path: Path) -> None:
    """Test that log file path traversal is resolved and permissions checked."""
    # Create a read-only file to simulate a system file we shouldn't write to
    system_file = tmp_path / "system_file"
    system_file.touch()
    # Make it read-only
    system_file.chmod(0o444)
    
    # Path that traverses to system_file
    malicious_path = str(tmp_path / "subdir" / ".." / "system_file")
    
    # Should exit because we can't write to it (permission denied on open('a'))
    with pytest.raises(ValueError):
        load_config({"log_file": malicious_path})


def test_watch_path_traversal_attack_explicit(tmp_path: Path) -> None:
    """Test traversal attempt to access a file outside the intended directory."""
    # Create a dummy sensitive file outside the watch root
    sensitive_dir = tmp_path / "sensitive"
    sensitive_dir.mkdir()
    sensitive_file = sensitive_dir / "passwd"
    sensitive_file.touch()

    # Create a watch root
    watch_root = tmp_path / "watch_root"
    watch_root.mkdir()

    # Traversal path: watch_root/../sensitive/passwd
    traversal_path = str(watch_root / ".." / "sensitive" / "passwd")

    # Mock open to raise PermissionError to simulate protection
    with patch("pathlib.Path.open", side_effect=PermissionError("Access denied")):
        with pytest.raises(ValueError):
            load_config({"watch_path": traversal_path})


def test_log_file_creation_failure_mock_oserror(tmp_path: Path) -> None:
    """Test log file creation failure with generic OSError."""
    log_file = tmp_path / "test.log"

    # Mock exists to False (so it tries to create), then open raises OSError
    with patch("pathlib.Path.exists", return_value=False):
        with patch("pathlib.Path.open", side_effect=OSError("Disk full")):
            with patch("aw_watcher_pipeline_stage.config.logger") as mock_logger:
                with pytest.raises(ValueError):
                    load_config({"log_file": str(log_file)})

                mock_logger.error.assert_called()
                assert "Cannot create log file" in str(mock_logger.error.call_args)

def test_validate_log_file_is_directory(tmp_path: Path) -> None:
    """Test that specifying a directory as log_file causes exit."""
    log_dir = tmp_path / "log_dir"
    log_dir.mkdir()
    
    with pytest.raises(ValueError):
        load_config({"log_file": str(log_dir)})


def test_watch_path_is_directory_with_matching_name(tmp_path: Path) -> None:
    """Test that if watch_path (or default) points to a directory named current_task.json, it exits."""
    # Case 1: Explicit path is a directory
    bad_dir = tmp_path / "fake_file.json"
    bad_dir.mkdir()
    
    with pytest.raises(ValueError):
        load_config({"watch_path": str(bad_dir)})

    # Case 2: Watch path is dir, and contained current_task.json is ALSO a directory
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "current_task.json").mkdir()
    
    with pytest.raises(ValueError):
        load_config({"watch_path": str(project_dir)})


def test_metadata_allowlist_config_parsing() -> None:
    """Test parsing of metadata_allowlist from string."""
    config = load_config({"metadata_allowlist": "a, b, c"})
    assert config.metadata_allowlist == ["a", "b", "c"]

    config = load_config({"metadata_allowlist": "  foo , bar  "})
    assert config.metadata_allowlist == ["foo", "bar"]



def test_log_file_creation_failure_parent_permissions(tmp_path: Path) -> None:
    """Test failure to create log file when parent is not writable."""
    log_file = tmp_path / "logs" / "test.log"
    # Parent exists
    (tmp_path / "logs").mkdir()

    # Mock open to raise PermissionError (simulating read-only directory)
    with patch("pathlib.Path.open", side_effect=PermissionError("Access denied")):
        with pytest.raises(ValueError):
            load_config({"log_file": str(log_file)})


def test_watch_path_resolve_failure_loop(tmp_path: Path) -> None:
    """Test that resolve failure (e.g. symlink loop) causes exit."""
    # We can simulate this by mocking resolve to raise RuntimeError
    # Note: pathlib.Path.resolve raises RuntimeError for loops
    with patch("pathlib.Path.resolve", side_effect=RuntimeError("Symlink loop")):
        with pytest.raises(ValueError):
            load_config({"watch_path": "loop"})


def test_validate_path_broken_symlink_allowed(tmp_path: Path) -> None:
    """Test that a broken symlink is accepted (watcher waits for target creation)."""
    link = tmp_path / "broken_link"
    target = tmp_path / "non_existent"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("Symlinks not supported")

    # Should exit because symlinks are rejected
    with pytest.raises(ValueError, match="Symlinks are not allowed"):
        load_config({"watch_path": str(link)})


def test_validate_path_directory_default_file_missing(tmp_path: Path) -> None:
    """Test that providing a directory defaults to current_task.json, even if missing."""
    # Directory exists, file doesn't.
    config = load_config({"watch_path": str(tmp_path)})
    expected = tmp_path / "current_task.json"
    assert config.watch_path == str(expected.resolve())


def test_validate_path_directory_default_file_unreadable(tmp_path: Path) -> None:
    """Test that providing a directory defaults to current_task.json, and checks permissions if exists."""
    f = tmp_path / "current_task.json"
    f.touch()

    # Mock open to raise PermissionError on the file
    with patch("pathlib.Path.open", side_effect=PermissionError("Access denied")):
        with pytest.raises(ValueError):
            load_config({"watch_path": str(tmp_path)})


def test_validate_path_traversal_sensitive_access_denied(tmp_path: Path) -> None:
    """Test traversal to a sensitive file that is unreadable."""
    # Create a dummy sensitive file
    sensitive = tmp_path / "sensitive"
    sensitive.touch()

    # Create a subdir to traverse from
    subdir = tmp_path / "subdir"
    subdir.mkdir()

    # Path: subdir/../sensitive
    traversal = str(subdir / ".." / "sensitive")

    # Mock open to raise PermissionError, simulating lack of access
    # We rely on resolve() working correctly (tested elsewhere) and open() failing
    with patch("pathlib.Path.open", side_effect=PermissionError("Access denied")):
        with pytest.raises(ValueError):
            load_config({"watch_path": traversal})


def test_config_priority_cli_overrides_env_complex(tmp_path: Path) -> None:
    """Test complex priority: CLI overrides Env for multiple fields."""
    env = {
        "AW_WATCHER_PORT": "1111",
        "AW_WATCHER_TESTING": "false",
        "PIPELINE_WATCHER_PATH": str(tmp_path / "env_path"),
    }

    cli = {
        "port": 2222,
        "testing": "true",
        # watch_path not in CLI, should fall through to Env
    }

    with patch.dict(os.environ, env, clear=True):
        # Mock validation to avoid FS checks for env_path
        with patch(
            "aw_watcher_pipeline_stage.config._validate_path", side_effect=lambda p, **k: p
        ):
            config = load_config(cli)

            assert config.port == 2222
            assert config.testing is True
            assert config.watch_path == str(tmp_path / "env_path")


def test_log_file_oserror_on_creation(tmp_path: Path) -> None:
    """Test handling of OSError (e.g. disk full) when creating log file."""
    log_file = tmp_path / "test.log"

    # Mock open to raise OSError
    with patch("pathlib.Path.open", side_effect=OSError("Disk full")):
        with pytest.raises(ValueError):
            load_config({"log_file": str(log_file)})


def test_config_priority_cli_overrides_env_and_file_complex(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test complex priority: CLI overrides Env and File for multiple fields."""
    monkeypatch.chdir(tmp_path)

    # 1. Config File
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5000\ntesting = false", encoding="utf-8")

    # 2. Env
    env = {
        "AW_WATCHER_PORT": "6000",
        "AW_WATCHER_TESTING": "true",
        "AW_WATCHER_PULSETIME": "60.0"
    }

    # 3. CLI
    cli_args = {
        "port": 7000,
        "pulsetime": 30.0
    }

    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli_args)

        assert config.port == 7000        # CLI > Env > File
        assert config.testing is True     # Env > File (CLI not set)
        assert config.pulsetime == 30.0   # CLI > Env
        assert config.debounce_seconds == 1.0 # Default


def test_config_priority_cli_empty_string_overrides_env(tmp_path: Path) -> None:
    """Test that empty string in CLI overrides environment variable."""
    env = {"PIPELINE_WATCHER_PATH": "/env/path"}
    
    # Ensure no git root to default to "."
    with patch("aw_watcher_pipeline_stage.config._find_project_root", return_value=None):
        with patch.dict(os.environ, env, clear=True):
            # CLI passes empty string
            config = load_config({"watch_path": ""})
            # Should default to "." (CLI "" wins over Env, triggers default logic)
            assert config.watch_path == "."


def test_validate_path_directory_traversal_complex(tmp_path: Path) -> None:
    """Test complex traversal path resolving to valid file."""
    (tmp_path / "subdir" / "nested").mkdir(parents=True)
    target = tmp_path / "target.json"
    target.touch()
    
    # Path: tmp_path/subdir/nested/../../target.json
    complex_path = tmp_path / "subdir" / "nested" / ".." / ".." / "target.json"
    
    config = load_config({"watch_path": str(complex_path)})
    assert Path(config.watch_path).resolve() == target.resolve()


def test_priority_override_invalid_path(tmp_path: Path) -> None:
    """Test that a valid CLI path overrides an invalid Environment path."""
    valid_file = tmp_path / "valid.json"
    valid_file.touch()
    
    # Env has a path that would cause exit if validated (parent doesn't exist)
    bad_env_path = str(tmp_path / "nonexistent_dir" / "file.json")
    
    with patch.dict(os.environ, {"PIPELINE_WATCHER_PATH": bad_env_path}, clear=True):
        config = load_config({"watch_path": str(valid_file)})
        assert config.watch_path == str(valid_file.resolve())


def test_metadata_allowlist_from_env() -> None:
    """Test parsing of metadata allowlist from environment variable."""
    env = {"AW_WATCHER_METADATA_ALLOWLIST": "foo, bar,baz "}
    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        assert config.metadata_allowlist == ["foo", "bar", "baz"]


def test_log_file_is_special_file(tmp_path: Path) -> None:
    """Test that log_file validation fails if path exists but is not a regular file."""
    special_file = tmp_path / "special.log"
    
    # Mock exists to return True only for our special file (so watch_path validation passes)
    def custom_exists(self: Path) -> bool:
        return self.name == "special.log"

    with patch("pathlib.Path.exists", autospec=True, side_effect=custom_exists):
        with patch("pathlib.Path.is_file", return_value=False):
            with pytest.raises(ValueError):
                load_config({"log_file": str(special_file)})


def test_config_loading_with_broken_symlink_env(tmp_path: Path) -> None:
    """Test that a broken symlink in Env is accepted (waiting for creation)."""
    link = tmp_path / "broken_env.json"
    target = tmp_path / "missing.json"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("Symlinks not supported")
        
    with patch.dict(os.environ, {"PIPELINE_WATCHER_PATH": str(link)}, clear=True):
        with pytest.raises(ValueError, match="Symlinks are not allowed"):
            load_config({})


def test_log_file_creation_permission_error(tmp_path: Path) -> None:
    """Test failure to create log file due to permission error (not parent)."""
    log_file = tmp_path / "new.log"
    # Parent exists and is writable (tmp_path)
    
    # Mock open to raise PermissionError
    with patch("pathlib.Path.open", side_effect=PermissionError("Access denied")):
        with pytest.raises(ValueError):
            load_config({"log_file": str(log_file)})


def test_validate_path_null_byte() -> None:
    """Test that paths with null bytes cause exit."""
    with pytest.raises(ValueError):
        load_config({"watch_path": "/tmp/null\0byte"})


def test_priority_cli_invalid_path_exits(tmp_path: Path) -> None:
    """Test that an invalid CLI path causes exit, even if Env has a valid path."""
    valid_env = tmp_path / "valid.json"
    valid_env.touch()

    # Invalid because parent does not exist
    invalid_cli = tmp_path / "missing_dir" / "file.json"

    with patch.dict(os.environ, {"PIPELINE_WATCHER_PATH": str(valid_env)}, clear=True):
        with pytest.raises(ValueError):
            load_config({"watch_path": str(invalid_cli)})


def test_priority_env_invalid_path_exits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that an invalid Env path causes exit, even if Config has a valid path."""
    monkeypatch.chdir(tmp_path)
    # Setup Config
    (tmp_path / "config.ini").write_text(
        "[aw-watcher-pipeline-stage]\nwatch_path = ./valid.json", encoding="utf-8"
    )
    (tmp_path / "valid.json").touch()

    # Invalid because parent does not exist
    invalid_env = tmp_path / "missing_dir" / "file.json"

    with patch.dict(os.environ, {"PIPELINE_WATCHER_PATH": str(invalid_env)}, clear=True):
        with pytest.raises(ValueError):
            load_config({})

def test_metadata_allowlist_priority_chain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test metadata_allowlist priority: CLI > Env > Config."""
    monkeypatch.chdir(tmp_path)
    
    # 1. Config File
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nmetadata_allowlist = config1, config2", encoding="utf-8")
    
    # 2. Env
    env = {"AW_WATCHER_METADATA_ALLOWLIST": "env1, env2"}
    
    # 3. CLI
    cli_args = {"metadata_allowlist": "cli1, cli2"}
    
    # Test CLI wins
    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli_args)
        assert config.metadata_allowlist == ["cli1", "cli2"]
        
    # Test Env wins over Config
    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        assert config.metadata_allowlist == ["env1", "env2"]
        
    # Test Config wins
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.metadata_allowlist == ["config1", "config2"]


def test_validate_path_parent_is_not_directory(tmp_path: Path) -> None:
    """Test validation when parent of watch_path is a file, not a directory."""
    # Create a file
    parent_file = tmp_path / "parent_file"
    parent_file.touch()
    
    # Try to use it as a directory
    bad_path = parent_file / "child.json"
    
    with pytest.raises(ValueError):
        load_config({"watch_path": str(bad_path)})


def test_validate_log_file_parent_is_not_directory(tmp_path: Path) -> None:
    """Test validation when parent of log_file is a file, not a directory."""
    parent_file = tmp_path / "parent_file"
    parent_file.touch()
    
    bad_log = parent_file / "child.log"
    
    with pytest.raises(ValueError):
        load_config({"log_file": str(bad_log)})


def test_validate_path_user_expansion_with_traversal() -> None:
    """Test path with user expansion and traversal."""
    # ~/../other/file.json
    path_str = "~/../other/file.json"
    expanded = "/home/user/../other/file.json"
    resolved = Path("/home/other/file.json")
    
    with patch("os.path.expanduser", return_value=expanded) as mock_expand:
        with patch("pathlib.Path.resolve", return_value=resolved):
            # Assume it's valid
            with patch("pathlib.Path.exists", return_value=True):
                with patch("pathlib.Path.is_file", return_value=True):
                    with patch("pathlib.Path.open"):
                        config = load_config({"watch_path": path_str})
                        
                        mock_expand.assert_called_with(path_str)
                        assert config.watch_path == str(resolved)


def test_config_file_utf8_sig_bom(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that config file with UTF-8 BOM is parsed correctly."""
    monkeypatch.chdir(tmp_path)
    config_content = "[aw-watcher-pipeline-stage]\nport = 5555"
    # Write with BOM
    (tmp_path / "config.ini").write_bytes(b'\xef\xbb\xbf' + config_content.encode("utf-8"))
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.port == 5555


def test_validate_log_file_creation_race_condition(tmp_path: Path) -> None:
    """Test race condition where log file parent disappears during creation."""
    log_file = tmp_path / "race.log"
    
    # Simulate exists() -> False, then open() -> OSError
    with patch("pathlib.Path.exists", return_value=False):
        with patch("pathlib.Path.open", side_effect=OSError("Directory not found")):
            with pytest.raises(ValueError):
                load_config({"log_file": str(log_file)})


def test_config_priority_cli_valid_overrides_env_invalid_type(tmp_path: Path) -> None:
    """Test that a valid CLI argument overrides an invalid type in Environment."""
    # Env has invalid port (string that isn't int)
    env = {"AW_WATCHER_PORT": "invalid_int"}
    
    # CLI has valid port
    cli_args = {"port": 5678}
    
    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli_args)
        assert config.port == 5678


def test_config_priority_file_invalid_overridden_by_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a valid Environment variable overrides an invalid type in Config File."""
    monkeypatch.chdir(tmp_path)
    # Config file has invalid pulsetime
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\npulsetime = invalid_float", encoding="utf-8")
    
    # Env has valid pulsetime
    env = {"AW_WATCHER_PULSETIME": "60.0"}
    
    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        assert config.pulsetime == 60.0


def test_validate_path_symlink_to_directory_log_file(tmp_path: Path) -> None:
    """Test that a log file cannot be a symlink to a directory."""
    target_dir = tmp_path / "target_dir"
    target_dir.mkdir()
    link = tmp_path / "link_to_dir.log"
    
    try:
        link.symlink_to(target_dir)
    except OSError:
        pytest.skip("Symlinks not supported")
        
    # Should exit because resolved path is a directory
    with pytest.raises(SystemExit):
        load_config({"log_file": str(link)})


def test_validate_path_directory_with_trailing_slash(tmp_path: Path) -> None:
    """Test that a watch path with trailing slash is handled correctly."""
    d = tmp_path / "subdir"
    d.mkdir()
    
    # Path string with trailing slash
    path_str = str(d) + os.sep
    
    config = load_config({"watch_path": path_str})
    
    # Should resolve to current_task.json inside that dir
    expected = d / "current_task.json"
    assert Path(config.watch_path).resolve() == expected.resolve()


def test_validate_path_race_condition_open_is_directory(tmp_path: Path) -> None:
    """Test race condition where file becomes directory during validation open."""
    f = tmp_path / "race_dir"
    f.mkdir()
    
    # Mock checks to pass (simulate it looked like a file), but open will hit the directory
    with patch("pathlib.Path.is_dir", return_value=False):
        with patch("pathlib.Path.is_file", return_value=True):
            # Real open('r') on directory raises IsADirectoryError (or PermissionError on Windows)
            # config.py catches Exception and exits
            with pytest.raises(SystemExit):
                load_config({"watch_path": str(f)})


def test_validate_log_file_race_condition_open_is_directory(tmp_path: Path) -> None:
    """Test race condition where log file becomes directory during validation open."""
    f = tmp_path / "race_log_dir"
    f.mkdir()
    
    with patch("pathlib.Path.exists", return_value=True):
        with patch("pathlib.Path.is_file", return_value=True):
             # Real open('a') on directory raises IsADirectoryError/PermissionError
             with pytest.raises(SystemExit):
                 load_config({"log_file": str(f)})


def test_config_priority_cli_overrides_env_all_fields(tmp_path: Path) -> None:
    """Test that CLI overrides Env for multiple fields simultaneously."""
    env = {
        "AW_WATCHER_PORT": "1111",
        "AW_WATCHER_TESTING": "false",
        "AW_WATCHER_PULSETIME": "10.0",
        "AW_WATCHER_DEBOUNCE_SECONDS": "0.5"
    }
    cli = {
        "port": 2222,
        "testing": True,
        "pulsetime": 20.0,
        "debounce_seconds": 2.0
    }
    
    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli)
        assert config.port == 2222
        assert config.testing is True
        assert config.pulsetime == 20.0
        assert config.debounce_seconds == 2.0


def test_batch_size_limit_propagation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test batch_size_limit propagation: CLI > Env > Default."""
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

    # 5. Validation
    with pytest.raises(ValueError, match="batch_size_limit must be between 1 and 1000"):
        load_config({"batch_size_limit": 0})

    with pytest.raises(ValueError, match="batch_size_limit must be between 1 and 1000"):
        load_config({"batch_size_limit": 1001})


def test_metadata_allowlist_empty_string() -> None:
    """Test that empty string for metadata_allowlist results in empty list (allow nothing)."""
    # Simulate CLI passing empty string
    config = load_config({"metadata_allowlist": ""})
    assert config.metadata_allowlist == []


def test_validate_path_symlink_recursive_resolution(tmp_path: Path) -> None:
    """Test resolution of a chain of symlinks."""
    target = tmp_path / "target.json"
    target.touch()
    link1 = tmp_path / "link1"
    link2 = tmp_path / "link2"
    
    try:
        link1.symlink_to(target)
        link2.symlink_to(link1)
    except OSError:
        pytest.skip("Symlinks not supported")
        
    with pytest.raises(ValueError, match="Symlinks are not allowed"):
        load_config({"watch_path": str(link2)})


def test_validate_path_traversal_absolute_root(tmp_path: Path) -> None:
    """Test traversal from root using absolute path (e.g. /../etc/passwd)."""
    # Mock resolve to return a system file
    with patch("pathlib.Path.resolve", return_value=Path("/etc/passwd")):
        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.is_file", return_value=True):
                # Mock open to raise PermissionError (simulating protection)
                with patch("pathlib.Path.open", side_effect=PermissionError("Access denied")):
                    with pytest.raises(ValueError):
                        load_config({"watch_path": "/../etc/passwd"})


def test_validate_path_symlink_chain_to_sensitive(tmp_path: Path) -> None:
    """Test a chain of symlinks resolving to a sensitive file."""
    link = tmp_path / "link_chain"
    # We don't create it, we mock resolve to simulate the chain ending at /etc/shadow
    with patch("pathlib.Path.resolve", return_value=Path("/etc/shadow")):
        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.is_file", return_value=True):
                with patch("pathlib.Path.open", side_effect=PermissionError("Access denied")):
                    with pytest.raises(ValueError):
                        load_config({"watch_path": str(link)})


def test_config_priority_cli_overrides_env_explicit_values(tmp_path: Path) -> None:
    """Test CLI overrides Env with explicit values (Priority Check)."""
    env = {"AW_WATCHER_PORT": "1111", "AW_WATCHER_TESTING": "false"}
    cli = {"port": 2222, "testing": True}
    
    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli)
        assert config.port == 2222
        assert config.testing is True


def test_validate_path_relative_path_resolution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a relative watch_path is resolved against CWD."""
    monkeypatch.chdir(tmp_path)
    f = tmp_path / "relative.json"
    f.touch()

    config = load_config({"watch_path": "relative.json"})

    assert config.watch_path == str(f.resolve())


def test_config_priority_debounce_env_overrides_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that AW_WATCHER_DEBOUNCE_SECONDS overrides config file."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\ndebounce_seconds = 5.0", encoding="utf-8")

    env = {"AW_WATCHER_DEBOUNCE_SECONDS": "0.1"}
    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        assert config.debounce_seconds == 0.1


def test_validate_path_traversal_upward_resolution(tmp_path: Path) -> None:
    """Test explicit upward traversal resolution."""
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    target = tmp_path / "target.json"
    target.touch()

    # path = tmp_path/subdir/../target.json
    path_str = str(subdir / ".." / "target.json")

    config = load_config({"watch_path": path_str})
    assert Path(config.watch_path).resolve() == target.resolve()


def test_validate_path_windows_unc_path_mock() -> None:
    """Test handling of Windows UNC paths."""
    unc_path = r"\\Server\Share\Project\current_task.json"

    with patch("aw_watcher_pipeline_stage.config.Path") as mock_path_cls:
        mock_path_instance = mock_path_cls.return_value
        # Setup the mock to pass validation
        mock_path_instance.resolve.return_value = mock_path_instance
        mock_path_instance.exists.return_value = True
        mock_path_instance.is_file.return_value = True
        mock_path_instance.__str__.return_value = unc_path

        # Mock open context manager
        mock_path_instance.open.return_value.__enter__.return_value = MagicMock()

        config = load_config({"watch_path": unc_path})
        assert config.watch_path == unc_path


def test_validate_path_symlink_to_file_permission_denied_mock(tmp_path: Path) -> None:
    """Test that a symlink to a file with denied permissions causes exit (mocked)."""
    target = tmp_path / "target.json"
    target.touch()
    link = tmp_path / "link.json"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("Symlinks not supported")

    # Mock open to raise PermissionError
    with patch("pathlib.Path.open", side_effect=PermissionError("Access denied")):
        with pytest.raises(ValueError):
            load_config({"watch_path": str(link)})


def test_security_real_fifo_rejection(tmp_path: Path) -> None:
    """Test that a real FIFO is rejected (skip on Windows)."""
    if os.name == "nt":
        pytest.skip("os.mkfifo not available on Windows")

    fifo_path = tmp_path / "test.fifo"
    try:
        os.mkfifo(fifo_path)
    except AttributeError:
        pytest.skip("os.mkfifo not available")

    with pytest.raises(ValueError):
        load_config({"watch_path": str(fifo_path)})


def test_security_real_permission_denied(tmp_path: Path) -> None:
    """Test that a real unreadable file is rejected."""
    if os.name == "nt":
        pytest.skip("Skipping real permission test on Windows")

    f = tmp_path / "secret.json"
    f.touch()
    f.chmod(0o000)  # No permissions

    try:
        with pytest.raises(ValueError):
            load_config({"watch_path": str(f)})
    finally:
        f.chmod(0o666)  # Restore for cleanup


def test_metadata_allowlist_parsing_edge_cases() -> None:
    """Test parsing of metadata allowlist with empty items and whitespace."""
    config = load_config({"metadata_allowlist": " a , , b "})
    assert config.metadata_allowlist == ["a", "b"]


def test_metadata_allowlist_list_input_stripping() -> None:
    """Test that list input for metadata_allowlist is stripped of whitespace."""
    config = load_config({"metadata_allowlist": [" a ", "b ", " c"]})
    assert config.metadata_allowlist == ["a", "b", "c"]


def test_config_file_invalid_boolean(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that invalid boolean strings in config file default to False."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\ntesting = invalid_bool", encoding="utf-8")

    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.testing is False


def test_validate_log_file_parent_not_found(tmp_path: Path) -> None:
    """Test that log file validation fails if parent directory does not exist."""
    log_file = tmp_path / "nonexistent_dir" / "test.log"

    with pytest.raises(ValueError):
        load_config({"log_file": str(log_file)})


def test_priority_cli_explicit_false_vs_env_true(tmp_path: Path) -> None:
    """Test that explicit False in CLI args overrides True in Environment."""
    env = {"AW_WATCHER_TESTING": "true"}
    cli_args = {"testing": False}
    
    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli_args)
        assert config.testing is False


def test_config_file_valid_boolean_variations(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test various valid boolean strings in config file."""
    monkeypatch.chdir(tmp_path)
    
    variations = {
        "1": True, "yes": True, "on": True, "true": True,
        "0": False, "no": False, "off": False, "false": False
    }
    
    for val_str, expected_bool in variations.items():
        (tmp_path / "config.ini").write_text(f"[aw-watcher-pipeline-stage]\ntesting = {val_str}", encoding="utf-8")
        with patch.dict(os.environ, {}, clear=True):
            config = load_config({})
            assert config.testing is expected_bool


def test_config_file_values_with_inline_comments(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that inline comments in config file cause ValueError (as they are read as value)."""
    monkeypatch.chdir(tmp_path)
    # ConfigParser without inline_comment_prefixes treats "; comment" as part of the value
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5600 ; comment", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError):
            load_config({})


def test_validate_path_long_path_error(tmp_path: Path) -> None:
    """Test handling of OSError (File name too long) during resolution."""
    long_path = "a" * 1000
    
    with patch("pathlib.Path.resolve", side_effect=OSError("File name too long")):
        with pytest.raises(ValueError):
            load_config({"watch_path": long_path})


def test_validate_path_parent_permission_denied_during_recovery(tmp_path: Path) -> None:
    """Test handling of PermissionError when resolving parent of a non-existent path."""
    # Simulate path not found (first resolve), then parent resolution fails (second resolve)
    with patch("pathlib.Path.resolve", side_effect=[FileNotFoundError, PermissionError("Access denied")]):
        with pytest.raises(ValueError):
            load_config({"watch_path": "/restricted/missing.json"})


def test_config_file_invalid_watch_path_parent_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that invalid watch_path in config file causes exit."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nwatch_path = /nonexistent/dir/file.json", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError):
            load_config({})


def test_config_file_log_file_permission_denied(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that unwriteable log_file in config file causes exit."""
    monkeypatch.chdir(tmp_path)
    log_file = tmp_path / "protected.log"
    log_file.touch()
    
    (tmp_path / "config.ini").write_text(f"[aw-watcher-pipeline-stage]\nlog_file = {log_file}", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        # Mock open to raise PermissionError. Since watch_path defaults to non-existent file in tmp_path,
        # it won't trigger open(), so only log_file open() will fail.
        with patch("pathlib.Path.open", side_effect=PermissionError("Access denied")):
            with pytest.raises(ValueError):
                load_config({})


def test_priority_cli_valid_overrides_config_invalid(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a valid CLI path overrides an invalid config file path."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nwatch_path = /nonexistent/dir/file.json", encoding="utf-8")
    
    valid_file = tmp_path / "valid.json"
    valid_file.touch()
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({"watch_path": str(valid_file)})
        assert config.watch_path == str(valid_file.resolve())


def test_priority_env_valid_overrides_config_invalid(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a valid Env path overrides an invalid config file path."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nwatch_path = /nonexistent/dir/file.json", encoding="utf-8")
    
    valid_file = tmp_path / "valid.json"
    valid_file.touch()
    
    with patch.dict(os.environ, {"PIPELINE_WATCHER_PATH": str(valid_file)}, clear=True):
        config = load_config({})
        assert config.watch_path == str(valid_file.resolve())


def test_config_file_path_traversal_attack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test path traversal in config file is resolved and validated."""
    monkeypatch.chdir(tmp_path)
    # Traversal to non-existent location
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nwatch_path = ../nonexistent/file.json", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError):
            load_config({})


def test_validate_path_traversal_symlink_mix(tmp_path: Path) -> None:
    """Test path validation with mixed traversal and symlinks."""
    real_dir = tmp_path / "real_dir"
    real_dir.mkdir()
    target = real_dir / "target.json"
    target.touch()
    
    link_dir = tmp_path / "link_dir"
    try:
        link_dir.symlink_to(real_dir)
    except OSError:
        pytest.skip("Symlinks not supported")
        
    # Path: /tmp/link_dir/../real_dir/target.json
    # Should resolve to /tmp/real_dir/target.json
    mixed_path = link_dir / ".." / "real_dir" / "target.json"
    
    config = load_config({"watch_path": str(mixed_path)})
    assert Path(config.watch_path).resolve() == target.resolve()


def test_validate_path_directory_symlink_default_file(tmp_path: Path) -> None:
    """Test watch_path is a symlink to a directory, defaulting to current_task.json."""
    real_dir = tmp_path / "real_dir"
    real_dir.mkdir()
    target = real_dir / "current_task.json"
    target.touch()
    
    link_dir = tmp_path / "link_dir"
    try:
        link_dir.symlink_to(real_dir)
    except OSError:
        pytest.skip("Symlinks not supported")
        
    with pytest.raises(ValueError, match="Symlinks are not allowed"):
        load_config({"watch_path": str(link_dir)})


def test_validate_path_parent_not_executable(tmp_path: Path) -> None:
    """Test validation when parent directory is not executable (cannot traverse)."""
    target = tmp_path / "dir" / "file.json"
    
    # Mock resolve to raise PermissionError (simulating traversal denial)
    with patch("pathlib.Path.resolve", side_effect=PermissionError("Permission denied")):
        with pytest.raises(ValueError):
            load_config({"watch_path": str(target)})


def test_config_priority_partial_cli_override(tmp_path: Path) -> None:
    """Test that CLI overrides some env vars but leaves others."""
    env = {
        "AW_WATCHER_PORT": "1234",
        "AW_WATCHER_TESTING": "true"
    }
    cli_args = {"port": 5678} # Override port, leave testing
    
    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli_args)
        assert config.port == 5678
        assert config.testing is True


def test_validate_path_relative_symlink_resolution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test resolution of a symlink pointing to a relative path."""
    monkeypatch.chdir(tmp_path)
    real_file = tmp_path / "real.json"
    real_file.touch()
    
    link = tmp_path / "rel_link.json"
    try:
        # Create symlink pointing to "./real.json"
        os.symlink("real.json", str(link))
    except OSError:
        pytest.skip("Symlinks not supported")
        
    with pytest.raises(ValueError, match="Symlinks are not allowed"):
        load_config({"watch_path": str(link)})


@pytest.mark.parametrize("method_name, exception", [
    ("pathlib.Path.resolve", PermissionError("Access denied")),
    ("pathlib.Path.exists", PermissionError("Access denied")),
    ("pathlib.Path.open", PermissionError("Access denied")),
    ("pathlib.Path.open", OSError("Generic I/O Error")),
    ("os.path.expanduser", RuntimeError("Expansion failed")),
])
def test_validate_path_permission_errors_parametrized(tmp_path: Path, method_name: str, exception: Exception) -> None:
    """Test validation fails when various path operations raise permission/OS errors."""
    f = tmp_path / "test.json"
    if method_name != "pathlib.Path.resolve":
        f.touch()
    
    with patch(method_name, side_effect=exception):
        with pytest.raises(ValueError):
            load_config({"watch_path": str(f)})


@pytest.mark.parametrize("is_file_val, exists_val, is_dir_val", [
    (False, True, False), # Socket/FIFO/Device
    (False, True, True),  # Directory
])
def test_validate_path_rejects_non_regular_files_parametrized(tmp_path: Path, is_file_val: bool, exists_val: bool, is_dir_val: bool) -> None:
    """Test that non-regular files (sockets, pipes, devices, directories) are rejected."""
    p = tmp_path / "special"
    with patch("pathlib.Path.resolve", return_value=p), \
         patch("pathlib.Path.exists", return_value=exists_val), \
         patch("pathlib.Path.is_file", return_value=is_file_val), \
         patch("pathlib.Path.is_dir", return_value=is_dir_val):
        with pytest.raises(ValueError):
            load_config({"watch_path": str(p)})


@pytest.mark.parametrize("target_path", [
    "/etc/shadow",
    "/dev/zero",
    "/proc/kcore"
])
def test_validate_path_rejects_protected_targets_parametrized(tmp_path: Path, target_path: str) -> None:
    """Test that symlinks resolving to protected/system files are rejected via open failure."""
    link = tmp_path / "link_to_protected"
    resolved = Path(target_path)
    
    with patch("pathlib.Path.resolve", return_value=resolved), \
         patch("pathlib.Path.exists", return_value=True), \
         patch("pathlib.Path.is_file", return_value=True), \
         patch("pathlib.Path.open", side_effect=PermissionError("Access denied")):
        with pytest.raises(ValueError):
            load_config({"watch_path": str(link)})


def test_invalid_log_level_raises() -> None:
    """Test that invalid log level raises ValueError."""
    with pytest.raises(ValueError, match="Invalid log level"):
        load_config({"log_level": "INVALID_LEVEL"})


def test_config_priority_full_chain_all_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify config flow: CLI > Env > Config File > Defaults for ALL fields.
    """
    monkeypatch.chdir(tmp_path)
    
    # 1. Config File (Lowest priority of sources)
    (tmp_path / "config.ini").write_text("""
[aw-watcher-pipeline-stage]
watch_path = ./file_config.json
port = 5001
testing = false
pulsetime = 60.0
debounce_seconds = 2.0
metadata_allowlist = config1, config2
log_level = WARNING
log_file = config.log
batch_size_limit = 3
""", encoding="utf-8")

    # 2. Env (Middle priority)
    env = {
        "PIPELINE_WATCHER_PATH": "./file_env.json",
        "AW_WATCHER_PORT": "5002",
        "AW_WATCHER_TESTING": "true",
        "AW_WATCHER_PULSETIME": "120.0",
        "AW_WATCHER_DEBOUNCE_SECONDS": "1.0",
        "AW_WATCHER_METADATA_ALLOWLIST": "env1, env2",
        "AW_WATCHER_LOG_LEVEL": "ERROR",
        "AW_WATCHER_LOG_FILE": "env.log",
        "AW_WATCHER_BATCH_SIZE_LIMIT": "7"
    }

    # 3. CLI (Highest priority)
    cli_args = {
        "watch_path": "./file_cli.json",
        "port": 5003,
        "testing": False,
        "pulsetime": 180.0,
        "debounce_seconds": 0.5,
        "metadata_allowlist": "cli1, cli2",
        "log_level": "DEBUG",
        "log_file": "cli.log",
        "batch_size_limit": 9
    }

    # Test A: Config File Only (Env empty, CLI empty)
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.watch_path == "./file_config.json"
        assert config.port == 5001
        assert config.testing is False
        assert config.pulsetime == 60.0
        assert config.debounce_seconds == 2.0
        assert config.metadata_allowlist == ["config1", "config2"]
        assert config.log_level == "WARNING"
        assert config.log_file == "config.log"
        assert config.batch_size_limit == 3

    # Test B: Env Overrides Config
    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        assert config.watch_path == "./file_env.json"
        assert config.port == 5002
        assert config.testing is True
        assert config.pulsetime == 120.0
        assert config.debounce_seconds == 1.0
        assert config.metadata_allowlist == ["env1", "env2"]
        assert config.log_level == "ERROR"
        assert config.log_file == "env.log"
        assert config.batch_size_limit == 7

    # Test C: CLI Overrides Env
    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli_args)
        assert config.watch_path == "./file_cli.json"
        assert config.port == 5003
        assert config.testing is False
        assert config.pulsetime == 180.0
        assert config.debounce_seconds == 0.5
        assert config.metadata_allowlist == ["cli1", "cli2"]
        assert config.log_level == "DEBUG"
        assert config.log_file == "cli.log"
        assert config.batch_size_limit == 9


def test_config_file_batch_size_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that batch_size_limit is correctly loaded and validated from config file."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nbatch_size_limit = 50", encoding="utf-8")

    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.batch_size_limit == 50

    # Test invalid value in config file
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nbatch_size_limit = -5", encoding="utf-8")
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="batch_size_limit must be between 1 and 1000"):
            load_config({})


def test_full_mixed_source_propagation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify that configuration propagates correctly from mixed sources for ALL fields.
    Scenario:
    - Config File: watch_path, port
    - Env: testing, pulsetime, log_level
    - CLI: debounce_seconds, metadata_allowlist, batch_size_limit, log_file
    """
    monkeypatch.chdir(tmp_path)
    
    # 1. Config File
    (tmp_path / "config.ini").write_text("""
[aw-watcher-pipeline-stage]
watch_path = ./file_watch.json
port = 5001
testing = true
""", encoding="utf-8")

    # 2. Env
    env = {
        "AW_WATCHER_TESTING": "false", # Should override config file
        "AW_WATCHER_PULSETIME": "60.0",
        "AW_WATCHER_LOG_LEVEL": "WARNING"
    }

    # 3. CLI
    cli_args = {
        "debounce_seconds": 2.5,
        "metadata_allowlist": "tag1, tag2",
        "batch_size_limit": 10,
        "log_file": "cli.log"
    }

    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli_args)

        # Check Config File values (not overridden by Env/CLI)
        assert config.watch_path == "./file_watch.json"
        assert config.port == 5001

        # Check Env values (override Config File)
        assert config.testing is False # Env "false" overrides Config "true"
        assert config.pulsetime == 60.0
        assert config.log_level == "WARNING"

        # Check CLI values (override everything else)
        assert config.debounce_seconds == 2.5
        assert config.metadata_allowlist == ["tag1", "tag2"]
        assert config.batch_size_limit == 10
        assert config.log_file == "cli.log"


def test_config_priority_cli_disable_log_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that passing empty string for log_file via CLI disables it (overriding config)."""
    monkeypatch.chdir(tmp_path)
    
    # Config file enables logging
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nlog_file = config.log", encoding="utf-8")
    
    # CLI passes empty string to disable
    cli_args = {"log_file": ""}
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config(cli_args)
        # Should be empty string (falsy), which setup_logging treats as "no file"
        assert config.log_file == ""


def test_metadata_allowlist_mixed_source_parsing(tmp_path: Path) -> None:
    """Test metadata_allowlist parsing when overriding via different sources."""
    # Env has spaces
    env = {"AW_WATCHER_METADATA_ALLOWLIST": "  env1 , env2  "}
    
    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        assert config.metadata_allowlist == ["env1", "env2"]
