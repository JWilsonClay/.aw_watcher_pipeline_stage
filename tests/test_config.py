"""Tests for configuration loading and priority logic."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

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

def test_config_priority_hierarchy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test the full priority chain: CLI > Env > Config File > Default.
    """
    # 1. Default
    monkeypatch.chdir(tmp_path)
    
    # Create a config file
    config_file = tmp_path / "config.ini"
    config_file.write_text("[aw-watcher-pipeline-stage]\nport = 5601\nwatch_path = ./file_cfg", encoding="utf-8")

    # 1. Config File > Default
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.port == 5601
        assert config.watch_path == "./file_cfg"

    # 2. Env > Config File
    with patch.dict(os.environ, {"AW_WATCHER_PORT": "5602", "PIPELINE_WATCHER_PATH": "./env_path"}):
        config = load_config({})
        assert config.port == 5602
        assert config.watch_path == "./env_path"

    # 3. CLI > Env
    with patch.dict(os.environ, {"AW_WATCHER_PORT": "5602", "PIPELINE_WATCHER_PATH": "./env_path"}):
        config = load_config({"port": 5603, "watch_path": "./cli_path"})
        assert config.port == 5603
        assert config.watch_path == "./cli_path"


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


def test_full_precedence_stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test all 4 layers of precedence simultaneously: CLI > Env > Local Config > Default."""
    monkeypatch.chdir(tmp_path)
    
    # 1. Local Config (Priority 3)
    (tmp_path / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5620", encoding="utf-8")
    
    # 2. Env (Priority 2)
    env = {"AW_WATCHER_PORT": "5621"}
    
    # 3. CLI (Priority 1)
    cli_args = {"port": 5622}
    
    with patch.dict(os.environ, env, clear=True):
        config = load_config(cli_args)
        assert config.port == 5622
        
    # Remove CLI, check Env
    with patch.dict(os.environ, env, clear=True):
        config = load_config({})
        assert config.port == 5621
        
    # Remove Env, check Config
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.port == 5620


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


def test_cli_overrides_git_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that CLI watch_path takes precedence over git root detection."""
    # Setup git root
    (tmp_path / ".git").mkdir()
    monkeypatch.chdir(tmp_path)
    
    # Ensure no config/env interference
    with patch.dict(os.environ, {}, clear=True):
        # CLI arg provided
        config = load_config({"watch_path": "./cli_explicit"})
        assert config.watch_path == "./cli_explicit"


def test_absolute_path_in_config_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test absolute path in config file is respected."""
    monkeypatch.chdir(tmp_path)
    abs_path = str(tmp_path / "absolute" / "path")
    
    (tmp_path / "config.ini").write_text(f"[aw-watcher-pipeline-stage]\nwatch_path = {abs_path}", encoding="utf-8")
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({})
        assert config.watch_path == abs_path


def test_macos_config_path_explicit(tmp_path: Path) -> None:
    """Test that macOS (POSIX) falls back to ~/.config if XDG not set."""
    mock_home = tmp_path / "home"
    mock_home.mkdir()
    
    config_dir = mock_home / ".config" / "aw-watcher-pipeline-stage"
    config_dir.mkdir(parents=True)
    (config_dir / "config.ini").write_text("[aw-watcher-pipeline-stage]\nport = 5699", encoding="utf-8")

    # Mock os.name to posix (macOS is posix)
    with patch("os.name", "posix"):
        with patch.dict(os.environ, {}, clear=True): # No XDG_CONFIG_HOME
            with patch("os.path.expanduser") as mock_expand:
                def expand_side_effect(path: str) -> str:
                    if path == "~":
                        return str(mock_home)
                    if path.startswith("~"):
                        return str(mock_home / path[2:])
                    return path
                mock_expand.side_effect = expand_side_effect
                
                config = load_config({})
                assert config.port == 5699


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
    with pytest.raises(ValueError):
        load_config({"port": "invalid_int"})
        
    with pytest.raises(ValueError):
        load_config({"pulsetime": "invalid_float"})


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


def test_absolute_cli_override_with_git_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that absolute CLI path overrides git root detection."""
    (tmp_path / ".git").mkdir()
    monkeypatch.chdir(tmp_path)
    
    abs_path = str((tmp_path / "other").resolve())
    
    with patch.dict(os.environ, {}, clear=True):
        config = load_config({"watch_path": abs_path})
        assert config.watch_path == abs_path


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


def test_log_level_case_preservation() -> None:
    """Test that log_level case is preserved (main.py handles normalization)."""
    config = load_config({"log_level": "debug"})
    assert config.log_level == "debug"


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