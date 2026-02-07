"""Configuration management for aw-watcher-pipeline-stage.

Handles loading configuration from defaults, config files, environment variables,
and CLI arguments with the priority: CLI > Env > Config File > Defaults.
Supports XDG_CONFIG_HOME (Linux/macOS), APPDATA (Windows), and ~/.config fallback.
"""

from __future__ import annotations

import logging
import os
from configparser import ConfigParser, Error as ConfigParserError
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


@dataclass
class Config:
    """Application configuration.

    Attributes:
        watch_path: Path to the directory to watch.
        port: Port for the ActivityWatch server.
        testing: Whether to run in testing mode.
        log_file: Path to the log file.
        log_level: Logging level (e.g., INFO, DEBUG).
        pulsetime: Time in seconds to wait before considering a task finished.
        debounce_seconds: Time in seconds to debounce file events.
    """

    watch_path: str
    port: Optional[int]
    testing: bool
    log_file: Optional[str]
    log_level: str
    pulsetime: float
    debounce_seconds: float


def _find_project_root(start_path: Path) -> Optional[Path]:
    """Find the project root by looking for .git directory upwards."""
    try:
        path = start_path.resolve()
        if path.is_file():
            path = path.parent

        for parent in [path] + list(path.parents):
            # Check for .git, catching potential permission errors via the outer try/except
            if (parent / ".git").exists():
                return parent
    except OSError:
        pass
    return None

def _get_config_file_paths() -> List[str]:
    """Return a list of potential config file paths in order of priority."""
    paths = ["config.ini"]  # Local directory (highest priority among files)

    xdg_config_home = os.environ.get("XDG_CONFIG_HOME")
    if xdg_config_home:
        paths.append(
            os.path.join(
                os.path.expanduser(xdg_config_home),
                "aw-watcher-pipeline-stage",
                "config.ini",
            )
        )
    elif os.name == "nt" and os.environ.get("APPDATA"):
        paths.append(
            os.path.join(
                os.path.expanduser(os.environ["APPDATA"]),
                "aw-watcher-pipeline-stage",
                "config.ini",
            )
        )
    else:
        # Fallback for POSIX (macOS/Linux) and Windows without APPDATA
        paths.append(
            os.path.join(
                os.path.expanduser("~"),
                ".config",
                "aw-watcher-pipeline-stage",
                "config.ini",
            )
        )
    return paths

def load_config(cli_args: Dict[str, Any]) -> Config:
    """Load configuration with priority: CLI > Env > Config File > Defaults.

    Supports cross-platform config paths (XDG on Linux/macOS, APPDATA on Windows).

    Args:
        cli_args: Dictionary of parsed CLI arguments.

    Returns:
        Config: The fully resolved configuration object.
    """
    # 1. Defaults
    config_values: Dict[str, Any] = {
        "watch_path": None,
        "port": 5600,
        "testing": False,
        "log_file": None,
        "log_level": "INFO",
        "pulsetime": 120.0,
        "debounce_seconds": 1.0,
    }

    # 2. Config File (simple INI support)
    # Check for config file in XDG_CONFIG_HOME or local directory
    for path in _get_config_file_paths():
        if os.path.isfile(path):
            logger.debug(f"Loading config from {path}")
            parser = ConfigParser(interpolation=None)
            try:
                parser.read(path, encoding="utf-8-sig")
                if "aw-watcher-pipeline-stage" in parser:
                    for key, value in parser["aw-watcher-pipeline-stage"].items():
                        # Basic type conversion could go here
                        if value is not None and value != "":
                            config_values[key] = value
            except (ConfigParserError, UnicodeDecodeError, OSError) as e:
                logger.error(f"Failed to parse config file {path}: {e}")
            break

    # 3. Environment Variables
    env_map = {
        "AW_WATCHER_WATCH_PATH": "watch_path",
        "PIPELINE_WATCHER_PATH": "watch_path",
        "AW_WATCHER_PORT": "port",
        "AW_WATCHER_TESTING": "testing",
        "AW_WATCHER_LOG_FILE": "log_file",
        "AW_WATCHER_LOG_LEVEL": "log_level",
        "AW_WATCHER_PULSETIME": "pulsetime",
        "AW_WATCHER_DEBOUNCE_SECONDS": "debounce_seconds",
    }
    for env_var, config_key in env_map.items():
        val = os.getenv(env_var)
        if val is not None and val != "":
            config_values[config_key] = val

    # 4. CLI Arguments (override if not None)
    for key, value in cli_args.items():
        if value is not None:
            # Note: Empty string "" from CLI will override previous values
            # and later trigger default logic for watch_path
            config_values[key] = value

    # Type casting for specific fields
    if config_values["port"] is not None:
        config_values["port"] = int(config_values["port"])
    
    if config_values["pulsetime"] is not None:
        config_values["pulsetime"] = float(config_values["pulsetime"])
        if config_values["pulsetime"] <= 0:
            raise ValueError(f"pulsetime must be positive, got {config_values['pulsetime']}")

    if config_values["debounce_seconds"] is not None:
        config_values["debounce_seconds"] = float(config_values["debounce_seconds"])
        if config_values["debounce_seconds"] < 0:
            raise ValueError(f"debounce_seconds must be non-negative, got {config_values['debounce_seconds']}")

    # Expand user path for watch_path (e.g. ~)
    if config_values["watch_path"]:
        config_values["watch_path"] = str(config_values["watch_path"])
        config_values["watch_path"] = os.path.expanduser(config_values["watch_path"])
    else:
        # Default to git root if available, else "."
        try:
            cwd = Path.cwd()
            root = _find_project_root(cwd)
        except OSError:
            logger.debug("Could not determine CWD, defaulting watch_path to '.'")
            root = None

        if root:
            config_values["watch_path"] = str(root)
        else:
            logger.debug("No project root found, defaulting watch_path to '.'")
            config_values["watch_path"] = "."

    if config_values["log_file"]:
        config_values["log_file"] = str(config_values["log_file"])
        config_values["log_file"] = os.path.expanduser(config_values["log_file"])

    # Handle boolean conversion for testing
    if isinstance(config_values["testing"], str):
        config_values["testing"] = config_values["testing"].lower() in ("true", "1", "yes", "on")

    # Handle debug flag
    if cli_args.get("debug"):
        config_values["log_level"] = "DEBUG"

    # Filter out keys that are not in Config fields (e.g. 'debug' from CLI)
    config_fields = {f.name for f in fields(Config)}
    filtered_values = {k: v for k, v in config_values.items() if k in config_fields}

    return Config(**filtered_values)