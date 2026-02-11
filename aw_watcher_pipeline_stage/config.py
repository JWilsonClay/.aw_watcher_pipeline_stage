"""Configuration management for aw-watcher-pipeline-stage.

This module handles loading configuration from defaults, config files, environment variables,
and CLI arguments. It enforces a strict priority order and ensures all paths are validated
for security. Supports XDG_CONFIG_HOME (Linux/macOS), APPDATA (Windows), and ~/.config fallback.

The configuration is aggregated into a :class:`Config` dataclass, which serves as the
single source of truth for application settings.

Priority Order:
    1. CLI Arguments
    2. Environment Variables
    3. Config File
    4. Defaults

Supported Environment Variables:
    * ``AW_WATCHER_WATCH_PATH`` / ``PIPELINE_WATCHER_PATH``: Path to the file to watch.
    * ``AW_WATCHER_PORT``: Port for the ActivityWatch server.
    * ``AW_WATCHER_TESTING``: Enable testing mode.
    * ``AW_WATCHER_LOG_FILE``: Path to the log file.
    * ``AW_WATCHER_LOG_LEVEL``: Logging level.
    * ``AW_WATCHER_PULSETIME``: Heartbeat merge window.
    * ``AW_WATCHER_DEBOUNCE_SECONDS``: Event debounce interval.
    * ``AW_WATCHER_METADATA_ALLOWLIST``: Comma-separated list of allowed metadata keys.
    *   ``AW_WATCHER_BATCH_SIZE_LIMIT``: Max events to queue before forcing a batch process.

Configuration Loading Invariants:
    * **Path Validation**: All paths are strictly resolved to canonical paths to prevent
      traversal attacks. Symlinks are resolved, non-regular files are rejected, and
      permissions (read/write) are verified.
    * **Cross-Platform**: Config paths are determined using OS-specific conventions
      (e.g., XDG on Linux, APPDATA on Windows).
    * **Type Safety**: Numeric values are validated for positivity.
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

__all__ = ["Config", "load_config"]


@dataclass
class Config:
    """Define the application configuration structure.

    Attributes:
        watch_path (str): Absolute path to the watched file.
            Defaults to current directory or git root.
        port (Optional[int]): Port for the ActivityWatch server. Defaults to 5600.
        testing (bool): Whether to run in testing mode. Defaults to False.
        log_file (Optional[str]): Absolute path to the log file. Defaults to None.
        log_level (str): Logging level (e.g., INFO, DEBUG). Defaults to "INFO".
        pulsetime (float): Time in seconds to wait before considering a task finished. Defaults to 120.0.
        debounce_seconds (float): Time in seconds to debounce file events. Defaults to 1.0.
        metadata_allowlist (Optional[List[str]]): Optional list of allowed metadata keys. Defaults to None.
        batch_size_limit (int): Max events to queue before forcing a batch process. Defaults to 5.
    """

    watch_path: str
    port: Optional[int]
    testing: bool
    log_file: Optional[str]
    log_level: str
    pulsetime: float
    debounce_seconds: float
    metadata_allowlist: Optional[List[str]]
    batch_size_limit: int


def _find_project_root(start_path: Path) -> Optional[Path]:
    """Find the project root by looking for the .git directory upwards.

    Traverses parents of the start path looking for a `.git` directory.
    Catches OSError during traversal to ensure robustness.

    This is primarily used to determine a default watch path if none is provided.

    Args:
        start_path (Path): The starting path for the search.
            This path is resolved to absolute before traversal.

    Returns:
        Optional[Path]: The path to the project root if found, else None.
    """
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
    """Return a list of potential config file paths in order of priority.

    Checks the following locations:
    1. Local `config.ini` (current working directory).
    2. `$XDG_CONFIG_HOME/aw-watcher-pipeline-stage/config.ini` (Linux/macOS).
    3. `%APPDATA%\\aw-watcher-pipeline-stage\\config.ini` (Windows).
    4. `~/.config/aw-watcher-pipeline-stage/config.ini` (Fallback).

    Returns:
        List[str]: A list of file paths to check for configuration.
    """
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

def _validate_path(path_str: str, is_log: bool = False) -> str:
    """Resolve and validate path security, expanding the user tilde.

    Performs strict resolution to canonicalize paths, preventing directory traversal
    and ensuring the path points to a valid location. Checks for regular file type
    (rejecting directories/devices for file paths) and verifies read/write permissions.
    Expands user tilde (~) to the user's home directory.

    If `path_str` points to a directory and `is_log` is False, the function automatically
    appends 'current_task.json' to the path.

    Note:
        Security checks performed include:
        - Symlinks are resolved (strict=True).
        - Path must be a regular file (if it exists).
        - Parent directory must exist (for creation).
        - Permissions are verified (read for watch path, write for log file).

    Args:
        path_str (str): The raw path string to validate (e.g., "~/task.json").
        is_log (bool): If True, validates for log file usage (write permission).
            If False, validates for watch path usage (read permission).
            Defaults to False.

    Returns:
        str: The resolved, absolute, and canonicalized path string.

    Raises:
        ValueError: If the path is invalid, contains traversal attempts, does not exist (and cannot be created),
            is not a regular file, or lacks required permissions.
    """
    try:
        path = Path(os.path.expanduser(path_str))
        
        # Security: Reject symlinks explicitly
        if path.is_symlink():
            raise ValueError(f"Invalid path: Symlinks are not allowed (security): {path}")

        # Resolve strictly to canonical path
        try:
            resolved = path.resolve(strict=True)
        except FileNotFoundError:
            # If not found, resolve parent strictly (for creation/waiting)
            try:
                parent = path.parent.resolve(strict=True)
                resolved = parent / path.name
            except (FileNotFoundError, RuntimeError, OSError) as e:
                raise ValueError(f"Invalid path (parent directory not found): {path}") from e
        except (RuntimeError, OSError) as e:
            raise ValueError(f"Error resolving path {path}: {e}") from e
            
        if not is_log:
            # For watch_path, if it's a directory, append default file
            if resolved.is_dir():
                resolved = resolved / "current_task.json"
                # Re-resolve if exists to ensure canonical
                if resolved.exists():
                    # Security: Check if the default file is a symlink
                    if resolved.is_symlink():
                        raise ValueError(f"Invalid path: Symlinks are not allowed (security): {resolved}")
                    resolved = resolved.resolve(strict=True)
            
            # Check if it is a regular file (if it exists)
            if resolved.exists():
                if not resolved.is_file():
                    raise ValueError(f"Invalid path: Watch path is not a regular file (directories/devices not allowed): {resolved}")
                # Check read permission
                try:
                    with resolved.open('r'):
                        pass
                except PermissionError as e:
                    raise ValueError(f"Read permission denied for watch path: {resolved}") from e
        else:
            # For log_file
            if resolved.exists():
                if not resolved.is_file():
                    raise ValueError(f"Invalid path: Log file is not a regular file: {resolved}")
                # Check write permission
                try:
                    with resolved.open('a'):
                        pass
                except PermissionError as e:
                    raise ValueError(f"Write permission denied for log file: {resolved}") from e
            else:
                # Try creating to verify permissions
                try:
                    with resolved.open('a'):
                        pass
                except PermissionError as e:
                    raise ValueError(f"Cannot create log file (permission denied): {resolved}") from e
                except OSError as e:
                    raise ValueError(f"Cannot create log file: {e}") from e

        return str(resolved)

    except Exception as e:
        if isinstance(e, ValueError):
            raise
        raise ValueError(f"Path validation failed for '{path_str}': {e}") from e

def load_config(args: Dict[str, Any]) -> Config:
    """Load and validate configuration with strict priority, returning a Config object.

    Aggregates configuration from multiple sources, resolving conflicts by
    prioritizing command-line arguments, then environment variables, then
    configuration files, and finally hardcoded defaults.

    This function ensures that the returned configuration is fully validated,
    with all paths resolved to absolute, secure paths (expanding user tilde `~`)
    and all numeric values checked for validity. Comma-separated strings in
    `metadata_allowlist` (from env/config) are parsed into lists.

    Priority Order (Highest to Lowest):
        1. CLI Arguments (passed via `args`)
        2. Environment Variables (e.g., `AW_WATCHER_PORT`, `PIPELINE_WATCHER_PATH`)
        3. Config File (e.g., `config.ini` in XDG/AppData)
        4. Hardcoded Defaults

    Args:
        args (Dict[str, Any]): Dictionary of parsed CLI arguments from argparse.
            Keys should match Config attributes (e.g., 'watch_path', 'port').
            Values of None are ignored to allow lower-priority sources (Env, Config File) to take effect.
            Empty strings ("") in CLI arguments are treated as explicit values and will override
            lower-priority sources (though they may trigger default logic later).
            Typically obtained via ``vars(parser.parse_args())``.

    Returns:
        Config: The fully resolved and validated configuration object.
            Access settings as attributes (e.g., `config.port`, `config.watch_path`).
            See :class:`Config` for full attribute details.

    Raises:
        ValueError: If numeric configuration values are invalid (e.g., negative pulsetime),
            or if path validation fails for 'watch_path' or 'log_file' (e.g., path traversal,
            invalid file type, permission denied).

    Examples:
        >>> from aw_watcher_pipeline_stage.config import load_config
        >>> # 1. CLI arguments (Highest Priority)
        >>> args = {"watch_path": ".", "port": 9999}
        >>> config = load_config(args)
        >>> config.port
        9999

        >>> # 2. Environment variables (Medium Priority)
        >>> import os
        >>> os.environ["AW_WATCHER_PORT"] = "1234"
        >>> config = load_config({})
        >>> config.port
        1234
        >>> del os.environ["AW_WATCHER_PORT"]

        >>> # 3. Config File (Low Priority)
        >>> # (Skipped in doctest to avoid file I/O dependency)

        >>> # 4. Defaults (Lowest Priority)
        >>> config = load_config({})
        >>> config.debounce_seconds
        1.0

        >>> # 5. Metadata allowlist parsing
        >>> import os
        >>> os.environ["AW_WATCHER_METADATA_ALLOWLIST"] = "foo, bar, baz"
        >>> config = load_config({})
        >>> config.metadata_allowlist
        ['foo', 'bar', 'baz']
        >>> del os.environ["AW_WATCHER_METADATA_ALLOWLIST"]
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
        "metadata_allowlist": None,
        "batch_size_limit": 5,
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
        "AW_WATCHER_METADATA_ALLOWLIST": "metadata_allowlist",
        "AW_WATCHER_BATCH_SIZE_LIMIT": "batch_size_limit",
    }
    for env_var, config_key in env_map.items():
        val = os.getenv(env_var)
        if val is not None and val != "":
            config_values[config_key] = val

    # 4. CLI Arguments (override if not None)
    for key, value in args.items():
        if value is not None:
            # Note: Empty string "" from CLI will override previous values
            # and later trigger default logic for watch_path
            config_values[key] = value

    # Type casting for specific fields
    if config_values["port"] is not None:
        try:
            config_values["port"] = int(config_values["port"])
        except ValueError as e:
            raise ValueError(f"Invalid integer for port: {config_values['port']}") from e
        if not (1 <= config_values["port"] <= 65535):
            raise ValueError(f"Port must be between 1 and 65535, got {config_values['port']}")
    
    if config_values["pulsetime"] is not None:
        try:
            config_values["pulsetime"] = float(config_values["pulsetime"])
        except ValueError as e:
            raise ValueError(f"Invalid float for pulsetime: {config_values['pulsetime']}") from e
        if config_values["pulsetime"] <= 0:
            raise ValueError(f"pulsetime must be positive, got {config_values['pulsetime']}")

    if config_values["debounce_seconds"] is not None:
        try:
            config_values["debounce_seconds"] = float(config_values["debounce_seconds"])
        except ValueError as e:
            raise ValueError(f"Invalid float for debounce_seconds: {config_values['debounce_seconds']}") from e
        if config_values["debounce_seconds"] < 0:
            raise ValueError(f"debounce_seconds must be non-negative, got {config_values['debounce_seconds']}")

    if config_values["batch_size_limit"] is not None:
        try:
            config_values["batch_size_limit"] = int(config_values["batch_size_limit"])
        except ValueError as e:
            raise ValueError(f"Invalid integer for batch_size_limit: {config_values['batch_size_limit']}") from e
        if not (1 <= config_values["batch_size_limit"] <= 1000):
            raise ValueError(f"batch_size_limit must be between 1 and 1000, got {config_values['batch_size_limit']}")

    if config_values["metadata_allowlist"] is not None:
        if isinstance(config_values["metadata_allowlist"], str):
            # Parse comma-separated string into list
            config_values["metadata_allowlist"] = [
                k.strip()
                for k in config_values["metadata_allowlist"].split(",")
                if k.strip()
            ]
        elif isinstance(config_values["metadata_allowlist"], list):
            # Ensure all elements are strings (defensive) and strip whitespace
            config_values["metadata_allowlist"] = [
                str(k).strip() for k in config_values["metadata_allowlist"] if str(k).strip()
            ]

    # Expand user path for watch_path (e.g. ~)
    if config_values["watch_path"]:
        config_values["watch_path"] = _validate_path(str(config_values["watch_path"]), is_log=False)
    else:
        # Default to git root if available, else "."
        try:
            cwd = Path.cwd()
            root = _find_project_root(cwd)
        except OSError:
            logger.debug("Could not determine CWD, defaulting watch_path to '.'")
            root = None

        if root:
            raw_path = str(root)
        else:
            logger.debug("No project root found, defaulting watch_path to '.'")
            raw_path = "."
        config_values["watch_path"] = _validate_path(raw_path, is_log=False)

    if config_values["log_file"]:
        config_values["log_file"] = _validate_path(str(config_values["log_file"]), is_log=True)

    # Handle boolean conversion for testing
    if isinstance(config_values["testing"], str):
        config_values["testing"] = config_values["testing"].lower() in ("true", "1", "yes", "on")

    # Handle debug flag
    if args.get("debug"):
        config_values["log_level"] = "DEBUG"

    # Validate log_level
    if config_values["log_level"]:
        config_values["log_level"] = config_values["log_level"].upper()
        level = config_values["log_level"]
        if not isinstance(getattr(logging, level, None), int):
            raise ValueError(f"Invalid log level: {config_values['log_level']}")

    # Filter out keys that are not in Config fields (e.g. 'debug' from CLI)
    config_fields = {f.name for f in fields(Config)}
    filtered_values = {k: v for k, v in config_values.items() if k in config_fields}

    return Config(**filtered_values)