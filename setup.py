"""Setup script for legacy installation support.

This file is provided for compatibility with legacy tools and environments
that do not support PEP 517/518 (pyproject.toml) builds.
Primary configuration is maintained in pyproject.toml.
"""

from setuptools import setup, find_packages
import os

def get_version():
    """Read version from __init__.py."""
    init_py = os.path.join(os.path.dirname(__file__), "aw_watcher_pipeline_stage", "__init__.py")
    if os.path.exists(init_py):
        with open(init_py, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("__version__"):
                    delim = '"' if '"' in line else "'"
                    return line.split(delim)[1]
    return "0.1.0"

def get_long_description():
    """Read long description from README.md."""
    readme = os.path.join(os.path.dirname(__file__), "README.md")
    if os.path.exists(readme):
        with open(readme, "r", encoding="utf-8") as f:
            return f.read()
    return ""

setup(
    name="aw-watcher-pipeline-stage",
    version=get_version(),
    description="ActivityWatch watcher for development pipeline stages via current_task.json",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=[
        "aw-client>=0.6",
        "watchdog>=4.0",
        "tomli>=2.0",
    ],
    entry_points={
        "console_scripts": [
            "aw-watcher-pipeline-stage=aw_watcher_pipeline_stage.main:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)