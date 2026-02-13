# Contributing to aw-watcher-pipeline-stage

Thank you for considering contributing! We follow the ActivityWatch community guidelines and enforce strict code quality standards to ensure reliability and privacy.

## Development Setup

1.  **Fork** the repository and **Clone** it locally.
2.  Install dependencies using Poetry:
    ```bash
    poetry install --with dev
    ```
3.  Install pre-commit hooks to ensure code style consistency:
    ```bash
    pre-commit install
    ```

## Code Style & Quality

We enforce the following standards via pre-commit hooks:
*   **Formatting**: `black` and `isort` (PEP 8 compliant).
*   **Type Checking**: `mypy` (strict mode).
*   **Linting**: `flake8`.

These checks run automatically on every commit. You can also run them manually:

```bash
poetry run pre-commit run --all-files
```

## Testing

We maintain high test coverage (>90%) to ensure robustness. Before submitting a PR, run the full test suite:

```bash
poetry run pytest -v --cov=aw_watcher_pipeline_stage --cov-report=term-missing --cov-fail-under=90
```

Tests cover:
*   Unit logic (JSON parsing, debounce).
*   Integration flows (lifecycle, offline recovery).
*   Security (symlink attacks, path traversal).
*   Robustness (malformed data, permission errors).

For a detailed testing strategy, see [TESTING.md](TESTING.md).

## Pull Request Guidelines

1.  **Feature Branches**: Create a new branch for your changes (e.g., `feature/my-feature` or `fix/issue-123`).
2.  **Code Style**: Ensure your code matches the style of official ActivityWatch watchers.
3.  **Coverage**: New code must include tests. We enforce >90% coverage.
4.  **Security & Privacy**: This watcher is "Local-Only" and "Privacy-First". Do not add external network calls or telemetry. Ensure input validation is robust.
5.  **References**: Link related issues in your PR description.

## Code of Conduct

We follow the ActivityWatch Code of Conduct. Please be respectful and constructive.