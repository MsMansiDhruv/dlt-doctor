# Contributing to dlt-doctor

Thank you for your interest in contributing to dlt-doctor!

## Development Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/mansidhr/dlt-doctor.git
   cd dlt-doctor
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

## Code Quality Standards

Before submitting a pull request, ensure all checks pass:

- **Linting & Formatting**:
  ```bash
  ruff check src tests
  ruff format --check src tests
  ```

- **Type Checking**:
  ```bash
  mypy src
  ```

- **Security Scanning**:
  ```bash
  bandit -r src
  ```

- **Tests**:
  ```bash
  pytest
  ```

## Commit Guidelines

- Use conventional commits format (e.g. `feat(rules): ...`, `fix(expectations): ...`, `docs: ...`).
- Include Developer Certificate of Origin (DCO) sign-off (`git commit -s`).
- No emojis in commit messages or code comments.

## Pull Requests

1. Fork the repo and create your branch from `main`.
2. Add failing test cases before writing fixes or features.
3. Keep changes focused and include documentation updates where applicable.
