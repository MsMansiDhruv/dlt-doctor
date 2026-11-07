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

## Integration Testing with PySpark & Delta Lake

To run the integration test suite (`pytest -m integration` or `REQUIRE_SPARK=1 pytest`):

1. **Java Runtime**: Java 11 or 17 must be installed. Set `JAVA_HOME` pointing to your JDK home directory:
   - On Linux/macOS: `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk`
   - On Windows (PowerShell): `$env:JAVA_HOME="C:\Program Files\Eclipse Adoptium\jdk-17.0.x"`

2. **Windows Hadoop Native Libraries (`winutils.exe`)**:
   On Windows, Apache Spark requires `winutils.exe` and `hadoop.dll`. Set `HADOOP_HOME`:
   - `$env:HADOOP_HOME="C:\hadoop"` (with `winutils.exe` located under `$env:HADOOP_HOME\bin`).

3. **Running Integration Tests**:
   ```bash
   REQUIRE_SPARK=1 pytest -m integration -v
   ```

## Commit Guidelines

- Use conventional commits format (e.g. `feat(rules): ...`, `fix(expectations): ...`, `docs: ...`).
- Include Developer Certificate of Origin (DCO) sign-off (`git commit -s`).
- No emojis in commit messages or code comments.

## Pull Requests

1. Fork the repo and create your branch from `main`.
2. Add failing test cases before writing fixes or features.
3. Keep changes focused and include documentation updates where applicable.
