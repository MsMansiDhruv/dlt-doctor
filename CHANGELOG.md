# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2026-11-04

### Added
- Rule registry (`src/dlt_doctor/rules/`) with `@register`, `list_rules()`, `get_rule()`, and CLI commands `dlt-doctor rules` and `dlt-doctor explain <ID>`.
- Rule `DD001`: Detects `expect_or_drop` and `expect_all_or_drop` anti-patterns on regulated pipeline tables and SQL `ON VIOLATION DROP ROW`.
- Rule `DD002`: Detects quarantine filters that fail to provide three-valued logic NULL safety.
- Rule `DD003`: Identifies dynamic expectation expressions that cannot be statically resolved.
- Automated `--auto-heal` quarantine synthesis generating single-pass pipeline diffs for Databricks Lakeflow declarative pipelines (`pyspark.pipelines`) and legacy Delta Live Tables (`dlt`).
- Support for SQL (`.sql`) file scanning and per-cell notebook parsing for Jupyter (`.ipynb`) and Databricks source notebooks.
- Subcommand `dlt-doctor replay` to generate idempotent MERGE SQL for replaying resolved quarantined records back into target tables.
- Configuration support via `pyproject.toml` (`[tool.dlt-doctor]`), supporting `select`, `ignore`, `fail_on`, `regulated_tables`, `severity_overrides`, and `per_file_ignores`.
- Rich CLI output format options: `text`, `markdown`, `json`, `sarif`, and `html`.
- PEP 561 marker `py.typed` and comprehensive typing across the package.
- Benchmarking suite in `benchmarks/run_benchmarks.py`.

### Changed
- Fixed NULL-safety bug in quarantine routing by wrapping expectation conditions in `coalesce((<cond>), false)`.
- Replaced monolithic CLI with modular subcommand architecture under `dlt_doctor.cli`.
- Lazily exported `load_ipython_extension` in top-level `dlt_doctor` package without eager imports of live workspace connector dependencies.
- Unity Catalog volume DDL generator now generates `CREATE EXTERNAL VOLUME IF NOT EXISTS ... LOCATION '<uri>'` templates instead of empty managed volumes.
- Reconciler restatement and streaming CDC templates now strictly validate SQL identifiers against injection patterns.
- Streaming CDC templates now automatically filter `update_preimage` from Change Data Feed sources and recommend modern Lakeflow Declarative CDC APIs.

### Fixed
- Fixed duplicate diagnostic findings on UDF function decorators.
- Fixed cascading diagnostic findings on deep `.withColumn()` chains, reporting once on the root of the chain.
- Fixed HTML report generation to properly escape user strings, avoiding injection.

## [0.4.2] - 2026-10-25

### Added
- Initial support for Unity Catalog migration scanner and Delta Lake optimization calculators.
