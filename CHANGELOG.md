# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2026-11-07

### Breaking
- **Expectation Pipeline Synthesis**: `ExpectationDoctor.generate_pipeline_code` and `QuarantineRoutingPlan.generate_pipeline_code` now require an explicit `source_table` parameter and raise `ValueError` if omitted, eliminating fabricated default table names.
- **Quarantine Replay Revalidation Guard**: `generate_replay_sql` now requires either a parsed `valid_filter_sql` condition or an explicit `allow_unvalidated=True` confirmation to prevent re-injecting unvalidated data.
- **Quarantine Replay Resurrection Guard**: `generate_replay_sql` now requires either an `erasure_table` tombstone anti-join or an explicit `allow_resurrection=True` confirmation to guard against resurrecting records deleted for retention or compliance (GDPR/CCPA).

### Added
- **Diagnostic Rule Registry**: Modular rule registry (`dlt_doctor.rules`) with `@register`, `list_rules()`, `get_rule()`, and CLI commands `dlt-doctor rules` and `dlt-doctor explain <RULE_ID>`.
- **Quality & Expectation Rules**:
  - `DD001`: Detects destructive `expect_or_drop` and `expect_all_or_drop` policies on regulated tables and SQL `ON VIOLATION DROP ROW`.
  - `DD002`: Flags quarantine filters that fail three-valued SQL NULL-safety complements.
  - `DD003`: Identifies dynamic expectation dictionaries that cannot be statically resolved.
  - `DD004`: Detects non-deterministic functions (e.g. `current_timestamp`, `rand`, `uuid`) in expectation expressions.
- **Dedicated Auto-Heal CLI (`dlt-doctor heal`)**:
  - Synthesizes zero-loss, single-pass dual-routing pipeline code for Databricks Lakeflow (`pyspark.pipelines`) and legacy Delta Live Tables (`dlt`).
  - Added `--check` mode for CI pipelines (exit codes: `0` = Compliant, `1` = Needs healing, `2` = Usage error, `3` = Cannot heal).
  - Directory scanning with atomic writes: if any file in a directory fails to heal, zero files are modified on disk and exit code 3 is returned.
  - Honors `# dlt-doctor: ignore[DD001]` inline suppressions, config `ignore`, `per_file_ignores`, and regulated tables filtering.
  - Automatically skips ignored development directories (`.venv`, `node_modules`, `build`, `dist`).
- **Resilient Quarantine Replay Engine (`dlt-doctor replay`)**:
  - Emits idempotent Delta `MERGE` SQL to reprocess quarantined records.
  - Dedupes candidate records by primary key (`QUALIFY row_number() OVER (PARTITION BY ... ORDER BY _quarantined_at DESC) = 1`).
  - Evaluates rule and partition filters after deduplication to ensure older stale failures are not mistakenly replayed.
  - Literal partition pruning predicates placed directly inside `MERGE ON` clauses for targeted partition scans.
  - Opt-in `erasure_table` anti-join guard with optional `erasure_partition_col`.
  - Optional `mark_replayed` injecting `current_timestamp() AS _replayed_at`.
- **Streaming CDC Enhancements (`dlt-doctor cdc`)**:
  - Support for composite match keys and explicit `sequence_col` ordering.
  - Microbatch deduplication filtering `update_preimage` from Change Data Feed sources.
  - SCD Type 2 merge templates with partition columns included in insert statements.
  - Tested handling for out-of-order deletes and equal-timestamp redeliveries.
- **Unified GitHub Action**:
  - Composite GitHub Action (`action.yml`) running single-pass JSON analysis via `dlt_doctor.action`.
  - Derives console output, GitHub Step Summary Markdown tables, and standard SARIF 2.1.0 diagnostics without multi-scan overhead.
  - Configurable `fail-on` (defaulting to `CRITICAL`), `check-heal` gate, and custom `--config` path.
- **Project Configuration**:
  - Discovers `pyproject.toml` (`[tool.dlt-doctor]`) or `dlt-doctor.toml` across parent directories.
  - Supports `select`, `ignore`, `fail_on`, `regulated_tables`, `per_file_ignores`, and `severity_overrides`.
- **Multi-Format Reporting**: Export audit results in `text`, `markdown`, `json`, `sarif`, or Databricks-styled `html`.

### Changed
- **Event Log Metric Retention**: Retain-only `@expect_all` and `@expect` warn decorators are attached to both the clean table and the quarantine table rather than the shared intermediate staging view, preventing metric loss and double-counting in Databricks event logs.
- **Deterministic Evaluation**: Replaced hardcoded default table names with strict validation across `expectations.py`, `cdc.py`, and `reconciler.py`.
- **Modular CLI Architecture**: Replaced monolithic CLI dispatch with individual command handlers under `dlt_doctor.cli`.
- **Lazy Extension Loading**: Interactive cell magic (`load_ipython_extension`) is lazily registered to eliminate unnecessary runtime dependencies.

### Fixed
- Fixed NULL-safety complement in quarantine routing by wrapping SQL expectation expressions in `coalesce((<condition>), false)`.
- Fixed decorator ordering in Lakeflow declarative pipelines: dataset decorator (`@dp.table` / `@dp.temporary_view`) now strictly precedes expectation decorators.
- Fixed duplicate diagnostic findings on decorated Python UDFs.
- Fixed cascading diagnostics on deep `.withColumn()` chains, reporting once per chain root.
- Escaped backslashes and quotes in synthesized expectation rules and SQL strings.

### Security
- Added strict identifier validation via regex and AST parsing across all SQL and DDL generators to prevent SQL injection.
- Sanitized HTML report generation by escaping all user-controlled pipeline names, rule descriptions, and file paths.

---

## [0.4.2] - 2026-11-03

### Added
- Initial support for Unity Catalog migration scanner (`uc-migrate`) and Delta Lake table telemetry inspection (`inspect-table`).
- Calculators for shuffle spill risk, compaction debt, and DBU cost forecasting.
- Local Spark AST optimization checks (`PlanDoctor`).
