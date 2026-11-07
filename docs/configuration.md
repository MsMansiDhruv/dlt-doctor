# Configuration Guide

`dlt-doctor` discovers configuration automatically from `pyproject.toml` or `dlt-doctor.toml` located in the target directory or any of its parent directories. Custom configuration files can also be specified using the `--config <path>` flag.

## TOML Configuration Schema

Below is a complete `pyproject.toml` configuration snippet:

```toml
[tool.dlt-doctor]
# Failure severity threshold for CI exit code 1 (CRITICAL, HIGH, MEDIUM, LOW, INFO, NONE)
fail_on = "CRITICAL"

# Global rule IDs to select (if non-empty, only these rules run)
select = []

# Global rule IDs to ignore across all scanned files
ignore = ["DD003"]

# Table name globs subject to strict DD001 quarantine requirements
regulated_tables = ["*orders*", "*customers*", "*txn*", "*finance*"]

# Per-file rule suppressions
[tool.dlt-doctor.per_file_ignores]
"legacy_pipeline.py" = ["PYSPARK-001", "PYSPARK-002"]

# Rule severity overrides
[tool.dlt-doctor.severity_overrides]
"DD001" = "HIGH"
```

## Inline Suppressions

Rules can also be suppressed inline within source files:

### Python Source Files
```python
# Suppress a single rule for the whole file
# dlt-doctor: ignore[DD001]

# Suppress multiple rules for a specific function
# dlt-doctor: ignore[PYSPARK-001, PYSPARK-002]
@dlt.table
def bronze_events(): ...
```

### SQL Source Files
```sql
-- dlt-doctor: ignore[DD001]
CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW
) AS SELECT * FROM STREAM(bronze_orders);
```
