"""Automated replay SQL generator for reprocessing quarantined rows into target tables."""

from __future__ import annotations

import re
from datetime import datetime

IDENTIFIER_PATTERN = re.compile(
    r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$|^`[a-zA-Z0-9_\.]+`$"
)


def _validate_identifier(identifier: str) -> str:
    if not IDENTIFIER_PATTERN.match(identifier.strip()):
        raise ValueError(f"Invalid SQL identifier '{identifier}'.")
    return identifier.strip()


def _validate_iso_timestamp(ts: str, param_name: str) -> str:
    clean = ts.strip()
    try:
        normalized = clean.replace("Z", "+00:00")
        datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid ISO timestamp for {param_name}: '{ts}'.") from exc
    return clean


def generate_replay_sql(
    quarantine_table: str,
    target_table: str,
    primary_keys: list[str],
    rule_name: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    recency_col: str | None = "updated_at",
) -> str:
    """Generate idempotent Delta MERGE SQL to reprocess quarantined records.

    Architecture Note & Lakeflow DML Guidance:
    Direct DML operations (such as MERGE, UPDATE, DELETE) against pipeline-managed
    streaming tables or materialized views in Databricks Lakeflow / Delta Live Tables
    are restricted and discouraged. External DML can disrupt streaming checkpoints,
    state stores, and automated pipeline maintenance.

    Recommended Best Practice:
    Instead of executing ad-hoc MERGE directly into downstream managed tables, prefer
    re-feeding corrected rows into the pipeline's upstream source stream (e.g. Bronze or
    landing area) with updated sequence/watermark values so the pipeline processes them
    natively through its standard lineage.
    """
    clean_quarantine = _validate_identifier(quarantine_table)
    clean_target = _validate_identifier(target_table)
    clean_keys = [_validate_identifier(k) for k in primary_keys]

    conditions: list[str] = []
    if rule_name:
        escaped_rule = rule_name.replace("'", "''")
        conditions.append(f"array_contains(_failed_rules, '{escaped_rule}')")
    if start_time:
        valid_start = _validate_iso_timestamp(start_time, "start_time")
        conditions.append(f"_quarantined_at >= '{valid_start}'")
    if end_time:
        valid_end = _validate_iso_timestamp(end_time, "end_time")
        conditions.append(f"_quarantined_at <= '{valid_end}'")

    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    pk_join = " AND ".join(f"target.{k} = source.{k}" for k in clean_keys)
    partition_cols = ", ".join(clean_keys)

    recency_guard = ""
    if recency_col:
        clean_recency = _validate_identifier(recency_col)
        recency_guard = f" AND (target.{clean_recency} IS NULL OR source.{clean_recency} >= target.{clean_recency})"

    lines = [
        "-- Replay SQL: Reprocess quarantined records into target table idempotently",
        "-- Note: Prefer re-feeding corrected rows into the upstream source stream rather than direct DML.",
        f"MERGE INTO {clean_target} AS target",
        "USING (",
        "    SELECT * EXCEPT (_failed_rules, _quarantined_at),",
        "           current_timestamp() AS _replayed_at",
        f"    FROM {clean_quarantine}{where_clause}",
        f"    QUALIFY row_number() OVER (PARTITION BY {partition_cols} ORDER BY _quarantined_at DESC) = 1",
        ") AS source",
        f"ON {pk_join}",
        f"WHEN MATCHED{recency_guard} THEN UPDATE SET *",  # nosec B608
        "WHEN NOT MATCHED THEN INSERT *;",
    ]
    return "\n".join(lines)
