"""Automated replay SQL generator for reprocessing quarantined rows into target tables."""

from __future__ import annotations

import math
import re
from datetime import datetime

import sqlglot

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
    recency_col: str | None = None,
    partition_col: str | None = None,
    partition_val: str | int | float | bool | None = None,
    mark_replayed: bool = False,
    valid_filter_sql: str | None = None,
    allow_unvalidated: bool = False,
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

    Insert-Only vs Recency Update:
    By default (when recency_col is None), replay SQL is insert-only (no WHEN MATCHED)
    to prevent stale quarantined rows from overwriting newer live data in target.
    An UPDATE clause is only emitted when recency_col is explicitly provided to guard
    against regressions.

    Deduplication Ordering:
    Deduplication (QUALIFY row_number OVER (PARTITION BY ... ORDER BY _quarantined_at DESC) = 1)
    runs before the validity filter so that if a key's latest version remains invalid, an older
    passing version for that key is not replayed.

    Schema Requirement for mark_replayed:
    Setting `mark_replayed=True` injects `current_timestamp() AS _replayed_at` into the source.
    Because Delta `INSERT *` and `UPDATE SET *` expand to all source columns, the target table
    MUST already have a `_replayed_at TIMESTAMP` column in its schema.
    """
    if not valid_filter_sql and not allow_unvalidated:
        raise ValueError(
            "Replay requires either a revalidation condition ('valid_filter_sql') or explicit "
            "confirmation ('allow_unvalidated=True') to prevent re-injecting unvalidated data."
        )

    if partition_val is not None and not partition_col:
        raise ValueError("partition_val requires partition_col to be specified.")

    clean_quarantine = _validate_identifier(quarantine_table)
    clean_target = _validate_identifier(target_table)
    clean_keys = [_validate_identifier(k) for k in primary_keys]
    clean_part = _validate_identifier(partition_col) if partition_col else None

    formatted_val = ""
    if partition_val is not None:
        if isinstance(partition_val, bool):
            formatted_val = "true" if partition_val else "false"
        elif isinstance(partition_val, (int, float)):
            if isinstance(partition_val, float) and (
                math.isnan(partition_val) or math.isinf(partition_val)
            ):
                raise ValueError(f"partition_val must be a finite number, got {partition_val}.")
            formatted_val = str(partition_val)
        else:
            escaped_val = str(partition_val).replace("'", "''")
            formatted_val = f"'{escaped_val}'"

    base_conditions: list[str] = []
    if rule_name:
        escaped_rule = rule_name.replace("'", "''")
        base_conditions.append(f"_failed_rules = array('{escaped_rule}')")
    if start_time:
        valid_start = _validate_iso_timestamp(start_time, "start_time")
        base_conditions.append(f"_quarantined_at >= '{valid_start}'")
    if end_time:
        valid_end = _validate_iso_timestamp(end_time, "end_time")
        base_conditions.append(f"_quarantined_at <= '{valid_end}'")
    if clean_part and partition_val is not None:
        base_conditions.append(f"{clean_part} = {formatted_val}")

    base_where_clause = f" WHERE {' AND '.join(base_conditions)}" if base_conditions else ""

    valid_filter_where = ""
    if valid_filter_sql:
        try:
            parsed_vf = sqlglot.parse(valid_filter_sql, read="spark")
            if len(parsed_vf) != 1 or parsed_vf[0] is None:
                raise ValueError("valid_filter_sql must be a single SQL expression.")
        except Exception as exc:
            raise ValueError(
                f"Failed to parse valid_filter_sql '{valid_filter_sql}': {exc}"
            ) from exc
        valid_filter_where = f"\n    WHERE ({valid_filter_sql.strip()})"

    join_predicates = [f"target.{k} = source.{k}" for k in clean_keys]
    if clean_part:
        join_predicates.append(f"target.{clean_part} = source.{clean_part}")
        if partition_val is not None:
            join_predicates.append(f"target.{clean_part} = {formatted_val}")
    pk_join = " AND ".join(join_predicates)

    if clean_part and clean_part not in clean_keys:
        dedupe_partition_cols = f"{', '.join(clean_keys)}, {clean_part}"
    else:
        dedupe_partition_cols = ", ".join(clean_keys)

    matched_clauses: list[str] = []
    if recency_col:
        clean_recency = _validate_identifier(recency_col)
        matched_clauses.append(
            f"WHEN MATCHED AND (target.{clean_recency} IS NULL OR source.{clean_recency} >= target.{clean_recency}) THEN UPDATE SET *"
        )

    select_clause = "* EXCEPT (_failed_rules, _quarantined_at)"
    if mark_replayed:
        select_clause += ",\n               current_timestamp() AS _replayed_at"

    lines = [
        "-- Replay SQL: Reprocess quarantined records into target table idempotently",
        "-- Note: Prefer re-feeding corrected rows into the upstream source stream rather than direct DML.",
        f"MERGE INTO {clean_target} AS target",
        "USING (",
        f"    SELECT {select_clause}",
        "    FROM (",
        "        SELECT *",
        f"        FROM {clean_quarantine}{base_where_clause}",
        f"        QUALIFY row_number() OVER (PARTITION BY {dedupe_partition_cols} ORDER BY _quarantined_at DESC) = 1",
        f"    ) AS deduped{valid_filter_where}",
        ") AS source",
        f"ON {pk_join}",
        *matched_clauses,
        "WHEN NOT MATCHED THEN INSERT *;",
    ]
    return "\n".join(lines)
    return "\n".join(lines)
