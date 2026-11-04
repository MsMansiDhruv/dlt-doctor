"""Automated replay SQL generator for reprocessing quarantined rows into target tables."""

from __future__ import annotations

import re

IDENTIFIER_PATTERN = re.compile(
    r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$|^`[a-zA-Z0-9_\.]+`$"
)


def _validate_identifier(identifier: str) -> str:
    if not IDENTIFIER_PATTERN.match(identifier.strip()):
        raise ValueError(f"Invalid SQL identifier '{identifier}'.")
    return identifier.strip()


def generate_replay_sql(
    quarantine_table: str,
    target_table: str,
    primary_keys: list[str],
    rule_name: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
) -> str:
    """Generate idempotent Delta MERGE SQL to reprocess quarantined records.

    Note on AUTO CDC:
    When replaying records into pipelines using declarative apply_changes / AUTO CDC,
    ensure replay batches contain valid sequence IDs higher than the last processed
    version to prevent silent out-of-order drop semantics.
    """
    clean_quarantine = _validate_identifier(quarantine_table)
    clean_target = _validate_identifier(target_table)
    clean_keys = [_validate_identifier(k) for k in primary_keys]

    conditions: list[str] = []
    if rule_name:
        escaped_rule = rule_name.replace("'", "''")
        conditions.append(f"array_contains(_failed_rules, '{escaped_rule}')")
    if start_time:
        conditions.append(f"_quarantined_at >= '{start_time}'")
    if end_time:
        conditions.append(f"_quarantined_at <= '{end_time}'")

    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    pk_join = " AND ".join(f"target.{k} = source.{k}" for k in clean_keys)

    lines = [
        "-- Replay SQL: Reprocess quarantined records into target table idempotently",
        f"MERGE INTO {clean_target} AS target",
        "USING (",
        "    SELECT * EXCEPT (_failed_rules, _quarantined_at)",
        f"    FROM {clean_quarantine}{where_clause}",
        ") AS source",
        f"ON {pk_join}",
        "WHEN MATCHED THEN UPDATE SET *",
        "WHEN NOT MATCHED THEN INSERT *;",
    ]
    return "\n".join(lines)
