"""Delta Lake surgical MERGE restatement and transaction log inspection."""

from __future__ import annotations

import datetime
import re
import warnings
from dataclasses import dataclass
from typing import Any

IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$|^`[a-zA-Z0-9_\.]+`$")


def _validate_identifier(identifier: str) -> str:
    """Validate SQL identifier against injection patterns."""
    if not IDENTIFIER_PATTERN.match(identifier.strip()):
        raise ValueError(f"Invalid SQL identifier '{identifier}'. Only alphanumeric and dotted names are allowed.")
    return identifier.strip()


@dataclass
class BlastRadiusScope:
    """Bounded partition dates invalidated by delayed data."""

    late_partition_date: str
    window_size_days: int
    blast_radius_start: str
    blast_radius_end: str
    partition_pruning_clause: str
    delta_merge_sql: str


@dataclass
class DeltaCommitLogMetric:
    """Metrics extracted from Delta Lake transaction log."""

    version: int
    timestamp: str
    operation: str
    num_added_files: int
    num_removed_files: int
    total_bytes_added: int = 0


class RestatementDoctor:
    """Calculates surgical blast-radius reconciliation and Delta MERGE partition pruning."""

    def calculate_blast_radius(
        self,
        target_table: str,
        late_date_iso: str,
        window_size_days: int,
        partition_col: str = "order_date",
        match_key: str = "id",
        source_table: str = "source_recalculated",
        delete_unmatched: bool = False,
    ) -> BlastRadiusScope:
        """Calculate the exact affected partition window [t_late, t_late + W - 1]."""
        if window_size_days < 1:
            raise ValueError(f"window_size_days must be at least 1, got {window_size_days}.")

        clean_target = _validate_identifier(target_table)
        clean_source = _validate_identifier(source_table)
        clean_part = _validate_identifier(partition_col)
        clean_key = _validate_identifier(match_key)

        t_late = datetime.date.fromisoformat(late_date_iso)
        t_end = t_late + datetime.timedelta(days=window_size_days - 1)

        t_late_str = t_late.isoformat()
        t_end_str = t_end.isoformat()

        pruning_clause = f"{clean_part} BETWEEN '{t_late_str}' AND '{t_end_str}'"

        delete_clause = ""
        if delete_unmatched:
            warnings.warn(
                "delete_unmatched is enabled: this will permanently DELETE records in target within the "
                "partition window that are missing from source. Ensure source represents a complete, "
                "authoritative replacement for the partition range.",
                category=UserWarning,
                stacklevel=2,
            )
            delete_clause = f" WHEN NOT MATCHED BY SOURCE AND target.{clean_part} BETWEEN '{t_late_str}' AND '{t_end_str}' THEN DELETE"

        merge_sql = (
            f"MERGE INTO {clean_target} AS target "  # nosec B608
            f"USING {clean_source} AS source "
            f"ON target.{clean_part} = source.{clean_part} "
            f"AND target.{clean_key} = source.{clean_key} "
            f"AND target.{clean_part} BETWEEN '{t_late_str}' AND '{t_end_str}' "
            f"WHEN MATCHED THEN UPDATE SET * "
            f"WHEN NOT MATCHED AND source.{clean_part} BETWEEN '{t_late_str}' AND '{t_end_str}' THEN INSERT *{delete_clause};"
        )

        return BlastRadiusScope(
            late_partition_date=t_late_str,
            window_size_days=window_size_days,
            blast_radius_start=t_late_str,
            blast_radius_end=t_end_str,
            partition_pruning_clause=pruning_clause,
            delta_merge_sql=merge_sql,
        )

    def analyze_commit_history(
        self,
        commits: list[dict[str, Any]],
    ) -> list[DeltaCommitLogMetric]:
        """Parse raw Delta commit histories to detect file write bloat or compaction debt."""
        metrics: list[DeltaCommitLogMetric] = []
        for c in commits:
            metrics.append(
                DeltaCommitLogMetric(
                    version=c.get("version", 0),
                    timestamp=c.get("timestamp", ""),
                    operation=c.get("operation", "UNKNOWN"),
                    num_added_files=c.get("num_added_files", 0),
                    num_removed_files=c.get("num_removed_files", 0),
                    total_bytes_added=c.get("total_bytes_added", 0),
                )
            )
        return metrics

    def advise_liquid_clustering(
        self,
        table_name: str,
        filter_columns: list[str],
        join_columns: list[str],
    ) -> str:
        """Generate optimal Delta Liquid Clustering DDL statement (backward compatible)."""
        clean_table = _validate_identifier(table_name)
        candidates: list[str] = []
        for col in join_columns + filter_columns:
            clean = col.strip().lower()
            if clean and clean not in candidates:
                candidates.append(clean)

        chosen_keys = candidates[:3]
        keys_str = ", ".join(chosen_keys)
        return f"ALTER TABLE {clean_table} CLUSTER BY ({keys_str});"
