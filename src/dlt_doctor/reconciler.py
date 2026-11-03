"""Delta Lake surgical MERGE restatement and transaction log inspection."""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Any


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
    ) -> BlastRadiusScope:
        """Calculate the exact affected partition window [t_late, t_late + W - 1]."""
        t_late = datetime.date.fromisoformat(late_date_iso)
        t_end = t_late + datetime.timedelta(days=window_size_days - 1)

        t_late_str = t_late.isoformat()
        t_end_str = t_end.isoformat()

        pruning_clause = f"{partition_col} BETWEEN '{t_late_str}' AND '{t_end_str}'"

        merge_sql = (
            f"MERGE INTO {target_table} AS target "  # nosec B608
            f"USING source_recalculated AS source "
            f"ON target.{partition_col} = source.{partition_col} "
            f"AND target.{match_key} = source.{match_key} "
            f"AND target.{partition_col} BETWEEN '{t_late_str}' AND '{t_end_str}' "
            f"WHEN MATCHED THEN UPDATE SET * "
            f"WHEN NOT MATCHED THEN INSERT *;"
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
        """Generate optimal Delta Liquid Clustering DDL statement."""
        # Pick top 2-4 candidate keys
        candidates: list[str] = []
        for col in join_columns + filter_columns:
            clean = col.strip().lower()
            if clean and clean not in candidates:
                candidates.append(clean)

        chosen_keys = candidates[:3]
        keys_str = ", ".join(chosen_keys)
        return f"ALTER TABLE {table_name} CLUSTER BY ({keys_str});"  # nosec B608
