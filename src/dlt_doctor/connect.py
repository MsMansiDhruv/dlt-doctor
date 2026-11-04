"""Databricks workspace connector for live Delta Lake inspection."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from dlt_doctor.compaction import CompactionDoctor
from dlt_doctor.history import HistoryDoctor
from dlt_doctor.vacuum import VacuumDoctor


@dataclass
class TableInspectionReport:
    """Comprehensive live telemetry report for a Delta Lake table."""

    table_name: str
    num_files: int
    size_in_bytes: int
    avg_file_size_mb: float
    clustering_columns: list[str]
    partition_columns: list[str]
    commits_last_24h: int
    compaction_debt_status: str
    compaction_recommended_sql: str
    checkpoint_interval_advice: str
    vacuum_safety_status: str
    vacuum_recommended_sql: str
    action_items: list[str] = field(default_factory=list)

    def to_text(self) -> str:
        """Render human-readable telemetry summary."""
        lines = [
            "=" * 70,
            f" Databricks Live Table Telemetry: {self.table_name}",
            f" Size: {self.size_in_bytes / (1024**3):.2f} GB | Files: {self.num_files} | Avg File: {self.avg_file_size_mb:.1f} MB",
            "=" * 70,
            f"Clustering: {self.clustering_columns if self.clustering_columns else 'None (Unclustered)'}",
            f"Partitions: {self.partition_columns if self.partition_columns else 'None'}",
            f"Commit Velocity: {self.commits_last_24h} commits in past 24h",
            "",
            f"Compaction Debt: {self.compaction_debt_status}",
            f"  SQL: {self.compaction_recommended_sql}",
            "",
            f"Checkpoint Health: {self.checkpoint_interval_advice}",
            "",
            f"Vacuum Safety: {self.vacuum_safety_status}",
            f"  SQL: {self.vacuum_recommended_sql}",
            "-" * 70,
        ]
        return "\n".join(lines)


class WorkspaceInspector:
    """Connects to Databricks workspace to inspect Delta table properties."""

    def __init__(
        self,
        host: str | None = None,
        token: str | None = None,
        warehouse_id: str | None = None,
        mock: bool = False,
    ) -> None:
        self.host = (host or os.getenv("DATABRICKS_HOST") or "").rstrip("/")
        self.token = token or os.getenv("DATABRICKS_TOKEN", "")
        self.warehouse_id = warehouse_id or os.getenv("DATABRICKS_WAREHOUSE_ID", "")
        self.mock = mock
        self.compaction_doctor = CompactionDoctor()
        self.history_doctor = HistoryDoctor()
        self.vacuum_doctor = VacuumDoctor()

    def execute_sql(self, statement: str) -> list[dict[str, Any]]:
        """Execute a SQL query against Databricks SQL Statements API."""
        if not self.host or not self.token or not self.warehouse_id:
            raise ValueError(
                "Missing Databricks connection credentials (DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID)."
            )

        url = f"{self.host}/api/2.0/sql/statements"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        payload = json.dumps(
            {
                "warehouse_id": self.warehouse_id,
                "statement": statement,
                "wait_timeout": "30s",
            }
        ).encode("utf-8")

        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")  # nosec B310
        with urllib.request.urlopen(req, timeout=35) as resp:  # nosec B310
            data = json.loads(resp.read().decode("utf-8"))

        manifest = data.get("manifest", {}).get("schema", {}).get("columns", [])
        col_names = [c["name"] for c in manifest]
        data_rows = data.get("result", {}).get("data_array", [])

        rows: list[dict[str, Any]] = []
        for row in data_rows:
            rows.append(dict(zip(col_names, row, strict=False)))
        return rows

    def inspect_table(
        self,
        table_name: str,
        mock_detail: dict[str, Any] | None = None,
        mock_history: list[dict[str, Any]] | None = None,
    ) -> TableInspectionReport:
        """Inspect a table using live SQL API or provided metadata."""
        if mock_detail is None and self.mock:
            mock_detail = {
                "numFiles": 5000,
                "sizeInBytes": 20 * 1024 * 1024 * 1024,
                "clusteringColumns": ["customer_id", "order_date"],
                "partitionColumns": [],
            }
            mock_history = [{"version": i} for i in range(48)]

        if mock_detail is not None:
            detail = mock_detail
            history = mock_history or []
        else:
            detail_rows = self.execute_sql(f"DESCRIBE DETAIL {table_name}")
            if not detail_rows:
                raise ValueError(f"Table not found or no detail returned: {table_name}")
            detail = detail_rows[0]
            history = self.execute_sql(f"DESCRIBE HISTORY {table_name} LIMIT 30")

        num_files = int(detail.get("numFiles", 0) or 0)
        size_in_bytes = int(detail.get("sizeInBytes", 0) or 0)
        clustering = detail.get("clusteringColumns") or []
        if isinstance(clustering, str):
            try:
                clustering = json.loads(clustering)
            except Exception:
                clustering = [
                    c.strip() for c in clustering.strip("[]").split(",") if c.strip()
                ]
        partitions = detail.get("partitionColumns") or []

        avg_mb = (
            (size_in_bytes / (1024 * 1024 * max(1, num_files)))
            if num_files > 0
            else 0.0
        )

        # Run Compaction diagnosis
        sample_count = min(100, max(1, num_files))
        synthetic_sizes = [int(avg_mb * 1024 * 1024)] * sample_count
        compaction = self.compaction_doctor.analyze_file_distribution(
            table_name=table_name,
            file_sizes_bytes=synthetic_sizes,
        )

        # Run History diagnosis
        commits_24h = len(history)
        history_eval = self.history_doctor.evaluate_commit_velocity(
            table_name=table_name,
            commits_per_hour=max(1, int(commits_24h / 24.0)),
        )

        # Run Vacuum diagnosis
        vac = self.vacuum_doctor.evaluate_vacuum_safety(
            table_name=table_name,
            requested_retention_hours=24,
            active_streaming_readers=1,
            cdf_enabled=True,
        )

        debt_label = (
            "HIGH (Compaction Debt)" if compaction.has_compaction_debt else "OPTIMAL"
        )
        optimize_sql = compaction.recommended_optimize_sql
        if clustering:
            cluster_cols = ", ".join(clustering)
            optimize_sql = f"OPTIMIZE {table_name} CLUSTER BY ({cluster_cols});"

        checkpoint_advice = (
            f"Interval: {history_eval.recommended_checkpoint_interval} commits. "
            f"Status: {'Bloat Risk' if history_eval.has_log_bloat_risk else 'Healthy'}"
        )

        return TableInspectionReport(
            table_name=table_name,
            num_files=num_files,
            size_in_bytes=size_in_bytes,
            avg_file_size_mb=avg_mb,
            clustering_columns=clustering,
            partition_columns=partitions,
            commits_last_24h=commits_24h,
            compaction_debt_status=debt_label,
            compaction_recommended_sql=optimize_sql,
            checkpoint_interval_advice=checkpoint_advice,
            vacuum_safety_status=vac.safety_level.value,
            vacuum_recommended_sql=vac.recommended_vacuum_sql,
        )
