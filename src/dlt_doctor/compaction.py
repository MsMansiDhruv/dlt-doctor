"""Delta Lake small-file fragmentation and compaction debt analysis."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dlt_doctor.catalyst import DiagnosticFinding, DiagnosticSeverity


@dataclass
class CompactionDebtReport:
    """Audit of small file fragmentation in a Delta Lake table."""

    table_name: str
    total_files: int
    small_files_count: int
    small_file_pct: float
    average_file_size_mb: float
    has_compaction_debt: bool
    recommended_optimize_sql: str
    recommended_tblproperties_sql: str
    findings: list[DiagnosticFinding] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "total_files": self.total_files,
            "small_files_count": self.small_files_count,
            "small_file_pct": self.small_file_pct,
            "average_file_size_mb": self.average_file_size_mb,
            "has_compaction_debt": self.has_compaction_debt,
            "recommended_optimize_sql": self.recommended_optimize_sql,
            "recommended_tblproperties_sql": self.recommended_tblproperties_sql,
            "findings": [f.to_dict() for f in self.findings],
        }


class CompactionDoctor:
    """Detects small-file fragmentation and generates targeted Delta OPTIMIZE schedules."""

    def __init__(self, target_file_size_mb: int = 128) -> None:
        self.target_file_size_mb = target_file_size_mb

    def analyze_file_distribution(
        self,
        table_name: str,
        file_sizes_bytes: list[int],
        small_file_threshold_mb: int = 32,
    ) -> CompactionDebtReport:
        """Evaluate file size array from Delta transaction log metadata."""
        if not file_sizes_bytes:
            return CompactionDebtReport(
                table_name=table_name,
                total_files=0,
                small_files_count=0,
                small_file_pct=0.0,
                average_file_size_mb=0.0,
                has_compaction_debt=False,
                recommended_optimize_sql=f"OPTIMIZE {table_name};",  # nosec B608
                recommended_tblproperties_sql=(
                    f"ALTER TABLE {table_name} SET TBLPROPERTIES ("  # nosec B608
                    "delta.autoOptimize.optimizeWrite = true, "
                    "delta.autoOptimize.autoCompact = true);"
                ),
            )

        threshold_bytes = small_file_threshold_mb * 1024 * 1024
        total_files = len(file_sizes_bytes)
        small_files = sum(1 for size in file_sizes_bytes if size < threshold_bytes)
        small_file_pct = round((small_files / total_files) * 100.0, 1)

        total_bytes = sum(file_sizes_bytes)
        avg_mb = round((total_bytes / total_files) / (1024 * 1024), 2)

        has_debt = small_file_pct >= 40.0 and total_files >= 20
        findings: list[DiagnosticFinding] = []

        optimize_sql = f"OPTIMIZE {table_name};"  # nosec B608
        tblprops_sql = (
            f"ALTER TABLE {table_name} SET TBLPROPERTIES ("  # nosec B608
            "delta.autoOptimize.optimizeWrite = true, "
            "delta.autoOptimize.autoCompact = true);"
        )

        if has_debt:
            findings.append(
                DiagnosticFinding(
                    rule_id="COMPACT-001",
                    severity=DiagnosticSeverity.HIGH,
                    title="High small-file compaction debt in Delta table",
                    impact_summary=(
                        f"{small_file_pct}% of files ({small_files}/{total_files}) in '{table_name}' "
                        f"are below {small_file_threshold_mb}MB (Average size: {avg_mb}MB). "
                        "Causes metadata listing bottlenecks and high scan latencies in downstream queries."
                    ),
                    recommendation=(
                        f"Run targeted compaction: '{optimize_sql}', and enable Auto-Compaction: '{tblprops_sql}'."
                    ),
                    estimated_dbu_impact="High (3x-5x query scan speedup)",
                )
            )

        return CompactionDebtReport(
            table_name=table_name,
            total_files=total_files,
            small_files_count=small_files,
            small_file_pct=small_file_pct,
            average_file_size_mb=avg_mb,
            has_compaction_debt=has_debt,
            recommended_optimize_sql=optimize_sql,
            recommended_tblproperties_sql=tblprops_sql,
            findings=findings,
        )
