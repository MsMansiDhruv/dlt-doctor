"""Delta Lake VACUUM safety analyzer and tombstone retention auditor."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class VacuumSafetyLevel(str, Enum):
    """Safety grade for proposed VACUUM operation."""

    SAFE = "SAFE"
    WARNING = "WARNING"
    DANGEROUS = "DANGEROUS"


@dataclass
class VacuumSafetyReport:
    """Safety evaluation and generated maintenance SQL for Delta VACUUM."""

    table_name: str
    safety_level: VacuumSafetyLevel
    safe_retention_hours: int
    recommended_vacuum_sql: str
    dry_run_sql: str
    warnings: list[str] = field(default_factory=list)
    guidance: str = ""


class VacuumDoctor:
    """Audits Delta table VACUUM retention to prevent stream FileNotFoundException crashes."""

    MIN_DEFAULT_RETENTION_HOURS = 168  # 7 days (standard Databricks Delta retention)

    def evaluate_vacuum_safety(
        self,
        table_name: str,
        requested_retention_hours: int = 168,
        max_streaming_lag_hours: float = 0.0,
        cdf_enabled: bool = False,
        active_streaming_readers: int = 0,
    ) -> VacuumSafetyReport:
        """Evaluate retention period safety against streaming and CDF reader constraints."""
        warnings: list[str] = []
        safety_level = VacuumSafetyLevel.SAFE

        # 1. Check against aggressive retention with active streaming readers
        if requested_retention_hours < self.MIN_DEFAULT_RETENTION_HOURS:
            if active_streaming_readers > 0:
                safety_level = VacuumSafetyLevel.DANGEROUS
                warnings.append(
                    f"Retention of {requested_retention_hours}h is below default 168h while {active_streaming_readers} "
                    "streaming reader(s) are active. This risks fatal FileNotFoundException crashes on tombstoned files."
                )
            else:
                safety_level = VacuumSafetyLevel.WARNING
                warnings.append(
                    f"Requested retention {requested_retention_hours}h requires disabling "
                    "'spark.databricks.delta.vacuum.parallelDelete.enabled' or safety checks."
                )

        # 2. Check against CDF reader lag
        if cdf_enabled and requested_retention_hours < self.MIN_DEFAULT_RETENTION_HOURS:
            warnings.append(
                f"Change Data Feed (CDF) is enabled on {table_name}. Retaining < 168h may truncate CDF history "
                "before downstream consumer pipelines can process commit deltas."
            )
            if safety_level != VacuumSafetyLevel.DANGEROUS:
                safety_level = VacuumSafetyLevel.WARNING

        # 3. Calculate safe retention threshold
        safe_hours = max(
            self.MIN_DEFAULT_RETENTION_HOURS,
            int(max_streaming_lag_hours * 2.0),
        )

        vacuum_sql = f"VACUUM {table_name} RETAIN {safe_hours} HOURS;"
        dry_run_sql = f"VACUUM {table_name} RETAIN {safe_hours} HOURS DRY RUN;"

        guidance = (
            f"Always execute the DRY RUN first to inspect the list of tombstoned candidate Parquet files. "
            f"Configured safe retention is {safe_hours} hours."
        )

        return VacuumSafetyReport(
            table_name=table_name,
            safety_level=safety_level,
            safe_retention_hours=safe_hours,
            recommended_vacuum_sql=vacuum_sql,
            dry_run_sql=dry_run_sql,
            warnings=warnings,
            guidance=guidance,
        )
