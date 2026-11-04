"""Delta Lake transaction log commit velocity and checkpoint interval optimizer."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class LogVelocityReport:
    """Findings and table property recommendations for Delta transaction log management."""

    table_name: str
    commits_per_hour: int
    commits_per_day: int
    estimated_monthly_log_files: int
    has_log_bloat_risk: bool
    recommended_checkpoint_interval: int
    recommended_tblproperties_ddl: str
    warnings: list[str] = field(default_factory=list)
    guidance: str = ""


class HistoryDoctor:
    """Audits Delta commit frequency to prevent transaction log replay degradation."""

    DEFAULT_CHECKPOINT_INTERVAL = 10

    def evaluate_commit_velocity(
        self,
        table_name: str,
        commits_per_hour: int,
        current_checkpoint_interval: int = 10,
        log_retention_days: int = 30,
    ) -> LogVelocityReport:
        """Evaluate commit frequency and recommend checkpointing / log retention properties."""
        commits_per_day = commits_per_hour * 24
        # Each commit creates 1 JSON log + 1 Parquet checkpoint every checkpoint_interval commits
        checkpoints_per_day = commits_per_day / max(current_checkpoint_interval, 1)
        total_daily_log_files = commits_per_day + checkpoints_per_day
        monthly_log_files = int(total_daily_log_files * min(log_retention_days, 30))

        warnings: list[str] = []
        has_bloat_risk = monthly_log_files > 25000

        # Optimal checkpoint interval:
        # For high-velocity streaming (> 60 commits/hr), write checkpoints every 50-100 commits
        if commits_per_hour >= 360:  # <= 10s micro-batch
            rec_checkpoint_interval = 100
            rec_retention = "interval 7 days"
            warnings.append(
                f"Ultra-high commit velocity ({commits_per_hour} commits/hr) generates ~{monthly_log_files:,} "
                "log files monthly. This causes multi-second query planning latencies during log replay."
            )
        elif commits_per_hour >= 60:  # <= 1m micro-batch
            rec_checkpoint_interval = 50
            rec_retention = "interval 14 days"
            warnings.append(
                f"High streaming commit velocity ({commits_per_hour} commits/hr). "
                "Default checkpoint interval of 10 creates excessive checkpoint metadata files."
            )
        else:
            rec_checkpoint_interval = 10
            rec_retention = f"interval {log_retention_days} days"

        tblproperties = (
            f"ALTER TABLE {table_name} SET TBLPROPERTIES (\n"
            f"  'delta.checkpointInterval' = '{rec_checkpoint_interval}',\n"
            f"  'delta.logRetentionDuration' = '{rec_retention}'\n"
            f");"
        )

        guidance = (
            f"Applying delta.checkpointInterval={rec_checkpoint_interval} reduces metadata storage IOPS "
            f"while preserving fast state reconstruction for time-travel queries."
        )

        return LogVelocityReport(
            table_name=table_name,
            commits_per_hour=commits_per_hour,
            commits_per_day=commits_per_day,
            estimated_monthly_log_files=monthly_log_files,
            has_log_bloat_risk=has_bloat_risk,
            recommended_checkpoint_interval=rec_checkpoint_interval,
            recommended_tblproperties_ddl=tblproperties,
            warnings=warnings,
            guidance=guidance,
        )
