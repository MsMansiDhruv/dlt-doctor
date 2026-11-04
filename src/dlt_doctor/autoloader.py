"""Auto Loader (cloudFiles) streaming diagnostics, configuration, and monitoring."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dlt_doctor.catalyst import DiagnosticFinding, DiagnosticSeverity


@dataclass
class AutoLoaderConfig:
    """Configured Databricks Auto Loader options."""

    format: str
    source_path: str
    schema_location: str
    schema_evolution_mode: str = "rescue"
    infer_column_types: bool = True
    max_files_per_trigger: int | None = 1000
    use_notifications: bool = False
    extra_options: dict[str, str] = field(default_factory=dict)

    def to_spark_options(self) -> dict[str, str]:
        """Convert configuration into PySpark .readStream.format('cloudFiles').options(...) map."""
        opts: dict[str, str] = {
            "cloudFiles.format": self.format,
            "cloudFiles.schemaLocation": self.schema_location,
            "cloudFiles.schemaEvolutionMode": self.schema_evolution_mode,
            "cloudFiles.inferColumnTypes": str(self.infer_column_types).lower(),
        }
        if self.max_files_per_trigger is not None:
            opts["cloudFiles.maxFilesPerTrigger"] = str(self.max_files_per_trigger)
        if self.use_notifications:
            opts["cloudFiles.useNotifications"] = "true"
        opts.update(self.extra_options)
        return opts


@dataclass
class StreamHealthSummary:
    """Telemetry summary of an active Auto Loader structured stream."""

    is_healthy: bool
    is_backpressured: bool
    input_rate_avg: float
    process_rate_avg: float
    total_input_rows: int
    estimated_backlog_rows: int = 0
    findings: list[DiagnosticFinding] = field(default_factory=list)


class AutoLoaderDoctor:
    """Validates Auto Loader configurations and diagnoses streaming bottlenecks."""

    def validate_options(self, options: dict[str, str]) -> list[DiagnosticFinding]:
        """Audit an Auto Loader option map for production anti-patterns."""
        findings: list[DiagnosticFinding] = []

        # 1. Missing schemaLocation
        if "cloudFiles.schemaLocation" not in options:
            findings.append(
                DiagnosticFinding(
                    rule_id="AUTOLOADER-001",
                    severity=DiagnosticSeverity.CRITICAL,
                    title="Missing cloudFiles.schemaLocation",
                    impact_summary=(
                        "Auto Loader cannot track schema evolution across restarts without a persistent schema location."
                    ),
                    recommendation="Set .option('cloudFiles.schemaLocation', '/path/to/schema_checkpoint').",
                    estimated_dbu_impact="Critical",
                )
            )

        # 2. Dangerous schemaEvolutionMode
        mode = options.get("cloudFiles.schemaEvolutionMode", "addNewColumns").lower()
        if mode in ("none", "failonnewcolumns"):
            findings.append(
                DiagnosticFinding(
                    rule_id="AUTOLOADER-002",
                    severity=DiagnosticSeverity.HIGH,
                    title=f"Restrictive schema evolution mode: '{mode}'",
                    impact_summary=(
                        f"Mode '{mode}' will fail the streaming pipeline immediately when upstream data introduces a new column."
                    ),
                    recommendation=(
                        "Use 'rescue' or 'addNewColumns' to gracefully capture schema evolution into _rescued_data."
                    ),
                    estimated_dbu_impact="High (pipeline downtime)",
                )
            )

        # 3. Missing maxFilesPerTrigger on large directory scans
        if (
            "cloudFiles.maxFilesPerTrigger" not in options
            and "cloudFiles.maxBytesPerTrigger" not in options
        ):
            findings.append(
                DiagnosticFinding(
                    rule_id="AUTOLOADER-003",
                    severity=DiagnosticSeverity.MEDIUM,
                    title="Unbounded micro-batch size (missing maxFilesPerTrigger)",
                    impact_summary=(
                        "Without maxFilesPerTrigger, an initial backfill or sudden file arrival spike "
                        "can ingest millions of files in one micro-batch, causing driver OOM."
                    ),
                    recommendation="Set .option('cloudFiles.maxFilesPerTrigger', 1000) for deterministic batch sizes.",
                    estimated_dbu_impact="Medium",
                )
            )

        return findings

    def diagnose_stream_progress(
        self, progress_batches: list[dict[str, Any]] | dict[str, Any]
    ) -> StreamHealthSummary:
        """Inspect recent micro-batch progress metrics from query.recentProgress."""
        if isinstance(progress_batches, dict):
            progress_batches = [progress_batches]

        if not progress_batches:
            return StreamHealthSummary(
                is_healthy=True,
                is_backpressured=False,
                input_rate_avg=0.0,
                process_rate_avg=0.0,
                total_input_rows=0,
                findings=[],
            )

        findings: list[DiagnosticFinding] = []
        input_rates: list[float] = []
        process_rates: list[float] = []
        total_rows = 0

        for batch in progress_batches:
            in_rate = batch.get("inputRowsPerSecond", 0.0)
            proc_rate = batch.get("processedRowsPerSecond", 0.0)
            num_rows = batch.get("numInputRows", 0)

            input_rates.append(in_rate)
            process_rates.append(proc_rate)
            total_rows += num_rows

        avg_in = sum(input_rates) / len(input_rates) if input_rates else 0.0
        avg_proc = sum(process_rates) / len(process_rates) if process_rates else 0.0

        is_backpressured = False
        estimated_backlog = 0
        if avg_in > (avg_proc * 1.5) and avg_in > 100.0:
            is_backpressured = True
            estimated_backlog = int((avg_in - avg_proc) * 60)
            findings.append(
                DiagnosticFinding(
                    rule_id="AUTOLOADER-004",
                    severity=DiagnosticSeverity.HIGH,
                    title="Auto Loader stream backpressure detected",
                    impact_summary=(
                        f"Average input rate ({avg_in:.1f} rows/s) exceeds processing rate ({avg_proc:.1f} rows/s). "
                        "Micro-batch latency is accumulating, leading to SLA breaches."
                    ),
                    recommendation=(
                        "Scale worker nodes, optimize stateful transformations, or increase cluster cores."
                    ),
                    estimated_dbu_impact="High",
                )
            )

        return StreamHealthSummary(
            is_healthy=not findings,
            is_backpressured=is_backpressured,
            input_rate_avg=round(avg_in, 2),
            process_rate_avg=round(avg_proc, 2),
            total_input_rows=total_rows,
            estimated_backlog_rows=estimated_backlog,
            findings=findings,
        )
