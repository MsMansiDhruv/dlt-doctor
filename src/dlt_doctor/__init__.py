"""dlt-doctor: Production optimization engine for Databricks DLT, Auto Loader, and PySpark."""

from __future__ import annotations

from typing import Any

from dlt_doctor.advisor import AdvisorDoctor, PipelineAuditReport
from dlt_doctor.autoloader import (
    AutoLoaderConfig,
    AutoLoaderDoctor,
    StreamHealthSummary,
)
from dlt_doctor.catalyst import (
    DiagnosticFinding,
    DiagnosticSeverity,
    PlanDoctor,
)
from dlt_doctor.cdc import CdcDoctor, CdcMergeTemplate
from dlt_doctor.checkpoint import (
    CheckpointDoctor,
    CheckpointHealthReport,
    CheckpointIntegrityStatus,
)
from dlt_doctor.cluster import (
    ClusterDoctor,
    ColumnProfile,
    LiquidClusterRecommendation,
)
from dlt_doctor.compaction import CompactionDebtReport, CompactionDoctor
from dlt_doctor.cost import (
    ClusterSizingProfile,
    CostDoctor,
    DbuCostForecast,
)
from dlt_doctor.expectations import (
    ExpectationDoctor,
    ExpectationRule,
    QuarantineRoutingPlan,
)
from dlt_doctor.heal import HealResult, auto_heal_source, heal_file
from dlt_doctor.history import HistoryDoctor, LogVelocityReport
from dlt_doctor.notebook import extract_notebook_code, is_notebook_file
from dlt_doctor.reconciler import BlastRadiusScope, RestatementDoctor
from dlt_doctor.scanner import DirectoryAuditReport, DirectoryScanner
from dlt_doctor.schema import (
    SchemaChange,
    SchemaDoctor,
    SchemaDriftSeverity,
    SchemaEvolutionReport,
)
from dlt_doctor.skew import SkewAnalysisResult, SkewDoctor
from dlt_doctor.spill import ShuffleTuningAdvice, SpillDoctor
from dlt_doctor.uc import (
    UcDoctor,
    UcFinding,
    UcMigrationPlan,
    UcMigrationSeverity,
)
from dlt_doctor.vacuum import (
    VacuumDoctor,
    VacuumSafetyLevel,
    VacuumSafetyReport,
)

__version__ = "0.5.0"

__all__ = [
    "AdvisorDoctor",
    "AutoLoaderConfig",
    "AutoLoaderDoctor",
    "BlastRadiusScope",
    "CdcDoctor",
    "CdcMergeTemplate",
    "CheckpointDoctor",
    "CheckpointHealthReport",
    "CheckpointIntegrityStatus",
    "ClusterDoctor",
    "ClusterSizingProfile",
    "ColumnProfile",
    "CompactionDebtReport",
    "CompactionDoctor",
    "CostDoctor",
    "DbuCostForecast",
    "DiagnosticFinding",
    "DiagnosticSeverity",
    "DirectoryAuditReport",
    "DirectoryScanner",
    "ExpectationDoctor",
    "ExpectationRule",
    "HealResult",
    "HistoryDoctor",
    "LiquidClusterRecommendation",
    "LogVelocityReport",
    "PipelineAuditReport",
    "PlanDoctor",
    "QuarantineRoutingPlan",
    "RestatementDoctor",
    "SchemaChange",
    "SchemaDoctor",
    "SchemaDriftSeverity",
    "SchemaEvolutionReport",
    "ShuffleTuningAdvice",
    "SkewAnalysisResult",
    "SkewDoctor",
    "SpillDoctor",
    "StreamHealthSummary",
    "TableInspectionReport",
    "UcDoctor",
    "UcFinding",
    "UcMigrationPlan",
    "UcMigrationSeverity",
    "VacuumDoctor",
    "VacuumSafetyLevel",
    "VacuumSafetyReport",
    "WorkspaceInspector",
    "audit_code_cell",
    "auto_heal_source",
    "extract_notebook_code",
    "heal_file",
    "is_notebook_file",
    "load_ipython_extension",
]


def __getattr__(name: str) -> Any:
    """Lazily load interactive magic and live telemetry modules on access."""
    if name in {"load_ipython_extension", "audit_code_cell"}:
        import dlt_doctor.magic as magic_mod

        return getattr(magic_mod, name)
    if name in {"WorkspaceInspector", "TableInspectionReport"}:
        import dlt_doctor.connect as connect_mod

        return getattr(connect_mod, name)
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + __all__)
