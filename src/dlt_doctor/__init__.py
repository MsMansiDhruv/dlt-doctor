"""dlt-doctor: Production optimization engine for Databricks DLT, Auto Loader, and PySpark."""

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
from dlt_doctor.connect import TableInspectionReport, WorkspaceInspector
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
from dlt_doctor.history import HistoryDoctor, LogVelocityReport
from dlt_doctor.magic import audit_code_cell
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

__version__ = "0.4.2"

__all__ = [
    "DirectoryScanner",
    "DirectoryAuditReport",
    "extract_notebook_code",
    "is_notebook_file",
    "audit_code_cell",
    "WorkspaceInspector",
    "TableInspectionReport",
    "PlanDoctor",
    "DiagnosticFinding",
    "DiagnosticSeverity",
    "AutoLoaderDoctor",
    "AutoLoaderConfig",
    "StreamHealthSummary",
    "ExpectationDoctor",
    "ExpectationRule",
    "QuarantineRoutingPlan",
    "RestatementDoctor",
    "BlastRadiusScope",
    "SkewDoctor",
    "SkewAnalysisResult",
    "CompactionDoctor",
    "CompactionDebtReport",
    "CdcDoctor",
    "CdcMergeTemplate",
    "ClusterDoctor",
    "ColumnProfile",
    "LiquidClusterRecommendation",
    "UcDoctor",
    "UcFinding",
    "UcMigrationPlan",
    "UcMigrationSeverity",
    "VacuumDoctor",
    "VacuumSafetyLevel",
    "VacuumSafetyReport",
    "SchemaDoctor",
    "SchemaEvolutionReport",
    "SchemaChange",
    "SchemaDriftSeverity",
    "CostDoctor",
    "DbuCostForecast",
    "ClusterSizingProfile",
    "SpillDoctor",
    "ShuffleTuningAdvice",
    "HistoryDoctor",
    "LogVelocityReport",
    "CheckpointDoctor",
    "CheckpointHealthReport",
    "CheckpointIntegrityStatus",
    "AdvisorDoctor",
    "PipelineAuditReport",
]
