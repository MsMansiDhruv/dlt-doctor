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
from dlt_doctor.cluster import (
    ClusterDoctor,
    ColumnProfile,
    LiquidClusterRecommendation,
)
from dlt_doctor.compaction import CompactionDebtReport, CompactionDoctor
from dlt_doctor.expectations import (
    ExpectationDoctor,
    ExpectationRule,
    QuarantineRoutingPlan,
)
from dlt_doctor.reconciler import BlastRadiusScope, RestatementDoctor
from dlt_doctor.skew import SkewAnalysisResult, SkewDoctor
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

__version__ = "0.3.0"

__all__ = [
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
    "AdvisorDoctor",
    "PipelineAuditReport",
]
