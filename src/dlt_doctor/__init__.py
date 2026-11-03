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
from dlt_doctor.compaction import CompactionDebtReport, CompactionDoctor
from dlt_doctor.expectations import (
    ExpectationDoctor,
    ExpectationRule,
    QuarantineRoutingPlan,
)
from dlt_doctor.reconciler import BlastRadiusScope, RestatementDoctor
from dlt_doctor.skew import SkewAnalysisResult, SkewDoctor

__version__ = "0.2.0"

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
    "AdvisorDoctor",
    "PipelineAuditReport",
]
