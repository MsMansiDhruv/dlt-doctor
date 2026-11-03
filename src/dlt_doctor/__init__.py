"""dlt-doctor: Production optimization engine for Databricks DLT, Auto Loader, and PySpark."""

from dlt_doctor.advisor import AdvisorDoctor, PipelineAuditReport
from dlt_doctor.autoloader import (
    AutoLoaderConfig,
    AutoLoaderDoctor,
    StreamHealthSummary,
)
from dlt_doctor.catalyst import DiagnosticFinding, DiagnosticSeverity, PlanDoctor
from dlt_doctor.expectations import (
    ExpectationDoctor,
    ExpectationRule,
    QuarantineRoutingPlan,
)
from dlt_doctor.reconciler import BlastRadiusScope, RestatementDoctor

__version__ = "0.1.0"

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
    "AdvisorDoctor",
    "PipelineAuditReport",
]
