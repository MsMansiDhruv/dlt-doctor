"""Tests for AdvisorDoctor report generation."""

from dlt_doctor.advisor import AdvisorDoctor
from dlt_doctor.catalyst import DiagnosticFinding, DiagnosticSeverity


def test_advisor_report_compilation() -> None:
    advisor = AdvisorDoctor()
    findings = [
        DiagnosticFinding(
            rule_id="PYSPARK-001",
            severity=DiagnosticSeverity.CRITICAL,
            title="Python UDF serialization",
            impact_summary="Socket overhead",
            recommendation="Use native F",
        ),
        DiagnosticFinding(
            rule_id="PYSPARK-002",
            severity=DiagnosticSeverity.HIGH,
            title="withColumn cascade",
            impact_summary="Plan bloat",
            recommendation="Use withColumns",
        ),
    ]

    report = advisor.compile_report("test_pipeline", findings)
    assert report.overall_health_score < 70
    assert report.estimated_dbu_savings_pct >= 40

    md = advisor.to_markdown(report)
    assert "# DLT-Doctor Pipeline Audit: test_pipeline" in md
    assert "PYSPARK-001" in md

    html = advisor.to_databricks_html(report)
    assert "DLT-Doctor: test_pipeline" in html
    assert "ESTIMATED DBU SAVINGS" in html
