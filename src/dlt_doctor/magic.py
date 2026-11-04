"""Interactive Databricks notebook and IPython cell magic integration."""

from __future__ import annotations

from typing import Any

from dlt_doctor.advisor import AdvisorDoctor
from dlt_doctor.catalyst import PlanDoctor


def audit_code_cell(code: str, cell_title: str = "Notebook Cell") -> str:
    """Analyze a single code block and generate an interactive Databricks HTML scorecard."""
    doctor = PlanDoctor()
    advisor = AdvisorDoctor()
    findings = doctor.analyze_source_code(code)
    report = advisor.compile_report(cell_title, findings)
    return advisor.to_databricks_html(report)


def dlt_doctor_cell_magic(line: str, cell: str) -> None:
    """IPython cell magic handler: %%dlt_doctor."""
    title = line.strip() if line.strip() else "Databricks Cell Audit"
    html_output = audit_code_cell(cell, cell_title=title)

    # Detect Databricks runtime environment
    try:
        # Databricks notebooks have a global displayHTML function
        import builtins

        if hasattr(builtins, "displayHTML"):
            builtins.displayHTML(html_output)
            return
    except (AttributeError, ImportError):
        pass

    # Fallback to standard IPython HTML display
    try:
        from IPython.display import HTML, display  # type: ignore

        display(HTML(html_output))
    except ImportError:
        # Plain terminal fallback
        doctor = PlanDoctor()
        advisor = AdvisorDoctor()
        findings = doctor.analyze_source_code(cell)
        report = advisor.compile_report(title, findings)
        print(
            f"Health Score: {report.overall_health_score}/100 | Est. Savings: ~{report.estimated_dbu_savings_pct}%"
        )
        for f in findings:
            print(f"[{f.severity.value}] {f.title}: {f.recommendation}")


def load_ipython_extension(ipython: Any) -> None:
    """Register %%dlt_doctor cell magic in Jupyter or Databricks."""
    ipython.register_magic_function(
        dlt_doctor_cell_magic, magic_kind="cell", magic_name="dlt_doctor"
    )
