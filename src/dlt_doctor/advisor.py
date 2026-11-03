"""Diagnostic scorecards, markdown summaries, and Databricks notebook HTML reports."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dlt_doctor.catalyst import DiagnosticFinding, DiagnosticSeverity


@dataclass
class PipelineAuditReport:
    """Consolidated health and optimization audit report for a Databricks pipeline."""

    pipeline_name: str
    overall_health_score: int
    findings: list[DiagnosticFinding] = field(default_factory=list)
    estimated_dbu_savings_pct: int = 0
    summary_text: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "overall_health_score": self.overall_health_score,
            "findings": [f.to_dict() for f in self.findings],
            "estimated_dbu_savings_pct": self.estimated_dbu_savings_pct,
            "summary_text": self.summary_text,
        }


class AdvisorDoctor:
    """Formats diagnostic findings into terminal scorecards, markdown, and notebook HTML."""

    def compile_report(
        self,
        pipeline_name: str,
        findings: list[DiagnosticFinding],
    ) -> PipelineAuditReport:
        """Compile raw findings into a scored audit report."""
        critical_count = sum(
            1 for f in findings if f.severity == DiagnosticSeverity.CRITICAL
        )
        high_count = sum(1 for f in findings if f.severity == DiagnosticSeverity.HIGH)
        medium_count = sum(
            1 for f in findings if f.severity == DiagnosticSeverity.MEDIUM
        )

        # Base score 100, deduct based on severity
        penalty = (critical_count * 30) + (high_count * 15) + (medium_count * 5)
        health_score = max(0, 100 - penalty)

        # Estimated DBU savings
        savings_pct = min(
            85, (critical_count * 25) + (high_count * 15) + (medium_count * 5)
        )

        summary = (
            f"Pipeline '{pipeline_name}' scored {health_score}/100. "
            f"Found {len(findings)} potential optimization points "
            f"({critical_count} critical, {high_count} high, {medium_count} medium)."
        )

        return PipelineAuditReport(
            pipeline_name=pipeline_name,
            overall_health_score=health_score,
            findings=findings,
            estimated_dbu_savings_pct=savings_pct,
            summary_text=summary,
        )

    def to_markdown(self, report: PipelineAuditReport) -> str:
        """Render report as clean GitHub Flavored Markdown."""
        lines = [
            f"# DLT-Doctor Pipeline Audit: {report.pipeline_name}",
            "",
            f"- **Health Score**: `{report.overall_health_score} / 100`",
            f"- **Estimated DBU Savings**: `~{report.estimated_dbu_savings_pct}%`",
            f"- **Total Findings**: `{len(report.findings)}`",
            "",
            "## Identified Bottlenecks",
            "",
            "| Rule ID | Severity | Bottleneck | Estimated Impact | Line |",
            "| :--- | :--- | :--- | :--- | :--- |",
        ]

        for f in report.findings:
            line_str = str(f.line_number) if f.line_number else "-"
            lines.append(
                f"| `{f.rule_id}` | `{f.severity.value}` | {f.title} | {f.estimated_dbu_impact} | {line_str} |"
            )

        lines.extend(
            [
                "",
                "## Recommendations",
                "",
            ]
        )

        for f in report.findings:
            lines.append(f"### [{f.severity.value}] {f.title} (`{f.rule_id}`)")
            lines.append(f"- **Impact**: {f.impact_summary}")
            lines.append(f"- **Action**: {f.recommendation}")
            lines.append("")

        return "\n".join(lines)

    def to_databricks_html(self, report: PipelineAuditReport) -> str:
        """Render self-contained HTML for displayHTML(...) in Databricks notebooks."""
        score_color = (
            "#2EE6A7"
            if report.overall_health_score >= 80
            else "#FF9B4A"
            if report.overall_health_score >= 50
            else "#EF4444"
        )

        html_blocks = [
            "<div style='font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; background: #0E1726; color: #EAF4F0; padding: 24px; border-radius: 12px; border: 1px solid #1F2E45;'>",
            f"  <h2 style='margin-top: 0; color: #FFFFFF;'>DLT-Doctor: {report.pipeline_name}</h2>",
            "  <div style='display: flex; gap: 24px; margin-bottom: 24px;'>",
            f"    <div style='background: #14233A; padding: 16px; border-radius: 8px; flex: 1;'><div style='font-size: 12px; color: #91A8A2;'>HEALTH SCORE</div><div style='font-size: 32px; font-weight: 700; color: {score_color};'>{report.overall_health_score}/100</div></div>",
            f"    <div style='background: #14233A; padding: 16px; border-radius: 8px; flex: 1;'><div style='font-size: 12px; color: #91A8A2;'>ESTIMATED DBU SAVINGS</div><div style='font-size: 32px; font-weight: 700; color: #38BDF8;'>~{report.estimated_dbu_savings_pct}%</div></div>",
            f"    <div style='background: #14233A; padding: 16px; border-radius: 8px; flex: 1;'><div style='font-size: 12px; color: #91A8A2;'>FINDINGS</div><div style='font-size: 32px; font-weight: 700; color: #FF9B4A;'>{len(report.findings)}</div></div>",
            "  </div>",
            "  <h3 style='color: #FFFFFF; border-bottom: 1px solid #1F2E45; padding-bottom: 8px;'>Optimization Findings</h3>",
        ]

        for f in report.findings:
            sev_badge = (
                "#EF4444"
                if f.severity == DiagnosticSeverity.CRITICAL
                else "#FF9B4A"
                if f.severity == DiagnosticSeverity.HIGH
                else "#38BDF8"
            )
            html_blocks.append(
                f"  <div style='background: #14233A; border-left: 4px solid {sev_badge}; padding: 14px; margin-bottom: 12px; border-radius: 4px;'>"
                f"    <div style='font-weight: 600; color: #FFFFFF;'>[{f.severity.value}] {f.title} <span style='font-size: 12px; color: #91A8A2;'>({f.rule_id})</span></div>"
                f"    <div style='font-size: 13px; color: #B0C4DE; margin-top: 4px;'>{f.impact_summary}</div>"
                f"    <div style='font-size: 13px; color: #2EE6A7; margin-top: 6px;'><b>Fix:</b> {f.recommendation}</div>"
                "  </div>"
            )

        html_blocks.append("</div>")
        return "\n".join(html_blocks)
