"""Recursive directory scanner for Lakehouse pipelines, SQL scripts, and notebooks."""

from __future__ import annotations

import html
import json
from dataclasses import dataclass, field
from pathlib import Path

from dlt_doctor.advisor import AdvisorDoctor, PipelineAuditReport
from dlt_doctor.catalyst import DiagnosticFinding, DiagnosticSeverity, PlanDoctor
from dlt_doctor.config import DoctorConfig, load_config
from dlt_doctor.expectations_rules import audit_sql_source
from dlt_doctor.notebook import is_notebook_file, parse_notebook_cells

IGNORED_DIRECTORIES = {
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    ".pytest_cache",
    ".ruff_cache",
    "dist",
    "build",
    ".tox",
    "node_modules",
    ".eggs",
}


@dataclass
class DirectoryAuditReport:
    """Consolidated audit report for an entire directory of pipelines."""

    root_path: str
    total_files_scanned: int = 0
    clean_files_count: int = 0
    flagged_files_count: int = 0
    overall_health_score: int = 100
    file_reports: list[PipelineAuditReport] = field(default_factory=list)
    estimated_dbu_savings_pct: float = 0.0  # Kept for backward compatibility

    @property
    def has_critical_findings(self) -> bool:
        """Check if any scanned file contains critical severity issues."""
        for report in self.file_reports:
            for finding in report.findings:
                if finding.severity == DiagnosticSeverity.CRITICAL:
                    return True
        return False

    @property
    def all_findings(self) -> list[tuple[str, DiagnosticFinding]]:
        """Return all findings across files tagged with their source file name."""
        results: list[tuple[str, DiagnosticFinding]] = []
        for report in self.file_reports:
            for finding in report.findings:
                results.append((report.pipeline_name, finding))
        return results

    def to_text(self) -> str:
        """Render a formatted multi-file terminal audit summary."""
        lines: list[str] = [
            "=" * 70,
            f" DLT-Doctor Repository Audit: {self.root_path}",
            f" Files Scanned: {self.total_files_scanned} | Clean: {self.clean_files_count} | Flagged: {self.flagged_files_count}",
            f" Overall Health Score: {self.overall_health_score}/100",
            "=" * 70,
        ]

        if not self.all_findings:
            lines.append("All scanned pipelines are clean. No anti-patterns detected.")
            lines.append("-" * 70)
            return "\n".join(lines)

        for report in self.file_reports:
            if not report.findings:
                continue
            lines.append(
                f"\n>>> File: {report.pipeline_name} (Health Score: {report.overall_health_score}/100)"
            )
            for f in report.findings:
                lines.append(f"  [{f.severity.value}] {f.title} ({f.rule_id})")
                if f.line_number:
                    lines.append(f"    Line: {f.line_number}")
                lines.append(f"    Impact: {f.impact_summary}")
                lines.append(f"    Fix:    {f.recommendation}")

        lines.append("-" * 70)
        return "\n".join(lines)

    def to_markdown(self) -> str:
        """Render markdown summary for pull request CI comments."""
        lines: list[str] = [
            f"## DLT-Doctor Multi-Pipeline Audit: `{self.root_path}`",
            "",
            f"- **Overall Health Score**: **{self.overall_health_score}/100**",
            f"- **Files Scanned**: {self.total_files_scanned} ({self.clean_files_count} clean, {self.flagged_files_count} flagged)",
            "",
            "| File | Health Score | Critical | High | Medium | Low |",
            "| :--- | :---: | :---: | :---: | :---: | :---: |",
        ]

        for report in self.file_reports:
            crit = sum(
                1 for f in report.findings if f.severity == DiagnosticSeverity.CRITICAL
            )
            high = sum(
                1 for f in report.findings if f.severity == DiagnosticSeverity.HIGH
            )
            med = sum(
                1 for f in report.findings if f.severity == DiagnosticSeverity.MEDIUM
            )
            low = sum(
                1 for f in report.findings if f.severity == DiagnosticSeverity.LOW
            )
            lines.append(
                f"| `{report.pipeline_name}` | {report.overall_health_score} | {crit} | {high} | {med} | {low} |"
            )

        return "\n".join(lines)

    def to_json(self) -> str:
        """Serialize audit results to JSON."""
        data = {
            "root_path": self.root_path,
            "total_files_scanned": self.total_files_scanned,
            "clean_files_count": self.clean_files_count,
            "flagged_files_count": self.flagged_files_count,
            "overall_health_score": self.overall_health_score,
            "file_reports": [r.to_dict() for r in self.file_reports],
        }
        return json.dumps(data, indent=2)

    def to_sarif(self) -> str:
        """Serialize findings into standard GitHub Code Scanning SARIF format."""
        results: list[dict] = []
        rules_map: dict[str, dict] = {}

        for file_name, f in self.all_findings:
            sev_level = "error" if f.severity in {DiagnosticSeverity.CRITICAL, DiagnosticSeverity.HIGH} else "warning" if f.severity == DiagnosticSeverity.MEDIUM else "note"
            if f.rule_id not in rules_map:
                rules_map[f.rule_id] = {
                    "id": f.rule_id,
                    "name": f.title,
                    "shortDescription": {"text": f.title},
                    "fullDescription": {"text": f.impact_summary},
                }

            start_line = f.line_number or 1
            if isinstance(start_line, str):
                start_line = 1

            results.append({
                "ruleId": f.rule_id,
                "level": sev_level,
                "message": {"text": f"{f.title}: {f.impact_summary}"},
                "locations": [
                    {
                        "physicalLocation": {
                            "artifactLocation": {"uri": file_name},
                            "region": {"startLine": int(start_line) if isinstance(start_line, (int, float)) else 1},
                        }
                    }
                ],
            })

        sarif_obj = {
            "$schema": "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemata/sarif-schema-2.1.0.json",
            "version": "2.1.0",
            "runs": [
                {
                    "tool": {
                        "driver": {
                            "name": "dlt-doctor",
                            "version": "0.5.0.dev0",
                            "rules": list(rules_map.values()),
                        }
                    },
                    "results": results,
                }
            ],
        }
        return json.dumps(sarif_obj, indent=2)

    def to_html(self) -> str:
        """Render a consolidated multi-pipeline HTML dashboard."""
        score_color = (
            "#2EE6A7"
            if self.overall_health_score >= 80
            else "#FF9B4A"
            if self.overall_health_score >= 50
            else "#EF4444"
        )
        blocks = [
            "<!DOCTYPE html>",
            "<html><head><meta charset='utf-8'><title>DLT-Doctor Repository Audit</title></head>",
            "<body style='font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; background: #0E1726; color: #EAF4F0; padding: 24px;'>",
            f"  <h1 style='color: #FFFFFF;'>DLT-Doctor Audit: {html.escape(self.root_path)}</h1>",
            "  <div style='display: flex; gap: 24px; margin-bottom: 24px;'>",
            f"    <div style='background: #14233A; padding: 16px; border-radius: 8px; flex: 1;'><div style='font-size: 12px; color: #91A8A2;'>OVERALL SCORE</div><div style='font-size: 32px; font-weight: 700; color: {score_color};'>{self.overall_health_score}/100</div></div>",
            f"    <div style='background: #14233A; padding: 16px; border-radius: 8px; flex: 1;'><div style='font-size: 12px; color: #91A8A2;'>SCANNED FILES</div><div style='font-size: 32px; font-weight: 700; color: #38BDF8;'>{self.total_files_scanned}</div></div>",
            f"    <div style='background: #14233A; padding: 16px; border-radius: 8px; flex: 1;'><div style='font-size: 12px; color: #91A8A2;'>FLAGGED FILES</div><div style='font-size: 32px; font-weight: 700; color: #FF9B4A;'>{self.flagged_files_count}</div></div>",
            "  </div>",
        ]

        advisor = AdvisorDoctor()
        for report in self.file_reports:
            blocks.append(f"<div style='margin-bottom: 24px;'>{advisor.to_databricks_html(report)}</div>")

        blocks.append("</body></html>")
        return "\n".join(blocks)


class DirectoryScanner:
    """Discovers and audits PySpark pipelines, SQL definitions, and Databricks notebooks."""

    SUPPORTED_EXTENSIONS = {".py", ".ipynb", ".sql"}

    def __init__(self, config: DoctorConfig | None = None) -> None:
        self.config = config or load_config()
        self.doctor = PlanDoctor()
        self.advisor = AdvisorDoctor()

    def discover_files(self, root: Path | str, recursive: bool = True) -> list[Path]:
        """Discover valid Python pipelines, SQL files, and Databricks notebooks."""
        root_path = Path(root)
        if not root_path.exists():
            raise FileNotFoundError(f"Path not found: {root_path}")

        files: list[Path] = []
        if root_path.is_file():
            if root_path.suffix.lower() in self.SUPPORTED_EXTENSIONS:
                return [root_path]
            return []

        glob_pattern = "**/*" if recursive else "*"
        for path in root_path.glob(glob_pattern):
            if path.is_file():
                if any(ignored in path.parts for ignored in IGNORED_DIRECTORIES):
                    continue
                if path.suffix.lower() in self.SUPPORTED_EXTENSIONS:
                    files.append(path)

        return sorted(files)

    def scan_directory(
        self, root: Path | str, recursive: bool = True
    ) -> DirectoryAuditReport:
        """Scan all pipelines in directory and compile a consolidated report."""
        files = self.discover_files(root, recursive=recursive)
        file_reports: list[PipelineAuditReport] = []
        total_score = 0

        for file_path in files:
            findings: list[DiagnosticFinding] = []
            try:
                suffix = file_path.suffix.lower()
                if suffix == ".sql":
                    sql_text = file_path.read_text(encoding="utf-8")
                    findings = audit_sql_source(sql_text, config=self.config)
                elif suffix == ".ipynb":
                    cells = parse_notebook_cells(file_path)
                    for cell in cells:
                        if cell.cell_type == "python":
                            cell_findings = self.doctor.analyze_source_code(
                                cell.source_code, config=self.config, file_name=file_path.name
                            )
                            for f in cell_findings:
                                f.line_number = f"Cell {cell.cell_index}, line {f.line_number}" if f.line_number else f"Cell {cell.cell_index}"
                                findings.append(f)
                        elif cell.cell_type == "sql":
                            sql_findings = audit_sql_source(cell.source_code, config=self.config)
                            for f in sql_findings:
                                f.line_number = f"Cell {cell.cell_index}, line {f.line_number}" if f.line_number else f"Cell {cell.cell_index}"
                                findings.append(f)
                else:
                    # Python source file or Databricks source notebook
                    if is_notebook_file(file_path):
                        cells = parse_notebook_cells(file_path)
                        for cell in cells:
                            if cell.cell_type == "python":
                                cell_findings = self.doctor.analyze_source_code(
                                    cell.source_code, config=self.config, file_name=file_path.name
                                )
                                for f in cell_findings:
                                    f.line_number = f"Cell {cell.cell_index}, line {f.line_number}" if f.line_number else f"Cell {cell.cell_index}"
                                    findings.append(f)
                            elif cell.cell_type == "sql":
                                sql_findings = audit_sql_source(cell.source_code, config=self.config)
                                for f in sql_findings:
                                    f.line_number = f"Cell {cell.cell_index}, line {f.line_number}" if f.line_number else f"Cell {cell.cell_index}"
                                    findings.append(f)
                    else:
                        code = file_path.read_text(encoding="utf-8")
                        findings = self.doctor.analyze_source_code(
                            code, config=self.config, file_name=file_path.name
                        )

                report = self.advisor.compile_report(file_path.name, findings)
                file_reports.append(report)
                total_score += report.overall_health_score

            except Exception as exc:
                finding = DiagnosticFinding(
                    rule_id="SYNTAX-001",
                    severity=DiagnosticSeverity.CRITICAL,
                    title="Parse Failure",
                    impact_summary=f"Failed to parse {file_path.name}: {exc}",
                    recommendation="Ensure source is valid syntax or a valid notebook format.",
                )
                report = self.advisor.compile_report(file_path.name, [finding])
                file_reports.append(report)
                total_score += report.overall_health_score

        count = len(file_reports)
        overall_health = round(total_score / count) if count > 0 else 100
        clean_count = sum(1 for r in file_reports if not r.findings)
        flagged_count = count - clean_count

        return DirectoryAuditReport(
            root_path=str(root),
            total_files_scanned=count,
            clean_files_count=clean_count,
            flagged_files_count=flagged_count,
            overall_health_score=overall_health,
            file_reports=file_reports,
        )
