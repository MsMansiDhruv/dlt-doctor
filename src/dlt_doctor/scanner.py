"""Recursive directory scanner for Lakehouse pipelines and notebooks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from dlt_doctor.advisor import AdvisorDoctor, PipelineAuditReport
from dlt_doctor.catalyst import DiagnosticFinding, DiagnosticSeverity, PlanDoctor
from dlt_doctor.notebook import extract_notebook_code, is_notebook_file

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
    estimated_dbu_savings_pct: float = 0.0
    file_reports: list[PipelineAuditReport] = field(default_factory=list)

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
            f" Overall Health Score: {self.overall_health_score}/100 | Avg Est. DBU Savings: ~{self.estimated_dbu_savings_pct:.1f}%",
            "=" * 70,
        ]

        if not self.all_findings:
            lines.append(
                "All scanned pipelines are clean. No PySpark anti-patterns detected."
            )
            lines.append("-" * 70)
            return "\n".join(lines)

        for report in self.file_reports:
            if not report.findings:
                continue
            lines.append(
                f"\n>>> File: {report.pipeline_name} (Health Score: {report.overall_health_score}/100, Est. Savings: ~{report.estimated_dbu_savings_pct}%)"
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
            f"- **Average Estimated DBU Savings**: ~{self.estimated_dbu_savings_pct:.1f}%",
            "",
            "| File | Health Score | Est. Savings | Critical | High | Medium |",
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
            lines.append(
                f"| `{report.pipeline_name}` | {report.overall_health_score} | {report.estimated_dbu_savings_pct}% | {crit} | {high} | {med} |"
            )

        return "\n".join(lines)


class DirectoryScanner:
    """Discovers and audits PySpark pipelines and Databricks notebooks."""

    def __init__(self) -> None:
        self.doctor = PlanDoctor()
        self.advisor = AdvisorDoctor()

    def discover_files(self, root: Path | str, recursive: bool = True) -> list[Path]:
        """Discover valid Python pipelines and Databricks notebooks."""
        root_path = Path(root)
        if not root_path.exists():
            raise FileNotFoundError(f"Directory not found: {root_path}")

        files: list[Path] = []
        if root_path.is_file():
            if root_path.suffix.lower() in {".py", ".ipynb"}:
                return [root_path]
            return []

        glob_pattern = "**/*" if recursive else "*"
        for path in root_path.glob(glob_pattern):
            if path.is_file():
                # Skip ignored directory trees
                if any(ignored in path.parts for ignored in IGNORED_DIRECTORIES):
                    continue
                if path.suffix.lower() in {".py", ".ipynb"}:
                    files.append(path)

        return sorted(files)

    def scan_directory(
        self, root: Path | str, recursive: bool = True
    ) -> DirectoryAuditReport:
        """Scan all pipelines in the directory and compile a consolidated report."""
        files = self.discover_files(root, recursive=recursive)
        file_reports: list[PipelineAuditReport] = []

        total_score = 0
        total_savings = 0.0

        for file_path in files:
            try:
                if is_notebook_file(file_path):
                    code = extract_notebook_code(file_path)
                else:
                    code = file_path.read_text(encoding="utf-8")

                findings = self.doctor.analyze_source_code(code)
                report = self.advisor.compile_report(file_path.name, findings)
                file_reports.append(report)
                total_score += report.overall_health_score
                total_savings += report.estimated_dbu_savings_pct
            except Exception as exc:
                # In case of syntax error or unreadable file, record a critical finding
                finding = DiagnosticFinding(
                    rule_id="SYNTAX-001",
                    severity=DiagnosticSeverity.CRITICAL,
                    title="Parse Failure",
                    impact_summary=f"Failed to parse {file_path.name}: {exc}",
                    recommendation="Ensure source is valid Python syntax or a valid notebook format.",
                )
                report = self.advisor.compile_report(file_path.name, [finding])
                file_reports.append(report)
                total_score += report.overall_health_score

        count = len(file_reports)
        overall_health = round(total_score / count) if count > 0 else 100
        avg_savings = round(total_savings / count, 1) if count > 0 else 0.0
        clean_count = sum(1 for r in file_reports if not r.findings)
        flagged_count = count - clean_count

        return DirectoryAuditReport(
            root_path=str(root),
            total_files_scanned=count,
            clean_files_count=clean_count,
            flagged_files_count=flagged_count,
            overall_health_score=overall_health,
            estimated_dbu_savings_pct=avg_savings,
            file_reports=file_reports,
        )
