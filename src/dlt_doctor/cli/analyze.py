"""Handler for 'dlt-doctor analyze' subcommand."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dlt_doctor.advisor import AdvisorDoctor
from dlt_doctor.catalyst import DiagnosticFinding, DiagnosticSeverity, PlanDoctor
from dlt_doctor.config import load_config
from dlt_doctor.expectations_rules import audit_sql_source
from dlt_doctor.heal import heal_file
from dlt_doctor.notebook import is_notebook_file, parse_notebook_cells
from dlt_doctor.scanner import DirectoryAuditReport, DirectoryScanner


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "analyze", help="Analyze PySpark or SQL source file, notebook, or directory"
    )
    p.add_argument("path", help="Path to Python (.py), SQL (.sql), notebook (.ipynb), or directory")
    p.add_argument(
        "--no-recursive",
        action="store_true",
        default=False,
        help="Disable recursive scanning of subdirectories",
    )
    p.add_argument(
        "--format",
        choices=["text", "markdown", "json", "sarif", "html"],
        default="text",
        help="Output format (default: text)",
    )
    p.add_argument(
        "--fix",
        "--auto-heal",
        dest="fix",
        action="store_true",
        default=False,
        help="Automatically synthesize zero-loss quarantine tables for expect_or_drop",
    )
    p.add_argument(
        "--write",
        action="store_true",
        default=False,
        help="Apply synthesized code changes directly to the file (default: preview only)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="When used with --fix/--auto-heal, display diff without modifying files",
    )
    p.add_argument(
        "--quarantine-suffix",
        default="_quarantine",
        help="Suffix for synthesized quarantine table (default: _quarantine)",
    )
    p.add_argument(
        "--config",
        default=None,
        help="Path to custom configuration file or directory containing pyproject.toml",
    )
    p.add_argument(
        "--fail-on",
        choices=["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO", "NONE"],
        default=None,
        help="Exit code 1 severity threshold (default: config value or CRITICAL, NONE to never fail)",
    )


def handle(args: argparse.Namespace) -> int:
    target = Path(args.path)
    if not target.exists():
        print(f"Error: Path not found: {target}", file=sys.stderr)
        return 2

    config_target = (
        Path(args.config)
        if getattr(args, "config", None)
        else (target if target.is_dir() else target.parent)
    )
    config = load_config(config_target)

    # If --fix / --auto-heal is requested on a file, delegate to heal
    if args.fix:
        if target.is_dir():
            print(
                "Error: --fix / --auto-heal must be targeted at a specific file.", file=sys.stderr
            )
            return 2
        write_mode = getattr(args, "write", False) and not getattr(args, "dry_run", False)
        heal_res = heal_file(
            target,
            write=write_mode,
            quarantine_suffix=getattr(args, "quarantine_suffix", "_quarantine"),
            config=config,
        )
        if not heal_res.can_heal:
            print(heal_res.reason or "Cannot auto-heal file.", file=sys.stderr)
            return 3
        if heal_res.diff:
            if write_mode:
                print("Applied automated fix:")
                print(heal_res.diff)
            else:
                print("Preview diff (run with --write to apply):")
                print(heal_res.diff)
        else:
            print(heal_res.reason or "No changes needed.")
        return 0

    severity_rank = {
        DiagnosticSeverity.CRITICAL: 5,
        DiagnosticSeverity.HIGH: 4,
        DiagnosticSeverity.MEDIUM: 3,
        DiagnosticSeverity.LOW: 2,
        DiagnosticSeverity.INFO: 1,
    }
    fail_on_str = str(args.fail_on).upper() if args.fail_on else None
    if fail_on_str == "NONE":
        threshold = 999
    elif fail_on_str:
        threshold = severity_rank.get(DiagnosticSeverity(fail_on_str), 5)
    else:
        threshold = severity_rank.get(config.fail_on, 5)

    advisor = AdvisorDoctor()
    scanner = DirectoryScanner(config=config)

    if target.is_dir():
        recursive = not args.no_recursive
        dir_report = scanner.scan_directory(target, recursive=recursive)

        if args.format == "markdown":
            print(dir_report.to_markdown())
        elif args.format == "json":
            print(dir_report.to_json())
        elif args.format == "sarif":
            print(dir_report.to_sarif())
        elif args.format == "html":
            print(dir_report.to_html())
        else:
            print(dir_report.to_text())

        for _, f in dir_report.all_findings:
            if severity_rank.get(f.severity, 0) >= threshold:
                return 1
        return 0

    # Single file analysis
    suffix = target.suffix.lower()
    findings: list[DiagnosticFinding] = []
    doctor = PlanDoctor()

    if suffix == ".sql":
        sql_text = target.read_text(encoding="utf-8")
        findings = audit_sql_source(sql_text, config=config)
    elif is_notebook_file(target):
        cells = parse_notebook_cells(target)
        for cell in cells:
            if cell.cell_type == "python":
                c_findings = doctor.analyze_source_code(
                    cell.source_code, config=config, file_name=target.name
                )
                for f in c_findings:
                    f.line_number = (
                        f"Cell {cell.cell_index}, line {f.line_number}"
                        if f.line_number
                        else f"Cell {cell.cell_index}"
                    )
                    findings.append(f)
            elif cell.cell_type == "sql":
                s_findings = audit_sql_source(cell.source_code, config=config)
                for f in s_findings:
                    f.line_number = (
                        f"Cell {cell.cell_index}, line {f.line_number}"
                        if f.line_number
                        else f"Cell {cell.cell_index}"
                    )
                    findings.append(f)
    else:
        content = target.read_text(encoding="utf-8")
        findings = doctor.analyze_source_code(content, config=config, file_name=target.name)

    report = advisor.compile_report(target.name, findings)

    if args.format == "markdown":
        print(advisor.to_markdown(report))
    elif args.format == "html":
        print(advisor.to_databricks_html(report))
    elif args.format == "json":
        import json

        print(json.dumps(report.to_dict(), indent=2))
    elif args.format == "sarif":
        dir_rep = DirectoryAuditReport(
            root_path=str(target),
            total_files_scanned=1,
            clean_files_count=0 if findings else 1,
            flagged_files_count=1 if findings else 0,
            overall_health_score=report.overall_health_score,
            file_reports=[report],
        )
        print(dir_rep.to_sarif())
    else:
        print("=" * 70)
        print(f" DLT-Doctor Audit: {report.pipeline_name}")
        print(f" Health Score: {report.overall_health_score}/100")
        print("=" * 70)
        if not findings:
            print("All checks clean. No anti-patterns found.")
        for f in findings:
            print(f"\n[{f.severity.value}] {f.title} ({f.rule_id})")
            if f.line_number:
                print(f"Line: {f.line_number}")
            print(f"Impact: {f.impact_summary}")
            print(f"Fix:    {f.recommendation}")
        print("-" * 70)

    for f in findings:
        if severity_rank.get(f.severity, 0) >= threshold:
            return 1
    return 0
