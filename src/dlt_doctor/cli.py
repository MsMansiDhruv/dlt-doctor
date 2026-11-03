"""Command-line interface for dlt-doctor."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dlt_doctor.advisor import AdvisorDoctor
from dlt_doctor.catalyst import DiagnosticSeverity, PlanDoctor
from dlt_doctor.cdc import CdcDoctor
from dlt_doctor.compaction import CompactionDoctor
from dlt_doctor.reconciler import RestatementDoctor


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="dlt-doctor",
        description="Production optimization and quality engine for Databricks DLT and PySpark",
    )
    subparsers = parser.add_subparsers(dest="command", help="Sub-commands")

    # analyze command
    analyze_parser = subparsers.add_parser(
        "analyze", help="Analyze PySpark or SQL source file"
    )
    analyze_parser.add_argument("path", help="Path to Python (.py) or SQL (.sql) file")
    analyze_parser.add_argument(
        "--format",
        choices=["text", "markdown", "html"],
        default="text",
        help="Output format",
    )

    # restate command
    restate_parser = subparsers.add_parser(
        "restate", help="Generate targeted Delta MERGE restatement"
    )
    restate_parser.add_argument(
        "--table", required=True, help="Target Delta Lake table"
    )
    restate_parser.add_argument(
        "--late-date", required=True, help="Late arriving partition date (YYYY-MM-DD)"
    )
    restate_parser.add_argument(
        "--window", type=int, default=7, help="Rolling window size in days"
    )

    # cdc command
    cdc_parser = subparsers.add_parser(
        "cdc", help="Generate streaming CDC deduplication and MERGE templates"
    )
    cdc_parser.add_argument("--table", required=True, help="Target Delta table")
    cdc_parser.add_argument(
        "--keys", required=True, nargs="+", help="Primary key column(s)"
    )
    cdc_parser.add_argument(
        "--event-time", default="updated_at", help="Event timestamp column"
    )

    # compact command
    compact_parser = subparsers.add_parser(
        "compact", help="Generate Delta table compaction and auto-optimize properties"
    )
    compact_parser.add_argument("--table", required=True, help="Target Delta table")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    if args.command == "analyze":
        target = Path(args.path)
        if not target.exists():
            print(f"Error: File not found: {target}", file=sys.stderr)
            return 1

        content = target.read_text(encoding="utf-8")
        doctor = PlanDoctor()
        advisor = AdvisorDoctor()

        findings = doctor.analyze_source_code(content)
        report = advisor.compile_report(target.name, findings)

        if args.format == "markdown":
            print(advisor.to_markdown(report))
        elif args.format == "html":
            print(advisor.to_databricks_html(report))
        else:
            print("=" * 70)
            print(f" DLT-Doctor Audit: {report.pipeline_name}")
            print(
                f" Health Score: {report.overall_health_score}/100 | Est. DBU Savings: ~{report.estimated_dbu_savings_pct}%"
            )
            print("=" * 70)
            if not findings:
                print("All checks clean. No critical PySpark anti-patterns found.")
            for f in findings:
                print(f"\n[{f.severity.value}] {f.title} ({f.rule_id})")
                if f.line_number:
                    print(f"Line: {f.line_number}")
                print(f"Impact: {f.impact_summary}")
                print(f"Fix:    {f.recommendation}")
            print("-" * 70)

        critical_count = sum(
            1 for f in findings if f.severity == DiagnosticSeverity.CRITICAL
        )
        return 1 if critical_count > 0 else 0

    elif args.command == "restate":
        reconciler = RestatementDoctor()
        scope = reconciler.calculate_blast_radius(
            target_table=args.table,
            late_date_iso=args.late_date,
            window_size_days=args.window,
        )
        print("=" * 70)
        print(f" Delta Restatement Plan: {args.table}")
        print(f" Blast Radius: {scope.blast_radius_start} to {scope.blast_radius_end}")
        print(f" Partition Pruning: {scope.partition_pruning_clause}")
        print("=" * 70)
        print("\nSurgical Delta MERGE SQL:\n")
        print(scope.delta_merge_sql)
        print("-" * 70)
        return 0

    elif args.command == "cdc":
        cdc = CdcDoctor()
        tmpl = cdc.generate_cdc_templates(
            target_table=args.table,
            primary_keys=args.keys,
            event_time_col=args.event_time,
        )
        print("=" * 70)
        print(f" Delta CDC Stream Deduplication: {args.table}")
        print("=" * 70)
        print("\nPySpark In-Batch Deduplication:\n")
        print(tmpl.microbatch_dedup_code)
        print("\nSCD Type 1 MERGE SQL:\n")
        print(tmpl.scd1_merge_sql)
        print("-" * 70)
        return 0

    elif args.command == "compact":
        doctor = CompactionDoctor()
        report = doctor.analyze_file_distribution(args.table, [])
        print("=" * 70)
        print(f" Delta Compaction Advisory: {args.table}")
        print("=" * 70)
        print(f"\nTargeted Optimize SQL:\n{report.recommended_optimize_sql}\n")
        print(
            f"Auto-Optimize Table Properties:\n{report.recommended_tblproperties_sql}"
        )
        print("-" * 70)
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
