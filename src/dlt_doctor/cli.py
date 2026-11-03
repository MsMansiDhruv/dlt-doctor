"""Command-line interface for dlt-doctor."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dlt_doctor.advisor import AdvisorDoctor
from dlt_doctor.catalyst import DiagnosticSeverity, PlanDoctor
from dlt_doctor.cdc import CdcDoctor
from dlt_doctor.cluster import ClusterDoctor, ColumnProfile
from dlt_doctor.compaction import CompactionDoctor
from dlt_doctor.reconciler import RestatementDoctor
from dlt_doctor.uc import UcDoctor
from dlt_doctor.vacuum import VacuumDoctor


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

    # uc-migrate command
    uc_parser = subparsers.add_parser(
        "uc-migrate", help="Scan code for legacy DBFS mounts and two-tier Hive tables"
    )
    uc_parser.add_argument(
        "path", help="Path to Python or SQL file to audit for Unity Catalog"
    )
    uc_parser.add_argument(
        "--catalog", default="main", help="Target Unity Catalog name"
    )
    uc_parser.add_argument(
        "--schema", default="bronze", help="Target Unity Catalog schema"
    )

    # cluster command
    cluster_parser = subparsers.add_parser(
        "cluster", help="Recommend optimal Liquid Clustering (CLUSTER BY) keys"
    )
    cluster_parser.add_argument("--table", required=True, help="Target Delta table")
    cluster_parser.add_argument(
        "--cols", nargs="+", required=True, help="Candidate column names"
    )

    # vacuum command
    vacuum_parser = subparsers.add_parser(
        "vacuum",
        help="Evaluate Delta table VACUUM retention safety against streaming readers",
    )
    vacuum_parser.add_argument("--table", required=True, help="Target Delta table")
    vacuum_parser.add_argument(
        "--retention", type=int, default=168, help="Requested retention hours"
    )
    vacuum_parser.add_argument(
        "--readers", type=int, default=0, help="Active streaming reader queries"
    )
    vacuum_parser.add_argument(
        "--cdf", action="store_true", help="Whether Change Data Feed (CDF) is enabled"
    )

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

    elif args.command == "uc-migrate":
        target = Path(args.path)
        if not target.exists():
            print(f"Error: File not found: {target}", file=sys.stderr)
            return 1

        content = target.read_text(encoding="utf-8")
        uc_doc = UcDoctor()
        plan = uc_doc.analyze_source_code(
            content, target_catalog=args.catalog, target_schema=args.schema
        )

        print("=" * 70)
        print(f" Unity Catalog Migration Audit: {target.name}")
        print(f" Target Catalog/Schema: {plan.target_catalog}.{plan.target_schema}")
        print("=" * 70)
        if not plan.findings:
            print(
                "Clean. No legacy DBFS mounts or un-cataloged table references detected."
            )
        for f in plan.findings:
            print(f"\n[{f.severity.value}] {f.description}")
            if f.line_number:
                print(f"Line: {f.line_number}")
            print(f"Target: {f.recommended_target}")

        if plan.generated_volume_ddl:
            print("\nRecommended Volume DDL:")
            for ddl in plan.generated_volume_ddl:
                print(f"  {ddl}")
        print("-" * 70)
        return 1 if any(f.severity.value == "CRITICAL" for f in plan.findings) else 0

    elif args.command == "cluster":
        doctor = ClusterDoctor()
        profiles = [
            ColumnProfile(
                name=col,
                data_type="string",
                distinct_count=5000,
                filter_frequency_score=0.8,
            )
            for col in args.cols
        ]
        rec = doctor.advise_clustering(args.table, profiles)
        print("=" * 70)
        print(f" Delta Liquid Clustering Advisory: {args.table}")
        print("=" * 70)
        print(f"Recommended Cluster Keys: {', '.join(rec.recommended_keys)}")
        print(f"\nAlter Table DDL:\n{rec.alter_cluster_ddl}")
        print(f"\nOptimize DDL:\n{rec.optimize_ddl}")
        print(f"\nAdvice:\n{rec.migration_advice}")
        print("-" * 70)
        return 0

    elif args.command == "vacuum":
        doctor = VacuumDoctor()
        report = doctor.evaluate_vacuum_safety(
            table_name=args.table,
            requested_retention_hours=args.retention,
            active_streaming_readers=args.readers,
            cdf_enabled=args.cdf,
        )
        print("=" * 70)
        print(f" Delta VACUUM Safety Report: {args.table}")
        print(f" Safety Grade: {report.safety_level.value}")
        print(f" Safe Retention: {report.safe_retention_hours} hours")
        print("=" * 70)
        if report.warnings:
            for w in report.warnings:
                print(f"[WARNING] {w}")
        print(f"\nRecommended Dry Run:\n{report.dry_run_sql}")
        print(f"\nRecommended Vacuum SQL:\n{report.recommended_vacuum_sql}")
        print(f"\nGuidance:\n{report.guidance}")
        print("-" * 70)
        return 1 if report.safety_level.value == "DANGEROUS" else 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
