"""Command-line interface for dlt-doctor."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dlt_doctor.advisor import AdvisorDoctor
from dlt_doctor.catalyst import DiagnosticSeverity, PlanDoctor
from dlt_doctor.cdc import CdcDoctor
from dlt_doctor.checkpoint import CheckpointDoctor
from dlt_doctor.cluster import ClusterDoctor, ColumnProfile
from dlt_doctor.compaction import CompactionDoctor
from dlt_doctor.connect import WorkspaceInspector
from dlt_doctor.cost import CostDoctor
from dlt_doctor.history import HistoryDoctor
from dlt_doctor.notebook import extract_notebook_code, is_notebook_file
from dlt_doctor.reconciler import RestatementDoctor
from dlt_doctor.scanner import DirectoryScanner
from dlt_doctor.schema import SchemaDoctor
from dlt_doctor.spill import SpillDoctor
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
        "analyze", help="Analyze PySpark or SQL source file, notebook, or directory"
    )
    analyze_parser.add_argument(
        "path", help="Path to Python (.py), notebook (.ipynb), or directory"
    )
    analyze_parser.add_argument(
        "--recursive",
        action="store_true",
        default=True,
        help="Scan directories recursively (default: True)",
    )
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

    # schema command
    schema_parser = subparsers.add_parser(
        "schema", help="Compare incoming batch schema and generate Delta migration DDL"
    )
    schema_parser.add_argument("--table", required=True, help="Target Delta table")
    schema_parser.add_argument(
        "--incoming-cols",
        nargs="+",
        required=True,
        help="Incoming column definitions (e.g. name:type)",
    )

    # cost command
    cost_parser = subparsers.add_parser(
        "cost", help="Forecast Databricks DBU burn and cluster sizing"
    )
    cost_parser.add_argument(
        "--files-per-hour", type=int, required=True, help="Landing files per hour"
    )
    cost_parser.add_argument(
        "--avg-file-mb", type=float, default=10.0, help="Average file size in MB"
    )
    cost_parser.add_argument(
        "--batch", action="store_true", help="Triggered batch workload mode"
    )
    cost_parser.add_argument(
        "--hours", type=float, default=24.0, help="Active daily hours"
    )
    cost_parser.add_argument(
        "--joins", type=int, default=1, help="Number of shuffle joins"
    )

    # spill command
    spill_parser = subparsers.add_parser(
        "spill",
        help="Calculate optimal spark.sql.shuffle.partitions to prevent disk spill",
    )
    spill_parser.add_argument(
        "--dataset-gb", type=float, required=True, help="Dataset size in GB"
    )
    spill_parser.add_argument(
        "--cores", type=int, default=16, help="Total cluster worker cores"
    )

    # history command
    history_parser = subparsers.add_parser(
        "history",
        help="Evaluate Delta transaction log velocity and checkpoint interval",
    )
    history_parser.add_argument("--table", required=True, help="Target Delta table")
    history_parser.add_argument(
        "--commits-per-hour", type=int, required=True, help="Commits written per hour"
    )

    # checkpoint command
    checkpoint_parser = subparsers.add_parser(
        "checkpoint", help="Audit Structured Streaming checkpoint directory health"
    )
    checkpoint_parser.add_argument(
        "--dir", required=True, help="Path to checkpoint directory"
    )
    checkpoint_parser.add_argument(
        "--stateful", action="store_true", help="Stateful operators present"
    )
    checkpoint_parser.add_argument(
        "--schema-changed", action="store_true", help="Source schema changed"
    )
    checkpoint_parser.add_argument(
        "--keys-changed", action="store_true", help="Aggregation keys changed"
    )

    # inspect-table command
    inspect_parser = subparsers.add_parser(
        "inspect-table",
        help="Inspect live Delta Lake table telemetry in Databricks workspace",
    )
    inspect_parser.add_argument(
        "table", help="Target table name (e.g. main.gold.orders)"
    )
    inspect_parser.add_argument("--host", help="Databricks host URL")
    inspect_parser.add_argument("--token", help="Databricks personal access token")
    inspect_parser.add_argument("--warehouse-id", help="Databricks SQL warehouse ID")
    inspect_parser.add_argument(
        "--mock",
        action="store_true",
        help="Run against simulated production telemetry",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    if args.command == "analyze":
        target = Path(args.path)
        if not target.exists():
            print(f"Error: Path not found: {target}", file=sys.stderr)
            return 1

        if target.is_dir():
            scanner = DirectoryScanner()
            dir_report = scanner.scan_directory(target, recursive=args.recursive)
            if args.format == "markdown":
                print(dir_report.to_markdown())
            else:
                print(dir_report.to_text())
            return 1 if dir_report.has_critical_findings else 0

        if is_notebook_file(target):
            content = extract_notebook_code(target)
        else:
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

    elif args.command == "schema":
        doctor = SchemaDoctor()
        # Parse incoming columns format col:type
        inc_schema = {}
        for item in args.incoming_cols:
            if ":" in item:
                col, dtype = item.split(":", 1)
                inc_schema[col.strip()] = dtype.strip()
            else:
                inc_schema[item.strip()] = "string"

        # Baseline demo target schema
        existing_schema = {"id": "bigint", "created_at": "timestamp"}
        report = doctor.compare_schemas(args.table, existing_schema, inc_schema)

        print("=" * 70)
        print(f" Delta Schema Evolution Audit: {args.table}")
        print(f" Has Breaking Mutations: {report.has_breaking_changes}")
        print("=" * 70)
        for chg in report.changes:
            print(f"\n[{chg.severity.value}] {chg.description}")
            print(f"Action: {chg.migration_action}")

        if report.generated_ddl:
            print("\nGenerated DDL Statements:")
            for ddl in report.generated_ddl:
                print(f"  {ddl}")
        print("-" * 70)
        return 1 if report.has_breaking_changes else 0

    elif args.command == "cost":
        doctor = CostDoctor()
        forecast = doctor.forecast_pipeline_cost(
            files_per_hour=args.files_per_hour,
            avg_file_mb=args.avg_file_mb,
            is_continuous_streaming=not args.batch,
            active_hours_per_day=args.hours,
            shuffle_join_count=args.joins,
        )
        print("=" * 70)
        print(" Databricks DBU Cost & Sizing Forecast")
        print(
            f" Workload: {forecast.workload_type} | Daily Volume: {forecast.daily_ingestion_gb} GB"
        )
        print("=" * 70)
        print(f"Recommended Compute Tier: {forecast.recommended_tier}")
        print(
            f"Classic DLT:    ~{forecast.classic_dlt_dbus_per_hour} DBU/hr (~{forecast.classic_dlt_monthly_dbus:,.0f} DBU/mo)"
        )
        print(
            f"Serverless DLT: ~{forecast.serverless_dlt_dbus_per_hour} DBU/hr (~{forecast.serverless_dlt_monthly_dbus:,.0f} DBU/mo)"
        )
        print("\nCluster Configuration:")
        print(f"  Driver:  {forecast.recommended_cluster.driver_node_type}")
        print(
            f"  Workers: {forecast.recommended_cluster.min_workers} to {forecast.recommended_cluster.max_workers}x {forecast.recommended_cluster.worker_node_type}"
        )
        print("\nRecommendations:")
        for r in forecast.cost_saving_recommendations:
            print(f"  * {r}")
        print("-" * 70)
        return 0

    elif args.command == "spill":
        doctor = SpillDoctor()
        advice = doctor.calculate_optimal_partitions(
            dataset_size_gb=args.dataset_gb,
            total_cluster_cores=args.cores,
        )
        print("=" * 70)
        print(" PySpark Shuffle Spill & Partition Optimization")
        print(
            f" Dataset: {advice.dataset_size_gb} GB | Cluster Cores: {advice.total_cluster_cores}"
        )
        print("=" * 70)
        print(f"Default spark.sql.shuffle.partitions: {advice.default_partitions}")
        print(f"Optimal spark.sql.shuffle.partitions: {advice.optimal_partitions}")
        print(f"Severe Disk Spill Risk: {advice.has_spill_risk}")
        print(f"\nRationale:\n{advice.rationale}")
        print("\nRecommended Spark Configurations:")
        for k, v in advice.recommended_configs.items():
            print(f"  spark.conf.set('{k}', '{v}')")
        print("-" * 70)
        return 0

    elif args.command == "history":
        doctor = HistoryDoctor()
        report = doctor.evaluate_commit_velocity(
            table_name=args.table,
            commits_per_hour=args.commits_per_hour,
        )
        print("=" * 70)
        print(f" Delta Transaction Log Audit: {args.table}")
        print(
            f" Commits/Hr: {report.commits_per_hour} | Monthly Log Files: ~{report.estimated_monthly_log_files:,}"
        )
        print("=" * 70)
        print(f"Log Bloat Risk: {report.has_log_bloat_risk}")
        if report.warnings:
            for w in report.warnings:
                print(f"[WARNING] {w}")
        print(f"\nRecommended TBLPROPERTIES:\n{report.recommended_tblproperties_ddl}")
        print(f"\nGuidance:\n{report.guidance}")
        print("-" * 70)
        return 0

    elif args.command == "checkpoint":
        doctor = CheckpointDoctor()
        report = doctor.audit_checkpoint_state(
            checkpoint_dir=args.dir,
            has_stateful_operators=args.stateful,
            source_schema_changed=args.schema_changed,
            aggregation_keys_changed=args.keys_changed,
        )
        print("=" * 70)
        print(f" Structured Streaming Checkpoint Health: {args.dir}")
        print(f" Integrity Status: {report.status.value}")
        print("=" * 70)
        if report.issues:
            for issue in report.issues:
                print(f"[ISSUE] {issue}")
        print(f"\nRecovery Guidance:\n{report.recovery_guidance}")
        print("\nMigration Procedure:")
        for step in report.safe_migration_procedure:
            print(f"  {step}")
        print("-" * 70)
        return 1 if report.status.value == "INCOMPATIBLE" else 0

    elif args.command == "inspect-table":
        inspector = WorkspaceInspector(
            host=args.host,
            token=args.token,
            warehouse_id=args.warehouse_id,
        )
        mock_detail = None
        mock_history = None
        if args.mock:
            mock_detail = {
                "numFiles": 4500,
                "sizeInBytes": 45 * 1024 * 1024 * 1024,  # 45 GB
                "clusteringColumns": ["customer_id", "order_date"],
                "partitionColumns": [],
            }
            mock_history = [{"version": i} for i in range(25)]

        try:
            report = inspector.inspect_table(
                table_name=args.table,
                mock_detail=mock_detail,
                mock_history=mock_history,
            )
            print(report.to_text())
            return 0
        except Exception as exc:
            print(f"Error inspecting table {args.table}: {exc}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
