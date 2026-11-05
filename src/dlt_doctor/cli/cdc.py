"""Handler for 'dlt-doctor cdc' subcommand."""

from __future__ import annotations

import argparse

from dlt_doctor.cdc import CdcDoctor


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "cdc", help="Generate streaming CDC deduplication and MERGE templates"
    )
    p.add_argument("--table", required=True, help="Target Delta table")
    p.add_argument("--keys", required=True, nargs="+", help="Primary key column(s)")
    p.add_argument(
        "--event-time", default="updated_at", help="Event timestamp column (default: updated_at)"
    )
    p.add_argument(
        "--sequence-col", default="_cdc_seq_id", help="Sequence column (default: _cdc_seq_id)"
    )
    p.add_argument(
        "--source", default=None, help="Source CDC stream or table name (default: <table_prefix>_cdc_stream)"
    )
    p.add_argument(
        "--partition-col", default=None, help="Partition column for MERGE pruning"
    )
    p.add_argument(
        "--values", nargs="*", default=None, help="Value/attribute columns for SCD2 tracking"
    )
    p.add_argument(
        "--no-cdf-filter",
        action="store_true",
        default=False,
        help="Disable automatic update_preimage filtering for CDF sources",
    )


def handle(args: argparse.Namespace) -> int:
    cdc = CdcDoctor()
    try:
        tmpl = cdc.generate_cdc_templates(
            target_table=args.table,
            primary_keys=args.keys,
            event_time_col=args.event_time,
            sequence_col=args.sequence_col,
            partition_col=args.partition_col,
            source_is_cdf=not args.no_cdf_filter,
            value_cols=args.values,
            source_stream=args.source,
        )
    except ValueError as exc:
        import sys

        print(f"Error: {exc}", file=sys.stderr)
        return 2

    print("=" * 70)
    print(f" Delta CDC Stream Deduplication & MERGE: {args.table}")
    print("=" * 70)
    print("\n1. PySpark In-Batch Window Deduplication:\n")
    print(tmpl.microbatch_dedup_code)
    print("\n2. SCD Type 1 MERGE SQL:\n")
    print(tmpl.scd1_merge_sql)
    print("\n3. SCD Type 2 Staging-Union MERGE SQL:\n")
    print(tmpl.scd2_merge_sql)
    print("\n4. Recommended Databricks Declarative Lakeflow Auto CDC Pattern:\n")
    print(tmpl.auto_cdc_code)
    print("-" * 70)
    return 0
