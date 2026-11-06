"""Handler for 'dlt-doctor restate' subcommand."""

from __future__ import annotations

import argparse

from dlt_doctor.reconciler import RestatementDoctor


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("restate", help="Generate targeted Delta MERGE restatement")
    p.add_argument("--table", required=True, help="Target Delta Lake table")
    p.add_argument("--late-date", required=True, help="Late arriving partition date (YYYY-MM-DD)")
    p.add_argument("--window", type=int, default=7, help="Rolling window size in days (default: 7)")
    p.add_argument(
        "--partition-col", default="order_date", help="Partition column (default: order_date)"
    )
    p.add_argument(
        "--match-key",
        nargs="+",
        default=["id"],
        help="Primary match key column(s) (default: id)",
    )
    p.add_argument(
        "--source",
        required=True,
        help="Source staging table",
    )
    p.add_argument(
        "--delete-unmatched",
        action="store_true",
        default=False,
        help="Enable WHEN NOT MATCHED BY SOURCE ... THEN DELETE (WARNING: permanently deletes unmatched target rows in window)",
    )


def handle(args: argparse.Namespace) -> int:
    reconciler = RestatementDoctor()
    delete_unmatched = args.delete_unmatched

    try:
        scope = reconciler.calculate_blast_radius(
            target_table=args.table,
            late_date_iso=args.late_date,
            window_size_days=args.window,
            partition_col=args.partition_col,
            match_key=args.match_key,
            source_table=args.source,
            delete_unmatched=delete_unmatched,
        )
    except ValueError as exc:
        import sys

        print(f"Error: {exc}", file=sys.stderr)
        return 2

    print("=" * 70)
    print(f" Delta Restatement Plan: {args.table}")
    print(f" Blast Radius: {scope.blast_radius_start} to {scope.blast_radius_end}")
    print(f" Partition Pruning: {scope.partition_pruning_clause}")
    print("=" * 70)
    print("\nSurgical Delta MERGE SQL:\n")
    print(scope.delta_merge_sql)
    print("-" * 70)
    return 0
