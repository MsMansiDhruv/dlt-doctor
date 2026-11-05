"""Handler for 'dlt-doctor replay' subcommand."""

from __future__ import annotations

import argparse

from dlt_doctor.replay import generate_replay_sql


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "replay", help="Generate SQL to reprocess quarantined records into target tables"
    )
    p.add_argument(
        "--quarantine",
        "--quarantine-table",
        dest="quarantine",
        required=True,
        help="Source quarantine Delta table",
    )
    p.add_argument(
        "--target",
        "--target-table",
        dest="target",
        required=True,
        help="Destination target Delta table",
    )
    p.add_argument("--keys", required=True, nargs="+", help="Primary key column(s) for MERGE")
    p.add_argument("--rule", default=None, help="Specific failed expectation rule to filter by")
    p.add_argument(
        "--start-time", default=None, help="Filter records quarantined after this timestamp"
    )
    p.add_argument(
        "--end-time", default=None, help="Filter records quarantined before this timestamp"
    )
    p.add_argument(
        "--recency-col",
        default="updated_at",
        help="Timestamp column for recency guard (default: updated_at)",
    )
    p.add_argument(
        "--partition-col",
        default=None,
        help="Target partition column for partition pruning during MERGE",
    )
    p.add_argument(
        "--partition-val",
        default=None,
        help="Partition value to filter in quarantine and target",
    )


def handle(args: argparse.Namespace) -> int:
    try:
        sql = generate_replay_sql(
            quarantine_table=args.quarantine,
            target_table=args.target,
            primary_keys=args.keys,
            rule_name=args.rule,
            start_time=args.start_time,
            end_time=args.end_time,
            recency_col=args.recency_col,
            partition_col=args.partition_col,
            partition_val=args.partition_val,
        )
    except ValueError as exc:
        import sys

        print(f"Error: {exc}", file=sys.stderr)
        return 2

    print("=" * 70)
    print(" DLT-Doctor Quarantine Replay SQL")
    print("=" * 70)
    print(sql)
    print("-" * 70)
    print(
        "Note: In Lakeflow apply_changes / AUTO CDC pipelines, ensure sequence ordering is preserved."
    )
    return 0
