"""Handler for 'dlt-doctor history' subcommand."""

from __future__ import annotations

import argparse

from dlt_doctor.history import HistoryDoctor


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "history",
        help="Evaluate Delta table commit velocity and optimize log checkpoint interval",
    )
    p.add_argument("--table", required=True, help="Target Delta table")
    p.add_argument(
        "--commits-per-hour",
        type=int,
        required=True,
        help="Estimated commit rate per hour",
    )
    p.add_argument(
        "--checkpoint-interval",
        type=int,
        default=10,
        help="Current delta.checkpointInterval (default: 10)",
    )
    p.add_argument(
        "--retention-days",
        type=int,
        default=30,
        help="Log retention duration in days (default: 30)",
    )


def handle(args: argparse.Namespace) -> int:
    doctor = HistoryDoctor()
    report = doctor.evaluate_commit_velocity(
        table_name=args.table,
        commits_per_hour=args.commits_per_hour,
        current_checkpoint_interval=args.checkpoint_interval,
        log_retention_days=args.retention_days,
    )

    print("=" * 70)
    print(f" Delta Transaction Log Velocity Audit: {args.table}")
    print("=" * 70)
    print(f"Commits Per Hour:              {report.commits_per_hour}")
    print(f"Estimated Monthly Log Files:   {report.estimated_monthly_log_files:,}")
    print(f"Log Bloat Risk:                {report.has_log_bloat_risk}")
    print(f"Recommended Checkpoint Int.:   {report.recommended_checkpoint_interval}")
    print(f"Guidance:                      {report.guidance}")
    if report.warnings:
        print("\nWarnings:")
        for w in report.warnings:
            print(f"  - {w}")
    print(f"\nRecommended Table Properties:\n  {report.recommended_tblproperties_ddl}")
    print("-" * 70)
    return 0
