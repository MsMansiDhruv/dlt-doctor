"""Handler for 'dlt-doctor compact' subcommand."""

from __future__ import annotations

import argparse

from dlt_doctor.compaction import CompactionDoctor


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "compact", help="Generate Delta table compaction and auto-optimize properties"
    )
    p.add_argument("--table", required=True, help="Target Delta table")
    p.add_argument(
        "--file-sizes-mb",
        nargs="+",
        type=float,
        default=None,
        help="Optional sample list of file sizes in MB for live compaction debt evaluation",
    )


def handle(args: argparse.Namespace) -> int:
    doctor = CompactionDoctor()
    file_sizes_bytes = (
        [int(s * 1024 * 1024) for s in args.file_sizes_mb]
        if args.file_sizes_mb
        else [10 * 1024 * 1024, 15 * 1024 * 1024]
    )

    report = doctor.analyze_file_distribution(args.table, file_sizes_bytes)

    print("=" * 70)
    print(" DLT-Doctor Auto-Optimize & Compaction Advisor (Template)")
    print(f" Target Table: {args.table}")
    print("=" * 70)
    print(f"Average File Size:    {report.average_file_size_mb:.2f} MB")
    print(f"Small Files (< 32MB): {report.small_files_count}")
    print(f"Compaction Debt:      {report.has_compaction_debt}")
    print("\nRecommended Table Properties (Auto-Optimize & Auto-Compaction DDL):")
    print(report.recommended_tblproperties_sql)
    print("\nTargeted OPTIMIZE Statement:")
    print(report.recommended_optimize_sql)
    print("-" * 70)
    return 0
