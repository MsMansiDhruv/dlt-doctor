"""Handler for 'dlt-doctor vacuum' subcommand."""

from __future__ import annotations

import argparse

from dlt_doctor.vacuum import VacuumDoctor, VacuumSafetyLevel


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "vacuum",
        help="Evaluate Delta table VACUUM retention safety against streaming readers",
    )
    p.add_argument("--table", required=True, help="Target Delta table")
    p.add_argument(
        "--retention",
        type=int,
        default=168,
        help="Requested retention hours (default: 168)",
    )
    p.add_argument(
        "--readers",
        type=int,
        default=0,
        help="Active streaming reader queries (default: 0)",
    )
    p.add_argument("--cdf", action="store_true", help="Whether Change Data Feed (CDF) is enabled")


def handle(args: argparse.Namespace) -> int:
    doctor = VacuumDoctor()
    report = doctor.evaluate_vacuum_safety(
        table_name=args.table,
        requested_retention_hours=args.retention,
        active_streaming_readers=args.readers,
        cdf_enabled=args.cdf,
    )

    print("=" * 70)
    print(f" Delta VACUUM Safety Assessment: {args.table}")
    print(f" Safety Level: {report.safety_level.value}")
    print("=" * 70)
    print(f"Recommended Retention: {report.safe_retention_hours} hours")
    print(f"Guidance:              {report.guidance}")
    if report.warnings:
        print("\nWarnings:")
        for w in report.warnings:
            print(f"  - {w}")
    print(f"\nRecommended VACUUM command:\n  {report.recommended_vacuum_sql}")
    print("-" * 70)
    return 0 if report.safety_level != VacuumSafetyLevel.DANGEROUS else 1
