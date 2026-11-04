"""Handler for 'dlt-doctor uc-migrate' subcommand."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dlt_doctor.uc import UcDoctor, UcMigrationSeverity


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "uc-migrate", help="Scan code for legacy DBFS mounts and two-tier Hive tables"
    )
    p.add_argument("path", help="Path to Python or SQL file to audit for Unity Catalog")
    p.add_argument("--catalog", default="main", help="Target Unity Catalog name (default: main)")
    p.add_argument("--schema", default="bronze", help="Target Unity Catalog schema (default: bronze)")


def handle(args: argparse.Namespace) -> int:
    target = Path(args.path)
    if not target.exists():
        print(f"Error: File not found: {target}", file=sys.stderr)
        return 2

    source_code = target.read_text(encoding="utf-8")
    doctor = UcDoctor()
    plan = doctor.analyze_source_code(
        source_code, target_catalog=args.catalog, target_schema=args.schema
    )

    print("=" * 70)
    print(f" Unity Catalog Migration Audit: {target.name}")
    print(f" Target Catalog: {args.catalog} | Target Schema: {args.schema}")
    print("=" * 70)

    if not plan.findings:
        print("All checks clean. No deprecated DBFS mounts or two-tier tables found.")
    for f in plan.findings:
        line_str = f"Line {f.line_number}: " if f.line_number else ""
        print(f"\n[{f.severity.value}] {line_str}{f.description}")
        print(f"  Legacy: {f.legacy_pattern}")
        print(f"  Target: {f.recommended_target}")

    if plan.generated_volume_ddl:
        print("\n" + "=" * 70)
        print(" Recommended Unity Catalog External Volumes DDL:")
        print("=" * 70)
        for ddl in plan.generated_volume_ddl:
            print(ddl)

    print("-" * 70)
    has_critical = any(f.severity == UcMigrationSeverity.CRITICAL for f in plan.findings)
    return 1 if has_critical else 0
