"""Handler for 'dlt-doctor schema' subcommand."""

from __future__ import annotations

import argparse

from dlt_doctor.schema import SchemaDoctor


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "schema", help="Compare incoming batch schema and generate Delta migration DDL"
    )
    p.add_argument("--table", required=True, help="Target Delta table")
    p.add_argument(
        "--incoming-cols",
        nargs="+",
        required=True,
        help="Incoming column definitions (e.g. col_name:type)",
    )
    p.add_argument(
        "--existing-cols",
        nargs="+",
        default=["id:string", "created_at:timestamp"],
        help="Existing table column definitions (e.g. col_name:type, default: id:string created_at:timestamp)",
    )


def handle(args: argparse.Namespace) -> int:
    existing_schema = {}
    for col_def in args.existing_cols:
        parts = col_def.split(":")
        existing_schema[parts[0]] = parts[1] if len(parts) > 1 else "string"

    incoming_schema = {}
    for col_def in args.incoming_cols:
        parts = col_def.split(":")
        incoming_schema[parts[0]] = parts[1] if len(parts) > 1 else "string"

    doctor = SchemaDoctor()
    report = doctor.compare_schemas(args.table, existing_schema, incoming_schema)

    print("=" * 70)
    print(" DLT-Doctor Schema Evolution Analysis (Template Generator)")
    print(f" Target Table: {args.table} | Has Breaking Changes: {report.has_breaking_changes}")
    print("=" * 70)
    if report.changes:
        print("\nDetected Schema Changes:")
        for c in report.changes:
            print(
                f"  - [{c.severity.value}] {c.column_name}: {c.existing_type} -> {c.incoming_type} ({c.description})"
            )
    else:
        print("No schema changes detected.")

    if report.generated_ddl:
        print("\nGenerated Delta Migration DDL:")
        for ddl in report.generated_ddl:
            print(f"  {ddl}")
    print("-" * 70)
    return 0
