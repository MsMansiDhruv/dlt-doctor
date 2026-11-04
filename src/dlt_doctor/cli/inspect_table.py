"""Handler for 'dlt-doctor inspect-table' subcommand."""

from __future__ import annotations

import argparse
import sys

from dlt_doctor.connect import WorkspaceInspector


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "inspect-table", help="Inspect a live Delta Lake table via Databricks SQL Warehouse"
    )
    p.add_argument("table", help="Fully qualified Delta table name (catalog.schema.table)")
    p.add_argument("--warehouse-id", default=None, help="Databricks SQL Warehouse ID")
    p.add_argument(
        "--mock",
        action="store_true",
        default=False,
        help="Run against simulated telemetry data for testing without live credentials",
    )


def handle(args: argparse.Namespace) -> int:
    if args.mock:
        print("**********************************************************************")
        print("*** NOTICE: SIMULATED / MOCK DATA (NO LIVE WORKSPACE CONNECTION)  ***")
        print("**********************************************************************\n")

    inspector = WorkspaceInspector(warehouse_id=args.warehouse_id, mock=args.mock)
    try:
        report = inspector.inspect_table(args.table)
    except Exception as exc:
        print(f"Error inspecting table '{args.table}': {exc}", file=sys.stderr)
        return 1

    print(report.to_text())
    return 0
    return 0
