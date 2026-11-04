"""Main entry point and parser assembly for dlt-doctor CLI."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable

from dlt_doctor.cli import (
    analyze,
    cdc,
    checkpoint,
    cluster,
    compact,
    cost,
    explain,
    heal,
    history,
    inspect_table,
    replay,
    restate,
    rules,
    schema,
    spill,
    uc_migrate,
    vacuum,
)


def build_parser() -> argparse.ArgumentParser:
    """Construct command-line argument parser with all registered subcommands."""
    parser = argparse.ArgumentParser(
        prog="dlt-doctor",
        description="Production optimization and quality engine for Databricks Lakeflow, DLT, and PySpark",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    analyze.register_parser(subparsers)
    heal.register_parser(subparsers)
    rules.register_parser(subparsers)
    explain.register_parser(subparsers)
    restate.register_parser(subparsers)
    cdc.register_parser(subparsers)
    replay.register_parser(subparsers)
    cluster.register_parser(subparsers)
    compact.register_parser(subparsers)
    schema.register_parser(subparsers)
    vacuum.register_parser(subparsers)
    cost.register_parser(subparsers)
    spill.register_parser(subparsers)
    checkpoint.register_parser(subparsers)
    history.register_parser(subparsers)
    inspect_table.register_parser(subparsers)
    uc_migrate.register_parser(subparsers)

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI execution entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    handlers: dict[str, Callable[[argparse.Namespace], int]] = {
        "analyze": analyze.handle,
        "heal": heal.handle,
        "rules": rules.handle,
        "explain": explain.handle,
        "restate": restate.handle,
        "cdc": cdc.handle,
        "replay": replay.handle,
        "cluster": cluster.handle,
        "compact": compact.handle,
        "schema": schema.handle,
        "vacuum": vacuum.handle,
        "cost": cost.handle,
        "spill": spill.handle,
        "checkpoint": checkpoint.handle,
        "history": history.handle,
        "inspect-table": inspect_table.handle,
        "uc-migrate": uc_migrate.handle,
    }

    handler = handlers.get(args.command)
    if not handler:
        parser.print_help()
        return 2

    return handler(args)


if __name__ == "__main__":
    sys.exit(main())
