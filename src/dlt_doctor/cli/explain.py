"""Handler for 'dlt-doctor explain' subcommand."""

from __future__ import annotations

import argparse
import sys

from dlt_doctor.rules import get_rule


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("explain", help="Explain a specific diagnostic rule by ID")
    p.add_argument("rule_id", help="Rule ID (e.g. DD001, PYSPARK-001, UC-001)")


def handle(args: argparse.Namespace) -> int:
    rule = get_rule(args.rule_id)
    if not rule:
        print(f"Error: Rule '{args.rule_id}' is not registered.", file=sys.stderr)
        return 2

    print("=" * 70)
    print(f" Rule: {rule.id} - {rule.name}")
    print(f" Default Severity: {rule.default_severity.value}")
    print("=" * 70)
    print("\nDescription:")
    print(rule.description)
    print(f"\nDocumentation Reference:\nhttps://msmansidhruv.github.io/dlt-doctor/{rule.docs_slug}")
    print("-" * 70)
    return 0
