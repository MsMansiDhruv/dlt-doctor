"""Handler for 'dlt-doctor rules' subcommand."""

from __future__ import annotations

import argparse

from dlt_doctor.rules import list_rules


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    subparsers.add_parser("rules", help="List all registered diagnostic rules")


def handle(args: argparse.Namespace) -> int:
    rules = list_rules()
    print("=" * 80)
    print(" DLT-Doctor Registered Diagnostic Rules")
    print("=" * 80)
    print(f"{'Rule ID':<14} | {'Severity':<10} | {'Name'}")
    print("-" * 80)
    for r in rules:
        print(f"{r.id:<14} | {r.default_severity.value:<10} | {r.name}")
    print("-" * 80)
    print(f"Total Rules: {len(rules)} | Run 'dlt-doctor explain <RULE_ID>' for details.")
    return 0
