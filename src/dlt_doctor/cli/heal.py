"""Handler for 'dlt-doctor heal' subcommand."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dlt_doctor.heal import heal_file


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "heal", help="Synthesize zero-loss quarantine routing for expect_or_drop"
    )
    p.add_argument("path", help="Path to Python (.py) pipeline file")
    p.add_argument(
        "--write",
        action="store_true",
        default=False,
        help="Apply synthesized code changes directly to the file",
    )
    p.add_argument(
        "--check",
        action="store_true",
        default=False,
        help="Don't write changes; exit 1 if changes are needed and exit 0 if compliant",
    )
    p.add_argument(
        "--quarantine-suffix",
        default="_quarantine",
        help="Suffix for synthesized quarantine table (default: _quarantine)",
    )


def handle(args: argparse.Namespace) -> int:
    target = Path(args.path)
    if not target.exists():
        print(f"Error: File not found: {target}", file=sys.stderr)
        return 2

    if target.is_dir():
        print("Error: 'heal' must be executed on a specific file.", file=sys.stderr)
        return 2

    write_mode = args.write and not args.check
    res = heal_file(
        target,
        write=write_mode,
        quarantine_suffix=args.quarantine_suffix,
    )
    if not res.can_heal:
        print(f"Cannot auto-heal: {res.reason}", file=sys.stderr)
        return 1

    if res.diff:
        if args.check:
            print(f"File {target.name} needs healing:")
            print(res.diff)
            return 1
        if args.write:
            print(f"Applied zero-loss quarantine routing to {target.name}:")
            print(res.diff)
        else:
            print("Unified Diff (run with --write to apply):")
            print(res.diff)
    else:
        print(f"File {target.name} is already compliant or has no expect_or_drop rules.")

    return 0
