"""Handler for 'dlt-doctor heal' subcommand."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from dlt_doctor.config import DoctorConfig, load_config
from dlt_doctor.heal import HealResult, auto_heal_source
from dlt_doctor.scanner import IGNORED_DIRECTORIES


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "heal", help="Synthesize zero-loss quarantine routing for expect_or_drop"
    )
    p.add_argument("path", help="Path to Python (.py) or SQL (.sql) pipeline file or directory")
    p.add_argument(
        "--write",
        action="store_true",
        default=False,
        help="Apply synthesized code changes directly to files",
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


def _discover_heal_files(target: Path) -> list[Path]:
    """Discover Python and SQL pipeline files while skipping ignored directories."""
    if target.is_file():
        if target.suffix.lower() in {".py", ".sql"}:
            return [target]
        return []

    discovered: list[Path] = []
    for path in target.rglob("*"):
        if path.is_file():
            if any(part in IGNORED_DIRECTORIES for part in path.parts):
                continue
            if path.suffix.lower() in {".py", ".sql"}:
                discovered.append(path)
    return sorted(discovered)


def _check_sql_file_compliance(
    file_path: Path, content: str, config: DoctorConfig
) -> tuple[bool, str]:
    """Check SQL pipeline compliance for ON VIOLATION DROP ROW, honoring suppressions."""
    # 1. Config rule enablement check
    if not config.is_rule_enabled("DD001", file_path.name):
        return True, ""

    # 2. Inline comments (-- dlt-doctor: ignore[DD001] or # dlt-doctor: ignore[DD001])
    for line in content.splitlines()[:25]:
        clean = line.strip()
        comment_prefix = None
        if clean.startswith("--"):
            comment_prefix = "--"
        elif clean.startswith("#"):
            comment_prefix = "#"

        if comment_prefix:
            body = clean[len(comment_prefix) :].strip()
            if body.startswith("dlt-doctor: ignore"):
                after = body[len("dlt-doctor: ignore") :].strip()
                if not after:
                    return True, ""
                if after.startswith("[") and after.endswith("]"):
                    rules = [r.strip().upper() for r in after[1:-1].split(",")]
                    if "DD001" in rules or "ALL" in rules:
                        return True, ""

    # 3. Detect ON VIOLATION DROP ROW
    if re.search(r"\bON\s+VIOLATION\s+DROP\s+ROW\b", content, re.IGNORECASE):
        # If explicit regulated_tables configured, verify table matches
        if config.has_explicit_regulated_tables:
            m = re.search(
                r"CREATE\s+(?:OR\s+REFRESH\s+)?(?:STREAMING\s+)?TABLE\s+([A-Za-z0-9_\.]+)",
                content,
                re.IGNORECASE,
            )
            if m:
                tbl = m.group(1).split(".")[-1]
                if not config.is_regulated_table(tbl):
                    return True, ""

        return (
            False,
            "Cannot auto-heal SQL pipeline: ON VIOLATION DROP ROW requires manual migration to dual-table quarantine.",
        )

    return True, ""


def handle(args: argparse.Namespace) -> int:
    target = Path(args.path)
    if not target.exists():
        print(f"Error: Path not found: {target}", file=sys.stderr)
        return 2

    config = load_config(target if target.is_dir() else target.parent)
    files = _discover_heal_files(target)

    if not files:
        if target.is_dir():
            print(f"No pipeline files found in directory {target}")
        else:
            print(f"File {target.name} is not a supported pipeline format (.py or .sql)")
        return 0

    write_mode = args.write and not args.check

    # Compute all results in memory first to prevent partial application in write mode
    cannot_heal: list[tuple[Path, str]] = []
    needs_healing: list[tuple[Path, HealResult]] = []
    compliant_files: list[Path] = []

    for f in files:
        try:
            content = f.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            cannot_heal.append((f, f"Cannot decode file as UTF-8: {exc}"))
            continue

        if f.suffix.lower() == ".sql":
            can_heal, reason = _check_sql_file_compliance(f, content, config)
            if not can_heal:
                cannot_heal.append((f, reason))
            else:
                compliant_files.append(f)
            continue

        res = auto_heal_source(
            content,
            file_path=f.name,
            quarantine_suffix=args.quarantine_suffix,
            config=config,
        )
        if not res.can_heal:
            cannot_heal.append((f, res.reason))
        elif res.diff:
            needs_healing.append((f, res))
        else:
            compliant_files.append(f)

    # If any file cannot be healed, do NOT write any file and exit 3
    if cannot_heal:
        for f, reason in cannot_heal:
            print(f"Cannot auto-heal {f.name}: {reason}", file=sys.stderr)
        return 3

    # Handle files that need healing
    if needs_healing:
        # Print any warnings from healing
        for f, res in needs_healing:
            for w in res.warnings:
                print(f"Warning in {f.name}: {w}", file=sys.stderr)

        if args.check:
            for f, res in needs_healing:
                print(f"File {f.name} needs healing:\n{res.diff}")
            return 1

        if write_mode:
            # Safely apply writes now that all files have been validated
            for f, res in needs_healing:
                f.write_text(res.healed_code, encoding="utf-8")
                print(f"Applied zero-loss quarantine routing to {f.name}:\n{res.diff}")
            return 0

        # Preview mode
        for f, res in needs_healing:
            print(f"Unified Diff for {f.name} (run with --write to apply):\n{res.diff}")
        return 0

    if target.is_dir():
        print(f"All {len(files)} files in {target} are compliant.")
    else:
        print(
            f"File {target.name} is already compliant or has no unsuppressed expect_or_drop rules."
        )

    return 0
