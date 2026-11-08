"""Handler for 'dlt-doctor heal' subcommand."""

from __future__ import annotations

import argparse
import contextlib
import os
import re
import sys
import tempfile
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
    p.add_argument(
        "--config",
        default=None,
        help="Path to custom configuration file or directory containing pyproject.toml",
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
            try:
                rel_parts = path.relative_to(target).parts[:-1]
            except ValueError:
                rel_parts = path.parts[:-1]
            if any(part in IGNORED_DIRECTORIES for part in rel_parts):
                continue
            if path.suffix.lower() in {".py", ".sql"}:
                discovered.append(path)
    return sorted(discovered)


def _check_sql_file_compliance(
    file_path: Path, content: str, config: DoctorConfig, rel_path: str | None = None
) -> tuple[bool, str]:
    """Check SQL pipeline compliance for ON VIOLATION DROP ROW, honoring suppressions."""
    # 1. Config rule enablement check
    target_path = rel_path or file_path.name
    if not config.is_rule_enabled("DD001", target_path):
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

    # 3. Detect table statements and evaluate violations per table
    table_pattern = re.compile(
        r"CREATE\s+(?:OR\s+REFRESH\s+)?(?:STREAMING\s+)?TABLE\s+([A-Za-z0-9_\.]+)",
        re.IGNORECASE,
    )
    table_matches = list(table_pattern.finditer(content))

    if not table_matches:
        if re.search(r"\bON\s+VIOLATION\s+DROP\s+ROW\b", content, re.IGNORECASE):
            return (
                False,
                "Cannot auto-heal SQL pipeline: ON VIOLATION DROP ROW requires manual migration to dual-table quarantine.",
            )
        return True, ""

    for i, m in enumerate(table_matches):
        tbl_full = m.group(1)
        tbl_name = tbl_full.split(".")[-1]
        start_pos = m.start()
        end_pos = table_matches[i + 1].start() if i + 1 < len(table_matches) else len(content)
        chunk = content[start_pos:end_pos]

        if re.search(r"\bON\s+VIOLATION\s+DROP\s+ROW\b", chunk, re.IGNORECASE):
            if config.has_explicit_regulated_tables and not config.is_regulated_table(tbl_name):
                continue
            return (
                False,
                f"Cannot auto-heal SQL pipeline: ON VIOLATION DROP ROW in table '{tbl_name}' requires manual migration to dual-table quarantine.",
            )

    return True, ""


def handle(args: argparse.Namespace) -> int:
    target = Path(args.path)
    if not target.exists():
        print(f"Error: Path not found: {target}", file=sys.stderr)
        return 2

    config_target = (
        Path(args.config)
        if getattr(args, "config", None)
        else (target if target.is_dir() else target.parent)
    )
    config = load_config(config_target)
    files = _discover_heal_files(target)

    if not files:
        if target.is_dir():
            print(f"No pipeline files found in directory {target}")
        else:
            print(f"File {target.name} is not a supported pipeline format (.py or .sql)")
        return 0

    config_root = config_target if config_target.is_dir() else config_target.parent
    write_mode = args.write and not args.check

    # Compute all results in memory first to prevent partial application in write mode
    cannot_heal: list[tuple[Path, str]] = []
    needs_healing: list[tuple[Path, HealResult]] = []
    compliant_files: list[Path] = []

    for f in files:
        try:
            rel_file_path = str(f.relative_to(config_root)).replace("\\", "/")
        except ValueError:
            rel_file_path = str(f).replace("\\", "/")

        try:
            content = f.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            cannot_heal.append((f, f"Cannot decode file as UTF-8: {exc}"))
            continue

        if f.suffix.lower() == ".sql":
            can_heal, reason = _check_sql_file_compliance(
                f, content, config, rel_path=rel_file_path
            )
            if not can_heal:
                cannot_heal.append((f, reason))
            else:
                compliant_files.append(f)
            continue

        res = auto_heal_source(
            content,
            file_path=rel_file_path,
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
            # Safely apply writes with line ending preservation and atomic rename
            for f, res in needs_healing:
                raw_bytes = f.read_bytes()
                target_newline = "\r\n" if b"\r\n" in raw_bytes else "\n"

                # Normalize healed code to preserve original line endings
                normalized_code = res.healed_code.replace("\r\n", "\n")
                if target_newline == "\r\n":
                    normalized_code = normalized_code.replace("\n", "\r\n")

                temp_file: Path | None = None
                try:
                    with tempfile.NamedTemporaryFile(
                        mode="w",
                        encoding="utf-8",
                        newline="",
                        dir=f.parent,
                        delete=False,
                    ) as tmp:
                        tmp.write(normalized_code)
                        temp_file = Path(tmp.name)
                    os.replace(temp_file, f)
                except Exception:
                    if temp_file and temp_file.exists():
                        with contextlib.suppress(OSError):
                            temp_file.unlink()
                    raise

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
