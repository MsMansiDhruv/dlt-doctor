"""Configuration loading and rule suppression management for dlt-doctor."""

from __future__ import annotations

import contextlib
import fnmatch
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dlt_doctor.catalyst import DiagnosticSeverity

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib  # type: ignore[no-redef, import-not-found]


@dataclass
class DoctorConfig:
    """Configuration options for dlt-doctor audits."""

    select: list[str] = field(default_factory=list)
    ignore: list[str] = field(default_factory=list)
    fail_on: DiagnosticSeverity = DiagnosticSeverity.CRITICAL
    regulated_tables: list[str] = field(
        default_factory=lambda: ["*orders*", "*customers*", "*txn*", "*finance*"]
    )
    has_explicit_regulated_tables: bool = False
    per_file_ignores: dict[str, list[str]] = field(default_factory=dict)
    severity_overrides: dict[str, DiagnosticSeverity] = field(default_factory=dict)

    def is_rule_enabled(self, rule_id: str, file_name: str | None = None) -> bool:
        """Check if a rule is enabled considering select, ignore, and per-file ignores."""
        rule_upper = rule_id.upper()
        if self.select and rule_upper not in [s.upper() for s in self.select]:
            return False
        if rule_upper in [i.upper() for i in self.ignore]:
            return False
        if not file_name or not self.per_file_ignores:
            return True
        normalized_file = file_name.replace("\\", "/")
        base_name = Path(file_name).name
        for pattern, ignored_rules in self.per_file_ignores.items():
            normalized_pattern = pattern.replace("\\", "/")
            matches = (
                fnmatch.fnmatch(normalized_file, normalized_pattern)
                or fnmatch.fnmatch(normalized_file, f"*/{normalized_pattern.lstrip('/')}")
                or fnmatch.fnmatch(base_name, normalized_pattern)
            )
            if matches and rule_upper in [i.upper() for i in ignored_rules]:
                return False
        return True

    def get_severity(self, rule_id: str, default: DiagnosticSeverity) -> DiagnosticSeverity:
        """Get severity of a rule with config overrides applied."""
        return self.severity_overrides.get(rule_id.upper(), default)

    def is_regulated_table(self, table_name: str) -> bool:
        """Check if a table matches any regulated table globs."""
        for pattern in self.regulated_tables:
            if fnmatch.fnmatch(table_name.lower(), pattern.lower()):
                return True
        return False


def load_config(root_path: Path | str | None = None) -> DoctorConfig:
    """Discover and parse dlt-doctor configuration from pyproject.toml or dlt-doctor.toml."""
    target_path = Path(root_path) if root_path else Path.cwd()
    if target_path.is_file() and target_path.exists():
        try:
            data = tomllib.loads(target_path.read_text(encoding="utf-8"))
            section: dict[str, Any] = {}
            if "tool" in data and "dlt-doctor" in data["tool"]:
                section = data["tool"]["dlt-doctor"]
            elif "dlt-doctor" in data:
                section = data["dlt-doctor"]
            elif data:
                section = data

            if section:
                return _parse_config_dict(section)
        except (tomllib.TOMLDecodeError, OSError):
            return DoctorConfig()
        search_dir = target_path.parent
    else:
        search_dir = target_path

    # Check dlt-doctor.toml then pyproject.toml
    config_files = [
        search_dir / "dlt-doctor.toml",
        search_dir / "pyproject.toml",
    ]

    for p in search_dir.parents:
        config_files.append(p / "dlt-doctor.toml")
        config_files.append(p / "pyproject.toml")

    for cfg_file in config_files:
        if cfg_file.exists():
            try:
                data = tomllib.loads(cfg_file.read_text(encoding="utf-8"))
                cfg_section: dict[str, Any] = {}
                if "tool" in data and "dlt-doctor" in data["tool"]:
                    cfg_section = data["tool"]["dlt-doctor"]
                elif "dlt-doctor" in data:
                    cfg_section = data["dlt-doctor"]

                if cfg_section:
                    return _parse_config_dict(cfg_section)
            except (tomllib.TOMLDecodeError, OSError):
                return DoctorConfig()

    return DoctorConfig()


def _parse_config_dict(data: dict[str, Any]) -> DoctorConfig:
    """Parse raw dictionary into DoctorConfig."""
    fail_on_str = str(data.get("fail_on", "CRITICAL")).upper()
    try:
        fail_on = DiagnosticSeverity(fail_on_str)
    except ValueError:
        fail_on = DiagnosticSeverity.CRITICAL

    severity_overrides: dict[str, DiagnosticSeverity] = {}
    for r_id, sev in data.get("severity_overrides", {}).items():
        with contextlib.suppress(ValueError):
            severity_overrides[r_id.upper()] = DiagnosticSeverity(str(sev).upper())

    has_explicit_reg = "regulated_tables" in data
    return DoctorConfig(
        select=data.get("select", []),
        ignore=data.get("ignore", []),
        fail_on=fail_on,
        regulated_tables=data.get(
            "regulated_tables", ["*orders*", "*customers*", "*txn*", "*finance*"]
        ),
        has_explicit_regulated_tables=has_explicit_reg,
        per_file_ignores=data.get("per_file_ignores", {}),
        severity_overrides=severity_overrides,
    )
