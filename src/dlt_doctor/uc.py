"""Unity Catalog (UC) migration and namespace governance."""

from __future__ import annotations

import ast
import enum
import re
from dataclasses import dataclass, field
from typing import Any


class UcMigrationSeverity(str, enum.Enum):
    """Urgency level for Unity Catalog compatibility."""

    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass
class UcFinding:
    """An individual Unity Catalog migration item."""

    rule_id: str
    severity: UcMigrationSeverity
    legacy_pattern: str
    recommended_target: str
    line_number: int | None = None
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "severity": self.severity.value,
            "legacy_pattern": self.legacy_pattern,
            "recommended_target": self.recommended_target,
            "line_number": self.line_number,
            "description": self.description,
        }


@dataclass
class UcMigrationPlan:
    """Generated instructions and automated DDL for Unity Catalog adoption."""

    target_catalog: str
    target_schema: str
    findings: list[UcFinding] = field(default_factory=list)
    path_mapping: dict[str, str] = field(default_factory=dict)
    table_mapping: dict[str, str] = field(default_factory=dict)
    generated_volume_ddl: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_catalog": self.target_catalog,
            "target_schema": self.target_schema,
            "findings": [f.to_dict() for f in self.findings],
            "path_mapping": self.path_mapping,
            "table_mapping": self.table_mapping,
            "generated_volume_ddl": self.generated_volume_ddl,
        }


class UcDoctor:
    """Audits PySpark/SQL codebases for DBFS mounts, legacy paths, and two-tier tables."""

    DBFS_MOUNT_REGEX = re.compile(
        r"['\"](?:dbfs:)?/mnt/([a-zA-Z0-9_\-\.]+)(?:/([^'\"]*))?['\"]"
    )
    RAW_STORAGE_REGEX = re.compile(
        r"(s3a?://|abfss?://|gs://)([^/\s'\"\`]+)/([^\s'\"\`]+)", re.IGNORECASE
    )

    def analyze_source_code(
        self,
        source_code: str,
        target_catalog: str = "main",
        target_schema: str = "bronze",
    ) -> UcMigrationPlan:
        """Scan source code for legacy storage paths and two-tier table references."""
        plan = UcMigrationPlan(
            target_catalog=target_catalog, target_schema=target_schema
        )

        # 1. Regex scan for raw DBFS mount paths (/mnt/...)
        for line_num, line in enumerate(source_code.splitlines(), start=1):
            for match in self.DBFS_MOUNT_REGEX.finditer(line):
                mount_name = match.group(1)
                sub_path = match.group(2) or ""
                legacy_str = match.group(0)
                vol_path = f"/Volumes/{target_catalog}/{target_schema}/{mount_name}/{sub_path}".rstrip(
                    "/"
                )

                plan.findings.append(
                    UcFinding(
                        rule_id="UC-001",
                        severity=UcMigrationSeverity.CRITICAL,
                        legacy_pattern=legacy_str,
                        recommended_target=vol_path,
                        line_number=line_num,
                        description=(
                            f"Legacy DBFS mount path '{legacy_str}' detected. "
                            "DBFS mounts are deprecated in Unity Catalog workspaces."
                        ),
                    )
                )
                plan.path_mapping[legacy_str] = vol_path
                plan.generated_volume_ddl.append(
                    f"-- External volume template for legacy mount '{mount_name}'. Data is not moved automatically.\n"
                    f"CREATE EXTERNAL VOLUME IF NOT EXISTS {target_catalog}.{target_schema}.{mount_name}\n"
                    "LOCATION '<set-underlying-storage-location-uri>';"
                )

            # 2. Check for raw cloud storage URLs (s3://, abfss://) outside Unity Catalog Volumes
            for match in self.RAW_STORAGE_REGEX.finditer(line):
                raw_uri = match.group(0)
                bucket_or_container = match.group(2)
                sub_path = match.group(3)
                vol_path = f"/Volumes/{target_catalog}/{target_schema}/{bucket_or_container}/{sub_path}".rstrip(
                    "/"
                )

                plan.findings.append(
                    UcFinding(
                        rule_id="UC-002",
                        severity=UcMigrationSeverity.WARNING,
                        legacy_pattern=raw_uri,
                        recommended_target=vol_path,
                        line_number=line_num,
                        description=f"Direct cloud storage URI '{raw_uri}' bypasses Unity Catalog governance.",
                    )
                )
                plan.path_mapping[raw_uri] = vol_path

        # 3. AST analysis for spark.table(...) and spark.read.table(...) calls
        # Note: Do not flag dlt.read, dlt.read_stream, or dp.read as these are idiomatic pipeline table references.
        try:
            tree = ast.parse(source_code)
            # Collect project/pipeline-defined tables to avoid false positives
            defined_tables: set[str] = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    for dec in node.decorator_list:
                        dec_name = ""
                        if isinstance(dec, ast.Name):
                            dec_name = dec.id
                        elif isinstance(dec, ast.Attribute):
                            dec_name = dec.attr
                        elif isinstance(dec, ast.Call):
                            if isinstance(dec.func, ast.Name):
                                dec_name = dec.func.id
                            elif isinstance(dec.func, ast.Attribute):
                                dec_name = dec.func.attr
                        if dec_name in {"table", "view", "materialized_view", "streaming_table"}:
                            defined_tables.add(node.name)
                            if isinstance(dec, ast.Call):
                                for kw in dec.keywords:
                                    if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                                        defined_tables.add(str(kw.value.value))

            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    # Only flag spark.table / spark.read.table / spark.readStream.table
                    is_spark_table_call = False
                    if node.func.attr == "table":
                        caller = node.func.value
                        if (isinstance(caller, ast.Name) and caller.id == "spark") or (isinstance(caller, ast.Attribute) and caller.attr in {"read", "readStream"}):
                            is_spark_table_call = True

                    if is_spark_table_call and node.args:
                        arg = node.args[0]
                        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                            table_ref = arg.value
                            # Skip if defined as an internal pipeline table
                            if table_ref in defined_tables:
                                continue

                            parts = table_ref.split(".")
                            if len(parts) == 1:
                                target_ref = f"{target_catalog}.{target_schema}.{parts[0]}"
                                plan.findings.append(
                                    UcFinding(
                                        rule_id="UC-003",
                                        severity=UcMigrationSeverity.WARNING,
                                        legacy_pattern=table_ref,
                                        recommended_target=target_ref,
                                        line_number=node.lineno,
                                        description=f"Single-tier table reference '{table_ref}' relies on default session catalog.",
                                    )
                                )
                                plan.table_mapping[table_ref] = target_ref
                            elif len(parts) == 2:
                                target_ref = f"{target_catalog}.{parts[0]}.{parts[1]}"
                                plan.findings.append(
                                    UcFinding(
                                        rule_id="UC-004",
                                        severity=UcMigrationSeverity.INFO,
                                        legacy_pattern=table_ref,
                                        recommended_target=target_ref,
                                        line_number=node.lineno,
                                        description=f"Two-tier Hive Metastore table reference '{table_ref}' should be qualified with catalog.",
                                    )
                                )
                                plan.table_mapping[table_ref] = target_ref
        except SyntaxError:
            pass

        # Deduplicate volume DDL
        plan.generated_volume_ddl = sorted(list(set(plan.generated_volume_ddl)))
        return plan

    def audit_code(
        self,
        source_code: str,
        target_catalog: str = "main",
        target_schema: str = "bronze",
    ) -> list[UcFinding]:
        """Convenience method returning findings directly."""
        plan = self.analyze_source_code(source_code, target_catalog, target_schema)
        return plan.findings
