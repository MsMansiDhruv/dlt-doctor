"""Unity Catalog migration scanner and DBFS mount path auditor."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from enum import Enum


class UcMigrationSeverity(str, Enum):
    """Severity of legacy catalog or storage path pattern."""

    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass
class UcFinding:
    """Identified legacy pattern requiring Unity Catalog migration."""

    rule_id: str
    severity: UcMigrationSeverity
    legacy_pattern: str
    recommended_target: str
    line_number: int | None
    description: str


@dataclass
class UcMigrationPlan:
    """Consolidated migration report and generated DDL for Unity Catalog."""

    target_catalog: str
    target_schema: str
    findings: list[UcFinding] = field(default_factory=list)
    generated_volume_ddl: list[str] = field(default_factory=list)
    table_mapping: dict[str, str] = field(default_factory=dict)
    path_mapping: dict[str, str] = field(default_factory=dict)


class UcDoctor:
    """Audits PySpark and SQL pipelines for legacy Hive Metastore and DBFS patterns."""

    DBFS_MOUNT_REGEX = re.compile(
        r"(?:dbfs:)?/mnt/([a-zA-Z0-9_\-]+)(?:/([^\s'\"\`]+))?", re.IGNORECASE
    )
    TWO_TIER_TABLE_REGEX = re.compile(
        r"\b(?:table|from|join)\s*\(?['\"]([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)['\"]",
        re.IGNORECASE,
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
                        description=f"Legacy DBFS mount path '{legacy_str}' detected. DBFS mounts are deprecated in Unity Catalog.",
                    )
                )
                plan.path_mapping[legacy_str] = vol_path
                plan.generated_volume_ddl.append(
                    f"CREATE VOLUME IF NOT EXISTS {target_catalog}.{target_schema}.{mount_name};"
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
        try:
            tree = ast.parse(source_code)
            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    func_name = ""
                    if isinstance(node.func, ast.Attribute):
                        func_name = node.func.attr
                    elif isinstance(node.func, ast.Name):
                        func_name = node.func.id

                    if func_name in ("table", "read_stream", "read"):
                        if (
                            node.args
                            and isinstance(node.args[0], ast.Constant)
                            and isinstance(node.args[0].value, str)
                        ):
                            table_ref = node.args[0].value
                            parts = table_ref.split(".")
                            if len(parts) == 1:
                                target_ref = (
                                    f"{target_catalog}.{target_schema}.{parts[0]}"
                                )
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
