"""Delta Live Tables (DLT) and Lakeflow Declarative Pipelines expectation wrappers and single-pass quarantine routing."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import sqlglot

ALLOWED_ACTIONS = {"quarantine", "drop", "fail", "warn"}


def _safe_identifier(name: str) -> str:
    """Convert dotted or special character table names into valid Python identifiers."""
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if cleaned and cleaned[0].isdigit():
        cleaned = f"t_{cleaned}"
    return cleaned or "pipeline_table"


@dataclass
class ExpectationRule:
    """A business data quality expectation for DLT / Lakeflow tables."""

    rule_name: str
    condition_sql: str
    action_on_failure: str = "quarantine"  # quarantine, drop, fail, warn
    description: str = ""

    def __post_init__(self) -> None:
        if self.action_on_failure not in ALLOWED_ACTIONS:
            raise ValueError(
                f"Invalid action_on_failure '{self.action_on_failure}'. Allowed: {sorted(ALLOWED_ACTIONS)}"
            )
        try:
            statements = sqlglot.parse(self.condition_sql, read="spark")
        except Exception as exc:
            raise ValueError(
                f"Failed to parse expectation condition '{self.condition_sql}': {exc}"
            ) from exc

        if len(statements) != 1 or statements[0] is None:
            raise ValueError(
                f"Expectation condition must be a single expression, got {len(statements)} statements."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "condition_sql": self.condition_sql,
            "action_on_failure": self.action_on_failure,
            "description": self.description,
        }


@dataclass
class QuarantineRoutingPlan:
    """Execution instructions for routing valid records and quarantining invalid rows."""

    target_clean_table: str
    target_quarantine_table: str
    valid_filter_sql: str
    invalid_filter_sql: str
    failed_rules_expr_sql: str = "array()"
    rules: list[ExpectationRule] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_clean_table": self.target_clean_table,
            "target_quarantine_table": self.target_quarantine_table,
            "valid_filter_sql": self.valid_filter_sql,
            "invalid_filter_sql": self.invalid_filter_sql,
            "failed_rules_expr_sql": self.failed_rules_expr_sql,
            "rules": [r.to_dict() for r in self.rules],
        }


class ExpectationDoctor:
    """Manages resilient quality gates without silent data drops or pipeline halts."""

    def __init__(self) -> None:
        pass

    def build_quarantine_routing(
        self,
        table_name: str,
        rules: list[ExpectationRule],
    ) -> QuarantineRoutingPlan:
        """Construct NULL-safe dual-routing filters separating clean data from quarantined records."""
        if not rules:
            return QuarantineRoutingPlan(
                target_clean_table=table_name,
                target_quarantine_table=f"{table_name}_quarantine",
                valid_filter_sql="1 = 1",
                invalid_filter_sql="1 = 0",
                failed_rules_expr_sql="array()",
                rules=[],
            )

        # All rules must pass for clean stream, wrapped in coalesce((<cond>), false) for NULL safety
        valid_conditions = [f"coalesce(({r.condition_sql}), false)" for r in rules]
        valid_filter = " AND ".join(valid_conditions)

        # Quarantine stream is the strict complement: NOT (valid_filter)
        invalid_filter = f"NOT ({valid_filter})"

        # Compute array of failed rule names (NULL condition evaluation counts as failure)
        rule_case_exprs: list[str] = []
        for r in rules:
            escaped_name = r.rule_name.replace("'", "''")
            rule_case_exprs.append(
                f"CASE WHEN NOT (coalesce(({r.condition_sql}), false)) THEN '{escaped_name}' ELSE NULL END"
            )

        failed_rules_expr = f"array_compact(array({', '.join(rule_case_exprs)}))"

        return QuarantineRoutingPlan(
            target_clean_table=table_name,
            target_quarantine_table=f"{table_name}_quarantine",
            valid_filter_sql=valid_filter,
            invalid_filter_sql=invalid_filter,
            failed_rules_expr_sql=failed_rules_expr,
            rules=rules,
        )

    def generate_pipeline_code(
        self,
        routing_plan: QuarantineRoutingPlan,
        source_table: str,
        flavor: str = "dp",
    ) -> str:
        """Generate single-pass Lakeflow or DLT pipeline definitions with quarantine routing."""
        clean_func = _safe_identifier(routing_plan.target_clean_table)
        quarantine_func = _safe_identifier(routing_plan.target_quarantine_table)
        staging_view_name = f"{routing_plan.target_clean_table}_staged"
        staging_func = f"{clean_func}_staged"

        if flavor == "dp":
            import_line = "from pyspark import pipelines as dp\nfrom pyspark.sql import functions as F"
            pkg = "dp"
        elif flavor == "dlt":
            import_line = "import dlt\nfrom pyspark.sql import functions as F"
            pkg = "dlt"
        else:
            raise ValueError(f"Unknown pipeline flavor '{flavor}'. Choose 'dp' or 'dlt'.")

        escaped_expr = (
            routing_plan.failed_rules_expr_sql.replace("\\", "\\\\").replace('"', '\\"')
        )

        code_lines = [
            "# Auto-generated single-pass validation and quarantine pipeline by dlt-doctor",
            import_line,
            "",
            f"@{pkg}.view(",
            f"    name='{staging_view_name}',",
            "    comment='Staging stream with expectation contract evaluation.'",
            ")",
            f"def {staging_func}():",
            "    return (",
            f"        {pkg}.read_stream('{source_table}')",
            f'        .withColumn("_failed_rules", F.expr("{escaped_expr}"))',
            '        .withColumn("_quarantined_at", F.current_timestamp())',
            "    )",
            "",
            f"# Clean target table: {routing_plan.target_clean_table}",
            f"@{pkg}.table(",
            f"    name='{routing_plan.target_clean_table}',",
            "    comment='Clean records passing all expectation contracts.'",
            ")",
            f"def {clean_func}():",
            "    return (",
            f"        {pkg}.read_stream('{staging_view_name}')",
            '        .filter(F.size(F.col("_failed_rules")) == 0)',
            '        .drop("_failed_rules", "_quarantined_at")',
            "    )",
            "",
            f"# Dead-letter quarantine table: {routing_plan.target_quarantine_table}",
            f"@{pkg}.table(",
            f"    name='{routing_plan.target_quarantine_table}',",
            "    comment='Quarantined records failing business expectations for triage.'",
            ")",
            f"def {quarantine_func}():",
            "    return (",
            f"        {pkg}.read_stream('{staging_view_name}')",
            '        .filter(F.size(F.col("_failed_rules")) > 0)',
            "    )",
        ]
        return "\n".join(code_lines)

    def generate_dlt_decorator_code(
        self,
        routing_plan: QuarantineRoutingPlan,
        source_table: str = "source_bronze",
        flavor: str = "dp",
    ) -> str:
        """Backward-compatible wrapper for generate_pipeline_code."""
        return self.generate_pipeline_code(
            routing_plan, source_table=source_table, flavor=flavor
        )
