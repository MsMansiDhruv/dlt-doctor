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


def _escape_table_name(table_name: str) -> str:
    """Safely escape catalog, schema, and table components with backticks for Unity Catalog."""
    parts = table_name.strip().split(".")
    escaped = [f"`{p.strip('`')}`" for p in parts]
    return ".".join(escaped)


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
    source_table: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_clean_table": self.target_clean_table,
            "target_quarantine_table": self.target_quarantine_table,
            "valid_filter_sql": self.valid_filter_sql,
            "invalid_filter_sql": self.invalid_filter_sql,
            "failed_rules_expr_sql": self.failed_rules_expr_sql,
            "rules": [r.to_dict() for r in self.rules],
            "source_table": self.source_table,
        }

    def generate_pipeline_code(self, source_table: str | None = None, flavor: str = "dp") -> str:
        """Convenience method to synthesize pipeline code directly from routing plan."""
        return ExpectationDoctor().generate_pipeline_code(
            self, source_table=source_table, flavor=flavor
        )


class ExpectationDoctor:
    """Manages resilient quality gates without silent data drops or pipeline halts."""

    def __init__(self) -> None:
        pass

    def build_quarantine_routing(
        self,
        table_name: str,
        rules: list[ExpectationRule],
        source_table: str | None = None,
        quarantine_table: str | None = None,
    ) -> QuarantineRoutingPlan:
        """Construct NULL-safe dual-routing filters separating clean data from quarantined records.

        Runtime Compatibility Note:
        The generated `failed_rules_expr_sql` uses `array_compact(...)`, which requires
        Apache Spark 3.4.0+ or Databricks Runtime (DBR) 13.0+. For older runtimes (DBR <= 12.2),
        replace with `filter(array(...), x -> x IS NOT NULL)`.
        """
        # Honor action_on_failure: only rules with action_on_failure in {'quarantine', 'drop'} are routed to quarantine.
        # Rules with action_on_failure == 'warn' log metrics but do not drop or quarantine records.
        quarantine_rules = [r for r in rules if r.action_on_failure in {"quarantine", "drop"}]
        effective_quarantine_table = quarantine_table or f"{table_name}_quarantine"

        if not quarantine_rules:
            return QuarantineRoutingPlan(
                target_clean_table=table_name,
                target_quarantine_table=effective_quarantine_table,
                valid_filter_sql="1 = 1",
                invalid_filter_sql="1 = 0",
                failed_rules_expr_sql="array()",
                rules=rules,
                source_table=source_table,
            )

        # All quarantine rules must pass for clean stream, wrapped in coalesce((<cond>), false) for NULL safety
        valid_conditions = [f"coalesce(({r.condition_sql}), false)" for r in quarantine_rules]
        valid_filter = " AND ".join(valid_conditions)

        # Quarantine stream is the strict complement: NOT (valid_filter)
        invalid_filter = f"NOT ({valid_filter})"

        # Compute array of failed rule names (NULL condition evaluation counts as failure)
        rule_case_exprs: list[str] = []
        for r in quarantine_rules:
            escaped_name = r.rule_name.replace("'", "''")
            rule_case_exprs.append(
                f"CASE WHEN NOT (coalesce(({r.condition_sql}), false)) THEN '{escaped_name}' ELSE NULL END"
            )

        failed_rules_expr = f"array_compact(array({', '.join(rule_case_exprs)}))"

        return QuarantineRoutingPlan(
            target_clean_table=table_name,
            target_quarantine_table=effective_quarantine_table,
            valid_filter_sql=valid_filter,
            invalid_filter_sql=invalid_filter,
            failed_rules_expr_sql=failed_rules_expr,
            rules=rules,
            source_table=source_table,
        )

    def generate_pipeline_code(
        self,
        routing_plan: QuarantineRoutingPlan,
        source_table: str | None = None,
        flavor: str = "dp",
    ) -> str:
        """Generate single-pass Lakeflow or DLT pipeline definitions with quarantine routing.

        Runtime Compatibility Note:
        Requires Apache Spark 3.4.0+ / Databricks Runtime (DBR) 13.0+ for `array_compact`.

        API Note on `pyspark.pipelines` (dp):
        In Databricks Lakeflow Declarative Pipelines (`from pyspark import pipelines as dp`),
        `dp.read_stream` does not exist. Stream ingestion uses `spark.readStream.table(...)`.
        """
        resolved_source = source_table or routing_plan.source_table
        if not resolved_source:
            raise ValueError(
                "source_table must be provided either in build_quarantine_routing or in generate_pipeline_code."
            )
        clean_func = _safe_identifier(routing_plan.target_clean_table)
        quarantine_func = _safe_identifier(routing_plan.target_quarantine_table)
        staging_view_name = f"{routing_plan.target_clean_table}_staged"
        staging_func = f"{clean_func}_staged"

        escaped_src = _escape_table_name(resolved_source)
        escaped_staging = _escape_table_name(staging_view_name)

        if flavor == "dp":
            import_line = (
                "from pyspark import pipelines as dp\nfrom pyspark.sql import functions as F"
            )
            pkg = "dp"
            read_src = f"spark.readStream.table('{escaped_src}')"
            read_staging = f"spark.readStream.table('{escaped_staging}')"
        elif flavor == "dlt":
            import_line = "import dlt\nfrom pyspark.sql import functions as F"
            pkg = "dlt"
            read_src = f"dlt.read_stream('{escaped_src}')"
            read_staging = f"dlt.read_stream('{escaped_staging}')"
        else:
            raise ValueError(f"Unknown pipeline flavor '{flavor}'. Choose 'dp' or 'dlt'.")

        escaped_expr = routing_plan.failed_rules_expr_sql.replace("\\", "\\\\").replace('"', '\\"')

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
            f"        {read_src}",
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
            f"        {read_staging}",
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
            f"        {read_staging}",
            '        .filter(F.size(F.col("_failed_rules")) > 0)',
            "    )",
        ]
        return "\n".join(code_lines)

    def generate_dlt_decorator_code(
        self,
        routing_plan: QuarantineRoutingPlan,
        source_table: str | None = None,
        flavor: str = "dp",
    ) -> str:
        """Backward-compatible wrapper for generate_pipeline_code."""
        return self.generate_pipeline_code(routing_plan, source_table=source_table, flavor=flavor)
