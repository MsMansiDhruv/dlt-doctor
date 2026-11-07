"""Delta Live Tables (DLT) and Lakeflow Declarative Pipelines expectation wrappers and single-pass quarantine routing."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import sqlglot

ALLOWED_ACTIONS = {"quarantine", "drop", "fail", "warn"}

RETENTION_PROPERTY_KEYS = {
    "delta.deletedFileRetentionDuration",
    "delta.logRetentionDuration",
    "delta.enableChangeDataFeed",
    "delta.autoOptimize.optimizeWrite",
    "delta.autoOptimize.autoCompact",
    "delta.dataSkippingNumIndexedCols",
}


def filter_retention_table_properties(props: dict[str, Any]) -> dict[str, str]:
    """Filter table properties to retain retention and lifecycle properties for quarantine table."""
    result: dict[str, str] = {}
    for k, v in props.items():
        k_str = str(k)
        k_lower = k_str.lower()
        if (
            k_str in RETENTION_PROPERTY_KEYS
            or "retention" in k_lower
            or "changedatafeed" in k_lower
        ):
            result[k_str] = str(v)
    return result


def _safe_identifier(name: str) -> str:
    """Convert dotted or special character table names into valid Python identifiers."""
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if cleaned and cleaned[0].isdigit():
        cleaned = f"t_{cleaned}"
    return cleaned or "pipeline_table"


def _escape_table_name(table_name: str) -> str:
    """Safely escape catalog, schema, and table components with backticks for Unity Catalog."""
    parts = table_name.strip().split(".")
    if len(parts) == 1 and parts[0].isidentifier():
        return parts[0]
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
    table_properties: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_clean_table": self.target_clean_table,
            "target_quarantine_table": self.target_quarantine_table,
            "valid_filter_sql": self.valid_filter_sql,
            "invalid_filter_sql": self.invalid_filter_sql,
            "failed_rules_expr_sql": self.failed_rules_expr_sql,
            "rules": [r.to_dict() for r in self.rules],
            "source_table": self.source_table,
            "table_properties": self.table_properties,
        }

    def generate_pipeline_code(self, source_table: str | None = None, flavor: str = "dp") -> str:
        """Convenience method to synthesize pipeline code directly from routing plan."""
        return ExpectationDoctor().generate_pipeline_code(
            self, source_table=source_table, flavor=flavor
        )


def generate_healed_pipeline_nodes(
    clean_func_name: str,
    clean_table_name: str,
    quarantine_table_name: str,
    source_table_or_view: str,
    rules: list[ExpectationRule],
    flavor: str = "dp",
    is_streaming: bool = True,
    clean_table_decorator_text: str | None = None,
    clean_extra_decorators: list[str] | None = None,
    table_properties: dict[str, Any] | None = None,
    quarantine_func_name: str | None = None,
    staging_func_name: str | None = None,
    staging_view_name: str | None = None,
    clean_comment: str = "Clean records passing all expectation contracts.",
    quarantine_comment: str = "Quarantined records failing business expectations for triage.",
) -> tuple[str, str, str]:
    """Generate (staged_func_code, clean_func_code, quarantine_func_code) code blocks.

    Shared by both heal.py and ExpectationDoctor.generate_pipeline_code to ensure uniform
    pipeline contracts, event-log metric retention, and quarantine table properties.
    """
    pkg = "dp" if flavor == "dp" else "dlt"
    escaped_src = _escape_table_name(source_table_or_view)

    staging_name = staging_view_name or f"{clean_table_name.split('.')[-1]}_staged"
    escaped_staged = _escape_table_name(staging_name)
    staging_func = staging_func_name or f"{clean_func_name}_staged"

    # View decorator: dp uses @dp.temporary_view, dlt uses @dlt.view
    if flavor == "dp":
        view_dec = f'@dp.temporary_view(name="{staging_name}")'
    else:
        view_dec = f'@dlt.view(name="{staging_name}")'

    # Filter rules by action
    quarantine_rules = [r for r in rules if r.action_on_failure in {"quarantine", "drop"}]
    warn_rules = [r for r in rules if r.action_on_failure == "warn"]
    fail_rules = [r for r in rules if r.action_on_failure == "fail"]

    # Retain metrics in event log:
    # Staged view is read by both clean and quarantine tables. Per Databricks documentation,
    # views queried by multiple downstream datasets can report multiple sets of metrics or
    # no metrics. Therefore, retain-only @expect_all is placed on the clean table and on the
    # quarantine table rather than the shared staged view.
    # Per-rule totals are: clean.passed + quarantine.passed + quarantine.failed, with no double counting.
    expect_all_dec = ""
    if quarantine_rules:
        entries: list[str] = []
        for r in quarantine_rules:
            r_esc = r.rule_name.replace('"', '\\"')
            c_esc = r.condition_sql.replace("\\", "\\\\").replace('"', '\\"')
            entries.append(f'        "{r_esc}": "{c_esc}",')
        dict_body = "{\n" + "\n".join(entries) + "\n    }"
        expect_all_dec = f"@{pkg}.expect_all({dict_body})"

    staged_decs = view_dec

    # Readers
    if flavor == "dp":
        read_src_call = (
            f'spark.readStream.table("{escaped_src}")'
            if is_streaming
            else f'spark.read.table("{escaped_src}")'
        )
        read_staged_call = (
            f'spark.readStream.table("{escaped_staged}")'
            if is_streaming
            else f'spark.read.table("{escaped_staged}")'
        )
    else:
        read_src_call = (
            f'dlt.read_stream("{escaped_src}")' if is_streaming else f'dlt.read("{escaped_src}")'
        )
        read_staged_call = (
            f'dlt.read_stream("{escaped_staged}")'
            if is_streaming
            else f'dlt.read("{escaped_staged}")'
        )

    # Staging select expressions
    failed_items: list[str] = []
    for r in quarantine_rules:
        cond_escaped = r.condition_sql.replace("\\", "\\\\").replace('"', '\\"')
        r_escaped = r.rule_name.replace("\\", "\\\\").replace('"', '\\"')
        failed_items.append(
            f'                F.when(~F.expr("coalesce(({cond_escaped}), false)"), F.lit("{r_escaped}")),'
        )

    if failed_items:
        failed_rules_block = "\n".join(failed_items)
        failed_rules_expr = (
            f"F.array_compact(\n"
            f"                F.array(\n"
            f"{failed_rules_block}\n"
            f"                )\n"
            f'            ).alias("_failed_rules")'
        )
    else:
        failed_rules_expr = 'F.array_compact(F.array()).alias("_failed_rules")'

    staged_func_code = (
        f"{staged_decs}\n"
        f"def {staging_func}():\n"
        f"    return (\n"
        f"        {read_src_call}\n"
        f"        .select(\n"
        f'            "*",\n'
        f"            {failed_rules_expr},\n"
        f'            F.current_timestamp().alias("_quarantined_at"),\n'
        f"        )\n"
        f"    )"
    )

    # Clean table decorators
    clean_decs: list[str] = []
    if clean_table_decorator_text:
        clean_decs.append(clean_table_decorator_text)
    else:
        clean_decs.append(
            f'@{pkg}.table(\n    name="{clean_table_name}",\n    comment="{clean_comment}",\n)'
        )

    if expect_all_dec:
        clean_decs.append(expect_all_dec)

    for wr in warn_rules:
        wr_esc = wr.rule_name.replace('"', '\\"')
        wc_esc = wr.condition_sql.replace("\\", "\\\\").replace('"', '\\"')
        clean_decs.append(f'@{pkg}.expect("{wr_esc}", "{wc_esc}")')

    for fr in fail_rules:
        fr_esc = fr.rule_name.replace('"', '\\"')
        fc_esc = fr.condition_sql.replace("\\", "\\\\").replace('"', '\\"')
        clean_decs.append(f'@{pkg}.expect_or_fail("{fr_esc}", "{fc_esc}")')

    if clean_extra_decorators:
        clean_decs.extend(clean_extra_decorators)

    clean_decs_str = "\n".join(clean_decs)

    clean_func_code = (
        f"{clean_decs_str}\n"
        f"def {clean_func_name}():\n"
        f"    return (\n"
        f"        {read_staged_call}\n"
        f'        .filter(F.size(F.col("_failed_rules")) == 0)\n'
        f'        .drop("_failed_rules", "_quarantined_at")\n'
        f"    )"
    )

    # Quarantine table
    quarantine_func = quarantine_func_name or f"{clean_func_name}_quarantine"
    retention_props = filter_retention_table_properties(table_properties or {})
    quar_decs: list[str] = []

    if retention_props:
        props_lines = [f'        "{k}": "{v}",' for k, v in sorted(retention_props.items())]
        props_dict_str = "{\n" + "\n".join(props_lines) + "\n    }"
        quar_decs.append(
            f"@{pkg}.table(\n"
            f'    name="{quarantine_table_name}",\n'
            f'    comment="{quarantine_comment}",\n'
            f"    table_properties={props_dict_str},\n"
            f")"
        )
    else:
        quar_decs.append(
            f"@{pkg}.table(\n"
            f'    name="{quarantine_table_name}",\n'
            f'    comment="{quarantine_comment}",\n'
            f")"
        )

    if expect_all_dec:
        quar_decs.append(expect_all_dec)

    quarantine_decs_str = "\n".join(quar_decs)

    quarantine_func_code = (
        f"{quarantine_decs_str}\n"
        f"def {quarantine_func}():\n"
        f"    return (\n"
        f"        {read_staged_call}\n"
        f'        .filter(F.size(F.col("_failed_rules")) > 0)\n'
        f"    )"
    )

    return (staged_func_code, clean_func_code, quarantine_func_code)


class ExpectationDoctor:
    """Manages resilient quality gates without silent data drops or pipeline halts.

    Event Log Metrics & Data Quality Auditing:
    Retain-only expectations (`@expect_all`) are placed on both the clean table and the
    quarantine table rather than the shared intermediate staging view. In Databricks Delta
    Live Tables and Lakeflow Declarative Pipelines, intermediate views read by multiple downstream
    datasets may produce duplicated metrics or omit metrics because views are evaluated lazily.

    With dual-dataset metrics:
    - `clean.passed`: rows successfully written to clean table (all quarantine rules satisfied).
    - `clean.failed`: 0 (clean table drops all failed rows).
    - `quarantine.passed`: rows in quarantine that satisfied a particular rule (failed on another).
    - `quarantine.failed`: rows in quarantine that failed that specific rule.
    - Total per-rule evaluated rows: `clean.passed + quarantine.passed + quarantine.failed` with
      no double-counting.

    To query expectation metrics from the Databricks Event Log:
    ```sql
    SELECT
        timestamp,
        message,
        details:flow_progress.metrics.expectations AS expectations_metrics,
        details:flow_progress.data_quality.expectations AS dq_expectations
    FROM event_log(TABLE(<pipeline_target_table>))
    WHERE event_type = 'flow_progress'
    ORDER BY timestamp DESC;
    ```
    """

    def __init__(self) -> None:
        pass

    def build_quarantine_routing(
        self,
        table_name: str,
        rules: list[ExpectationRule],
        source_table: str | None = None,
        quarantine_table: str | None = None,
        table_properties: dict[str, Any] | None = None,
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
                table_properties=table_properties or {},
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
            table_properties=table_properties or {},
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
        leaf_clean = routing_plan.target_clean_table.split(".")[-1]
        staging_view_name = f"{leaf_clean}_staged"
        staging_func = f"{clean_func}_staged"

        staged_code, clean_code, quarantine_code = generate_healed_pipeline_nodes(
            clean_func_name=clean_func,
            clean_table_name=routing_plan.target_clean_table,
            quarantine_table_name=routing_plan.target_quarantine_table,
            source_table_or_view=resolved_source,
            rules=routing_plan.rules,
            flavor=flavor,
            is_streaming=True,
            table_properties=routing_plan.table_properties,
            quarantine_func_name=quarantine_func,
            staging_func_name=staging_func,
            staging_view_name=staging_view_name,
        )

        if flavor == "dp":
            import_line = (
                "from pyspark import pipelines as dp\nfrom pyspark.sql import functions as F"
            )
        elif flavor == "dlt":
            import_line = "import dlt\nfrom pyspark.sql import functions as F"
        else:
            raise ValueError(f"Unknown pipeline flavor '{flavor}'. Choose 'dp' or 'dlt'.")

        return (
            "# Auto-generated single-pass validation and quarantine pipeline by dlt-doctor\n"
            f"{import_line}\n\n"
            f"{staged_code}\n\n"
            f"# Clean target table: {routing_plan.target_clean_table}\n"
            f"{clean_code}\n\n"
            f"# Dead-letter quarantine table: {routing_plan.target_quarantine_table}\n"
            f"{quarantine_code}\n"
        )

    def generate_dlt_decorator_code(
        self,
        routing_plan: QuarantineRoutingPlan,
        source_table: str | None = None,
        flavor: str = "dp",
    ) -> str:
        """Backward-compatible wrapper for generate_pipeline_code."""
        return self.generate_pipeline_code(routing_plan, source_table=source_table, flavor=flavor)
