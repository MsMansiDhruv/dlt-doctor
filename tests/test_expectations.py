"""Tests for ExpectationDoctor DLT quarantine routing."""

import ast
import sys
import types
from typing import Any

import pytest

from dlt_doctor.expectations import ExpectationDoctor, ExpectationRule


def test_build_quarantine_routing_null_safe() -> None:
    doctor = ExpectationDoctor()
    rules = [
        ExpectationRule("valid_id", "order_id IS NOT NULL"),
        ExpectationRule("positive_amount", "amount > 0"),
    ]
    plan = doctor.build_quarantine_routing("silver_orders", rules)

    assert plan.target_clean_table == "silver_orders"
    assert plan.target_quarantine_table == "silver_orders_quarantine"
    assert "coalesce((order_id IS NOT NULL), false)" in plan.valid_filter_sql
    assert "coalesce((amount > 0), false)" in plan.valid_filter_sql
    assert "NOT (" in plan.invalid_filter_sql
    assert "array_compact(array(" in plan.failed_rules_expr_sql
    assert "'valid_id'" in plan.failed_rules_expr_sql
    assert "'positive_amount'" in plan.failed_rules_expr_sql


def test_three_valued_logic_completeness() -> None:
    """Pure-Python property verification of three-valued-logic:
    valid union quarantine = all_records, and valid intersect quarantine is empty.
    """
    records = [
        {"amount": 100, "status": "A"},  # passes
        {"amount": None, "status": "A"},  # NULL amount -> fails
        {"amount": -5, "status": "A"},  # amount <= 0 -> fails
        {"amount": 50, "status": None},  # NULL status -> fails
        {"amount": None, "status": None},  # both NULL -> fails
    ]

    def eval_rule(rule_expr: str, rec: dict) -> bool | None:
        amount = rec["amount"]
        status = rec["status"]
        if rule_expr == "amount > 0":
            return None if amount is None else (amount > 0)
        if rule_expr == "status IS NOT NULL":
            return status is not None
        raise ValueError(f"Unknown rule {rule_expr}")

    def coalesce_cond(val: bool | None) -> bool:
        return False if val is None else bool(val)

    rules = ["amount > 0", "status IS NOT NULL"]

    valid_records = []
    quarantine_records = []

    for r in records:
        # valid: all coalesce((cond), false) must be True
        passes_all = all(coalesce_cond(eval_rule(rule, r)) for rule in rules)
        # invalid: NOT (valid)
        is_invalid = not passes_all

        if passes_all:
            valid_records.append(r)
        if is_invalid:
            quarantine_records.append(r)

    # Disjoint check: valid ∩ quarantine = empty
    assert len(valid_records) + len(quarantine_records) == len(records)
    for vr in valid_records:
        assert vr not in quarantine_records
    assert len(valid_records) == 1
    assert len(quarantine_records) == 4


def test_condition_validation_rejects_invalid_and_multistatement() -> None:
    # Invalid SQL syntax
    with pytest.raises(ValueError, match="Failed to parse expectation condition"):
        ExpectationRule("bad_syntax", "amount >>> 0")

    # Multi-statement input
    with pytest.raises(ValueError, match="single expression"):
        ExpectationRule("multi_stmt", "SELECT 1; DROP TABLE orders;")


def test_action_on_failure_validation() -> None:
    # Valid actions
    for action in ["quarantine", "drop", "fail", "warn"]:
        rule = ExpectationRule("r1", "x > 0", action_on_failure=action)
        assert rule.action_on_failure == action

    # Invalid action
    with pytest.raises(ValueError, match="Invalid action_on_failure"):
        ExpectationRule("r1", "x > 0", action_on_failure="ignore_everything")


def test_generate_pipeline_code_compiles_ast() -> None:
    doctor = ExpectationDoctor()
    rules = [
        ExpectationRule("valid_id", "order_id IS NOT NULL"),
        ExpectationRule("valid_amount", "amount > 0"),
    ]
    plan = doctor.build_quarantine_routing("main.silver.orders", rules)

    # Test "dp" flavor
    dp_code = doctor.generate_pipeline_code(plan, source_table="bronze_orders", flavor="dp")
    parsed_dp = ast.parse(dp_code)
    assert parsed_dp is not None
    assert "from pyspark import pipelines as dp" in dp_code
    assert "@dp.table(" in dp_code
    assert "def main_silver_orders():" in dp_code
    assert "def main_silver_orders_quarantine():" in dp_code

    # Test "dlt" flavor
    dlt_code = doctor.generate_pipeline_code(plan, source_table="bronze_orders", flavor="dlt")
    parsed_dlt = ast.parse(dlt_code)
    assert parsed_dlt is not None
    assert "import dlt" in dlt_code
    assert "@dlt.table(" in dlt_code


def test_dp_flavor_uses_spark_readstream_table_not_dp_read_stream() -> None:
    """dp.read_stream does not exist in pyspark.pipelines; dp flavor must use spark.readStream.table."""
    doctor = ExpectationDoctor()
    rules = [ExpectationRule("valid_id", "id IS NOT NULL")]
    plan = doctor.build_quarantine_routing("silver_orders", rules)
    code = doctor.generate_pipeline_code(plan, source_table="bronze_orders", flavor="dp")

    assert "dp.read_stream" not in code, "dp.read_stream does not exist in pyspark.pipelines"
    assert "spark.readStream.table(" in code


def test_honors_action_on_failure_for_quarantine_routing() -> None:
    """Quarantine routing must only route rules with action_on_failure in {'quarantine', 'drop'}, ignoring 'warn'."""
    doctor = ExpectationDoctor()
    rules = [
        ExpectationRule("quarantine_rule", "amount > 0", action_on_failure="quarantine"),
        ExpectationRule("warn_rule", "qty > 0", action_on_failure="warn"),
    ]
    plan = doctor.build_quarantine_routing("silver_orders", rules)

    assert "amount > 0" in plan.valid_filter_sql
    assert "quarantine_rule" in plan.failed_rules_expr_sql
    # Warning rule should not filter or route to quarantine
    assert "qty > 0" not in plan.valid_filter_sql
    assert "warn_rule" not in plan.failed_rules_expr_sql


def test_escapes_table_names_in_generated_code() -> None:
    """Generated code must escape table names with backticks for Unity Catalog compatibility."""
    doctor = ExpectationDoctor()
    rules = [ExpectationRule("valid_id", "id IS NOT NULL")]
    plan = doctor.build_quarantine_routing("main.silver.orders", rules)
    code = doctor.generate_pipeline_code(plan, source_table="main.bronze.orders", flavor="dp")

    assert "`main`.`bronze`.`orders`" in code or "`main.bronze.orders`" in code


def test_quarantine_routing_custom_source_and_quarantine_tables() -> None:
    """Verify build_quarantine_routing supports custom source_table and quarantine_table."""
    doctor = ExpectationDoctor()
    rules = [ExpectationRule("valid_id", "id IS NOT NULL")]
    plan = doctor.build_quarantine_routing(
        table_name="silver_orders",
        rules=rules,
        source_table="raw_bronze_stream",
        quarantine_table="silver_orders_dlq",
    )
    assert plan.target_clean_table == "silver_orders"
    assert plan.target_quarantine_table == "silver_orders_dlq"
    assert plan.source_table == "raw_bronze_stream"

    code = plan.generate_pipeline_code()
    assert "raw_bronze_stream" in code
    assert "silver_orders_dlq" in code
    assert "source_bronze" not in code


def test_generate_pipeline_code_requires_source_table() -> None:
    """When source_table is omitted both in routing_plan and generate_pipeline_code, raise ValueError."""
    doctor = ExpectationDoctor()
    rules = [ExpectationRule("valid_id", "id IS NOT NULL")]
    plan = doctor.build_quarantine_routing("my_clean_table", rules)
    with pytest.raises(ValueError, match="source_table must be provided"):
        plan.generate_pipeline_code()


def test_quarantine_routing_plan_to_dict_includes_source_table() -> None:
    doctor = ExpectationDoctor()
    rules = [ExpectationRule("valid_id", "id IS NOT NULL")]
    plan = doctor.build_quarantine_routing(
        "tbl", rules, source_table="src_tbl", quarantine_table="tbl_quarantine"
    )
    d = plan.to_dict()
    assert d["source_table"] == "src_tbl"
    assert d["target_quarantine_table"] == "tbl_quarantine"


def test_staged_view_retains_expect_all_for_event_log_metrics() -> None:
    """Staged view must retain @dlt.expect_all (or @dp.expect_all) to preserve event-log metrics."""
    doctor = ExpectationDoctor()
    rules = [
        ExpectationRule("valid_id", "id IS NOT NULL", action_on_failure="quarantine"),
        ExpectationRule("positive_amt", "amount > 0", action_on_failure="drop"),
    ]
    plan = doctor.build_quarantine_routing("orders", rules)
    code = doctor.generate_pipeline_code(plan, source_table="bronze", flavor="dlt")

    assert "@dlt.expect_all({" in code
    assert '"valid_id": "id IS NOT NULL"' in code
    assert '"positive_amt": "amount > 0"' in code


def test_clean_table_emits_warn_and_fail_expectations() -> None:
    """Clean table must emit @expect for warn rules and @expect_or_fail for fail rules."""
    doctor = ExpectationDoctor()
    rules = [
        ExpectationRule("check_warn", "discount < 0.5", action_on_failure="warn"),
        ExpectationRule("check_fail", "total > 0", action_on_failure="fail"),
        ExpectationRule("check_quarantine", "id IS NOT NULL", action_on_failure="quarantine"),
    ]
    plan = doctor.build_quarantine_routing("orders", rules)
    code = doctor.generate_pipeline_code(plan, source_table="bronze", flavor="dlt")

    assert '@dlt.expect("check_warn", "discount < 0.5")' in code
    assert '@dlt.expect_or_fail("check_fail", "total > 0")' in code
    # Quarantine rule must be in expect_all on staged view, not standalone on clean table
    assert '@dlt.expect_or_fail("check_quarantine"' not in code


def test_quarantine_table_copies_retention_table_properties() -> None:
    """Quarantine table decorator must copy retention-related table properties while discarding unrelated ones."""
    doctor = ExpectationDoctor()
    rules = [ExpectationRule("valid_id", "id IS NOT NULL")]
    table_props = {
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.enableChangeDataFeed": "true",
        "delta.logRetentionDuration": "interval 60 days",
        "custom_team": "analytics",
    }
    plan = doctor.build_quarantine_routing(
        "orders", rules, source_table="bronze", table_properties=table_props
    )
    code = doctor.generate_pipeline_code(plan, flavor="dlt")

    assert '"delta.deletedFileRetentionDuration": "interval 30 days"' in code
    assert '"delta.enableChangeDataFeed": "true"' in code
    assert '"delta.logRetentionDuration": "interval 60 days"' in code
    assert "custom_team" not in code


def test_unified_generator_dp_uses_temporary_view_and_dlt_uses_view() -> None:
    """Lakeflow (dp) must use @dp.temporary_view, whereas DLT must use @dlt.view."""
    doctor = ExpectationDoctor()
    rules = [ExpectationRule("valid_id", "id IS NOT NULL")]
    plan = doctor.build_quarantine_routing("orders", rules)

    dp_code = doctor.generate_pipeline_code(plan, source_table="bronze", flavor="dp")
    assert '@dp.temporary_view(name="orders_staged")' in dp_code
    assert "@dp.view" not in dp_code

    dlt_code = doctor.generate_pipeline_code(plan, source_table="bronze", flavor="dlt")
    assert '@dlt.view(name="orders_staged")' in dlt_code


@pytest.mark.integration
def test_pyspark_integration_quarantine_partition(spark: Any) -> None:
    """Verify NULL-safe partition property using the shared SparkSession."""
    data = [(1, 100), (2, None), (3, -10), (None, 50)]
    df = spark.createDataFrame(data, ["id", "amount"])

    doctor = ExpectationDoctor()
    rules = [
        ExpectationRule("valid_id", "id IS NOT NULL"),
        ExpectationRule("valid_amount", "amount > 0"),
    ]
    plan = doctor.build_quarantine_routing("test_table", rules)

    clean_df = df.filter(plan.valid_filter_sql)
    quarantine_df = df.filter(plan.invalid_filter_sql)

    total_rows = df.count()
    clean_rows = clean_df.count()
    quarantine_rows = quarantine_df.count()

    assert clean_rows + quarantine_rows == total_rows
    assert clean_rows == 1  # only (1, 100)
    assert quarantine_rows == 3


class ProxyReader:
    def __init__(self, tables: dict[str, Any]) -> None:
        self.tables = tables

    def table(self, name: str) -> Any:
        clean_name = name.strip("`")
        return self.tables[clean_name]


class ProxySpark:
    def __init__(self, tables: dict[str, Any]) -> None:
        self.tables = tables
        self.read = ProxyReader(tables)
        self.readStream = ProxyReader(tables)


class StubDP:
    def __init__(self, tables: dict[str, Any]) -> None:
        self.tables = tables

    def _decorator(self, *args: Any, **kwargs: Any) -> Any:
        if len(args) == 1 and callable(args[0]) and not kwargs:
            return args[0]

        def inner(fn: Any) -> Any:
            return fn

        return inner

    def __getattr__(self, name: str) -> Any:
        if name in {
            "table",
            "view",
            "temporary_view",
            "materialized_view",
            "expect",
            "expect_all",
            "expect_or_fail",
            "expect_all_or_fail",
        }:
            return self._decorator
        raise AttributeError(f"StubDP has no attribute {name}")


@pytest.mark.integration
def test_pyspark_integration_dp_flavor_pipeline_execution(
    spark: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Execute synthesized dp-flavor pipeline on real Spark DataFrames via proxy spark and stub dp."""
    data = [(1, 100), (2, None), (3, -10), (None, 50)]
    source_df = spark.createDataFrame(data, ["id", "amount"])

    doctor = ExpectationDoctor()
    rules = [
        ExpectationRule("valid_id", "id IS NOT NULL", action_on_failure="quarantine"),
        ExpectationRule("positive_amount", "amount > 0", action_on_failure="drop"),
        ExpectationRule("warn_small", "amount < 1000", action_on_failure="warn"),
    ]
    plan = doctor.build_quarantine_routing("silver_orders", rules)
    pipeline_code = doctor.generate_pipeline_code(plan, source_table="bronze_orders", flavor="dp")

    # Assert decorator order: dataset decorator is above expectation decorator
    assert '@dp.temporary_view(name="silver_orders_staged")\n@dp.expect_all({' in pipeline_code
    assert '@dp.expect("warn_small"' in pipeline_code

    # Execute with proxy spark and stub dp
    tables: dict[str, Any] = {"bronze_orders": source_df}
    proxy_spark = ProxySpark(tables)
    stub_dp = StubDP(tables)

    fake_dp_module = types.ModuleType("pyspark.pipelines")
    for attr in [
        "temporary_view",
        "table",
        "view",
        "expect_all",
        "expect",
        "expect_or_fail",
        "expect_all_or_fail",
    ]:
        setattr(fake_dp_module, attr, getattr(stub_dp, attr))

    monkeypatch.setitem(sys.modules, "pyspark.pipelines", fake_dp_module)

    from pyspark.sql import functions as F

    exec_env = {
        "dp": stub_dp,
        "spark": proxy_spark,
        "F": F,
    }
    exec(pipeline_code, exec_env)

    # Invoke staged, clean, and quarantine functions
    staged_fn = exec_env["silver_orders_staged"]
    clean_fn = exec_env["silver_orders"]
    quarantine_fn = exec_env["silver_orders_quarantine"]

    staged_df = staged_fn()
    tables["silver_orders_staged"] = staged_df

    clean_df = clean_fn()
    quarantine_df = quarantine_fn()

    clean_rows = clean_df.collect()
    quarantine_rows = quarantine_df.collect()

    assert len(clean_rows) + len(quarantine_rows) == 4
    assert [(r.id, r.amount) for r in clean_rows] == [(1, 100)]
    assert len(quarantine_rows) == 3
