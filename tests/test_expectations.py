"""Tests for ExpectationDoctor DLT quarantine routing."""

import ast

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
        {"amount": 100, "status": "A"},    # passes
        {"amount": None, "status": "A"},   # NULL amount -> fails
        {"amount": -5, "status": "A"},     # amount <= 0 -> fails
        {"amount": 50, "status": None},    # NULL status -> fails
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


@pytest.mark.integration
def test_pyspark_integration_quarantine_partition() -> None:
    """Verify NULL-safe partition property using a local PySpark session if available."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("PySpark is not installed; skipping integration test.")

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("dlt-doctor-test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
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
    finally:
        spark.stop()
