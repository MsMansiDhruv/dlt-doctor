"""Tests for dlt-doctor auto-heal engine (WP3)."""

import ast
import tempfile
from pathlib import Path
from typing import Any

import pytest

from dlt_doctor.heal import auto_heal_source, heal_file


def test_auto_heal_single_pass_diff_generation() -> None:
    input_code = """import dlt

@dlt.table(name="silver_orders", comment="Curated orders")
@dlt.expect_or_drop("valid_amt", "amount > 0")
def silver_orders():
    return dlt.read_stream("bronze_orders")
"""
    result = auto_heal_source(input_code, file_path="test_orders.py")
    assert result.can_heal is True
    assert result.diff != ""
    assert "--- a/test_orders.py" in result.diff
    assert "+++ b/test_orders.py" in result.diff
    assert "_failed_rules" in result.healed_code
    assert "silver_orders_staged" in result.healed_code
    assert "silver_orders_quarantine" in result.healed_code

    # Verify healed code compiles cleanly
    parsed = ast.parse(result.healed_code)
    assert parsed is not None


def test_auto_heal_idempotence() -> None:
    input_code = """import dlt

@dlt.table(name="silver_orders")
@dlt.expect_or_drop("valid_amt", "amount > 0")
def silver_orders():
    return dlt.read_stream("bronze_orders")
"""
    res1 = auto_heal_source(input_code)
    assert res1.diff != ""

    # Second run on already healed code produces empty diff
    res2 = auto_heal_source(res1.healed_code)
    assert res2.diff == ""
    assert res2.healed_code == res1.healed_code


def test_auto_heal_multi_statement_body_healed() -> None:
    input_code = """import dlt

@dlt.table
@dlt.expect_or_drop("valid_amt", "amount > 0")
def complex_pipeline():
    df = dlt.read_stream("bronze_orders")
    df = df.filter("active = true")
    return df
"""
    result = auto_heal_source(input_code)
    assert result.can_heal is True
    assert "def complex_pipeline_raw():" in result.healed_code
    assert 'df = df.filter("active = true")' in result.healed_code
    assert "def complex_pipeline():" in result.healed_code
    assert "def complex_pipeline_quarantine():" in result.healed_code


def test_auto_heal_refuses_dynamic_expectations() -> None:
    input_code = """import dlt

@dlt.table
@dlt.expect_all_or_drop(fetch_remote_rules())
def dynamic_pipeline():
    return dlt.read_stream("bronze_orders")
"""
    result = auto_heal_source(input_code)
    assert result.can_heal is False
    assert "dynamic expectation variables cannot be safely resolved" in result.reason


def test_heal_file_write_mode() -> None:
    input_code = """from pyspark import pipelines as dp

@dp.table(name="silver_txns")
@dp.expect_or_drop("valid_id", "id IS NOT NULL")
def silver_txns():
    return dp.read_stream("bronze_txns")
"""
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False, encoding="utf-8") as f:
        f.write(input_code)
        temp_path = f.name

    try:
        # Dry-run does not write
        dry_run = heal_file(temp_path, write=False)
        assert dry_run.diff != ""
        assert Path(temp_path).read_text(encoding="utf-8") == input_code

        # Write mode applies changes
        written = heal_file(temp_path, write=True)
        assert written.diff != ""
        healed_text = Path(temp_path).read_text(encoding="utf-8")
        assert "_failed_rules" in healed_text
        assert "@dp.expect_or_drop" not in healed_text

        # Idempotence check on file
        second_run = heal_file(temp_path, write=True)
        assert second_run.diff == ""
    finally:
        Path(temp_path).unlink(missing_ok=True)


def test_auto_heal_preserves_multistatement_body() -> None:
    input_code = """import dlt

@dlt.table(name="silver_orders", comment="Curated orders", table_properties={"delta.enableChangeDataFeed": "true"})
@dlt.expect("non_negative_qty", "qty >= 0")
@dlt.expect_or_drop("valid_amt", "amount > 0")
@dlt.expect_or_fail("valid_id", "id IS NOT NULL")
def silver_orders():
    df = dlt.read_stream("bronze_orders")
    df = df.filter("active = true")
    return df
"""
    result = auto_heal_source(input_code)
    assert result.can_heal is True
    assert 'df = dlt.read_stream("bronze_orders")' in result.healed_code
    assert 'df = df.filter("active = true")' in result.healed_code
    assert '@dlt.expect("non_negative_qty", "qty >= 0")' in result.healed_code
    assert '@dlt.expect_or_fail("valid_id", "id IS NOT NULL")' in result.healed_code
    assert "expect_or_drop" not in result.healed_code
    assert 'table_properties={"delta.enableChangeDataFeed": "true"}' in result.healed_code
    assert 'comment="Curated orders"' in result.healed_code


def test_auto_heal_refuses_materialized_view_reading_dlt_read() -> None:
    input_code = """from pyspark import pipelines as dp

@dp.materialized_view(name="mv_summary")
@dp.expect_or_drop("valid_sum", "total > 0")
def mv_summary():
    return dlt.read("silver_orders")
"""
    result = auto_heal_source(input_code)
    assert result.can_heal is False
    assert "materialized view" in result.reason.lower()


def test_auto_heal_preserves_batch_table() -> None:
    input_code = """import dlt

@dlt.table(name="batch_snapshot")
@dlt.expect_or_drop("valid_amt", "amount > 0")
def batch_snapshot():
    return dlt.read("bronze_batch")
"""
    result = auto_heal_source(input_code)
    assert result.can_heal is True
    assert "dlt.read(" in result.healed_code
    assert "dlt.read_stream" not in result.healed_code


def test_auto_heal_hoists_imports_once() -> None:
    input_code = """import dlt

@dlt.table(name="t1")
@dlt.expect_or_drop("v1", "x > 0")
def t1():
    return dlt.read_stream("s1")

@dlt.table(name="t2")
@dlt.expect_or_drop("v2", "y > 0")
def t2():
    return dlt.read_stream("s2")
"""
    result = auto_heal_source(input_code)
    assert result.can_heal is True
    assert result.healed_code.count("from pyspark.sql import functions as F") == 1


class StubDLT:
    """Stub module simulating dlt / dp execution runtime for testing synthesized pipelines."""

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
            "streaming_table",
            "expect",
            "expect_or_drop",
            "expect_or_fail",
            "expect_all",
            "expect_all_or_drop",
            "expect_all_or_fail",
        }:
            return self._decorator
        raise AttributeError(f"StubDLT has no attribute {name}")

    def read(self, name: str) -> Any:
        return self.tables[name]

    def read_stream(self, name: str) -> Any:
        return self.tables[name]


def _execute_healed_pipeline_on_spark(
    spark: Any,
    healed_code: str,
    source_table: str,
    source_df: Any,
    func_name: str,
    table_name: str | None = None,
) -> tuple[Any, Any]:
    """Execute generated raw, staged, clean, and quarantine functions against real PySpark DataFrames."""
    import sys

    tbl = table_name or func_name
    tables: dict[str, Any] = {source_table: source_df}
    stub = StubDLT(tables)
    sys.modules["dlt"] = stub  # type: ignore[assignment]

    env: dict[str, Any] = {"spark": spark, "dlt": stub}
    exec(healed_code, env)  # nosec B102

    # 1. Execute raw view
    raw_fn = env[f"{func_name}_raw"]
    df_raw = raw_fn()
    tables[f"{tbl}_raw"] = df_raw
    df_raw.createOrReplaceTempView(f"{tbl}_raw")

    # 2. Execute staged view
    staged_fn = env[f"{func_name}_staged"]
    df_staged = staged_fn()
    tables[f"{tbl}_staged"] = df_staged
    df_staged.createOrReplaceTempView(f"{tbl}_staged")

    # 3. Execute clean table
    clean_fn = env[func_name]
    df_clean = clean_fn()

    # 4. Execute quarantine table
    quar_fn = env[f"{func_name}_quarantine"]
    df_quarantine = quar_fn()

    return df_clean, df_quarantine


@pytest.mark.integration
def test_auto_heal_clean_and_quarantine_partition_on_nulls(spark: Any) -> None:
    """Execute healed code on Spark with NULL-heavy data:
    assert clean == original table with drop rule applied, and clean U quarantine == pre-expectation rows.
    """
    input_code = """import dlt

@dlt.table(name="curated_orders")
@dlt.expect_or_drop("valid_amt", "amount > 0")
def curated_orders():
    return dlt.read_stream("bronze_orders")
"""
    result = auto_heal_source(input_code)
    assert result.can_heal is True

    raw_data = [(1, 100), (2, None), (3, -10), (4, 0), (5, 50)]
    df_bronze = spark.createDataFrame(raw_data, ["id", "amount"])

    df_clean, df_quarantine = _execute_healed_pipeline_on_spark(
        spark=spark,
        healed_code=result.healed_code,
        source_table="bronze_orders",
        source_df=df_bronze,
        func_name="curated_orders",
        table_name="curated_orders",
    )

    clean_rows = [(r.id, r.amount) for r in df_clean.collect()]
    quarantine_rows = [(r.id, r.amount) for r in df_quarantine.collect()]

    # Clean matches original logic with drop rule applied
    expected_clean_df = df_bronze.filter("amount IS NOT NULL AND amount > 0")
    expected_clean_rows = [(r.id, r.amount) for r in expected_clean_df.collect()]
    assert set(clean_rows) == set(expected_clean_rows)

    # Disjoint and complete
    clean_ids = {r[0] for r in clean_rows}
    quarantine_ids = {r[0] for r in quarantine_rows}
    assert clean_ids == {1, 5}
    assert quarantine_ids == {2, 3, 4}
    assert clean_ids.isdisjoint(quarantine_ids)
    assert clean_ids.union(quarantine_ids) == {1, 2, 3, 4, 5}


@pytest.mark.integration
def test_pyspark_integration_healed_pipeline_execution(spark: Any) -> None:
    """Execute healed multi-statement body code on local PySpark session:
    assert clean == original function output with drop rule applied, and clean U quarantine == pre-expectation rows.
    """
    input_code = """import dlt

@dlt.table(name="filtered_orders")
@dlt.expect_or_drop("valid_amt", "amount > 0")
def filtered_orders():
    df = dlt.read_stream("raw_orders")
    df = df.filter("active = true")
    return df
"""
    result = auto_heal_source(input_code)
    assert result.can_heal is True

    data = [(1, 100, True), (2, None, True), (3, -10, True), (4, 200, False), (5, 50, True)]
    df_raw = spark.createDataFrame(data, ["id", "amount", "active"])

    df_clean, df_quarantine = _execute_healed_pipeline_on_spark(
        spark=spark,
        healed_code=result.healed_code,
        source_table="raw_orders",
        source_df=df_raw,
        func_name="filtered_orders",
        table_name="filtered_orders",
    )

    clean_rows = [(r.id, r.amount, r.active) for r in df_clean.collect()]
    quarantine_rows = [(r.id, r.amount, r.active) for r in df_quarantine.collect()]

    # Expected pre-expectation rows are rows matching active = true
    expected_pre_expectation_df = df_raw.filter("active = true")
    expected_clean_df = expected_pre_expectation_df.filter("amount IS NOT NULL AND amount > 0")
    expected_clean_rows = [(r.id, r.amount, r.active) for r in expected_clean_df.collect()]

    assert set(clean_rows) == set(expected_clean_rows)
    clean_ids = {r[0] for r in clean_rows}
    quarantine_ids = {r[0] for r in quarantine_rows}
    assert clean_ids == {1, 5}
    assert quarantine_ids == {2, 3}
    assert clean_ids.isdisjoint(quarantine_ids)
    assert clean_ids.union(quarantine_ids) == {1, 2, 3, 5}


def test_auto_heal_custom_quarantine_suffix() -> None:
    code = """import dlt
@dlt.table(name="events")
@dlt.expect_or_drop("v", "event_id IS NOT NULL")
def events():
    return dlt.read_stream("bronze")
"""
    result = auto_heal_source(code, quarantine_suffix="_quarantine_dlq")
    assert result.can_heal is True
    assert "events_quarantine_dlq" in result.healed_code


def test_auto_heal_validates_quarantine_suffix_pattern() -> None:
    code = """import dlt
@dlt.table(name="events")
@dlt.expect_or_drop("v", "event_id IS NOT NULL")
def events():
    return dlt.read_stream("bronze")
"""
    with pytest.raises(ValueError, match="Invalid quarantine_suffix"):
        auto_heal_source(code, quarantine_suffix="; DROP TABLE")

    with pytest.raises(ValueError, match="Invalid quarantine_suffix"):
        auto_heal_source(code, quarantine_suffix="")


def test_hoist_imports_preserves_notebook_and_future_headers() -> None:
    """Hoisted imports must appear after Databricks notebook header, docstrings, and from __future__."""
    source = '''# Databricks notebook source
"""Pipeline module docstring."""
from __future__ import annotations

@dlt.table
@dlt.expect_or_drop("v1", "x > 0")
def tbl():
    return dlt.read_stream("raw")
'''
    result = auto_heal_source(source)
    assert result.can_heal is True
    lines = result.healed_code.splitlines()

    # Notebook header must be line 0
    assert lines[0] == "# Databricks notebook source"
    # Docstring before imports
    assert '"""Pipeline module docstring."""' in lines[1:3]
    # Future import before normal imports
    future_idx = next(i for i, line in enumerate(lines) if "from __future__" in line)
    imports_idx = next(
        i for i, line in enumerate(lines) if "from pyspark.sql import functions as F" in line
    )
    assert future_idx < imports_idx


def test_auto_heal_escapes_backslashes_in_conditions() -> None:
    """Conditions with backslashes must extract to exact AST expression matching coalesce((<orig>), false)."""
    source = r"""import dlt

@dlt.table
@dlt.expect_or_drop("valid_zip", r"zip RLIKE '^\d{5}$'")
def tbl():
    return dlt.read_stream("raw")
"""
    result = auto_heal_source(source)
    assert result.can_heal is True
    parsed = ast.parse(result.healed_code)
    assert parsed is not None

    expr_literals: list[str] = []
    for node in ast.walk(parsed):
        if isinstance(node, ast.Call):
            is_expr = (isinstance(node.func, ast.Name) and node.func.id == "expr") or (
                isinstance(node.func, ast.Attribute) and node.func.attr == "expr"
            )
            if is_expr and node.args and isinstance(node.args[0], ast.Constant):
                expr_literals.append(str(node.args[0].value))

    assert len(expr_literals) == 1
    expected_condition_literal = r"coalesce((zip RLIKE '^\d{5}$'), false)"
    assert expr_literals[0] == expected_condition_literal


def test_auto_heal_refuses_nondeterministic_body() -> None:
    """Functions with non-deterministic or stateful calls must be refused."""
    source = """import dlt
from pyspark.sql import functions as F

@dlt.table
@dlt.expect_or_drop("v", "x > 0")
def nondet_tbl():
    df = dlt.read_stream("raw")
    return df.withColumn("token", F.rand())
"""
    result = auto_heal_source(source)
    assert result.can_heal is False
    assert "non-deterministic" in result.reason.lower() or "stateful" in result.reason.lower()


@pytest.mark.integration
def test_auto_heal_executes_healed_code_on_regex_data(spark: Any) -> None:
    """Execute healed code with regex pattern on Spark: assert clean == original with drop rule."""
    source = r"""import dlt

@dlt.table(name="addresses")
@dlt.expect_or_drop("valid_zip", r"zip RLIKE '^[0-9]{5}$'")
def addresses():
    return dlt.read_stream("bronze_addresses")
"""
    result = auto_heal_source(source)
    assert result.can_heal is True

    raw_data = [(1, "94105"), (2, "abcde"), (3, None), (4, "1234"), (5, "90210")]
    df_bronze = spark.createDataFrame(raw_data, ["id", "zip"])

    df_clean, df_quarantine = _execute_healed_pipeline_on_spark(
        spark=spark,
        healed_code=result.healed_code,
        source_table="bronze_addresses",
        source_df=df_bronze,
        func_name="addresses",
        table_name="addresses",
    )

    clean_ids = {r.id for r in df_clean.collect()}
    quarantine_ids = {r.id for r in df_quarantine.collect()}

    assert clean_ids == {1, 5}
    assert quarantine_ids == {2, 3, 4}
    assert clean_ids.isdisjoint(quarantine_ids)
    assert clean_ids.union(quarantine_ids) == {1, 2, 3, 4, 5}
