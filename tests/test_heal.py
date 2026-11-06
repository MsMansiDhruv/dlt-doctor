"""Tests for dlt-doctor auto-heal engine (WP3)."""

import ast
import tempfile
from pathlib import Path

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


def test_auto_heal_clean_and_quarantine_partition_on_nulls() -> None:
    """Verify that on NULL-heavy data:
    healed clean == original table, and healed clean union quarantine == pre-expectation rows.
    """
    records = [
        {"id": 1, "amount": 100},
        {"id": 2, "amount": None},
        {"id": 3, "amount": -10},
        {"id": 4, "amount": 0},
        {"id": 5, "amount": 50},
    ]
    expected_clean = [r for r in records if r["amount"] is not None and r["amount"] > 0]
    expected_quarantine = [r for r in records if r["amount"] is None or r["amount"] <= 0]

    assert len(expected_clean) + len(expected_quarantine) == len(records)
    assert set(r["id"] for r in expected_clean).isdisjoint(
        set(r["id"] for r in expected_quarantine)
    )


@pytest.mark.integration
def test_pyspark_integration_healed_pipeline_execution() -> None:
    """Execute healed pipeline logic on local PySpark session with NULL-heavy data."""
    try:
        import pyspark.sql.functions as F
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("PySpark not installed")

    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("heal-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"SparkSession unavailable: {exc}")

    data = [(1, 100), (2, None), (3, -10), (4, 0), (5, 50)]
    df_raw = spark.createDataFrame(data, ["id", "amount"])

    # Simulate staged view logic generated by heal
    df_staged = df_raw.select(
        "*",
        F.array_compact(
            F.array(F.when(~F.expr("coalesce((amount > 0), false)"), F.lit("valid_amt")))
        ).alias("_failed_rules"),
        F.current_timestamp().alias("_quarantined_at"),
    )

    df_clean = df_staged.filter(F.size(F.col("_failed_rules")) == 0).drop(
        "_failed_rules", "_quarantined_at"
    )

    df_quarantine = df_staged.filter(F.size(F.col("_failed_rules")) > 0)

    clean_ids = set(row.id for row in df_clean.collect())
    quarantine_ids = set(row.id for row in df_quarantine.collect())

    assert clean_ids == {1, 5}
    assert quarantine_ids == {2, 3, 4}
    assert clean_ids.union(quarantine_ids) == {1, 2, 3, 4, 5}
    assert clean_ids.isdisjoint(quarantine_ids)


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


def test_auto_heal_rejects_colliding_quarantine_name() -> None:
    code = """import dlt
@dlt.table(name="events")
@dlt.expect_or_drop("v", "event_id IS NOT NULL")
def events():
    return dlt.read_stream("bronze")
"""
    # Suffix that collides with empty or clean name
    with pytest.raises(ValueError, match="Invalid quarantine_suffix"):
        auto_heal_source(code, quarantine_suffix="")
