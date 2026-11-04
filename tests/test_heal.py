"""Tests for dlt-doctor auto-heal engine (WP3)."""

import ast
import tempfile
from pathlib import Path

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


def test_auto_heal_refuses_multi_statement_body() -> None:
    input_code = """import dlt

@dlt.table
@dlt.expect_or_drop("valid_amt", "amount > 0")
def complex_pipeline():
    df = dlt.read_stream("bronze_orders")
    df = df.filter("active = true")
    return df
"""
    result = auto_heal_source(input_code)
    assert result.can_heal is False
    assert "multi-statement bodies require manual refactoring" in result.reason
    assert result.diff == ""


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
