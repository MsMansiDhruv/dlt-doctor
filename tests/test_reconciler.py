"""Tests for RestatementDoctor blast radius and Delta restatement."""

from __future__ import annotations

import pytest
import sqlglot

from dlt_doctor.reconciler import RestatementDoctor


def test_calculate_blast_radius_math() -> None:
    doctor = RestatementDoctor()
    scope = doctor.calculate_blast_radius(
        target_table="main.gold.fact_orders",
        late_date_iso="2026-10-25",
        window_size_days=7,
        partition_col="order_date",
        match_key="order_id",
    )

    assert scope.blast_radius_start == "2026-10-25"
    assert scope.blast_radius_end == "2026-10-31"
    assert "BETWEEN '2026-10-25' AND '2026-10-31'" in scope.delta_merge_sql
    assert "MERGE INTO main.gold.fact_orders" in scope.delta_merge_sql


def test_validate_window_size_days_minimum() -> None:
    """RestatementDoctor must validate that window_size_days is >= 1."""
    doctor = RestatementDoctor()
    with pytest.raises(ValueError, match="window_size_days must be at least 1"):
        doctor.calculate_blast_radius("gold_orders", "2026-11-01", 0)

    with pytest.raises(ValueError, match="window_size_days must be at least 1"):
        doctor.calculate_blast_radius("gold_orders", "2026-11-01", -3)


def test_delete_unmatched_defaults_false_and_warns_when_enabled() -> None:
    """delete_unmatched must default to False, and emit a loud warning when explicitly enabled."""
    doctor = RestatementDoctor()
    # Default is False: no delete clause
    scope_default = doctor.calculate_blast_radius("gold_orders", "2026-11-01", 7)
    assert "WHEN NOT MATCHED BY SOURCE" not in scope_default.delta_merge_sql

    # When enabled: warns and includes delete clause
    with pytest.warns(UserWarning, match="delete_unmatched is enabled"):
        scope_warn = doctor.calculate_blast_radius("gold_orders", "2026-11-01", 7, delete_unmatched=True)
    assert "WHEN NOT MATCHED BY SOURCE" in scope_warn.delta_merge_sql


def test_restate_merge_stops_inserting_out_of_window_source_rows() -> None:
    """Restatement MERGE must restrict NOT MATCHED insert to in-window source rows."""
    doctor = RestatementDoctor()
    scope = doctor.calculate_blast_radius("gold_orders", "2026-11-01", 7, partition_col="order_date")
    sql = scope.delta_merge_sql

    # Must parse with sqlglot
    parsed = sqlglot.parse_one(sql, read="spark")
    assert parsed is not None

    # Must restrict insert condition to the partition window
    assert "WHEN NOT MATCHED AND source.order_date BETWEEN '2026-11-01' AND '2026-11-07' THEN INSERT *" in sql


def test_advise_liquid_clustering() -> None:
    doctor = RestatementDoctor()
    ddl = doctor.advise_liquid_clustering(
        table_name="main.gold.orders",
        filter_columns=["order_date", "region"],
        join_columns=["customer_id", "region"],
    )
    assert "ALTER TABLE main.gold.orders CLUSTER BY" in ddl
    assert "customer_id" in ddl
    assert "region" in ddl


@pytest.mark.integration
def test_execute_restate_merge_on_pyspark() -> None:
    """Execute restatement MERGE on local PySpark Delta table."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("PySpark not installed")

    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("RestateTest")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"Local PySpark/JVM not available: {exc}")

    try:
        spark.sql("CREATE TABLE IF NOT EXISTS default.restate_tgt (id INT, order_date DATE, amt DOUBLE) USING DELTA")
        spark.sql("CREATE TABLE IF NOT EXISTS default.source_recalc (id INT, order_date DATE, amt DOUBLE) USING DELTA")
        doctor = RestatementDoctor()
        scope = doctor.calculate_blast_radius(
            target_table="default.restate_tgt",
            late_date_iso="2026-11-01",
            window_size_days=7,
            partition_col="order_date",
            match_key="id",
            source_table="default.source_recalc",
            delete_unmatched=False,
        )
        spark.sql(scope.delta_merge_sql)
    finally:
        spark.stop()
