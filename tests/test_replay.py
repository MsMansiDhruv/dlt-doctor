"""Tests for quarantine replay SQL generator."""

from __future__ import annotations

import pytest
import sqlglot

from dlt_doctor.replay import generate_replay_sql


def test_generate_replay_sql_dedupes_quarantine_by_key() -> None:
    """Replay SQL must deduplicate quarantine rows by primary key to prevent Delta MERGE cardinality errors."""
    sql = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
    )
    # Parse SQL to verify syntax validity
    parsed = sqlglot.parse_one(sql, read="spark")
    assert parsed is not None
    # Must contain row_number deduplication
    assert "row_number()" in sql.lower() or "row_number ()" in sql.lower()
    assert "order_id" in sql


def test_generate_replay_sql_validates_iso_timestamps() -> None:
    """Replay SQL generator must strictly validate start_time and end_time as ISO timestamps."""
    with pytest.raises(ValueError, match="Invalid ISO timestamp"):
        generate_replay_sql(
            quarantine_table="bronze_quarantine",
            target_table="silver_orders",
            primary_keys=["order_id"],
            start_time="not-an-iso-time",
        )

    with pytest.raises(ValueError, match="Invalid ISO timestamp"):
        generate_replay_sql(
            quarantine_table="bronze_quarantine",
            target_table="silver_orders",
            primary_keys=["order_id"],
            end_time="2026/13/45",
        )


def test_generate_replay_sql_recency_guard() -> None:
    """Replay SQL must include a recency guard so older quarantined records do not overwrite newer target records."""
    sql = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        recency_col="updated_at",
    )
    parsed = sqlglot.parse_one(sql, read="spark")
    assert parsed is not None
    assert "target.updated_at IS NULL OR source.updated_at >= target.updated_at" in sql


def test_generate_replay_sql_marks_replayed_rows() -> None:
    """Replay SQL must mark replayed records with _replayed_at."""
    sql = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
    )
    parsed = sqlglot.parse_one(sql, read="spark")
    assert parsed is not None
    assert "_replayed_at" in sql


def test_generate_replay_sql_partition_pruning() -> None:
    """Replay SQL should prune partitions when partition_col and partition_val are provided."""
    sql = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        partition_col="event_date",
        partition_val="2026-11-01",
    )
    parsed = sqlglot.parse_one(sql, read="spark")
    assert parsed is not None
    assert "event_date = '2026-11-01'" in sql
    assert "target.event_date = source.event_date" in sql


def test_cli_replay_partition_flags() -> None:
    from unittest.mock import patch
    from dlt_doctor.cli import main

    test_args = [
        "dlt-doctor",
        "replay",
        "--quarantine",
        "bronze_quarantine",
        "--target",
        "silver_orders",
        "--keys",
        "order_id",
        "--partition-col",
        "event_date",
        "--partition-val",
        "2026-11-01",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0



@pytest.mark.integration
def test_execute_replay_sql_on_pyspark() -> None:
    """Execute generated replay SQL against local PySpark Delta tables."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("PySpark not installed")

    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("ReplayTest")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"Local PySpark/JVM not available: {exc}")

    try:
        spark.sql(
            "CREATE TABLE IF NOT EXISTS default.test_tgt (id INT, amt DOUBLE, updated_at TIMESTAMP, _replayed_at TIMESTAMP) USING DELTA"
        )
        spark.sql(
            "CREATE TABLE IF NOT EXISTS default.test_quarantine (id INT, amt DOUBLE, updated_at TIMESTAMP, _failed_rules ARRAY<STRING>, _quarantined_at TIMESTAMP) USING DELTA"
        )
        sql = generate_replay_sql(
            quarantine_table="default.test_quarantine",
            target_table="default.test_tgt",
            primary_keys=["id"],
            recency_col="updated_at",
        )
        spark.sql(sql)
    finally:
        spark.stop()
