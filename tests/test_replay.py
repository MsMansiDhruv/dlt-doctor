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
    """Replay SQL marks replayed records with _replayed_at only when mark_replayed=True."""
    # Omitted by default to prevent target schema collision on UPDATE SET * / INSERT *
    sql_default = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
    )
    parsed_default = sqlglot.parse_one(sql_default, read="spark")
    assert parsed_default is not None
    assert "_replayed_at" not in sql_default

    # Explicitly enabled
    sql_marked = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        mark_replayed=True,
    )
    parsed_marked = sqlglot.parse_one(sql_marked, read="spark")
    assert parsed_marked is not None
    assert "_replayed_at" in sql_marked


def test_generate_replay_sql_partition_pruning() -> None:
    """Replay SQL must include literal target-side predicate for Delta static partition pruning."""
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
    # Must have literal target-side predicate in ON clause:
    assert "target.event_date = '2026-11-01'" in sql

    # Numeric typed partition values
    sql_int = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        partition_col="year",
        partition_val=2026,
    )
    assert "target.year = 2026" in sql_int


def test_replay_partition_val_without_col_raises() -> None:
    """Passing partition_val without partition_col must raise ValueError."""
    with pytest.raises(ValueError, match="partition_val requires partition_col"):
        generate_replay_sql(
            quarantine_table="bronze_quarantine",
            target_table="silver_orders",
            primary_keys=["order_id"],
            partition_val="2026-11-01",
        )


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
