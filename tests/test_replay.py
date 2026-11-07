"""Tests for quarantine replay SQL generator."""

from __future__ import annotations

from typing import Any

import pytest
import sqlglot

from dlt_doctor.replay import generate_replay_sql


def test_generate_replay_sql_dedupes_quarantine_by_key() -> None:
    """Replay SQL must deduplicate quarantine rows by primary key to prevent Delta MERGE cardinality errors."""
    sql = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        allow_unvalidated=True,
    )
    parsed = sqlglot.parse_one(sql, read="spark")
    assert parsed is not None
    assert "row_number()" in sql.lower() or "row_number ()" in sql.lower()
    assert "order_id" in sql


def test_replay_requires_revalidation_or_allow_unvalidated() -> None:
    """Replay generator must raise ValueError if neither valid_filter_sql nor allow_unvalidated is provided."""
    with pytest.raises(ValueError, match="Replay requires either a revalidation condition"):
        generate_replay_sql(
            quarantine_table="bronze_quarantine",
            target_table="silver_orders",
            primary_keys=["order_id"],
        )


def test_replay_with_valid_filter_sql() -> None:
    """Providing valid_filter_sql must include the condition in WHERE clause and pass SQL validation."""
    sql = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        valid_filter_sql="amount > 0 AND order_id IS NOT NULL",
    )
    assert "(amount > 0 AND order_id IS NOT NULL)" in sql
    parsed = sqlglot.parse_one(sql, read="spark")
    assert parsed is not None


def test_replay_invalid_filter_sql_raises() -> None:
    """Invalid SQL in valid_filter_sql must raise ValueError."""
    with pytest.raises(ValueError, match="Failed to parse valid_filter_sql"):
        generate_replay_sql(
            quarantine_table="bronze_quarantine",
            target_table="silver_orders",
            primary_keys=["order_id"],
            valid_filter_sql="amount >>> 0",
        )


def test_replay_rule_name_matches_exact_array() -> None:
    """Replay with rule_name must only reprocess rows where that specific rule was the sole failure."""
    sql = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        rule_name="bad_zip",
        allow_unvalidated=True,
    )
    assert "_failed_rules = array('bad_zip')" in sql
    assert "array_contains" not in sql


def test_generate_replay_sql_validates_iso_timestamps() -> None:
    """Replay SQL generator must strictly validate start_time and end_time as ISO timestamps."""
    with pytest.raises(ValueError, match="Invalid ISO timestamp"):
        generate_replay_sql(
            quarantine_table="bronze_quarantine",
            target_table="silver_orders",
            primary_keys=["order_id"],
            start_time="not-an-iso-time",
            allow_unvalidated=True,
        )

    with pytest.raises(ValueError, match="Invalid ISO timestamp"):
        generate_replay_sql(
            quarantine_table="bronze_quarantine",
            target_table="silver_orders",
            primary_keys=["order_id"],
            end_time="2026/13/45",
            allow_unvalidated=True,
        )


def test_generate_replay_sql_recency_guard() -> None:
    """Replay SQL must include a recency guard so older quarantined records do not overwrite newer target records."""
    sql = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        recency_col="updated_at",
        allow_unvalidated=True,
    )
    parsed = sqlglot.parse_one(sql, read="spark")
    assert parsed is not None
    assert (
        "WHEN MATCHED AND (target.updated_at IS NULL OR source.updated_at >= target.updated_at) THEN UPDATE SET *"
        in sql
    )


def test_replay_defaults_to_insert_only_without_recency_col() -> None:
    """When recency_col is None, replay MERGE must be insert-only with no WHEN MATCHED clause."""
    sql = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        allow_unvalidated=True,
    )
    assert "WHEN MATCHED" not in sql
    assert "WHEN NOT MATCHED THEN INSERT *;" in sql


def test_replay_dedupes_latest_per_key_before_valid_filter_sql() -> None:
    """Validity filter must run on the deduplicated latest-per-key row, not prior to deduplication."""
    sql = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        valid_filter_sql="amount > 0",
    )
    assert ") AS deduped\n    WHERE (amount > 0)" in sql


def test_generate_replay_sql_marks_replayed_rows() -> None:
    """Replay SQL marks replayed records with _replayed_at only when mark_replayed=True."""
    sql_default = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        allow_unvalidated=True,
    )
    parsed_default = sqlglot.parse_one(sql_default, read="spark")
    assert parsed_default is not None
    assert "_replayed_at" not in sql_default

    sql_marked = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        mark_replayed=True,
        allow_unvalidated=True,
    )
    parsed_marked = sqlglot.parse_one(sql_marked, read="spark")
    assert parsed_marked is not None
    assert "_replayed_at" in sql_marked


def test_generate_replay_sql_partition_pruning_and_dedupe() -> None:
    """Replay SQL must include partition_col in QUALIFY dedupe and literal predicate in ON clause."""
    sql = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        partition_col="event_date",
        partition_val="2026-11-01",
        allow_unvalidated=True,
    )
    parsed = sqlglot.parse_one(sql, read="spark")
    assert parsed is not None
    assert "event_date = '2026-11-01'" in sql
    assert "target.event_date = '2026-11-01'" in sql
    assert "PARTITION BY order_id, event_date" in sql

    sql_int = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        partition_col="year",
        partition_val=2026,
        allow_unvalidated=True,
    )
    assert "target.year = 2026" in sql_int


def test_replay_rejects_non_finite_float() -> None:
    """Replay generator must reject non-finite float partition values (NaN, inf, -inf)."""
    for bad_val in [float("nan"), float("inf"), float("-inf")]:
        with pytest.raises(ValueError, match="partition_val must be a finite number"):
            generate_replay_sql(
                quarantine_table="bronze_quarantine",
                target_table="silver_orders",
                primary_keys=["order_id"],
                partition_col="part_ratio",
                partition_val=bad_val,
                allow_unvalidated=True,
            )


def test_replay_partition_val_without_col_raises() -> None:
    """Passing partition_val without partition_col must raise ValueError."""
    with pytest.raises(ValueError, match="partition_val requires partition_col"):
        generate_replay_sql(
            quarantine_table="bronze_quarantine",
            target_table="silver_orders",
            primary_keys=["order_id"],
            partition_val="2026-11-01",
            allow_unvalidated=True,
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
        "--allow-unvalidated",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


@pytest.mark.integration
def test_execute_replay_sql_on_pyspark(spark: Any) -> None:
    """Execute generated replay SQL against local PySpark Delta tables and assert reprocessed rows."""
    import shutil
    import tempfile

    tmp_tgt = tempfile.mkdtemp().replace("\\", "/")
    tmp_q = tempfile.mkdtemp().replace("\\", "/")

    try:
        spark.sql("DROP TABLE IF EXISTS default.test_replay_target")
        spark.sql("DROP TABLE IF EXISTS default.test_replay_quarantine")

        spark.sql(f"""
            CREATE TABLE default.test_replay_target (
                id INT,
                amt DOUBLE,
                updated_at STRING
            ) USING DELTA LOCATION '{tmp_tgt}'
        """)
        spark.sql("""
            INSERT INTO default.test_replay_target VALUES
            (1, 50.0, '2026-01-01 08:00:00')
        """)

        spark.sql(f"""
            CREATE TABLE default.test_replay_quarantine (
                id INT,
                amt DOUBLE,
                updated_at STRING,
                _failed_rules ARRAY<STRING>,
                _quarantined_at STRING
            ) USING DELTA LOCATION '{tmp_q}'
        """)

        # Quarantine rows:
        # id=1 older quarantined (amt=100.0 at 10:00)
        # id=1 newer quarantined (amt=200.0 at 12:00) -> must win deduplication!
        # id=2 quarantined for 2 rules (['bad_amt', 'bad_zip']) -> must be skipped when filtering for 'bad_amt'!
        spark.sql("""
            INSERT INTO default.test_replay_quarantine VALUES
            (1, 100.0, '2026-01-01 10:00:00', array('bad_amt'), '2026-01-01 10:00:00'),
            (1, 200.0, '2026-01-01 12:00:00', array('bad_amt'), '2026-01-01 12:00:00'),
            (2, 300.0, '2026-01-01 11:00:00', array('bad_amt', 'bad_zip'), '2026-01-01 11:00:00')
        """)

        sql = generate_replay_sql(
            quarantine_table="default.test_replay_quarantine",
            target_table="default.test_replay_target",
            primary_keys=["id"],
            rule_name="bad_amt",
            recency_col="updated_at",
            valid_filter_sql="amt > 0",
        )

        spark.sql(sql)

        target_rows = {
            (r.id, r.amt, r.updated_at)
            for r in spark.sql(
                "SELECT id, amt, updated_at FROM default.test_replay_target"
            ).collect()
        }
        # id=1 updated to 200.0; id=2 not replayed because it had 2 failed rules
        assert target_rows == {(1, 200.0, "2026-01-01 12:00:00")}
    finally:
        spark.sql("DROP TABLE IF EXISTS default.test_replay_target")
        spark.sql("DROP TABLE IF EXISTS default.test_replay_quarantine")
        shutil.rmtree(tmp_tgt, ignore_errors=True)
        shutil.rmtree(tmp_q, ignore_errors=True)


@pytest.mark.integration
def test_execute_replay_with_mark_replayed_on_pyspark(spark: Any) -> None:
    """Execute replay with mark_replayed=True into a Delta table containing _replayed_at."""
    import shutil
    import tempfile

    tmp_tgt = tempfile.mkdtemp().replace("\\", "/")
    tmp_q = tempfile.mkdtemp().replace("\\", "/")

    try:
        spark.sql("DROP TABLE IF EXISTS default.test_replay_marked_tgt")
        spark.sql("DROP TABLE IF EXISTS default.test_replay_marked_q")

        spark.sql(f"""
            CREATE TABLE default.test_replay_marked_tgt (
                id INT,
                amt DOUBLE,
                updated_at STRING,
                _replayed_at TIMESTAMP
            ) USING DELTA LOCATION '{tmp_tgt}'
        """)

        spark.sql(f"""
            CREATE TABLE default.test_replay_marked_q (
                id INT,
                amt DOUBLE,
                updated_at STRING,
                _failed_rules ARRAY<STRING>,
                _quarantined_at STRING
            ) USING DELTA LOCATION '{tmp_q}'
        """)

        # Add quarantined rows for id=10 (valid now) and id=20 (older valid at 10:00, but latest at 12:00 is invalid amt=-50)
        spark.sql("""
            INSERT INTO default.test_replay_marked_q VALUES
            (10, 100.0, '2026-01-01 10:00:00', array('bad_amt'), '2026-01-01 10:00:00'),
            (20, 50.0, '2026-01-01 10:00:00', array('bad_amt'), '2026-01-01 10:00:00'),
            (20, -50.0, '2026-01-01 12:00:00', array('bad_amt'), '2026-01-01 12:00:00')
        """)

        sql = generate_replay_sql(
            quarantine_table="default.test_replay_marked_q",
            target_table="default.test_replay_marked_tgt",
            primary_keys=["id"],
            recency_col="updated_at",
            mark_replayed=True,
            valid_filter_sql="amt > 0",
        )

        spark.sql(sql)

        target_rows = spark.sql(
            "SELECT id, amt, _replayed_at FROM default.test_replay_marked_tgt"
        ).collect()
        # id=10 was inserted and has non-null _replayed_at
        # id=20 was NOT inserted because its latest quarantined state was amt=-50 (failed valid_filter_sql)
        assert len(target_rows) == 1
        assert target_rows[0].id == 10
        assert target_rows[0].amt == 100.0
        assert target_rows[0]._replayed_at is not None
    finally:
        spark.sql("DROP TABLE IF EXISTS default.test_replay_marked_tgt")
        spark.sql("DROP TABLE IF EXISTS default.test_replay_marked_q")
        shutil.rmtree(tmp_tgt, ignore_errors=True)
        shutil.rmtree(tmp_q, ignore_errors=True)


def test_replay_filters_applied_post_dedupe() -> None:
    """rule_name and partition filters must be evaluated on the deduplicated row, not before QUALIFY."""
    sql = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        rule_name="bad_zip",
        partition_col="region",
        partition_val="US",
        start_time="2026-01-01T00:00:00",
        end_time="2026-01-02T00:00:00",
        allow_unvalidated=True,
    )
    parsed = sqlglot.parse_one(sql, read="spark")
    assert parsed is not None

    # start_time and end_time define candidate window before QUALIFY
    before_qualify, after_qualify = sql.split("QUALIFY", 1)
    assert "_quarantined_at >= '2026-01-01T00:00:00'" in before_qualify
    assert "_quarantined_at <= '2026-01-02T00:00:00'" in before_qualify
    assert "_failed_rules = array" not in before_qualify
    assert "region = 'US'" not in before_qualify

    # rule_name and partition filters sit on the deduplicated relation
    assert "_failed_rules = array('bad_zip')" in after_qualify
    assert "region = 'US'" in after_qualify


def test_replay_erasure_table_guard_sql() -> None:
    """Providing erasure_table must generate an anti-join NOT EXISTS guard on target keys."""
    sql = generate_replay_sql(
        quarantine_table="bronze_quarantine",
        target_table="silver_orders",
        primary_keys=["order_id"],
        erasure_table="audit_tombstones",
        allow_unvalidated=True,
    )
    parsed = sqlglot.parse_one(sql, read="spark")
    assert parsed is not None
    assert (
        "NOT EXISTS (SELECT 1 FROM audit_tombstones AS erased WHERE erased.order_id = deduped.order_id)"
        in sql
    )


@pytest.mark.integration
def test_execute_replay_dedupe_before_rule_filter_on_pyspark(spark: Any) -> None:
    """Deduplication before rule filter ensures newer failure prevents picking an older rule failure."""
    import shutil
    import tempfile

    tmp_tgt = tempfile.mkdtemp().replace("\\", "/")
    tmp_q = tempfile.mkdtemp().replace("\\", "/")

    try:
        spark.sql("DROP TABLE IF EXISTS default.test_replay_filter_tgt")
        spark.sql("DROP TABLE IF EXISTS default.test_replay_filter_q")

        spark.sql(f"""
            CREATE TABLE default.test_replay_filter_tgt (
                id INT,
                amt DOUBLE
            ) USING DELTA LOCATION '{tmp_tgt}'
        """)

        spark.sql(f"""
            CREATE TABLE default.test_replay_filter_q (
                id INT,
                amt DOUBLE,
                _failed_rules ARRAY<STRING>,
                _quarantined_at STRING
            ) USING DELTA LOCATION '{tmp_q}'
        """)

        # id=1 failed 'bad_amt' at 10:00:00, but latest at 12:00:00 failed 'bad_zip'
        # id=2 failed 'bad_amt' at 11:00:00 and that is its latest row
        spark.sql("""
            INSERT INTO default.test_replay_filter_q VALUES
            (1, 100.0, array('bad_amt'), '2026-01-01 10:00:00'),
            (1, 100.0, array('bad_zip'), '2026-01-01 12:00:00'),
            (2, 200.0, array('bad_amt'), '2026-01-01 11:00:00')
        """)

        # Replay filtering specifically for rule 'bad_amt'
        sql = generate_replay_sql(
            quarantine_table="default.test_replay_filter_q",
            target_table="default.test_replay_filter_tgt",
            primary_keys=["id"],
            rule_name="bad_amt",
            valid_filter_sql="amt > 0",
        )
        spark.sql(sql)

        target_rows = {
            (r.id, r.amt)
            for r in spark.sql("SELECT id, amt FROM default.test_replay_filter_tgt").collect()
        }
        # id=1 must NOT be replayed because its latest state failed 'bad_zip'
        # id=2 must be replayed because its latest state failed 'bad_amt'
        assert target_rows == {(2, 200.0)}
    finally:
        spark.sql("DROP TABLE IF EXISTS default.test_replay_filter_tgt")
        spark.sql("DROP TABLE IF EXISTS default.test_replay_filter_q")
        shutil.rmtree(tmp_tgt, ignore_errors=True)
        shutil.rmtree(tmp_q, ignore_errors=True)


@pytest.mark.integration
def test_execute_replay_erasure_guard_prevents_resurrection_on_pyspark(spark: Any) -> None:
    """Erasure guard prevents resurrecting records previously deleted/erased from target."""
    import shutil
    import tempfile

    tmp_tgt = tempfile.mkdtemp().replace("\\", "/")
    tmp_q = tempfile.mkdtemp().replace("\\", "/")
    tmp_erased = tempfile.mkdtemp().replace("\\", "/")

    try:
        spark.sql("DROP TABLE IF EXISTS default.test_replay_erase_tgt")
        spark.sql("DROP TABLE IF EXISTS default.test_replay_erase_q")
        spark.sql("DROP TABLE IF EXISTS default.test_replay_erase_tombstones")

        spark.sql(f"""
            CREATE TABLE default.test_replay_erase_tgt (
                id INT,
                amt DOUBLE
            ) USING DELTA LOCATION '{tmp_tgt}'
        """)

        spark.sql(f"""
            CREATE TABLE default.test_replay_erase_q (
                id INT,
                amt DOUBLE,
                _failed_rules ARRAY<STRING>,
                _quarantined_at STRING
            ) USING DELTA LOCATION '{tmp_q}'
        """)

        spark.sql(f"""
            CREATE TABLE default.test_replay_erase_tombstones (
                id INT,
                erased_at STRING
            ) USING DELTA LOCATION '{tmp_erased}'
        """)

        # Tombstone: id=99 was erased
        spark.sql("""
            INSERT INTO default.test_replay_erase_tombstones VALUES
            (99, '2026-01-01 12:00:00')
        """)

        # Quarantine has older rows for both id=99 (erased) and id=100 (active)
        spark.sql("""
            INSERT INTO default.test_replay_erase_q VALUES
            (99, 50.0, array('bad_amt'), '2026-01-01 10:00:00'),
            (100, 150.0, array('bad_amt'), '2026-01-01 10:00:00')
        """)

        sql = generate_replay_sql(
            quarantine_table="default.test_replay_erase_q",
            target_table="default.test_replay_erase_tgt",
            primary_keys=["id"],
            valid_filter_sql="amt > 0",
            erasure_table="default.test_replay_erase_tombstones",
        )
        spark.sql(sql)

        target_ids = {
            r.id for r in spark.sql("SELECT id FROM default.test_replay_erase_tgt").collect()
        }
        # id=99 was NOT resurrected due to erasure guard; id=100 was successfully replayed
        assert target_ids == {100}
    finally:
        spark.sql("DROP TABLE IF EXISTS default.test_replay_erase_tgt")
        spark.sql("DROP TABLE IF EXISTS default.test_replay_erase_q")
        spark.sql("DROP TABLE IF EXISTS default.test_replay_erase_tombstones")
        shutil.rmtree(tmp_tgt, ignore_errors=True)
        shutil.rmtree(tmp_q, ignore_errors=True)
        shutil.rmtree(tmp_erased, ignore_errors=True)
