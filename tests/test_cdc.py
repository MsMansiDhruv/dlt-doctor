"""Tests for CdcDoctor streaming deduplication and SCD templates."""

from __future__ import annotations

import ast
from typing import Any
from unittest.mock import patch

import pytest
import sqlglot

from dlt_doctor.cdc import CdcDoctor


def test_cdc_doctor_templates() -> None:
    doctor = CdcDoctor()
    template = doctor.generate_cdc_templates(
        target_table="main.silver.dim_merchants",
        primary_keys=["merchant_id"],
        event_time_col="updated_at",
        sequence_col="_cdc_seq_id",
        partition_col="region",
        value_cols=["merchant_name"],
    )

    assert "MERGE INTO main.silver.dim_merchants" in template.scd1_merge_sql
    assert "target.merchant_id = source.merchant_id" in template.scd1_merge_sql
    assert "target.region = source.region" in template.scd1_merge_sql
    assert "Window.partitionBy(['merchant_id'])" in template.microbatch_dedup_code
    assert "row_number().over(window_spec)" in template.microbatch_dedup_code
    assert "is_current = false" in template.scd2_merge_sql


def test_cdf_filter_indentation() -> None:
    """Microbatch dedup code must have consistent 4-space indentation for cdf_filter."""
    doctor = CdcDoctor()
    template = doctor.generate_cdc_templates(
        target_table="main.silver.orders",
        primary_keys=["order_id"],
        source_is_cdf=True,
    )
    # ast.parse ensures Python syntax is valid
    parsed_ast = ast.parse(template.microbatch_dedup_code)
    assert parsed_ast is not None
    # Check that cdf_filter does not contain 8 spaces
    assert "        microbatch_df = microbatch_df.filter" not in template.microbatch_dedup_code
    assert "    microbatch_df = microbatch_df.filter" in template.microbatch_dedup_code


def test_cdc_drops_change_type_before_merge_and_handles_deletes_separately() -> None:
    """_change_type must be dropped before MERGE, with deletes handled in a separate clause."""
    doctor = CdcDoctor()
    template = doctor.generate_cdc_templates(
        target_table="silver_orders",
        primary_keys=["order_id"],
    )
    # Microbatch dedup code drops _change_type before temp view
    assert (
        "drop('_change_type'" in template.microbatch_dedup_code
        or ".drop('_change_type'," in template.microbatch_dedup_code
    )
    # SCD1 MERGE must not update or insert _change_type column
    assert "source._change_type" not in template.scd1_merge_sql
    # Deletes must be handled in a separate clause
    assert "cdc_deletes_batch" in template.scd1_merge_sql
    assert "THEN DELETE" in template.scd1_merge_sql


def test_scd2_template_structure_and_syntax() -> None:
    """SCD2 template must use distinct merge_key, correct join alias, no helper columns inserted, and set valid_from/to/is_current."""
    doctor = CdcDoctor()
    template = doctor.generate_cdc_templates(
        target_table="silver_customers",
        primary_keys=["customer_id"],
        event_time_col="updated_at",
        value_cols=["name", "tier"],
    )
    sql = template.scd2_merge_sql

    # Must parse with sqlglot
    parsed = sqlglot.parse_one(sql, read="spark")
    assert parsed is not None

    # Distinct merge_key column used in ON clause and staging query
    assert "merge_key" in sql
    assert "merge_insert" not in sql  # Old bug helper column removed

    # Correct join alias: current_target.customer_id = source.customer_id
    assert "current_target.customer_id = source.customer_id" in sql
    join_clause = sql.split("JOIN")[1].split("UNION")[0]
    assert " target.customer_id = source.customer_id" not in join_clause

    # Target fields set: valid_from, valid_to, is_current
    assert "valid_to = staged.updated_at" in sql or "valid_to = source.updated_at" in sql
    assert "is_current = false" in sql or "is_current = FALSE" in sql
    assert "is_current" in sql
    assert "valid_from" in sql


def test_scd2_composite_keys_and_recency_guard() -> None:
    """SCD2 must join on all composite keys and include a recency guard."""
    doctor = CdcDoctor()
    template = doctor.generate_cdc_templates(
        target_table="silver_customers",
        primary_keys=["tenant_id", "user_id"],
        event_time_col="updated_at",
        value_cols=["email", "status"],
    )
    sql = template.scd2_merge_sql
    parsed = sqlglot.parse_one(sql, read="spark")
    assert parsed is not None
    # Both composite keys must be joined in ON clause
    assert "target.tenant_id = staged.merge_key_tenant_id" in sql
    assert "target.user_id = staged.merge_key_user_id" in sql
    # Recency guard on SCD2 valid_from in staging
    assert "source.updated_at > current_target.valid_from" in sql


def test_scd1_recency_guard() -> None:
    """SCD1 MERGE must include a recency guard on event timestamp."""
    doctor = CdcDoctor()
    template = doctor.generate_cdc_templates(
        target_table="silver_orders",
        primary_keys=["order_id"],
        event_time_col="updated_at",
    )
    assert (
        "target.updated_at IS NULL OR source.updated_at >= target.updated_at"
        in template.scd1_merge_sql
    )


def test_scd2_requires_value_cols() -> None:
    """SCD2 generation without value_cols must raise ValueError."""
    doctor = CdcDoctor()
    with pytest.raises(ValueError, match="value_cols must be provided"):
        doctor.generate_cdc_templates(
            target_table="silver_customers",
            primary_keys=["customer_id"],
            include_scd2=True,
            value_cols=None,
        )


def test_auto_cdc_creates_streaming_table() -> None:
    """AUTO CDC template must include dp.create_streaming_table before dp.create_auto_cdc_flow and filter update_preimage."""
    doctor = CdcDoctor()
    template = doctor.generate_cdc_templates(
        target_table="main.silver.orders",
        primary_keys=["order_id"],
        source_stream="bronze_orders_cdc",
        include_auto_cdc=True,
    )
    code = template.auto_cdc_code

    # Must be valid Python AST
    parsed_ast = ast.parse(code)
    assert parsed_ast is not None

    # Must use leaf name for temporary view
    assert "@dp.temporary_view(name='orders_cdf_clean')" in code
    # Must filter update_preimage and declare create_streaming_table before create_auto_cdc_flow
    assert "update_preimage" in code
    assert "dp.create_streaming_table" in code
    assert "dp.create_auto_cdc_flow" in code
    assert code.index("dp.create_streaming_table") < code.index("dp.create_auto_cdc_flow")


def test_cdc_sequence_col_defaults_to_commit_version_for_cdf() -> None:
    """When source_is_cdf=True, sequence_col must default to _commit_version."""
    doctor = CdcDoctor()
    template = doctor.generate_cdc_templates(
        target_table="silver_orders",
        primary_keys=["order_id"],
        source_stream="bronze_cdc",
        source_is_cdf=True,
        include_auto_cdc=True,
    )
    assert "_commit_version" in template.microbatch_dedup_code
    assert "_cdc_seq_id" not in template.microbatch_dedup_code
    assert "_commit_version" in template.auto_cdc_code


def test_source_is_cdf_false_omits_change_type() -> None:
    """When source_is_cdf is False, code must not reference _change_type."""
    doctor = CdcDoctor()
    template = doctor.generate_cdc_templates(
        target_table="silver_orders",
        primary_keys=["order_id"],
        source_is_cdf=False,
    )
    assert "_change_type" not in template.microbatch_dedup_code
    assert "cdc_deletes_batch" not in template.microbatch_dedup_code
    assert "cdc_deletes_batch" not in template.scd1_merge_sql
    assert "THEN DELETE" not in template.scd1_merge_sql
    assert ast.parse(template.microbatch_dedup_code) is not None


def test_auto_cdc_requires_source_stream() -> None:
    """When auto_cdc_code is requested (include_auto_cdc=True), source_stream is required."""
    doctor = CdcDoctor()
    with pytest.raises(ValueError, match="source_stream must be provided"):
        doctor.generate_cdc_templates(
            target_table="silver_orders",
            primary_keys=["order_id"],
            include_auto_cdc=True,
        )


def test_cdc_custom_source_stream_and_template_attributes() -> None:
    doctor = CdcDoctor()
    template = doctor.generate_cdc_templates(
        target_table="main.silver.customers",
        primary_keys=["customer_id"],
        source_stream="bronze_customers_cdc",
        partition_col="region",
        value_cols=["name", "email"],
        include_auto_cdc=True,
    )
    assert template.source_stream == "bronze_customers_cdc"
    assert "spark.readStream.table('bronze_customers_cdc')" in template.auto_cdc_code
    assert "@dp.temporary_view(name='customers_cdf_clean')" in template.auto_cdc_code
    assert "target.region = source.region" in template.scd1_merge_sql
    assert "email" in template.scd2_merge_sql


def test_cdc_cli_flags_source_partition_values() -> None:

    from dlt_doctor.cli import main

    test_args = [
        "dlt-doctor",
        "cdc",
        "--table",
        "main.silver.orders",
        "--keys",
        "order_id",
        "--source",
        "raw_orders_cdc",
        "--partition-col",
        "order_date",
        "--values",
        "amount",
        "status",
    ]
    with patch("sys.argv", test_args):
        exit_code = main()
        assert exit_code == 0


@pytest.mark.integration
def test_execute_cdc_scd_on_pyspark(spark: Any) -> None:
    """Execute generated CDC SCD1 and SCD2 statements against local PySpark Delta tables,
    verifying in-order, late, and delete events with exact rows and exactly one current row per key.
    """
    import shutil
    import tempfile

    doctor = CdcDoctor()

    tmp_scd1 = tempfile.mkdtemp().replace("\\", "/")
    tmp_scd2 = tempfile.mkdtemp().replace("\\", "/")

    try:
        # 1. SCD1 Delta Execution with in-order, late, and delete events
        spark.sql("DROP TABLE IF EXISTS default.test_cdc_scd1_orders")
        spark.sql(f"""
            CREATE TABLE default.test_cdc_scd1_orders (
                id INT,
                amt DOUBLE,
                updated_at STRING
            ) USING DELTA LOCATION '{tmp_scd1}'
        """)
        # Seed initial rows: id=1 and id=2 at 10:00:00
        spark.sql("""
            INSERT INTO default.test_cdc_scd1_orders VALUES
            (1, 100.0, '2026-01-01 10:00:00'),
            (2, 200.0, '2026-01-01 10:00:00')
        """)

        tmpl_scd1 = doctor.generate_cdc_templates(
            target_table="default.test_cdc_scd1_orders",
            primary_keys=["id"],
            event_time_col="updated_at",
            source_is_cdf=True,
        )

        # Execute generated merge_cdc_microbatch directly on CDF-shaped DataFrame
        loc: dict[str, Any] = {}
        exec(tmpl_scd1.microbatch_dedup_code, loc)
        merge_fn = loc["merge_cdc_microbatch"]

        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        cdf_schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("amt", DoubleType(), True),
                StructField("updated_at", StringType(), True),
                StructField("_change_type", StringType(), True),
                StructField("_commit_version", LongType(), True),
                StructField("_commit_timestamp", LongType(), True),
            ]
        )

        # Batch 1:
        # - In-order upsert on id=1 (11:00:00, 150.0, v=101)
        # - Update preimage on id=1 (10:00:00, 100.0, v=101) -> filtered out
        # - Older in-batch duplicate on id=1 (10:30:00, 120.0, v=100) -> deduplicated out
        # - New row insert on id=3 (11:00:00, 300.0, v=103)
        # - In-order delete on id=2 (12:00:00, 200.0, v=102) -> deletes id=2
        batch1_data = [
            (1, 100.0, "2026-01-01 10:00:00", "update_preimage", 101, 1000),
            (1, 150.0, "2026-01-01 11:00:00", "update_postimage", 101, 1000),
            (1, 120.0, "2026-01-01 10:30:00", "update_postimage", 100, 900),
            (2, 200.0, "2026-01-01 12:00:00", "delete", 102, 2000),
            (3, 300.0, "2026-01-01 11:00:00", "insert", 103, 3000),
        ]
        batch1_df = spark.createDataFrame(batch1_data, schema=cdf_schema)
        merge_fn(batch1_df, 1)

        scd1_rows = {
            (r.id, r.amt, r.updated_at)
            for r in spark.sql(
                "SELECT id, amt, updated_at FROM default.test_cdc_scd1_orders"
            ).collect()
        }
        # id=1 updated to 150.0; id=2 deleted; id=3 inserted at 300.0
        assert scd1_rows == {
            (1, 150.0, "2026-01-01 11:00:00"),
            (3, 300.0, "2026-01-01 11:00:00"),
        }

        # Late upsert on id=1 at 09:00:00 (v=99) -> must be ignored by recency guard
        late_data = [(1, 999.0, "2026-01-01 09:00:00", "update_postimage", 99, 800)]
        late_df = spark.createDataFrame(late_data, schema=cdf_schema)
        merge_fn(late_df, 2)

        scd1_rows_after_late = {
            (r.id, r.amt, r.updated_at)
            for r in spark.sql(
                "SELECT id, amt, updated_at FROM default.test_cdc_scd1_orders"
            ).collect()
        }
        assert scd1_rows_after_late == scd1_rows

        # 2. SCD2 Delta Execution: in-order update, late update, brand new key, and delete event
        spark.sql("DROP TABLE IF EXISTS default.test_cdc_scd2_customers")
        spark.sql(f"""
            CREATE TABLE default.test_cdc_scd2_customers (
                id INT,
                name STRING,
                is_current BOOLEAN,
                valid_from STRING,
                valid_to STRING
            ) USING DELTA LOCATION '{tmp_scd2}'
        """)
        # Seed initial current row for id=1
        spark.sql("""
            INSERT INTO default.test_cdc_scd2_customers VALUES
            (1, 'Alice V1', true, '2026-01-01 10:00:00', NULL)
        """)

        tmpl_scd2 = doctor.generate_cdc_templates(
            target_table="default.test_cdc_scd2_customers",
            primary_keys=["id"],
            event_time_col="updated_at",
            value_cols=["name"],
            source_is_cdf=True,
            include_scd2=True,
        )

        def _run_scd2_sql() -> None:
            for stmt in tmpl_scd2.scd2_merge_sql.split(";"):
                cleaned = stmt.strip()
                if cleaned:
                    spark.sql(cleaned)

        # In-order update on id=1 at 12:00:00
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW source_cdc_batch AS
            SELECT 1 AS id, 'Alice V2' AS name, '2026-01-01 12:00:00' AS updated_at
        """)
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW cdc_deletes_batch AS
            SELECT CAST(NULL AS INT) AS id, CAST(NULL AS STRING) AS name, CAST(NULL AS STRING) AS updated_at
            WHERE 1 = 0
        """)
        _run_scd2_sql()

        current_id1 = spark.sql("""
            SELECT id, name, is_current, valid_from, valid_to
            FROM default.test_cdc_scd2_customers
            WHERE id = 1 AND is_current = true
        """).collect()
        assert len(current_id1) == 1
        assert current_id1[0].name == "Alice V2"
        assert current_id1[0].valid_from == "2026-01-01 12:00:00"
        assert current_id1[0].valid_to is None

        # Assert closed SCD2 row for Alice V1
        closed_id1 = spark.sql("""
            SELECT id, name, is_current, valid_from, valid_to
            FROM default.test_cdc_scd2_customers
            WHERE id = 1 AND is_current = false
        """).collect()
        assert len(closed_id1) == 1
        assert closed_id1[0].name == "Alice V1"
        assert closed_id1[0].valid_from == "2026-01-01 10:00:00"
        assert closed_id1[0].valid_to == "2026-01-01 12:00:00"

        # Late update on id=1 at 11:00:00 (< current valid_from 12:00:00)
        # Must be completely ignored; must NOT insert a duplicate is_current row!
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW source_cdc_batch AS
            SELECT 1 AS id, 'Alice Late' AS name, '2026-01-01 11:00:00' AS updated_at
        """)
        _run_scd2_sql()

        current_id1_after_late = spark.sql("""
            SELECT id, name, is_current, valid_from, valid_to
            FROM default.test_cdc_scd2_customers
            WHERE id = 1 AND is_current = true
        """).collect()
        assert len(current_id1_after_late) == 1
        assert current_id1_after_late[0].name == "Alice V2"

        # Insert brand new key: id=2 at 13:00:00
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW source_cdc_batch AS
            SELECT 2 AS id, 'Bob New' AS name, '2026-01-01 13:00:00' AS updated_at
        """)
        _run_scd2_sql()

        all_current = spark.sql("""
            SELECT id, name, is_current, valid_from, valid_to
            FROM default.test_cdc_scd2_customers
            WHERE is_current = true
            ORDER BY id
        """).collect()
        assert len(all_current) == 2
        assert [(r.id, r.name, r.is_current, r.valid_from) for r in all_current] == [
            (1, "Alice V2", True, "2026-01-01 12:00:00"),
            (2, "Bob New", True, "2026-01-01 13:00:00"),
        ]

        # SCD2 Delete: close active record for Bob New (id=2) at 14:00:00
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW source_cdc_batch AS
            SELECT CAST(NULL AS INT) AS id, CAST(NULL AS STRING) AS name, CAST(NULL AS STRING) AS updated_at
            WHERE 1 = 0
        """)
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW cdc_deletes_batch AS
            SELECT 2 AS id, 'Bob New' AS name, '2026-01-01 14:00:00' AS updated_at
        """)
        _run_scd2_sql()

        # id=2 must no longer have an active record
        current_id2 = spark.sql("""
            SELECT id, name, is_current, valid_from, valid_to
            FROM default.test_cdc_scd2_customers
            WHERE id = 2 AND is_current = true
        """).collect()
        assert len(current_id2) == 0

        # id=2 must have a closed record with valid_to = '2026-01-01 14:00:00'
        closed_id2 = spark.sql("""
            SELECT id, name, is_current, valid_from, valid_to
            FROM default.test_cdc_scd2_customers
            WHERE id = 2 AND is_current = false
        """).collect()
        assert len(closed_id2) == 1
        assert closed_id2[0].name == "Bob New"
        assert closed_id2[0].valid_from == "2026-01-01 13:00:00"
        assert closed_id2[0].valid_to == "2026-01-01 14:00:00"

        # Overall active rows should now only be Alice V2
        remaining_current = spark.sql("""
            SELECT id, name, is_current, valid_from
            FROM default.test_cdc_scd2_customers
            WHERE is_current = true
        """).collect()
        assert len(remaining_current) == 1
        assert remaining_current[0].id == 1
        assert remaining_current[0].name == "Alice V2"
    finally:
        spark.sql("DROP TABLE IF EXISTS default.test_cdc_scd1_orders")
        spark.sql("DROP TABLE IF EXISTS default.test_cdc_scd2_customers")
        shutil.rmtree(tmp_scd1, ignore_errors=True)
        shutil.rmtree(tmp_scd2, ignore_errors=True)
