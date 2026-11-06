"""Tests for CdcDoctor streaming deduplication and SCD templates."""

from __future__ import annotations

import ast

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
    assert "DELETE FROM silver_orders" in template.scd1_merge_sql


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
    # Recency guard on SCD2 UPDATE
    assert "target.updated_at IS NULL OR staged.updated_at >= target.updated_at" in sql


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
    """AUTO CDC template must include dlt.create_streaming_table before dlt.apply_changes."""
    doctor = CdcDoctor()
    template = doctor.generate_cdc_templates(
        target_table="silver_orders",
        primary_keys=["order_id"],
        source_stream="bronze_orders_cdc",
        include_auto_cdc=True,
    )
    code = template.auto_cdc_code

    # Must be valid Python AST
    parsed_ast = ast.parse(code)
    assert parsed_ast is not None

    # Must declare create_streaming_table before apply_changes
    assert "dlt.create_streaming_table" in code
    assert "dlt.apply_changes" in code
    assert code.index("dlt.create_streaming_table") < code.index("dlt.apply_changes")


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
    )
    assert template.source_stream == "bronze_customers_cdc"
    assert "source='bronze_customers_cdc'" in template.auto_cdc_code
    assert "target.region = source.region" in template.scd1_merge_sql
    assert "email" in template.scd2_merge_sql


def test_cdc_cli_flags_source_partition_values() -> None:
    from unittest.mock import patch

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
def test_execute_cdc_scd_on_pyspark() -> None:
    """Execute generated CDC SCD1 and SCD2 statements against local PySpark Delta tables."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("PySpark not installed")

    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("CdcTest")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"Local PySpark/JVM not available: {exc}")

    try:
        # Create SCD1 target and batch
        spark.sql(
            "CREATE TABLE IF NOT EXISTS default.cdc_scd1_tgt (id INT, amt DOUBLE) USING DELTA"
        )
        spark.sql("CREATE OR REPLACE TEMP VIEW cdc_deletes_batch AS SELECT 1 AS id")
        spark.sql("CREATE OR REPLACE TEMP VIEW source_cdc_batch AS SELECT 2 AS id, 100.0 AS amt")
        doctor = CdcDoctor()
        tmpl = doctor.generate_cdc_templates("default.cdc_scd1_tgt", ["id"])
        for stmt in tmpl.scd1_merge_sql.split(";"):
            cleaned = stmt.strip()
            if cleaned:
                spark.sql(cleaned)

        # Create SCD2 target and execute SCD2 MERGE
        spark.sql("""
            CREATE TABLE IF NOT EXISTS default.cdc_scd2_tgt (
                id INT,
                amt DOUBLE,
                updated_at TIMESTAMP,
                is_current BOOLEAN,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP
            ) USING DELTA
        """)
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW source_cdc_batch AS
            SELECT 2 AS id, 150.0 AS amt, current_timestamp() AS updated_at
        """)
        tmpl_scd2 = doctor.generate_cdc_templates(
            "default.cdc_scd2_tgt", ["id"], event_time_col="updated_at", value_cols=["amt"]
        )
        spark.sql(tmpl_scd2.scd2_merge_sql)
    finally:
        spark.stop()
