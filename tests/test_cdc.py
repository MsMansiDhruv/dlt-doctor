"""Tests for CdcDoctor streaming deduplication and SCD templates."""

from dlt_doctor.cdc import CdcDoctor


def test_cdc_doctor_templates() -> None:
    doctor = CdcDoctor()
    template = doctor.generate_cdc_templates(
        target_table="main.silver.dim_merchants",
        primary_keys=["merchant_id"],
        event_time_col="updated_at",
        sequence_col="_cdc_seq_id",
        partition_col="region",
    )

    assert "MERGE INTO main.silver.dim_merchants" in template.scd1_merge_sql
    assert "target.merchant_id = source.merchant_id" in template.scd1_merge_sql
    assert "target.region = source.region" in template.scd1_merge_sql
    assert "Window.partitionBy(['merchant_id'])" in template.microbatch_dedup_code
    assert "row_number().over(window_spec)" in template.microbatch_dedup_code
    assert "is_current = false" in template.scd2_merge_sql
