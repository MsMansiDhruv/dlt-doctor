"""Unit tests for UcDoctor Unity Catalog migration and DBFS scanner."""

from __future__ import annotations

from dlt_doctor.uc import UcDoctor, UcMigrationSeverity


def test_detect_dbfs_mount_paths():
    doctor = UcDoctor()
    code = """
raw_df = spark.read.json("/mnt/landing/raw_orders/2026")
checkpoint_path = "dbfs:/mnt/checkpoints/orders"
"""
    plan = doctor.analyze_source_code(code, target_catalog="prod_lakehouse", target_schema="bronze")

    assert len(plan.findings) >= 2
    assert any(
        f.rule_id == "UC-001" and f.severity == UcMigrationSeverity.CRITICAL for f in plan.findings
    )
    assert "/Volumes/prod_lakehouse/bronze/landing/raw_orders/2026" in plan.path_mapping.values()
    assert any(
        "CREATE EXTERNAL VOLUME IF NOT EXISTS prod_lakehouse.bronze.landing" in ddl
        for ddl in plan.generated_volume_ddl
    )


def test_detect_legacy_table_references():
    doctor = UcDoctor()
    code = """
silver_df = spark.table("orders_silver")
fact_df = spark.read.table("gold.fact_orders")
"""
    plan = doctor.analyze_source_code(code, target_catalog="prod_lakehouse", target_schema="silver")

    assert any(f.rule_id == "UC-003" for f in plan.findings)
    assert any(f.rule_id == "UC-004" for f in plan.findings)
    assert plan.table_mapping.get("orders_silver") == "prod_lakehouse.silver.orders_silver"
    assert plan.table_mapping.get("gold.fact_orders") == "prod_lakehouse.gold.fact_orders"


def test_clean_unity_catalog_code():
    doctor = UcDoctor()
    code = """
df = spark.read.table("prod_lakehouse.silver.orders")
vol_path = "/Volumes/prod_lakehouse/bronze/landing/files"
"""
    plan = doctor.analyze_source_code(code)
    assert len(plan.findings) == 0
