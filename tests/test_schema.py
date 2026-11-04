"""Unit tests for SchemaDoctor schema drift and evolution advisor."""

from __future__ import annotations

from dlt_doctor.schema import SchemaDoctor, SchemaDriftSeverity


def test_schema_safe_additions():
    doctor = SchemaDoctor()
    existing = {"id": "bigint", "email": "string"}
    incoming = {
        "id": "bigint",
        "email": "string",
        "phone": "string",
        "status": "string",
    }

    report = doctor.compare_schemas("main.silver.customers", existing, incoming)

    assert not report.has_breaking_changes
    additions = [c for c in report.changes if c.severity == SchemaDriftSeverity.SAFE_ADDITION]
    assert len(additions) == 2
    assert len(report.generated_ddl) == 2
    assert "ALTER TABLE main.silver.customers ADD COLUMNS (phone string);" in report.generated_ddl


def test_schema_safe_widening():
    doctor = SchemaDoctor()
    existing = {"quantity": "int", "price": "float"}
    incoming = {"quantity": "bigint", "price": "double"}

    report = doctor.compare_schemas("main.silver.orders", existing, incoming)

    assert not report.has_breaking_changes
    widenings = [c for c in report.changes if c.severity == SchemaDriftSeverity.SAFE_WIDENING]
    assert len(widenings) == 2


def test_schema_breaking_mutation():
    doctor = SchemaDoctor()
    existing = {"order_timestamp": "timestamp", "amount": "decimal(18,2)"}
    incoming = {"order_timestamp": "int", "amount": "string"}

    report = doctor.compare_schemas("main.silver.orders", existing, incoming)

    assert report.has_breaking_changes
    breaking = [c for c in report.changes if c.severity == SchemaDriftSeverity.BREAKING]
    assert len(breaking) == 2
    assert (
        report.recommended_spark_options.get("spark.databricks.delta.schema.autoMerge.enabled")
        == "false"
    )
