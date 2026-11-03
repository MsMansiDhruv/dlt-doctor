"""Tests for RestatementDoctor blast radius and Delta restatement."""

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
