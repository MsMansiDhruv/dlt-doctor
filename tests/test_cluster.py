"""Unit tests for ClusterDoctor Liquid Clustering advisor."""

from __future__ import annotations

from dlt_doctor.cluster import ClusterDoctor, ColumnProfile


def test_select_optimal_cluster_keys():
    doctor = ClusterDoctor()
    columns = [
        ColumnProfile(
            name="customer_id",
            data_type="string",
            distinct_count=25000,
            filter_frequency_score=0.9,
        ),
        ColumnProfile(
            name="order_date",
            data_type="date",
            distinct_count=1800,
            filter_frequency_score=0.95,
        ),
        ColumnProfile(
            name="country_code",
            data_type="string",
            distinct_count=120,
            filter_frequency_score=0.7,
        ),
        ColumnProfile(
            name="order_uuid",
            data_type="string",
            distinct_count=9_990_000,
            filter_frequency_score=0.1,
        ),  # near unique
        ColumnProfile(
            name="is_active",
            data_type="boolean",
            distinct_count=2,
            filter_frequency_score=0.5,
        ),  # too low
    ]

    rec = doctor.advise_clustering("main.gold.fact_orders", columns, total_rows=10_000_000)

    # Best candidates should be order_date, customer_id, country_code
    assert "order_date" in rec.recommended_keys
    assert "customer_id" in rec.recommended_keys
    assert "order_uuid" not in rec.recommended_keys
    assert "is_active" not in rec.recommended_keys
    assert len(rec.recommended_keys) <= 4
    assert "ALTER TABLE main.gold.fact_orders CLUSTER BY" in rec.alter_cluster_ddl
    assert "OPTIMIZE main.gold.fact_orders;" in rec.optimize_ddl


def test_reject_complex_types():
    doctor = ClusterDoctor()
    columns = [
        ColumnProfile(
            name="attributes_map",
            data_type="map<string,string>",
            distinct_count=500,
            filter_frequency_score=0.8,
        ),
    ]
    rec = doctor.advise_clustering("main.gold.events", columns, total_rows=10_000)
    assert any("unsupported" in reason.lower() for _, reason in rec.rejected_keys)
