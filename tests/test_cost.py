"""Unit tests for CostDoctor DBU forecasting and cluster sizing advisor."""

from __future__ import annotations

from dlt_doctor.cost import CostDoctor


def test_forecast_continuous_streaming_cost():
    doctor = CostDoctor()
    forecast = doctor.forecast_pipeline_cost(
        files_per_hour=25000,
        avg_file_mb=10.0,
        is_continuous_streaming=True,
    )

    assert forecast.daily_ingestion_gb > 0
    assert forecast.recommended_cluster.min_workers >= 1
    assert forecast.classic_dlt_monthly_dbus > 0
    assert forecast.serverless_dlt_monthly_dbus > 0


def test_forecast_batch_cost():
    doctor = CostDoctor()
    forecast = doctor.forecast_pipeline_cost(
        files_per_hour=5000,
        avg_file_mb=5.0,
        is_continuous_streaming=False,
        active_hours_per_day=2.0,
    )

    assert forecast.workload_type == "Triggered Batch"
    assert forecast.classic_dlt_monthly_dbus < 10000.0
