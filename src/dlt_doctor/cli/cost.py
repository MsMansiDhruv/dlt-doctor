"""Handler for 'dlt-doctor cost' subcommand."""

from __future__ import annotations

import argparse

from dlt_doctor.cost import CostDoctor


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("cost", help="Forecast Databricks DBU burn and cluster sizing")
    p.add_argument("--files-per-hour", type=int, required=True, help="Landing files per hour")
    p.add_argument("--avg-file-mb", type=float, default=10.0, help="Average file size in MB")
    p.add_argument("--batch", action="store_true", help="Triggered batch workload mode")
    p.add_argument("--hours", type=float, default=24.0, help="Active daily hours")
    p.add_argument("--joins", type=int, default=1, help="Number of shuffle joins")


def handle(args: argparse.Namespace) -> int:
    doctor = CostDoctor()
    forecast = doctor.forecast_pipeline_cost(
        files_per_hour=args.files_per_hour,
        avg_file_mb=args.avg_file_mb,
        is_continuous_streaming=not args.batch,
        active_hours_per_day=args.hours,
        shuffle_join_count=args.joins,
    )

    print("=" * 70)
    print(" DLT-Doctor Workload Sizing & DBU Cost Forecast (Estimator)")
    print("=" * 70)
    print(f"Workload Type:             {forecast.workload_type}")
    print(f"Daily Ingestion:           {forecast.daily_ingestion_gb:.1f} GB")
    print(f"Recommended Tier:          {forecast.recommended_tier}")
    print(f"Recommended Driver:        {forecast.recommended_cluster.driver_node_type}")
    print(
        f"Recommended Workers:       {forecast.recommended_cluster.worker_node_type} (min: {forecast.recommended_cluster.min_workers}, max: {forecast.recommended_cluster.max_workers})"
    )
    print(f"Serverless DLT DBUs/hr:    {forecast.serverless_dlt_dbus_per_hour:.2f}")
    print(f"Classic DLT DBUs/hr:       {forecast.classic_dlt_dbus_per_hour:.2f}")
    print("-" * 70)
    return 0
