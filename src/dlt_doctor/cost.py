"""Databricks DBU cost forecasting and cluster sizing advisor."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ClusterSizingProfile:
    """Recommended Databricks cluster sizing configuration."""

    driver_node_type: str
    worker_node_type: str
    min_workers: int
    max_workers: int
    driver_memory_gb: int
    worker_memory_gb: int


@dataclass
class DbuCostForecast:
    """Estimated DBU burn rate and cost comparison for Databricks execution tiers."""

    workload_type: str
    daily_ingestion_gb: float
    recommended_cluster: ClusterSizingProfile
    classic_dlt_dbus_per_hour: float
    classic_dlt_monthly_dbus: float
    serverless_dlt_dbus_per_hour: float
    serverless_dlt_monthly_dbus: float
    recommended_tier: str
    cost_saving_recommendations: list[str]


class CostDoctor:
    """Estimates DBU consumption and recommends cluster sizing to prevent over-provisioning."""

    DBU_RATE_CLASSIC_PRO = 0.25  # DBUs per vCPU-hour
    DBU_RATE_SERVERLESS = 0.40  # Serverless DLT DBUs per unit-hour

    def forecast_pipeline_cost(
        self,
        files_per_hour: int,
        avg_file_mb: float = 10.0,
        is_continuous_streaming: bool = True,
        active_hours_per_day: float = 24.0,
        shuffle_join_count: int = 1,
    ) -> DbuCostForecast:
        """Calculate recommended cluster size and compare Serverless vs Classic DLT DBU costs."""
        hourly_mb = files_per_hour * avg_file_mb
        daily_gb = (hourly_mb * active_hours_per_day) / 1024.0

        # Sizing heuristic:
        # Worker count scales with throughput and shuffle complexity
        throughput_mb_sec = hourly_mb / 3600.0
        base_workers = max(1, int(throughput_mb_sec / 25.0) + (shuffle_join_count // 2))
        max_workers = max(4, base_workers * 3)

        if daily_gb < 100.0:
            driver_type = "Standard_D4ds_v5 (16 GB, 4 vCPUs)"
            worker_type = "Standard_D4ds_v5 (16 GB, 4 vCPUs)"
            driver_mem = 16
            worker_mem = 16
            cores_per_worker = 4
        elif daily_gb < 1000.0:
            driver_type = "Standard_D8ds_v5 (32 GB, 8 vCPUs)"
            worker_type = "Standard_D8ds_v5 (32 GB, 8 vCPUs)"
            driver_mem = 32
            worker_mem = 32
            cores_per_worker = 8
        else:
            driver_type = "Standard_D16ds_v5 (64 GB, 16 vCPUs)"
            worker_type = "Standard_D16ds_v5 (64 GB, 16 vCPUs)"
            driver_mem = 64
            worker_mem = 64
            cores_per_worker = 16

        profile = ClusterSizingProfile(
            driver_node_type=driver_type,
            worker_node_type=worker_type,
            min_workers=base_workers,
            max_workers=max_workers,
            driver_memory_gb=driver_mem,
            worker_memory_gb=worker_mem,
        )

        total_cores = (cores_per_worker * base_workers) + 4  # workers + driver
        classic_dbu_per_hour = total_cores * self.DBU_RATE_CLASSIC_PRO

        if is_continuous_streaming:
            # Continuous streaming runs 24/7
            classic_monthly_dbus = classic_dbu_per_hour * 24.0 * 30.5
            # Serverless scales to 0 during idle streaming pauses
            effective_serverless_duty_cycle = (
                0.65  # 35% idle time saved by serverless instant pause
            )
            serverless_dbu_per_hour = (
                total_cores * self.DBU_RATE_SERVERLESS
            ) * effective_serverless_duty_cycle
            serverless_monthly_dbus = serverless_dbu_per_hour * 24.0 * 30.5
        else:
            # Triggered batch mode
            active_daily_hours = min(24.0, max(0.5, active_hours_per_day))
            classic_monthly_dbus = classic_dbu_per_hour * active_daily_hours * 30.5
            serverless_dbu_per_hour = total_cores * self.DBU_RATE_SERVERLESS
            serverless_monthly_dbus = (
                serverless_dbu_per_hour * active_daily_hours * 30.5
            )

        if serverless_monthly_dbus < classic_monthly_dbus:
            rec_tier = "Serverless DLT"
        else:
            rec_tier = "Classic Compute DLT (Pro tier)"

        recommendations = [
            f"Recommended cluster: {base_workers}x {worker_type} (autoscaling up to {max_workers}x).",
        ]
        if is_continuous_streaming and throughput_mb_sec < 10.0:
            recommendations.append(
                "Ingestion rate is under 10 MB/s. Consider switching from continuous streaming to "
                "Trigger(availableNow=True) hourly batches to eliminate 24/7 idle cluster DBU spend."
            )
        if serverless_monthly_dbus < classic_monthly_dbus:
            recommendations.append(
                f"Serverless DLT reduces monthly DBU cost by ~{abs(classic_monthly_dbus - serverless_monthly_dbus):,.0f} DBUs "
                "by eliminating cluster cold-start provisioning delays and idle worker allocation."
            )

        return DbuCostForecast(
            workload_type="Continuous Streaming"
            if is_continuous_streaming
            else "Triggered Batch",
            daily_ingestion_gb=round(daily_gb, 2),
            recommended_cluster=profile,
            classic_dlt_dbus_per_hour=round(classic_dbu_per_hour, 2),
            classic_dlt_monthly_dbus=round(classic_monthly_dbus, 1),
            serverless_dlt_dbus_per_hour=round(serverless_dbu_per_hour, 2),
            serverless_dlt_monthly_dbus=round(serverless_monthly_dbus, 1),
            recommended_tier=rec_tier,
            cost_saving_recommendations=recommendations,
        )
