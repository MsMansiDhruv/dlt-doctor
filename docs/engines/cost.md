# DBU Cost & Sizing Forecast (`CostDoctor`)

`CostDoctor` models Databricks DBU burn comparing Serverless DLT against Classic Compute pools based on pipeline ingestion profiles.

## Sizing Model

- Calculates daily volume in GB from landing file velocity and average file size.
- Models continuous streaming vs triggered batch workloads.
- Evaluates cost trade-offs between Serverless DLT (instant startup, per-second billing) and Classic Compute (reserved pool autoscaling).
- Recommends worker instance types and min/max cluster core counts.

## Python API Usage

```python
from dlt_doctor import CostDoctor

doctor = CostDoctor()
forecast = doctor.forecast_pipeline_cost(
    landing_files_per_hour=25000,
    avg_file_size_mb=12.0,
    is_batch=False,
    active_daily_hours=24.0,
    shuffle_joins_count=2,
)

print(f"Daily Volume: {forecast.daily_data_gb:.1f} GB")
print(f"Classic DLT: ~{forecast.classic_dbu_per_hour:.1f} DBU/hr")
print(f"Serverless DLT: ~{forecast.serverless_dbu_per_hour:.1f} DBU/hr")
print(f"Recommended Compute: {forecast.recommended_tier.value}")
```
