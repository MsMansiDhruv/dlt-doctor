# Auto Loader Ingestion (`AutoLoaderDoctor`)

Databricks Auto Loader (`cloudFiles`) incrementally ingests landing files. When files arrive faster than worker processing rates, streams build backlogs and introduce latency SLA breaches.

## Backpressure Diagnostics

`AutoLoaderDoctor` monitors `StreamingQueryProgress` metrics:
- Compares `inputRowsPerSecond` vs `processedRowsPerSecond`.
- Detects streaming backpressure conditions.
- Estimates backpressure row backlog and advises cluster scale-out.

## Python API Usage

```python
from dlt_doctor import AutoLoaderDoctor

doctor = AutoLoaderDoctor()
health = doctor.diagnose_stream_progress({
    "numInputRows": 50000,
    "inputRowsPerSecond": 1200.0,
    "processedRowsPerSecond": 450.0,  # Stream falling behind
})

if health.is_backpressured:
    print(f"Backpressure detected! Estimated backlog: ~{health.estimated_backlog_rows:,} rows.")
    print("Action: Increase maxFilesPerTrigger or enable cluster autoscaling.")
```
