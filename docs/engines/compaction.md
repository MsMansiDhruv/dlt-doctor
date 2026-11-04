# Compaction Debt (`CompactionDoctor`)

High-velocity streaming ingestion creates thousands of small Delta files (under 32MB). This degrades driver partition planning and query scan times by up to **18.8x**.

## Compaction Debt Analysis

`CompactionDoctor` analyzes file size distributions from Delta log metadata:
- Measures percentage of small files (< 32MB).
- Identifies compaction debt threshold (>= 40% small files across >= 20 files).
- Generates targeted `OPTIMIZE` and `delta.autoOptimize` table properties.

## Python API Usage

```python
from dlt_doctor import CompactionDoctor

doctor = CompactionDoctor()
report = doctor.analyze_file_distribution(
    table_name="main.silver.orders",
    file_sizes_bytes=[8 * 1024 * 1024] * 200,  # 200 8MB files
)

if report.has_compaction_debt:
    print(f"Small File Percentage: {report.small_file_pct}%")
    print(report.recommended_optimize_sql)
    print(report.recommended_tblproperties_sql)
```
