# Shuffle Spill Partition Tuning (`SpillDoctor`)

The default PySpark shuffle partition count (`spark.sql.shuffle.partitions = 200`) is either too small (causing high-memory tasks to spill gigabytes to disk) or too large (causing scheduling overhead on small datasets).

## Optimal Partition Formula

`SpillDoctor` balances partitions such that target partition size stays between **100MB and 200MB**:
$$\text{partitions} = \max(\text{cores} \times 2, \; \text{ceil}(\text{dataset\_mb} / 128))$$

## Python API Usage

```python
from dlt_doctor import SpillDoctor

doctor = SpillDoctor()
advice = doctor.tune_shuffle_partitions(
    dataset_size_gb=50.0,
    worker_cores_total=16,
)

print(f"Optimal Shuffle Partitions: {advice.optimal_partitions}")
print(f"Set: spark.conf.set('spark.sql.shuffle.partitions', {advice.optimal_partitions})")
```
