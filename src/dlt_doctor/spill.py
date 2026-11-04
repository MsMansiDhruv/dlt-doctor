"""PySpark shuffle spill minimization and dynamic shuffle partition tuner."""

from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass
class ShuffleTuningAdvice:
    """Calculated partition counts and Spark session configurations to prevent disk spills."""

    dataset_size_gb: float
    total_cluster_cores: int
    default_partitions: int
    optimal_partitions: int
    target_partition_size_mb: int
    has_spill_risk: bool
    recommended_configs: dict[str, str] = field(default_factory=dict)
    rationale: str = ""


class SpillDoctor:
    """Calculates mathematically optimal shuffle partitions to eliminate disk spill."""

    IDEAL_PARTITION_SIZE_MB = 128  # Target task partition size in Spark memory
    DEFAULT_SPARK_SHUFFLE_PARTITIONS = 200

    def calculate_optimal_partitions(
        self,
        dataset_size_gb: float,
        total_cluster_cores: int = 16,
        target_partition_size_mb: int = 128,
    ) -> ShuffleTuningAdvice:
        """Calculate dynamic partition sizing replacing static default 200."""
        dataset_size_mb = dataset_size_gb * 1024.0
        data_based_partitions = math.ceil(dataset_size_mb / target_partition_size_mb)

        # Ensure at least 2 tasks per core to maximize parallelism and prevent idle cores
        core_based_partitions = total_cluster_cores * 2
        optimal = max(core_based_partitions, data_based_partitions)

        # Assess spill risk under default 200 partitions
        default_partition_size_mb = dataset_size_mb / self.DEFAULT_SPARK_SHUFFLE_PARTITIONS
        spill_risk = (
            default_partition_size_mb > 500.0
        )  # Partitions > 500MB risk memory spill to disk

        configs: dict[str, str] = {
            "spark.sql.shuffle.partitions": str(optimal),
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": f"{target_partition_size_mb * 1024 * 1024}",
        }

        # For large shuffle volumes (> 200 GB), recommend off-heap memory
        if dataset_size_gb > 200.0:
            configs["spark.memory.offHeap.enabled"] = "true"
            configs["spark.memory.offHeap.size"] = "8g"

        if spill_risk:
            rationale = (
                f"Dataset size ({dataset_size_gb:.1f} GB) under default 200 partitions creates ~{default_partition_size_mb:.0f} MB "
                f"partitions, causing severe executor disk spills. Tuning to {optimal} partitions guarantees task partitions "
                f"remain around ~{target_partition_size_mb} MB."
            )
        elif default_partition_size_mb < 30.0 and dataset_size_gb > 0:
            rationale = (
                f"Dataset size ({dataset_size_gb:.1f} GB) under default 200 partitions creates tiny ~{default_partition_size_mb:.1f} MB "
                f"partitions, creating excessive task scheduling latency. Tuning to {optimal} partitions optimizes throughput."
            )
        else:
            rationale = (
                f"Configuring {optimal} shuffle partitions balances cluster core parallelism ({total_cluster_cores} cores) "
                f"with ~{target_partition_size_mb} MB target task sizing."
            )

        return ShuffleTuningAdvice(
            dataset_size_gb=dataset_size_gb,
            total_cluster_cores=total_cluster_cores,
            default_partitions=self.DEFAULT_SPARK_SHUFFLE_PARTITIONS,
            optimal_partitions=optimal,
            target_partition_size_mb=target_partition_size_mb,
            has_spill_risk=spill_risk,
            recommended_configs=configs,
            rationale=rationale,
        )
