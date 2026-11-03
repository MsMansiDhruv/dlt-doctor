"""Unit tests for SpillDoctor shuffle partition optimization."""

from __future__ import annotations

from dlt_doctor.spill import SpillDoctor


def test_spill_small_dataset():
    doctor = SpillDoctor()
    advice = doctor.calculate_optimal_partitions(
        dataset_size_gb=2.0, total_cluster_cores=8
    )

    assert not advice.has_spill_risk
    assert advice.optimal_partitions == 16  # 8 cores * 2


def test_spill_large_dataset_risk():
    doctor = SpillDoctor()
    advice = doctor.calculate_optimal_partitions(
        dataset_size_gb=400.0, total_cluster_cores=32
    )

    assert advice.has_spill_risk
    assert advice.optimal_partitions > 200
    assert "spark.memory.offHeap.enabled" in advice.recommended_configs
