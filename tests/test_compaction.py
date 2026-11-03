"""Tests for CompactionDoctor small-file analysis."""

from dlt_doctor.compaction import CompactionDoctor


def test_compaction_doctor_detects_small_file_debt() -> None:
    doctor = CompactionDoctor()
    # 25 files, 20 of them are tiny (1MB each), 5 are 128MB
    tiny_size = 1 * 1024 * 1024
    large_size = 128 * 1024 * 1024
    sizes = [tiny_size] * 20 + [large_size] * 5

    report = doctor.analyze_file_distribution(
        table_name="main.silver.orders",
        file_sizes_bytes=sizes,
        small_file_threshold_mb=32,
    )

    assert report.has_compaction_debt is True
    assert report.small_file_pct == 80.0
    assert report.total_files == 25
    assert "OPTIMIZE main.silver.orders" in report.recommended_optimize_sql
    assert (
        "delta.autoOptimize.autoCompact = true" in report.recommended_tblproperties_sql
    )


def test_compaction_doctor_healthy_table() -> None:
    doctor = CompactionDoctor()
    large_size = 128 * 1024 * 1024
    sizes = [large_size] * 20

    report = doctor.analyze_file_distribution(
        table_name="main.gold.customers",
        file_sizes_bytes=sizes,
        small_file_threshold_mb=32,
    )

    assert report.has_compaction_debt is False
    assert report.small_file_pct == 0.0
    assert len(report.findings) == 0
