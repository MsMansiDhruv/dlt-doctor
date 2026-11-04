"""Benchmark suite for dlt-doctor static analysis and AST evaluation."""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

from dlt_doctor.catalyst import PlanDoctor
from dlt_doctor.heal import auto_heal_source
from dlt_doctor.scanner import DirectoryScanner


def generate_sample_pipeline(function_count: int = 50) -> str:
    """Generate synthetic PySpark / Lakeflow pipeline source code."""
    blocks = [
        "import dlt",
        "from pyspark.sql import functions as F",
        "",
    ]
    for i in range(function_count):
        blocks.extend(
            [
                f"@dlt.table(name='table_{i}')",
                f"@dlt.expect_or_drop('rule_{i}_valid', 'amount > {i}')",
                f"def pipeline_stage_{i}():",
                f"    return spark.readStream.table('source_stage_{i}')",
                "",
            ]
        )
    return "\n".join(blocks)


def benchmark_analyzer(iterations: int = 5) -> dict[str, float]:
    """Measure raw AST analysis throughput."""
    doctor = PlanDoctor()
    source_50 = generate_sample_pipeline(50)
    line_count = len(source_50.splitlines())

    durations: list[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        findings = doctor.analyze_source_code(source_50)
        t1 = time.perf_counter()
        durations.append(t1 - t0)

    avg_duration_ms = (sum(durations) / len(durations)) * 1000.0
    throughput_lines_sec = line_count / (sum(durations) / len(durations))

    return {
        "lines": line_count,
        "findings": len(findings),
        "avg_duration_ms": avg_duration_ms,
        "throughput_lines_sec": throughput_lines_sec,
    }


def benchmark_heal(iterations: int = 5) -> dict[str, float]:
    """Measure single-pass quarantine synthesis latency."""
    source = generate_sample_pipeline(10)
    durations: list[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        res = auto_heal_source(source)
        t1 = time.perf_counter()
        durations.append(t1 - t0)

    avg_ms = (sum(durations) / len(durations)) * 1000.0
    return {
        "healed_count": len(res.tables_healed or []) if res else 0,
        "avg_duration_ms": avg_ms,
    }


def benchmark_directory_scan(file_count: int = 20) -> dict[str, float]:
    """Measure directory scan throughput across multiple files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        dir_path = Path(tmp_dir)
        for i in range(file_count):
            (dir_path / f"pipeline_{i}.py").write_text(
                generate_sample_pipeline(10), encoding="utf-8"
            )

        scanner = DirectoryScanner()
        t0 = time.perf_counter()
        report = scanner.scan_directory(dir_path)
        t1 = time.perf_counter()

        duration = t1 - t0
        files_per_sec = file_count / duration if duration > 0 else 0.0

        total_findings = sum(len(fr.findings) for fr in report.file_reports)
        return {
            "file_count": file_count,
            "total_findings": total_findings,
            "duration_ms": duration * 1000.0,
            "files_per_sec": files_per_sec,
        }


def run_all_benchmarks() -> None:
    print("=" * 60)
    print(" Running dlt-doctor Performance Benchmarks")
    print("=" * 60)

    res_analyzer = benchmark_analyzer()
    print("1. AST Code Analysis (50 pipeline functions):")
    print(f"   Lines Analyzed:  {res_analyzer['lines']}")
    print(f"   Findings Found:  {res_analyzer['findings']}")
    print(f"   Latency:         {res_analyzer['avg_duration_ms']:.2f} ms")
    print(f"   Throughput:      {res_analyzer['throughput_lines_sec']:,.0f} lines/sec\n")

    res_heal = benchmark_heal()
    print("2. Auto-Heal Diff Synthesis (10 pipeline functions):")
    print(f"   Tables Healed:   {res_heal['healed_count']}")
    print(f"   Latency:         {res_heal['avg_duration_ms']:.2f} ms\n")

    res_dir = benchmark_directory_scan(25)
    print("3. Multi-file Directory Scanner (25 files):")
    print(f"   Files Scanned:   {res_dir['file_count']}")
    print(f"   Total Findings:  {res_dir['total_findings']}")
    print(f"   Total Duration:  {res_dir['duration_ms']:.2f} ms")
    print(f"   Scan Rate:       {res_dir['files_per_sec']:.1f} files/sec")
    print("=" * 60)


if __name__ == "__main__":
    run_all_benchmarks()
