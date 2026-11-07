"""Shared pytest fixtures for dlt-doctor test suite."""

from __future__ import annotations

import os
import sys
from collections.abc import Generator

import pytest

# Ensure Windows Java and Hadoop paths from environment are present in PATH for PySpark / JVM invocation
if sys.platform == "win32":
    if os.environ.get("JAVA_HOME"):
        java_bin = os.path.join(os.environ["JAVA_HOME"], "bin")
        if java_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{java_bin};{os.environ.get('PATH', '')}"
    if os.environ.get("HADOOP_HOME"):
        hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{hadoop_bin};{os.environ.get('PATH', '')}"


@pytest.fixture(scope="session")
def spark() -> Generator:
    """Session-scoped local SparkSession configured with Delta Lake."""
    require_spark = os.environ.get("REQUIRE_SPARK") == "1"

    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
    except ImportError as exc:
        if require_spark:
            pytest.fail(
                f"PySpark or Delta Lake is required (REQUIRE_SPARK=1) but not installed: {exc}"
            )
        pytest.skip(f"PySpark or Delta Lake not installed: {exc}")

    try:
        builder = (
            SparkSession.builder.master("local[1]")
            .appName("dlt-doctor-tests")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
        )
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as exc:
        if require_spark:
            pytest.fail(f"SparkSession initialization failed with REQUIRE_SPARK=1: {exc}")
        pytest.skip(f"SparkSession unavailable: {exc}")

    try:
        yield session
    finally:
        session.stop()
