"""Shared pytest fixtures for dlt-doctor test suite."""

from __future__ import annotations

import os
import sys
from collections.abc import Generator

import pytest

# Ensure Windows Java and Hadoop paths are configured before any PySpark / JVM invocation
if sys.platform == "win32":
    if not os.environ.get("JAVA_HOME") and os.path.exists(
        r"C:\PROGRA~2\Android\openjdk\jdk-17.0.14"
    ):
        os.environ["JAVA_HOME"] = r"C:\PROGRA~2\Android\openjdk\jdk-17.0.14"
    if not os.environ.get("HADOOP_HOME") and os.path.exists(r"C:\tmp\hadoop"):
        os.environ["HADOOP_HOME"] = r"C:\tmp\hadoop"
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
        from pyspark.sql import SparkSession
    except ImportError as exc:
        if require_spark:
            pytest.fail(f"PySpark is required (REQUIRE_SPARK=1) but not installed: {exc}")
        pytest.skip(f"PySpark not installed: {exc}")

    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("dlt-doctor-tests")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .getOrCreate()
        )
        yield session
        session.stop()
    except Exception as exc:
        if require_spark:
            pytest.fail(f"SparkSession initialization failed with REQUIRE_SPARK=1: {exc}")
        pytest.skip(f"SparkSession unavailable: {exc}")
