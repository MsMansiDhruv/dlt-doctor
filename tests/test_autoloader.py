"""Tests for AutoLoaderDoctor streaming validation."""

from dlt_doctor.autoloader import AutoLoaderConfig, AutoLoaderDoctor


def test_autoloader_config_to_spark_options() -> None:
    config = AutoLoaderConfig(
        format="json",
        source_path="/Volumes/main/bronze/orders",
        schema_location="/Volumes/main/checkpoints/orders_schema",
        schema_evolution_mode="rescue",
        max_files_per_trigger=500,
    )
    opts = config.to_spark_options()
    assert opts["cloudFiles.format"] == "json"
    assert opts["cloudFiles.schemaLocation"] == "/Volumes/main/checkpoints/orders_schema"
    assert opts["cloudFiles.schemaEvolutionMode"] == "rescue"
    assert opts["cloudFiles.maxFilesPerTrigger"] == "500"


def test_validate_options_missing_schema_location() -> None:
    doctor = AutoLoaderDoctor()
    bad_opts = {"cloudFiles.format": "csv"}
    findings = doctor.validate_options(bad_opts)

    assert any(f.rule_id == "AUTOLOADER-001" for f in findings)


def test_diagnose_stream_progress_backpressure() -> None:
    doctor = AutoLoaderDoctor()
    # Ingestion rate 1000 rows/s vs processing rate 200 rows/s
    progress = [
        {
            "inputRowsPerSecond": 1000.0,
            "processedRowsPerSecond": 200.0,
            "numInputRows": 5000,
        },
        {
            "inputRowsPerSecond": 1200.0,
            "processedRowsPerSecond": 250.0,
            "numInputRows": 6000,
        },
    ]
    summary = doctor.diagnose_stream_progress(progress)
    assert summary.is_backpressured is True
    assert any(f.rule_id == "AUTOLOADER-004" for f in summary.findings)
