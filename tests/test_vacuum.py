"""Unit tests for VacuumDoctor Delta retention safety auditor."""

from __future__ import annotations

from dlt_doctor.vacuum import VacuumDoctor, VacuumSafetyLevel


def test_vacuum_safe_default():
    doctor = VacuumDoctor()
    report = doctor.evaluate_vacuum_safety("main.silver.orders", requested_retention_hours=168)
    assert report.safety_level == VacuumSafetyLevel.SAFE
    assert report.safe_retention_hours == 168
    assert "VACUUM main.silver.orders RETAIN 168 HOURS;" in report.recommended_vacuum_sql


def test_vacuum_dangerous_low_retention_with_active_readers():
    doctor = VacuumDoctor()
    report = doctor.evaluate_vacuum_safety(
        "main.silver.orders",
        requested_retention_hours=24,
        active_streaming_readers=2,
    )
    assert report.safety_level == VacuumSafetyLevel.DANGEROUS
    assert any("FileNotFoundException" in w for w in report.warnings)


def test_vacuum_cdf_retention_warning():
    doctor = VacuumDoctor()
    report = doctor.evaluate_vacuum_safety(
        "main.silver.orders",
        requested_retention_hours=72,
        cdf_enabled=True,
        active_streaming_readers=0,
    )
    assert report.safety_level == VacuumSafetyLevel.WARNING
    assert any("CDF" in w for w in report.warnings)
