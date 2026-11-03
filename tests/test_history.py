"""Unit tests for HistoryDoctor Delta transaction log auditor."""

from __future__ import annotations

from dlt_doctor.history import HistoryDoctor


def test_history_normal_commit_velocity():
    doctor = HistoryDoctor()
    report = doctor.evaluate_commit_velocity("main.silver.orders", commits_per_hour=10)

    assert not report.has_log_bloat_risk
    assert report.recommended_checkpoint_interval == 10


def test_history_high_streaming_commit_velocity():
    doctor = HistoryDoctor()
    report = doctor.evaluate_commit_velocity("main.silver.orders", commits_per_hour=360)

    assert report.has_log_bloat_risk
    assert report.recommended_checkpoint_interval == 100
    assert len(report.warnings) > 0
    assert "delta.checkpointInterval" in report.recommended_tblproperties_ddl
