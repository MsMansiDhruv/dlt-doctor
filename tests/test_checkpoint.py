"""Unit tests for CheckpointDoctor streaming checkpoint analyzer."""

from __future__ import annotations

from dlt_doctor.checkpoint import CheckpointDoctor, CheckpointIntegrityStatus


def test_checkpoint_healthy():
    doctor = CheckpointDoctor()
    report = doctor.audit_checkpoint_state("/Volumes/main/checkpoints/orders")

    assert report.status == CheckpointIntegrityStatus.HEALTHY
    assert len(report.uncommitted_batches) == 0


def test_checkpoint_uncommitted_batch():
    doctor = CheckpointDoctor()
    report = doctor.audit_checkpoint_state(
        "/Volumes/main/checkpoints/orders",
        latest_offset_batch=102,
        latest_commit_batch=100,
    )

    assert report.status == CheckpointIntegrityStatus.SUSPECT
    assert 101 in report.uncommitted_batches
    assert 102 in report.uncommitted_batches


def test_checkpoint_incompatible_state_keys():
    doctor = CheckpointDoctor()
    report = doctor.audit_checkpoint_state(
        "/Volumes/main/checkpoints/orders",
        has_stateful_operators=True,
        aggregation_keys_changed=True,
    )

    assert report.status == CheckpointIntegrityStatus.INCOMPATIBLE
    assert any("IllegalStateException" in issue for issue in report.issues)
    assert len(report.safe_migration_procedure) >= 3
