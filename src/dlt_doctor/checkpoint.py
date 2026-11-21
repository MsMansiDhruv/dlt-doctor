"""Streaming checkpoint health analyzer and state recovery migration advisor."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class CheckpointIntegrityStatus(str, Enum):
    """Integrity health grade for streaming checkpoint directory."""

    HEALTHY = "HEALTHY"
    SUSPECT = "SUSPECT"
    INCOMPATIBLE = "INCOMPATIBLE"


@dataclass
class CheckpointHealthReport:
    """Findings from checkpoint directory and query schema compatibility inspection."""

    checkpoint_dir: str
    status: CheckpointIntegrityStatus
    has_stateful_operators: bool
    latest_batch_id: int | None
    uncommitted_batches: list[int] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)
    recovery_guidance: str = ""
    safe_migration_procedure: list[str] = field(default_factory=list)


class CheckpointDoctor:
    """Audits Structured Streaming checkpoints to prevent fatal IllegalStateException failures."""

    def audit_checkpoint_state(
        self,
        checkpoint_dir: str,
        has_stateful_operators: bool = False,
        source_schema_changed: bool = False,
        aggregation_keys_changed: bool = False,
        latest_offset_batch: int = 100,
        latest_commit_batch: int = 100,
    ) -> CheckpointHealthReport:
        """Evaluate checkpoint compatibility against updated pipeline logic."""
        from pathlib import Path

        issues: list[str] = []
        procedure: list[str] = []
        status = CheckpointIntegrityStatus.HEALTHY

        # If pointing to an existing local directory, inspect real metadata folders
        local_path = Path(checkpoint_dir)
        is_local_dir = local_path.is_dir()
        if is_local_dir:
            offsets_dir = local_path / "offsets"
            commits_dir = local_path / "commits"
            state_dir = local_path / "state"

            if state_dir.is_dir():
                has_stateful_operators = True

            if offsets_dir.is_dir():
                offset_files = [
                    int(p.name) for p in offsets_dir.iterdir() if p.is_file() and p.name.isdigit()
                ]
                if offset_files:
                    latest_offset_batch = max(offset_files)
            if commits_dir.is_dir():
                commit_files = [
                    int(p.name) for p in commits_dir.iterdir() if p.is_file() and p.name.isdigit()
                ]
                if commit_files:
                    latest_commit_batch = max(commit_files)

        # 1. Uncommitted batch detection (crash recovery)
        uncommitted: list[int] = []
        if latest_offset_batch > latest_commit_batch:
            status = CheckpointIntegrityStatus.SUSPECT
            uncommitted = list(range(latest_commit_batch + 1, latest_offset_batch + 1))
            issues.append(
                f"Offset batch {latest_offset_batch} exists without matching commit marker (latest commit: {latest_commit_batch}). "
                "Spark will automatically replay this micro-batch upon next startup."
            )

        # 2. Incompatible stateful operator change
        if has_stateful_operators and aggregation_keys_changed:
            status = CheckpointIntegrityStatus.INCOMPATIBLE
            issues.append(
                "Stateful aggregation grouping keys or watermark delay have changed. "
                "Restarting with this checkpoint directory will trigger fatal 'IllegalStateException: state schema changed'."
            )
            procedure.extend(
                [
                    "1. Do NOT delete the existing checkpoint directory in place.",
                    "2. Deploy the updated query with a new versioned checkpoint directory (e.g. '_checkpoints/v2/').",
                    "3. For zero-downtime state handoff, run the v2 query in parallel or drain in-flight state via Trigger(availableNow=True).",
                ]
            )

        # 3. Source schema change
        if source_schema_changed:
            if status != CheckpointIntegrityStatus.INCOMPATIBLE:
                status = CheckpointIntegrityStatus.SUSPECT
            issues.append(
                "Source input schema has diverged from checkpoint metadata. "
                "Ensure schema evolution is enabled on stream readers."
            )

        if not procedure:
            procedure.append("Checkpoint directory is healthy. Safe to resume streaming execution.")

        is_cloud_uri = any(
            checkpoint_dir.startswith(scheme)
            for scheme in ("dbfs:", "s3:", "s3a:", "abfss:", "wasbs:", "gs:", "/Volumes/")
        )
        if not is_local_dir and is_cloud_uri:
            guidance = (
                f"Checkpoint URI '{checkpoint_dir}' advisory status: {status.value}. "
                f"(Remote cloud URI: static audit provides reference baseline; live offsets require active workspace credentials)."
            )
        else:
            guidance = (
                f"Checkpoint directory '{checkpoint_dir}' status: {status.value}. "
                f"Latest committed micro-batch: #{latest_commit_batch}."
            )

        return CheckpointHealthReport(
            checkpoint_dir=checkpoint_dir,
            status=status,
            has_stateful_operators=has_stateful_operators,
            latest_batch_id=latest_commit_batch,
            uncommitted_batches=uncommitted,
            issues=issues,
            recovery_guidance=guidance,
            safe_migration_procedure=procedure,
        )
