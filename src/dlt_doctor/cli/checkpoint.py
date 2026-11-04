"""Handler for 'dlt-doctor checkpoint' subcommand."""

from __future__ import annotations

import argparse
import sys

from dlt_doctor.checkpoint import CheckpointDoctor, CheckpointIntegrityStatus


def register_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "checkpoint", help="Audit Structured Streaming checkpoint integrity and metadata"
    )
    p.add_argument(
        "path",
        nargs="?",
        default=None,
        help="Path to checkpoint directory or mock location",
    )
    p.add_argument(
        "--dir",
        dest="dir_flag",
        default=None,
        help="Path to checkpoint directory (flag form)",
    )


def handle(args: argparse.Namespace) -> int:
    checkpoint_dir = args.dir_flag or args.path
    if not checkpoint_dir:
        print("Error: Missing checkpoint directory path.", file=sys.stderr)
        return 1
    doctor = CheckpointDoctor()
    report = doctor.audit_checkpoint_state(checkpoint_dir)

    print("=" * 70)
    print(f" Streaming Checkpoint Integrity Audit: {report.checkpoint_dir}")
    print(f" Status: {report.status.value}")
    print("=" * 70)
    print(f"Latest Batch ID:            {report.latest_batch_id}")
    print(f"Uncommitted Batches:        {report.uncommitted_batches}")
    print(f"Has Stateful Operators:     {report.has_stateful_operators}")
    print(f"Recovery Guidance:          {report.recovery_guidance}")
    if report.issues:
        print("\nDetected Issues:")
        for issue in report.issues:
            print(f"  - {issue}")
    if report.safe_migration_procedure:
        print("\nSafe Migration Procedure:")
        for step in report.safe_migration_procedure:
            print(f"  {step}")
    print("-" * 70)
    return 0 if report.status == CheckpointIntegrityStatus.HEALTHY else 1
