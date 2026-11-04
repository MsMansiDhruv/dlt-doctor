# Checkpoint Integrity (`CheckpointDoctor`)

Structured Streaming checkpoints store state store offsets and commit logs. Incompatible changes (modifying source schemas, changing aggregation keys) cause streaming queries to fail with unrecoverable `InvalidStateException`.

## Integrity Auditing

`CheckpointDoctor` audits state store metadata:
- Verifies state compatibility across query schema revisions.
- Identifies unrecoverable state key mismatches.
- Emits zero-downtime blue/green checkpoint migration procedures.

## Python API Usage

```python
from dlt_doctor import CheckpointDoctor

doctor = CheckpointDoctor()
report = doctor.audit_checkpoint_state(
    checkpoint_dir="/Volumes/checkpoints/orders",
    has_stateful_operators=True,
    source_schema_changed=False,
    aggregation_keys_changed=True,  # Incompatible state modification!
)

print(f"Integrity Status: {report.status.value}")
if report.status.value == "INCOMPATIBLE":
    print("Action Plan:")
    for step in report.safe_migration_procedure:
        print(f"  {step}")
```
