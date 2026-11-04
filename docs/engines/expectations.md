# Zero-Loss Quarantine (`ExpectationDoctor`)

Native DLT `@dlt.expect_or_drop` and `@dp.expect_or_drop` silently delete records that fail validation constraints. In regulatory and financial environments, silent drops violate compliance and auditability.

## The Dual-Routing Solution

`ExpectationDoctor` replaces destructive drops with dual streaming flows:

1. **Clean Table**: Valid records flowing forward into Silver / Gold.
2. **Quarantine Table**: Invalid records isolated with violation metadata and timestamping.

Both branches are strictly complete and disjoint via SQL three-valued logic (NULL-safe `coalesce` wrapping).

## Automated Healing via CLI

To automatically detect `@expect_or_drop` anti-patterns and synthesize non-destructive single-pass quarantine tables:

```bash
dlt-doctor heal src/pipelines/orders_silver.py
```

## Python API Usage

```python
from dlt_doctor import ExpectationDoctor, ExpectationRule

doctor = ExpectationDoctor()
rules = [
    ExpectationRule("valid_id", "order_id IS NOT NULL"),
    ExpectationRule("positive_price", "unit_price > 0"),
]
plan = doctor.build_quarantine_routing("silver_orders", rules)

# Generates single-pass Lakeflow pipeline code with _failed_rules tracking
pipeline_code = plan.generate_pipeline_code(source_table="bronze_orders", flavor="dp")
print(pipeline_code)
```
