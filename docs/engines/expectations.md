# Zero-Loss Quarantine (`ExpectationDoctor`)

Native DLT `@dlt.expect_or_drop` silently deletes records that fail validation constraints. In regulatory and financial environments, silent drops violate compliance and auditability.

## The Dual-Routing Solution

`ExpectationDoctor` replaces destructive drops with dual streaming flows:

1. **Clean Table**: Valid records flowing forward into Silver / Gold.
2. **Quarantine Table**: Invalid records isolated with violation metadata and timestamping.

## Python API Usage

```python
import dlt
from dlt_doctor import ExpectationDoctor, ExpectationRule

doctor = ExpectationDoctor()
rules = [
    ExpectationRule("valid_id", "order_id IS NOT NULL"),
    ExpectationRule("positive_price", "unit_price > 0"),
]
plan = doctor.build_quarantine_routing("silver_orders", rules)

@dlt.table(name=plan.target_clean_table)
def silver_orders():
    return dlt.read_stream("bronze_orders").filter(plan.valid_filter_sql)

@dlt.table(name=plan.target_quarantine_table)
def silver_orders_quarantine():
    return (
        dlt.read_stream("bronze_orders")
        .filter(plan.invalid_filter_sql)
        .withColumn("_quarantined_at", F.current_timestamp())
    )
```
