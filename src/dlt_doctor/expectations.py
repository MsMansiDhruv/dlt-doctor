"""Delta Live Tables (DLT) expectation wrappers and automated quarantine routing."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ExpectationRule:
    """A business data quality expectation for DLT tables."""

    rule_name: str
    condition_sql: str
    action_on_failure: str = "quarantine"  # quarantine, drop, fail, warn
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "condition_sql": self.condition_sql,
            "action_on_failure": self.action_on_failure,
            "description": self.description,
        }


@dataclass
class QuarantineRoutingPlan:
    """Execution instructions for routing valid records and quarantining invalid rows."""

    target_clean_table: str
    target_quarantine_table: str
    valid_filter_sql: str
    invalid_filter_sql: str
    rules: list[ExpectationRule] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_clean_table": self.target_clean_table,
            "target_quarantine_table": self.target_quarantine_table,
            "valid_filter_sql": self.valid_filter_sql,
            "invalid_filter_sql": self.invalid_filter_sql,
            "rules": [r.to_dict() for r in self.rules],
        }


class ExpectationDoctor:
    """Manages resilient DLT quality gates without silent data drops or pipeline halts."""

    def __init__(self) -> None:
        pass

    def build_quarantine_routing(
        self,
        table_name: str,
        rules: list[ExpectationRule],
    ) -> QuarantineRoutingPlan:
        """Construct dual-routing filters separating clean data from quarantined records."""
        if not rules:
            return QuarantineRoutingPlan(
                target_clean_table=table_name,
                target_quarantine_table=f"{table_name}_quarantine",
                valid_filter_sql="1 = 1",
                invalid_filter_sql="1 = 0",
                rules=[],
            )

        # All rules must pass for clean stream: (rule1) AND (rule2)
        valid_conditions = [f"({r.condition_sql})" for r in rules]
        valid_filter = " AND ".join(valid_conditions)

        # Any rule fails for quarantine stream: NOT ((rule1) AND (rule2))
        invalid_filter = f"NOT ({valid_filter})"

        return QuarantineRoutingPlan(
            target_clean_table=table_name,
            target_quarantine_table=f"{table_name}_quarantine",
            valid_filter_sql=valid_filter,
            invalid_filter_sql=invalid_filter,
            rules=rules,
        )

    def generate_dlt_decorator_code(
        self,
        routing_plan: QuarantineRoutingPlan,
    ) -> str:
        """Generate copy-pasteable PySpark DLT table definitions with quarantine routing."""
        clean_code = [
            f"# Clean target table: {routing_plan.target_clean_table}",
            "@dlt.table(",
            f"    name='{routing_plan.target_clean_table}',",
            "    comment='Clean records passing all expectation contracts.'",
            ")",
            f"def {routing_plan.target_clean_table}():",
            "    return (",
            "        dlt.read_stream('source_bronze')",
            f'        .filter("{routing_plan.valid_filter_sql}")',
            "    )",
            "",
            f"# Dead-letter quarantine table: {routing_plan.target_quarantine_table}",
            "@dlt.table(",
            f"    name='{routing_plan.target_quarantine_table}',",
            "    comment='Quarantined records failing business expectations for triage.'",
            ")",
            f"def {routing_plan.target_quarantine_table}():",
            "    return (",
            "        dlt.read_stream('source_bronze')",
            f'        .filter("{routing_plan.invalid_filter_sql}")',
            "        .withColumn('_dlt_quarantined_at', F.current_timestamp())",
            "    )",
        ]
        return "\n".join(clean_code)
