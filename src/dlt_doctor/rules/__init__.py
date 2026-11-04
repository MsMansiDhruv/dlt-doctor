"""Rule registry and official rules for dlt-doctor."""

# Import definitions to register all rules eagerly
import dlt_doctor.rules.definitions  # noqa: F401
from dlt_doctor.rules.registry import Rule, get_rule, list_rules, register, rule_exists

__all__ = [
    "Rule",
    "get_rule",
    "list_rules",
    "register",
    "rule_exists",
]
