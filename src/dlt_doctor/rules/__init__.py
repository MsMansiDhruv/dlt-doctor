"""Rule registry and official rules for dlt-doctor."""

from dlt_doctor.rules.registry import Rule, get_rule, list_rules, register, rule_exists

# Import definitions to register all rules eagerly
import dlt_doctor.rules.definitions  # noqa: F401

__all__ = [
    "Rule",
    "register",
    "get_rule",
    "list_rules",
    "rule_exists",
]
