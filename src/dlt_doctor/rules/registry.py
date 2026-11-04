"""Rule registry and schema definitions for dlt-doctor."""

from __future__ import annotations

from dataclasses import dataclass

from dlt_doctor.catalyst import DiagnosticSeverity


@dataclass(frozen=True)
class Rule:
    """A registered diagnostic rule."""

    id: str
    name: str
    default_severity: DiagnosticSeverity
    description: str
    docs_slug: str

    def to_dict(self) -> dict[str, str]:
        return {
            "id": self.id,
            "name": self.name,
            "default_severity": self.default_severity.value,
            "description": self.description,
            "docs_slug": self.docs_slug,
        }


_REGISTRY: dict[str, Rule] = {}


def register(rule: Rule) -> Rule:
    """Register a diagnostic rule."""
    if rule.id in _REGISTRY:
        raise ValueError(f"Rule '{rule.id}' is already registered.")
    _REGISTRY[rule.id] = rule
    return rule


def get_rule(rule_id: str) -> Rule | None:
    """Retrieve a rule by its ID."""
    return _REGISTRY.get(rule_id.upper())


def list_rules() -> list[Rule]:
    """Return all registered rules ordered by ID."""
    return sorted(_REGISTRY.values(), key=lambda r: r.id)


def rule_exists(rule_id: str) -> bool:
    """Check if a rule ID is registered."""
    return rule_id.upper() in _REGISTRY
