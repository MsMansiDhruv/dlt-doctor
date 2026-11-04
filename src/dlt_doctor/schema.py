"""Pre-flight schema drift detection and safe Delta Lake schema evolution advisor."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class SchemaDriftSeverity(str, Enum):
    """Severity of identified schema drift between incoming batch and target Delta table."""

    SAFE_ADDITION = "SAFE_ADDITION"
    SAFE_WIDENING = "SAFE_WIDENING"
    BREAKING = "BREAKING"
    INFO = "INFO"


@dataclass
class SchemaChange:
    """Specific column change identified during schema comparison."""

    column_name: str
    existing_type: str | None
    incoming_type: str | None
    severity: SchemaDriftSeverity
    description: str
    migration_action: str


@dataclass
class SchemaEvolutionReport:
    """Consolidated schema comparison report and generated DDL for Delta Lake."""

    table_name: str
    has_breaking_changes: bool
    changes: list[SchemaChange] = field(default_factory=list)
    generated_ddl: list[str] = field(default_factory=list)
    recommended_spark_options: dict[str, str] = field(default_factory=dict)


class SchemaDoctor:
    """Audits incoming batch schemas against target Delta table contracts."""

    # Compatible type widening map: existing_type -> set of safe widening targets
    WIDENING_MAP: dict[str, set[str]] = {
        "byte": {
            "short",
            "int",
            "integer",
            "bigint",
            "long",
            "float",
            "double",
            "decimal",
        },
        "short": {"int", "integer", "bigint", "long", "float", "double", "decimal"},
        "int": {"bigint", "long", "float", "double", "decimal"},
        "integer": {"bigint", "long", "float", "double", "decimal"},
        "bigint": {"decimal", "string"},
        "long": {"decimal", "string"},
        "float": {"double", "decimal"},
        "date": {"timestamp", "string"},
    }

    def compare_schemas(
        self,
        table_name: str,
        existing_schema: dict[str, str],
        incoming_schema: dict[str, str],
    ) -> SchemaEvolutionReport:
        """Compare incoming batch schema against existing target table schema."""
        changes: list[SchemaChange] = []
        generated_ddl: list[str] = []
        has_breaking = False

        norm_existing = {k.lower(): v.lower() for k, v in existing_schema.items()}
        norm_incoming = {k.lower(): v.lower() for k, v in incoming_schema.items()}

        # 1. Check for incoming columns not in target table (Additions)
        for col, inc_type in norm_incoming.items():
            if col not in norm_existing:
                ddl = f"ALTER TABLE {table_name} ADD COLUMNS ({col} {inc_type});"
                generated_ddl.append(ddl)
                changes.append(
                    SchemaChange(
                        column_name=col,
                        existing_type=None,
                        incoming_type=inc_type,
                        severity=SchemaDriftSeverity.SAFE_ADDITION,
                        description=f"New column '{col}' of type '{inc_type}' detected in incoming batch.",
                        migration_action=f"Add column via Delta schema evolution: {ddl}",
                    )
                )

        # 2. Check for type alterations on existing columns
        for col, ext_type in norm_existing.items():
            if col in norm_incoming:
                inc_type = norm_incoming[col]
                if ext_type != inc_type:
                    # Check if safe widening
                    safe_targets = self.WIDENING_MAP.get(ext_type, set())
                    if inc_type in safe_targets:
                        changes.append(
                            SchemaChange(
                                column_name=col,
                                existing_type=ext_type,
                                incoming_type=inc_type,
                                severity=SchemaDriftSeverity.SAFE_WIDENING,
                                description=f"Safe type widening on '{col}': '{ext_type}' -> '{inc_type}'.",
                                migration_action=f"Allow implicit or explicit cast: cast({col} AS {inc_type})",
                            )
                        )
                    else:
                        has_breaking = True
                        changes.append(
                            SchemaChange(
                                column_name=col,
                                existing_type=ext_type,
                                incoming_type=inc_type,
                                severity=SchemaDriftSeverity.BREAKING,
                                description=(
                                    f"Incompatible type mutation on '{col}': '{ext_type}' -> '{inc_type}'. "
                                    "Writing directly will cause AnalysisException or route values to _rescued_data."
                                ),
                                migration_action=f"Enforce quarantine filter or explicit try_cast({col} AS {ext_type})",
                            )
                        )
            else:
                # Missing column in incoming batch
                changes.append(
                    SchemaChange(
                        column_name=col,
                        existing_type=ext_type,
                        incoming_type=None,
                        severity=SchemaDriftSeverity.INFO,
                        description=f"Column '{col}' present in target table is omitted in incoming batch.",
                        migration_action="Target column will be populated as NULL unless defaults are defined.",
                    )
                )

        spark_options = {
            "spark.databricks.delta.schema.autoMerge.enabled": "true"
            if not has_breaking
            else "false",
        }

        return SchemaEvolutionReport(
            table_name=table_name,
            has_breaking_changes=has_breaking,
            changes=changes,
            generated_ddl=generated_ddl,
            recommended_spark_options=spark_options,
        )
