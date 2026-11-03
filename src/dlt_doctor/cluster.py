"""Delta Lake Liquid Clustering (CLUSTER BY) advisor and candidate key selector."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ColumnProfile:
    """Statistical profile of a table column for Liquid Clustering evaluation."""

    name: str
    data_type: str
    distinct_count: int
    filter_frequency_score: float = (
        0.0  # 0.0 to 1.0 (frequency in query WHERE/JOIN clauses)
    )
    is_monotonically_increasing: bool = False


@dataclass
class LiquidClusterRecommendation:
    """Recommended Liquid Clustering configuration and migration DDL."""

    table_name: str
    recommended_keys: list[str]
    rejected_keys: list[tuple[str, str]] = field(default_factory=list)  # (col, reason)
    alter_cluster_ddl: str = ""
    optimize_ddl: str = ""
    migration_advice: str = ""


class ClusterDoctor:
    """Evaluates column candidates and generates optimal Liquid Clustering configurations."""

    def advise_clustering(
        self,
        table_name: str,
        columns: list[ColumnProfile],
        total_rows: int = 10_000_000,
        current_partition_cols: list[str] | None = None,
    ) -> LiquidClusterRecommendation:
        """Select 1-4 optimal clustering keys according to Databricks Liquid Clustering principles."""
        candidate_scores: list[tuple[float, ColumnProfile]] = []
        rejected: list[tuple[str, str]] = []

        for col in columns:
            # 1. Reject primary keys or extreme high cardinality (e.g. distinct count > 95% of rows)
            if total_rows > 0 and (col.distinct_count / total_rows) > 0.95:
                rejected.append(
                    (
                        col.name,
                        f"Distinct count ({col.distinct_count:,}) is near-unique (>95% total rows).",
                    )
                )
                continue

            # 2. Reject extremely low cardinality (e.g. boolean or < 3 distinct values) if table is large
            if col.distinct_count < 3 and total_rows > 100_000:
                rejected.append(
                    (
                        col.name,
                        f"Distinct count ({col.distinct_count}) is too low to provide pruning.",
                    )
                )
                continue

            # 3. Reject complex or large binary types
            if any(
                t in col.data_type.lower()
                for t in ("array", "map", "struct", "binary", "blob")
            ):
                rejected.append(
                    (
                        col.name,
                        f"Data type '{col.data_type}' is unsupported for Liquid Clustering.",
                    )
                )
                continue

            # 4. Monotonically increasing timestamps alone are sub-optimal
            if col.is_monotonically_increasing and col.filter_frequency_score < 0.3:
                rejected.append(
                    (
                        col.name,
                        "Monotonically increasing key with low filter frequency causes clustered file skew.",
                    )
                )
                continue

            # Calculate composite suitability score:
            # High filter frequency + balanced cardinality (between 10 and 500,000)
            cardinality_ratio = col.distinct_count / max(total_rows, 1)
            cardinality_score = 1.0 - abs(cardinality_ratio - 0.05) * 5.0
            cardinality_score = max(0.1, min(1.0, cardinality_score))

            score = (col.filter_frequency_score * 0.7) + (cardinality_score * 0.3)
            candidate_scores.append((score, col))

        # Sort by composite score descending and pick top 1 to 4 keys
        candidate_scores.sort(key=lambda x: x[0], reverse=True)
        chosen_keys = [c.name for _, c in candidate_scores[:4]]

        if not chosen_keys and columns:
            # Fallback to the column with the highest filter frequency if none passed strict filters
            fallback = max(columns, key=lambda c: c.filter_frequency_score)
            chosen_keys = [fallback.name]

        keys_clause = ", ".join(chosen_keys)
        alter_ddl = f"ALTER TABLE {table_name} CLUSTER BY ({keys_clause});"  # nosec B608
        optimize_ddl = f"OPTIMIZE {table_name};"  # nosec B608

        advice_parts = [
            f"Liquid Clustering replaces legacy Hive-style partitioning and Z-Order on {table_name}."
        ]
        if current_partition_cols:
            part_str = ", ".join(current_partition_cols)
            advice_parts.append(
                f"Drop static physical partitioning on ({part_str}) to eliminate small file directories and partition skew."
            )
        advice_parts.append(
            f"Cluster keys ({keys_clause}) support concurrent write operations without partition lock conflicts."
        )

        return LiquidClusterRecommendation(
            table_name=table_name,
            recommended_keys=chosen_keys,
            rejected_keys=rejected,
            alter_cluster_ddl=alter_ddl,
            optimize_ddl=optimize_ddl,
            migration_advice=" ".join(advice_parts),
        )
