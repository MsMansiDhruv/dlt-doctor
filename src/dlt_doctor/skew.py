"""Data skew detection and AQE skew-mitigation advisor for PySpark joins."""

from __future__ import annotations

from dataclasses import dataclass, field

from dlt_doctor.catalyst import DiagnosticFinding, DiagnosticSeverity


@dataclass
class SkewAnalysisResult:
    """Findings from partition and join key distribution analysis."""

    has_severe_skew: bool
    skewed_keys: list[str] = field(default_factory=list)
    max_to_median_ratio: float = 1.0
    recommended_configs: dict[str, str] = field(default_factory=dict)
    salting_code_snippet: str = ""
    findings: list[DiagnosticFinding] = field(default_factory=list)


class SkewDoctor:
    """Diagnoses data skew and generates Adaptive Query Execution (AQE) and salting fixes."""

    def analyze_key_distribution(
        self,
        key_counts: dict[str, int],
        skew_ratio_threshold: float = 5.0,
    ) -> SkewAnalysisResult:
        """Evaluate key frequency distribution to detect straggler partitions."""
        if not key_counts:
            return SkewAnalysisResult(has_severe_skew=False)

        sorted_counts = sorted(key_counts.values())
        max_count = sorted_counts[-1]
        median_count = sorted_counts[len(sorted_counts) // 2] or 1

        ratio = round(max_count / median_count, 2)
        skewed_keys = [
            k
            for k, v in key_counts.items()
            if (v / median_count) >= skew_ratio_threshold
        ]

        has_skew = ratio >= skew_ratio_threshold
        findings: list[DiagnosticFinding] = []

        recommended_configs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": str(
                int(skew_ratio_threshold)
            ),
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "67108864",  # 64MB
        }

        salting_snippet = (
            "# Salting PySpark fix for high-cardinality skew key:\n"
            "from pyspark.sql import functions as F\n\n"
            "# 1. Salt skewed table with random integer (e.g. 0 to 4)\n"
            "salted_left = left_df.withColumn('_salt', F.when(\n"
            f"    F.col('join_key').isin({skewed_keys[:3]}),\n"
            "    F.floor(F.rand() * 4)\n"
            ").otherwise(0))\n\n"
            "# 2. Replicate dimension table across the 4 salt values\n"
            "salt_replicas = spark.range(0, 4).withColumnRenamed('id', '_salt')\n"
            "replicated_right = right_df.crossJoin(salt_replicas)\n\n"
            "# 3. Execute balanced join on (join_key, _salt)\n"
            "balanced_df = salted_left.join(replicated_right, ['join_key', '_salt'])"
        )

        if has_skew:
            findings.append(
                DiagnosticFinding(
                    rule_id="SKEW-001",
                    severity=DiagnosticSeverity.CRITICAL,
                    title="Severe join key data skew detected",
                    impact_summary=(
                        f"Max-to-median partition key ratio is {ratio}x (threshold: {skew_ratio_threshold}x). "
                        f"Skewed keys: {skewed_keys[:5]}. Causes 99% stage completion hang on straggler tasks."
                    ),
                    recommendation=(
                        "Enable Spark Adaptive Query Execution (AQE) skew join configs or apply key salting."
                    ),
                    estimated_dbu_impact="Critical (eliminates straggler tasks)",
                )
            )

        return SkewAnalysisResult(
            has_severe_skew=has_skew,
            skewed_keys=skewed_keys,
            max_to_median_ratio=ratio,
            recommended_configs=recommended_configs,
            salting_code_snippet=salting_snippet,
            findings=findings,
        )
