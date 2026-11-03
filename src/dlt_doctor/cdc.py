"""Delta Change Data Feed (CDF) deduplication and SCD Type 1 & 2 stream reconciliation."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CdcMergeTemplate:
    """Generated PySpark and SQL templates for streaming CDC deduplication."""

    primary_keys: list[str]
    event_time_col: str
    target_table: str
    microbatch_dedup_code: str
    scd1_merge_sql: str
    scd2_merge_sql: str


class CdcDoctor:
    """Generates robust Delta CDC streaming deduplication and SCD reconciliation patterns."""

    def generate_cdc_templates(
        self,
        target_table: str,
        primary_keys: list[str],
        event_time_col: str = "updated_at",
        sequence_col: str = "_cdc_seq_id",
        partition_col: str | None = None,
    ) -> CdcMergeTemplate:
        """Generate deduplication and MERGE patterns to eliminate multiple-match errors."""
        pk_str = ", ".join(f"'{k}'" for k in primary_keys)
        pk_join = " AND ".join(f"target.{k} = source.{k}" for k in primary_keys)

        partition_clause = ""
        if partition_col:
            partition_clause = f"AND target.{partition_col} = source.{partition_col} "

        dedup_code = (
            "# 1. In-batch window deduplication (prevents Spark 'multiple matches' MERGE error)\n"
            "from pyspark.sql import Window, functions as F\n\n"
            "def merge_cdc_microbatch(microbatch_df, batch_id):\n"
            f"    window_spec = Window.partitionBy([{pk_str}]).orderBy(\n"
            f"        F.col('{event_time_col}').desc(),\n"
            f"        F.col('{sequence_col}').desc()\n"
            "    )\n"
            "    deduped_df = (\n"
            "        microbatch_df\n"
            "        .withColumn('_rn', F.row_number().over(window_spec))\n"
            "        .filter(F.col('_rn') == 1)\n"
            "        .drop('_rn')\n"
            "    )\n"
            "    deduped_df.createOrReplaceTempView('source_cdc_batch')\n"
        )

        scd1_sql = (
            f"MERGE INTO {target_table} AS target "  # nosec B608
            f"USING source_cdc_batch AS source "
            f"ON {pk_join} {partition_clause}"
            f"WHEN MATCHED AND source._change_type = 'delete' THEN DELETE "
            f"WHEN MATCHED THEN UPDATE SET * "
            f"WHEN NOT MATCHED AND source._change_type != 'delete' THEN INSERT *;"
        )

        scd2_sql = (
            f"MERGE INTO {target_table} AS target "  # nosec B608
            f"USING source_cdc_batch AS source "
            f"ON {pk_join} AND target.is_current = true "
            f"WHEN MATCHED AND source._change_type = 'update' THEN "
            f"UPDATE SET is_current = false, valid_to = source.{event_time_col} "
            f"WHEN NOT MATCHED THEN "
            f"INSERT (..., is_current, valid_from, valid_to) VALUES (..., true, source.{event_time_col}, NULL);"
        )

        return CdcMergeTemplate(
            primary_keys=primary_keys,
            event_time_col=event_time_col,
            target_table=target_table,
            microbatch_dedup_code=dedup_code,
            scd1_merge_sql=scd1_sql,
            scd2_merge_sql=scd2_sql,
        )
