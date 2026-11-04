"""Delta Change Data Feed (CDF) deduplication and SCD Type 1 & 2 stream reconciliation."""

from __future__ import annotations

import re
from dataclasses import dataclass

IDENTIFIER_PATTERN = re.compile(
    r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$|^`[a-zA-Z0-9_\.]+`$"
)


def _validate_identifier(identifier: str) -> str:
    """Validate SQL identifier against injection patterns."""
    if not IDENTIFIER_PATTERN.match(identifier.strip()):
        raise ValueError(f"Invalid SQL identifier '{identifier}'.")
    return identifier.strip()


@dataclass
class CdcMergeTemplate:
    """Generated PySpark and SQL templates for streaming CDC deduplication."""

    primary_keys: list[str]
    event_time_col: str
    target_table: str
    microbatch_dedup_code: str
    scd1_merge_sql: str
    scd2_merge_sql: str
    auto_cdc_code: str = ""


class CdcDoctor:
    """Generates robust Delta CDC streaming deduplication and SCD reconciliation patterns."""

    def generate_cdc_templates(
        self,
        target_table: str,
        primary_keys: list[str],
        event_time_col: str = "updated_at",
        sequence_col: str = "_cdc_seq_id",
        partition_col: str | None = None,
        source_is_cdf: bool = True,
        value_cols: list[str] | None = None,
    ) -> CdcMergeTemplate:
        """Generate deduplication and MERGE patterns to eliminate multiple-match errors."""
        clean_target = _validate_identifier(target_table)
        clean_keys = [_validate_identifier(k) for k in primary_keys]
        clean_time = _validate_identifier(event_time_col)
        clean_seq = _validate_identifier(sequence_col)
        clean_part = _validate_identifier(partition_col) if partition_col else None
        clean_values = [_validate_identifier(v) for v in value_cols] if value_cols else []

        pk_str = ", ".join(f"'{k}'" for k in clean_keys)
        pk_join = " AND ".join(f"target.{k} = source.{k}" for k in clean_keys)
        current_target_pk_join = " AND ".join(
            f"current_target.{k} = source.{k}" for k in clean_keys
        )
        del_join = " AND ".join(f"target.{k} = del.{k}" for k in clean_keys)

        partition_clause = ""
        if clean_part:
            partition_clause = f"AND target.{clean_part} = source.{clean_part} "

        # In-batch deduplication with proper 4-space indentation
        cdf_filter = (
            "    # Drop Change Data Feed update_preimage before deduplication\n"
            "    microbatch_df = microbatch_df.filter(F.col('_change_type') != 'update_preimage')\n"
            if source_is_cdf
            else ""
        )

        dedup_code = (
            "# 1. In-batch window deduplication (prevents Spark 'multiple matches' MERGE error)\n"
            "from pyspark.sql import Window, functions as F\n\n"
            "def merge_cdc_microbatch(microbatch_df, batch_id):\n"
            f"{cdf_filter}"
            f"    window_spec = Window.partitionBy([{pk_str}]).orderBy(\n"
            f"        F.col('{clean_time}').desc(),\n"
            f"        F.col('{clean_seq}').desc()\n"
            "    )\n"
            "    deduped_df = (\n"
            "        microbatch_df\n"
            "        .withColumn('_rn', F.row_number().over(window_spec))\n"
            "        .filter(F.col('_rn') == 1)\n"
            "        .drop('_rn')\n"
            "    )\n"
            "    # Handle deletes in a dedicated view, then drop _change_type before MERGE\n"
            "    deletes_df = deduped_df.filter(F.col('_change_type') == 'delete')\n"
            "    deletes_df.createOrReplaceTempView('cdc_deletes_batch')\n\n"
            "    # Clean CDF metadata and drop _change_type to prevent schema collision in target table\n"
            "    cleaned_df = (\n"
            "        deduped_df.filter(F.col('_change_type') != 'delete')\n"
            "        .drop('_change_type', '_commit_version', '_commit_timestamp')\n"
            "    )\n"
            "    cleaned_df.createOrReplaceTempView('source_cdc_batch')\n"
        )

        scd1_sql = (
            f"-- 1. Delete clause: process CDC deletes in a dedicated clause\n"
            f"DELETE FROM {clean_target} AS target\n"  # nosec B608
            f"WHERE EXISTS (\n"
            f"    SELECT 1 FROM cdc_deletes_batch AS del\n"
            f"    WHERE {del_join}\n"
            f");\n\n"
            f"-- 2. Upsert clause: source_cdc_batch has _change_type dropped\n"
            f"MERGE INTO {clean_target} AS target\n"  # nosec B608
            f"USING source_cdc_batch AS source\n"
            f"ON {pk_join} {partition_clause}\n"
            f"WHEN MATCHED THEN UPDATE SET *\n"
            f"WHEN NOT MATCHED THEN INSERT *;"
        )

        # Standard valid Databricks Delta SCD2 staging-union pattern
        insert_cols_list = (
            clean_keys
            + [c for c in clean_values if c not in clean_keys]
            + ["is_current", "valid_from", "valid_to"]
        )
        insert_vals_list = [
            f"staged.{c}" for c in clean_keys + [c for c in clean_values if c not in clean_keys]
        ] + ["true", f"staged.{clean_time}", "NULL"]
        insert_cols_str = ", ".join(insert_cols_list)
        insert_vals_str = ", ".join(insert_vals_list)

        scd2_sql = (
            f"MERGE INTO {clean_target} AS target\n"  # nosec B608
            f"USING (\n"
            f"    SELECT source.{clean_keys[0]} AS merge_key, source.* FROM source_cdc_batch AS source\n"
            f"    JOIN {clean_target} AS current_target ON {current_target_pk_join} AND current_target.is_current = true\n"
            f"    UNION ALL\n"
            f"    SELECT NULL AS merge_key, source.* FROM source_cdc_batch AS source\n"
            f") AS staged\n"
            f"ON target.{clean_keys[0]} = staged.merge_key AND target.is_current = true\n"
            f"WHEN MATCHED THEN\n"
            f"    UPDATE SET is_current = false, valid_to = staged.{clean_time}\n"
            f"WHEN NOT MATCHED THEN\n"
            f"    INSERT ({insert_cols_str})\n"
            f"    VALUES ({insert_vals_str});"
        )

        auto_cdc_code = (
            "# Modern Databricks Lakeflow Declarative CDC pattern (Recommended)\n"
            "import dlt\n"
            "from pyspark.sql import functions as F\n\n"
            "# 1. Declare target streaming table before applying changes\n"
            "dlt.create_streaming_table(\n"
            f"    name='{clean_target}'\n"
            ")\n\n"
            "# 2. Declare CDC flow into streaming table\n"
            "dlt.apply_changes(\n"
            f"    target='{clean_target}',\n"
            "    source='source_cdc_stream',\n"
            f"    keys=[{pk_str}],\n"
            f"    sequence_by=F.col('{clean_seq}'),\n"
            "    apply_as_deletes=F.expr(\"_change_type = 'delete'\"),\n"
            "    except_column_list=['_change_type', '_commit_version', '_commit_timestamp'],\n"
            "    stored_as_scd_type='1'  # or '2' for historical dimensional tracking\n"
            ")\n"
        )

        return CdcMergeTemplate(
            primary_keys=clean_keys,
            event_time_col=clean_time,
            target_table=clean_target,
            microbatch_dedup_code=dedup_code,
            scd1_merge_sql=scd1_sql,
            scd2_merge_sql=scd2_sql,
            auto_cdc_code=auto_cdc_code,
        )
