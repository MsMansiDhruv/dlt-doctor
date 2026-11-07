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
    source_stream: str = "source_cdc_stream"


class CdcDoctor:
    """Generates robust Delta CDC streaming deduplication and SCD reconciliation patterns."""

    def generate_cdc_templates(
        self,
        target_table: str,
        primary_keys: list[str],
        event_time_col: str = "updated_at",
        sequence_col: str | None = None,
        partition_col: str | None = None,
        source_is_cdf: bool = True,
        value_cols: list[str] | None = None,
        source_stream: str | None = None,
        include_auto_cdc: bool = False,
        include_scd2: bool = False,
    ) -> CdcMergeTemplate:
        """Generate deduplication and MERGE patterns to eliminate multiple-match errors."""
        clean_target = _validate_identifier(target_table)
        clean_keys = [_validate_identifier(k) for k in primary_keys]
        clean_time = _validate_identifier(event_time_col)
        # Change Data Feed (CDF) emits _commit_version by default; non-CDF streams use custom sequence
        if sequence_col is None or sequence_col == "_cdc_seq_id":
            clean_seq = "_commit_version" if source_is_cdf else "_cdc_seq_id"
        else:
            clean_seq = _validate_identifier(sequence_col)
        clean_part = _validate_identifier(partition_col) if partition_col else None
        clean_values = [_validate_identifier(v) for v in value_cols] if value_cols else []

        if include_auto_cdc and not source_stream:
            raise ValueError("source_stream must be provided when auto_cdc_code is requested.")

        if include_scd2 and not clean_values:
            raise ValueError("value_cols must be provided to generate SCD Type 2 MERGE SQL.")

        clean_source = _validate_identifier(source_stream) if source_stream else ""

        pk_str = ", ".join(f"'{k}'" for k in clean_keys)
        pk_join = " AND ".join(f"target.{k} = source.{k}" for k in clean_keys)
        current_target_pk_join = " AND ".join(
            f"current_target.{k} = source.{k}" for k in clean_keys
        )
        del_join = " AND ".join(f"target.{k} = del.{k}" for k in clean_keys)

        partition_clause = ""
        del_part_clause = ""
        if clean_part:
            partition_clause = f"AND target.{clean_part} = source.{clean_part} "
            del_part_clause = f"AND target.{clean_part} = del.{clean_part} "

        # In-batch deduplication with proper 4-space indentation
        cdf_filter = (
            "    # Drop Change Data Feed update_preimage before deduplication\n"
            "    microbatch_df = microbatch_df.filter(F.col('_change_type') != 'update_preimage')\n"
            if source_is_cdf
            else ""
        )

        if source_is_cdf:
            cdf_deletes_block = (
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
        else:
            cdf_deletes_block = "    deduped_df.createOrReplaceTempView('source_cdc_batch')\n"

        scd1_del_sql = (
            f"-- 1. Delete clause: process CDC deletes with recency guard\n"
            f"MERGE INTO {clean_target} AS target\n"  # nosec B608
            f"USING cdc_deletes_batch AS del\n"
            f"ON {del_join} {del_part_clause}\n"
            f"WHEN MATCHED AND (target.{clean_time} IS NULL OR del.{clean_time} >= target.{clean_time}) THEN DELETE;"
        )

        scd1_upsert_sql = (
            f"-- 2. Upsert clause: source_cdc_batch with recency guard\n"
            f"MERGE INTO {clean_target} AS target\n"  # nosec B608
            f"USING source_cdc_batch AS source\n"
            f"ON {pk_join} {partition_clause}\n"
            f"WHEN MATCHED AND (target.{clean_time} IS NULL OR source.{clean_time} >= target.{clean_time}) THEN UPDATE SET *\n"
            f"WHEN NOT MATCHED THEN INSERT *;"
        )

        scd1_sql = f"{scd1_del_sql}\n\n{scd1_upsert_sql}" if source_is_cdf else scd1_upsert_sql

        # Generate Python execution statements for microbatch_df.sparkSession
        exec_lines: list[str] = []
        if source_is_cdf:
            exec_lines.append(f'    spark.sql("""{scd1_del_sql}""")')
        exec_lines.append(f'    spark.sql("""{scd1_upsert_sql}""")')
        exec_block = "\n".join(exec_lines)

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
            f"{cdf_deletes_block}"
            "    spark = microbatch_df.sparkSession\n"
            f"{exec_block}\n"
        )

        scd2_sql = ""
        if clean_values or include_scd2:
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

            scd2_merge_key_selects = ", ".join(f"source.{k} AS merge_key_{k}" for k in clean_keys)
            scd2_null_key_selects = ", ".join(f"NULL AS merge_key_{k}" for k in clean_keys)
            scd2_on_predicates = [f"target.{k} = staged.merge_key_{k}" for k in clean_keys]
            scd2_on_predicates.append("target.is_current = true")
            if clean_part:
                scd2_on_predicates.append(f"target.{clean_part} = staged.{clean_part}")
            scd2_on_clause = " AND ".join(scd2_on_predicates)

            scd2_sql = (
                f"MERGE INTO {clean_target} AS target\n"  # nosec B608
                f"USING (\n"
                f"    SELECT {scd2_merge_key_selects}, source.* FROM source_cdc_batch AS source\n"
                f"    JOIN {clean_target} AS current_target ON {current_target_pk_join} AND current_target.is_current = true\n"
                f"    WHERE source.{clean_time} > current_target.valid_from\n"
                f"    UNION ALL\n"
                f"    SELECT {scd2_null_key_selects}, source.* FROM source_cdc_batch AS source\n"
                f"    LEFT JOIN {clean_target} AS current_target ON {current_target_pk_join} AND current_target.is_current = true\n"
                f"    WHERE current_target.valid_from IS NULL OR source.{clean_time} > current_target.valid_from\n"
                f") AS staged\n"
                f"ON {scd2_on_clause}\n"
                f"WHEN MATCHED THEN\n"
                f"    UPDATE SET is_current = false, valid_to = staged.{clean_time}\n"
                f"WHEN NOT MATCHED THEN\n"
                f"    INSERT ({insert_cols_str})\n"
                f"    VALUES ({insert_vals_str});"
            )

            if source_is_cdf:
                scd2_delete_sql = (
                    f"\n\n-- Close active SCD2 records for CDC delete events\n"
                    f"MERGE INTO {clean_target} AS target\n"  # nosec B608
                    f"USING cdc_deletes_batch AS del\n"
                    f"ON {del_join} AND target.is_current = true {del_part_clause}\n"
                    f"WHEN MATCHED AND (target.valid_from IS NULL OR del.{clean_time} > target.valid_from) THEN\n"
                    f"    UPDATE SET is_current = false, valid_to = del.{clean_time};"
                )
                scd2_sql = f"{scd2_sql}{scd2_delete_sql}"

        target_leaf = clean_target.split(".")[-1]
        cdf_clean_view = f"{target_leaf}_cdf_clean"
        auto_cdc_target_func = clean_target.replace(".", "_")
        auto_cdc_code = (
            (
                "# Modern Databricks Lakeflow Declarative CDC pattern (Recommended)\n"
                "from pyspark import pipelines as dp\n"
                "from pyspark.sql import functions as F\n\n"
                "# 1. Clean CDF stream to filter update_preimage before change application\n"
                f"@dp.temporary_view(name='{cdf_clean_view}')\n"
                f"def {auto_cdc_target_func}_cdf_clean():\n"
                f"    return (\n"
                f"        spark.readStream.table('{clean_source}')\n"
                f"        .filter(F.col('_change_type') != 'update_preimage')\n"
                f"    )\n\n"
                "# 2. Declare target streaming table before applying changes\n"
                "dp.create_streaming_table(\n"
                f"    name='{clean_target}'\n"
                ")\n\n"
                "# 3. Declare CDC flow into streaming table using create_auto_cdc_flow\n"
                "dp.create_auto_cdc_flow(\n"
                f"    target='{clean_target}',\n"
                f"    source='{cdf_clean_view}',\n"
                f"    keys=[{pk_str}],\n"
                f"    sequence_by=F.col('{clean_seq}'),\n"
                "    apply_as_deletes=F.expr(\"_change_type = 'delete'\"),\n"
                "    except_column_list=['_change_type', '_commit_version', '_commit_timestamp'],\n"
                "    stored_as_scd_type='1'  # or '2' for historical dimensional tracking\n"
                ")\n"
            )
            if clean_source
            else ""
        )

        return CdcMergeTemplate(
            primary_keys=clean_keys,
            event_time_col=clean_time,
            target_table=clean_target,
            microbatch_dedup_code=dedup_code,
            scd1_merge_sql=scd1_sql,
            scd2_merge_sql=scd2_sql,
            auto_cdc_code=auto_cdc_code,
            source_stream=clean_source,
        )
