"""
Shared reliability helpers for the Flight Delay Prediction pipeline.

Provides audit logging, Delta table rollback, idempotent batch management,
and schema evolution utilities used by every pipeline stage (Bronze → Silver →
Gold → ML).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


DEFAULT_AUDIT_TABLE = "flight_delay.audit.pipeline_run_log"
DEFAULT_RESCUED_COLUMN = "_rescued_data"


@dataclass(frozen=True)
class WritePreparation:
    previous_version: int | None
    batch_ids: tuple[str, ...]
    rows_removed: int


class PipelineAuditLogger:
    def __init__(self, spark: SparkSession, audit_table: str = DEFAULT_AUDIT_TABLE):
        self.spark = spark
        self.audit_table = audit_table

    def log_event(
        self,
        *,
        pipeline_name: str,
        stage_name: str,
        target_table: str,
        run_id: str,
        batch_id: str | None,
        status: str,
        rows_written: int | None = None,
        rows_removed: int | None = None,
        rows_rescued: int | None = None,
        restored_version: int | None = None,
        error_message: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        if not self.spark.catalog.tableExists(self.audit_table):
            return

        payload = {
            "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "pipeline_name": pipeline_name,
            "stage_name": stage_name,
            "target_table": target_table,
            "run_id": run_id,
            "batch_id": batch_id,
            "status": status,
            "rows_written": rows_written,
            "rows_removed": rows_removed,
            "rows_rescued": rows_rescued,
            "restored_version": restored_version,
            "error_message": error_message,
            "details": json.dumps(details, sort_keys=True) if details else None,
        }
        self.spark.createDataFrame([payload]).write.mode("append").saveAsTable(self.audit_table)


def get_current_table_version(spark: SparkSession, table_name: str) -> int | None:
    if not spark.catalog.tableExists(table_name):
        return None
    history_df = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1")
    rows = history_df.collect()
    return int(rows[0]["version"]) if rows else None


def restore_table_version(spark: SparkSession, table_name: str, version: int | None) -> int | None:
    if version is None or not spark.catalog.tableExists(table_name):
        return None
    spark.sql(f"RESTORE TABLE {table_name} TO VERSION AS OF {version}")
    return version


def split_rescued_records(
    df: DataFrame,
    rescued_column: str = DEFAULT_RESCUED_COLUMN,
) -> tuple[DataFrame, DataFrame]:
    """Split a freshly read source DataFrame into clean and rescued rows.

    When a Databricks reader is configured with ``rescuedDataColumn``, any
    values that do not fit the expected schema (type mismatches, unexpected
    extra fields, unparseable content) are collected into that column instead
    of silently dropping or failing the whole read.

    Returns ``(clean_df, rescued_df)`` where clean_df has the rescued column
    removed and rescued_df contains only the rows that had non-null rescued
    content.

    If the rescued column is absent, the input is treated as fully clean and
    an empty rescued DataFrame is returned.
    """
    from pyspark.sql import functions as F

    if rescued_column not in df.columns:
        return df, df.limit(0)

    clean_df = df.filter(F.col(rescued_column).isNull()).drop(rescued_column)
    rescued_df = df.filter(F.col(rescued_column).isNotNull())
    return clean_df, rescued_df


def extract_batch_ids(df: DataFrame) -> tuple[str, ...]:
    if "batch_id" not in df.columns:
        return ()
    values = []
    for row in df.select("batch_id").distinct().collect():
        batch_id = row["batch_id"]
        if batch_id is not None:
            values.append(str(batch_id))
    return tuple(values)


def delete_existing_batches(
    spark: SparkSession,
    table_name: str,
    batch_ids: tuple[str, ...],
) -> int:
    if not batch_ids or not spark.catalog.tableExists(table_name):
        return 0

    escaped_values = []
    for value in batch_ids:
        escaped_values.append("'" + value.replace("'", "''") + "'")
    escaped = ", ".join(escaped_values)
    rows_removed = spark.sql(
        f"SELECT COUNT(*) AS row_count FROM {table_name} WHERE batch_id IN ({escaped})"
    ).collect()[0]["row_count"]
    spark.sql(f"DELETE FROM {table_name} WHERE batch_id IN ({escaped})")
    return int(rows_removed)


def evolve_table_schema_for_dataframe(
    spark: SparkSession,
    table_name: str,
    df: DataFrame,
    *,
    protected_columns: tuple[str, ...] = (),
) -> list[str]:
    if not spark.catalog.tableExists(table_name):
        return []

    target_schema = spark.table(table_name).schema
    existing_names = {field.name for field in target_schema.fields}

    new_fields = [
        field
        for field in df.schema.fields
        if field.name not in existing_names and field.name not in protected_columns
    ]
    if not new_fields:
        return []

    column_defs = ", ".join(
        f"`{field.name}` {field.dataType.simpleString()}" for field in new_fields
    )
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({column_defs})")
    return [field.name for field in new_fields]
