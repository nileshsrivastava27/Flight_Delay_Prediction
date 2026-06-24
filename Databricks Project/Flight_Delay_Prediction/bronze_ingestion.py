"""
Bronze ingestion for the Flight Delay Prediction pipeline.

Loads raw CSV source files into Delta Bronze tables under the flight_delay
catalog. Schema-mismatching rows are captured via rescuedDataColumn and routed
to per-dataset *_rejected tables. A failed run rolls both the target table and
its rejected table back to their pre-run version, leaving no partial writes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterable, Literal
from uuid import uuid4

from pipeline_reliability import (
    DEFAULT_AUDIT_TABLE,
    DEFAULT_RESCUED_COLUMN,
    PipelineAuditLogger,
    delete_existing_batches,
    evolve_table_schema_for_dataframe,
    extract_batch_ids,
    get_current_table_version,
    restore_table_version,
    split_rescued_records,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


SourceVariant = Literal["csv", "jsonl", "mixed"]


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    source_path: str
    source_format: str
    target_table: str
    source_system: str
    source_file_extension: str
    write_mode: str = "append"
    options: dict[str, str] = field(default_factory=dict)
    allow_schema_evolution: bool = True
    rejected_table: str | None = None
    rescued_column: str = DEFAULT_RESCUED_COLUMN
    capture_rescued: bool = True


def normalize_source_format(source_format: str) -> str:
    cleaned = source_format.strip().lower()
    if cleaned == "jsonl":
        return "json"
    if cleaned in {"csv", "json"}:
        return cleaned
    raise ValueError(f"Unsupported source format: {source_format}")


def _rejected_table_for(target_table: str) -> str:
    """Derive the rejected-records table name from a Bronze target table."""
    if target_table.endswith("_raw"):
        return target_table[: -len("_raw")] + "_rejected"
    return target_table + "_rejected"


def build_default_dataset_specs(data_root: str, variant: SourceVariant = "csv") -> list[DatasetSpec]:
    cleaned_root = data_root.rstrip("/")
    raw_root = f"{cleaned_root}/data/raw"

    if variant == "csv":
        return [
            DatasetSpec(
                name="flight_operations_raw",
                source_path=f"{raw_root}/flight_operations",
                source_format="csv",
                target_table="flight_delay.bronze.flight_operations_raw",
                source_system="bts_on_time",
                source_file_extension=".csv",
                options={"header": "true", "inferSchema": "true"},
                rejected_table="flight_delay.bronze.flight_operations_rejected",
            ),
            DatasetSpec(
                name="airports_raw",
                source_path=f"{raw_root}/airports",
                source_format="csv",
                target_table="flight_delay.bronze.airports_raw",
                source_system="ourairports",
                source_file_extension=".csv",
                options={"header": "true", "inferSchema": "true"},
                rejected_table="flight_delay.bronze.airports_rejected",
            ),
            DatasetSpec(
                name="aircraft_reference_raw",
                source_path=f"{raw_root}/aircraft_reference",
                source_format="csv",
                target_table="flight_delay.bronze.aircraft_reference_raw",
                source_system="aircraft_ref",
                source_file_extension=".csv",
                options={"header": "true", "inferSchema": "true"},
                rejected_table="flight_delay.bronze.aircraft_reference_rejected",
            ),
            DatasetSpec(
                name="weather_metar_raw",
                source_path=f"{raw_root}/weather_metar",
                source_format="csv",
                target_table="flight_delay.bronze.weather_metar_raw",
                source_system="noaa_metar",
                source_file_extension=".csv",
                options={"header": "true", "inferSchema": "true"},
                rejected_table="flight_delay.bronze.weather_metar_rejected",
            ),
        ]

    raise ValueError(f"Unsupported source variant: {variant}")


class BronzeIngestionJob:
    def __init__(
        self,
        spark: SparkSession,
        *,
        pipeline_name: str = "bronze_ingestion",
        audit_table: str = DEFAULT_AUDIT_TABLE,
    ):
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.audit_logger = PipelineAuditLogger(spark, audit_table)

    def ingest_all(
        self,
        specs: Iterable[DatasetSpec],
        *,
        batch_id: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        run_id = pipeline_run_id or str(uuid4())
        return [self.ingest_dataset(spec, batch_id=batch_id, pipeline_run_id=run_id) for spec in specs]

    def ingest_dataset(
        self,
        spec: DatasetSpec,
        *,
        batch_id: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> dict[str, Any]:
        run_id = pipeline_run_id or str(uuid4())
        stage_name = spec.name
        self.audit_logger.log_event(
            pipeline_name=self.pipeline_name,
            stage_name=stage_name,
            target_table=spec.target_table,
            run_id=run_id,
            batch_id=batch_id,
            status="STARTED",
            details={"source_path": spec.source_path, "source_format": spec.source_format},
        )

        previous_version = get_current_table_version(self.spark, spec.target_table)
        rejected_previous_version = (
            get_current_table_version(self.spark, spec.rejected_table)
            if spec.rejected_table
            else None
        )
        try:
            source_df = self._read_source(spec)
            clean_df, rescued_df = split_rescued_records(source_df, spec.rescued_column)

            rows_rescued = 0
            if spec.capture_rescued and spec.rejected_table:
                rows_rescued = self._write_rejected(rescued_df, spec, batch_id=batch_id)

            enriched_df = self._apply_metadata(clean_df, spec, batch_id=batch_id)
            evolved_columns = []
            if spec.allow_schema_evolution:
                evolved_columns = evolve_table_schema_for_dataframe(
                    self.spark,
                    spec.target_table,
                    enriched_df,
                    protected_columns=("_metadata", spec.rescued_column),
                )
            aligned_df = self._align_to_target_table(enriched_df, spec.target_table)
            batch_ids = extract_batch_ids(aligned_df)
            rows_removed = delete_existing_batches(self.spark, spec.target_table, batch_ids)
            row_count = aligned_df.count()
            (
                aligned_df.write.format("delta")
                .mode(spec.write_mode)
                .option("mergeSchema", "false")
                .saveAsTable(spec.target_table)
            )
            self.audit_logger.log_event(
                pipeline_name=self.pipeline_name,
                stage_name=stage_name,
                target_table=spec.target_table,
                run_id=run_id,
                batch_id=batch_id,
                status="SUCCESS",
                rows_written=row_count,
                rows_removed=rows_removed,
                rows_rescued=rows_rescued,
                details={
                    "evolved_columns": evolved_columns,
                    "batch_ids": list(batch_ids),
                    "rejected_table": spec.rejected_table,
                },
            )
            return {
                "dataset": spec.name,
                "source_path": spec.source_path,
                "target_table": spec.target_table,
                "rows_written": row_count,
                "rows_removed": rows_removed,
                "rows_rescued": rows_rescued,
                "rejected_table": spec.rejected_table,
                "evolved_columns": evolved_columns,
            }
        except Exception as exc:
            restored_version = restore_table_version(self.spark, spec.target_table, previous_version)
            restored_rejected_version = None
            if spec.rejected_table:
                restored_rejected_version = restore_table_version(
                    self.spark, spec.rejected_table, rejected_previous_version
                )
            self.audit_logger.log_event(
                pipeline_name=self.pipeline_name,
                stage_name=stage_name,
                target_table=spec.target_table,
                run_id=run_id,
                batch_id=batch_id,
                status="FAILED",
                restored_version=restored_version,
                error_message=str(exc),
                details={"restored_rejected_version": restored_rejected_version},
            )
            raise

    def _read_source(self, spec: DatasetSpec) -> DataFrame:
        reader = self.spark.read.format(normalize_source_format(spec.source_format))
        if spec.capture_rescued:
            reader = reader.option("rescuedDataColumn", spec.rescued_column)
        for key, value in spec.options.items():
            reader = reader.option(key, value)
        if normalize_source_format(spec.source_format) == "json":
            reader = reader.option("multiLine", "false")
        return reader.load(spec.source_path)

    def _write_rejected(
        self,
        rescued_df: DataFrame,
        spec: DatasetSpec,
        *,
        batch_id: str | None = None,
    ) -> int:
        from pyspark.sql import functions as F

        if rescued_df is None:
            return 0

        rescued_count = rescued_df.count()
        if rescued_count == 0:
            return 0

        if not self.spark.catalog.tableExists(spec.rejected_table):
            return rescued_count

        file_name_expr = F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+$)", 1)
        payload_columns = [c for c in rescued_df.columns if c != spec.rescued_column]

        rejected_records = rescued_df.select(
            F.lit(spec.target_table).alias("source_table"),
            F.lit(spec.source_system).alias("source_system"),
            file_name_expr.alias("source_file_name"),
            F.col(spec.rescued_column).cast("string").alias("rescued_data"),
            F.to_json(F.struct(*payload_columns)).alias("raw_payload"),
            F.lit(batch_id).alias("batch_id"),
            F.current_timestamp().alias("rejection_timestamp"),
        )

        aligned_rejected = self._align_to_target_table(rejected_records, spec.rejected_table)
        (
            aligned_rejected.write.format("delta")
            .mode("append")
            .option("mergeSchema", "false")
            .saveAsTable(spec.rejected_table)
        )
        return rescued_count

    def _apply_metadata(
        self,
        df: DataFrame,
        spec: DatasetSpec,
        *,
        batch_id: str | None = None,
    ) -> DataFrame:
        from pyspark.sql import functions as F

        file_name_expr = F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+$)", 1)

        if "source_file_name" in df.columns:
            df = df.withColumn(
                "source_file_name",
                F.when(
                    F.col("source_file_name").isNull() | (F.trim(F.col("source_file_name")) == ""),
                    file_name_expr,
                ).otherwise(F.col("source_file_name")),
            )
        else:
            df = df.withColumn("source_file_name", file_name_expr)

        if "source_system" in df.columns:
            df = df.withColumn(
                "source_system",
                F.when(
                    F.col("source_system").isNull() | (F.trim(F.col("source_system")) == ""),
                    F.lit(spec.source_system),
                ).otherwise(F.col("source_system")),
            )
        else:
            df = df.withColumn("source_system", F.lit(spec.source_system))

        if "ingestion_timestamp" in df.columns:
            df = df.withColumn(
                "ingestion_timestamp",
                F.coalesce(F.to_timestamp("ingestion_timestamp"), F.current_timestamp()),
            )
        else:
            df = df.withColumn("ingestion_timestamp", F.current_timestamp())

        if batch_id:
            df = df.withColumn("batch_id", F.lit(batch_id))

        return df

    def _align_to_target_table(self, df: DataFrame, target_table: str) -> DataFrame:
        from pyspark.sql import functions as F

        if not self.spark.catalog.tableExists(target_table):
            raise ValueError(
                f"Target table '{target_table}' does not exist. Run the DDL scripts first."
            )

        target_schema = self.spark.table(target_table).schema
        projected_columns = []
        for field in target_schema.fields:
            if field.name in df.columns:
                projected_columns.append(F.col(field.name).cast(field.dataType).alias(field.name))
            else:
                projected_columns.append(F.lit(None).cast(field.dataType).alias(field.name))
        return df.select(*projected_columns)


def create_job(spark: SparkSession) -> BronzeIngestionJob:
    return BronzeIngestionJob(spark)


if __name__ == "__main__":
    raise SystemExit(
        "Import this module from a Databricks notebook and call BronzeIngestionJob(spark)."
    )
