"""
Silver transformation for the Flight Delay Prediction pipeline.

Reads Bronze Delta tables, validates and cleans each dataset, joins flights
with weather on (flight_date, origin_airport), and writes to Silver Delta
tables under the flight_delay catalog.

Every write is idempotent: existing rows for a given batch_id are deleted
before the new rows are inserted. A failed stage rolls back via RESTORE TABLE.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from pipeline_reliability import (
    DEFAULT_AUDIT_TABLE,
    PipelineAuditLogger,
    delete_existing_batches,
    extract_batch_ids,
    get_current_table_version,
    restore_table_version,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


@dataclass(frozen=True)
class SilverWriteSpec:
    target_table: str
    temp_view: str
    identity_columns: tuple[str, ...] = ()


class SilverTransformationJob:
    def __init__(
        self,
        spark: SparkSession,
        *,
        pipeline_name: str = "silver_transformation",
        audit_table: str = DEFAULT_AUDIT_TABLE,
    ):
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.audit_logger = PipelineAuditLogger(spark, audit_table)

    def run_all(self, *, pipeline_run_id: str | None = None) -> list[dict[str, Any]]:
        run_id = pipeline_run_id or str(uuid4())
        airports_df = self.transform_airports()
        aircraft_df = self.transform_aircraft_reference()
        weather_df = self.transform_weather_observations()
        valid_flights_df, quarantine_df = self.transform_flight_operations()
        route_df = self.transform_route_reference(valid_flights_df)

        return [
            self._write_dataframe(
                airports_df,
                SilverWriteSpec(
                    target_table="flight_delay.silver.airports_clean",
                    temp_view="silver_airports_clean_stage",
                    identity_columns=("airport_key",),
                ),
                pipeline_run_id=run_id,
            ),
            self._write_dataframe(
                aircraft_df,
                SilverWriteSpec(
                    target_table="flight_delay.silver.aircraft_reference_clean",
                    temp_view="silver_aircraft_reference_clean_stage",
                    identity_columns=("aircraft_key",),
                ),
                pipeline_run_id=run_id,
            ),
            self._write_dataframe(
                weather_df,
                SilverWriteSpec(
                    target_table="flight_delay.silver.weather_observations_clean",
                    temp_view="silver_weather_observations_clean_stage",
                    identity_columns=("weather_key",),
                ),
                pipeline_run_id=run_id,
            ),
            self._write_dataframe(
                valid_flights_df,
                SilverWriteSpec(
                    target_table="flight_delay.silver.flight_operations_clean",
                    temp_view="silver_flight_operations_clean_stage",
                    identity_columns=("flight_operation_key",),
                ),
                pipeline_run_id=run_id,
            ),
            self._write_dataframe(
                quarantine_df,
                SilverWriteSpec(
                    target_table="flight_delay.silver.flight_operations_quarantine",
                    temp_view="silver_flight_operations_quarantine_stage",
                    identity_columns=("quarantine_key",),
                ),
                pipeline_run_id=run_id,
            ),
            self._write_dataframe(
                route_df,
                SilverWriteSpec(
                    target_table="flight_delay.silver.route_reference",
                    temp_view="silver_route_reference_stage",
                    identity_columns=("route_key",),
                ),
                pipeline_run_id=run_id,
            ),
        ]

    def transform_airports(self) -> DataFrame:
        from pyspark.sql import functions as F

        bronze_df = self.spark.table("flight_delay.bronze.airports_raw")
        return (
            bronze_df.filter(
                F.col("iata_code").isNotNull() | F.col("icao_code").isNotNull()
            )
            .dropDuplicates(["iata_code", "icao_code", "ident"])
            .select(
                F.upper(F.col("iata_code")).alias("airport_iata_code"),
                F.upper(F.col("icao_code")).alias("airport_icao_code"),
                F.upper(F.col("ident")).alias("airport_ident"),
                F.col("name").alias("airport_name"),
                F.col("type").alias("airport_type"),
                F.col("municipality"),
                F.upper(F.col("iso_country")).alias("iso_country"),
                F.upper(F.col("iso_region")).alias("iso_region"),
                F.upper(F.col("continent")).alias("continent"),
                F.col("latitude_deg").cast("double").alias("latitude_deg"),
                F.col("longitude_deg").cast("double").alias("longitude_deg"),
                F.col("elevation_ft").cast("double").alias("elevation_ft"),
                (
                    F.lower(F.coalesce(F.col("scheduled_service"), F.lit("no")))
                    == F.lit("yes")
                ).alias("scheduled_service_flag"),
                F.col("ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
                F.col("batch_id").alias("batch_id"),
            )
        )

    def transform_aircraft_reference(self) -> DataFrame:
        from pyspark.sql import functions as F

        bronze_df = self.spark.table("flight_delay.bronze.aircraft_reference_raw")
        return (
            bronze_df.filter(F.col("aircraft_code").isNotNull())
            .dropDuplicates(["aircraft_code"])
            .select(
                F.upper(F.col("aircraft_code")).alias("aircraft_code"),
                F.col("aircraft_name").alias("aircraft_name"),
                F.upper(F.col("iata_code")).alias("iata_code"),
                F.upper(F.col("icao_code")).alias("icao_code"),
                F.col("ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
                F.col("batch_id").alias("batch_id"),
            )
        )

    def transform_weather_observations(self) -> DataFrame:
        from pyspark.sql import functions as F

        bronze_df = self.spark.table("flight_delay.bronze.weather_metar_raw")
        return (
            bronze_df.filter(F.col("station_id").isNotNull())
            .dropDuplicates(["station_id", "observation_time"])
            .select(
                F.upper(F.col("station_id")).alias("station_id"),
                F.col("observation_time").cast("timestamp").alias("observation_time"),
                F.col("observation_date").cast("date").alias("observation_date"),
                F.col("temp_c").cast("double").alias("temp_c"),
                F.col("dewpoint_c").cast("double").alias("dewpoint_c"),
                F.col("wind_dir_degrees").cast("int").alias("wind_dir_degrees"),
                F.col("wind_speed_kt").cast("double").alias("wind_speed_kt"),
                F.col("wind_gust_kt").cast("double").alias("wind_gust_kt"),
                F.col("visibility_statute_mi").cast("double").alias("visibility_statute_mi"),
                F.col("altim_in_hg").cast("double").alias("altim_in_hg"),
                F.upper(F.col("flight_category")).alias("flight_category"),
                F.col("precip_in").cast("double").alias("precip_in"),
                F.upper(F.col("sky_cover")).alias("sky_cover"),
                F.col("ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
                F.col("batch_id").alias("batch_id"),
            )
        )

    def transform_flight_operations(self) -> tuple[DataFrame, DataFrame]:
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        bronze_df = self.spark.table("flight_delay.bronze.flight_operations_raw")

        base_df = (
            bronze_df.withColumn("flight_date", F.col("flight_date").cast("date"))
            .withColumn("origin_airport", F.upper(F.trim(F.col("origin_airport"))))
            .withColumn("dest_airport", F.upper(F.trim(F.col("dest_airport"))))
            .withColumn("flight_number", F.trim(F.col("flight_number")))
            .withColumn("year_month", F.date_format(F.col("flight_date"), "yyyy-MM"))
            .withColumn("route_code", F.concat_ws("-", F.col("origin_airport"), F.col("dest_airport")))
            .withColumn(
                "scheduled_departure_ts",
                self._build_timestamp("flight_date", "crs_dep_time"),
            )
            .withColumn(
                "scheduled_arrival_base_ts",
                self._build_timestamp("flight_date", "crs_arr_time"),
            )
            .withColumn(
                "scheduled_arrival_ts",
                F.when(
                    F.col("scheduled_arrival_base_ts") < F.col("scheduled_departure_ts"),
                    F.col("scheduled_arrival_base_ts") + F.expr("INTERVAL 1 DAY"),
                ).otherwise(F.col("scheduled_arrival_base_ts")),
            )
            .withColumn(
                "actual_departure_ts",
                F.when(
                    F.col("cancelled").cast("int") == 1,
                    F.lit(None).cast("timestamp"),
                ).otherwise(self._build_timestamp("flight_date", "dep_time")),
            )
            .withColumn(
                "actual_arrival_base_ts",
                F.when(
                    F.col("cancelled").cast("int") == 1,
                    F.lit(None).cast("timestamp"),
                ).otherwise(self._build_timestamp("flight_date", "arr_time")),
            )
            .withColumn(
                "actual_arrival_ts",
                F.when(
                    F.col("actual_arrival_base_ts").isNull(),
                    F.lit(None).cast("timestamp"),
                ).when(
                    F.col("actual_arrival_base_ts") < F.col("actual_departure_ts"),
                    F.col("actual_arrival_base_ts") + F.expr("INTERVAL 1 DAY"),
                ).otherwise(F.col("actual_arrival_base_ts")),
            )
            .withColumn("cancelled_flag", F.col("cancelled").cast("int") == 1)
            .withColumn("diverted_flag", F.col("diverted").cast("int") == 1)
            .withColumn("departure_delay_minutes", F.col("dep_delay_minutes").cast("int"))
            .withColumn("arrival_delay_minutes", F.col("arr_delay_minutes").cast("int"))
            .withColumn("taxi_out_minutes", F.col("taxi_out").cast("int"))
            .withColumn("taxi_in_minutes", F.col("taxi_in").cast("int"))
            .withColumn("carrier_delay_minutes", F.col("carrier_delay").cast("int"))
            .withColumn("weather_delay_minutes", F.col("weather_delay").cast("int"))
            .withColumn("nas_delay_minutes", F.col("nas_delay").cast("int"))
            .withColumn("security_delay_minutes", F.col("security_delay").cast("int"))
            .withColumn(
                "late_aircraft_delay_minutes",
                F.col("late_aircraft_delay").cast("int"),
            )
            .withColumn(
                "quality_issue_reason",
                F.when(
                    F.col("flight_number").isNull() | (F.col("flight_number") == ""),
                    F.lit("MISSING_FLIGHT_NUMBER"),
                )
                .when(
                    F.col("origin_airport").isNull()
                    | F.col("dest_airport").isNull()
                    | (F.col("origin_airport") == "")
                    | (F.col("dest_airport") == ""),
                    F.lit("MISSING_AIRPORT_CODE"),
                )
                .when(F.col("origin_airport") == F.col("dest_airport"), F.lit("INVALID_ROUTE"))
                .when(F.col("scheduled_departure_ts").isNull(), F.lit("INVALID_SCHEDULED_DEPARTURE"))
                .when(F.col("scheduled_arrival_ts").isNull(), F.lit("INVALID_SCHEDULED_ARRIVAL"))
                .when(
                    (~F.col("cancelled_flag")) & F.col("actual_departure_ts").isNull(),
                    F.lit("MISSING_ACTUAL_DEPARTURE"),
                )
                .when(
                    (~F.col("cancelled_flag")) & F.col("actual_arrival_ts").isNull(),
                    F.lit("MISSING_ACTUAL_ARRIVAL"),
                )
                .otherwise(F.lit(None).cast("string")),
            )
        )

        dedupe_window = Window.partitionBy(
            "flight_date",
            "reporting_airline",
            "flight_number",
            "origin_airport",
            "dest_airport",
        ).orderBy(
            F.col("ingestion_timestamp").desc_nulls_last(),
            F.col("record_hash").desc_nulls_last(),
        )

        deduped_df = (
            base_df.withColumn("dedupe_rank", F.row_number().over(dedupe_window))
            .filter(F.col("dedupe_rank") == 1)
            .drop("dedupe_rank", "scheduled_arrival_base_ts", "actual_arrival_base_ts")
        )

        valid_df = (
            deduped_df.filter(F.col("quality_issue_reason").isNull())
            .withColumn("is_delayed_flag", F.coalesce(F.col("arrival_delay_minutes"), F.lit(0)) > 15)
            .withColumn(
                "is_on_time_arrival_flag",
                (~F.col("cancelled_flag"))
                & (F.coalesce(F.col("arrival_delay_minutes"), F.lit(0)) <= 15),
            )
            .withColumn(
                "is_on_time_departure_flag",
                (~F.col("cancelled_flag"))
                & (F.coalesce(F.col("departure_delay_minutes"), F.lit(0)) <= 15),
            )
            .withColumn("processing_status", F.lit("VALID"))
            .select(
                "flight_date",
                "reporting_airline",
                "reporting_airline_name",
                "tail_number",
                "flight_number",
                "origin_airport",
                "dest_airport",
                "route_code",
                "origin_city_name",
                "origin_state_abbr",
                "dest_city_name",
                "dest_state_abbr",
                "scheduled_departure_ts",
                "actual_departure_ts",
                "scheduled_arrival_ts",
                "actual_arrival_ts",
                "departure_delay_minutes",
                "arrival_delay_minutes",
                "taxi_out_minutes",
                "taxi_in_minutes",
                "cancelled_flag",
                "cancellation_code",
                "diverted_flag",
                "distance_miles",
                "carrier_delay_minutes",
                "weather_delay_minutes",
                "nas_delay_minutes",
                "security_delay_minutes",
                "late_aircraft_delay_minutes",
                "is_delayed_flag",
                "is_on_time_arrival_flag",
                "is_on_time_departure_flag",
                "processing_status",
                "quality_issue_reason",
                "ingestion_timestamp",
                "batch_id",
                "source_file_name",
                "record_hash",
                "year_month",
            )
        )

        quarantine_df = (
            deduped_df.filter(F.col("quality_issue_reason").isNotNull())
            .select(
                F.col("flight_date").cast("string").alias("raw_flight_date"),
                "reporting_airline",
                "flight_number",
                "origin_airport",
                "dest_airport",
                "quality_issue_reason",
                "raw_record",
                "source_file_name",
                "ingestion_timestamp",
                "batch_id",
                "record_hash",
            )
        )

        return valid_df, quarantine_df

    def transform_route_reference(self, valid_flights_df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F

        return (
            valid_flights_df.groupBy("origin_airport", "dest_airport", "route_code")
            .agg(
                F.min("flight_date").alias("first_seen_flight_date"),
                F.max("flight_date").alias("last_seen_flight_date"),
                F.count("*").alias("total_record_count"),
                F.max("ingestion_timestamp").alias("ingestion_timestamp"),
                F.max("batch_id").alias("batch_id"),
            )
            .withColumn(
                "route_group",
                F.concat_ws("_", F.col("origin_airport"), F.col("dest_airport")),
            )
            .select(
                "origin_airport",
                "dest_airport",
                "route_code",
                "route_group",
                "first_seen_flight_date",
                "last_seen_flight_date",
                "total_record_count",
                "ingestion_timestamp",
                "batch_id",
            )
        )

    def _build_timestamp(self, date_col: str, hhmm_col: str):
        from pyspark.sql import functions as F

        normalized_hhmm = F.lpad(F.regexp_replace(F.trim(F.col(hhmm_col)), r"[^0-9]", ""), 4, "0")
        return F.when(
            F.col(hhmm_col).isNull() | (F.trim(F.col(hhmm_col)) == ""),
            F.lit(None).cast("timestamp"),
        ).otherwise(
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.date_format(F.col(date_col), "yyyy-MM-dd"),
                    F.concat_ws(
                        ":",
                        F.substring(normalized_hhmm, 1, 2),
                        F.substring(normalized_hhmm, 3, 2),
                    ),
                ),
                "yyyy-MM-dd HH:mm",
            )
        )

    def _align_to_target_table(
        self,
        df: DataFrame,
        target_table: str,
        identity_columns: tuple[str, ...] = (),
    ) -> DataFrame:
        from pyspark.sql import functions as F

        if not self.spark.catalog.tableExists(target_table):
            raise ValueError(
                f"Target table '{target_table}' does not exist. Run the Silver DDL first."
            )

        target_schema = self.spark.table(target_table).schema
        projected_columns = []
        for field in target_schema.fields:
            if field.name in identity_columns:
                continue
            if field.name in df.columns:
                projected_columns.append(F.col(field.name).cast(field.dataType).alias(field.name))
            else:
                projected_columns.append(F.lit(None).cast(field.dataType).alias(field.name))
        return df.select(*projected_columns)

    def _write_dataframe(
        self,
        df: DataFrame,
        spec: SilverWriteSpec,
        *,
        pipeline_run_id: str,
    ) -> dict[str, Any]:
        stage_name = spec.target_table.split(".")[-1]
        staged_df = self._align_to_target_table(
            df,
            spec.target_table,
            identity_columns=spec.identity_columns,
        )
        batch_ids = extract_batch_ids(staged_df)
        self.audit_logger.log_event(
            pipeline_name=self.pipeline_name,
            stage_name=stage_name,
            target_table=spec.target_table,
            run_id=pipeline_run_id,
            batch_id=batch_ids[0] if batch_ids else None,
            status="STARTED",
            details={"batch_ids": list(batch_ids)},
        )

        previous_version = get_current_table_version(self.spark, spec.target_table)
        try:
            rows_removed = delete_existing_batches(self.spark, spec.target_table, batch_ids)
            staged_df.createOrReplaceTempView(spec.temp_view)
            target_columns = staged_df.columns
            insert_sql = f"""
            INSERT INTO {spec.target_table} ({", ".join(target_columns)})
            SELECT {", ".join(target_columns)}
            FROM {spec.temp_view}
            """
            self.spark.sql(insert_sql)
            rows_written = staged_df.count()
            self.audit_logger.log_event(
                pipeline_name=self.pipeline_name,
                stage_name=stage_name,
                target_table=spec.target_table,
                run_id=pipeline_run_id,
                batch_id=batch_ids[0] if batch_ids else None,
                status="SUCCESS",
                rows_written=rows_written,
                rows_removed=rows_removed,
                details={"batch_ids": list(batch_ids)},
            )
            return {
                "target_table": spec.target_table,
                "rows_written": rows_written,
                "rows_removed": rows_removed,
            }
        except Exception as exc:
            restored_version = restore_table_version(self.spark, spec.target_table, previous_version)
            self.audit_logger.log_event(
                pipeline_name=self.pipeline_name,
                stage_name=stage_name,
                target_table=spec.target_table,
                run_id=pipeline_run_id,
                batch_id=batch_ids[0] if batch_ids else None,
                status="FAILED",
                restored_version=restored_version,
                error_message=str(exc),
                details={"batch_ids": list(batch_ids)},
            )
            raise


def create_job(spark: SparkSession) -> SilverTransformationJob:
    return SilverTransformationJob(spark)


if __name__ == "__main__":
    raise SystemExit(
        "Import this module from a Databricks notebook and call SilverTransformationJob(spark)."
    )
