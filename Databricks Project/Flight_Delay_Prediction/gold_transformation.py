"""
Gold analytics transformation for the Flight Delay Prediction pipeline.

Reads Silver Delta tables and produces reporting aggregates (on-time
performance, route delay summaries, airport summaries, cancellations) in the
flight_delay.gold schema.

For ML feature engineering see gold_ml_features.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from pipeline_reliability import (
    DEFAULT_AUDIT_TABLE,
    PipelineAuditLogger,
    get_current_table_version,
    restore_table_version,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


@dataclass(frozen=True)
class GoldWriteSpec:
    target_table: str
    write_mode: str = "overwrite"


class GoldTransformationJob:
    def __init__(
        self,
        spark: SparkSession,
        *,
        pipeline_name: str = "gold_transformation",
        audit_table: str = DEFAULT_AUDIT_TABLE,
    ):
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.audit_logger = PipelineAuditLogger(spark, audit_table)

    def run_all(self, *, pipeline_run_id: str | None = None) -> list[dict[str, Any]]:
        run_id = pipeline_run_id or str(uuid4())
        return [
            self._write_dataframe(
                self.transform_on_time_performance_daily(),
                GoldWriteSpec("flight_delay.gold.on_time_performance_daily"),
                pipeline_run_id=run_id,
            ),
            self._write_dataframe(
                self.transform_route_delay_summary(),
                GoldWriteSpec("flight_delay.gold.route_delay_summary"),
                pipeline_run_id=run_id,
            ),
            self._write_dataframe(
                self.transform_airport_delay_summary(),
                GoldWriteSpec("flight_delay.gold.airport_delay_summary"),
                pipeline_run_id=run_id,
            ),
            self._write_dataframe(
                self.transform_cancellation_summary(),
                GoldWriteSpec("flight_delay.gold.cancellation_summary"),
                pipeline_run_id=run_id,
            ),
        ]

    def transform_on_time_performance_daily(self) -> DataFrame:
        from pyspark.sql import functions as F

        flights_df = self.spark.table("flight_delay.silver.flight_operations_clean")
        return (
            flights_df.groupBy("flight_date", "reporting_airline")
            .agg(
                F.count("*").alias("total_flights"),
                F.sum(F.when(~F.col("cancelled_flag"), 1).otherwise(0)).alias("completed_flights"),
                F.sum(F.when(F.col("cancelled_flag"), 1).otherwise(0)).alias("cancelled_flights"),
                F.sum(F.when(F.col("diverted_flag"), 1).otherwise(0)).alias("diverted_flights"),
                F.sum(
                    F.when(F.coalesce(F.col("departure_delay_minutes"), F.lit(0)) > 15, 1).otherwise(0)
                ).alias("delayed_departure_flights"),
                F.sum(
                    F.when(F.coalesce(F.col("arrival_delay_minutes"), F.lit(0)) > 15, 1).otherwise(0)
                ).alias("delayed_arrival_flights"),
                F.sum(F.when(F.col("is_on_time_departure_flag"), 1).otherwise(0)).alias(
                    "on_time_departure_flights"
                ),
                F.sum(F.when(F.col("is_on_time_arrival_flag"), 1).otherwise(0)).alias(
                    "on_time_arrival_flights"
                ),
                F.avg(F.col("departure_delay_minutes")).alias("avg_departure_delay_minutes"),
                F.avg(F.col("arrival_delay_minutes")).alias("avg_arrival_delay_minutes"),
                F.sum(F.coalesce(F.col("weather_delay_minutes"), F.lit(0))).alias("total_weather_delay_minutes"),
                F.sum(F.coalesce(F.col("carrier_delay_minutes"), F.lit(0))).alias("total_carrier_delay_minutes"),
                F.sum(F.coalesce(F.col("nas_delay_minutes"), F.lit(0))).alias("total_nas_delay_minutes"),
                F.sum(F.coalesce(F.col("security_delay_minutes"), F.lit(0))).alias(
                    "total_security_delay_minutes"
                ),
                F.sum(F.coalesce(F.col("late_aircraft_delay_minutes"), F.lit(0))).alias(
                    "total_late_aircraft_delay_minutes"
                ),
            )
            .withColumn(
                "on_time_arrival_percentage",
                F.when(
                    F.col("completed_flights") > 0,
                    (F.col("on_time_arrival_flights") * F.lit(100.0)) / F.col("completed_flights"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn("load_timestamp", F.current_timestamp())
        )

    def transform_route_delay_summary(self) -> DataFrame:
        from pyspark.sql import functions as F

        flights_df = self.spark.table("flight_delay.silver.flight_operations_clean")
        return (
            flights_df.groupBy(
                "flight_date",
                "route_code",
                "origin_airport",
                "dest_airport",
                "reporting_airline",
            )
            .agg(
                F.count("*").alias("total_flights"),
                F.sum(F.when(~F.col("cancelled_flag"), 1).otherwise(0)).alias("completed_flights"),
                F.sum(F.when(F.col("cancelled_flag"), 1).otherwise(0)).alias("cancelled_flights"),
                F.sum(
                    F.when(F.coalesce(F.col("arrival_delay_minutes"), F.lit(0)) > 15, 1).otherwise(0)
                ).alias("delayed_arrival_flights"),
                F.avg(F.col("departure_delay_minutes")).alias("avg_departure_delay_minutes"),
                F.avg(F.col("arrival_delay_minutes")).alias("avg_arrival_delay_minutes"),
                F.max(F.col("arrival_delay_minutes")).alias("max_arrival_delay_minutes"),
                F.sum(F.coalesce(F.col("weather_delay_minutes"), F.lit(0))).alias("total_weather_delay_minutes"),
                F.sum(F.coalesce(F.col("carrier_delay_minutes"), F.lit(0))).alias("total_carrier_delay_minutes"),
                F.sum(F.coalesce(F.col("late_aircraft_delay_minutes"), F.lit(0))).alias(
                    "total_late_aircraft_delay_minutes"
                ),
            )
            .withColumn("load_timestamp", F.current_timestamp())
        )

    def transform_airport_delay_summary(self) -> DataFrame:
        from pyspark.sql import functions as F

        flights_df = self.spark.table("flight_delay.silver.flight_operations_clean")

        departures_df = flights_df.select(
            "flight_date",
            F.col("origin_airport").alias("airport_code"),
            F.lit("DEPARTURE").alias("airport_role"),
            "reporting_airline",
            "cancelled_flag",
            "is_on_time_departure_flag",
            F.coalesce(F.col("departure_delay_minutes"), F.lit(0)).alias("delay_minutes"),
            F.coalesce(F.col("weather_delay_minutes"), F.lit(0)).alias("weather_delay_minutes"),
            F.coalesce(F.col("carrier_delay_minutes"), F.lit(0)).alias("carrier_delay_minutes"),
            F.coalesce(F.col("late_aircraft_delay_minutes"), F.lit(0)).alias("late_aircraft_delay_minutes"),
        )

        arrivals_df = flights_df.select(
            "flight_date",
            F.col("dest_airport").alias("airport_code"),
            F.lit("ARRIVAL").alias("airport_role"),
            "reporting_airline",
            "cancelled_flag",
            F.col("is_on_time_arrival_flag").alias("is_on_time_departure_flag"),
            F.coalesce(F.col("arrival_delay_minutes"), F.lit(0)).alias("delay_minutes"),
            F.coalesce(F.col("weather_delay_minutes"), F.lit(0)).alias("weather_delay_minutes"),
            F.coalesce(F.col("carrier_delay_minutes"), F.lit(0)).alias("carrier_delay_minutes"),
            F.coalesce(F.col("late_aircraft_delay_minutes"), F.lit(0)).alias("late_aircraft_delay_minutes"),
        )

        return (
            departures_df.unionByName(arrivals_df)
            .groupBy("flight_date", "airport_code", "airport_role", "reporting_airline")
            .agg(
                F.count("*").alias("total_flights"),
                F.sum(F.when(~F.col("cancelled_flag"), 1).otherwise(0)).alias("completed_flights"),
                F.sum(F.when(F.col("cancelled_flag"), 1).otherwise(0)).alias("cancelled_flights"),
                F.sum(F.when(F.col("delay_minutes") > 15, 1).otherwise(0)).alias("delayed_flights"),
                F.sum(F.when(F.col("is_on_time_departure_flag"), 1).otherwise(0)).alias("on_time_flights"),
                F.avg(F.col("delay_minutes")).alias("avg_delay_minutes"),
                F.max(F.col("delay_minutes")).alias("max_delay_minutes"),
                F.sum(F.col("weather_delay_minutes")).alias("total_weather_delay_minutes"),
                F.sum(F.col("carrier_delay_minutes")).alias("total_carrier_delay_minutes"),
                F.sum(F.col("late_aircraft_delay_minutes")).alias("total_late_aircraft_delay_minutes"),
            )
            .withColumn("load_timestamp", F.current_timestamp())
        )

    def transform_cancellation_summary(self) -> DataFrame:
        from pyspark.sql import functions as F

        flights_df = self.spark.table("flight_delay.silver.flight_operations_clean")
        reason_map = (
            F.when(F.col("cancellation_code") == "A", F.lit("Carrier"))
            .when(F.col("cancellation_code") == "B", F.lit("Weather"))
            .when(F.col("cancellation_code") == "C", F.lit("National Air System"))
            .when(F.col("cancellation_code") == "D", F.lit("Security"))
            .otherwise(F.lit("Unknown"))
        )

        return (
            flights_df.filter(F.col("cancelled_flag"))
            .groupBy(
                "flight_date",
                "reporting_airline",
                "origin_airport",
                "dest_airport",
                "cancellation_code",
            )
            .agg(F.count("*").alias("cancellation_count"))
            .withColumn("cancellation_reason", reason_map)
            .withColumn("load_timestamp", F.current_timestamp())
        )

    def _align_to_target_table(self, df: DataFrame, target_table: str) -> DataFrame:
        from pyspark.sql import functions as F

        if not self.spark.catalog.tableExists(target_table):
            raise ValueError(
                f"Target table '{target_table}' does not exist. Run the Gold DDL first."
            )

        target_schema = self.spark.table(target_table).schema
        projected_columns = []
        for field in target_schema.fields:
            if field.name in df.columns:
                projected_columns.append(F.col(field.name).cast(field.dataType).alias(field.name))
            else:
                projected_columns.append(F.lit(None).cast(field.dataType).alias(field.name))
        return df.select(*projected_columns)

    def _write_dataframe(
        self,
        df: DataFrame,
        spec: GoldWriteSpec,
        *,
        pipeline_run_id: str,
    ) -> dict[str, Any]:
        stage_name = spec.target_table.split(".")[-1]
        self.audit_logger.log_event(
            pipeline_name=self.pipeline_name,
            stage_name=stage_name,
            target_table=spec.target_table,
            run_id=pipeline_run_id,
            batch_id=None,
            status="STARTED",
            details={"write_mode": spec.write_mode},
        )

        previous_version = get_current_table_version(self.spark, spec.target_table)
        try:
            staged_df = self._align_to_target_table(df, spec.target_table)
            row_count = staged_df.count()
            (
                staged_df.write.format("delta")
                .mode(spec.write_mode)
                .option("overwriteSchema", "true")
                .saveAsTable(spec.target_table)
            )
            self.audit_logger.log_event(
                pipeline_name=self.pipeline_name,
                stage_name=stage_name,
                target_table=spec.target_table,
                run_id=pipeline_run_id,
                batch_id=None,
                status="SUCCESS",
                rows_written=row_count,
                details={"write_mode": spec.write_mode},
            )
            return {
                "target_table": spec.target_table,
                "rows_written": row_count,
                "write_mode": spec.write_mode,
            }
        except Exception as exc:
            restored_version = restore_table_version(self.spark, spec.target_table, previous_version)
            self.audit_logger.log_event(
                pipeline_name=self.pipeline_name,
                stage_name=stage_name,
                target_table=spec.target_table,
                run_id=pipeline_run_id,
                batch_id=None,
                status="FAILED",
                restored_version=restored_version,
                error_message=str(exc),
                details={"write_mode": spec.write_mode},
            )
            raise


def create_job(spark: SparkSession) -> GoldTransformationJob:
    return GoldTransformationJob(spark)


if __name__ == "__main__":
    raise SystemExit(
        "Import this module from a Databricks notebook and call GoldTransformationJob(spark)."
    )
