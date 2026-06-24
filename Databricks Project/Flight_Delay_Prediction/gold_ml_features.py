"""
Gold ML feature engineering for the Flight Delay Prediction pipeline.

Reads flight_delay.silver.flight_operations_clean and
flight_delay.silver.weather_observations_clean, engineers all model features,
and writes one row per flight to flight_delay.gold.ml_features.

Features produced:
  - Temporal:  dep_hour, dep_hour_bucket, day_of_week, month, season,
               is_weekend, is_holiday
  - Route:     route_avg_arr_delay_90d, route_delay_rate_90d  (window functions)
  - Carrier:   carrier_on_time_rate_30d, carrier_avg_dep_delay_30d
  - Aircraft:  distance_miles, aircraft_code
  - Weather:   origin temp, wind, visibility, precip, flight_category,
               composite weather_severity score (0-3)
  - Label:     is_delayed  (arrival delay >= 15 minutes)
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

DELAY_THRESHOLD_MINUTES = 15
ROUTE_WINDOW_DAYS = 90
CARRIER_WINDOW_DAYS = 30

# US federal holidays (month, day) — extend as needed
_US_HOLIDAYS: set[tuple[int, int]] = {
    (1, 1), (7, 4), (11, 11), (12, 25), (12, 26),
    (1, 15), (2, 19), (5, 27), (9, 2), (11, 28),
}


@dataclass(frozen=True)
class GoldMLWriteSpec:
    target_table: str
    write_mode: str = "overwrite"


class GoldMLFeaturesJob:
    def __init__(
        self,
        spark: SparkSession,
        *,
        pipeline_name: str = "gold_ml_features",
        audit_table: str = DEFAULT_AUDIT_TABLE,
    ):
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.audit_logger = PipelineAuditLogger(spark, audit_table)

    def run(self, *, pipeline_run_id: str | None = None) -> dict[str, Any]:
        run_id = pipeline_run_id or str(uuid4())
        spec = GoldMLWriteSpec("flight_delay.gold.ml_features")
        return self._write_dataframe(self.build_features(), spec, pipeline_run_id=run_id)

    # ------------------------------------------------------------------
    # Feature construction
    # ------------------------------------------------------------------

    def build_features(self) -> DataFrame:
        from pyspark.sql import functions as F

        flights_df = self.spark.table("flight_delay.silver.flight_operations_clean")

        base_df = self._add_temporal_features(flights_df)
        base_df = self._add_route_features(base_df)
        base_df = self._add_carrier_features(base_df)
        base_df = self._add_aircraft_features(base_df)
        base_df = self._add_weather_features(base_df)

        return base_df.withColumn(
            "is_delayed",
            F.coalesce(F.col("arrival_delay_minutes"), F.lit(0)) >= DELAY_THRESHOLD_MINUTES,
        ).select(
            # identity
            "flight_date",
            "reporting_airline",
            "flight_number",
            "origin_airport",
            "dest_airport",
            "route_code",
            "tail_number",
            # temporal
            "dep_hour",
            "dep_hour_bucket",
            "day_of_week",
            "month",
            "is_weekend",
            "is_holiday",
            "season",
            # route
            "route_avg_arr_delay_90d",
            "route_delay_rate_90d",
            # carrier
            "carrier_on_time_rate_30d",
            "carrier_avg_dep_delay_30d",
            # aircraft
            "distance_miles",
            "aircraft_code",
            # weather
            "origin_temp_c",
            "origin_wind_speed_kt",
            "origin_wind_gust_kt",
            "origin_visibility_mi",
            "origin_precip_in",
            "origin_flight_category",
            "origin_weather_severity",
            # label
            "is_delayed",
            # lineage
            "ingestion_timestamp",
            "batch_id",
            "year_month",
        )

    def _add_temporal_features(self, df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F

        holiday_pairs = [F.struct(F.lit(m).alias("m"), F.lit(d).alias("d")) for m, d in _US_HOLIDAYS]

        return (
            df.withColumn("dep_hour", F.hour(F.col("scheduled_departure_ts")))
            .withColumn(
                "dep_hour_bucket",
                F.when(F.col("dep_hour").between(0, 4), "red_eye")
                .when(F.col("dep_hour").between(5, 11), "morning")
                .when(F.col("dep_hour").between(12, 17), "afternoon")
                .otherwise("evening"),
            )
            .withColumn("day_of_week", F.dayofweek(F.col("flight_date")))
            .withColumn("month", F.month(F.col("flight_date")))
            .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
            .withColumn(
                "is_holiday",
                F.struct(
                    F.month(F.col("flight_date")).alias("m"),
                    F.dayofmonth(F.col("flight_date")).alias("d"),
                ).isin(holiday_pairs),
            )
            .withColumn(
                "season",
                F.when(F.col("month").isin(12, 1, 2), "winter")
                .when(F.col("month").isin(3, 4, 5), "spring")
                .when(F.col("month").isin(6, 7, 8), "summer")
                .otherwise("autumn"),
            )
        )

    def _add_route_features(self, df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        # Trailing 90-day window per route ordered by flight_date.
        # rangeBetween uses days (cast to long seconds for date type).
        route_window = (
            Window.partitionBy("route_code")
            .orderBy(F.col("flight_date").cast("long"))
            .rangeBetween(-(ROUTE_WINDOW_DAYS * 86400), -1)
        )

        return df.withColumn(
            "route_avg_arr_delay_90d",
            F.avg("arrival_delay_minutes").over(route_window),
        ).withColumn(
            "route_delay_rate_90d",
            F.avg(
                F.when(
                    F.coalesce(F.col("arrival_delay_minutes"), F.lit(0)) >= DELAY_THRESHOLD_MINUTES,
                    1.0,
                ).otherwise(0.0)
            ).over(route_window),
        )

    def _add_carrier_features(self, df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        carrier_window = (
            Window.partitionBy("reporting_airline")
            .orderBy(F.col("flight_date").cast("long"))
            .rangeBetween(-(CARRIER_WINDOW_DAYS * 86400), -1)
        )

        return df.withColumn(
            "carrier_on_time_rate_30d",
            F.avg(
                F.when(F.col("is_on_time_arrival_flag"), 1.0).otherwise(0.0)
            ).over(carrier_window),
        ).withColumn(
            "carrier_avg_dep_delay_30d",
            F.avg("departure_delay_minutes").over(carrier_window),
        )

    def _add_aircraft_features(self, df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F

        # Extract equipment type from tail number suffix as a proxy for aircraft code.
        return df.withColumn(
            "aircraft_code",
            F.upper(F.regexp_extract(F.col("tail_number"), r"([A-Z0-9]{2,4})$", 1)),
        )

    def _add_weather_features(self, df: DataFrame) -> DataFrame:
        from pyspark.sql import functions as F

        weather_df = (
            self.spark.table("flight_delay.silver.weather_observations_clean")
            .withColumn("obs_hour", F.hour(F.col("observation_time")))
            .select(
                F.col("station_id").alias("w_station_id"),
                F.col("observation_date").alias("w_obs_date"),
                F.col("obs_hour").alias("w_obs_hour"),
                F.col("temp_c").alias("origin_temp_c"),
                F.col("wind_speed_kt").alias("origin_wind_speed_kt"),
                F.col("wind_gust_kt").alias("origin_wind_gust_kt"),
                F.col("visibility_statute_mi").alias("origin_visibility_mi"),
                F.col("precip_in").alias("origin_precip_in"),
                F.col("flight_category").alias("origin_flight_category"),
            )
        )

        # Join on (origin_airport ≈ station_id, flight_date, dep_hour).
        # station_id in METAR data matches the IATA code for major US airports.
        joined_df = df.join(
            weather_df,
            (F.col("origin_airport") == F.col("w_station_id"))
            & (F.col("flight_date") == F.col("w_obs_date"))
            & (F.col("dep_hour") == F.col("w_obs_hour")),
            how="left",
        ).drop("w_station_id", "w_obs_date", "w_obs_hour")

        # Composite severity: 0=clear(VFR), 1=marginal(MVFR), 2=poor(IFR), 3=severe(LIFR)
        severity_expr = (
            F.when(F.col("origin_flight_category") == "LIFR", 3)
            .when(F.col("origin_flight_category") == "IFR", 2)
            .when(F.col("origin_flight_category") == "MVFR", 1)
            .otherwise(0)
        )

        return joined_df.withColumn("origin_weather_severity", severity_expr)

    # ------------------------------------------------------------------
    # Write helper
    # ------------------------------------------------------------------

    def _align_to_target_table(self, df: DataFrame, target_table: str) -> DataFrame:
        from pyspark.sql import functions as F

        if not self.spark.catalog.tableExists(target_table):
            raise ValueError(
                f"Target table '{target_table}' does not exist. Run sql/ddl/040_gold_ml_tables.sql first."
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
        spec: GoldMLWriteSpec,
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


def create_job(spark: SparkSession) -> GoldMLFeaturesJob:
    return GoldMLFeaturesJob(spark)


if __name__ == "__main__":
    raise SystemExit(
        "Import this module from a Databricks notebook and call GoldMLFeaturesJob(spark)."
    )
