-- Gold ML layer — feature-engineered table consumed by MLflow training jobs.
-- Produces one row per flight with all features ready for model ingestion.
-- Run after 020_silver_tables.sql.

CREATE TABLE IF NOT EXISTS flight_delay.gold.ml_features (
    -- identity
    flight_date              DATE,
    reporting_airline        STRING,
    flight_number            STRING,
    origin_airport           STRING,
    dest_airport             STRING,
    route_code               STRING,
    tail_number              STRING,

    -- temporal features
    dep_hour                 INT,       -- scheduled departure hour (0-23)
    dep_hour_bucket          STRING,    -- red_eye / morning / afternoon / evening
    day_of_week              INT,       -- 1=Mon … 7=Sun
    month                    INT,
    is_weekend               BOOLEAN,
    is_holiday               BOOLEAN,
    season                   STRING,    -- spring / summer / autumn / winter

    -- route features (trailing 90-day window)
    route_avg_arr_delay_90d  DOUBLE,
    route_delay_rate_90d     DOUBLE,    -- fraction of flights delayed ≥15 min

    -- carrier features (trailing 30-day window)
    carrier_on_time_rate_30d DOUBLE,
    carrier_avg_dep_delay_30d DOUBLE,

    -- aircraft features
    distance_miles           DOUBLE,
    aircraft_code            STRING,

    -- weather features at origin at scheduled departure hour
    origin_temp_c            DOUBLE,
    origin_wind_speed_kt     DOUBLE,
    origin_wind_gust_kt      DOUBLE,
    origin_visibility_mi     DOUBLE,
    origin_precip_in         DOUBLE,
    origin_flight_category   STRING,    -- VFR / MVFR / IFR / LIFR
    origin_weather_severity  INT,       -- 0=clear, 1=marginal, 2=poor, 3=severe

    -- label (target variable)
    is_delayed               BOOLEAN,   -- arrival delay ≥ 15 minutes

    -- lineage
    ingestion_timestamp      TIMESTAMP,
    batch_id                 STRING,
    year_month               STRING
)
USING DELTA
PARTITIONED BY (year_month);


-- Batch predictions written back by the inference job.
CREATE TABLE IF NOT EXISTS flight_delay.gold.ml_predictions (
    flight_date              DATE,
    reporting_airline        STRING,
    flight_number            STRING,
    origin_airport           STRING,
    dest_airport             STRING,
    route_code               STRING,
    delay_probability        DOUBLE,    -- model output probability [0, 1]
    predicted_delayed        BOOLEAN,   -- probability >= threshold
    model_version            STRING,    -- MLflow registered model version
    prediction_timestamp     TIMESTAMP,
    batch_id                 STRING,
    year_month               STRING
)
USING DELTA
PARTITIONED BY (year_month);
