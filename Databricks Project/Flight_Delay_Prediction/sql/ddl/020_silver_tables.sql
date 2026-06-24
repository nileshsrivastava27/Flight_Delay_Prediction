-- Silver layer — cleaned, validated, and joined tables.
-- Run after 010_bronze_tables.sql.

CREATE TABLE IF NOT EXISTS flight_delay.silver.airports_clean (
    airport_key           BIGINT GENERATED ALWAYS AS IDENTITY,
    airport_iata_code     STRING,
    airport_icao_code     STRING,
    airport_ident         STRING,
    airport_name          STRING,
    airport_type          STRING,
    municipality          STRING,
    iso_country           STRING,
    iso_region            STRING,
    continent             STRING,
    latitude_deg          DOUBLE,
    longitude_deg         DOUBLE,
    elevation_ft          DOUBLE,
    scheduled_service_flag BOOLEAN,
    ingestion_timestamp   TIMESTAMP,
    batch_id              STRING
)
USING DELTA;


CREATE TABLE IF NOT EXISTS flight_delay.silver.aircraft_reference_clean (
    aircraft_key        BIGINT GENERATED ALWAYS AS IDENTITY,
    aircraft_code       STRING,
    aircraft_name       STRING,
    iata_code           STRING,
    icao_code           STRING,
    ingestion_timestamp TIMESTAMP,
    batch_id            STRING
)
USING DELTA;


CREATE TABLE IF NOT EXISTS flight_delay.silver.route_reference (
    route_key              BIGINT GENERATED ALWAYS AS IDENTITY,
    origin_airport         STRING,
    dest_airport           STRING,
    route_code             STRING,
    route_group            STRING,
    first_seen_flight_date DATE,
    last_seen_flight_date  DATE,
    total_record_count     BIGINT,
    ingestion_timestamp    TIMESTAMP,
    batch_id               STRING
)
USING DELTA;


CREATE TABLE IF NOT EXISTS flight_delay.silver.weather_observations_clean (
    weather_key             BIGINT GENERATED ALWAYS AS IDENTITY,
    station_id              STRING,
    observation_time        TIMESTAMP,
    observation_date        DATE,
    temp_c                  DOUBLE,
    dewpoint_c              DOUBLE,
    wind_dir_degrees        INT,
    wind_speed_kt           DOUBLE,
    wind_gust_kt            DOUBLE,
    visibility_statute_mi   DOUBLE,
    altim_in_hg             DOUBLE,
    flight_category         STRING,
    precip_in               DOUBLE,
    sky_cover               STRING,
    ingestion_timestamp     TIMESTAMP,
    batch_id                STRING
)
USING DELTA
PARTITIONED BY (observation_date);


CREATE TABLE IF NOT EXISTS flight_delay.silver.flight_operations_clean (
    flight_operation_key        BIGINT GENERATED ALWAYS AS IDENTITY,
    flight_date                 DATE,
    reporting_airline           STRING,
    reporting_airline_name      STRING,
    tail_number                 STRING,
    flight_number               STRING,
    origin_airport              STRING,
    dest_airport                STRING,
    route_code                  STRING,
    origin_city_name            STRING,
    origin_state_abbr           STRING,
    dest_city_name              STRING,
    dest_state_abbr             STRING,
    scheduled_departure_ts      TIMESTAMP,
    actual_departure_ts         TIMESTAMP,
    scheduled_arrival_ts        TIMESTAMP,
    actual_arrival_ts           TIMESTAMP,
    departure_delay_minutes     INT,
    arrival_delay_minutes       INT,
    taxi_out_minutes            INT,
    taxi_in_minutes             INT,
    cancelled_flag              BOOLEAN,
    cancellation_code           STRING,
    diverted_flag               BOOLEAN,
    distance_miles              DOUBLE,
    carrier_delay_minutes       INT,
    weather_delay_minutes       INT,
    nas_delay_minutes           INT,
    security_delay_minutes      INT,
    late_aircraft_delay_minutes INT,
    is_delayed_flag             BOOLEAN,
    is_on_time_arrival_flag     BOOLEAN,
    is_on_time_departure_flag   BOOLEAN,
    processing_status           STRING,
    quality_issue_reason        STRING,
    ingestion_timestamp         TIMESTAMP,
    batch_id                    STRING,
    source_file_name            STRING,
    record_hash                 STRING,
    year_month                  STRING
)
USING DELTA
PARTITIONED BY (year_month);


CREATE TABLE IF NOT EXISTS flight_delay.silver.flight_operations_quarantine (
    quarantine_key       BIGINT GENERATED ALWAYS AS IDENTITY,
    raw_flight_date      STRING,
    reporting_airline    STRING,
    flight_number        STRING,
    origin_airport       STRING,
    dest_airport         STRING,
    quality_issue_reason STRING,
    raw_record           STRING,
    source_file_name     STRING,
    ingestion_timestamp  TIMESTAMP,
    batch_id             STRING,
    record_hash          STRING
)
USING DELTA;
