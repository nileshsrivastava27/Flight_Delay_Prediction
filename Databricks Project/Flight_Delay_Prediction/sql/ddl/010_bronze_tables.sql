-- Bronze layer — raw ingest tables.
-- Full source fidelity: no type coercion, no nullability constraints.
-- Run after 001_create_schemas.sql.

CREATE TABLE IF NOT EXISTS flight_delay.bronze.flight_operations_raw (
    flight_date              DATE,
    reporting_airline        STRING,
    reporting_airline_name   STRING,
    tail_number              STRING,
    flight_number            STRING,
    origin_airport           STRING,
    origin_airport_seq_id    STRING,
    origin_city_name         STRING,
    origin_state_abbr        STRING,
    dest_airport             STRING,
    dest_airport_seq_id      STRING,
    dest_city_name           STRING,
    dest_state_abbr          STRING,
    crs_dep_time             STRING,
    dep_time                 STRING,
    dep_delay_minutes        DOUBLE,
    taxi_out                 DOUBLE,
    wheels_off               STRING,
    wheels_on                STRING,
    taxi_in                  DOUBLE,
    crs_arr_time             STRING,
    arr_time                 STRING,
    arr_delay_minutes        DOUBLE,
    cancelled                INT,
    cancellation_code        STRING,
    diverted                 INT,
    distance_miles           DOUBLE,
    carrier_delay            DOUBLE,
    weather_delay            DOUBLE,
    nas_delay                DOUBLE,
    security_delay           DOUBLE,
    late_aircraft_delay      DOUBLE,
    raw_record               STRING,
    source_file_name         STRING,
    source_system            STRING,
    ingestion_timestamp      TIMESTAMP,
    batch_id                 STRING,
    record_hash              STRING,
    year_month               STRING
)
USING DELTA
PARTITIONED BY (year_month);


CREATE TABLE IF NOT EXISTS flight_delay.bronze.airports_raw (
    airport_id        STRING,
    ident             STRING,
    type              STRING,
    name              STRING,
    latitude_deg      DOUBLE,
    longitude_deg     DOUBLE,
    elevation_ft      DOUBLE,
    continent         STRING,
    iso_country       STRING,
    iso_region        STRING,
    municipality      STRING,
    scheduled_service STRING,
    gps_code          STRING,
    icao_code         STRING,
    iata_code         STRING,
    local_code        STRING,
    home_link         STRING,
    wikipedia_link    STRING,
    keywords          STRING,
    raw_record        STRING,
    source_file_name  STRING,
    source_system     STRING,
    ingestion_timestamp TIMESTAMP,
    batch_id          STRING,
    record_hash       STRING
)
USING DELTA;


CREATE TABLE IF NOT EXISTS flight_delay.bronze.aircraft_reference_raw (
    aircraft_code       STRING,
    aircraft_name       STRING,
    iata_code           STRING,
    icao_code           STRING,
    raw_record          STRING,
    source_file_name    STRING,
    source_system       STRING,
    ingestion_timestamp TIMESTAMP,
    batch_id            STRING,
    record_hash         STRING
)
USING DELTA;


CREATE TABLE IF NOT EXISTS flight_delay.bronze.weather_metar_raw (
    station_id              STRING,
    observation_time        TIMESTAMP,
    raw_text                STRING,
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
    raw_record              STRING,
    source_file_name        STRING,
    source_system           STRING,
    ingestion_timestamp     TIMESTAMP,
    batch_id                STRING,
    record_hash             STRING,
    observation_date        DATE
)
USING DELTA
PARTITIONED BY (observation_date);
