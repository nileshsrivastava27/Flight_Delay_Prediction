-- Gold layer — analytics aggregates for reporting and dashboards.
-- Run after 020_silver_tables.sql.

CREATE TABLE IF NOT EXISTS flight_delay.gold.on_time_performance_daily (
    flight_date                     DATE,
    reporting_airline               STRING,
    total_flights                   BIGINT,
    completed_flights               BIGINT,
    cancelled_flights               BIGINT,
    diverted_flights                BIGINT,
    delayed_departure_flights       BIGINT,
    delayed_arrival_flights         BIGINT,
    on_time_departure_flights       BIGINT,
    on_time_arrival_flights         BIGINT,
    on_time_arrival_percentage      DECIMAL(9,2),
    avg_departure_delay_minutes     DECIMAL(9,2),
    avg_arrival_delay_minutes       DECIMAL(9,2),
    total_weather_delay_minutes     BIGINT,
    total_carrier_delay_minutes     BIGINT,
    total_nas_delay_minutes         BIGINT,
    total_security_delay_minutes    BIGINT,
    total_late_aircraft_delay_minutes BIGINT,
    load_timestamp                  TIMESTAMP
)
USING DELTA
PARTITIONED BY (flight_date);


CREATE TABLE IF NOT EXISTS flight_delay.gold.route_delay_summary (
    flight_date                     DATE,
    route_code                      STRING,
    origin_airport                  STRING,
    dest_airport                    STRING,
    reporting_airline               STRING,
    total_flights                   BIGINT,
    completed_flights               BIGINT,
    cancelled_flights               BIGINT,
    delayed_arrival_flights         BIGINT,
    avg_departure_delay_minutes     DECIMAL(9,2),
    avg_arrival_delay_minutes       DECIMAL(9,2),
    max_arrival_delay_minutes       INT,
    total_weather_delay_minutes     BIGINT,
    total_carrier_delay_minutes     BIGINT,
    total_late_aircraft_delay_minutes BIGINT,
    load_timestamp                  TIMESTAMP
)
USING DELTA
PARTITIONED BY (flight_date);


CREATE TABLE IF NOT EXISTS flight_delay.gold.airport_delay_summary (
    flight_date                     DATE,
    airport_code                    STRING,
    airport_role                    STRING,
    reporting_airline               STRING,
    total_flights                   BIGINT,
    completed_flights               BIGINT,
    cancelled_flights               BIGINT,
    delayed_flights                 BIGINT,
    on_time_flights                 BIGINT,
    avg_delay_minutes               DECIMAL(9,2),
    max_delay_minutes               INT,
    total_weather_delay_minutes     BIGINT,
    total_carrier_delay_minutes     BIGINT,
    total_late_aircraft_delay_minutes BIGINT,
    load_timestamp                  TIMESTAMP
)
USING DELTA
PARTITIONED BY (flight_date);


CREATE TABLE IF NOT EXISTS flight_delay.gold.cancellation_summary (
    flight_date          DATE,
    reporting_airline    STRING,
    origin_airport       STRING,
    dest_airport         STRING,
    cancellation_code    STRING,
    cancellation_reason  STRING,
    cancellation_count   BIGINT,
    load_timestamp       TIMESTAMP
)
USING DELTA
PARTITIONED BY (flight_date);
