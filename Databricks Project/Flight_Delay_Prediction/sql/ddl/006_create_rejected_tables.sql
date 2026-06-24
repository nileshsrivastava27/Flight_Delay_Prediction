-- Bronze rejected-record (rescued) tables.
--
-- Rows whose values do not fit the expected schema are captured via
-- Databricks' rescuedDataColumn during ingestion and routed here instead of
-- being silently dropped or failing the load.
-- Run after 001_create_schemas.sql and before bronze ingestion.

-- Common shape for every rejected table:
--   source_table        : the Bronze target the row was meant for
--   source_system       : logical source system label
--   source_file_name    : originating file name
--   rescued_data        : JSON of values that did not match the schema
--   raw_payload         : JSON snapshot of the full incoming row
--   batch_id            : ingestion batch identifier
--   rejection_timestamp : when the row was rejected

CREATE TABLE IF NOT EXISTS flight_delay.bronze.flight_operations_rejected (
    source_table        STRING,
    source_system       STRING,
    source_file_name    STRING,
    rescued_data        STRING,
    raw_payload         STRING,
    batch_id            STRING,
    rejection_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS flight_delay.bronze.airports_rejected (
    source_table        STRING,
    source_system       STRING,
    source_file_name    STRING,
    rescued_data        STRING,
    raw_payload         STRING,
    batch_id            STRING,
    rejection_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS flight_delay.bronze.aircraft_reference_rejected (
    source_table        STRING,
    source_system       STRING,
    source_file_name    STRING,
    rescued_data        STRING,
    raw_payload         STRING,
    batch_id            STRING,
    rejection_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS flight_delay.bronze.weather_metar_rejected (
    source_table        STRING,
    source_system       STRING,
    source_file_name    STRING,
    rescued_data        STRING,
    raw_payload         STRING,
    batch_id            STRING,
    rejection_timestamp TIMESTAMP
) USING DELTA;
