-- Audit log for every pipeline stage event (STARTED / SUCCESS / FAILED).
-- Run after 001_create_schemas.sql.

CREATE TABLE IF NOT EXISTS flight_delay.audit.pipeline_run_log (
    event_timestamp  TIMESTAMP,
    pipeline_name    STRING,
    stage_name       STRING,
    target_table     STRING,
    run_id           STRING,
    batch_id         STRING,
    status           STRING,
    rows_written     BIGINT,
    rows_removed     BIGINT,
    rows_rescued     BIGINT,
    restored_version BIGINT,
    error_message    STRING,
    details          STRING
)
USING DELTA;
