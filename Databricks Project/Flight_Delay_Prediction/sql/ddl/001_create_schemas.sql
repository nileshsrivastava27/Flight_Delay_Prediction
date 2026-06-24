-- Flight Delay Prediction — catalog and schema setup.
-- Run once before any other DDL or ingestion.

CREATE CATALOG IF NOT EXISTS flight_delay;

CREATE SCHEMA IF NOT EXISTS flight_delay.bronze;
CREATE SCHEMA IF NOT EXISTS flight_delay.silver;
CREATE SCHEMA IF NOT EXISTS flight_delay.gold;
CREATE SCHEMA IF NOT EXISTS flight_delay.audit;
