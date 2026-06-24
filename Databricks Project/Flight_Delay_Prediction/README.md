# Flight Delay Prediction — End-to-End Databricks Pipeline

A production-style data engineering and machine learning pipeline built entirely on **Databricks** and **Delta Lake**, using a file-based Medallion architecture. Designed to be runnable on a free Databricks Community Edition or trial workspace — no Kafka, no ADF, no Event Hubs.

---

## Why This Project Exists

Most flight-delay tutorials stop at a cleaned CSV and a trained model. This project treats the dataset as a real operational pipeline: raw files land in a landing zone, flow through Bronze → Silver → Gold Delta tables, feed a feature store, and produce a registered MLflow model — with every layer built for reliability, schema safety, and free reruns.

The three patterns that make this stand out from a generic tutorial:

| Pattern | Where It Appears | Why It Matters |
|---|---|---|
| `RESTORE TABLE` | Bronze rollback | Point-in-time recovery without backups |
| `rescuedDataColumn` | Bronze ingest | Schema drift captured, never lost |
| Idempotent reruns | Every layer | Safe to re-run any notebook; no duplicates |

---

## Architecture

```
Landing Zone (DBFS / cloud storage)
  ├── flights/          ← BTS On-Time CSV drops
  └── weather/          ← NOAA/synthetic weather CSV drops
          │
          ▼
┌─────────────────────────────────────────────────────┐
│  BRONZE LAYER  (raw ingest, Delta Lake)              │
│  • Auto Loader with rescuedDataColumn                │
│  • Schema enforcement + evolve mode                  │
│  • Full history preserved → RESTORE TABLE on demand  │
└─────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────┐
│  SILVER LAYER  (cleaned, validated, joined)          │
│  • Deduplication via MERGE INTO (idempotent)         │
│  • Null handling, type casting, outlier removal      │
│  • Flight + weather joined on (date, origin_airport) │
└─────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────┐
│  GOLD LAYER  (features + aggregates)                 │
│  • Route-level historical delay averages             │
│  • Carrier on-time rate (trailing 30-day window)     │
│  • Departure hour, day-of-week, season encoding      │
│  • Weather severity score                            │
└─────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────┐
│  ML LAYER  (MLflow + Databricks ML)                  │
│  • LightGBM binary classifier (delayed ≥ 15 min)    │
│  • Experiment tracking, parameter logging            │
│  • Model registered in MLflow Model Registry         │
│  • Batch inference → predictions Delta table         │
└─────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────┐
│  MONITORING (optional)                               │
│  • Feature drift detection (Evidently AI)            │
│  • Databricks SQL dashboard for prediction trends    │
└─────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Tool |
|---|---|
| Compute | Databricks (PySpark) |
| Storage format | Delta Lake |
| Ingestion | Auto Loader (`cloudFiles`) |
| Orchestration | Databricks Workflows (Job clusters) |
| ML tracking | MLflow (built-in to Databricks) |
| Model | LightGBM via `lightgbm` / `sklearn` pipeline |
| Monitoring | Evidently AI (optional) |
| Visualization | Databricks SQL |
| Cost | $0 — runs on Community Edition / free trial |

---

## Project Structure

```
Flight_Delay_Prediction/
│
├── data/
│   ├── flights_sample.csv          # Synthetic but realistic flight data
│   └── weather_sample.csv          # Synthetic NOAA-style weather data
│
├── notebooks/
│   ├── 00_setup.py                 # Mount storage, create databases
│   ├── 01_bronze_ingest.py         # Auto Loader → bronze_flights, bronze_weather
│   ├── 02_silver_clean.py          # Validate, deduplicate, join → silver_flights
│   ├── 03_gold_features.py         # Feature engineering → gold_flight_features
│   ├── 04_ml_train.py              # Train LightGBM, log to MLflow
│   ├── 05_ml_inference.py          # Batch scoring → predictions table
│   └── 06_monitoring.py            # Drift detection (optional)
│
├── src/
│   └── utils/
│       ├── schema.py               # Shared schema definitions
│       └── transforms.py           # Reusable PySpark transform functions
│
└── README.md
```

---

## Key Implementation Details

### Bronze — Schema Safety with `rescuedDataColumn`

```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("rescuedDataColumn", "_rescued_data")  # never drop unknown columns
    .load(landing_path))
```

Any column in the CSV that doesn't match the enforced schema is captured in `_rescued_data` as JSON instead of silently dropped or erroring. This is the pattern that handles upstream schema drift without pipeline failure.

### Bronze — Rollback with `RESTORE TABLE`

```sql
-- If a bad file lands and poisons the bronze table:
DESCRIBE HISTORY bronze_flights;
RESTORE TABLE bronze_flights TO VERSION AS OF 12;
```

Delta Lake's transaction log makes point-in-time recovery a one-liner. No backup infrastructure needed.

### Silver — Idempotent Merge

```python
(silver_table.alias("target")
    .merge(new_data.alias("source"),
           "target.flight_id = source.flight_id AND target.flight_date = source.flight_date")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
```

Re-running the Silver notebook after a failure never produces duplicate rows.

### Gold — Feature Engineering Highlights

- **Route delay average**: trailing 90-day mean delay per `(origin, dest)` pair using a window function
- **Carrier on-time rate**: trailing 30-day on-time % per carrier
- **Weather severity score**: composite of wind speed, visibility, and precipitation bucketed into 0–3
- **Temporal features**: departure hour bucket (red-eye / morning / afternoon / evening), day of week, is_holiday flag

### ML — What Gets Logged to MLflow

```
Run: flight_delay_lgbm_v1
├── params/    num_leaves, max_depth, learning_rate, n_estimators
├── metrics/   accuracy, AUC-ROC, F1, precision, recall
├── artifacts/ model pickle, feature importance plot, confusion matrix
└── tags/      dataset_version, gold_table_version
```

---

## Data Sources

| Dataset | Source | Notes |
|---|---|---|
| Flight on-time performance | [BTS TranStats](https://www.transtats.bts.gov/) | Free, monthly CSV downloads |
| Weather data | [NOAA ISD](https://www.ncdc.noaa.gov/isd) | Hourly station data, free |
| Synthetic samples | `data/` folder in this repo | Ready to use immediately |

Synthetic sample CSVs in `data/` are included so you can run the full pipeline end-to-end without downloading anything.

---

## How to Run

### Prerequisites
- Databricks Community Edition (free) or any Databricks workspace
- Databricks Runtime 13.3 LTS or higher
- `lightgbm` and `evidently` installed on cluster (or via `%pip install`)

### Steps

1. **Clone this repo** into your Databricks workspace (Repos → Add Repo)
2. **Run `00_setup.py`** — creates databases and mounts the landing zone
3. **Drop the sample CSVs** from `data/` into your DBFS landing zone path
4. **Run notebooks 01–05 in order** — each is independently re-runnable
5. **View results** in the MLflow Experiments UI and the `predictions` Delta table

---

## Portfolio Talking Points

When walking an interviewer through this project, the story is:

> "The most important architectural decision was ditching streaming ingestion early — Kafka and ADF add cost and operational complexity that obscures the actual data engineering work. Going file-based let me focus on the Medallion patterns that matter: Delta Lake reliability, schema drift handling with `rescuedDataColumn`, point-in-time rollback with `RESTORE TABLE`, and idempotent merges that make every stage re-runnable safely. The ML layer on top uses MLflow for full experiment lineage — so I can tell you exactly which version of the Gold table produced any given model."

---

## What's Next (Stretch Goals)

- [ ] Real-time serving via MLflow Model Serving REST endpoint
- [ ] Databricks SQL dashboard for delay probability by route
- [ ] Retraining trigger: if model accuracy drops below threshold, auto-kick a training job
- [ ] Great Expectations integration for data quality assertions at Silver layer

---

## Author

**Nilesh Srivastava** — [GitHub](https://github.com/nileshsrivastava27)
