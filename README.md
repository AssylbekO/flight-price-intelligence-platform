# Flight Price Intelligence Platform

An end-to-end data engineering platform that ingests US domestic flight data from the BTS DB1B dataset, transforms it into analytics-ready models, and exposes pricing signals for machine learning — built on AWS S3, PostgreSQL, dbt, and Apache Airflow.

---

## Architecture

```
BTS DB1B (US DOT)
       ↓
AWS S3 (raw ZIP files, Hive-partitioned)
       ↓
bts_s3_loader.py (Python — in-memory ZIP extraction, COPY bulk load)
       ↓
raw.bts_db1b (PostgreSQL — 4.1M rows per quarter, all TEXT)
       ↓
stg_bts_db1b (dbt view — type casting, outlier filtering, dedup)
       ↓
mart_routes (dbt table — route-level aggregations + airport reference)
       ↓
mart_hhi (dbt table — Herfindahl-Hirschman Index per route)
       ↓
mart_seasonal (dbt table — rolling baselines, window functions)
       ↓
XGBoost + SHAP (price driver analysis, fare anomaly detection)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Storage | AWS S3 (Hive-style partitioning) |
| Infrastructure | Docker, Docker Compose |
| Database | PostgreSQL 15 |
| Orchestration | Apache Airflow 2.8 |
| Transformation | dbt 1.8 (dbt-postgres) |
| Language | Python 3.13 |
| ML | XGBoost, SHAP |
| Data Source | BTS DB1B via US DOT |

---

## Project Structure

```
flight-price-intelligence-platform/
├── ingestion/
│   ├── bts_s3_loader.py        # S3 → PostgreSQL bulk loader
│   └── utils.py                # S3 client factory
├── airflow/
│   └── dags/                   # Airflow DAG definitions
├── dbt_flight/                 # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_bts_db1b.sql
│   │   │   ├── sources.yml
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── mart_routes.sql
│   │       ├── mart_hhi.sql
│   │       ├── mart_seasonal.sql
│   │       └── schema.yml
│   └── dbt_project.yml
├── init-scripts/               # PostgreSQL init (creates airflow_metadata DB)
├── docker-compose.yml
└── .env
```

---

## Data Pipeline

### Ingestion (`bts_s3_loader.py`)

- Fetches quarterly ZIP files from S3 using `boto3`
- Extracts CSV entirely in memory using `io.BytesIO` — no disk writes
- Dynamically reads CSV headers and creates a `TEXT`-typed raw table
- Bulk loads via PostgreSQL `COPY FROM STDIN` — 4.1M rows in ~20 seconds
- Idempotent: drops and recreates the table on each run
- Full transaction safety: rolls back on any failure

### Transformation (dbt)

**Staging layer** — `stg_bts_db1b`
- Casts all 25 TEXT columns to correct types (BIGINT, INT, NUMERIC)
- Filters invalid records: fares ≤ $0, fares > $25,000, zero passengers, zero distance
- Derives `quarter_start_date` from year + quarter using `MAKE_DATE()`
- Renames all columns to snake_case

**Marts layer**
- `mart_routes` — route-level aggregations joined with airport reference data
- `mart_hhi` — Market Concentration Index per route using CTEs and window functions
- `mart_seasonal` — rolling 4-quarter fare baselines using `AVG() OVER (PARTITION BY ...)`

---

## Data Quality

dbt tests across all models:
- `not_null` on key identifiers and fare columns
- `positive_values` on `itin_fare`, `passengers`, `distance`
- `accepted_values` on `quarter` (1–4)
- `unique` on route + quarter combinations in mart layer

---

## Setup

### Prerequisites
- Docker & Docker Compose
- Python 3.13+
- AWS credentials with S3 read access
- dbt-postgres

### Environment variables

Create a `.env` file in the project root:

```
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=flight_intelligence
POSTGRES_HOST=postgres
AIRFLOW_IMAGE_NAME=apache/airflow:2.8.0
FERNET_KEY=your_fernet_key
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=your_password
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=eu-north-1
S3_BUCKET_NAME=your_bucket
```

### Start containers

```bash
docker-compose up -d
```

### Run ingestion manually

```bash
python ingestion/bts_s3_loader.py
```

### Run dbt manually

```bash
cd dbt_flight
dbt run
dbt test
dbt docs serve --port 8081
```

---

## Dataset

BTS DB1B (Origin and Destination Survey) is a 10% sample of all US domestic airline tickets published quarterly by the Bureau of Transportation Statistics.

- ~4.1M rows per quarter
- 25 columns including origin, destination, fare, passengers, distance, carrier
- Coverage: 2019–2023 (20 quarters = ~80M rows)
