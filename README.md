# SNCF End-to-End Data Platform

An end-to-end data engineering pipeline that extracts operational SNCF (French railway) data from an OLTP source database, transforms it, and loads it into a dimensional data warehouse — orchestrated with Apache Airflow and fully containerized with Docker.

## Overview

The platform simulates a realistic railway operations dataset (trains, stations, trips, reservations) and builds a star-schema data warehouse from it, following a classic ETL pattern:

```
raw (OLTP source, sncf_oltp)  --->  ETL (Airflow + Python)  --->  dw (star schema, sncf_dw)
```

- **Source**: `sncf_source` — a normalized OLTP-style database (`raw` schema) with ~6M trips and ~10M reservations.
- **Warehouse**: `sncf_dw` — a dimensional model (`dw` schema) with 3 dimensions and 2 fact tables, ready for analytics.
- **Orchestration**: Apache Airflow 3.x, running fully in Docker, on a daily schedule.

## Architecture

### Data model

**Dimensions**
- `dw.dim_client` — passengers (SCD Type 1)
- `dw.dim_gare` — stations
- `dw.dim_train` — trains
- `dw.dim_date` — calendar dimension (pre-generated, 2019–2026)

**Facts**
- `dw.fact_trajet` — one row per train trip on a given day (grain: train × day)
- `dw.fact_reservation` — one row per ticket sold (grain: reservation)

### Services (Docker Compose)

| Service | Role |
|---|---|
| `source_db` | PostgreSQL — OLTP source (`raw` schema) |
| `dw_db` | PostgreSQL — data warehouse (`dw` schema) |
| `airflow-postgres` | PostgreSQL — Airflow metadata database |
| `airflow-init` | One-off container: DB migration + admin user creation |
| `airflow-api-server` | Airflow 3 API server / web UI (port `8080`) |
| `airflow-scheduler` | Schedules and executes DAG runs (LocalExecutor) |
| `airflow-dag-processor` | Parses DAG files (separate service since Airflow 3.0) |
| `airflow-triggerer` | Handles deferrable operators |

### ETL pipeline

Implemented in `src/` and orchestrated by the Airflow DAG `sncf_etl_pipeline` (`dags/sncf_etl_dag.py`):

```
purge_dw
   │
   ├──► load_dim_client ──┐
   ├──► load_dim_gare  ───┤
   └──► load_dim_train ───┤
                           ▼
                    load_fact_trajet
                           │
                           ▼
                  load_fact_reservation
                           │
                           ▼
                    quality_check
```

- **Extraction** (`src/extract/`): reads from `raw.*` via SQLAlchemy/pandas. Large fact tables (trips, reservations — millions of rows) are streamed in chunks using a server-side cursor (`stream_results=True`) rather than loaded into memory all at once.
- **Transformation** (`src/transform/`): data cleaning (city casing, sex code normalization, electrification flags) and surrogate-key resolution (natural keys from `raw` mapped to warehouse surrogate keys).
- **Loading** (`src/load/`): bulk loading via PostgreSQL `COPY` (not row-by-row `INSERT`) for performance at scale.
- **Quality check**: post-load validation — no negative amounts, no orphaned reservations, no unexpectedly empty tables.

## Getting started

### Prerequisites

- Docker and Docker Compose
- A `.env` file at the project root (see `.env.example` if present, or the variables referenced in `docker-compose.yml`: `SOURCE_DB_*`, `DW_DB_*`, `AIRFLOW_FERNET_KEY`, `AIRFLOW_JWT_SECRET`)

### 1. Start the stack

```bash
docker compose up -d
```

This starts all databases and Airflow services. Airflow's web UI becomes available at [http://localhost:8080](http://localhost:8080) (default login: `admin` / `admin`).

### 2. Initialize the database schemas

The schema creation scripts are **not** run automatically on first boot — they must be applied manually the first time (or wired into `docker-entrypoint-initdb.d/` for automatic init on a fresh volume):

```bash
# Source (raw) schema
docker cp sql/source/01_schema_raw.sql sncf_source:/schema_raw.sql
docker exec -it sncf_source psql -U <SOURCE_DB_USER> -d <SOURCE_DB_NAME> -f /schema_raw.sql

# Warehouse (dw) schema
docker cp sql/dw/01_schema_dw.sql sncf_dw:/schema.sql
docker exec -it sncf_dw psql -U <DW_DB_USER> -d <DW_DB_NAME> -f /schema.sql
```

> Replace `<SOURCE_DB_USER>`, `<SOURCE_DB_NAME>`, `<DW_DB_USER>`, `<DW_DB_NAME>` with the values from your `.env` file.

### 3. Load the source data

Populate `raw.*` with your dataset (CSV import, seed script, etc. — depends on how you're sourcing the operational data).

### 4. Run the pipeline

From the Airflow UI, trigger the `sncf_etl_pipeline` DAG manually, or wait for its daily schedule (`@daily`, at 00:00 UTC).

From the CLI:

```bash
docker exec -it airflow-scheduler airflow dags trigger sncf_etl_pipeline
```

## Scheduling

The DAG runs **once per day** (`schedule="@daily"`), with `catchup=False` (no backfill of missed runs) and `max_active_runs=1` (only one run at a time, to avoid concurrent writes to the warehouse).

## Testing

The test suite is split into two layers:

### Unit tests (`tests/unit/`)

Fast, fully mocked — no database or Docker required. Cover `extract.py`, `load.py`, `db.py`, and `transform.py`.

```bash
pytest tests/unit -v
```

### Integration tests (`tests/integration/`)

Validate real data quality against a live, already-loaded warehouse (row counts, uniqueness, referential integrity, business rules). Automatically skipped if the databases aren't reachable.

```bash
pytest tests/integration -v
```

### Full suite with coverage

```bash
pytest --cov=src tests/ -v
```

## CI/CD

A GitHub Actions workflow (`.github/workflows/ci.yml`) runs on every push and pull request to `master`:

- **Lint**: `ruff check`
- **Formatting**: `ruff format --check`
- **Unit tests**: `pytest tests/unit` with coverage report uploaded as a build artifact

Integration tests are **not** run in CI (they require live multi-million-row databases) — they're intended for local/manual verification after a pipeline run.

To run the same checks locally before pushing:

```bash
pip install -r requirements.txt -r requirements-dev.txt
ruff check .
ruff format --check .
pytest tests/unit -v --cov=src
```

## Project structure

```
.
├── dags/
│   └── sncf_etl_dag.py          # Airflow DAG definition
├── src/
│   ├── config.py                # Environment-based configuration
│   ├── db.py                    # SQLAlchemy engines / psycopg2 connections
│   ├── main.py                  # Standalone (non-Airflow) pipeline entrypoint
│   ├── extract/
│   │   └── extract.py
│   ├── transform/
│   │   └── transform.py
│   └── load/
│       └── load.py
├── sql/
│   ├── source/
│   │   └── 01_schema_raw.sql    # OLTP source schema
│   └── dw/
│       └── 01_schema_dw.sql     # Star schema (dimensions + facts)
├── tests/
│   ├── conftest.py              # Shared fixtures (DB engines, auto-skip if unreachable)
│   ├── unit/                    # Mocked, no external dependencies
│   └── integration/             # Data quality checks against a live warehouse
├── .github/workflows/ci.yml     # Lint + unit tests on every push/PR
├── docker-compose.yml
├── Dockerfile.airflow
├── requirements.txt
└── requirements-dev.txt
```

## Operational notes

A few non-obvious things worth knowing if you're debugging this pipeline:

- **Streaming large extracts**: `extract_chunks()` requires a SQLAlchemy engine created with `execution_options(stream_results=True)` (see `get_source_engine_stream()` in `src/db.py`). Without it, psycopg2 buffers the entire result set client-side before pandas chunks it — which defeats the purpose of chunking and can stall a task for tens of minutes on multi-million-row tables.
- **Logging, not `print()`**: task callables use the standard `logging` module rather than `print()`. On long-running tasks, Airflow 3's stdout capture pipe can close prematurely (`BrokenPipeError`), which would otherwise mask a successful load as a failed task.
- **`max_active_runs=1`**: prevents concurrent DAG runs from writing to `fact_trajet`/`fact_reservation` at the same time, which previously caused Postgres deadlocks on the unique index.
- **Connection timeouts**: `get_dw_connection()` sets `statement_timeout` and `idle_in_transaction_session_timeout` to avoid orphaned connections lingering after a killed task (which can also lead to deadlocks on the next run).
