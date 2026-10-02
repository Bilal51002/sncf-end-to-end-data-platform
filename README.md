# SNCF End-to-End Data Platform

An end-to-end data engineering platform that extracts operational SNCF-style railway data from a PostgreSQL OLTP source, transforms it with Python, and loads it into a dimensional PostgreSQL data warehouse.

The pipeline is orchestrated with **Apache Airflow 3**, containerized with **Docker Compose**, validated with **pytest**, and continuously checked with **GitHub Actions**.

---

## Overview

The project simulates a realistic railway data platform containing trains, stations, trips, clients, and reservations.

The architecture follows a classic ETL flow:

```text
PostgreSQL OLTP (raw)
        │
        ▼
Python ETL + Apache Airflow
        │
        ▼
PostgreSQL Data Warehouse (dw)
```

Main components:

- **Source database**: PostgreSQL OLTP database using the `raw` schema.
- **ETL layer**: Python modules for extraction, transformation, and loading.
- **Orchestration**: Apache Airflow 3.
- **Warehouse**: PostgreSQL dimensional model using the `dw` schema.
- **Testing**: unit and integration tests with pytest.
- **Code quality**: Ruff.
- **CI**: GitHub Actions.
- **Analytics**: warehouse designed to be consumed by Power BI.

The source dataset contains millions of records, including approximately:

- ~6 million trips
- ~10 million reservations

---

## Architecture

### Data model

The warehouse follows a dimensional/star-schema approach.

### Dimensions

- `dw.dim_client` — passenger/customer dimension
- `dw.dim_gare` — railway station dimension
- `dw.dim_train` — train dimension
- `dw.dim_date` — calendar dimension

### Fact tables

- `dw.fact_trajet` — one row per train trip
- `dw.fact_reservation` — one row per reservation/ticket

---

## Docker services

The complete platform is orchestrated with Docker Compose.

| Service | Role |
|---|---|
| `source_db` | PostgreSQL OLTP source database |
| `dw_db` | PostgreSQL dimensional data warehouse |
| `airflow-postgres` | PostgreSQL metadata database used by Airflow |
| `airflow-init` | One-off Airflow initialization container |
| `airflow-api-server` | Airflow 3 API server and web UI |
| `airflow-scheduler` | Schedules and executes DAG tasks with LocalExecutor |
| `airflow-dag-processor` | Parses and processes DAG definitions |
| `airflow-triggerer` | Handles deferrable Airflow tasks |

The Airflow image is built from:

```text
Dockerfile.airflow
```

and currently uses:

```text
apache/airflow:3.0.6
```

---

## ETL pipeline

The ETL logic is implemented in `src/` and orchestrated through the Airflow DAG:

```text
sncf_etl_pipeline
```

Pipeline structure:

```text
purge_dw
   │
   ├──► load_dim_client ──┐
   ├──► load_dim_gare   ──┤
   └──► load_dim_train  ──┤
                          ▼
                    load_fact_trajet
                          │
                          ▼
                  load_fact_reservation
                          │
                          ▼
                     quality_check
```

### Extraction

Implemented in:

```text
src/extract/
```

The extraction layer reads from the PostgreSQL source using SQLAlchemy and pandas.

Large tables are streamed in chunks instead of being fully loaded into memory.

A server-side SQLAlchemy cursor is used with:

```python
stream_results=True
```

This is important for multi-million-row source tables.

### Transformation

Implemented in:

```text
src/transform/
```

Main transformations include:

- normalization of city names
- normalization of sex/gender values
- mapping of electrification flags
- date-key generation
- surrogate-key resolution
- rejection of fact rows whose required dimension keys cannot be resolved

### Loading

Implemented in:

```text
src/load/
```

Bulk loading is performed using PostgreSQL `COPY` rather than row-by-row `INSERT` operations.

This significantly improves performance for large fact tables.

### Quality check

The final Airflow task validates the loaded warehouse.

Examples of checks include:

- non-empty warehouse tables
- no negative monetary amounts
- strictly positive passenger counts
- non-negative distances
- valid reservations
- no orphaned foreign keys

---

## Airflow scheduling

The DAG runs once per day:

```text
@daily
```

It uses:

```python
catchup=False
max_active_runs=1
```

`max_active_runs=1` prevents concurrent runs from writing to the same warehouse tables.

---

## Getting started

### Prerequisites

You need:

- Docker
- Docker Compose
- Git
- Python 3.11+ for local development/testing

---

## Environment variables

Create a `.env` file at the project root.

Example:

```env
# Source database
SOURCE_DB_NAME=sncf_oltp
SOURCE_DB_USER=northwind_user
SOURCE_DB_PASSWORD=change-me
SOURCE_DB_PORT=5433

# Data warehouse
DW_DB_NAME=sncf_dw
DW_DB_USER=dw_user
DW_DB_PASSWORD=change-me
DW_DB_PORT=5434

# ETL
ETL_CHUNK_SIZE=100000

# Airflow
AIRFLOW_FERNET_KEY=replace-with-your-fernet-key
AIRFLOW_JWT_SECRET=replace-with-a-long-random-secret
```

> Do not commit your real `.env` file or production secrets to Git.

A public repository should contain a `.env.example` file with placeholders instead.

---

## Start the platform

Start the complete stack with:

```bash
docker compose up -d
```

Check the service status:

```bash
docker compose ps
```

The Airflow UI is available at:

```text
http://localhost:8080
```

---

## Database initialization

The PostgreSQL schema directories are mounted into:

```text
/docker-entrypoint-initdb.d
```

for both the source and warehouse databases.

Therefore, when PostgreSQL starts with a **fresh volume**, the SQL initialization scripts are executed automatically.

Source initialization scripts:

```text
sql/source/
```

Warehouse initialization scripts:

```text
sql/dw/
```

### Important

PostgreSQL only executes scripts from `/docker-entrypoint-initdb.d` when its data directory is empty.

If the Docker volumes already exist, the initialization scripts are **not executed again**.

For a normal restart:

```bash
docker compose down
docker compose up -d
```

For a complete reset of all PostgreSQL volumes:

```bash
docker compose down -v
docker compose up -d
```

> Warning: `docker compose down -v` permanently deletes the source, warehouse, and Airflow metadata volumes.

---

## Load source data

The source CSV/data files are exposed inside the source PostgreSQL container through:

```text
./data:/data:ro
```

Populate the `raw` schema using the project's source-loading process/scripts before running the ETL.

---

## Run the Airflow pipeline

You can trigger the DAG from the Airflow UI.

Alternatively:

```bash
docker compose exec airflow-scheduler airflow dags trigger sncf_etl_pipeline
```

Check active DAG runs:

```bash
docker compose exec airflow-scheduler airflow dags list-runs sncf_etl_pipeline --state running
```

Check all DAG runs:

```bash
docker compose exec airflow-scheduler airflow dags list-runs sncf_etl_pipeline
```

Check the state of every task in a specific run:

```bash
docker compose exec airflow-scheduler airflow tasks states-for-dag-run sncf_etl_pipeline "<RUN_ID>"
```

---

## Testing

The project contains **44 pytest tests** split between unit tests and integration/data-quality tests.

A complete local run against a successfully loaded warehouse produced:

```text
44 passed
```

---

### Unit tests

Located in:

```text
tests/unit/
```

They test the Python ETL components without requiring the live PostgreSQL warehouse.

Covered areas include:

- database configuration
- source extraction
- chunked extraction
- bulk loading
- key mapping
- normalization
- dimension transformations
- fact transformations
- surrogate-key resolution

Run them with:

```bash
pytest tests/unit -v
```

---

### Integration and data-quality tests

Located in:

```text
tests/integration/
```

They validate the real warehouse after a successful ETL run.

Current checks cover:

#### Volumetry

- `dim_client`
- `dim_gare`
- `dim_train`
- `fact_trajet`
- `fact_reservation`

#### Uniqueness

- client IDs
- trip IDs
- reservation IDs

#### Data profiling

- normalized sex values
- normalized city casing
- station service-year consistency

#### Business rules

- coherent reservation totals
- non-negative monetary values
- positive passenger counts
- non-negative trip distances
- reservation date not after trip date

#### Referential integrity

- no orphaned reservations
- no orphaned trips

Run them with:

```bash
pytest tests/integration -v
```

If the live databases cannot be reached, these tests are automatically skipped.

---

## Run the full test suite

```bash
pytest tests -v
```

Expected result with the databases available and the warehouse loaded:

```text
44 passed
```

---

## Test coverage

Run:

```bash
pytest tests --cov=src --cov-report=term-missing
```

The project currently reaches very high coverage across the ETL source code.

Example coverage obtained during development:

```text
src/config.py                  100%
src/db.py                      100%
src/extract/extract.py          92%
src/load/load.py                90%
src/transform/transform.py     100%
```

---

## Code quality

The project uses Ruff for linting and formatting.

Run:

```bash
ruff check .
```

Format the code:

```bash
ruff format .
```

Validate formatting without modifying files:

```bash
ruff format --check .
```

Before pushing:

```bash
ruff check .
ruff format --check .
pytest tests/unit -v --cov=src
```

---

## CI/CD

GitHub Actions is configured through:

```text
.github/workflows/ci.yml
```

The workflow runs automatically on pushes and pull requests to `master`.

The CI currently validates:

- Ruff linting
- Ruff formatting
- unit tests
- test coverage artifact/report

Integration tests are not executed in CI because they require live PostgreSQL databases populated with the large SNCF dataset.

They are intended to be executed locally after a successful ETL run.

---

## Project structure

```text
.
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── dags/
│   └── sncf_etl_dag.py
│
├── data/
│   └── ...
│
├── plugins/
│
├── sql/
│   ├── source/
│   │   └── 01_schema_raw.sql
│   └── dw/
│       └── 01_schema_dw.sql
│
├── src/
│   ├── config.py
│   ├── db.py
│   ├── main.py
│   │
│   ├── extract/
│   │   └── extract.py
│   │
│   ├── transform/
│   │   └── transform.py
│   │
│   └── load/
│       └── load.py
│
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── test_db.py
│   │   ├── test_extract.py
│   │   ├── test_load.py
│   │   └── test_transform.py
│   └── integration/
│       └── test_dw_quality.py
│
├── .env
├── docker-compose.yml
├── Dockerfile.airflow
├── pytest.ini
├── requirements.txt
├── requirements-airflow.txt
├── requirements-dev.txt
└── README.md
```

---

## Operational notes

### Large-data streaming

`extract_chunks()` requires a SQLAlchemy engine configured with:

```python
execution_options(stream_results=True)
```

Without server-side streaming, the PostgreSQL driver may buffer a very large result set before pandas processes the chunks.

---

### PostgreSQL bulk loading

The project uses PostgreSQL `COPY` for large loads rather than row-by-row inserts.

This is especially important for the multi-million-row fact tables.

---

### Airflow logging

Task callables use Python's standard logging system rather than relying on `print()`.

For long-running Airflow tasks, this avoids stdout-related issues that can hide an otherwise successful task execution.

---

### Airflow Execution API

Airflow 3 tasks communicate with the Execution API.

All Airflow services must therefore use the same configuration for:

```text
AIRFLOW__CORE__EXECUTION_API_SERVER_URL
AIRFLOW__API_AUTH__JWT_SECRET
AIRFLOW__EXECUTION_API__JWT_EXPIRATION_TIME
```

The Docker configuration uses the internal Docker service hostname:

```text
http://airflow-api-server:8080/execution/
```

instead of `localhost`.

---

### Windows / WSL / Docker clock synchronization

Airflow 3 uses JWT tokens internally.

If the Windows host, WSL environment, and Docker VM clocks become desynchronized, Airflow may reject valid task tokens with errors such as:

```text
ImmatureSignatureError: The token is not yet valid (iat)
```

If that happens, compare the clocks:

```powershell
(Get-Date).ToUniversalTime()
docker compose exec airflow-scheduler date -u
docker compose exec airflow-api-server date -u
docker compose exec airflow-dag-processor date -u
```

They should be synchronized within a few seconds.

A full WSL/Docker restart may be required if the Docker VM clock has drifted.

---

### Single active DAG run

The DAG uses:

```python
max_active_runs=1
```

This prevents concurrent DAG runs from writing simultaneously to `fact_trajet` and `fact_reservation`.

---

### Database connection timeouts

Warehouse connections use PostgreSQL timeout settings to reduce the risk of orphaned sessions after interrupted tasks.

This helps avoid locking and deadlock issues on later executions.

---

## Current project status

- [x] PostgreSQL source database
- [x] PostgreSQL dimensional warehouse
- [x] Docker Compose environment
- [x] Python ETL pipeline
- [x] Chunked extraction for large datasets
- [x] Bulk loading with PostgreSQL `COPY`
- [x] Apache Airflow 3 orchestration
- [x] Successful end-to-end Airflow execution
- [x] Post-load quality-check task
- [x] Unit tests
- [x] Integration/data-quality tests
- [x] 44/44 pytest tests passing locally on a loaded warehouse
- [x] Ruff linting and formatting
- [x] GitHub Actions CI
- [ ] Power BI dashboard
- [ ] SQL Server implementation/comparison

---

## Roadmap

### Completed

- Dockerized PostgreSQL source and warehouse
- OLTP and dimensional schemas
- Python extract/transform/load modules
- Airflow orchestration
- Large-table streaming
- Bulk warehouse loading
- Data-quality validation
- Unit and integration tests
- Code-quality checks
- Continuous integration

### Next steps

1. Build the Power BI dashboard.
2. Add business-oriented analytical views and KPIs.
3. Implement or reproduce the same warehouse workflow with SQL Server.
4. Compare PostgreSQL/Python/Airflow with the SQL Server implementation.
5. Document performance and architectural trade-offs.

---

## Tech stack

- Python 3.11
- PostgreSQL 16
- Apache Airflow 3.0.6
- Docker / Docker Compose
- pandas
- SQLAlchemy
- psycopg2
- pytest
- pytest-cov
- Ruff
- GitHub Actions
- Power BI

---

## Author

**Bilal Khallabi**

GitHub: `Bilal51002`

---

## License

This project is intended for educational and portfolio purposes.
