# Dataflow Sentinel — Architecture

## 1. Overview

**Dataflow Sentinel** is a lightweight, production-oriented **data pipeline and monitoring system** designed to ingest raw data, validate it, transform it through layered storage (Bronze → Silver → Gold), and continuously track **data freshness and data quality**.

The project mirrors a real-world **DataOps / Analytics Engineering** system and emphasizes:

* Deterministic execution
* Idempotent re-runs
* Clear separation of concerns
* Validation-first data promotion
* Containerized reproducibility
* CI-driven automation

The system is intentionally compact, but architected using production principles.

---

## 2. High-Level Architecture

```
External Data Source
        │
        ▼
 ┌────────────┐
 │ Ingestion  │  (src/ingestion.py)
 └────────────┘
        │
        ▼
 ┌────────────┐
 │ Validation │  (src/validation.py)
 └────────────┘
        │
        ▼
 ┌────────────┐
 │  Storage   │  (src/storage.py)
 │ Bronze     │
 │ Silver     │
 │ Gold       │
 └────────────┘
        │
        ▼
 ┌────────────┐
 │  Metrics   │  (src/gold_metrics.py)
 └────────────┘
        │
        ▼
 ┌────────────┐
 │ Monitoring │  (freshness.json)
 └────────────┘
```

The pipeline is orchestrated through **`src/pipeline.py`**, which:

* Enforces deterministic execution order
* Coordinates logging
* Propagates errors explicitly
* Ensures safe re-runs (idempotency)

The same orchestration logic is used across:

* Local execution
* Docker containers
* GitHub Actions CI

This guarantees **behavioral parity across environments**.

---

## 3. Data Layers (Medallion Architecture)

The pipeline follows the **Bronze → Silver → Gold** medallion pattern to ensure reliability and traceability.

### Bronze Layer (`data/bronze/`)

* Raw, minimally processed data
* Closest representation of the source
* Immutable audit and recovery layer
* No transformation logic applied

### Silver Layer (`data/silver/`)

* Cleaned and validated datasets
* Schema enforcement and quality checks applied
* Safe for analytical consumption
* No business aggregation logic

### Gold Layer (`data/gold/`)

* Aggregated, analytics-ready outputs
* Derived from validated Silver datasets
* Example artifacts:

  * `aggregates.csv`
  * `freshness.json`

This layered model:

* Prevents corrupted data from reaching analytics
* Improves debuggability
* Enables safe reprocessing from any layer
* Decouples ingestion from business logic

---

## 4. Core Modules

### 4.1 Pipeline Orchestrator (`src/pipeline.py`)

* Single entry point of the system

* Enforces strict execution order:

  1. Ingestion
  2. Validation
  3. Storage (Bronze → Silver → Gold)
  4. Metrics generation

* Central coordination layer

* Guarantees idempotent re-execution

---

### 4.2 Ingestion (`src/ingestion.py`)

* Fetches or simulates external data sources
* Writes exclusively to the Bronze layer
* Fully isolated from validation and transformation logic
* Designed for easy replacement with real APIs or streaming systems

---

### 4.3 Validation (`src/validation.py`)

* Gatekeeper between Bronze and Silver
* Enforces data quality before promotion
* Includes:

  * Required field validation
  * Data type enforcement
  * Schema validation via **Pydantic models**
  * Non-empty dataset checks

Invalid data is rejected early to prevent downstream corruption.

---

### 4.4 Storage (`src/storage.py`)

* Centralized abstraction for all filesystem operations
* Encapsulates read/write logic
* Prevents business logic from coupling to storage mechanics
* Improves testability and future extensibility

Future targets could include:

* Object storage (S3)
* Data warehouses

---

### 4.5 Gold Metrics (`src/gold_metrics.py`)

* Computes analytics-ready aggregates
* Generates data freshness metrics
* Produces structured Gold outputs
* Responsible for monitoring signals such as:

  * Dataset staleness
  * Last available data timestamps

---

### 4.6 Logging (`src/logger.py`)

* Centralized logging configuration
* Structured, consistent logs across all modules
* Logs persisted under `logs/`
* Designed for observability and post-failure analysis

---

### 4.7 Error Monitoring (`src/monitoring.py`)

* Integrates **Sentry** for real-time runtime error tracking
* Initialized at application startup inside `pipeline.py`
* Isolated from business logic to preserve clean architecture
* Automatically captures:

  * Unhandled exceptions
  * Stack traces
  * Execution context
  * Environment (local / Docker)

Monitoring activation is controlled via environment variables:

* `SENTRY_DSN`
* `ENVIRONMENT`
* `SENTRY_RELEASE`

If `SENTRY_DSN` is not provided, monitoring remains disabled — ensuring safe local development.

This layer enhances operational visibility beyond CI logs by providing persistent external error tracking.

---

## 5. Configuration Management

### Assets Configuration (`config/assets.yaml`)

* Defines symbols, assets, and runtime parameters
* Decouples configuration from business logic
* Enables environment-agnostic execution

### Environment Variables

Managed via:

* `.env`
* `.env.local`
* Template examples for onboarding

Secrets and environment-specific values are never hard-coded.

---

## 6. Idempotency & Re-Run Safety

The pipeline is designed to be safely re-executed without corrupting state.

* Layered overwrite strategy prevents duplication
* Deterministic transformations ensure consistent outputs
* Re-running the pipeline produces stable results
* Gold metrics always reflect the latest validated Silver state

This makes the system suitable for scheduled CI runs and production-like environments.

---

## 7. Testing Strategy

Tests reside under `tests/` and mirror the source structure.

| Component    | Test File            |
| ------------ | -------------------- |
| Ingestion    | `test_ingestion.py`  |
| Validation   | `test_validation.py` |
| Storage      | `test_storage.py`    |
| Gold Metrics | `test_gold.py`       |

* Built with **pytest**
* Covers happy paths and failure cases
* Includes validation edge cases
* Protects against regressions during refactors

Testing ensures architectural guarantees remain intact as the project evolves.

---

## 8. Automation & CI/CD

### Docker

* `Dockerfile` defines a reproducible runtime
* `docker-compose.yml` runs the pipeline as an isolated service
* Ensures consistency across machines and environments

### Makefile

Standardized developer interface for:

* Running pipeline
* Running tests
* Managing containers
* Managing cleanups

Removes manual command repetition.

### GitHub Actions (`.github/workflows/`)

#### `daily_run.yml`

* Scheduled pipeline execution
* Simulates production recurring workloads
* Verifies freshness and integrity

#### `email-notify.yml`

* Sends notifications on success or failure
* Surfaces operational health signals in CI

---

## 9. Observability & Monitoring

* Data freshness tracked via `freshness.json`
* Structured logs for traceability
* CI notifications for operational awareness
* Sentry-based runtime error monitoring
* Failure propagation prevents silent corruption

The system reflects core **data reliability engineering** principles by combining logging, alerting, and external error tracking.

---

## 10. Design Principles

* Deterministic execution
* Idempotent re-runs
* Validation-first data promotion
* Clear separation of concerns
* Configuration over hard-coding
* Reproducible environments
* Production-inspired structure

---

## 11. Future Extensions

* Replace simulated ingestion with real external APIs
* Persist Gold outputs to PostgreSQL
* Introduce anomaly detection
* Add observability dashboards (Grafana)
* Implement alert thresholds for freshness violations

---

## 12. Summary

Dataflow Sentinel demonstrates how a compact yet production-inspired data pipeline can be engineered using Python, Docker, and CI automation.

The system prioritizes:

* Reliability
* Observability
* Testability
* Reproducibility
* Operational realism

It is intentionally simple in scope but structured to reflect real-world data engineering practices.