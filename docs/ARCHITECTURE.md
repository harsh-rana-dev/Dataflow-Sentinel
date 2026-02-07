# Dataflow Sentinel — Architecture

## 1. Overview

Dataflow Sentinel is a lightweight, production‑oriented **data pipeline and monitoring system** designed to ingest raw data, validate it, transform it through layered storage (Bronze → Silver → Gold), and continuously track **data freshness and data quality**.

The project is intentionally structured to mirror a real‑world **DataOps / Analytics Engineering** system, emphasizing clear separation of concerns, automated testing, containerized execution, and CI‑driven orchestration.

---

## 2. High‑Level Architecture

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

The pipeline is orchestrated through **`pipeline.py`**, which coordinates execution order, logging, and error propagation. The same orchestration logic is used across **local runs**, **Docker**, and **GitHub Actions**, ensuring behavioral parity across environments.

---

## 3. Data Layers (Medallion Architecture)

### Bronze Layer (`data/bronze/`)

* Raw, minimally processed data
* Closest possible representation of the source
* Serves as an immutable audit and recovery layer

### Silver Layer (`data/silver/`)

* Cleaned and validated datasets
* Schema and basic quality guarantees enforced
* Ready for downstream analytical use, but not aggregated

### Gold Layer (`data/gold/`)

* Aggregated, analytics‑ready outputs
* Example artifacts:

  * `aggregates.csv`
  * `freshness.json`

This layered approach improves **data reliability**, **debuggability**, and **long‑term scalability** by preventing tight coupling between raw ingestion and analytical outputs.

---

## 4. Core Modules

### 4.1 Pipeline Orchestrator (`src/pipeline.py`)

* Primary entry point of the system
* Enforces deterministic execution order:

  1. Ingestion
  2. Validation
  3. Storage (Bronze → Silver → Gold)
  4. Metrics generation
* Acts as the single coordination layer for the pipeline

---

### 4.2 Ingestion (`src/ingestion.py`)

* Responsible for fetching or simulating raw source data
* Writes outputs exclusively to the Bronze layer
* Designed to be easily replaceable with real APIs or streaming sources

---

### 4.3 Validation (`src/validation.py`)

* Performs data quality checks before layer promotion
* Typical validations include:

  * Required fields and schema presence
  * Data type correctness
  * Non-empty and minimally viable datasets
  * **Schema enforcement via Pydantic models**
* Prevents corrupted or incomplete data from propagating downstream

---

### 4.4 Storage (`src/storage.py`)

* Centralized read/write abstraction for all data layers
* Isolates filesystem and persistence logic from business rules
* Improves testability and future extensibility (e.g., object storage, databases)

---

### 4.5 Gold Metrics (`src/gold_metrics.py`)

* Computes aggregated metrics from Silver‑layer data
* Produces analytics‑ready Gold outputs
* Responsible for:

  * Summary aggregations
  * Data freshness calculations

---

### 4.6 Logging (`src/logger.py`)

* Centralized logging configuration
* Consistent log structure across all pipeline stages
* Logs persisted under `logs/` for debugging and audit purposes

---

## 5. Configuration Management

### Assets Configuration (`config/assets.yaml`)

* Defines pipeline assets, symbols, and parameters
* Decouples runtime configuration from application logic
* Enables environment‑agnostic execution

### Environment Variables

* Managed via:

  * `.env`
  * `.env.local`
  * example templates for onboarding
* Prevents hard‑coding of secrets and environment‑specific values

---

## 6. Testing Strategy

Tests are located under `tests/` and mirror the source module structure.

| Component    | Test File            |
| ------------ | -------------------- |
| Ingestion    | `test_ingestion.py`  |
| Validation   | `test_validation.py` |
| Storage      | `test_storage.py`    |
| Gold Metrics | `test_gold.py`       |

* Uses **pytest** for unit and integration‑style testing
* Covers happy paths, edge cases, and failure scenarios
* Protects pipeline stability during refactors and feature additions

---

## 7. Automation & CI/CD

### Docker

* `Dockerfile` defines a reproducible runtime environment
* `docker-compose.yml` runs the pipeline as an isolated service
* Ensures consistent behavior across local and CI environments

### Makefile

* Provides standardized developer commands
* Abstracts common tasks such as:

  * Pipeline execution
  * Test execution
  * Container lifecycle management

### GitHub Actions (`.github/workflows/`)

#### `daily_run.yml`

* Executes the pipeline on a fixed schedule
* Simulates production‑like recurring data workloads

#### `email-notify.yml`

* Sends notifications on success or failure
* Integrates operational feedback directly into the CI layer

---

## 8. Observability & Monitoring

* **Data freshness** tracked via `freshness.json`
* Structured logs capture execution flow and error context
* Notifications alert maintainers when freshness or reliability expectations are violated

This observability model reflects real‑world **data reliability engineering** practices.

---

## 9. Design Principles

* Clear separation of concerns
* Configuration over hard‑coded values
* Test‑driven and failure‑aware development
* Reproducible, environment‑agnostic execution
* Production‑inspired folder and module structure

---

## 10. Future Extensions

* Replace simulated ingestion with real external APIs
* Persist Gold outputs to PostgreSQL or cloud storage
* Add volume‑based and anomaly detection checks
* Integrate observability dashboards (Grafana)

---

## 11. Summary

Dataflow Sentinel demonstrates how a **compact yet well‑architected data pipeline** can be built using Python, Docker, and CI automation. The architecture prioritizes reliability, clarity, and operational realism, making the project suitable both as a learning system and as a professional portfolio artifact.
