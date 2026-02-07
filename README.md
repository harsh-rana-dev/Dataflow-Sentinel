# DATAFLOW-SENTINEL
Production-inspired DataOps pipeline with freshness monitoring and CI-driven alerting

## Overview

DATAFLOW-SENTINEL is a production-inspired **DataOps pipeline and monitoring system** that ingests financial market data, validates schema and quality, processes it through a layered storage model, and continuously tracks data freshness and reliability.

The project solves a common real-world problem: **data pipelines that “run” but silently degrade or go stale**. DATAFLOW-SENTINEL makes failures visible through validation, logging, freshness metrics, and automated notifications.

This project is designed to demonstrate **production-ready DataOps and Analytics Engineering practices** for recruiters and hiring managers, showcasing practical pipeline design, operational thinking, and CI-driven automation.

---

## Architecture Summary

DATAFLOW-SENTINEL follows a **Medallion Architecture**:

**Bronze → Silver → Gold**

* **Bronze**: raw market data ingested from Yahoo Finance
* **Silver**: validated, schema-enforced datasets
* **Gold**: analytics-ready aggregates and freshness metrics

The pipeline is orchestrated centrally and runs identically across **local**, **Docker**, and **GitHub Actions** environments.

📄 Detailed design: see **docs/ARCHITECTURE.md**

---

## Project Structure

Key directories and their responsibilities:

* `src/`  
  Core pipeline logic (ingestion, validation, storage, metrics, orchestration)

* `data/`  
  Layered data storage:
  * `bronze/` – raw ingested data  
  * `silver/` – validated datasets  
  * `gold/` – aggregates and monitoring artifacts  

* `tests/`  
  Pytest-based unit and integration-style tests mirroring `src/`

* `config/`  
  Runtime configuration (`assets.yaml`, pipeline parameters)

* `logs/`  
  Structured logs for debugging and auditability

This structure mirrors production DataOps systems and enforces clear separation of concerns.

---

## Pipeline Flow

1. **Ingestion**
   * Pulls market data from Yahoo Finance
   * Assets defined in `assets.yaml`
   * Writes raw data to the Bronze layer

2. **Validation**
   * Enforces schema and data quality using **Pydantic**
   * Blocks invalid or incomplete datasets

3. **Storage**
   * Promotes data through Bronze → Silver → Gold
   * Centralized storage abstraction

4. **Metrics**
   * Computes aggregated analytics outputs
   * Calculates data freshness indicators

5. **Monitoring**
   * Writes `freshness.json`
   * Triggers alerts via CI when expectations are violated

---

## How to Run

### Local Run

```bash
make run
````

Uses local environment configuration and **Neon PostgreSQL** when configured via environment variables.

---

### Docker Run

```bash
make docker_run
```

Runs the full pipeline inside a containerized environment with a local PostgreSQL instance.

---

### CI / GitHub Actions

* Scheduled daily runs
* Manual workflow dispatch
* Uses **Neon PostgreSQL**
* Sends email notifications on success or failure

---

## Configuration

### Assets Configuration

`config/assets.yaml` defines tracked assets:

* AAPL
* BTC-USD
* ETH-USD
* SPY
* TSLA
* GC=F

This decouples pipeline logic from runtime configuration.

---

### Environment Variables

* Managed via `.env` and `.env.local`
* Separate configurations for:

  * Local
  * Docker
  * GitHub Actions
* Secrets are never hard-coded

---

## Testing

* Tests located under `tests/`
* Mirrors source structure for clarity
* Uses **pytest**

Tests validate:

* Ingestion correctness
* Schema and data quality enforcement
* Storage behavior
* Gold-layer aggregations and freshness logic

Testing is enforced in CI to prevent regressions.

---

## Automation

* **GitHub Actions**

  * Scheduled daily pipeline runs
  * CI checks on changes
  * Email notifications for pipeline status

* **Makefile**

  * Standardized developer commands
  * Abstracts local, Docker, and test workflows

Automation ensures the pipeline behaves consistently across environments.

---

## Observability & Monitoring

* Centralized structured logging (`logs/`)
* Data freshness tracked via `data/gold/freshness.json`
* Alerts triggered when:

  * Pipeline fails
  * Data becomes stale
  * Expected outputs are missing

Alerting and response procedures are defined in:

📄 **docs/NOTIFY_TEAM.md**

---

## Tech Stack

**Languages**

* Python

**Core Libraries**

* Pydantic
* pandas
* yfinance
* SQLAlchemy
* python-dotenv

**Infrastructure & Tooling**

* PostgreSQL (local & Neon)
* Docker & Docker Compose
* GitHub Actions
* pytest
* Makefile
* Git & GitHub

---

## Design Decisions

* **Medallion Architecture**
  Improves data reliability, debuggability, and long-term scalability

* **Pydantic Everywhere**
  Strong schema guarantees and early failure detection

* **Dockerized Execution**
  Ensures reproducibility across local and CI environments

* **CI Automation**
  Pipelines should fail loudly and visibly, not silently

These decisions reflect real-world DataOps practices.

---

## Limitations

* No real-time or streaming ingestion
* No dashboard visualization layer
* Limited anomaly detection beyond freshness checks
* Designed for clarity and correctness over raw throughput

---

## Future Improvements

* Integrate real-time or streaming data sources
* Persist Gold outputs to analytical databases or warehouses
* Add volume and anomaly-based monitoring
* Introduce dashboards (Grafana)
* Expand alerting channels (Slack, Discord)

---

## License / Disclaimer

This project is provided under the **MIT License**.