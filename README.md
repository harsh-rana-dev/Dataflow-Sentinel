# DATAFLOW-SENTINEL

Production-Inspired DataOps Pipeline with Freshness Monitoring & CI Alerting

---

## Overview

**DATAFLOW-SENTINEL** is a production-inspired data pipeline that ingests financial market data, validates schema and quality, promotes it through a **Bronze → Silver → Gold** architecture, and continuously monitors data freshness.

Modern pipelines often *run* but silently degrade — schemas drift, data becomes stale, and failures go unnoticed.

This system is designed to:

* Detect failures early
* Prevent silent data corruption
* Enforce schema validation
* Guarantee safe re-runs (idempotency)
* Surface freshness violations via CI alerts

The focus is not just moving data — but protecting its integrity.

---

## Why This Project Matters

Most beginner pipelines demonstrate ingestion.

This project demonstrates:

* Validation-first data promotion
* Deterministic orchestration
* Idempotent re-execution
* Structured logging for traceability
* CI-driven monitoring and alerting
* Environment parity (Local ↔ Docker ↔ CI)

It simulates production-grade DataOps project in a compact, readable system.

---

## Architecture

The pipeline follows a **Medallion Architecture**:

**Bronze → Silver → Gold**

**Bronze**
Immutable raw market data from Yahoo Finance

**Silver**
Schema-enforced and validated datasets

**Gold**
Analytics-ready aggregates and freshness monitoring artifacts

Execution is orchestrated via `src/pipeline.py` and runs identically across:

* Local environment
* Docker container
* GitHub Actions (scheduled CI runs)

📄 Detailed architecture: `docs/ARCHITECTURE.md`

---

## Reliability Guarantees

The system is intentionally designed to fail loudly.

* Safe re-runs (idempotent writes and controlled promotion)
* Validation gates prevent downstream corruption
* Structured logs for debugging and auditability
* Freshness tracking via `data/gold/freshness.json`
* CI alerts on failure

No silent degradation.

---

## Project Structure

```
src/        Core pipeline logic (ingestion, validation, storage, metrics)
data/       Bronze / Silver / Gold
tests/      Pytest-based unit & integration tests
config/     Runtime / assets configuration (assets.yaml)
logs/       Structured execution logs
docs/       Architecture & operational runbook
```

The structure enforces strict separation of concerns and stage isolation.

---

## Pipeline Flow

1. **Ingestion**

   * Pulls market data via `yfinance`
   * Writes immutable datasets to Bronze

2. **Validation**

   * Enforces schema with Pydantic
   * Blocks invalid datasets

3. **Promotion**

   * Bronze → Silver → Gold
   * Centralized storage abstraction controls writes

4. **Metrics**

   * Computes aggregates
   * Calculates freshness indicators

5. **Monitoring**

   * Writes `freshness.json`
   * Triggers CI-based email alerts when needed

---

## How to Run

### Local

```bash
make run
```

Uses environment variables defined in `.env`.

---

### Docker

```bash
make docker_run
```

Runs the pipeline in a containerized environment with local PostgreSQL.

---

### CI (GitHub Actions)

* Scheduled daily runs
* Manual workflow dispatch
* Executes tests before pipeline
* Sends email notifications on success or failure

---

## Configuration

### Assets

Defined in:

```
config/assets.yaml
```

This decouples runtime symbols from pipeline logic.

---

### Environment Variables

Managed via:

* `.env`
* `.env.local`
* GitHub Secrets (CI)

Secrets.

---

## Testing

* Built with **pytest**
* Tests mirror the `src/` structure
* Covers ingestion, validation, storage and metrics
* Enforced in CI to prevent regressions

---

## Observability

* Structured logs in `logs/`
* Data freshness tracking in `data/gold/freshness.json`
* CI email alerts on failure

Operational response guide:

📄 `docs/NOTIFY_TEAM.md`

---

## Tech Stack

**Language**

* Python

**Core Libraries**

* pandas
* Pydantic
* yfinance
* SQLAlchemy
* python-dotenv

**Infrastructure**

* PostgreSQL (Local & Neon)
* Docker & Docker Compose
* GitHub Actions
* pytest
* Makefile
* Git & GitHub

---

## Design Principles

* Deterministic execution
* Idempotent re-runs
* Validation-first promotion
* Configuration over hard-coded values
* Reproducible environments
* Fail fast, fail visibly

---

## Limitations

* No real-time ingestion
* No dashboard UI
* Limited anomaly detection beyond freshness
* Optimized for clarity and reliability over scale

---

## Future Improvements

* Replace simulated ingestion with additional external APIs
* Add anomaly-based monitoring
* Integrate observability dashboards (Grafana)
* Expand alert channels (Slack / Discord)

---

## License

MIT License