# DATAFLOW-SENTINEL  
**Production-Ready Market Data Pipeline with Monitoring & Alerts**

---

## Overview

**DATAFLOW-SENTINEL** is a production-oriented batch data pipeline that ingests, validates, transforms, and monitors market data for equities and cryptocurrencies.

The project is designed to demonstrate **real-world DataOps and Data Engineering practices**, focusing on **reliability, observability, and operational clarity**.

This repository reflects how a small but production-grade pipeline is built, tested, monitored, and maintained in a professional environment.

---

## Why This Project Exists

Many portfolio projects focus on tools rather than outcomes.  
DATAFLOW-SENTINEL focuses on **operational correctness**.

It demonstrates the ability to:
- Build pipelines that **fail loudly, not silently**
- Enforce **data quality and freshness**
- Provide **clear signals when things go wrong**
- Support **repeatable recovery actions**
- Keep the system **simple and maintainable**

---

## Key Capabilities

- Daily automated data ingestion
- Layered data architecture (Bronze / Silver / Gold)
- Schema validation and fail-fast behavior
- Structured logging for observability
- Data freshness monitoring
- Automated tests and CI workflows
- Email alerts on pipeline success or failure

---

## Data Sources & Assets

- **Source**: Yahoo Finance
- **Assets**:
  - Equities: `AAPL`, `SPY`, `TSLA`
  - Cryptocurrency: `BTC-USD`

---

## Architecture Overview

```
Yahoo Finance
│
▼
Bronze Layer (Raw CSV)
│
▼
Silver Layer (Validated & Cleaned)
│
▼
Gold Layer (Aggregates & Freshness Checks)

```

---

## Data Layers

### Bronze Layer — Raw Ingestion
- Stores raw CSV data per asset
- No destructive transformations
- Enables traceability and replay

📁 `data/bronze/`

---

### Silver Layer — Validated Data
- Schema and type validation
- Missing or malformed record handling
- Optional PostgreSQL persistence

📁 `data/silver/`

---

### Gold Layer — Analytics & Monitoring
- Aggregated metrics
- Data freshness evaluation
- Health indicators for downstream consumers

📁 `data/gold/`
- `aggregates.csv`
- `data_freshness.json`

---

## Testing Strategy

Automated tests verify:
- Validation logic
- Transformation correctness
- Failure scenarios

Tests are designed to **fail fast** and surface actionable errors.

Run locally:
```bash
make test
```

---

## Running the Pipeline Locally

### Requirements

* Python 3.10+
* `make`
* Virtual environment recommended

### Install dependencies

```bash
make install
```

### Run the pipeline

```bash
make run
```

---

## Logging & Observability

Each run produces a structured log file:

📄 `logs/pipeline_run.json`

Logs include:

* Execution timestamps
* Step-level outcomes
* Error context and failure reasons

The format is suitable for both human inspection and future log ingestion systems.

---

## Alerts & Notifications

* Email notifications are sent via GitHub Actions
* Notifications are triggered on:

  * Successful pipeline completion
  * Pipeline failure
* Emails contain **only high-level status** to avoid noise

Detailed diagnostics remain available via GitHub Actions logs and artifacts.

Alert response procedures are documented in:

📄 `docs/NOTIFY_TEAM.md`

---

## Scheduling & Automation

* Pipeline runs **daily** via GitHub Actions
* Manual re-runs supported
* CI validates tests on every change

Workflow files:

* `.github/workflows/daily_run.yml`
* `.github/workflows/email-notify.yml`

---

## Docker Status (Intentional Design Choice)

A minimal `docker-compose.yml` exists for PostgreSQL experimentation and future expansion.

Current design choice:

* Pipeline runs **natively**
* Docker is **not required at this stage**

Reasoning:

* Keeps the system simpler
* Avoids unnecessary operational overhead
* Matches early-stage production realities

Full containerized execution is planned as a **future enhancement**, not a premature optimization.

---

## Repository Structure

```
DATAFLOW-SENTINEL/
├── .github/workflows/
├── config/
│   └── assets.yaml
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docs/
│   └── NOTIFY_TEAM.md
├── logs/
│   └── pipeline_run.json
├── src/
├── tests/
├── .env.example
├── docker-compose.yml
├── Makefile
├── README.md
└── requirements.txt
```

---

## Production Mindset

DATAFLOW-SENTINEL is treated as a **production-grade data pipeline**.

Failures are expected.
Silent failures are unacceptable.
Every failure must be **observable, explainable, and recoverable**.

---

## Final Note

This repository represents how **real pipelines are built in small teams**:

* Simple
* Testable
* Observable
* Maintainable