# Dataflow-Sentinel

Automated data pipeline for financial market assets.  
Ingests, validates, stores, and aggregates data for AAPL, BTC-USD, SPY, and others.

---

## Features

- **Bronze layer**: Raw data ingestion from Yahoo Finance
- **Silver layer**: Validation and cleaning
- **Gold layer**: Aggregates and freshness checks
- **PostgreSQL**: Idempotent storage, no duplicate records
- **CI/CD**: GitHub Actions workflow, Dockerized local Postgres
- **Automated testing**: Pytest coverage for ingestion, validation, storage, and gold computations

---

## Setup

1. Clone repository
2. Copy `.env.example` → `.env` and fill your credentials
3. Start local Postgres:  
   make docker_up

4. Run pipeline locally:
   make run

5. Run tests:
   make test

---

## Gold Outputs

* `data/gold/aggregates.csv` – 7-day & 30-day average close, latest close & volume
* `data/gold/data_freshness.json` – last available date and staleness check

---

## CI/CD

* Runs daily via GitHub Actions
* Connects to cloud PostgreSQL (Neon) for CI
* Executes pipeline and tests automatically

---

