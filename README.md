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

