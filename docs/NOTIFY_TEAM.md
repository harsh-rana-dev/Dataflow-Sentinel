# DATAFLOW-SENTINEL  
## Pipeline Failure & Alert Guide

This document defines when and how to respond to failures or data-quality issues
in the DATAFLOW-SENTINEL market data pipeline.

---

## 📌 Pipeline Overview

- **Assets**: Equities & crypto (AAPL, SPY, BTC-USD)
- **Source**: Yahoo Finance
- **Layers**:
    - **Bronze** → Raw CSV ingestion
    - **Silver** → Validated & cleaned data (CSV + PostgreSQL)
    - **Gold** → Aggregates & data freshness monitoring
- **Schedule**: Daily via GitHub Actions

---

## 🚨 When to Notify

Notify the data owner if **any** of the following occur:

- Pipeline execution fails (non-zero exit)
- `data/gold/data_freshness.json` reports:
  - `"is_stale": true`
- No new bronze or silver files are generated for **2 consecutive runs**
- Gold aggregates (`data/gold/aggregates.csv`) are missing or empty

---

## 🔍 Initial Checks (Run in Order)

1. **GitHub Actions**
   - Open `.github/workflows/daily_run.yml`
   - Inspect the latest workflow run and logs

2. **Pipeline Logs**
   - Review `logs/pipeline_run.json`
   - Identify ingestion, validation, storage, or gold-layer errors

3. **Source Availability**
   - Verify Yahoo Finance availability
   - Confirm markets were open for the expected date

4. **Data Artifacts**
   - Check for newly created files in:
     - `data/bronze/`
     - `data/silver/`
     - `data/gold/`

5. **Database (if enabled)**
   - Confirm PostgreSQL is reachable
   - Check for connection or insert errors

---

## ⚠️ Expected Non-Critical Issues

These do **not** require escalation unless they persist:

- Market holidays (no trading data)
- Temporary Yahoo Finance API rate limits
- Partial asset failure (one symbol missing, others succeed)

---

## 🛠️ Recovery Actions

- **Ingestion failure**
  - Re-run pipeline manually
  - Validate API response format

- **Validation failure**
  - Inspect malformed bronze CSV files
  - Verify expected schema and required columns

- **Gold layer failure**
  - Confirm silver data completeness
  - Recompute gold layer if required

---

## 🚩 Escalation Policy

Escalate if **any** of the following persist:

- Data freshness is stale for **more than 48 hours**
- Pipeline fails for **2 consecutive scheduled runs**
- Gold aggregates are outdated and consumed downstream

**Escalation actions:**
- Notify stakeholders
- Pause dependent analytics or reports
- Document root cause after resolution

---

## ✅ Ownership Statement

DATAFLOW-SENTINEL is treated as a production-grade data pipeline.
Failures are expected — unhandled failures are not.
