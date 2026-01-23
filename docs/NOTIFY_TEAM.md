# DATAFLOW-SENTINEL
## Pipeline Alerts & Response Guide

This document describes how the DATAFLOW-SENTINEL pipeline notifies maintainers about execution outcomes and how to respond to failures or data-related issues.

The goal is to ensure timely awareness and predictable recovery actions.

---

## 📌 Pipeline Overview

- **Assets**: Equities & crypto (AAPL, SPY, BTC-USD, TSLA)
- **Source**: Yahoo Finance
- **Layers**:
  - **Bronze** → Raw CSV ingestion
  - **Silver** → Validated & cleaned data (CSV + PostgreSQL)
  - **Gold** → Aggregates & data freshness monitoring
- **Schedule**: Daily via GitHub Actions

---

## 🔔 Notification Summary

The DATAFLOW-SENTINEL pipeline sends **email notifications** upon pipeline completion.

Notifications are sent when:
- The pipeline completes successfully
- The pipeline fails during execution

Skipped runs do **not** trigger notifications.

Emails are intentionally minimal and include only high-level status information.  
Detailed logs and artifacts remain available in GitHub Actions.

---

## 🚨 When to Notify

Notify the pipeline owner or on-call maintainer if **any** of the following occur:

- Pipeline execution fails (non-zero exit)
- `data/gold/data_freshness.json` reports:
  - `"is_stale": true`
- No new bronze or silver files are generated for **two consecutive scheduled runs**
- Gold aggregates (`data/gold/aggregates.csv`) are missing or empty

---

## 🔍 Initial Checks (Run in Order)

1. **GitHub Actions**
   - Open the workflow run associated with the failure
   - Inspect step-level logs and failure messages

2. **Pipeline Logs**
   - Review `logs/pipeline_run.json`
   - Identify ingestion, validation, storage, or gold-layer errors

3. **Source Availability**
   - Verify Yahoo Finance availability
   - Confirm whether markets were open for the expected date

4. **Data Artifacts**
   - Check for newly created files in:
     - `data/bronze/`
     - `data/silver/`
     - `data/gold/`

5. **Database (if enabled)**
   - Confirm PostgreSQL connectivity
   - Check for connection, authentication, or insert errors

---

## ⚠️ Expected Non-Critical Issues

The following conditions do **not** require escalation unless they persist:

- Market holidays or non-trading days
- Temporary Yahoo Finance API rate limits
- Partial asset failure (one symbol missing while others succeed)

---

## 🛠️ Recovery Actions

- **Ingestion failure**
  - Re-run the pipeline using the GitHub Actions manual trigger
  - Inspect upstream API responses for schema or format changes

- **Validation failure**
  - Inspect malformed bronze CSV files
  - Verify expected schema and required columns

- **Gold layer failure**
  - Confirm silver-layer completeness
  - Recompute gold aggregates if required

---

## 🚩 Escalation Policy

Escalate if **any** of the following persist:

- Data freshness remains stale for **more than 48 hours**
- Pipeline fails for **two consecutive scheduled runs**
- Gold aggregates are outdated and consumed downstream

**Escalation actions may include:**
- Notifying stakeholders
- Pausing dependent analytics or reports
- Documenting root cause and resolution after recovery

---

## 🚫 Out of Scope

This document does not cover:
- Infrastructure outages outside the pipeline scope
- Long-term schema migrations
- Upstream data provider contract or policy changes

---

## ✅ Ownership Statement

DATAFLOW-SENTINEL is treated as a production-grade data pipeline.  
Failures are expected — **unhandled failures are not**.
