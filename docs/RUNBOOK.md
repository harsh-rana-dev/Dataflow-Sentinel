# DATAFLOW-SENTINEL — Operational Runbook

This document defines the **alerting behavior, failure conditions, and operational response procedures** for the Dataflow Sentinel pipeline.

It functions as a lightweight **runbook** for maintainers and reflects production-oriented operational standards.

---

## 1. Alerting & Monitoring Overview

Pipeline health signals are surfaced through multiple monitoring layers:

* GitHub Actions (CI execution status)
* Email notifications (success / failure)
* Sentry (runtime exception tracking)

Email alerts are triggered for:

* ✅ Successful scheduled run
* ❌ Failed scheduled run

Sentry captures:

* Unhandled runtime exceptions
* Full stack traces
* Environment context (local / Docker)

### Design Principles

* Alerts are intentionally minimal and mobile-friendly
* Detailed logs and artifacts are available within GitHub Actions
* Runtime errors are persisted externally via Sentry
* Failures must always be actionable
* Silent failures are unacceptable
---

## 2. 🚨 Alert Conditions

The pipeline owner or on-call maintainer must investigate immediately if any of the following occur:

### 2.1 Execution Failure

* GitHub Actions workflow fails
* Unhandled exception during pipeline run
* Non-zero exit status

---

### 2.2 Data Freshness Violation

If `data/gold/freshness.json` reports:

```json
{"status": "STALE"}
```

This indicates:

* Upstream ingestion delay
* Market holiday or source outage
* Validation failure preventing promotion
* Pipeline regression

---

### 2.3 Missing Layer Updates

If either condition occurs:

* No new Bronze or Silver outputs for **two consecutive scheduled runs**
* `data/gold/aggregates.csv` missing, empty, or unchanged

This signals possible ingestion failure or blocked promotion.

---

## 3. 🔍 Incident Response Procedure

Follow the steps in order.

---

### Step 1 — Inspect GitHub Actions

* Open the failed workflow run
* Review step-level logs
* Identify the failing module (Ingestion, Validation, Storage, or Metrics)

Do not rerun blindly without reviewing logs.

---

### Step 1A — Inspect Sentry (Runtime Errors)

If the pipeline fails due to an unhandled exception:

1. Open the Sentry dashboard
2. Locate the most recent event
3. Inspect:

   * Stack trace
   * Affected module

Sentry provides:

* Faster root cause identification
* Persistent error history across runs
* Aggregated recurring failure insights

If no Sentry event exists:

* Confirm `SENTRY_DSN` is configured
* Verify environment variable injection in CI
* Ensure monitoring initialization executes before pipeline logic

---

### Step 2 — Attempt Local Reproduction

Run:

```bash
make test
make docker_test
```

If the issue reproduces locally, the root cause is deterministic.

If not, investigate environment-specific differences.

---

### Step 3 — Inspect Pipeline Logs

Review structured logs under:

* `logs/`
* `logs/pipeline.json`

Focus on:

* Tracebacks
* Validation errors
* Storage write failures
* Data volume anomalies

---

### Step 4 — Verify Source Availability

Check:

* Yahoo Finance availability
* Market holidays

If the external source is unavailable, document the incident and monitor next scheduled run.

---

### Step 5 — Verify Database Health

Confirm:

* PostgreSQL service is running
* Credentials are valid
* Connection pool is functional
* No insertion conflicts or schema drift

Environment mapping:

* Local → Neon PostgreSQL
* Docker → Local PostgreSQL container
* GitHub Actions → Neon PostgreSQL

---

## 4. 🛠️ Recovery Actions

Recovery actions depend on failure classification.

---

### 4.1 Ingestion Failures

If ingestion fails:

* Manually re-run via GitHub Actions
  OR
* Execute locally:

```bash
make run
make docker_run
```

Verify Bronze output is regenerated.

---

### 4.2 Validation Failures

If validation blocks promotion:

* Inspect malformed Bronze-layer CSV files
* Check for upstream schema changes
* Confirm required fields remain present
* Review Pydantic schema enforcement rules

Never bypass validation without understanding the cause.

---

### 4.3 Gold Layer Failures

If metrics or aggregation fails:

* Confirm Silver completeness
* Recompute aggregates after upstream resolution
* Validate freshness logic

Gold failures are usually downstream symptoms.

---

### 4.4 Docker / Environment Failures

If containerization fails:

```bash
make docker_clean
make docker_all
```

Rebuild environment to eliminate stale state.

---

## 5. ✅ Healthy Run Signals

A successful pipeline execution produces:

* New files in `data/bronze/`
* Validated outputs in `data/silver/`
* Updated `data/gold/aggregates.csv`
* `data/gold/freshness.json` reporting:

```json
{"status": "FRESH"}
```

Additionally:

* Logs show deterministic execution flow
* No validation errors
* No skipped assets
* GitHub Actions status: **Success**

---

## 6. Operational Philosophy

Dataflow Sentinel is treated as a **production-inspired data pipeline**.

Principles:

* Fail fast
* Never silently ignore corruption
* Validate before promotion
* Prefer reproducibility over convenience
* Re-runs must be safe (idempotency guaranteed)
* Runtime errors must be observable (CI + Sentry)

Failures are expected.

**Silent or unhandled failures are not acceptable.**