# DATAFLOW-SENTINEL — Pipeline Alerts & Response Guide

This document defines **when and how the DATAFLOW-SENTINEL pipeline notifies maintainers**, and the **standard response procedure** for pipeline failures or data quality issues.

---

## Alerting & Notification Overview

Email notifications are sent via **GitHub Actions** for the following events:

* ✅ Pipeline run successful
* ❌ Pipeline run failed

**Notes**

* Notifications are intentionally minimal and mobile-friendly
* Full logs and artifacts remain available in GitHub Actions

---

## 🚨 When to Notify the Team

Notify the pipeline owner or on-call maintainer if **any** of the following conditions occur:

* Pipeline execution fails

* `data/gold/freshness.json` reports:

  ```json
  {"status": "STALE"}
  ```

* No new Bronze or Silver outputs for **two consecutive scheduled runs**

* `data/gold/aggregates.csv` is missing, empty, or not updated

---

## 🔍 Initial Checks (Follow in Order)

1. **GitHub Actions**

   * Open the failed workflow run
   * Inspect step-level logs and failure messages

2. **Local Reproduction**

   * Run one of the following:

     ```bash
     make test
     make docker_test
     ```

   * Confirm whether the failure reproduces locally

3. **Pipeline Logs**

   * Inspect log outputs under:

     * `logs/`
     * `logs/pipeline.json`

4. **Source Availability**

   * Verify Yahoo Finance availability
   * Check for market holidays, API downtime, or symbol delistings

5. **Database Health**

   * Confirm the database service is running
   * Check for connection, authentication, or insertion errors

---

## 🛠️ Recovery Actions

### Ingestion Failures

* Re-run the pipeline via GitHub Actions (manual trigger)
* Or execute locally:

  ```bash
  make run
  make docker_run
  ```

### Validation Failures

* Inspect malformed Bronze-layer CSV files
* Confirm no upstream schema or format changes occurred

### Gold Layer Failures

* Verify Silver-layer completeness
* Recompute aggregates after resolving upstream issues

### Environment / Docker Issues

* Reset the environment:

  ```bash
  make docker_clean
  make docker_all
  ```

**Notes**

* The pipeline supports **local**, **Docker**, and **GitHub Actions** execution
* Local and Docker commands are documented in the `Makefile`
* GitHub Actions runs are executed on a schedule

---

## ✅ Healthy Run Signals

A healthy pipeline execution produces:

* New files in `data/bronze/` and `data/silver/`
* Updated `data/gold/aggregates.csv`
* `data/gold/freshness.json` reporting:

  ```json
  {"status": "FRESH"}
  ```

**Environment Mapping**

* Local run → Neon PostgreSQL
* Docker run → Local PostgreSQL container
* GitHub Actions run → Neon PostgreSQL

---

## 🧾 Ownership Statement

DATAFLOW-SENTINEL is treated as a **production-grade data pipeline**.

Failures are expected.

**Silent or unhandled failures are not acceptable.**
