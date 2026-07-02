# Mussoorie Historical & Real-Time Weather Data Engineering Pipeline

A production-ready Data Lakehouse pipeline built on **Apache Spark (PySpark)** and **Databricks** to ingest, process, and analyze over 45 years of historical climate data for Mussoorie, India (1980–2026). The project utilizes an **ELT (Extract, Load, Transform)** pattern structured around the **Medallion Architecture** (Bronze, Silver, and Gold layers), built with modularity at its core to ensure seamless integration with real-time operational feeds.

---

## 🏗️ Medallion Pipeline Architecture

### 1. 🟫 Bronze Layer (Raw Ingestion)
* **Goal:** Act as the historical, unaltered single source of truth.
* **Process:** Handles the initial ingestion of `mussoorie_1980_2026.csv`. Because the raw source contains an arbitrary meteorological metadata preamble (latitude, longitude, elevation) in its first few lines, standard automatic schema inference fails. This layer safely captures the raw text state before parsing.

### 2. 🥈 Silver Layer (Cleansing & Enrichment)
* **Goal:** Produce a standardized, validated, and highly queryable dataset.
* **Process:** 
  * Truncates the file metadata to extract the core 13-column weather stream.
  * Standardizes messy header columns into clean database conventions (e.g., converting `temperature_2m (°C)` to `temperature_2m_c`).
  * Enforces strict data schemas by casting metrics to appropriate numeric types (`FloatType`) and handles missing value boundaries (e.g., relative humidity between `0` and `100%`).
  * **Feature Engineering:** Extracted `date` timestamps and mapped meteorological seasons (`WR` for Winter, `SG` for Spring, `SR` for Summer, `AU` for Autumn).
  * Grouped the physical data into decadal folders using Spark's `partitionBy()` to resolve the **"Small Files Problem"** and maximize partition pruning performance.

### 3. 🥇 Gold Layer (Analytics & Business Intelligence)
* **Goal:** Present aggregated, high-value metrics optimized for business intelligence tools (PowerBI/Tableau) and machine learning models.
* **Process:** Summarizes highly granular hourly sensor data into analytical views:
  * Monthly and yearly climate trends (averages, maximums, minimums).
  * Extreme weather event anomalies (freezing points, high-wind thresholds).

---

## ⏱️ Project Evolution: Present vs. Future

To demonstrate production-grade data engineering concepts, this project is divided into two operational phases:

### 🟢 Present State (Historical Batch Ingestion)
* **Status:** Complete.
* **Scope:** Processes the 46-year historical static baseline (`mussoorie_1980_2026.csv`) to seed the Lakehouse.
* **Implementation:** Core transformations are completely encapsulated inside modular Python functions rather than loose script cells. This means the cleaning logic is decoupled from the storage layer, allowing it to be reused dynamically in the next phase.

### 🔵 Future Roadmap (Automated Live Streaming)
To transition this from an offline project into a living enterprise pipeline, the following architecture is being deployed:
1. **Automated Live Daily Ingestion:** Hooking up a cron-scheduled script or an AWS Lambda trigger to fetch the previous day's live Mussoorie weather metrics via weather APIs.
2. **Incremental Upserts (Delta MERGE):** Replacing standard overwrites with Delta Lake `MERGE INTO` operations in the Silver layer. This ensures that incoming daily streams are deduplicated on the fly, updating existing records if sensor data changes or appending them if they are new.
3. **Dimensional Enrichment:** Ingesting a separate Uttarakhand cultural and tourism calendar table into the Silver layer. In the Gold layer, a `gold_tourism_weather_impact` view will join this calendar to the weather data to analyze how extreme climate events correlate with regional holiday tourist spikes.
4. **Orchestration:** Moving from manual execution to automated scheduling using **Databricks Workflows** or **Apache Airflow** for end-to-end pipeline tracking, retries, and alerting.

---

## 🛠️ Tech Stack & Tools
* **Engine:** Apache Spark (PySpark)
* **Platform:** Databricks Environment
* **Storage Format:** Delta Lake (Parquet-backed)
* **Language:** Python
* **Libraries:** PySpark SQL Functions (`pyspark.sql.functions`)