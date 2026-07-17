# 🌦️ Mussoorie Meteorological Data Lakehouse & Phenomenon Predictor

![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Apache%20Spark-E25A1C?logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID%20Transactions-00ADD8)
![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze--Silver--Gold)-blueviolet)

A three-layer **Bronze → Silver → Gold** data lakehouse built on **Databricks, PySpark, and Delta Lake**, deployed as a **Databricks Asset Bundle (DAB)**. It ingests 25+ years of historical climate records and live hourly forecast data for Mussoorie, India from the Open‑Meteo API, and transforms it into business-ready data marts — including a custom-engineered **"Sea of Clouds" (Valley Thermal Inversion) Predictor**.

---

## 📌 Overview

Raw weather APIs return messy, loosely-typed, high-frequency data that isn't directly usable for decision-making. This project applies core **data engineering principles** — schema enforcement, partitioning, idempotent upserts, and layered transformation — to turn that raw feed into three domain-specific analytical products:

- **Operational logistics intelligence** — dynamic delivery-surge pricing and rider-safety alerts driven by live weather conditions
- **Climate analytics** — daily, monthly, and annual aggregated summaries for trend analysis
- **A custom meteorological scoring engine** — the Sea of Clouds Predictor, which evaluates five independent atmospheric constraints per hour to flag optimal valley thermal-inversion viewing windows

---

## 🧱 Tech Stack

| Category | Tools / Concepts |
|---|---|
| **Languages** | Python, SQL |
| **Big Data Processing** | Apache Spark (PySpark), Spark SQL |
| **Storage Layer** | Delta Lake — ACID transactions, schema enforcement, `MERGE INTO` upserts |
| **Platform** | Databricks Lakehouse, Databricks Workflows, Unity Catalog (3-level namespace: catalog → schema → table) |
| **Deployment / IaC** | Databricks Asset Bundles (DAB), Databricks CLI |
| **Data Source** | Open-Meteo REST API (Historical Archive + Forecast endpoints) |
| **Architecture Pattern** | Medallion Architecture (Bronze → Silver → Gold) |
| **Core Concepts** | ETL/ELT design, idempotent ingestion, data partitioning, schema-on-write, data quality validation, dimensional data marts |

---

## 🏗️ System Architecture

```text
                    [ Open-Meteo REST API ]
                  (Historical Archive + Forecast)
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│  🥉 BRONZE — weather.bronze.bronze_weather                    │
│  • Raw hourly records, explicit typed schema on write         │
│  • Historical backfill (batch) + live merge (incremental)     │
│  • Delta Lake MERGE INTO for upsert-based deduplication       │
└──────────────────────────┬──────────────────────────────────┘
                            ▼
┌──────────────────────────────────────────────────────────────┐
│  🥈 SILVER — weather.silver.silver_weather                    │
│  • Deduplicated on primary time key                            │
│  • Derived fields: date, year, month, hour, season             │
│  • Partitioned by 5-year buckets for partition pruning         │
└──────────────────────────┬──────────────────────────────────┘
              ┌─────────────┼──────────────┐
              ▼             ▼              ▼
┌──────────────────┐ ┌──────────────────┐ ┌───────────────────────┐
│ 🥇 GOLD:          │ │ 🥇 GOLD:          │ │ 🥇 GOLD:               │
│ Climate Summaries │ │ Logistics Surge   │ │ Sea of Clouds          │
│ (daily/monthly/   │ │ & Safety Alerts   │ │ Predictor              │
│  annual rollups)  │ │                   │ │ (5-factor scoring)     │
└──────────────────┘ └──────────────────┘ └───────────────────────┘
```

---

## 🔄 Pipeline Details

### 🥉 Bronze — Ingestion
- Pulls hourly meteorological metrics (temperature, humidity, cloud cover at three altitudes, rain, snowfall, precipitation, dew point, wind speed/direction) from Open-Meteo.
- Enforces an explicit `StructType` schema at ingestion so every downstream column has a known, correct type — no lexical string-sorting bugs later in the pipeline.
- The **live daily stream** notebook performs a Delta Lake `MERGE INTO` keyed on timestamp (update-if-matched, insert-if-not) so repeated hourly pulls don't create duplicate records.

### 🥈 Silver — Standardization
- Deduplicates on the `time` primary key.
- Derives `date`, `year`, `month`, `hour`, and a `season` label (winter / summer / monsoon) for downstream filtering.
- Writes as a partitioned Delta table to keep query scans efficient as the historical window grows.

### 🥇 Gold — Business-Ready Data Marts

**1. Climate Summaries** — daily, monthly, and annual rollups (avg/min/max temperature, total rainfall & snowfall, average wind speed) for trend and seasonality analysis. Scheduled as part of the automated Databricks Workflow.

**2. Logistics Surge & Safety** — translates live weather into a `final_surge_multiplier` (up to +230% over base rate) using rain and snowfall severity bands, plus an automated `dispatch_safety_warning` (e.g. *"CRITICAL: Suspend 2-Wheeler Deliveries"*) for hazardous wind, rain, or snow conditions — the kind of rule engine used in real-world dynamic pricing and fleet-safety systems.

**3. Sea of Clouds Predictor** — a custom 5-constraint scoring engine that evaluates every hour against the atmospheric conditions required for a valley temperature inversion (low cloud cover 80–100%, clear mid/high sky ≤20%, humidity ≥85%, dew point depression 0–2°C, wind <8 km/h), then aggregates a monthly `perfect_viewing_hours` metric to surface optimal viewing windows.

---

## ⚙️ Orchestration & Deployment

Packaged as a **Databricks Asset Bundle**, so the entire pipeline — jobs, schedule, and task dependencies — is defined as code and deployed with the Databricks CLI rather than clicked together manually.

```bash
# 1. Install the Databricks CLI
# https://docs.databricks.com/en/dev-tools/cli/install.html

# 2. Authenticate to your workspace
databricks configure

# 3. Deploy the bundle
databricks bundle deploy

# 4. Trigger a run
databricks bundle run gold_summeries
```

The core Bronze → Silver → Gold (Climate Summaries) flow runs as a dependency-chained multi-task job on a configurable interval. The Logistics Surge and Sea of Clouds marts currently run as standalone notebooks and are next in line to be added to the same scheduled DAG (see Roadmap).

---

## 📂 Project Structure

```
weather-pipeline/
├── README.md
└── weather_forcasting/
    ├── databricks.yml              # Databricks Asset Bundle: job + task DAG definition
    ├── 1_setup.py                  # Catalog/schema bootstrap
    ├── historic_bronze.py          # Historical backfill → Bronze
    ├── Live_daily_stream.py        # Live hourly ingestion → Bronze (Delta MERGE)
    ├── silver_transformation.py    # Bronze → Silver cleaning & partitioning
    ├── gold_climate_summaries.py   # Gold: daily/monthly/annual climate rollups
    ├── gold_logistic_surge.py      # Gold: dynamic surge pricing & safety alerts
    └── gold_optimal_weather_view.py# Gold: Sea of Clouds predictor
```

---

## ✅ Data Quality

- Explicit typed schema enforced at ingestion (no implicit/inferred types).
- Row-count and null-count checks at each layer to catch upstream API issues early.
- Duplicate detection on the primary time key before promoting Bronze → Silver.

---

## 🗺️ Roadmap

- [ ] Move Bronze/Silver/Gold writes from full-table overwrite to incremental `MERGE`-based updates as data volume grows
- [ ] Add the Logistics Surge and Sea of Clouds marts to the scheduled Workflow DAG
- [ ] Enforce range-based data quality rules (e.g. humidity 0–100%, wind direction 0–360°) with automatic quarantine of invalid rows
- [ ] Add a Tourism Impact mart joining weather against an Indian holiday-calendar dimension
- [ ] CI/CD via GitHub Actions to validate and auto-deploy the Asset Bundle on push
- [ ] Job-failure alerting (email/Slack webhook) and a data-freshness SLA dashboard
- [ ] Scheduled `OPTIMIZE` / `ZORDER` / `VACUUM` maintenance on Delta tables
- [ ] Environment separation (dev/staging/prod) via Asset Bundle targets

---

## 💡 Key Engineering Highlights

- Designed and implemented a **Medallion Architecture** lakehouse from raw API ingestion through to business-facing data marts
- Built **idempotent, upsert-based ingestion** using Delta Lake `MERGE INTO` to safely re-run live data pulls without duplication
- Engineered a **custom multi-constraint scoring algorithm** (Sea of Clouds Predictor) translating raw meteorological fields into a domain-specific business metric
- Modeled a **dynamic pricing and safety rules engine** consuming live weather signals — patterns directly applicable to logistics, ride-hailing, and delivery platforms
- Deployed the full pipeline as **Infrastructure-as-Code** using Databricks Asset Bundles, with a dependency-chained multi-task Workflow DAG
- Processes **25+ years of hourly historical data** (~225,000+ records across 12 meteorological metrics) alongside a rolling live forecast window

---

## 👤 Author

**Devvrat Pandey**
Computer Science (Data Science) student · aspiring Data Engineer

- GitHub: [github.com/devvratin](https://github.com/devvratin)
- LinkedIn: [linkedin.com/in/devvrat-pandey](https://linkedin.com/in/devvrat-pandey)

---

## 📄 License

MIT License — see `LICENSE` for details.