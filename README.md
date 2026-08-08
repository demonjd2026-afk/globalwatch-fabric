# 🌍 GlobalWatch — World Air Quality Intelligence Platform

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-F64_Trial-0078D4?style=flat&logo=microsoft)](https://app.fabric.microsoft.com)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?style=flat&logo=apachespark)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.2-00ADD8?style=flat)](https://delta.io)
[![OpenAQ](https://img.shields.io/badge/OpenAQ-v3_API-4CAF50?style=flat)](https://openaq.org)
[![Claude](https://img.shields.io/badge/AI_Agent-Claude_Sonnet-8B5CF6?style=flat)](https://anthropic.com)
[![License](https://img.shields.io/badge/License-MIT-yellow?style=flat)](LICENSE)

> Production-grade Lambda architecture on Microsoft Fabric — real-time + batch air quality intelligence for 10,000+ stations across 90 countries, with PySpark optimization, KQL dashboards, Data Activator alerting, and a Claude-powered AI agent for natural language analytics.

**Built by:** [Jayanth Dolai](https://www.linkedin.com/in/jayanth-dolai-7b115213a/) · Senior Data Engineer
**Resume duration:** Apr 2026 – Jul 2026
**Platform:** Microsoft Fabric (F64 Trial) · Power BI Trial: 59 days

---

## 📌 Project Summary

GlobalWatch solves a real-world problem: air quality data is fragmented across dozens of public APIs with no unified platform that correlates real-time sensor readings with historical trends, weather patterns, and country-level health indicators at global scale.

This project builds that platform end-to-end on Microsoft Fabric — ingesting live readings from OpenAQ (10,000+ stations, 90+ countries), enriching with World Bank GDP/health data and OpenMeteo weather, processing through a Bronze → Silver → Gold medallion lakehouse, and serving via Direct Lake Power BI reports and a Claude Sonnet AI agent for natural language querying.

**What makes this globally unique:**
- No public portfolio project combines OpenAQ + World Bank + Fabric RTI + Claude tool-use agent
- Real data shows India at 100% PM2.5 WHO exceedance (avg 175 µg/m³ vs guideline of 15)
- Mongolia at 100% PM2.5 exceedance (avg 131 µg/m³) — real crisis, real data

---

## 🏗️ Architecture

### Lambda Architecture Overview

```
┌──────────────────────────────── DATA SOURCES ───────────────────────────────┐
│                                                                              │
│  OpenAQ API          WAQI API          World Bank        OpenMeteo           │
│  (real-time)         (batch)           (annual CSV)      (daily batch)       │
│  10K+ stations       AQI by city       GDP / health      Weather             │
└──────┬───────────────────┬─────────────────┬────────────────┬───────────────┘
       │                   │                 │                │
       ▼                   ▼                 ▼                ▼
┌──────────────────────────────── INGESTION LAYER ────────────────────────────┐
│                                                                              │
│  Fabric Eventstream   FDF Watermark      Dataflow Gen2     OneLake Shortcut  │
│  (5s polling)         Pipeline           (HTTP + CSV)       (ADLS Gen2)      │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                     ┌─────────────▼──────────────┐
                     │      ONELAKE MEDALLION      │
                     │                             │
                     │  bronze_globalwatch         │
                     │    Raw Delta + KQL DB       │
                     │         │                   │
                     │         ▼                   │
                     │  silver_globalwatch         │
                     │    Cleaned · DQ · AQI       │
                     │         │                   │
                     │         ▼                   │
                     │  gold_globalwatch           │
                     │    Star Schema · V-Order    │
                     │                             │
                     │  globalwatch_warehouse      │
                     │    Cross-LH SQL endpoint    │
                     └─────────────┬──────────────┘
                                   │
          ┌────────────────────────┼────────────────────────┐
          ▼                        ▼                        ▼
┌─────────────────┐   ┌────────────────────┐   ┌───────────────────┐
│  REAL-TIME      │   │  BATCH SERVING     │   │  AI AGENT         │
│  INTELLIGENCE   │   │                    │   │                   │
│                 │   │  Direct Lake PBI   │   │  Claude Sonnet    │
│  KQL Database   │   │  RLS by continent  │   │  Tool-use         │
│  Update policy  │   │                    │   │  NL → KQL/SQL     │
│  RTI Dashboard  │   │  Fabric Warehouse  │   │  Streamlit app    │
│  Data Activator │   │  Ad-hoc SQL        │   │                   │
└─────────────────┘   └────────────────────┘   └───────────────────┘
```

### Two Paths — One OneLake

| Path | Frequency | Latency | Use Case |
|---|---|---|---|
| **Batch** (FDF + PySpark + Gold) | Daily | Hours | Historical analytics, trend reports, WHO exceedance tracking |
| **Real-time** (Eventstream + KQL) | 5 seconds | Seconds | Live AQI dashboard, hazard alerting via Data Activator |

---

## 📂 Repository Structure

```
globalwatch-fabric/
│
├── README.md                        ← You are here
├── SETUP.md                         ← Step-by-step setup with screenshots
├── TECH_SPEC.md                     ← Full technical specification + ADRs
├── .gitignore
│
├── notebooks/                       ← PySpark notebooks (run in order)
│   ├── README.md                    ← Detailed notebook guide
│   ├── 01_bronze_ingest_openaq.ipynb
│   ├── 04_silver_transform.ipynb
│   └── 05_gold_star_schema.ipynb
│
├── docs/                            ← Deep-dive documentation
│   ├── architecture.md              ← ADRs + architecture decisions
│   ├── data_model.md                ← Star schema ERD + SCD logic
│   ├── spark_optimization.md        ← All 8 optimization patterns with code
│   └── interview_narratives.md      ← STAR stories per project phase
│
└── screenshots/                     ← Evidence of working implementation
    ├── 01_fabric_home.png
    ├── 02_workspace_created.png
    ├── 03_three_lakehouses.png
    ├── 04_warehouse_created.png
    ├── 10_bronze_notebook_run.png
    ├── 11_silver_transform_run.png
    ├── 12_spark_ui_aqe.png
    ├── 13_gold_notebook_scd2_merge.png
    └── 14_gold_delta_table_detail.png
```

---

## 🗃️ Data Sources

| Source | Data | Frequency | Ingestion Method | Cost |
|---|---|---|---|---|
| [OpenAQ v3](https://openaq.org) | PM2.5, PM10, NO2, CO, O3 — 10K+ stations globally | Real-time (5s) | Fabric Eventstream custom endpoint | Free |
| [WAQI](https://aqicn.org/api/) | Historical AQI by city | Daily batch | FDF watermark pipeline | Free (1K calls/day) |
| [World Bank](https://data.worldbank.org) | GDP per capita, health expenditure, population | Annual | Dataflow Gen2 HTTP connector | Free |
| [OpenMeteo](https://open-meteo.com) | Temperature, humidity, wind speed | Daily batch | Dataflow Gen2 HTTP connector | Free |

---

## 🥉🥈🥇 Medallion Lakehouse Design

### Bronze — Raw Landing Zone
- Append-only Delta tables — raw data never modified
- `mergeSchema=True` — handles OpenAQ API schema evolution automatically
- Partitioned by `ingestion_date`
- Watermark control table tracks last loaded date per source
- KQL Database receives parallel real-time Eventstream feed

### Silver — Cleaned and Conformed
Four DQ rules applied:

| Rule | Filter | Purpose |
|---|---|---|
| Empty parameter | `parameter != ""` | Drop unclassified sensors |
| Known pollutants | `parameter IN (pm25,pm10,no2,co,o3)` | Keep criteria pollutants only |
| Valid values | `0 < value < 10000` | Drop sensor malfunctions |
| Deduplication | `dropDuplicates(location_id, parameter, reading_ts)` | Remove duplicate API records |

Partitioned by `country_code + year_month` — enables country-level partition pruning.

### Gold — Star Schema Serving Layer

```
dim_date (5,844 rows)     dim_pollutant (5 rows)
     │                          │
     └──────────┬───────────────┘
                │
         fact_readings (344+ rows)
                │
     ┌──────────┴───────────┐
     │                      │
dim_station (121 rows)  dim_country (9 rows)
(SCD Type 2)            (SCD Type 1)
```

| Table | SCD | Rows | Key feature |
|---|---|---|---|
| `fact_readings` | — | 344+ | V-Order ON, Z-Order on location_id+reading_ts |
| `dim_station` | Type 2 | 121 | Delta MERGE — active_flag + effective_start/end |
| `dim_country` | Type 1 | 9 | Continent mapping + World Bank enrichment |
| `dim_date` | Type 0 | 5,844 | Pre-generated spine 2015–2030 |
| `dim_pollutant` | Type 0 | 5 | WHO 2021 annual guidelines |

---

## ⚡ PySpark Optimization Patterns

All 8 patterns implemented — see [`docs/spark_optimization.md`](docs/spark_optimization.md) for full code.

| Optimization | Where Applied | Benefit |
|---|---|---|
| **AQE** (Adaptive Query Execution) | All notebooks | Dynamic coalescing + skew join handling |
| **Broadcast join** | Gold fact build | Eliminates shuffle for dim_country (9 rows), dim_pollutant (5 rows) |
| **Salting** | Silver skewed joins | Prevents OOM on high-volume city stations (Beijing, Delhi) |
| **Partitioning** | Silver + Gold | Country + time-based partition pruning |
| **Z-Order** | fact_readings | Co-locate station+time data — reduces files scanned from 25 → 1-2 |
| **V-Order** | All Gold tables | Mandatory for Direct Lake Power BI — VertiPaq scan optimization |
| **Delta MERGE** | dim_station SCD2 | Atomic expire + insert in one transaction |
| **mergeSchema** | Bronze writes | Handles OpenAQ API schema drift |

---

## 📊 Real-Time Intelligence Layer

### KQL Database — Update Policy
Transforms raw readings to AQI categories automatically as they stream in:
```kql
.create-or-alter function TransformRawReadings() {
    raw_readings
    | extend aqi_category = case(
        pm25_value <= 12.0,  "Good",
        pm25_value <= 35.4,  "Moderate",
        pm25_value <= 55.4,  "Unhealthy for Sensitive",
        pm25_value <= 150.4, "Unhealthy",
                             "Hazardous")
}
```

### Data Activator — PM2.5 Hazard Alert
- **Trigger:** PM2.5 > 150 µg/m³ for 3 consecutive readings from same station
- **Action:** Microsoft Teams notification with station name, city, country, current reading
- **Threshold basis:** WHO Hazardous category — serious health risk for entire population

---

## 🤖 AI Agent (Claude Sonnet Tool-Use)

Streamlit app with three tools that route natural language questions to the correct data layer:

```
User question
      │
      ▼
Claude Sonnet
      │
      ├── "last 6 hours?" ──► query_kql ──► KQL Database (live data)
      │
      ├── "last 2 years?" ──► query_gold_sql ──► Gold Lakehouse SQL endpoint
      │
      └── "GDP correlation?" ──► get_country_health_context ──► dim_country
```

**Example queries:**
- *"Which Asian cities had hazardous PM2.5 in the last 6 hours?"*
- *"Show India's PM2.5 monthly average for 2025"*
- *"How does Mongolia's air quality compare to its health expenditure?"*

---

## 📈 Current Dataset Findings

Real findings from the implemented pipeline:

| Country | Pollutant | Avg Value | WHO Guideline | Exceedance |
|---|---|---|---|---|
| India | PM2.5 | 175.83 µg/m³ | 15 µg/m³ | **11.7x** |
| Mongolia | PM2.5 | 131.4 µg/m³ | 15 µg/m³ | **8.8x** |
| India | NO2 | 81.02 µg/m³ | 10 µg/m³ | **8.1x** |
| Mongolia | NO2 | 58.23 µg/m³ | 10 µg/m³ | **5.8x** |
| Chile | NO2 | 25.19 µg/m³ | 10 µg/m³ | **2.5x** |

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Platform | Microsoft Fabric (F64 Trial → F2) |
| Storage | OneLake, Delta Lake 3.2, Parquet |
| Batch ingestion | Fabric Data Factory, Dataflow Gen2 |
| Real-time ingestion | Fabric Eventstream (custom endpoint) |
| Compute | Fabric Spark (PySpark 3.5) |
| Real-time analytics | KQL Database, KQL Queryset, RTI Dashboard |
| Alerting | Data Activator → Microsoft Teams |
| Serving | Direct Lake Power BI + RLS, Fabric Warehouse |
| AI agent | Claude Sonnet (tool-use), Streamlit |
| Governance | Microsoft Purview, sensitivity labels, workspace RBAC |
| CI/CD | Azure DevOps Git integration, Fabric deployment pipelines |

---

## 🚀 Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/demonjd2026-afk/globalwatch-fabric.git

# 2. Follow the setup guide
# See SETUP.md for full step-by-step with screenshots

# 3. Get a free OpenAQ API key
# https://explore.openaq.org/register

# 4. Create Fabric workspace + lakehouses
# See SETUP.md Phase 1-2

# 5. Run notebooks in order
# notebooks/01_bronze_ingest_openaq.ipynb
# notebooks/04_silver_transform.ipynb
# notebooks/05_gold_star_schema.ipynb

# 6. Run AI agent locally
cd ai_agent
pip install -r requirements.txt
streamlit run app.py
```

---

## 📸 Screenshots

| What | Screenshot |
|---|---|
| Fabric home (Power BI trial active) | [01_fabric_home](screenshots/01_fabric_home.png) |
| globalwatch-dev workspace | [02_workspace_created](screenshots/02_workspace_created.png) |
| Three medallion lakehouses | [03_three_lakehouses](screenshots/03_three_lakehouses.png) |
| Fabric Warehouse | [04_warehouse_created](screenshots/04_warehouse_created.png) |
| Bronze notebook — 368 rows written | [10_bronze_notebook_run](screenshots/10_bronze_notebook_run.png) |
| Silver validation — 9 countries, zero nulls | [11_silver_transform_run](screenshots/11_silver_transform_run.png) |
| Spark UI — 17 jobs succeeded | [12_spark_ui_aqe](screenshots/12_spark_ui_aqe.png) |
| Gold validation — SCD2 + WHO exceedances | [13_gold_notebook_scd2_merge](screenshots/13_gold_notebook_scd2_merge.png) |
| Delta table detail — OneLake path | [14_gold_delta_table_detail](screenshots/14_gold_delta_table_detail.png) |

---

## 📋 Interview Talking Points

See [`docs/interview_narratives.md`](docs/interview_narratives.md) for full STAR stories.

| Question | Short answer |
|---|---|
| Lakehouse vs Warehouse? | Lakehouse for Delta medallion + Direct Lake; Warehouse for cross-LH T-SQL joins |
| How did you handle skew? | Salting on station_id for Beijing/Delhi; AQE skewJoin for Silver transform |
| How does Direct Lake work? | Reads Delta Parquet from OneLake into VertiPaq — no import copy, no live SQL |
| What is V-Order? | Fabric's Parquet write optimization — mandatory for Direct Lake scan speeds |
| How did you implement SCD2? | Delta MERGE — MD5 hash detects changes, expire old row, insert new atomically |
| Have you used LLMs? | Claude Sonnet tool-use agent — NL to KQL/SQL, grounded answers from live data |

---

## 📄 License

MIT — open for portfolio and educational use.

---

*Built by [Jayanth Dolai](https://www.linkedin.com/in/jayanth-dolai-7b115213a/) · Senior Data Engineer · Microsoft Fabric · Apr–Jul 2026*
