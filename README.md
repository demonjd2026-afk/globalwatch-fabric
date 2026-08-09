# 🌍 GlobalWatch — World Air Quality Intelligence Platform

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-Trial_Capacity-0078D4?style=flat&logo=microsoft)](https://app.fabric.microsoft.com)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?style=flat&logo=apachespark)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.2-00ADD8?style=flat)](https://delta.io)
[![OpenAQ](https://img.shields.io/badge/OpenAQ-v3_API-4CAF50?style=flat)](https://openaq.org)
[![Claude](https://img.shields.io/badge/AI_Agent-Claude_Sonnet_4.6-8B5CF6?style=flat)](https://anthropic.com)
[![Streamlit](https://img.shields.io/badge/Live_App-Streamlit-FF4B4B?style=flat&logo=streamlit)](https://globalwatch-fabric.streamlit.app/)
[![License](https://img.shields.io/badge/License-MIT-yellow?style=flat)](LICENSE)

> A Lambda-architecture air quality platform on Microsoft Fabric — real-time (Eventstream → KQL → Data Activator) and batch (Bronze → Silver → Gold medallion → ML) paths over a shared OneLake, served through Direct Lake Power BI with RLS and a public Streamlit dashboard with a Claude-powered analytics assistant.

**Live app:** <https://globalwatch-fabric.streamlit.app/>
**Repository:** <https://github.com/demonjd2026-afk/globalwatch-fabric>
**Full project report (PDF):** [`docs/GlobalWatch_Project_Report.pdf`](docs/GlobalWatch_Project_Report.pdf)
**Built by:** [Jayanth Dolai](https://www.linkedin.com/in/jayanth-dolai-7b115213a/) · Data Engineer
**Platform:** Microsoft Fabric (Trial capacity, Central India) · Power BI Trial

---

## 📌 What this project is

Air quality data is fragmented across public APIs with no unified platform that ties live sensor readings to historical trends, ML scoring, and alerting. GlobalWatch builds that platform end-to-end on Microsoft Fabric:

- **Ingest** live readings from the OpenAQ v3 API across 70 targeted country codes, in parallel, respecting the free-tier rate limit.
- **Process** them through a Bronze → Silver → Gold medallion lakehouse in PySpark with AQE, broadcast joins, partitioning, Z-Order, V-Order and Delta MERGE (SCD Type 2).
- **Score** PM2.5 readings with a Spark MLlib Random Forest tracked and registered in MLflow.
- **Stream** the same source into a Fabric Eventstream → KQL Eventhouse, where an update policy categorises AQI on arrival and Data Activator emails a hazard alert above 150 µg/m³.
- **Serve** through a Direct Lake Power BI semantic model with continent-level RLS, and a public Streamlit app whose data is pushed to GitHub by the pipeline itself.

Everything in this repository has been run — the [`screenshots/`](screenshots/) folder is the evidence trail, and the [Implementation status](#-implementation-status) table below is explicit about what is built versus what is designed but not deployed.

---

## 📊 Current dataset snapshot

Numbers below are the **actual contents of the Gold layer** as exported to [`streamlit/data/`](streamlit/data/) by `08_export_to_streamlit.ipynb` on **09 Aug 2026**. They change on every pipeline run — the Streamlit app always reflects the latest export.

| Metric | Value |
|---|---|
| Fact rows (`fact_readings`) | **894** |
| Distinct stations with readings | **303** (308 active rows in `dim_station`) |
| Countries | **23** across 6 continents |
| WHO guideline exceedances | **274** of 894 readings (30.6%) |
| ML predictions (`fact_aqi_predictions`) | **245** rows over 206 stations |
| Real-time events per hourly run | **5,056** (`pl_realtime_globalwatch`, status *Live*) |
| Reading date range | 2016-01-30 → 2026-08-09 |

**Pollutant mix:** PM2.5 245 · NO₂ 201 · PM10 189 · O₃ 168 · CO 91

**AQI category distribution** (PM2.5 only; other pollutants are `N/A` by design):

| Category | Readings |
|---|---|
| N/A (non-PM2.5 pollutants) | 649 |
| Good | 137 |
| Moderate | 72 |
| Unhealthy | 17 |
| Hazardous | 11 |
| Unhealthy for Sensitive | 8 |

**Readings by continent:** Europe 314 · South America 192 · Asia 181 · Other 89 · North America 67 · Africa 42 · Oceania 9

---

## 🔎 Findings from the current data

WHO 2021 annual guidelines: PM2.5 15 · PM10 45 · NO₂ 10 · CO 4000 · O₃ 60 µg/m³.

| Country | Pollutant | Avg | WHO limit | Exceedance | Readings | % over limit |
|---|---|---|---|---|---|---|
| India | PM2.5 | 173.24 | 15 | **11.5×** | 14 | 100% |
| Mongolia | PM2.5 | 131.40 | 15 | **8.8×** | 5 | 100% |
| India | NO₂ | 75.24 | 10 | **7.5×** | 17 | 100% |
| India | PM10 | 279.97 | 45 | **6.2×** | 9 | 100% |
| China | NO₂ | 61.83 | 10 | **6.2×** | 6 | 100% |
| Mongolia | PM10 | 230.62 | 45 | **5.1×** | 8 | 88% |
| Brazil | NO₂ | 47.73 | 10 | **4.8×** | 11 | 100% |
| China | PM2.5 | 61.00 | 15 | **4.1×** | 15 | 87% |
| Peru | PM10 | 131.34 | 45 | **2.9×** | 10 | 100% |
| United Kingdom | NO₂ | 15.99 | 10 | 1.6× | 37 | 57% |

**Most polluted stations by peak PM2.5:**

| Station | Country | Peak PM2.5 | AQI |
|---|---|---|---|
| Delhi Technological University | India | 300.0 | Hazardous |
| Income Tax Office, Delhi — CPCB | India | 219.0 | Hazardous |
| Bukhiin urguu | Mongolia | 207.0 | Hazardous |
| R K Puram, Delhi — DPCC | India | 195.0 | Hazardous |
| Chizhou Lao Gan Bu Ju | China | 175.0 | Hazardous |
| MNB | Mongolia | 172.0 | Hazardous |

Every Indian and Mongolian PM2.5 reading in the current snapshot breaches the WHO guideline. The cleanest countries in the set are Belgium (4.34), Hungary (6.99), the Netherlands (7.14) and the United Kingdom (7.55 µg/m³ average PM2.5).

---

## 🏗️ Architecture

### Lambda architecture on one OneLake

```
┌──────────────────────────── SOURCE ────────────────────────────┐
│              OpenAQ v3 API — /locations + /latest              │
│        70 target country codes · parallel fetch (10 workers)   │
└───────────────┬────────────────────────────┬───────────────────┘
                │ BATCH PATH                 │ SPEED PATH
                ▼                            ▼
   01_bronze_ingest_openaq.ipynb   07_streaming_openaq_eventstream.ipynb
   (watermark + mergeSchema)        (Azure Event Hub SDK producer)
                │                            │
                ▼                            ▼
   ┌────────────────────────┐     ┌──────────────────────────────┐
   │  bronze_globalwatch    │     │  es_openaq_realtime          │
   │  raw_openaq_readings   │     │  Eventstream custom endpoint │
   │  watermark_control     │     └──────────────┬───────────────┘
   └───────────┬────────────┘                    ▼
               │ 04_silver_transform    ┌────────────────────────────┐
               ▼                        │ globalwatch_eventhouse     │
   ┌────────────────────────┐           │  raw_readings   (30d)      │
   │  silver_globalwatch    │           │      │ update policy        │
   │  silver_readings       │           │      ▼ TransformRawReadings │
   │  4 DQ rules · AQI      │           │  silver_readings (365d)    │
   │  part: country+month   │           └──────────────┬─────────────┘
   └───────────┬────────────┘                          ▼
               │ 05_gold_star_schema        ┌──────────────────────┐
               ▼                            │ Data Activator       │
   ┌────────────────────────┐               │ pm25_hazard_alert    │
   │  gold_globalwatch      │               │ PM2.5 > 150 → email  │
   │  fact_readings         │               └──────────────────────┘
   │  dim_station (SCD2)    │
   │  dim_country (SCD1)    │
   │  dim_date · dim_pollutant
   │  fact_aqi_predictions  │◄── 06_ml_aqi_prediction (MLflow RF)
   └───────────┬────────────┘
               │
    ┌──────────┼────────────────────┐
    ▼          ▼                    ▼
 Direct    globalwatch_        08_export_to_streamlit
 Lake      warehouse           → GitHub JSONL
 Power BI  (cross-LH T-SQL)    → Streamlit Cloud app
 + RLS                            + Claude assistant
```

### Two paths, one storage layer

| Path | Orchestration | Cadence | Latency | Serves |
|---|---|---|---|---|
| **Batch** | `pl_batch_globalwatch` (5 chained notebooks) | Daily 02:00 IST | Hours | Power BI, ML scoring, Streamlit snapshot |
| **Real-time** | `pl_realtime_globalwatch` (`Stream_To_Eventstream`) | Hourly | Seconds | KQL queryset, Data Activator alerts, live event counter |

---

## 📂 Repository structure

```
globalwatch-fabric/
├── README.md                        ← you are here
├── SETUP.md                         ← reproducible step-by-step build guide
├── TECH_SPEC.md                     ← ADRs, schemas, cost model, security
│
├── notebooks/                       ← PySpark notebooks (Fabric)
│   ├── README.md                    ← cell-by-cell notebook guide
│   ├── 01_bronze_ingest_openaq.ipynb
│   ├── 04_silver_transform.ipynb
│   ├── 05_gold_star_schema.ipynb
│   ├── 06_ml_aqi_prediction.ipynb
│   ├── 07_streaming_openaq_eventstream.ipynb
│   ├── 07_data_agent_simulation.ipynb
│   └── 08_export_to_streamlit.ipynb
│
├── kql/                             ← Eventhouse DDL + dashboard queries
│   ├── schema_create.kql            ← tables, transform function, update policy, retention
│   └── queries_dashboard.kql        ← 7 KQL queries used by the RTI queryset
│
├── streamlit/                       ← public dashboard + AI assistant
│   ├── app.py                       ← Streamlit app (dashboard + agent pages)
│   ├── requirements.txt
│   ├── README.md
│   └── data/                        ← Gold snapshots, pushed by notebook 08
│       ├── fact_readings.json       ← JSONL, 894 rows
│       ├── dim_country.json         ← JSONL, 23 rows
│       ├── dim_station.json         ← JSONL, 308 active rows
│       ├── fact_aqi_predictions.json← JSONL, 245 rows
│       └── kql_stats.json           ← real-time run stats (JSON object)
│
├── docs/
│   ├── architecture.md              ← ADRs + medallion design
│   ├── data_model.md                ← star schema, SCD logic, surrogate keys
│   ├── spark_optimization.md        ← all 8 optimisation patterns with code
│   ├── interview_narratives.md      ← STAR stories per phase
│   └── GlobalWatch_Project_Report.pdf ← full project report (generated)
│
└── screenshots/                     ← 39 screenshots evidencing every phase
```

> **Note on pipelines:** `pl_batch_globalwatch` and `pl_realtime_globalwatch` are Fabric Data Factory items defined in the workspace. The trial tenant blocks Fabric Git integration (see [`screenshots/30_git_integration_limitation.png`](screenshots/30_git_integration_limitation.png)), so their JSON definitions are not exported here — pipeline configuration and successful runs are evidenced by screenshots 31–35.

---

## 🗃️ Data sources

| Source | Data | Method | Status |
|---|---|---|---|
| [OpenAQ v3](https://openaq.org) | PM2.5, PM10, NO₂, CO, O₃ from global reference + low-cost sensors | `/locations` then `/locations/{id}/latest`, 10-worker `ThreadPoolExecutor` with a semaphore holding to 60 req/min | ✅ Implemented (batch + streaming) |
| WAQI | Historical AQI by city | FDF watermark pipeline | 🔷 Designed — not deployed |
| World Bank | GDP per capita, health expenditure, population | Dataflow Gen2 HTTP connector → `dim_country` SCD1 columns | 🔷 Designed — `dim_country` columns reserved, not populated |
| OpenMeteo | Temperature, humidity, wind speed | Dataflow Gen2 HTTP connector | 🔷 Designed — not deployed |

The batch and streaming notebooks both target the same 70 ISO country codes spread across Asia (25), Europe (20), the Americas (15) and Africa (10); which of those actually return sensors on a given run determines the 23 countries currently in Gold.

---

## 🥉🥈🥇 Medallion design

### Bronze — raw landing zone
- Append-only Delta; raw payloads are never modified.
- `mergeSchema=true` absorbs OpenAQ schema drift instead of failing the write.
- Partitioned by `ingestion_date`.
- `watermark_control` Delta table tracks `last_loaded_date` / `last_loaded_ts` per source, so a failed run does not advance the watermark.

### Silver — cleaned and conformed
Four data quality rules, applied in order:

| # | Rule | Filter | Purpose |
|---|---|---|---|
| 1 | Empty parameter | `parameter != ""` | Drop unclassified sensors |
| 2 | Known pollutants | `parameter IN (pm25, pm10, no2, co, o3)` | Keep criteria pollutants only |
| 3 | Valid range | `0 < value < 10000` | Drop sensor malfunctions |
| 4 | Deduplication | `dropDuplicates(location_id, parameter, reading_ts)` | Remove repeated API records |

Then AQI categorisation (PM2.5 only), unit normalisation (`µg/m³` → `ug/m3`), reporting columns (`reading_date`, `reading_hour`, `year_month`, `is_recent`), and a write partitioned by `country_code` + `year_month`. V-Order stays off — Silver is intermediate, not a Direct Lake target.

### Gold — star schema serving layer

```
        dim_date (5,844)        dim_pollutant (5)
              │                        │
              └───────────┬────────────┘
                          │
                  fact_readings (894)
                          │
              ┌───────────┴───────────┐
              │                       │
      dim_station (308)         dim_country (23)
        SCD Type 2                SCD Type 1
              │
      fact_aqi_predictions (245)
```

| Table | SCD | Rows | Key feature |
|---|---|---|---|
| `fact_readings` | — | 894 | V-Order ON, Z-Order on `location_id` + `reading_ts`, partitioned by `year_month` |
| `dim_station` | Type 2 | 308 active | Delta MERGE — `active_flag`, `effective_start` / `effective_end`, MD5 `station_hash` |
| `dim_country` | Type 1 | 23 | Continent mapping; World Bank enrichment columns reserved |
| `dim_date` | Type 0 | 5,844 | Pre-generated spine 2015-01-01 → 2030-12-31 |
| `dim_pollutant` | Type 0 | 5 | WHO 2021 annual guideline values |
| `fact_aqi_predictions` | — | 245 | ML-scored AQI class per PM2.5 reading |

Surrogate keys are CRC32 hashes of the natural key — deterministic across runs, no distributed counter needed. Full detail in [`docs/data_model.md`](docs/data_model.md).

---

## ⚡ PySpark optimisation patterns

All eight patterns are implemented; full code and interview framing in [`docs/spark_optimization.md`](docs/spark_optimization.md).

| Optimisation | Where | Benefit |
|---|---|---|
| **AQE** (enabled, coalescePartitions, skewJoin) | All notebooks | Runtime re-planning; avoids 200 tiny shuffle files |
| **Broadcast join** | Gold fact build | Zero shuffle for `dim_country` (23), `dim_pollutant` (5), `dim_station` (308) |
| **Salting** | Silver skewed joins | Prevents straggler/OOM on high-volume city stations (Delhi, Beijing) |
| **Partitioning** | Silver (`country_code`+`year_month`), Gold (`year_month`) | Partition pruning on the dominant access pattern |
| **Z-Order** | `fact_readings` | Co-locates station+time; Data Skipping cuts files scanned |
| **V-Order** | All Gold tables | Mandatory for Direct Lake VertiPaq scan performance |
| **Delta MERGE** | `dim_station` SCD2 | Atomic expire + insert in one transaction |
| **mergeSchema** | Bronze writes | Absorbs OpenAQ API schema drift |

---

## 📡 Real-time intelligence layer

### KQL Eventhouse — update policy
`raw_readings` (30-day retention) receives the Eventstream feed. An update policy fires `TransformRawReadings()` on every ingestion and writes the enriched row into `silver_readings` (365-day retention) in the same transaction (`IsTransactional: true`):

```kql
.create-or-alter function TransformRawReadings() {
    raw_readings
    | where parameter in ("pm25", "pm10", "no2", "co", "o3")
    | where value >= 0 and value < 10000
    | extend aqi_category = case(
        parameter == "pm25" and value <= 12.0,  "Good",
        parameter == "pm25" and value <= 35.4,  "Moderate",
        parameter == "pm25" and value <= 55.4,  "Unhealthy for Sensitive",
        parameter == "pm25" and value <= 150.4, "Unhealthy",
        parameter == "pm25" and value >  150.4, "Hazardous",
        "N/A")
    | extend exceeds_who_guideline = case(
        parameter == "pm25" and value > 15.0,   true,
        parameter == "pm10" and value > 45.0,   true,
        parameter == "no2"  and value > 10.0,   true,
        parameter == "co"   and value > 4000.0, true,
        parameter == "o3"   and value > 60.0,   true,
        false)
    | project location_id, location_name, city, country_code, country_name,
              latitude, longitude, parameter, value, unit,
              aqi_category, exceeds_who_guideline,
              reading_ts, ingestion_ts, source_system
}
```

Full DDL — tables, function, update policy, retention policies, backfill — in [`kql/schema_create.kql`](kql/schema_create.kql). Seven dashboard queries (world map, top 10 polluted, exceedance rate, category distribution, hourly trend, hazard check, ingestion rate) in [`kql/queries_dashboard.kql`](kql/queries_dashboard.kql).

### Data Activator — PM2.5 hazard alert
- **Rule:** `pm25_hazard_alert` on the `pm25_station` object from the `es_openaq_realtime` stream.
- **Condition:** PM2.5 > 150 µg/m³ (WHO *Hazardous*).
- **Action:** Email with station name, city, country and current reading.
- **Status:** Running — 5 of 98 monitored station IDs actively triggering; alert email confirmed received 09 Aug 2026, 06:08 UTC.

---

## 🤖 Streamlit app and AI assistant

**Live:** <https://globalwatch-fabric.streamlit.app/>

**Page 1 — Dashboard.** Six KPI tiles (readings, stations, countries, WHO exceedances, hazardous, real-time events per run), a global PM2.5 bubble map (`Scattermap`, carto-darkmatter), average PM2.5 by country with a WHO reference line, AQI category donut, ML-predicted class distribution, and a top-10 most-polluted-stations table.

**Page 2 — AI Agent.** A Claude-powered assistant over the same data.

```
User question
      │
      ▼
Streamlit builds a grounded DATA_CONTEXT system prompt from the
loaded Gold snapshot — country PM2.5 averages, WHO exceedance counts,
AQI distribution, ML prediction distribution, top polluted stations,
and the platform architecture description
      │
      ▼
Anthropic Messages API · model claude-sonnet-4-6 · max_tokens 1000
      │
      ▼
Grounded natural-language answer citing the real numbers
```

Implementation notes, stated plainly:

- The assistant is a **single grounded API call per turn**, not a multi-tool agent: the app pre-computes the aggregates and injects them as the system prompt, so answers are grounded in the actual Gold snapshot and cannot query arbitrary tables. The three-tool design (`query_kql`, `query_gold_sql`, `get_country_health_context`) is specified in [`TECH_SPEC.md`](TECH_SPEC.md#8-ai-agent-specification) as the target architecture for a deployment with live endpoint access.
- The app reads its data from the **GitHub raw JSONL snapshots** in `streamlit/data/`, cached for 30 minutes — it does not hold a live connection to Fabric. Freshness therefore tracks the pipeline: the batch run refreshes the four Gold exports, the hourly real-time run refreshes `kql_stats.json`.
- `ANTHROPIC_API_KEY` is read from Streamlit secrets. No key is committed.

**Try asking:** *"Which country has the worst air quality?"* · *"How many stations exceed WHO guidelines?"* · *"Compare PM2.5 across Asian countries"* · *"Explain the Fabric pipeline architecture"* · *"What is a KQL update policy?"*

---

## 🧠 Machine learning

`06_ml_aqi_prediction.ipynb` — Spark MLlib + MLflow:

1. Read `silver_readings`, pivot long → wide so each pollutant becomes a feature column (`pm25`, `pm10`, `no2`, `o3`, `co`), null-filled with 0.
2. Label from WHO PM2.5 thresholds (0 = Good … 4 = Hazardous).
3. `VectorAssembler` → `RandomForestClassifier` in a Spark ML `Pipeline`; 80/20 split, `seed=42`.
4. MLflow logs params (`num_trees=100`, `max_depth=5`) and metrics, and registers `globalwatch_aqi_classifier v1`.
5. The registered model is loaded back, applied to Gold `fact_readings`, and written to `fact_aqi_predictions`.

**Results:** 96.15% test accuracy. Feature importance — PM2.5 76.32%, PM10 12.91%, remainder split across NO₂/O₃/CO. Current prediction distribution: Good 130, Moderate 87, Unhealthy 20, Hazardous 8.

Random Forest was chosen for class-imbalance tolerance on a small dataset, no feature scaling requirement across very different pollutant ranges, native Gini feature importance, and availability in Spark MLlib without extra packages.

---

## 🛠️ Tech stack

| Layer | Technology |
|---|---|
| Platform | Microsoft Fabric (Trial capacity, Central India) |
| Storage | OneLake, Delta Lake, Parquet with V-Order |
| Batch ingestion | Fabric Data Factory pipelines, PySpark notebooks |
| Real-time ingestion | Fabric Eventstream custom endpoint + Azure Event Hubs SDK |
| Compute | Fabric Spark (PySpark 3.5), `globalwatch-env` environment |
| Real-time analytics | KQL Eventhouse, update policies, KQL Queryset |
| Alerting | Fabric Data Activator → email |
| ML | Spark MLlib Random Forest + MLflow tracking & registry |
| Serving | Direct Lake Power BI semantic model + RLS, Fabric Warehouse |
| Public app | Streamlit Cloud, Plotly, Anthropic Messages API (Claude Sonnet 4.6) |
| Secrets | Fabric environment Spark properties, Streamlit secrets |

---

## 🚀 Quick start

```bash
# 1. Clone
git clone https://github.com/demonjd2026-afk/globalwatch-fabric.git
cd globalwatch-fabric

# 2. Run the Streamlit app locally (reads published snapshots from GitHub)
cd streamlit
pip install -r requirements.txt
streamlit run app.py
```

The AI Agent page needs an Anthropic API key. Create `streamlit/.streamlit/secrets.toml` (git-ignored):

```toml
ANTHROPIC_API_KEY = "sk-ant-..."
```

To rebuild the Fabric side from scratch — workspace, three lakehouses, warehouse, eventhouse, eventstream, environment, notebooks, pipelines, semantic model and alerts — follow [`SETUP.md`](SETUP.md). You will need a free [OpenAQ API key](https://explore.openaq.org/register).

---

## ✅ Implementation status

| Capability | Status | Evidence |
|---|---|---|
| Fabric workspace, 3 lakehouses, warehouse | ✅ Built | 02, 03, 04 |
| Bronze ingestion + watermark + mergeSchema | ✅ Built | 10 |
| Silver DQ + AQI categorisation | ✅ Built | 11, 12 |
| Gold star schema + SCD2 + V-Order + Z-Order | ✅ Built | 13, 14, 35 |
| KQL Eventhouse + update policy + retention | ✅ Built | 16 |
| Eventstream custom endpoint → KQL | ✅ Built | 17, 18, 33 |
| Data Activator hazard alert (email fired) | ✅ Built | 20, 21 |
| MLflow experiment + registered model + scoring | ✅ Built | 26, 27 |
| Batch pipeline (5 activities, daily 02:00 IST) | ✅ Built | 31, 34 |
| Real-time pipeline (hourly) | ✅ Built | 32 (scheduled + success) |
| Direct Lake semantic model + relationships | ✅ Built | 22 |
| Row-level security by continent | ✅ Built | 23 |
| Power BI report (2 pages) | ✅ Built | 24, 36 |
| Streamlit dashboard + Claude assistant | ✅ Built & public | 37, 38 |
| Fabric Data Agent (native) | ⚠️ Blocked — requires F64+ SKU; NL→SQL pattern simulated instead | 28, 29 |
| Fabric Git integration / deployment pipelines | ⚠️ Blocked — trial tenant account type | 30 |
| WAQI / World Bank / OpenMeteo enrichment | 🔷 Designed, not deployed | — |
| Tool-use AI agent against live KQL/SQL endpoints | 🔷 Designed, not deployed | — |

---

## 📸 Screenshots

All 39 screenshots live in [`screenshots/`](screenshots/).

**Foundation**

| What | File |
|---|---|
| Fabric home, trial active | [`01_fabric_home.png`](screenshots/01_fabric_home.png) |
| `globalwatch-dev` workspace | [`02_workspace_created.png`](screenshots/02_workspace_created.png) |
| Three medallion lakehouses | [`03_three_lakehouses.png`](screenshots/03_three_lakehouses.png) |
| Fabric Warehouse | [`04_warehouse_created.png`](screenshots/04_warehouse_created.png) |

**Medallion pipeline**

| What | File |
|---|---|
| Bronze notebook run | [`10_bronze_notebook_run.png`](screenshots/10_bronze_notebook_run.png) |
| Silver validation — zero nulls | [`11_silver_transform_run.png`](screenshots/11_silver_transform_run.png) |
| Spark UI — AQE, 17 jobs succeeded | [`12_spark_ui_aqe.png`](screenshots/12_spark_ui_aqe.png) |
| Gold validation — SCD2 + WHO exceedances | [`13_gold_notebook_scd2_merge.png`](screenshots/13_gold_notebook_scd2_merge.png) |
| Delta table detail — OneLake path | [`14_gold_delta_table_detail.png`](screenshots/14_gold_delta_table_detail.png) |
| Gold table counts after pipeline run | [`35_gold_table_counts_post_pipeline.png`](screenshots/35_gold_table_counts_post_pipeline.png) |

**Real-time intelligence**

| What | File |
|---|---|
| KQL database — `raw_readings` + `silver_readings` | [`16_kql_database_created.png`](screenshots/16_kql_database_created.png) |
| Eventstream configured → KQL destination Live | [`17_eventstream_configured.png`](screenshots/17_eventstream_configured.png) |
| Eventstream live data, multi-country | [`18_eventstream_live_data.png`](screenshots/18_eventstream_live_data.png) |
| KQL `raw_readings` count after run | [`33_kql_raw_readings_count.png`](screenshots/33_kql_raw_readings_count.png) |
| Data Activator rule Running | [`20_activator_rule_configured.png`](screenshots/20_activator_rule_configured.png) |
| Hazard alert email received | [`21_activator_alert_fired.png`](screenshots/21_activator_alert_fired.png) |

**ML and AI**

| What | File |
|---|---|
| MLflow run — 96.15% accuracy, registered model | [`26_mlflow_experiment_run.png`](screenshots/26_mlflow_experiment_run.png) |
| Predictions written to Gold | [`27_ml_predictions_output.png`](screenshots/27_ml_predictions_output.png) |
| ML predicted AQI classes | [`25_ml_aqi_predicted_classes.png`](screenshots/25_ml_aqi_predicted_classes.png) |
| Data Agent — NL→SQL pattern | [`28_data_agent_nl_to_sql.png`](screenshots/28_data_agent_nl_to_sql.png) |
| Data Agent — query results | [`29_data_agent_query_results.png`](screenshots/29_data_agent_query_results.png) |

**Orchestration**

| What | File |
|---|---|
| Batch pipeline scheduled, 5 notebooks chained | [`31_batch_pipeline_scheduled.png`](screenshots/31_batch_pipeline_scheduled.png) |
| Batch pipeline — all activities succeeded | [`34_batch_pipeline_success.png`](screenshots/34_batch_pipeline_success.png) |
| Real-time pipeline scheduled hourly | [`32_realtime_pipeline_scheduled.png`](screenshots/32_realtime_pipeline_scheduled.png) |
| Real-time pipeline succeeded in 1m 44s | [`32_realtime_pipeline_success.png`](screenshots/32_realtime_pipeline_success.png) |
| Git integration limitation on trial tenant | [`30_git_integration_limitation.png`](screenshots/30_git_integration_limitation.png) |

**Serving**

| What | File |
|---|---|
| Semantic model — 4 active relationships | [`22_semantic_model_relationships.png`](screenshots/22_semantic_model_relationships.png) |
| RLS — ContinentViewer role applied | [`23_rls_continent_viewer_role.png`](screenshots/23_rls_continent_viewer_role.png) |
| Power BI page 1 | [`24_powerbi_report_page1.png`](screenshots/24_powerbi_report_page1.png) |
| Power BI page 2 — readings by date | [`36_powerbi_page2_fixed.png`](screenshots/36_powerbi_page2_fixed.png) |
| Streamlit dashboard | [`37_streamlit_dashboard.png`](screenshots/37_streamlit_dashboard.png) |
| Streamlit AI agent | [`38_streamlit_ai_agent.png`](screenshots/38_streamlit_ai_agent.png) |
| Global bubble map | [`23_global_bubble_map.png`](screenshots/23_global_bubble_map.png) |
| PM2.5 by country chart | [`24_pm25_by_country_chart.png`](screenshots/24_pm25_by_country_chart.png) |
| Top 10 polluted stations | [`26_top10_polluted_stations.png`](screenshots/26_top10_polluted_stations.png) |
| AI agent — WHO exceedances answer | [`28_ai_agent_who_exceedances.png`](screenshots/28_ai_agent_who_exceedances.png) |
| AI agent — Asian PM2.5 comparison | [`29_ai_agent_pm25_asian_countries.png`](screenshots/29_ai_agent_pm25_asian_countries.png) |

---

## 📋 Interview talking points

Full STAR stories in [`docs/interview_narratives.md`](docs/interview_narratives.md).

| Question | Short answer |
|---|---|
| Lakehouse vs Warehouse? | Lakehouse for the Delta medallion and Direct Lake; Warehouse for cross-lakehouse T-SQL. A Warehouse can't write Delta, so it can't be the medallion target. |
| How did you handle skew? | Salting on station ID for high-volume city stations, plus AQE `skewJoin` for the Silver transform. |
| How does Direct Lake work? | Reads Delta Parquet from OneLake straight into VertiPaq — no import copy, no live DirectQuery. V-Order is the enabler. |
| What is a KQL update policy? | A function attached to a destination table that fires on ingestion into the source table; with `IsTransactional=true`, raw and silver can never diverge. |
| How did you implement SCD2? | Delta MERGE with an MD5 `station_hash` — expire the changed row, insert the new one, atomically. |
| Have you used MLflow? | Yes — logged params/metrics, registered `globalwatch_aqi_classifier v1`, then loaded the registered model to score Gold. |
| Have you used LLMs? | Claude Sonnet 4.6 via the Anthropic Messages API, grounded on pre-computed Gold aggregates so answers cite real numbers rather than hallucinating. |
| What went wrong? | `%pip` is disabled in pipeline runs (moved the library into the Fabric environment) and KQL outbound calls are blocked in the trial pipeline sandbox (verification cell made interactive-only). |

---

## 📄 License

MIT — open for portfolio and educational use.

---

*Built by [Jayanth Dolai](https://www.linkedin.com/in/jayanth-dolai-7b115213a/) · Microsoft Fabric · Data snapshot 09 Aug 2026*
