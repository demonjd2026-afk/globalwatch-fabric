# GlobalWatch — Technical Specification

**Project:** World Air Quality Intelligence Platform
**Platform:** Microsoft Fabric
**Architecture:** Lambda (Batch + Real-Time)
**Version:** 1.0
**Author:** Jayanth Dolai
**Period:** Apr 2026 – Jul 2026

---

## 1. Problem Statement

Air quality data exists across dozens of fragmented public APIs — OpenAQ, WAQI, government portals — but no unified platform correlates real-time sensor readings with historical trends, weather patterns, and country-level health/economic indicators at global scale. GlobalWatch solves this by building a production-grade lakehouse on Microsoft Fabric that ingests, unifies, and serves this data through both operational dashboards and AI-powered natural language querying.

---

## 2. Architecture Decision Records (ADRs)

### ADR-001: Why Microsoft Fabric over standalone Azure services?

**Decision:** Use Fabric as the single platform instead of ADF + Databricks + Synapse + Power BI separately.

**Rationale:**
- OneLake eliminates data silos — all engines (Spark, SQL, KQL) read the same Delta files
- Fabric-native pipelines, Eventstream, and RTI are fully integrated — no custom connectors
- Direct Lake mode removes the Power BI import/refresh cycle entirely
- Single governance layer (Purview + sensitivity labels) across all workloads
- Cost: one F-SKU covers all workloads vs paying separately for each service

**Trade-off:** Fabric is newer — fewer StackOverflow answers, some features still in preview.

---

### ADR-002: Why Lambda architecture (batch + real-time) over Kappa?

**Decision:** Maintain separate batch (Delta medallion) and speed (KQL) layers.

**Rationale:**
- Batch layer: historical completeness, complex enrichment (World Bank, weather), ML scoring — all require multi-pass Spark processing not suitable for streaming
- Speed layer: operational dashboards need sub-second freshness for current AQI readings — Delta streaming latency (~minutes) is too slow
- Kappa (streaming only) would require complex stateful aggregations in streaming that are better expressed as Spark batch jobs

**Trade-off:** Two code paths to maintain. Mitigated by Fabric Eventstream handling the real-time path declaratively.

---

### ADR-003: Why KQL Database for real-time vs Delta streaming to Gold?

**Decision:** Real-time OpenAQ feed lands in KQL Database, not directly into Delta Gold tables.

**Rationale:**
- KQL is purpose-built for time-series — aggregations like `summarize avg(pm25_value) by bin(reading_ts, 1h)` run 10-100x faster than equivalent Spark SQL on Delta
- KQL update policies handle stream-time transformations (AQI categorization) without separate compute
- Data Activator integrates natively with KQL — no connector needed
- Delta Gold tables get batch-reconciled hourly from KQL via Spark Structured Streaming sink

**Trade-off:** Two query languages (KQL + SQL). KQL is straightforward to learn; covered by the AI agent's NL-to-KQL translation layer.

---

### ADR-004: Why Lakehouse for medallion + Warehouse for ad-hoc?

**Decision:** Gold layer primary serving via Lakehouse (Direct Lake); Warehouse for cross-domain analytical SQL.

**Rationale:**
- Lakehouse + Direct Lake: fastest Power BI query path — no data copy, no DirectQuery overhead
- Fabric Warehouse: needed when queries join Gold Lakehouse tables with Warehouse-native aggregation tables or require T-SQL features (window functions, CTEs across lakehouses)
- A Warehouse cannot write Delta natively — so it cannot be the medallion target

**Trade-off:** Analysts need to know which endpoint to use. Mitigated by the AI agent routing queries to the correct endpoint automatically.

---

### ADR-005: Why Claude (Anthropic) for AI agent vs Azure OpenAI?

**Decision:** Use Claude Sonnet via Anthropic API for the tool-use agent.

**Rationale:**
- Claude's tool-use (function calling) is more reliable for structured KQL/SQL generation
- No Azure OpenAI quota approval process needed
- Anthropic API is directly accessible from Streamlit Cloud
- Cost: Claude Sonnet is cheaper per token than GPT-4o for this workload

**Trade-off:** Not on Azure — can't use Azure Managed Identity auth. Mitigated by storing API key in Streamlit secrets.

---

## 3. Data Model

### 3.1 Gold Star Schema

```
                    ┌──────────────┐
                    │  dim_date    │
                    │─────────────│
                    │ date_key PK  │
                    │ full_date    │
                    │ year         │
                    │ month        │
                    │ quarter      │
                    │ day_of_week  │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────▼────────────────────┐    ┌──────────────────┐
│ dim_station  │    │      fact_readings         │    │  dim_pollutant   │
│─────────────│    │───────────────────────────│    │─────────────────│
│ station_sk PK│◄───│ station_sk FK             │───►│ pollutant_sk PK  │
│ station_id   │    │ country_sk FK             │    │ pollutant_code   │
│ station_name │    │ date_key FK               │    │ pollutant_name   │
│ city         │    │ pollutant_sk FK           │    │ unit             │
│ latitude     │    │ reading_ts                │    │ who_guideline    │
│ longitude    │    │ pm25_value                │    └──────────────────┘
│ active_flag  │    │ pm10_value                │
│ effective_start   │ no2_value                 │
│ effective_end│    │ co_value                  │
└──────────────┘    │ o3_value                  │    ┌──────────────────┐
                    │ aqi_score                 │    │  dim_country     │
┌──────────────┐    │ health_risk_label         │◄───│─────────────────│
│ dim_country  │◄───│ country_sk FK             │    │ country_sk PK    │
│─────────────│    │ temperature_c              │    │ country_code     │
│ country_sk PK│    │ humidity_pct              │    │ country_name     │
│ country_code │    │ wind_speed_ms             │    │ continent        │
│ country_name │    │ ingestion_ts              │    │ gdp_per_capita   │
│ continent    │    │ source_system             │    │ health_exp_pct   │
│ gdp_per_capita    └───────────────────────────┘    │ population       │
│ health_exp_pct                                     │ scd_version      │
│ population   │                                     └──────────────────┘
└──────────────┘
```

### 3.2 SCD Implementation

| Dimension | SCD Type | Strategy | Key columns |
|---|---|---|---|
| `dim_station` | Type 2 | Delta MERGE — expire old, insert new | `active_flag`, `effective_start`, `effective_end`, `station_hash` |
| `dim_country` | Type 1 | Delta MERGE — overwrite current values | `gdp_per_capita`, `health_exp_pct`, `population` |
| `dim_pollutant` | Type 0 | Static — no changes expected | — |
| `dim_date` | Type 0 | Pre-generated spine 2015–2030 | — |

### 3.3 KQL Database Schema (Real-Time Layer)

```kql
-- Raw landing table (Eventstream destination)
.create table raw_readings (
    station_id: string,
    country_code: string,
    city: string,
    latitude: real,
    longitude: real,
    pm25_value: real,
    pm10_value: real,
    no2_value: real,
    co_value: real,
    o3_value: real,
    reading_ts: datetime,
    ingestion_ts: datetime,
    source: string
)

-- Transformed table (populated via update policy)
.create table silver_readings (
    station_id: string,
    country_code: string,
    city: string,
    pm25_value: real,
    aqi_category: string,
    reading_ts: datetime
)
```

---

## 4. Data Sources Specification

### 4.1 OpenAQ API (Real-Time)

| Property | Value |
|---|---|
| Endpoint | `https://api.openaq.org/v3/locations/{id}/measurements` |
| Auth | API key header (free tier) |
| Poll interval | Every 5 seconds via Eventstream custom endpoint |
| Format | JSON |
| Key fields | `location_id`, `parameter`, `value`, `unit`, `date.utc`, `coordinates` |
| Volume | ~2,000–5,000 readings/minute globally |
| Schema drift handling | `mergeSchema=True` on Bronze Delta writes |

### 4.2 WAQI API (Batch)

| Property | Value |
|---|---|
| Endpoint | `https://api.waqi.info/feed/{city}/?token={token}` |
| Auth | Free API token |
| Frequency | Daily batch at 02:00 AM IST |
| Format | JSON |
| Key fields | `aqi`, `dominentpol`, `iaqi.pm25`, `iaqi.pm10`, `time.iso` |
| Rate limit | 1,000 calls/day (free tier) |
| Watermark | `last_loaded_date` in `watermark_control` Delta table |

### 4.3 World Bank API (Annual Batch)

| Property | Value |
|---|---|
| Endpoint | `https://api.worldbank.org/v2/country/{code}/indicator/{indicator}` |
| Auth | None (fully public) |
| Frequency | Annual refresh |
| Indicators | `NY.GDP.PCAP.CD` (GDP per capita), `SH.XPD.CHEX.PC.CD` (health exp), `SP.POP.TOTL` (population) |
| Ingestion | Dataflow Gen2 HTTP connector |
| SCD | Type 1 on `dim_country` |

### 4.4 OpenMeteo API (Daily Batch)

| Property | Value |
|---|---|
| Endpoint | `https://api.open-meteo.com/v1/forecast` |
| Auth | None (fully public) |
| Frequency | Daily batch |
| Fields | `temperature_2m`, `relative_humidity_2m`, `wind_speed_10m` |
| Join key | Station latitude/longitude → nearest weather grid cell |
| Ingestion | Dataflow Gen2 HTTP connector |

---

## 5. PySpark Optimization Specifications

### 5.1 Cluster Configuration (F2 Capacity)

```python
# Starter pool — automatically provisioned by Fabric
# F2 = 2 CUs → ~4 vCores, 32GB RAM for Spark

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")
spark.conf.set("spark.sql.shuffle.partitions", "auto")  # AQE manages this
```

### 5.2 Partitioning Strategy

| Table | Partition columns | Rationale |
|---|---|---|
| `silver_globalwatch.readings` | `country_code`, `year_month` | Country-level queries are the dominant access pattern; year_month enables time range pruning |
| `gold_globalwatch.fact_readings` | `year_month` | Reporting queries always filter by time range |
| `bronze_globalwatch.*` | `ingestion_date` | Append-only; date partitioning enables efficient cleanup |

### 5.3 Z-Order Specification

```python
# Applied after each Gold load
spark.sql("""
OPTIMIZE gold_globalwatch.fact_readings
ZORDER BY (station_id, reading_ts)
""")
# Rationale: most queries filter on station_id + time range together
# Z-order co-locates data for these two columns — reduces files scanned by 60-80%

spark.sql("""
OPTIMIZE gold_globalwatch.dim_station
ZORDER BY (country_code, city)
""")
```

### 5.4 Broadcast Join Threshold

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")

# dim_country is ~250 rows (~15KB) — always broadcast
# dim_pollutant is 5 rows — always broadcast
# dim_station is ~10K rows (~2MB) — broadcast eligible
# fact_readings is billions of rows — never broadcast (build side)
```

### 5.5 Salting for Skewed Stations

```python
# Beijing, Delhi, Shanghai stations have 100x more rows than average
# Without salting: one executor handles all Beijing data → OOM / straggler

SALT_BUCKETS = 10

df_readings_salted = df_readings \
    .withColumn("salt", (F.rand() * SALT_BUCKETS).cast("int")) \
    .withColumn("station_id_salted",
                F.concat(F.col("station_id"), F.lit("_"), F.col("salt")))

df_station_exploded = df_station \
    .crossJoin(spark.range(SALT_BUCKETS).withColumnRenamed("id", "salt")) \
    .withColumn("station_id_salted",
                F.concat(F.col("station_id"), F.lit("_"), F.col("salt")))

df_joined = df_readings_salted.join(
    F.broadcast(df_station_exploded),
    on="station_id_salted",
    how="left"
).drop("salt", "station_id_salted")
```

---

## 6. Pipeline Specifications

### 6.1 Master Orchestrator Pipeline (`pl_daily_orchestrator`)

```
Trigger: Daily at 02:00 AM IST
│
├── Activity 1: Notebook — 01_bronze_ingest_openaq (batch catch-up)
│   └── On success →
├── Activity 2: Notebook — 02_bronze_ingest_waqi_batch
│   └── On success →
├── Activity 3: Dataflow Gen2 — World Bank refresh (weekly gate)
│   └── On success →
├── Activity 4: Notebook — 04_silver_transform
│   └── On success →
├── Activity 5: Notebook — 05_gold_star_schema
│   └── On success →
├── Activity 6: Script — OPTIMIZE + VACUUM on Gold tables
│   └── On success →
└── Activity 7: Script — Update watermark_control table
```

### 6.2 Pipeline Error Handling

- Each activity has **retry: 2, retry interval: 5 min**
- On failure: sends Teams notification via webhook
- Failed run details logged to `bronze_globalwatch.pipeline_run_log` Delta table

---

## 7. Security Specification

### 7.1 Workspace RBAC

| Role | Who | Permissions |
|---|---|---|
| Admin | `jayanthfabric@...` | Full control |
| Contributor | CI/CD service principal | Read/write items, no workspace settings |
| Viewer | Demo users | Read reports only |

### 7.2 Row-Level Security (Power BI)

```
Security table: gold_globalwatch.user_continent_map
Columns: user_email, continent

RLS filter on dim_country:
[continent] IN VALUES(user_continent_map[continent])
WHERE user_continent_map[user_email] = USERPRINCIPALNAME()

Effect:
- Asia analyst → sees only Asian country data
- Europe analyst → sees only European country data
- Admin → sees all (excluded from RLS role)
```

### 7.3 Sensitivity Labels

| Asset | Label |
|---|---|
| All Gold tables | `General — Public Data` (air quality is public) |
| `user_continent_map` | `Confidential — Internal` |
| Pipeline connection strings | `Highly Confidential` |
| AI agent `.env` file | Never committed — in `.gitignore` |

---

## 8. AI Agent Specification

### 8.1 Tool Definitions

#### Tool 1: `query_kql`
- **Trigger:** Questions about current/recent air quality (last 24h, live data)
- **Flow:** NL → Claude generates KQL → execute against KQL DB endpoint → return results
- **Output:** Formatted table + plain-language summary

#### Tool 2: `query_gold_sql`
- **Trigger:** Questions about historical trends, comparisons, aggregations
- **Flow:** NL → Claude generates T-SQL → execute against Gold Lakehouse SQL endpoint → return results
- **Output:** Formatted table + trend interpretation

#### Tool 3: `get_country_health_context`
- **Trigger:** Questions correlating air quality with economic/health indicators
- **Flow:** country_code → query `dim_country` → return GDP, health spend, population
- **Output:** Context block injected into agent's next response

### 8.2 System Prompt

```
You are GlobalWatch AI, an expert air quality analyst with access to:
1. Live sensor data from 10,000+ stations across 90 countries (KQL Database)
2. Historical aggregated air quality data 2015-present (Gold Lakehouse SQL)
3. Country-level health and economic context (World Bank data)

Always:
- State the time range of data used in your answer
- Flag if PM2.5 > 35.4 µg/m³ (WHO annual guideline exceeded)
- Suggest the most relevant tool before querying
- If a query returns no results, explain why and suggest alternatives

Never:
- Fabricate data — only use tool results
- Make medical recommendations
- Query data older than 2015 (not in the dataset)
```

---

## 9. Cost Estimation

### 9.1 F2 Capacity (Recommended)

| Component | Unit cost | Estimated usage | Monthly cost |
|---|---|---|---|
| F2 Capacity | $0.36/hr | 4 hrs/day active, paused rest | ~$43/mo (~₹3,600) |
| OneLake storage | $0.023/GB | ~50GB total dataset | ~$1.15/mo |
| Eventstream | Included in F2 | — | ₹0 |
| Data Activator | Included in F2 | — | ₹0 |
| Streamlit Cloud | Free tier | — | ₹0 |
| OpenAQ / WAQI / World Bank / OpenMeteo APIs | Free | — | ₹0 |
| **Total** | | | **~₹3,600/mo** |

### 9.2 Cost Reduction Tips

- Pause F2 capacity immediately after each dev session
- Run VACUUM weekly (keeps storage lean)
- Use Starter pools (not custom Spark pools) — no idle compute cost
- Set auto-pause on capacity after 30 minutes of inactivity

---

## 10. Git Repository Structure

```
demonjd2026-afk/globalwatch-fabric/
│
├── README.md                          # Project overview + architecture
├── SETUP.md                           # Step-by-step setup guide with screenshots
├── TECH_SPEC.md                       # This document
│
├── notebooks/                         # PySpark notebooks (.ipynb)
├── pipelines/                         # FDF pipeline definitions (.json)
├── kql/                               # KQL scripts
├── semantic_model/                    # Power BI semantic model
├── powerbi/                           # Power BI report (.pbip)
├── ai_agent/                          # Streamlit + Claude agent
├── infra/                             # Infrastructure setup notes
├── data_quality/                      # DQ expectation scripts
├── tests/                             # PySpark unit tests
├── docs/                              # Additional documentation
├── screenshots/                       # All setup + output screenshots
│   └── .gitkeep
│
├── .gitignore                         # Excludes .env, __pycache__, .ipynb_checkpoints
└── .github/
    └── workflows/
        └── fabric_deploy.yml          # CI trigger on main branch merge
```

---

## 11. Definition of Done

A phase is complete when:

- [ ] Code committed and pushed to `main` branch
- [ ] Screenshot taken and added to `screenshots/` folder
- [ ] SETUP.md checkbox marked ✅ for all steps in that phase
- [ ] No hardcoded credentials anywhere in committed code
- [ ] Notebook runs end-to-end without errors on a fresh cluster
- [ ] Row counts validated (source count = target count ± acceptable DQ threshold)

---

*Last updated: Aug 2026 · Jayanth Dolai · [LinkedIn](https://www.linkedin.com/in/jayanth-dolai-7b115213a/)*
