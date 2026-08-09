# GlobalWatch — Technical Specification

**Project:** World Air Quality Intelligence Platform
**Platform:** Microsoft Fabric
**Architecture:** Lambda (batch + real-time over one OneLake)
**Version:** 2.0
**Author:** Jayanth Dolai
**Last updated:** 09 Aug 2026 · data snapshot of the same date

---

## 1. Problem statement

Air quality data is published by dozens of independent networks through incompatible APIs. There is no readily available platform that unifies live sensor readings with historical trends, applies consistent WHO-based classification, scores readings with a model, alerts on hazardous conditions, and exposes all of it to both BI users and natural-language users.

GlobalWatch builds that platform on Microsoft Fabric with two paths over shared OneLake storage: a batch medallion path for historical analytics and ML, and a speed path for sub-minute operational awareness and alerting.

---

## 2. Implementation status

This spec distinguishes what is **built and evidenced** from what is **specified but not deployed**. Anything marked 🔷 is design intent, not a claim of delivery.

| Component | Status |
|---|---|
| Bronze / Silver / Gold medallion in PySpark | ✅ Built |
| SCD Type 2 `dim_station` via Delta MERGE | ✅ Built |
| V-Order + Z-Order on Gold | ✅ Built |
| KQL Eventhouse, update policy, retention policies | ✅ Built |
| Eventstream custom endpoint → KQL | ✅ Built |
| Data Activator PM2.5 hazard alert (email fired) | ✅ Built |
| Spark MLlib Random Forest + MLflow registry + scoring | ✅ Built |
| Batch + real-time Data Factory pipelines | ✅ Built |
| Direct Lake semantic model + continent RLS + report | ✅ Built |
| Streamlit dashboard + grounded Claude assistant | ✅ Built (public) |
| Native Fabric Data Agent | ⚠️ Blocked — requires F64+ SKU; NL→SQL pattern simulated |
| Fabric Git integration + deployment pipelines | ⚠️ Blocked — trial tenant account type |
| WAQI, World Bank, OpenMeteo enrichment | 🔷 Specified, not deployed |
| Tool-use agent against live KQL/SQL endpoints | 🔷 Specified, not deployed |

---

## 3. Architecture decision records

### ADR-001 — Microsoft Fabric over assembled Azure services

**Decision:** One platform rather than ADF + Databricks + Synapse + Power BI.

**Rationale:** OneLake removes copy-based silos — Spark, T-SQL and KQL engines read the same Delta files. Eventstream, Eventhouse, Data Activator and deployment pipelines are first-party, so there are no custom connectors to maintain. Direct Lake removes the Power BI import/refresh cycle entirely. Governance (workspace RBAC, sensitivity labels, Purview) is applied once across all workloads. Cost is one capacity rather than several independently-scaled services.

**Trade-off:** Fabric is newer — fewer community answers, and some features are gated behind SKU size (the Data Agent limitation in §2 is exactly this).

---

### ADR-002 — Lambda over Kappa

**Decision:** Keep a batch Delta medallion and a separate KQL speed layer.

**Rationale:** The batch layer needs multi-pass processing that streaming does not suit — SCD2 dimension maintenance, star schema joins, ML training and scoring. The speed layer needs seconds-level freshness for alerting, which Delta streaming latency cannot reach. Expressing SCD2 and model scoring as streaming state would be substantially more complex than the two-path split.

**Trade-off:** Two code paths. Mitigated by Eventstream and KQL update policies handling the speed path declaratively — there is no bespoke streaming job to maintain, only a producer notebook.

---

### ADR-003 — KQL Eventhouse for the speed layer

**Decision:** The real-time feed lands in a KQL database, not directly in Delta Gold.

**Rationale:** KQL is purpose-built for time-series — `summarize avg(value) by bin(reading_ts, 1h)` over ingestion-ordered data is dramatically cheaper than the equivalent Spark SQL over Delta. Update policies apply stream-time transformation with no separate compute. Data Activator binds natively to KQL/Eventstream with no connector.

**Trade-off:** Two query languages. Accepted: KQL is small to learn, and the queries the platform actually needs are captured in `kql/queries_dashboard.kql`.

---

### ADR-004 — Lakehouse for the medallion, Warehouse for cross-domain SQL

**Decision:** Gold serves primarily from a Lakehouse via Direct Lake; the Warehouse exists for T-SQL work across items.

**Rationale:** Direct Lake over a Lakehouse is the fastest Power BI path — no copy, no DirectQuery round trip. A Fabric Warehouse cannot write Delta, so it structurally cannot be the medallion target. It earns its place for ad-hoc T-SQL that spans lakehouses or needs warehouse-native constructs.

**Trade-off:** Consumers must know which endpoint to use. Mitigated by documenting the split and defaulting all reporting to the semantic model.

---

### ADR-005 — Claude for the natural language layer

**Decision:** Anthropic's Claude via the Messages API, called from Streamlit.

**Rationale:** The native Fabric Data Agent needs F64+, which this capacity does not have. Claude is reachable directly from Streamlit Cloud with no quota-approval process, and grounding it on pre-computed Gold aggregates keeps answers verifiable against the published data.

**Trade-off:** Outside Azure, so no Managed Identity — the key lives in Streamlit secrets. Model in use: `claude-sonnet-4-6`.

---

### ADR-006 — Publish Gold snapshots to GitHub instead of exposing an endpoint

**Decision:** The pipeline writes JSONL extracts of Gold to this repository; the Streamlit app reads them over `raw.githubusercontent.com` with a 30-minute cache.

**Rationale:** It makes the public app free to host, removes any inbound network path into the Fabric workspace, keeps no credentials in the app beyond the LLM key, and gives every published number a versioned, auditable commit history.

**Trade-off:** The app shows the last exported snapshot, not live Gold. Freshness is bounded by the pipeline cadence — daily for the four Gold extracts, hourly for `kql_stats.json`.

---

## 4. Data model

### 4.1 Gold star schema (as built)

`fact_readings` is a **long/tall** fact: one row per station × pollutant × reading timestamp, with `parameter` and `value` columns. It is deliberately not pivoted into `pm25_value` / `pm10_value` columns — the long shape means adding a pollutant needs no schema change, and `dim_pollutant` can carry the WHO guideline per parameter.

```
                    ┌──────────────────┐
                    │    dim_date      │  Type 0 · 5,844 rows
                    │──────────────────│
                    │ date_key    PK   │
                    │ full_date        │
                    │ year, month      │
                    │ quarter          │
                    │ day_of_week      │
                    │ is_weekend       │
                    │ year_month       │
                    └────────┬─────────┘
                             │ date_key
┌──────────────────┐  ┌──────▼───────────────────────┐  ┌──────────────────┐
│   dim_station    │  │        fact_readings          │  │  dim_pollutant   │
│──────────────────│  │───────────────────────────────│  │──────────────────│
│ station_sk   PK  │◄─│ station_sk          FK        │─►│ pollutant_sk PK  │
│ location_id      │  │ country_sk          FK        │  │ pollutant_code   │
│ location_name    │  │ pollutant_sk        FK        │  │ pollutant_name   │
│ city             │  │ date_key            FK        │  │ standard_unit    │
│ country_code     │  │ location_id                   │  │ who_guideline    │
│ latitude         │  │ parameter                     │  │ description      │
│ longitude        │  │ value                         │  └──────────────────┘
│ station_hash     │  │ unit                          │
│ active_flag      │  │ aqi_category                  │  ┌──────────────────┐
│ effective_start  │  │ exceeds_who_guideline         │  │   dim_country    │
│ effective_end    │  │ reading_ts, reading_date      │◄─│──────────────────│
└──────────────────┘  │ reading_hour, year_month      │  │ country_sk   PK  │
   SCD Type 2 · 308   │ is_recent                     │  │ country_code     │
                      │ latitude, longitude           │  │ country_name     │
┌──────────────────┐  │ continent                     │  │ continent        │
│fact_aqi_predictions │ source_system, ingestion_ts   │  │ gdp_per_capita 🔷│
│──────────────────│  └───────────────────────────────┘  │ health_exp_pct 🔷│
│ location_id      │        894 rows · V-Order          │  │ population    🔷│
│ country_sk       │        Z-Order (location_id,       │  │ scd_updated_ts   │
│ pollutant_sk     │                 reading_ts)        │  └──────────────────┘
│ date_key         │        partition: year_month       │    SCD Type 1 · 23
│ pm25             │
│ prediction       │  245 rows · RF model output
│ predicted_aqi_class │
└──────────────────┘
```

🔷 = column reserved for World Bank enrichment, currently unpopulated.

### 4.2 SCD strategy

| Dimension | Type | Mechanism | Why |
|---|---|---|---|
| `dim_station` | 2 | Delta MERGE — expire on `station_hash` change, then insert | Stations are renamed and reassigned between districts; regulatory reporting needs point-in-time station identity |
| `dim_country` | 1 | Overwrite | Country economic indicators are reference data; analysts want current values, and historical GDP series come from the World Bank directly |
| `dim_pollutant` | 0 | Static | WHO guidelines change rarely; a full overwrite is appropriate when they do |
| `dim_date` | 0 | Pre-generated spine | Power BI time intelligence requires a complete, gap-free calendar |

SCD2 MERGE logic:

```
Step 1 — expire changed rows
  MATCH  target.location_id = source.location_id
     AND target.active_flag = true
     AND target.station_hash <> source.station_hash
  SET    active_flag = false, effective_end = current_timestamp()

Step 2 — insert new versions
  Anti-join source against the active target set
  INSERT with active_flag = true, effective_end = 9999-12-31
```

### 4.3 Surrogate keys

CRC32 of the natural key, cast to integer:

```python
F.crc32(F.col("country_code")).cast(IntegerType())
F.crc32(F.col("location_id").cast("string")).cast(IntegerType())
```

Deterministic across runs and executors, so re-running a load produces identical keys with no distributed counter and no coordination overhead. Collision risk is acceptable at dimension cardinality.

### 4.4 KQL schema (real-time layer, as built)

The KQL tables mirror the Silver row shape rather than the star schema — the speed layer answers "what is happening now", not "how does this join to a conformed dimension".

```kql
.create table raw_readings (
    location_id: int, location_name: string, city: string,
    country_code: string, country_name: string,
    latitude: real, longitude: real,
    parameter: string, value: real, unit: string,
    reading_ts: datetime, ingestion_ts: datetime, source_system: string
)

.create table silver_readings (
    location_id: int, location_name: string, city: string,
    country_code: string, country_name: string,
    latitude: real, longitude: real,
    parameter: string, value: real, unit: string,
    aqi_category: string, exceeds_who_guideline: bool,
    reading_ts: datetime, ingestion_ts: datetime, source_system: string
)
```

`silver_readings` is populated exclusively by the update policy on `raw_readings`, running `TransformRawReadings()` transactionally. Retention: raw 30 days, silver 365 days. Full DDL in [`kql/schema_create.kql`](kql/schema_create.kql).

### 4.5 Published snapshot contract

`08_export_to_streamlit.ipynb` writes newline-delimited JSON (one object per line) to `streamlit/data/`. The app parses it line-by-line, not with a whole-file `json.load`.

| File | Columns |
|---|---|
| `fact_readings.json` | `location_id, parameter, value, unit, aqi_category, exceeds_who_guideline, reading_date, continent, country_sk` |
| `dim_country.json` | `country_sk, country_code, country_name, continent` |
| `dim_station.json` | `station_sk, location_id, location_name, city, country_code, latitude, longitude` (active rows only) |
| `fact_aqi_predictions.json` | `location_id, country_sk, pm25, predicted_aqi_class` |
| `kql_stats.json` | `events_sent_this_run, skipped, errors, exported_at, pipeline, frequency, status` — a single JSON object, not JSONL |

---

## 5. Data sources

### 5.1 OpenAQ v3 — implemented

| Property | Value |
|---|---|
| Endpoints | `GET /v3/locations` (discovery), `GET /v3/locations/{id}/latest` (readings) |
| Auth | `X-API-Key` header, free tier |
| Rate limit | 60 requests/minute — enforced client-side with a semaphore |
| Concurrency | `ThreadPoolExecutor(max_workers=10)` |
| Country scope | 70 ISO codes — Asia 25, Europe 20, Americas 15, Africa 10 |
| Batch cadence | Daily via `pl_batch_globalwatch` |
| Stream cadence | Hourly via `pl_realtime_globalwatch` |
| Schema drift | `mergeSchema=true` on the Bronze write |

Using `/latest` rather than per-sensor measurement history is a deliberate cost choice: one call per location instead of one per sensor, which is what makes 70-country coverage feasible inside the free tier.

### 5.2 WAQI, World Bank, OpenMeteo — specified, not deployed 🔷

| Source | Endpoint | Intended use |
|---|---|---|
| WAQI | `https://api.waqi.info/feed/{city}/?token=` | Historical AQI by city, daily watermark pipeline, 1,000 calls/day free |
| World Bank | `https://api.worldbank.org/v2/country/{code}/indicator/{ind}` | `NY.GDP.PCAP.CD`, `SH.XPD.CHEX.PC.CD`, `SP.POP.TOTL` → `dim_country` SCD1 columns |
| OpenMeteo | `https://api.open-meteo.com/v1/forecast` | `temperature_2m`, `relative_humidity_2m`, `wind_speed_10m` joined on nearest grid cell to station lat/long |

The `dim_country` schema already carries the placeholder columns so adding the Dataflow Gen2 ingestion is additive rather than a breaking change.

---

## 6. Spark specification

### 6.1 Session configuration

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
```

Fabric Starter Pools are used throughout — no custom pool, so there is no idle compute charge.

### 6.2 Partitioning

| Table | Partition columns | Rationale |
|---|---|---|
| `bronze.raw_openaq_readings` | `ingestion_date` | Append-only; date partitions make retention cleanup trivial |
| `silver.silver_readings` | `country_code`, `year_month` | Country-scoped time-range queries are the dominant access pattern |
| `gold.fact_readings` | `year_month` | Reporting always filters by period; country filtering is handled by Z-Order and dimension pushdown |

### 6.3 Z-Order

```python
spark.sql("OPTIMIZE gold_globalwatch.fact_readings ZORDER BY (location_id, reading_ts)")
```

Reports and RTI tiles filter station and time together, so co-locating those two columns lets Delta Data Skipping eliminate files that cannot contain matching rows.

### 6.4 Broadcast joins

Every dimension in this model is small enough to broadcast, and each is broadcast explicitly rather than relying on statistics:

| Table | Rows | Strategy |
|---|---|---|
| `dim_country` | 23 | Broadcast |
| `dim_pollutant` | 5 | Broadcast |
| `dim_station` | 308 | Broadcast |
| `fact_readings` | 894 and growing | Build side — never broadcast |

### 6.5 Salting

Reference-grade stations in dense cities return far more readings than the median station. Without intervention one executor receives that key's entire partition and becomes a straggler, or fails on memory. Salting spreads it:

```python
SALT_BUCKETS = 10

df_readings_salted = (df_readings
    .withColumn("salt", (F.rand() * SALT_BUCKETS).cast("int"))
    .withColumn("station_id_salted",
                F.concat(F.col("station_id"), F.lit("_"), F.col("salt"))))

df_station_exploded = (df_station
    .crossJoin(spark.range(SALT_BUCKETS).withColumnRenamed("id", "salt"))
    .withColumn("station_id_salted",
                F.concat(F.col("station_id"), F.lit("_"), F.col("salt"))))

df_joined = (df_readings_salted
    .join(F.broadcast(df_station_exploded), on="station_id_salted", how="left")
    .drop("salt", "station_id_salted"))
```

Full pattern catalogue with interview framing: [`docs/spark_optimization.md`](docs/spark_optimization.md).

---

## 7. Pipeline specification

### 7.1 `pl_batch_globalwatch`

```
Trigger: daily 02:00 IST
│
├── Bronze_Ingest          → 01_bronze_ingest_openaq
│     └── on success →
├── Silver_Transform       → 04_silver_transform
│     └── on success →
├── Gold_Star_Schema       → 05_gold_star_schema
│     └── on success →
├── ML_AQI_Prediction      → 06_ml_aqi_prediction
│     └── on success →
└── Data_Agent_Simulation  → 07_data_agent_simulation
```

Per activity: retry 2, retry interval 60s, timeout 1 hour, email on failure. Green (on-success) dependencies only — a Silver failure must not let a stale Gold be published.

### 7.2 `pl_realtime_globalwatch`

```
Trigger: hourly
└── Stream_To_Eventstream  → 07_streaming_openaq_eventstream
```

Retry 3, retry interval 30s, timeout 30 minutes. Latest run: succeeded in 1m 44s, 5,056 events dispatched.

### 7.3 Pipeline-mode constraints encountered

| Constraint | Resolution |
|---|---|
| `%pip` magic disabled in pipeline-triggered runs | `azure-eventhub` added to `globalwatch-env` as a PyPI library; `%pip` cell removed |
| Outbound KQL HTTP blocked in the trial pipeline sandbox | Verification cell short-circuits in pipeline mode with an explanatory message; run interactively to verify counts |

---

## 8. AI and natural-language specification

### 8.1 As built — grounded single-call assistant

The Streamlit AI Agent page constructs a `DATA_CONTEXT` system prompt from the loaded Gold snapshot on every render, then makes one Messages API call per user turn.

| Property | Value |
|---|---|
| Endpoint | `POST https://api.anthropic.com/v1/messages` |
| Model | `claude-sonnet-4-6` |
| `max_tokens` | 1000 |
| `anthropic-version` | `2023-06-01` |
| Auth | `x-api-key` from `st.secrets["ANTHROPIC_API_KEY"]` |
| Conversation | Full `st.session_state.messages` history sent each turn (the API is stateless) |

Facts injected into the system prompt: total readings, distinct stations, country list, per-country average PM2.5, per-country WHO exceedance counts, AQI category distribution, ML prediction distribution, the top five polluted stations, an architecture summary, and the WHO PM2.5 guideline. The instruction is to answer concisely and cite specific numbers.

The upside of grounding this way is that every figure the assistant states is traceable to the published snapshot. The limitation is that it cannot answer questions outside the injected aggregates — it has no query capability.

### 8.2 Specified — tool-use agent 🔷

For a deployment with network access to the Fabric endpoints, the agent becomes a three-tool router:

| Tool | Trigger | Flow |
|---|---|---|
| `query_kql` | Current/recent conditions ("last 6 hours") | NL → KQL → Eventhouse query endpoint → formatted result + summary |
| `query_gold_sql` | Historical trends, comparisons, aggregates | NL → T-SQL → Gold Lakehouse SQL endpoint → result + interpretation |
| `get_country_health_context` | Correlation with economic/health indicators | `country_code` → `dim_country` → GDP, health spend, population injected as context |

Target system prompt rules: always state the time range used; flag PM2.5 above the WHO guideline; never fabricate values outside tool results; make no medical recommendations.

### 8.3 Native Fabric Data Agent

Requires an F64+ SKU and is therefore unavailable on this capacity. `07_data_agent_simulation.ipynb` makes the pattern explicit instead — three natural language questions, their SQL translations, and their executed results — so the architecture is demonstrated even though the managed service is not provisioned.

---

## 9. Machine learning specification

| Property | Value |
|---|---|
| Notebook | `06_ml_aqi_prediction.ipynb` |
| Framework | Spark MLlib |
| Algorithm | `RandomForestClassifier`, `numTrees=100`, `maxDepth=5` |
| Features | `pm25`, `pm10`, `no2`, `o3`, `co` (pivoted long → wide, nulls → 0) |
| Label | 5-class WHO PM2.5 banding, 0 = Good … 4 = Hazardous |
| Split | 80/20, `seed=42` |
| Pipeline | `VectorAssembler` → `RandomForestClassifier` |
| Tracking | MLflow — params, metrics, model artifact |
| Registry | `globalwatch_aqi_classifier`, version 1 |
| Test accuracy | 96.15% |
| Feature importance | PM2.5 76.32%, PM10 12.91%, remainder across NO₂/O₃/CO |
| Inference output | `gold_globalwatch.dbo.fact_aqi_predictions`, `mode=overwrite` for idempotency |
| Current distribution | Good 130, Moderate 87, Unhealthy 20, Hazardous 8 |

Accuracy on this dataset should be read with its class imbalance in mind: PM2.5 is by construction the dominant signal for a PM2.5-derived label, so the headline number demonstrates a working end-to-end MLflow lifecycle more than it demonstrates a hard prediction problem. The transferable part is the pattern — track, register, load by URI, score, persist.

---

## 10. Security specification

### 10.1 Workspace RBAC

| Role | Assigned to | Permissions |
|---|---|---|
| Admin | Project owner | Full control |
| Contributor | CI/CD service principal (production design) | Read/write items, no workspace settings |
| Viewer | Report consumers | Read reports only |

### 10.2 Row-level security

A `ContinentViewer` role on `dim_country` restricts users to their own continent; admins are excluded from the role and see everything. Validated with **View as role** — see `screenshots/23_rls_continent_viewer_role.png`.

### 10.3 Secrets handling

| Secret | Storage | Notes |
|---|---|---|
| OpenAQ API key | `globalwatch-env` Spark property | Never in notebook source |
| Event Hub connection string | `globalwatch-env` Spark property | Never in notebook source |
| GitHub PAT | `globalwatch-env` Spark property | Scoped to contents write on this repo |
| Anthropic API key | Streamlit Cloud secrets | `.streamlit/secrets.toml` is git-ignored |

`.gitignore` excludes `.env`, `*.env`, `secrets.json`, `credentials.json`, and `.streamlit/secrets.toml`. No credential is committed anywhere in this repository.

### 10.4 Sensitivity labels

| Asset | Label |
|---|---|
| Gold tables | General — public data (air quality readings are public) |
| Any user-to-continent mapping table | Confidential — internal |
| Connection strings and tokens | Highly confidential — environment properties only |

---

## 11. Cost model

Costs assume a capacity that is paused outside active windows.

| Component | Basis | Estimated monthly |
|---|---|---|
| Fabric F2 capacity | ~$0.36/hr, ~4 active hrs/day, paused otherwise | ~$43 |
| OneLake storage | ~$0.023/GB; dataset is well under 1 GB today | < $1 |
| Eventstream, Eventhouse, Data Activator | Included in capacity | $0 |
| Streamlit Community Cloud | Free tier | $0 |
| OpenAQ / WAQI / World Bank / OpenMeteo | Free tiers | $0 |
| Anthropic API | Usage-based; assistant traffic is low-volume, ~1k output tokens per turn | Low single digits |
| **Total** | | **≈ $45/month** |

Cost controls in use: pause the capacity immediately after a session, run `OPTIMIZE` and `VACUUM` on a schedule to keep storage lean, use Starter Pools rather than custom Spark pools, and keep the streaming producer on an hourly rather than continuous cadence.

---

## 12. Repository layout

```
demonjd2026-afk/globalwatch-fabric/
├── README.md              # Overview, architecture, current findings, screenshot index
├── SETUP.md               # Phase-by-phase reproducible build guide
├── TECH_SPEC.md           # This document
├── .gitignore
├── .devcontainer/         # Codespaces definition
├── notebooks/             # 7 PySpark notebooks + guide
├── kql/                   # Eventhouse DDL + dashboard queries
├── streamlit/             # Public app, requirements, published Gold snapshots
├── docs/                  # Architecture, data model, Spark patterns, narratives, PDF report
└── screenshots/           # 39 screenshots evidencing every phase
```

Fabric item definitions (pipelines, semantic model, report, Eventstream, Activator rule) live in the workspace rather than this repository because Git integration is blocked on the trial tenant. Their configuration is captured in `SETUP.md` and evidenced in `screenshots/`.

---

## 13. Production hardening backlog

Ordered by value if this moved beyond a portfolio build:

1. **CI/CD** — Azure DevOps Git integration plus Dev → Test → Prod deployment pipelines with gated promotion on Silver DQ and Gold assertion results.
2. **Enrichment** — deploy the World Bank and OpenMeteo Dataflows to populate the reserved `dim_country` columns and add weather features to the model.
3. **Live-endpoint agent** — implement the §8.2 tool-use agent so natural-language answers reach beyond the pre-computed aggregates.
4. **Data quality as a first-class artefact** — persist DQ rule outcomes to a `dq_results` Delta table and alert on rule regressions rather than only on pipeline failure.
5. **Backfill** — replace `/latest` with a historical measurement pull for the leading countries so trend analysis has depth as well as breadth.
6. **Model improvement** — multi-pollutant labelling, class weighting for the minority Hazardous class, and scheduled retraining with model-version comparison in MLflow.
7. **Observability** — a `pipeline_run_log` Delta table capturing per-activity row counts and durations, surfaced as an operations page in the report.

---

## 14. Definition of done

A phase is complete when:

- [ ] Code is committed and pushed to `main`
- [ ] A screenshot evidencing the working result is in `screenshots/`
- [ ] `SETUP.md` marks the phase ✅
- [ ] No credential appears anywhere in committed code
- [ ] The notebook runs end-to-end on a fresh session
- [ ] Row counts are validated against the source within the accepted DQ tolerance
- [ ] The pipeline activity shows **Succeeded** in Monitor, not just interactively

---

*Jayanth Dolai · [LinkedIn](https://www.linkedin.com/in/jayanth-dolai-7b115213a/) · [Live app](https://globalwatch-fabric.streamlit.app/) · [Repository](https://github.com/demonjd2026-afk/globalwatch-fabric)*
