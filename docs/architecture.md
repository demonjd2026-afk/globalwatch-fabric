# Architecture

GlobalWatch is a Lambda architecture on Microsoft Fabric: a batch path for historical analytics and ML, and a speed path for operational awareness and alerting. Both read and write the same OneLake, so there is no copy-based integration between them.

---

## End-to-end flow

```
┌───────────────────────────── SOURCE ─────────────────────────────┐
│  OpenAQ v3 API — /locations then /locations/{id}/latest          │
│  70 ISO country codes · 10-worker parallel fetch · 60 req/min    │
└───────────────┬──────────────────────────────┬───────────────────┘
                │ BATCH PATH                   │ SPEED PATH
                ▼                              ▼
    01_bronze_ingest_openaq        07_streaming_openaq_eventstream
    watermark + mergeSchema        Azure Event Hubs SDK producer
                │                              │
                ▼                              ▼
    ┌───────────────────────┐      ┌───────────────────────────────┐
    │ bronze_globalwatch    │      │ es_openaq_realtime            │
    │  raw_openaq_readings  │      │ custom endpoint → Eventhouse  │
    │  watermark_control    │      └──────────────┬────────────────┘
    │  part: ingestion_date │                     ▼
    └──────────┬────────────┘      ┌──────────────────────────────┐
               │ 04_silver         │ globalwatch_eventhouse       │
               ▼                   │  raw_readings      (30 days) │
    ┌───────────────────────┐      │        │ update policy        │
    │ silver_globalwatch    │      │        ▼ TransformRawReadings │
    │  silver_readings      │      │  silver_readings  (365 days) │
    │  4 DQ rules + AQI     │      └──────────────┬───────────────┘
    │  part: country+month  │                     ▼
    └──────────┬────────────┘         ┌──────────────────────────┐
               │ 05_gold              │ Data Activator           │
               ▼                      │ pm25_hazard_alert        │
    ┌───────────────────────┐         │ PM2.5 > 150 → email      │
    │ gold_globalwatch      │         └──────────────────────────┘
    │  fact_readings        │
    │  dim_station (SCD2)   │◄── 06_ml_aqi_prediction
    │  dim_country (SCD1)   │     MLflow RF → fact_aqi_predictions
    │  dim_date, dim_pollutant
    │  V-Order + Z-Order    │
    └──────────┬────────────┘
               │
    ┌──────────┼───────────────────────┐
    ▼          ▼                       ▼
Direct Lake  globalwatch_        08_export_to_streamlit
Power BI     warehouse           → GitHub JSONL snapshots
+ RLS        cross-LH T-SQL      → Streamlit Cloud app
                                    + grounded Claude assistant
```

Orchestration: `pl_batch_globalwatch` chains the five batch notebooks daily at 02:00 IST; `pl_realtime_globalwatch` runs the streaming producer hourly.

---

## Architecture decision records

### ADR-001 — Microsoft Fabric over assembled Azure services

**Decision:** Use Fabric as the single platform instead of ADF + Databricks + Synapse + Power BI separately.

**Rationale:**
- OneLake removes data silos — Spark, T-SQL and KQL engines read the same Delta files with no copies.
- Eventstream, Eventhouse and Data Activator are first-party; there are no custom connectors to build or maintain.
- Direct Lake removes the Power BI import/refresh cycle entirely.
- One governance layer (workspace RBAC, sensitivity labels, Purview) covers every workload.
- One capacity covers all compute rather than several independently-scaled services.

**Trade-off:** Fabric is newer — fewer community answers, and some capabilities are gated by SKU size. This project hit exactly that: the native Data Agent requires F64+ and is therefore unavailable on trial capacity.

---

### ADR-002 — Lambda over Kappa

**Decision:** Maintain a batch Delta medallion and a separate KQL speed layer.

**Rationale:**
- The batch layer needs multi-pass work that streaming does not suit: SCD2 dimension maintenance, star schema joins, ML training and scoring.
- The speed layer needs seconds-level freshness for alerting; Delta streaming latency cannot reach it.
- Expressing SCD2 and model scoring as stateful streaming would be markedly more complex than running two paths.

**Trade-off:** Two code paths. Mitigated because the speed path is declarative — an Eventstream plus a KQL update policy, with the only custom code being the producer notebook.

---

### ADR-003 — KQL Eventhouse for the real-time layer

**Decision:** The live feed lands in a KQL database, not directly in Delta Gold.

**Rationale:**
- KQL is built for time-series: `summarize avg(value) by bin(reading_ts, 1h)` over ingestion-ordered data is far cheaper than the Spark SQL equivalent over Delta.
- Update policies handle stream-time transformation with no separate compute and transactional guarantees (`IsTransactional: true` means raw and silver cannot diverge).
- Data Activator binds natively to KQL and Eventstream — no connector.

**Trade-off:** Two query languages in the platform. Accepted: the KQL surface area actually needed is small and fully captured in `kql/queries_dashboard.kql`.

---

### ADR-004 — Lakehouse for the medallion, Warehouse for cross-domain SQL

**Decision:** Gold serves primarily via Lakehouse + Direct Lake; the Warehouse handles T-SQL that spans items.

**Rationale:**
- Direct Lake over a Lakehouse is the fastest Power BI path — no import copy, no DirectQuery round trip.
- A Fabric Warehouse cannot write Delta natively, so it structurally cannot be the medallion target.
- The Warehouse earns its place for ad-hoc T-SQL across lakehouses or warehouse-native constructs.

**Trade-off:** Consumers must know which endpoint to use. Mitigated by defaulting all reporting to the semantic model and documenting the split.

---

### ADR-005 — Claude for the natural language layer

**Decision:** Anthropic's Claude (`claude-sonnet-4-6`) via the Messages API, called from the Streamlit app.

**Rationale:**
- The native Fabric Data Agent requires F64+, which this capacity does not provide.
- The Anthropic API is directly reachable from Streamlit Cloud with no quota-approval process.
- Grounding the model on pre-computed Gold aggregates keeps every stated figure traceable to the published snapshot.

**Trade-off:** Outside Azure, so no Managed Identity — the key lives in Streamlit secrets, never in the repository.

---

### ADR-006 — Publish Gold snapshots to GitHub rather than exposing an endpoint

**Decision:** The pipeline writes JSONL extracts of Gold into this repository; the app reads them over `raw.githubusercontent.com` with a 30-minute cache.

**Rationale:**
- The public app costs nothing to host and needs no inbound path into the Fabric workspace.
- The only credential the app holds is the LLM key.
- Every published number has a versioned, auditable commit.

**Trade-off:** The app shows the last export, not live Gold — freshness is bounded by pipeline cadence (daily for the Gold extracts, hourly for the real-time counter).

---

## Medallion design

### Bronze — raw landing zone
- Delta writes with `mergeSchema=true` so OpenAQ schema drift extends the table instead of failing the run.
- Partitioned by `ingestion_date`.
- Append-only — raw payloads are never modified, preserving a full audit trail.
- `watermark_control` tracks `last_loaded_date` / `last_loaded_ts` per source; a failed run does not advance it, so the next run resumes from the last good point.

### Silver — cleaned and conformed
- Four DQ rules in order: drop empty parameter → keep only the five criteria pollutants → drop values outside `0 < value < 10000` → deduplicate on `(location_id, parameter, reading_ts)`.
- AQI categorisation applied to PM2.5; other pollutants carry `N/A`.
- Unit normalisation (`µg/m³` → `ug/m3`) plus reporting columns (`reading_date`, `reading_hour`, `year_month`, `is_recent`).
- Partitioned by `country_code` + `year_month`.
- V-Order off — Silver is intermediate, not a Direct Lake target.

### Gold — star schema serving layer
- `fact_readings` — long fact, one row per station × pollutant × reading, with the WHO exceedance flag precomputed per pollutant.
- `dim_station` — SCD Type 2 via Delta MERGE with an MD5 `station_hash`.
- `dim_country` — SCD Type 1, continent mapping plus reserved World Bank enrichment columns.
- `dim_date` — pre-generated spine 2015 → 2030.
- `dim_pollutant` — static WHO reference.
- `fact_aqi_predictions` — Random Forest output, rewritten idempotently each run.
- V-Order on all tables; `fact_readings` additionally Z-Ordered on `(location_id, reading_ts)` and partitioned by `year_month`.

Full schema and SCD detail: [`data_model.md`](data_model.md).

---

## Serving layer

| Consumer | Path | Notes |
|---|---|---|
| Power BI report | Direct Lake semantic model over Gold | Four active relationships; `ContinentViewer` RLS role on `dim_country` |
| Ad-hoc SQL | `globalwatch_warehouse` | Cross-lakehouse T-SQL |
| Real-time queryset | KQL Eventhouse | Seven dashboard queries in `kql/queries_dashboard.kql` |
| Alerting | Data Activator on the Eventstream | Email above 150 µg/m³ PM2.5 |
| Public web | Streamlit Cloud reading GitHub JSONL | Dashboard + grounded Claude assistant |

The Streamlit assistant is a **single grounded Messages API call per turn**, not a tool-use agent: the app pre-computes aggregates from the loaded snapshot and injects them as the system prompt. That makes every answer traceable to published data, and means the assistant cannot query beyond those aggregates. The three-tool design (`query_kql`, `query_gold_sql`, `get_country_health_context`) is specified in [`../TECH_SPEC.md`](../TECH_SPEC.md#8-ai-and-natural-language-specification) as the target for a deployment with live endpoint access.

---

## Spark optimisation summary

| Technique | Where applied | Benefit |
|---|---|---|
| AQE (`enabled`, `coalescePartitions`, `skewJoin`) | All notebooks | Runtime re-planning; avoids hundreds of tiny shuffle files |
| Broadcast join | Gold fact build | No shuffle for `dim_country` (23), `dim_pollutant` (5), `dim_station` (308) |
| Salting | Skewed station joins in Silver | Prevents stragglers and OOM on high-volume city stations |
| Partitioning | Silver + Gold | Partition pruning on the dominant access pattern |
| Z-Order | `fact_readings` | Data Skipping on combined station + time filters |
| V-Order | All Gold writes | Direct Lake VertiPaq scan performance |
| Delta MERGE | `dim_station` SCD2 | Atomic expire + insert in one transaction |
| `mergeSchema` | Bronze writes | Absorbs OpenAQ API schema evolution |

Code and interview framing for each: [`spark_optimization.md`](spark_optimization.md).

---

## Known constraints

| Constraint | Impact | Handling |
|---|---|---|
| Fabric Data Agent needs F64+ | No managed NL querying | NL→SQL pattern demonstrated in `07_data_agent_simulation`; user-facing NL delivered by the Streamlit assistant |
| Git integration blocked on trial tenant | Fabric items not version-controlled automatically | This repository is the source of truth; configuration documented in `SETUP.md` and evidenced in `screenshots/` |
| `%pip` disabled in pipeline runs | Streaming notebook failed under the pipeline | `azure-eventhub` moved into the `globalwatch-env` environment |
| Outbound KQL calls blocked in the pipeline sandbox | KQL verification cell failed | Cell short-circuits in pipeline mode; run interactively to verify counts |
| OpenAQ free tier 60 req/min | Limits ingestion breadth per run | Semaphore-bounded parallel fetch and `/latest` (one call per location, not per sensor) |
