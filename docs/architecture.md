# Architecture

## Overview

GlobalWatch uses a Lambda architecture on Microsoft Fabric — a batch path for historical analytics and a real-time path for operational dashboards. Both paths share the same OneLake storage layer.

```
┌─────────────────────────────── DATA SOURCES ────────────────────────────────┐
│  OpenAQ API (real-time)  │  WAQI API (batch)  │  World Bank CSV  │ OpenMeteo│
└──────────┬───────────────┴────────┬────────────┴───────┬──────────┴────┬────┘
           │                        │                     │               │
┌──────────▼────────────────────────────────────────────────────────────────────┐
│                              INGESTION LAYER                                  │
│  Fabric Eventstream    │  FDF Watermark Pipeline  │  Dataflow Gen2  │Shortcut │
└──────────┬────────────────────────────────────────────────────────────────────┘
           │
┌──────────▼──────────────── ONELAKE MEDALLION ─────────────────────────────────┐
│  bronze_globalwatch ──► silver_globalwatch ──► gold_globalwatch │  Warehouse  │
└──────────┬─────────────────────────────────────────────────────────────────────┘
           │
┌──────────▼──────────── SPARK COMPUTE (PySpark) ───────────────────────────────┐
│  Bronze→Silver DQ   │  Silver→Gold SCD   │  AQI ML scoring  │ Streaming job   │
└──────────┬─────────────────────────────────────────────────────────────────────┘
           │
┌──────────▼──────────── REAL-TIME INTELLIGENCE ────────────────────────────────┐
│  KQL Database (update policies)  │  RTI Dashboard  │  Data Activator           │
└──────────┬─────────────────────────────────────────────────────────────────────┘
           │
┌──────────▼──────────── SERVING + AI AGENT ────────────────────────────────────┐
│  Direct Lake Power BI + RLS  │  Fabric Copilot  │  Claude AI Agent  │  CI/CD  │
└───────────────────────────────────────────────────────────────────────────────┘
```

---

## Architecture Decision Records

### ADR-001: Why Microsoft Fabric over standalone Azure services?

**Decision:** Use Fabric as the single platform instead of ADF + Databricks + Synapse + Power BI separately.

**Rationale:**
- OneLake eliminates data silos — all engines (Spark, SQL, KQL) read the same Delta files
- Fabric-native pipelines, Eventstream, and RTI are fully integrated — no custom connectors
- Direct Lake mode removes the Power BI import/refresh cycle entirely
- Single governance layer (Purview + sensitivity labels) across all workloads
- Cost: one F-SKU covers all workloads vs paying separately for each service

**Trade-off:** Fabric is newer — fewer community resources, some features still in preview.

---

### ADR-002: Why Lambda architecture (batch + real-time)?

**Decision:** Maintain separate batch (Delta medallion) and speed (KQL) layers.

**Rationale:**
- Batch layer: historical completeness, complex enrichment (World Bank, weather), ML scoring — all require multi-pass Spark processing not suitable for streaming
- Speed layer: operational dashboards need sub-second freshness — Delta streaming latency (~minutes) is too slow for live AQI monitoring
- Kappa (streaming only) would require complex stateful aggregations better expressed as Spark batch jobs

**Trade-off:** Two code paths to maintain. Mitigated by Fabric Eventstream handling the real-time path declaratively.

---

### ADR-003: Why KQL Database for real-time vs Delta streaming?

**Decision:** Real-time OpenAQ feed lands in KQL Database, not directly into Delta Gold tables.

**Rationale:**
- KQL is purpose-built for time-series — aggregations like `summarize avg(pm25) by bin(ts, 1h)` run 10-100x faster than equivalent Spark SQL on Delta
- KQL update policies handle stream-time transformations without separate compute
- Data Activator integrates natively with KQL — no connector needed

**Trade-off:** Two query languages (KQL + SQL). Covered by the AI agent's NL-to-KQL translation layer.

---

### ADR-004: Why Lakehouse for medallion + Warehouse for ad-hoc SQL?

**Decision:** Gold layer primary serving via Lakehouse (Direct Lake); Warehouse for cross-domain SQL.

**Rationale:**
- Lakehouse + Direct Lake: fastest Power BI query path — no data copy, no DirectQuery overhead
- Fabric Warehouse: needed when queries join Gold Lakehouse tables with Warehouse-native aggregation tables or require T-SQL features
- A Warehouse cannot write Delta natively — so it cannot be the medallion target

**Trade-off:** Analysts need to know which endpoint to use. Mitigated by the AI agent routing queries automatically.

---

### ADR-005: Why Claude (Anthropic) for AI agent?

**Decision:** Use Claude Sonnet via Anthropic API for the tool-use agent.

**Rationale:**
- Claude's tool-use is reliable for structured KQL/SQL generation
- No Azure OpenAI quota approval process needed
- Anthropic API is directly accessible from Streamlit Cloud
- Cost: Claude Sonnet is cheaper per token than GPT-4o for this workload

**Trade-off:** Not on Azure — can't use Azure Managed Identity auth. API key stored in Streamlit secrets.

---

## Medallion Design

### Bronze — Raw Landing Zone
- Delta tables written with `mergeSchema=True` — handles API schema drift
- Partitioned by `ingestion_date`
- Append-only — raw data never modified
- Watermark control table tracks last loaded date per source

### Silver — Cleaned and Conformed
- DQ rules: drop empty parameters, invalid values, duplicates
- AQI categorization applied on PM2.5 readings
- Partitioned by `country_code` + `year_month`
- V-Order OFF — intermediate layer

### Gold — Star Schema Serving Layer
- `fact_readings` — pollutant readings with WHO exceedance flag
- `dim_station` — SCD Type 2 via Delta MERGE
- `dim_country` — SCD Type 1 (overwrite)
- `dim_date` — pre-generated spine 2015–2030
- `dim_pollutant` — static WHO reference
- V-Order ON — Direct Lake compatible
- Z-Ordered on `location_id` + `reading_ts`

---

## Spark Optimization Summary

| Technique | Where Applied | Benefit |
|---|---|---|
| AQE | All notebooks | Dynamic partition coalescing, skew join handling |
| Broadcast join | Gold fact build | Eliminates shuffle for small dimensions |
| Salting | Skewed station joins | Prevents OOM on high-volume city stations |
| V-Order | Gold Delta writes | Direct Lake scan performance |
| Z-Order | fact_readings | Filter pushdown for station + time queries |
| mergeSchema | Bronze writes | Handles OpenAQ API schema evolution |
| Delta MERGE | dim_station SCD2 | Atomic expire + insert in one operation |
| Partitioning | Silver + Gold | Country + time-based partition pruning |
