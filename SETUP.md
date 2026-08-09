# GlobalWatch — Setup Guide

A reproducible, phase-by-phase build guide for the GlobalWatch platform on Microsoft Fabric. Every phase lists what to create, what to run, what "done" looks like, and the screenshot that evidences it.

Prerequisites: a Microsoft Fabric workspace on trial or paid capacity, a free [OpenAQ v3 API key](https://explore.openaq.org/register), and a GitHub personal access token if you want the pipeline to publish snapshots to the Streamlit app.

---

## Environment used for this build

| Item | Value |
|---|---|
| Platform | Microsoft Fabric |
| Capacity | Fabric Trial — FTL64, Central India |
| Licence | Power BI Trial + Fabric Trial capacity |
| Workspace | `globalwatch-dev` |
| Git repo | `demonjd2026-afk/globalwatch-fabric` |
| Public app | `https://globalwatch-fabric.streamlit.app/` |
| Build period | Aug 2026 |

---

## Phase 1 — Workspace and storage

### 1.1 Sign in
`app.fabric.microsoft.com` → confirm the Fabric trial and Power BI trial are active in the profile panel.
📸 `screenshots/01_fabric_home.png` ✅

### 1.2 Create the workspace
**New workspace** → name `globalwatch-dev`, type **Fabric Trial**.
📸 `screenshots/02_workspace_created.png` ✅

### 1.3 Create three lakehouses
**New item → Lakehouse**, three times:

| Name | Purpose |
|---|---|
| `bronze_globalwatch` | Raw landing zone |
| `silver_globalwatch` | Cleaned, DQ-checked data |
| `gold_globalwatch` | Star schema serving layer |

📸 `screenshots/03_three_lakehouses.png` ✅

### 1.4 Create the warehouse
**New item → Warehouse** → `globalwatch_warehouse`. Used for cross-lakehouse T-SQL; it is deliberately *not* a medallion target (a Warehouse cannot write Delta).
📸 `screenshots/04_warehouse_created.png` ✅

### 1.5 Create the Eventhouse
**New item → Eventhouse** → `globalwatch_eventhouse`. A KQL database of the same name is created inside it.

### 1.6 Create the Eventstream
**New item → Eventstream** → `es_openaq_realtime`.

### 1.7 Create the Spark environment
**New item → Environment** → `globalwatch-env`.

Spark properties (these are how secrets reach the notebooks — nothing is hardcoded):

| Property | Value |
|---|---|
| `spark.openaq.api.key` | Your OpenAQ v3 API key |
| `spark.eventhub.connection.string` | Event Hub connection string from the Eventstream custom endpoint |
| `spark.eventhub.name` | Event Hub name from the same endpoint |
| `spark.github.token` | GitHub PAT with `contents:write` on the repo (used by notebooks 07 and 08) |

Public libraries → add `azure-eventhub` from PyPI. **Do this rather than `%pip install` in the notebook** — `%pip` magic is disabled in pipeline-triggered runs. Click **Publish** and wait for the environment to finish publishing.

---

## Phase 2 — Medallion notebooks

Import the notebooks from [`notebooks/`](notebooks/), attach each to its lakehouse, and set **Environment = `globalwatch-env`** on all of them.

### 2.1 Bronze ingestion
- Notebook: `01_bronze_ingest_openaq`
- Lakehouse: `bronze_globalwatch`
- Output: `raw_openaq_readings` (Delta, partitioned by `ingestion_date`) + `watermark_control`
- Key techniques: watermark control table, `mergeSchema=true`, AQE, 10-worker `ThreadPoolExecutor` over 70 country codes with a semaphore holding to OpenAQ's 60 req/min free tier

📸 `screenshots/10_bronze_notebook_run.png` ✅

### 2.2 Silver transform
- Notebook: `04_silver_transform`
- Lakehouse: `silver_globalwatch` (reads Bronze via the 3-part name `bronze_globalwatch.dbo.raw_openaq_readings`)
- Output: `silver_readings`, partitioned by `country_code` + `year_month`
- DQ rules: empty parameter → unknown pollutant → invalid value → deduplicate
- Adds: AQI category (PM2.5 only), unit normalisation, `reading_date` / `reading_hour` / `year_month` / `is_recent`

📸 `screenshots/11_silver_transform_run.png` ✅ · `screenshots/12_spark_ui_aqe.png` ✅

### 2.3 Gold star schema
- Notebook: `05_gold_star_schema`
- Lakehouse: `gold_globalwatch`
- Output: `dim_date`, `dim_pollutant`, `dim_country`, `dim_station`, `fact_readings`
- `dim_station` is SCD Type 2 via Delta MERGE with an MD5 `station_hash`; `dim_country` is SCD Type 1
- `fact_readings` is written with V-Order, then `OPTIMIZE … ZORDER BY (location_id, reading_ts)`
- Cell 7 is the "definition of done" validation: row counts, null checks, WHO exceedance report, SCD2 integrity

📸 `screenshots/13_gold_notebook_scd2_merge.png` ✅ · `screenshots/14_gold_delta_table_detail.png` ✅

---

## Phase 3 — KQL Eventhouse

Open `globalwatch_eventhouse` → **Query with code** → run the blocks in [`kql/schema_create.kql`](kql/schema_create.kql) in order:

1. `.create table raw_readings (…)` — real-time landing table
2. `.create table silver_readings (…)` — transformed table
3. `.create-or-alter function TransformRawReadings()` — AQI categorisation + WHO exceedance flags
4. `.alter table silver_readings policy update …` — attach the update policy (`IsEnabled: true`, `IsTransactional: true`)
5. Retention policies — raw 30 days, silver 365 days
6. `.set-or-append silver_readings <| TransformRawReadings()` — one-time backfill of anything already in raw

📸 `screenshots/16_kql_database_created.png` ✅

---

## Phase 4 — Eventstream

### 4.1 Configure `es_openaq_realtime`
1. **Use custom endpoint** → name `openaq-custom-source`. Copy the connection string and Event Hub name into `globalwatch-env`.
2. **Add destination → Eventhouse**
   - Mode: *Event processing before ingestion*
   - Eventhouse / KQL database: `globalwatch_eventhouse`
   - Table: `raw_readings`, format JSON
3. **Save → Publish**, and confirm the destination shows **Live**.

📸 `screenshots/17_eventstream_configured.png` ✅

### 4.2 Stream data
Run `07_streaming_openaq_eventstream`. It fetches OpenAQ locations across the same 70 country codes in parallel and sends one Event Hub batch per country, then writes run statistics to `streamlit/data/kql_stats.json` on GitHub.

The KQL verification cell is deliberately skipped in pipeline mode — outbound KQL calls are blocked in the trial pipeline sandbox. Run it manually in an interactive session to confirm counts.

📸 `screenshots/18_eventstream_live_data.png` ✅ · `screenshots/33_kql_raw_readings_count.png` ✅

---

## Phase 5 — Data Activator alerting

1. From the Eventstream, **Set alert** on the `pm25_station` object with `location_id` as the instance key.
2. Rule `pm25_hazard_alert`: fire when `value > 150` (WHO *Hazardous*).
3. Action: email containing station name, city, country and current PM2.5.
4. Start the rule and confirm the status is **Running**.

Result in this build: 5 of 98 monitored station IDs actively triggering; the alert email arrived 09 Aug 2026 at 06:08 UTC.

📸 `screenshots/20_activator_rule_configured.png` ✅ · `screenshots/21_activator_alert_fired.png` ✅

---

## Phase 6 — Machine learning

Run `06_ml_aqi_prediction` (attach `silver_globalwatch`, and `gold_globalwatch` for the write-back).

1. Load `silver_readings`, EDA on parameter and AQI distributions.
2. Pivot long → wide (`pm25`, `pm10`, `no2`, `o3`, `co`), fill nulls with 0, label from WHO PM2.5 thresholds.
3. `VectorAssembler` → `RandomForestClassifier` (`numTrees=100`, `maxDepth=5`), 80/20 split, `seed=42`, all tracked in MLflow.
4. Register the model as `globalwatch_aqi_classifier` version 1.
5. Load the registered model, score Gold PM2.5 rows, write `fact_aqi_predictions`.

Result: 96.15% test accuracy; PM2.5 is the dominant feature at 76.32%.

📸 `screenshots/26_mlflow_experiment_run.png` ✅ · `screenshots/27_ml_predictions_output.png` ✅ · `screenshots/25_ml_aqi_predicted_classes.png` ✅

---

## Phase 7 — Natural language querying

The native **Fabric Data Agent requires an F64+ SKU**, which the trial capacity does not provide. Instead, `07_data_agent_simulation` demonstrates the same NL → SQL → result pattern explicitly: three natural language questions, their SQL equivalents, and the executed results against Gold.

📸 `screenshots/28_data_agent_nl_to_sql.png` ✅ · `screenshots/29_data_agent_query_results.png` ✅

The user-facing natural language experience is delivered instead by the Streamlit AI Agent page (Phase 10).

---

## Phase 8 — Orchestration

### 8.1 `pl_batch_globalwatch` — daily 02:00 IST
Five notebook activities chained on success:

```
Bronze_Ingest → Silver_Transform → Gold_Star_Schema → ML_AQI_Prediction → Data_Agent_Simulation
```

Per activity: retry 2, retry interval 60s, timeout 1h. Failure notifications by email.

📸 `screenshots/31_batch_pipeline_scheduled.png` ✅ · `screenshots/34_batch_pipeline_success.png` ✅ · `screenshots/35_gold_table_counts_post_pipeline.png` ✅

### 8.2 `pl_realtime_globalwatch` — hourly
Single activity `Stream_To_Eventstream` running `07_streaming_openaq_eventstream`. Retry 3, interval 30s, timeout 30 min. Latest run: succeeded in 1m 44s, 5,056 events sent.

📸 `screenshots/32_realtime_pipeline_scheduled.png` ✅ · `screenshots/32_realtime_pipeline_success.png` ✅

### 8.3 Two pipeline-mode gotchas worth knowing
| Symptom | Cause | Fix |
|---|---|---|
| `%pip magic command is disabled` | `%pip install` works interactively but not in pipeline runs | Add the library to `globalwatch-env` as a PyPI dependency and delete the `%pip` cell |
| KQL verification cell fails on network resolution | Outbound KQL HTTP calls are blocked in the trial pipeline sandbox | Skip the cell in pipeline mode; run it interactively to verify |

---

## Phase 9 — Direct Lake serving

### 9.1 Semantic model
Open `gold_globalwatch` → **New semantic model** → `GlobalWatch_Model`, selecting `fact_readings`, `dim_station`, `dim_country`, `dim_date`, `dim_pollutant`. Storage mode is **Direct Lake** automatically. Create the four relationships from `fact_readings` to each dimension.

📸 `screenshots/22_semantic_model_relationships.png` ✅

### 9.2 Row-level security
Create a `ContinentViewer` role on `dim_country` filtering by continent, and validate with **View as role**.

📸 `screenshots/23_rls_continent_viewer_role.png` ✅

### 9.3 Report
Two pages — KPI cards with average PM2.5 by country and an AQI donut, then readings by date across all countries.

📸 `screenshots/24_powerbi_report_page1.png` ✅ · `screenshots/36_powerbi_page2_fixed.png` ✅

---

## Phase 10 — Public app

### 10.1 Export Gold to GitHub
Run `08_export_to_streamlit`. It reads four Gold tables, serialises each to newline-delimited JSON, and PUTs them to `streamlit/data/` through the GitHub Contents API (GET the file SHA, then PUT with it). This is what keeps the public app in step with the pipeline.

| Exported file | Source |
|---|---|
| `fact_readings.json` | `gold_globalwatch.dbo.fact_readings` (dashboard column subset) |
| `dim_country.json` | `gold_globalwatch.dbo.dim_country` |
| `dim_station.json` | `gold_globalwatch.dbo.dim_station` where `active_flag = true` |
| `fact_aqi_predictions.json` | `gold_globalwatch.dbo.fact_aqi_predictions` |
| `kql_stats.json` | written separately by `07_streaming_openaq_eventstream` |

### 10.2 Deploy on Streamlit Cloud
1. Connect the repo at [share.streamlit.io](https://share.streamlit.io).
2. Main file path: `streamlit/app.py`.
3. Add `ANTHROPIC_API_KEY` under **Settings → Secrets**.
4. Deploy.

📸 `screenshots/37_streamlit_dashboard.png` ✅ · `screenshots/38_streamlit_ai_agent.png` ✅

---

## Phase 11 — CI/CD (documented, not deployable here)

Fabric Git integration is blocked on this trial tenant: GitHub OAuth is disabled for the account type and Azure DevOps requires an organisational account.

📸 `screenshots/30_git_integration_limitation.png` ✅

The production design is documented in [`TECH_SPEC.md`](TECH_SPEC.md) and [`docs/interview_narratives.md`](docs/interview_narratives.md): workspace connected to an Azure DevOps branch for item-level version control, plus a Dev → Test → Prod deployment pipeline for environment promotion, with connection strings held in Fabric environment variables or Azure Key Vault. This repository is the source of truth in the meantime.

---

## Verification checklist

| # | Check | Expected |
|---|---|---|
| 1 | Bronze row count | > 0, watermark advanced |
| 2 | Silver null check | zero nulls in `location_id`, `country_code`, `parameter`, `value`, `reading_ts` |
| 3 | Gold row counts | `dim_date` 5,844 · `dim_pollutant` 5 · others variable |
| 4 | SCD2 integrity | every `location_id` has exactly one row with `active_flag = true` |
| 5 | Delta detail | `fact_readings` shows V-Order and Z-Order applied |
| 6 | KQL | `raw_readings` and `silver_readings` counts both increasing |
| 7 | Data Activator | rule status *Running*, alert email received |
| 8 | MLflow | `globalwatch_aqi_classifier` v1 registered, accuracy logged |
| 9 | Pipelines | both show *Succeeded* in Monitor |
| 10 | Streamlit | app loads and KPI counts match the exported JSONL row counts |

---

## Screenshot index

All 39 screenshots are in [`screenshots/`](screenshots/) and are indexed by phase in the [README](README.md#-screenshots).

| Phase | Screenshots |
|---|---|
| 1 — Workspace & storage | 01, 02, 03, 04 |
| 2 — Medallion | 10, 11, 12, 13, 14, 35 |
| 3–4 — KQL & Eventstream | 16, 17, 18, 33 |
| 5 — Data Activator | 20, 21 |
| 6 — ML | 25, 26, 27 |
| 7 — NL querying | 28, 29 |
| 8 — Orchestration | 30, 31, 32 (×2), 34 |
| 9 — Direct Lake serving | 22, 23, 24, 36 |
| 10 — Public app | 23 (map), 24 (chart), 26 (top 10), 27–29 (agent), 37, 38 |
