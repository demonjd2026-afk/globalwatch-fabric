# GlobalWatch — Setup Guide

> Document every step as you go. Replace each `📸 screenshot: description` line with the actual screenshot filename after you take it.

---

## Environment

| Item | Value |
|---|---|
| Platform | Microsoft Fabric |
| Account | `jayanthfabric@jayanthdolaigmail.onmicrosoft.com` |
| Tenant | Default Directory — jayanthdolaigmail |
| Fabric license | Free account (batch layers) → F2 capacity (RTI layer) |
| Git repo | `demonjd2026-afk/globalwatch-fabric` |
| Started | Aug 2026 |

---

## Phase 1 — Fabric Workspace Setup

### 1.1 Sign in to Microsoft Fabric

1. Go to `app.fabric.microsoft.com`
2. Sign in with `jayanthfabric@jayanthdolaigmail.onmicrosoft.com`
3. Verify license type shows "Free account" in profile panel

📸 screenshot: `screenshots/01_fabric_home.png`

---

### 1.2 Create Workspace

1. Click **New workspace** on the Fabric home page
2. Name: `globalwatch-dev`
3. Description: `GlobalWatch — World Air Quality Intelligence Platform (Dev)`
4. License mode: **Fabric (Trial)** or **Pro** — select whatever is available
5. Click **Apply**

📸 screenshot: `screenshots/02_workspace_created.png`

---

### 1.3 Create Three Lakehouses

Repeat **New → Lakehouse** three times inside `globalwatch-dev`:

| # | Name | Purpose |
|---|---|---|
| 1 | `bronze_globalwatch` | Raw landing zone — exact source representation |
| 2 | `silver_globalwatch` | Cleaned, conformed, DQ-checked data |
| 3 | `gold_globalwatch` | Star schema serving layer — Direct Lake ready |

📸 screenshot: `screenshots/03_three_lakehouses.png`

---

### 1.4 Create Fabric Warehouse

1. Click **New → Warehouse**
2. Name: `globalwatch_warehouse`
3. Purpose: Ad-hoc cross-lakehouse SQL queries

📸 screenshot: `screenshots/04_warehouse_created.png`

---

### 1.5 Connect Workspace to Git (Azure DevOps)

1. Go to workspace **Settings → Git integration**
2. Connect to Azure DevOps repo: `demonjd2026-afk/globalwatch-fabric`
3. Branch: `main`
4. Root folder: `/fabric`
5. Click **Connect and sync**

📸 screenshot: `screenshots/05_git_integration.png`

---

## Phase 2 — OneLake Shortcut Setup

### 2.1 Create Shortcut in Bronze Lakehouse

1. Open `bronze_globalwatch` lakehouse
2. Click **New shortcut** in the Files section
3. Select source: **Azure Data Lake Storage Gen2**
4. Enter connection details for external ADLS Gen2 (if available) OR skip and use direct API ingestion
5. Name shortcut: `external_raw_feed`

📸 screenshot: `screenshots/06_shortcut_created.png`

> **Interview note:** A Shortcut virtualizes external storage inside OneLake — no data copy, no ETL. The data stays in the source; Fabric reads it in-place. This is how you avoid data duplication across platforms.

---

## Phase 3 — Fabric Data Factory Pipeline (Batch — WAQI)

### 3.1 Create Watermark Table

Run this in a Bronze notebook first to create the watermark control table:

```python
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_globalwatch.watermark_control (
    source_name STRING,
    last_loaded_date DATE,
    last_loaded_ts TIMESTAMP
) USING DELTA
""")

spark.sql("""
INSERT INTO bronze_globalwatch.watermark_control VALUES
('waqi_batch', '2024-01-01', '2024-01-01T00:00:00')
""")
```

📸 screenshot: `screenshots/07_watermark_table.png`

---

### 3.2 Build FDF Pipeline

1. Click **New → Data pipeline** → name: `pl_waqi_batch_watermark`
2. Add activities in order:
   - **Lookup** → reads `watermark_control` for `last_loaded_date`
   - **Copy Activity** → hits WAQI API with date filter → writes to Bronze Delta
   - **Notebook Activity** → runs Silver transform notebook
   - **Stored Procedure / Script** → updates watermark to today's date
3. Set schedule: **Daily at 2:00 AM IST**

📸 screenshot: `screenshots/08_fdf_pipeline_canvas.png`
📸 screenshot: `screenshots/09_fdf_pipeline_run_success.png`

> **Interview note:** Watermark pattern avoids full reload every run — only pulls records newer than `last_loaded_date`. Essential for incremental batch pipelines on REST APIs.

---

## Phase 4 — PySpark Notebooks

### 4.1 Notebook: Bronze Ingestion — OpenAQ

File: `notebooks/01_bronze_ingest_openaq.ipynb`

Key cells to document:
- API call with pagination
- Schema enforcement on raw JSON
- Delta write with `mergeSchema=True`
- Row count validation

📸 screenshot: `screenshots/10_bronze_notebook_run.png`

---

### 4.2 Notebook: Silver Transform

File: `notebooks/04_silver_transform.ipynb`

Key optimizations to document with screenshots:
- AQE config cell
- Broadcast join on country dimension
- Salting logic for skewed station joins
- Partition write by `country_code` + `year_month`

📸 screenshot: `screenshots/11_silver_transform_run.png`
📸 screenshot: `screenshots/12_spark_ui_aqe.png` ← Spark UI showing AQE in action

---

### 4.3 Notebook: Gold Star Schema + SCD2

File: `notebooks/05_gold_star_schema.ipynb`

Key cells to document:
- Delta MERGE for SCD Type 2 on `dim_station`
- Delta MERGE for SCD Type 1 on `dim_country`
- V-Order write on `fact_readings`
- Z-order on `station_id`, `reading_ts`
- Row count reconciliation assert

📸 screenshot: `screenshots/13_gold_notebook_scd2_merge.png`
📸 screenshot: `screenshots/14_gold_delta_table_detail.png` ← `DESCRIBE DETAIL` output

---

## Phase 5 — Real-Time Intelligence (Requires F2 Capacity)

### 5.1 Buy / Activate F2 Capacity

1. Go to `portal.azure.com` → complete Azure free trial signup
2. Once subscription active: Fabric workspace → **Settings → License info → Buy capacity**
3. Select **F2** → region: **South India** (closest, cheapest)
4. Assign capacity to `globalwatch-dev` workspace

📸 screenshot: `screenshots/15_f2_capacity_active.png`

---

### 5.2 Create KQL Database

1. Workspace → **New → Eventhouse**
2. Name: `globalwatch_eventhouse`
3. Inside Eventhouse → KQL Database auto-created: `globalwatch_kql`

📸 screenshot: `screenshots/16_kql_database_created.png`

---

### 5.3 Create Eventstream

1. **New → Eventstream** → name: `es_openaq_realtime`
2. Source: **Custom endpoint** (generates an HTTPS ingest URL)
3. Copy the endpoint URL → paste into `notebooks/01_bronze_ingest_openaq.ipynb` as the streaming target
4. Destination: `globalwatch_kql` → table `raw_readings`

📸 screenshot: `screenshots/17_eventstream_configured.png`
📸 screenshot: `screenshots/18_eventstream_live_data.png`

---

### 5.4 KQL Update Policy

Run in KQL Queryset:

```kql
.create-or-alter function TransformRawReadings() {
    raw_readings
    | extend aqi_category = case(
        pm25_value <= 12.0, "Good",
        pm25_value <= 35.4, "Moderate",
        pm25_value <= 55.4, "Unhealthy for Sensitive Groups",
        pm25_value <= 150.4, "Unhealthy",
        "Hazardous")
    | project station_id, country_code, city, pm25_value,
              pm10_value, no2_value, aqi_category, reading_ts
}

.alter table silver_readings policy update
@'[{"IsEnabled": true, "Source": "raw_readings",
    "Query": "TransformRawReadings()", "IsTransactional": true}]'
```

📸 screenshot: `screenshots/19_kql_update_policy.png`

---

### 5.5 Data Activator

1. Workspace → **New → Reflex (Data Activator)**
2. Name: `act_pm25_hazard_alert`
3. Data source: KQL Database → `silver_readings`
4. Trigger condition: `pm25_value > 150` for 3 consecutive readings from same `station_id`
5. Action: **Send Teams notification** → message template:
   `⚠️ HAZARD ALERT: {station_id} in {city}, {country_code} — PM2.5: {pm25_value} µg/m³`

📸 screenshot: `screenshots/20_activator_rule_configured.png`
📸 screenshot: `screenshots/21_activator_alert_fired.png`

---

## Phase 6 — Power BI Direct Lake

### 6.1 Create Semantic Model

1. Open `gold_globalwatch` lakehouse
2. Click **New semantic model**
3. Name: `GlobalWatch_Model`
4. Select tables: `fact_readings`, `dim_station`, `dim_country`, `dim_date`, `dim_pollutant`
5. Mode auto-set to **Direct Lake** ✅

📸 screenshot: `screenshots/22_semantic_model_direct_lake.png`

---

### 6.2 Configure Row-Level Security

In Power BI Desktop (connected to the semantic model):

```dax
-- RLS filter on dim_country for continent-based access
[continent] = USERPRINCIPALNAME()
```

Or use a security mapping table:
```dax
[continent] IN
    CALCULATETABLE(
        VALUES(user_continent_map[continent]),
        user_continent_map[email] = USERPRINCIPALNAME()
    )
```

📸 screenshot: `screenshots/23_rls_configured.png`

---

### 6.3 Build Report Pages

| Page | Key visuals |
|---|---|
| Global AQI Overview | World map (AQI by station), KPI cards (Good/Moderate/Unhealthy/Hazardous counts) |
| Country Deep-Dive | PM2.5 trend line, pollutant breakdown bar, GDP vs AQI scatter |
| Station Explorer | Station-level timeline, min/max/avg per pollutant |
| Health Risk Heatmap | Country-level PM2.5 choropleth weighted by population |

📸 screenshot: `screenshots/24_pbi_global_overview.png`
📸 screenshot: `screenshots/25_pbi_country_deepdive.png`

---

## Phase 7 — AI Agent (Streamlit + Claude)

### 7.1 Local Setup

```bash
cd ai_agent
pip install -r requirements.txt
```

`requirements.txt`:
```
anthropic
streamlit
azure-kusto-data
pyodbc
pandas
python-dotenv
```

### 7.2 Environment Variables

Create `.env` (never commit this):
```
ANTHROPIC_API_KEY=your_key_here
KUSTO_URI=https://your-kql-cluster.kusto.fabric.microsoft.com
KUSTO_DATABASE=globalwatch_kql
FABRIC_SQL_ENDPOINT=your_gold_sql_endpoint
FABRIC_SQL_DATABASE=gold_globalwatch
```

### 7.3 Run Agent

```bash
streamlit run app.py
```

📸 screenshot: `screenshots/26_ai_agent_running.png`
📸 screenshot: `screenshots/27_ai_agent_query_response.png` ← example NL query + response

---

## Phase 8 — CI/CD

### 8.1 Git Integration Verification

1. Workspace → Settings → Git integration
2. Confirm all items show **Synced** status
3. Make a test change → commit message: `feat: initial workspace setup`

📸 screenshot: `screenshots/28_git_synced.png`

---

### 8.2 Deployment Pipeline

1. Workspace → **Create deployment pipeline**
2. Name: `globalwatch-deploy`
3. Stages: **Dev** → **Test** → **Prod**
4. Assign `globalwatch-dev` to Dev stage
5. Clone to create `globalwatch-test` and `globalwatch-prod` workspaces

📸 screenshot: `screenshots/29_deployment_pipeline.png`

---

## Screenshots Checklist

| # | File | Status |
|---|---|---|
| 01 | `01_fabric_home.png` | ⬜ |
| 02 | `02_workspace_created.png` | ⬜ |
| 03 | `03_three_lakehouses.png` | ⬜ |
| 04 | `04_warehouse_created.png` | ⬜ |
| 05 | `05_git_integration.png` | ⬜ |
| 06 | `06_shortcut_created.png` | ⬜ |
| 07 | `07_watermark_table.png` | ⬜ |
| 08 | `08_fdf_pipeline_canvas.png` | ⬜ |
| 09 | `09_fdf_pipeline_run_success.png` | ⬜ |
| 10 | `10_bronze_notebook_run.png` | ⬜ |
| 11 | `11_silver_transform_run.png` | ⬜ |
| 12 | `12_spark_ui_aqe.png` | ⬜ |
| 13 | `13_gold_notebook_scd2_merge.png` | ⬜ |
| 14 | `14_gold_delta_table_detail.png` | ⬜ |
| 15 | `15_f2_capacity_active.png` | ⬜ |
| 16 | `16_kql_database_created.png` | ⬜ |
| 17 | `17_eventstream_configured.png` | ⬜ |
| 18 | `18_eventstream_live_data.png` | ⬜ |
| 19 | `19_kql_update_policy.png` | ⬜ |
| 20 | `20_activator_rule_configured.png` | ⬜ |
| 21 | `21_activator_alert_fired.png` | ⬜ |
| 22 | `22_semantic_model_direct_lake.png` | ⬜ |
| 23 | `23_rls_configured.png` | ⬜ |
| 24 | `24_pbi_global_overview.png` | ⬜ |
| 25 | `25_pbi_country_deepdive.png` | ⬜ |
| 26 | `26_ai_agent_running.png` | ⬜ |
| 27 | `27_ai_agent_query_response.png` | ⬜ |
| 28 | `28_git_synced.png` | ⬜ |
| 29 | `29_deployment_pipeline.png` | ⬜ |

---

*Update ⬜ → ✅ as each screenshot is taken and committed to `screenshots/` folder.*
