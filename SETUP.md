# GlobalWatch — Setup Guide

> Document every step as you go. Replace each `📸 screenshot: description` line with the actual screenshot filename after you take it.

---

## Environment

| Item | Value |
|---|---|
| Platform | Microsoft Fabric |
| Account | `jaydolai@zohomail.in` |
| Tenant | ZOHOMAIL.IN |
| Fabric license | Power BI Trial (59 days) + Fabric Trial capacity |
| Capacity | Trial-20260610T005336Z — FTL64 — Central India |
| Git repo | `demonjd2026-afk/globalwatch-fabric` |
| Started | Aug 2026 |

---

## Phase 1 — Fabric Workspace Setup

### 1.1 Sign in to Microsoft Fabric
1. Go to `app.fabric.microsoft.com`
2. Sign in with `jaydolai@zohomail.in`
3. Verify Power BI trial active (59 days) in profile panel

📸 screenshot: `screenshots/01_fabric_home.png` ✅

---

### 1.2 Create Workspace
1. Click **New workspace**
2. Name: `globalwatch-dev`
3. Description: `GlobalWatch — World Air Quality Intelligence Platform. Batch + Real-Time lakehouse on Microsoft Fabric.`
4. Workspace type: **Fabric Trial**
5. Click **Apply**

📸 screenshot: `screenshots/02_workspace_created.png` ✅

---

### 1.3 Create Three Lakehouses
Repeat **New item → Lakehouse** three times:

| # | Name | Purpose |
|---|---|---|
| 1 | `bronze_globalwatch` | Raw landing zone |
| 2 | `silver_globalwatch` | Cleaned, DQ-checked data |
| 3 | `gold_globalwatch` | Star schema serving layer |

📸 screenshot: `screenshots/03_three_lakehouses.png` ✅

---

### 1.4 Create Fabric Warehouse
1. **New item → Warehouse**
2. Name: `globalwatch_warehouse`

📸 screenshot: `screenshots/04_warehouse_created.png` ✅

---

### 1.5 Create Eventhouse + KQL Database
1. **New item → Eventhouse**
2. Name: `globalwatch_eventhouse`
3. KQL Database auto-created inside: `globalwatch_eventhouse`

### 1.6 Create Eventstream
1. **New item → Eventstream**
2. Name: `es_openaq_realtime`

### 1.7 Create Environment
1. **New item → Environment**
2. Name: `globalwatch-env`
3. Add Spark properties:
   - `spark.openaq.api.key` = your OpenAQ API key
   - `spark.eventhub.connection.string` = full Event Hub connection string
   - `spark.eventhub.name` = `esehpna8o0d4gaxspkjtsh_eh`
4. Click **Publish**

---

## Phase 2 — PySpark Medallion Notebooks

### 2.1 Bronze Ingestion — OpenAQ API
- Notebook: `01_bronze_ingest_openaq`
- Attached lakehouse: `bronze_globalwatch`
- Environment: `globalwatch-env`
- Output: `raw_openaq_readings` Delta table (703 rows, 9 countries)
- Key features: watermark pattern, mergeSchema, AQE

📸 screenshot: `screenshots/10_bronze_notebook_run.png` ✅

---

### 2.2 Silver Transform
- Notebook: `04_silver_transform`
- Attached lakehouse: `silver_globalwatch`
- Output: `silver_readings` Delta table (344 rows)
- DQ rules: empty parameter, unknown pollutant, invalid value, dedup
- AQI categorization (WHO PM2.5 thresholds)
- Partitioned by `country_code + year_month`

📸 screenshot: `screenshots/11_silver_transform_run.png` ✅
📸 screenshot: `screenshots/12_spark_ui_aqe.png` ✅

---

### 2.3 Gold Star Schema
- Notebook: `05_gold_star_schema`
- Attached lakehouse: `gold_globalwatch`
- Output: 5 tables (dim_date, dim_pollutant, dim_country, dim_station, fact_readings)
- dim_station: SCD Type 2 via Delta MERGE
- dim_country: SCD Type 1
- fact_readings: V-Order ON + Z-Order on location_id + reading_ts

📸 screenshot: `screenshots/13_gold_notebook_scd2_merge.png` ✅
📸 screenshot: `screenshots/14_gold_delta_table_detail.png` ✅

**Key findings:**
- India PM2.5: 100% WHO exceedance (avg 175.83 µg/m³ vs guideline 15)
- Mongolia PM2.5: 100% WHO exceedance (avg 131.4 µg/m³)

---

## Phase 3 — KQL Database Schema

### 3.1 Create Tables
Open `globalwatch_eventhouse` → **Query with code** → run scripts from `kql/schema_create.kql`:

1. Create `raw_readings` table
2. Create `silver_readings` table
3. Create `TransformRawReadings()` function
4. Attach update policy (auto-transform on ingestion)
5. Set retention: raw=30 days, silver=365 days

📸 screenshot: `screenshots/16_kql_database_created.png` ✅

---

## Phase 4 — Eventstream Configuration

### 4.1 Configure es_openaq_realtime
1. Open `es_openaq_realtime`
2. **Use custom endpoint** → name: `openaq-custom-source`
3. **Add destination → Eventhouse**:
   - Mode: Event processing before ingestion
   - Eventhouse: `globalwatch_eventhouse`
   - KQL Database: `globalwatch_eventhouse`
   - Table: `raw_readings`
   - Format: JSON
4. Click **Save → Publish**

📸 screenshot: `screenshots/17_eventstream_configured.png` ✅

### 4.2 Stream Data via Notebook
- Notebook: `07_streaming_openaq_eventstream`
- Installs `azure-eventhub` package
- Fetches OpenAQ locations → sends to Event Hub → Eventstream → KQL
- 315 events streamed, 315 rows in silver_readings

📸 screenshot: `screenshots/18_eventstream_live_data.png` ✅

---

## Phase 5 — Data Activator (Remaining)

### 5.1 Create Reflex
1. **New item → Reflex**
2. Name: `act_pm25_hazard_alert`
3. Source: `silver_readings` KQL table
4. Trigger: `value > 150` AND `parameter == "pm25"`
5. Action: Teams notification

📸 screenshot: `screenshots/20_activator_rule_configured.png` ⬜

---

## Phase 6 — Direct Lake Power BI (Remaining)

### 6.1 Create Semantic Model
1. Open `gold_globalwatch` lakehouse
2. **New semantic model**
3. Name: `GlobalWatch_Model`
4. Select: fact_readings, dim_station, dim_country, dim_date, dim_pollutant
5. Mode: **Direct Lake** (auto-set)

📸 screenshot: `screenshots/22_semantic_model_direct_lake.png` ⬜

---

## Phase 7 — Fabric Data Agent (Remaining)

### 7.1 Create Data Agent
1. Open `globalwatch_eventhouse` → **Data Agent**
2. Connect to `gold_globalwatch` lakehouse + KQL Database
3. Configure NL query capabilities

📸 screenshot: `screenshots/26_data_agent_configured.png` ⬜

---

## Phase 8 — CI/CD (Remaining)

### 8.1 Git Integration
1. Workspace → Settings → **Git integration**
2. Connect to Azure DevOps repo: `demonjd2026-afk/globalwatch-fabric`
3. Branch: `main`

📸 screenshot: `screenshots/28_git_synced.png` ⬜

---

## Screenshots Checklist

| # | File | Status |
|---|---|---|
| 01 | `01_fabric_home.png` | ✅ |
| 02 | `02_workspace_created.png` | ✅ |
| 03 | `03_three_lakehouses.png` | ✅ |
| 04 | `04_warehouse_created.png` | ✅ |
| 10 | `10_bronze_notebook_run.png` | ✅ |
| 11 | `11_silver_transform_run.png` | ✅ |
| 12 | `12_spark_ui_aqe.png` | ✅ |
| 13 | `13_gold_notebook_scd2_merge.png` | ✅ |
| 14 | `14_gold_delta_table_detail.png` | ✅ |
| 16 | `16_kql_database_created.png` | ✅ |
| 17 | `17_eventstream_configured.png` | ✅ |
| 18 | `18_eventstream_live_data.png` | ✅ |
| 20 | `20_activator_rule_configured.png` | ⬜ |
| 22 | `22_semantic_model_direct_lake.png` | ⬜ |
| 26 | `26_data_agent_configured.png` | ⬜ |
| 28 | `28_git_synced.png` | ⬜ |

---

*Update ⬜ → ✅ as each screenshot is taken and committed to `screenshots/` folder.*
