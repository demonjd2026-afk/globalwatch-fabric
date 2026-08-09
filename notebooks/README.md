# Notebooks

Seven PySpark notebooks that make up the GlobalWatch pipeline on Microsoft Fabric. Each is self-contained, heavily commented, and written to run both interactively and under a Data Factory pipeline.

---

## Execution order

```
BATCH  —  pl_batch_globalwatch, daily 02:00 IST
   01_bronze_ingest_openaq
            ↓
   04_silver_transform
            ↓
   05_gold_star_schema
            ↓
   06_ml_aqi_prediction
            ↓
   07_data_agent_simulation
            ↓
   08_export_to_streamlit          (publishes Gold to GitHub)

REAL-TIME  —  pl_realtime_globalwatch, hourly
   07_streaming_openaq_eventstream (also writes kql_stats.json to GitHub)
```

| Notebook | Layer | Attach lakehouse | Writes |
|---|---|---|---|
| `01_bronze_ingest_openaq` | Bronze | `bronze_globalwatch` | `raw_openaq_readings`, `watermark_control` |
| `04_silver_transform` | Silver | `silver_globalwatch` | `silver_readings` |
| `05_gold_star_schema` | Gold | `gold_globalwatch` | 5 star-schema tables |
| `06_ml_aqi_prediction` | Gold | `silver_globalwatch` + `gold_globalwatch` | `fact_aqi_predictions`, MLflow model |
| `07_streaming_openaq_eventstream` | Real-time | none required | Event Hub → Eventstream → KQL; `kql_stats.json` |
| `07_data_agent_simulation` | Gold | `gold_globalwatch` | none (demonstration only) |
| `08_export_to_streamlit` | Gold | `gold_globalwatch` | 4 JSONL files on GitHub |

All notebooks use the `globalwatch-env` environment, which holds the API key, Event Hub connection string, Event Hub name and GitHub token as Spark properties — no secret appears in notebook source.

---

## 01_bronze_ingest_openaq.ipynb — 5 cells

**Layer:** Bronze · **Source:** OpenAQ v3 (`api.openaq.org/v3`)

| Cell | Purpose |
|---|---|
| 1 | Spark config (AQE: enabled, coalescePartitions, skewJoin), imports, DB context, API key from environment |
| 2 | Create `watermark_control` Delta table; seed it if empty |
| 3 | OpenAQ API helper functions + connectivity test |
| 4 | Parallel fetch across 70 country codes → Bronze Delta write with `mergeSchema=True` |
| 5 | Row-count assertion, DQ summary by country/parameter, watermark update |

**Ingestion approach (cell 4).** 70 ISO country codes spread across Asia (25), Europe (20), the Americas (15) and Africa (10) are passed to the API's country filter. A `ThreadPoolExecutor(max_workers=10)` fans out the location fetches, with a `Semaphore` holding total throughput to OpenAQ's free-tier 60 req/min. The `/locations/{id}/latest` endpoint is used rather than per-sensor measurement history — one call per location instead of one per sensor, which is what makes this breadth affordable inside the free tier.

**Design decisions**
- **Append-only Bronze** — raw payloads are never modified, so the full audit trail survives any downstream change.
- **`mergeSchema=True`** — OpenAQ occasionally adds fields; the write extends the schema instead of failing.
- **Watermark pattern** — `last_loaded_date` / `last_loaded_ts` per source, updated only after a successful write, so a mid-run failure resumes cleanly.
- **Partition by `ingestion_date`** — makes retention cleanup a directory drop.

**Output**

```
bronze_globalwatch.dbo.raw_openaq_readings
  location_id, location_name, city, country_code, country_name,
  latitude, longitude, parameter, value, unit,
  reading_ts, ingestion_ts, source_system,
  ingestion_date (partition), year_month
```

📸 `screenshots/10_bronze_notebook_run.png`

---

## 04_silver_transform.ipynb — 5 cells

**Layer:** Silver · **Source:** `bronze_globalwatch.dbo.raw_openaq_readings` (cross-lakehouse 3-part name)

| Cell | Purpose |
|---|---|
| 1 | Config — AQE + `autoBroadcastJoinThreshold = 50MB`, cross-lakehouse connectivity test |
| 2 | Read Bronze, apply the four DQ rules, log rows dropped by each |
| 3 | Enrich — AQI category, unit normalisation, reporting columns |
| 4 | Write Silver Delta partitioned by `country_code` + `year_month`, print partition summary |
| 5 | Validation — row count, null check, parameter distribution, country coverage |

**Data quality rules**

| # | Rule | Filter | Drops |
|---|---|---|---|
| 1 | Empty parameter | `parameter != ""` | Unclassified sensors |
| 2 | Unknown pollutant | `parameter IN (pm25, pm10, no2, co, o3)` | Non-criteria pollutants |
| 3 | Invalid value | `0 < value < 10000` | Sensor malfunctions |
| 4 | Deduplication | `dropDuplicates(location_id, parameter, reading_ts)` | Duplicate API records |

**AQI categories** (WHO 2021 PM2.5 thresholds; other pollutants get `N/A`)

| Category | PM2.5 range |
|---|---|
| Good | 0 – 12.0 |
| Moderate | 12.1 – 35.4 |
| Unhealthy for Sensitive | 35.5 – 55.4 |
| Unhealthy | 55.5 – 150.4 |
| Hazardous | > 150.4 |

V-Order is deliberately **off** here — Silver is an intermediate layer, not a Direct Lake target.

**Output**

```
silver_globalwatch.dbo.silver_readings
  location_id, location_name, city, country_code, country_name,
  latitude, longitude, parameter, value, unit, aqi_category,
  reading_ts, reading_date, reading_hour, year_month,
  is_recent, ingestion_ts, silver_processed_ts, source_system
  partitions: country_code / year_month
```

📸 `screenshots/11_silver_transform_run.png` · `screenshots/12_spark_ui_aqe.png`

---

## 05_gold_star_schema.ipynb — 7 cells

**Layer:** Gold · **Source:** `silver_globalwatch.dbo.silver_readings`

| Cell | Purpose |
|---|---|
| 1 | Config + Silver cross-lakehouse test; AQE, broadcast threshold, V-Order confirmation |
| 2 | `dim_date` — Spark `sequence()` date spine, Type 0 |
| 3 | `dim_pollutant` — static WHO reference, Type 0 |
| 4 | `dim_country` — SCD Type 1, continent mapping, CRC32 surrogate key |
| 5 | `dim_station` — SCD Type 2 via Delta MERGE with MD5 `station_hash` |
| 6 | `fact_readings` — broadcast joins, V-Order write, `OPTIMIZE … ZORDER BY` |
| 7 | Validation — row counts, nulls, WHO exceedances, SCD2 integrity |

**Tables built** (row counts as of the 09 Aug 2026 snapshot)

| Table | SCD | Rows | Description |
|---|---|---|---|
| `dim_date` | Type 0 | 5,844 | Pre-generated spine 2015-01-01 → 2030-12-31 |
| `dim_pollutant` | Type 0 | 5 | WHO reference — pm25, pm10, no2, co, o3 |
| `dim_country` | Type 1 | 23 | Continent mapping; World Bank columns reserved |
| `dim_station` | Type 2 | 308 active | `active_flag`, `effective_start` / `effective_end`, `station_hash` |
| `fact_readings` | — | 894 | Long fact; WHO exceedance flag per pollutant |

**SCD Type 2 MERGE**

```
Step 1 — expire changed rows
  MATCH  location_id matches AND active_flag = true
         AND station_hash <> source.station_hash
  SET    active_flag = false, effective_end = now()

Step 2 — insert new versions
  Anti-join source against the active target set
  INSERT with active_flag = true, effective_end = 9999-12-31
```

Comparing a single MD5 hash rather than every attribute keeps change detection O(1) in table width.

**Optimisations applied**
- AQE — all three settings
- `autoBroadcastJoinThreshold = 50MB` plus explicit `F.broadcast()` on every dimension join
- V-Order on all Gold writes (mandatory for Direct Lake)
- `OPTIMIZE fact_readings ZORDER BY (location_id, reading_ts)`
- CRC32 surrogate keys — deterministic, no distributed sequence generator

**Cell 7 is the definition of done.** It asserts fixed row counts for the static dimensions, checks nulls, reports WHO exceedances and verifies SCD2 integrity. If an assertion fails, the pipeline must not publish a stale Gold to the semantic model.

📸 `screenshots/13_gold_notebook_scd2_merge.png` · `screenshots/14_gold_delta_table_detail.png` · `screenshots/35_gold_table_counts_post_pipeline.png`

---

## 06_ml_aqi_prediction.ipynb — 7 cells

**Layer:** Gold enrichment · **Frameworks:** Spark MLlib + MLflow

| Cell | Purpose |
|---|---|
| 1 | Load `silver_readings`, validate |
| 2 | EDA — parameter distribution, AQI distribution, PM2.5 descriptive stats |
| 3 | Feature engineering — pivot long → wide, null-fill, WHO threshold label |
| 4 | Train Random Forest in a Spark ML Pipeline; log to MLflow; register the model |
| 5 | Feature importance analysis |
| 6 | Load the registered model and score Gold `fact_readings` |
| 7 | Write `fact_aqi_predictions` with readable class labels |

**Why the pivot (cell 3).** Silver is long — one row per parameter. The classifier needs one row per station+timestamp with each pollutant as a feature column, so `groupBy(...).pivot("parameter", [...]).agg(first("value"))` reshapes it, and nulls become 0 because not every station measures every pollutant.

**Model**

| Property | Value |
|---|---|
| Algorithm | `RandomForestClassifier`, `numTrees=100`, `maxDepth=5` |
| Features | `pm25`, `pm10`, `no2`, `o3`, `co` |
| Label | 5-class WHO PM2.5 banding, 0 = Good … 4 = Hazardous |
| Split | 80/20, `seed=42` |
| Registry | `globalwatch_aqi_classifier` v1 |
| Test accuracy | 96.15% |
| Feature importance | PM2.5 76.32%, PM10 12.91%, remainder across NO₂/O₃/CO |

**Why Random Forest** — tolerant of class imbalance on a small dataset, no feature scaling needed across very different pollutant ranges, native Gini feature importance, and available in Spark MLlib without installing anything.

**Output:** `gold_globalwatch.dbo.fact_aqi_predictions` — 245 rows over 206 stations; Good 130, Moderate 87, Unhealthy 20, Hazardous 8. Written with `mode=overwrite` so a re-run is idempotent.

📸 `screenshots/26_mlflow_experiment_run.png` · `screenshots/27_ml_predictions_output.png` · `screenshots/25_ml_aqi_predicted_classes.png`

---

## 07_streaming_openaq_eventstream.ipynb — 6 cells

**Layer:** Real-time producer · **Destination:** `es_openaq_realtime` Eventstream → KQL `raw_readings`

| Cell | Purpose |
|---|---|
| 0 | `%pip install azure-eventhub` — **commented out**; the library is provided by `globalwatch-env` |
| 1 | Config — API key, Event Hub connection string and name from Spark properties |
| 2 | OpenAQ API helper functions |
| 3 | Parallel fetch across 70 countries → one Event Hub batch per country |
| 4 | KQL verification — skipped in pipeline mode, run interactively |
| 5 | Export run statistics to `streamlit/data/kql_stats.json` on GitHub |

**Two pipeline-mode constraints resolved here**

| Symptom | Cause | Fix |
|---|---|---|
| `%pip magic command is disabled` | `%pip` works interactively but not in pipeline runs | `azure-eventhub` added to `globalwatch-env` as a PyPI library; the cell is commented out |
| KQL verification fails on network resolution | Outbound KQL HTTP is blocked in the trial pipeline sandbox | Cell 4 short-circuits with an explanatory message; run it interactively to check counts |

Cell 5 is defensive by design — it re-initialises `events_sent`, `skipped` and `errors` to 0 if they were lost between cells, so the stats export never crashes the run that produced the data.

**Latest run:** 5,056 events dispatched, 0 skipped, 0 errors, status *Live*.

📸 `screenshots/18_eventstream_live_data.png` · `screenshots/33_kql_raw_readings_count.png` · `screenshots/32_realtime_pipeline_success.png`

---

## 07_data_agent_simulation.ipynb — 2 cells

**Purpose:** demonstrate the NL → SQL → result pattern that a native Fabric Data Agent uses internally. The managed Data Agent requires an F64+ SKU, which this trial capacity does not have.

| Cell | Purpose |
|---|---|
| 1 | Three natural language questions with their SQL equivalents, printed side by side |
| 2 | Execute each query against Gold and show the results |

The three questions: which country has the highest average PM2.5, how many hazardous readings exist, and which stations exceed WHO guidelines. It is a demonstration of the pattern, not a production capability — the actual user-facing NL experience is the Streamlit AI Agent page.

📸 `screenshots/28_data_agent_nl_to_sql.png` · `screenshots/29_data_agent_query_results.png`

---

## 08_export_to_streamlit.ipynb — 4 cells

**Purpose:** publish Gold snapshots to GitHub so the public Streamlit app stays in step with the pipeline.

| Cell | Purpose |
|---|---|
| 1 | GitHub config — token from Spark properties, repo/branch/path constants |
| 2 | Read four Gold tables, serialise each to newline-delimited JSON |
| 3 | Push to GitHub via the Contents API — GET the file SHA, then PUT with it |
| 4 | Verify each file is live and report its size |

**Why the SHA round trip:** the GitHub Contents API requires the current blob SHA to update an existing file. The helper does a GET first; if the file exists it includes the SHA and the commit reads "Updated", otherwise it reads "Created".

**Exports**

| File | Source | Rows |
|---|---|---|
| `fact_readings.json` | `gold_globalwatch.dbo.fact_readings` (dashboard column subset) | 894 |
| `dim_country.json` | `gold_globalwatch.dbo.dim_country` | 23 |
| `dim_station.json` | `gold_globalwatch.dbo.dim_station` where `active_flag = true` | 308 |
| `fact_aqi_predictions.json` | `gold_globalwatch.dbo.fact_aqi_predictions` | 245 |

All four are JSONL — one JSON object per line — so the app parses them line-by-line, not with a whole-file `json.load`.

---

## Prerequisites

1. Fabric workspace `globalwatch-dev` containing `bronze_globalwatch`, `silver_globalwatch`, `gold_globalwatch`, `globalwatch_warehouse`, `globalwatch_eventhouse` and `es_openaq_realtime`.

2. Environment `globalwatch-env` published with these Spark properties:

   ```
   spark.openaq.api.key              = <OpenAQ v3 API key>
   spark.eventhub.connection.string  = <Eventstream custom endpoint connection string>
   spark.eventhub.name               = <Event Hub name>
   spark.github.token                = <GitHub PAT with contents:write>
   ```

   …and `azure-eventhub` added as a PyPI library. Get a free OpenAQ key at <https://explore.openaq.org/register>.

3. Each notebook attached to its lakehouse in the Explorer panel, with **Environment** set to `globalwatch-env`.

---

## Current results

| Layer | Table | Rows | Countries |
|---|---|---|---|
| Silver | `silver_readings` | variable per run | 23 |
| Gold | `fact_readings` | 894 | 23 |
| Gold | `dim_station` | 308 active | 23 |
| Gold | `dim_country` | 23 | — |
| Gold | `dim_date` | 5,844 | — |
| Gold | `dim_pollutant` | 5 | — |
| Gold | `fact_aqi_predictions` | 245 | — |
| KQL | events per hourly run | 5,056 | — |

Counts reflect the 09 Aug 2026 export and grow with every run — the Streamlit app always shows the latest.
