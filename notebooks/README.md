# Notebooks

PySpark notebooks for the GlobalWatch medallion pipeline on Microsoft Fabric.
Each notebook is self-contained with inline comments explaining every design decision.

---

## Execution Order

```
01_bronze_ingest_openaq.ipynb
            ↓
    (reads Bronze → writes Silver)
            ↓
04_silver_transform.ipynb
            ↓
    (reads Silver → writes Gold)
            ↓
05_gold_star_schema.ipynb
```

---

## 01_bronze_ingest_openaq.ipynb

**Layer:** Bronze — Raw Landing Zone
**Lakehouse:** `bronze_globalwatch`
**Source:** OpenAQ v3 API (`api.openaq.org/v3`)

### What it does
- Configures Spark with AQE enabled (adaptive query execution)
- Creates a `watermark_control` Delta table to track last loaded date per source
- Calls OpenAQ `/locations` endpoint to discover air quality stations globally
- Calls `/locations/{id}/sensors` to get sensor metadata (parameter + unit)
- Calls `/sensors/{id}/measurements/hourly` for latest readings per sensor
- Writes raw records to `raw_openaq_readings` Delta table, partitioned by `ingestion_date`
- Updates watermark after successful write
- Validates row count with assertion

### Key cells
| Cell | Purpose |
|---|---|
| Cell 1 | Spark config (AQE) + DB context + API key from environment |
| Cell 2 | Create watermark_control Delta table |
| Cell 3 | OpenAQ API functions + connectivity test |
| Cell 4 | Fetch loop + Bronze Delta write (mergeSchema=True) |
| Cell 5 | Row count validation + watermark update |

### Spark optimizations applied
- `spark.sql.adaptive.enabled = true` — AQE master switch
- `spark.sql.adaptive.coalescePartitions.enabled = true` — merge small shuffle partitions
- `spark.sql.adaptive.skewJoin.enabled = true` — handle high-volume city stations
- `mergeSchema=True` on Delta write — handles OpenAQ API schema evolution

### Design decisions
- **Append-only Bronze:** raw data is never modified — full audit trail preserved
- **mergeSchema:** OpenAQ adds new fields occasionally — we capture them without pipeline failure
- **Watermark pattern:** incremental ingestion — only pulls records newer than last run
- **Rate limit handling:** 1s delay between sensor calls + 0.5s between locations — avoids 429 errors

### Output table
```
bronze_globalwatch.dbo.raw_openaq_readings
Columns: location_id, location_name, city, country_code, country_name,
         latitude, longitude, parameter, value, unit,
         reading_ts, ingestion_ts, source_system,
         ingestion_date (partition), year_month
```

---

## 04_silver_transform.ipynb

**Layer:** Silver — Cleaned and Conformed
**Lakehouse:** `silver_globalwatch`
**Source:** `bronze_globalwatch.dbo.raw_openaq_readings`

### What it does
- Reads from Bronze via cross-lakehouse 3-part table reference
- Applies 4 data quality rules (empty parameter, unknown pollutant, invalid value, duplicate)
- Adds AQI category based on WHO PM2.5 thresholds
- Normalizes unit labels (µg/m³ → ug/m3)
- Adds reporting columns (reading_date, reading_hour, year_month, is_recent)
- Writes to Silver Delta table partitioned by `country_code` + `year_month`
- Validates with null checks and parameter distribution report

### Key cells
| Cell | Purpose |
|---|---|
| Cell 1 | Config + cross-lakehouse connectivity test |
| Cell 2 | Read Bronze + apply DQ rules (4 filters) |
| Cell 3 | Enrich + AQI categorization + schema standardization |
| Cell 4 | Write to Silver Delta (partitioned by country_code + year_month) |
| Cell 5 | Validate: null check + parameter distribution + country coverage |

### Data quality rules
| Rule | Filter | Rows dropped |
|---|---|---|
| Empty parameter | `parameter != ""` | Unclassified sensors |
| Unknown pollutant | `parameter IN (pm25, pm10, no2, co, o3)` | Non-criteria pollutants |
| Invalid value | `value > 0 AND value < 10000` | Sensor malfunctions |
| Deduplication | `dropDuplicates(location_id, parameter, reading_ts)` | Duplicate API records |

### AQI categories (WHO 2021 PM2.5 guidelines)
| Category | PM2.5 Range |
|---|---|
| Good | 0–12.0 µg/m³ |
| Moderate | 12.1–35.4 µg/m³ |
| Unhealthy for Sensitive | 35.5–55.4 µg/m³ |
| Unhealthy | 55.5–150.4 µg/m³ |
| Hazardous | >150.4 µg/m³ |

### Spark optimizations applied
- AQE (inherited from session config)
- `autoBroadcastJoinThreshold = 50MB`
- Partitioned by `country_code + year_month` — enables country-level partition pruning

### Output table
```
silver_globalwatch.dbo.silver_readings
Columns: location_id, location_name, city, country_code, country_name,
         latitude, longitude, parameter, value, unit, aqi_category,
         reading_ts, reading_date, reading_hour, year_month,
         is_recent, ingestion_ts, silver_processed_ts, source_system
Partitions: country_code / year_month
```

---

## 05_gold_star_schema.ipynb

**Layer:** Gold — Star Schema Serving Layer
**Lakehouse:** `gold_globalwatch`
**Source:** `silver_globalwatch.dbo.silver_readings`

### What it does
- Reads Silver via cross-lakehouse reference
- Builds 4 dimension tables + 1 fact table
- Applies SCD Type 1 on `dim_country` (overwrite)
- Applies SCD Type 2 on `dim_station` via Delta MERGE
- Writes `fact_readings` with V-Order enabled for Direct Lake
- Applies Z-Order on `fact_readings` for query optimization
- Validates with row counts, null checks, WHO exceedance report, SCD2 integrity check

### Tables built

| Table | SCD | Rows | Description |
|---|---|---|---|
| `dim_date` | Type 0 | 5,844 | Pre-generated date spine 2015–2030 |
| `dim_pollutant` | Type 0 | 5 | WHO reference — pm25, pm10, no2, co, o3 |
| `dim_country` | Type 1 | 9 | Countries with continent mapping |
| `dim_station` | Type 2 | 121 | Stations with active_flag + effective_start/end |
| `fact_readings` | — | 344 | Central fact — pollutant readings + WHO exceedance |

### Key cells
| Cell | Purpose |
|---|---|
| Cell 1 | Config + Silver cross-lakehouse test |
| Cell 2 | dim_date — Spark sequence function, date spine |
| Cell 3 | dim_pollutant — static WHO reference |
| Cell 4 | dim_country — SCD Type 1, continent mapping, CRC32 SK |
| Cell 5 | dim_station — SCD Type 2, Delta MERGE, MD5 hash |
| Cell 6 | fact_readings — broadcast joins, V-Order, Z-Order |
| Cell 7 | Validation — row counts, nulls, WHO exceedances, SCD2 check |

### SCD Type 2 — dim_station MERGE logic
```
Step 1 — Expire changed rows:
  MATCH: location_id = source.location_id
         AND active_flag = true
         AND station_hash <> source.station_hash
  ACTION: active_flag = false, effective_end = now()

Step 2 — Insert new active rows:
  Anti-join source against active target
  INSERT unmatched rows with active_flag=true, effective_end=9999-12-31
```

### Spark optimizations applied
- AQE — all three configs enabled
- `autoBroadcastJoinThreshold = 50MB` — dim_country (9 rows), dim_pollutant (5 rows), dim_station (121 rows) all broadcast
- `F.broadcast()` explicit hints on all dimension joins
- V-Order — enabled by default in Fabric Spark, applied on all Gold Delta writes
- Z-Order — `OPTIMIZE fact_readings ZORDER BY (location_id, reading_ts)`
- CRC32 surrogate keys — deterministic, no distributed sequence generator

### Key finding from current dataset
```
India   PM2.5: 100% WHO exceedance — avg 175.83 µg/m³ (guideline: 15)
India   NO2:   100% WHO exceedance — avg 81.02 µg/m³  (guideline: 10)
Mongolia PM2.5: 100% WHO exceedance — avg 131.4 µg/m³  (guideline: 15)
```

### Output tables
```
gold_globalwatch.dbo.dim_date
gold_globalwatch.dbo.dim_pollutant
gold_globalwatch.dbo.dim_country
gold_globalwatch.dbo.dim_station     ← SCD Type 2
gold_globalwatch.dbo.fact_readings   ← V-Order + Z-Order, partitioned by year_month
```

---

## Prerequisites

Before running any notebook:

1. Fabric workspace `globalwatch-dev` must exist with these items:
   - `bronze_globalwatch` Lakehouse
   - `silver_globalwatch` Lakehouse
   - `gold_globalwatch` Lakehouse

2. Fabric Environment `globalwatch-env` must have this Spark property:
   ```
   spark.openaq.api.key = <your OpenAQ v3 API key>
   ```
   Get a free key at: https://explore.openaq.org/register

3. Each notebook must be attached to its corresponding lakehouse in the Explorer panel:
   - `01_bronze_ingest_openaq` → attach `bronze_globalwatch`
   - `04_silver_transform` → attach `silver_globalwatch`
   - `05_gold_star_schema` → attach `gold_globalwatch`

---

## Results Summary

| Layer | Table | Rows | Countries |
|---|---|---|---|
| Bronze | raw_openaq_readings | 703 | 9 |
| Silver | silver_readings | 344 | 9 |
| Gold | fact_readings | 344 | 9 |
| Gold | dim_station | 121 stations | — |
| Gold | dim_date | 5,844 dates | — |
