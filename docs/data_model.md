# Data Model

All row counts reflect the Gold layer as of the **09 Aug 2026** export in [`streamlit/data/`](../streamlit/data/). They grow with every pipeline run.

---

## Gold layer — star schema

`fact_readings` is a long fact table: one row per station × pollutant × reading. Keeping the shape long (a `parameter` column plus a `value` column, rather than `pm25_value`/`pm10_value`/… columns) means adding a sixth pollutant requires no schema change, and lets `dim_pollutant` own the WHO guideline per parameter instead of hard-coding thresholds in the fact.

```
                    ┌─────────────┐
                    │  dim_date   │   Type 0 · 5,844 rows
                    │─────────────│
                    │ date_key PK │
                    │ full_date   │
                    │ year        │
                    │ month       │
                    │ month_name  │
                    │ quarter     │
                    │ day_of_week │
                    │ day_name    │
                    │ is_weekend  │
                    │ year_month  │
                    └──────┬──────┘
                           │ date_key
                           │
┌──────────────┐    ┌──────▼──────────────────────┐    ┌─────────────────┐
│ dim_station  │    │       fact_readings          │    │  dim_pollutant  │
│──────────────│    │──────────────────────────────│    │─────────────────│
│ station_sk PK│◄───│ station_sk            FK     │───►│ pollutant_sk PK │
│ location_id  │    │ country_sk            FK     │    │ pollutant_code  │
│ location_name│    │ pollutant_sk          FK     │    │ pollutant_name  │
│ city         │    │ date_key              FK     │    │ standard_unit   │
│ country_code │    │ location_id                  │    │ who_guideline   │
│ latitude     │    │ parameter                    │    │ description     │
│ longitude    │    │ value                        │    └─────────────────┘
│ station_hash │    │ unit                         │       Type 0 · 5 rows
│ active_flag  │    │ aqi_category                 │
│ effective_start   │ exceeds_who_guideline        │    ┌─────────────────┐
│ effective_end│    │ reading_ts                   │    │   dim_country   │
└──────────────┘    │ reading_date                 │◄───│─────────────────│
 Type 2 · 308 active│ reading_hour                 │    │ country_sk   PK │
                    │ year_month                   │    │ country_code    │
┌──────────────────┐│ is_recent                    │    │ country_name    │
│fact_aqi_predictions latitude, longitude          │    │ continent       │
│──────────────────││ continent                    │    │ gdp_per_capita  │
│ location_id      ││ source_system                │    │ health_exp_pct  │
│ country_sk       ││ ingestion_ts                 │    │ population      │
│ pollutant_sk     │└──────────────────────────────┘    │ scd_updated_ts  │
│ date_key         │        894 rows                    └─────────────────┘
│ pm25             │  V-Order · Z-Order(location_id,      Type 1 · 23 rows
│ prediction       │  reading_ts) · part: year_month
│ predicted_aqi_class
└──────────────────┘
   245 rows · 206 stations
```

`dim_country.gdp_per_capita`, `health_exp_pct` and `population` are reserved for World Bank enrichment and are currently unpopulated — the columns exist so adding the Dataflow Gen2 ingestion is an additive change.

---

## Current contents

| Table | Rows | Notes |
|---|---|---|
| `fact_readings` | 894 | 303 distinct stations, 23 countries, 30.6% breach a WHO guideline |
| `dim_station` | 308 active | SCD2; every `location_id` has exactly one `active_flag = true` row |
| `dim_country` | 23 | Asia 6, South America 5, Europe 4, Other 4, Africa 2, North America 1, Oceania 1 |
| `dim_date` | 5,844 | 2015-01-01 → 2030-12-31, fixed |
| `dim_pollutant` | 5 | pm25, pm10, no2, co, o3 |
| `fact_aqi_predictions` | 245 | One row per scored PM2.5 reading, 206 distinct stations |

Pollutant mix in `fact_readings`: PM2.5 245 · NO₂ 201 · PM10 189 · O₃ 168 · CO 91.
Readings by continent: Europe 314 · South America 192 · Asia 181 · Other 89 · North America 67 · Africa 42 · Oceania 9.

---

## SCD implementation

### `dim_station` — SCD Type 2

Tracks changes to station name and city assignment over time.

| Column | Type | Description |
|---|---|---|
| `station_sk` | INT | CRC32 surrogate key, deterministic from `location_id` |
| `location_id` | INT | OpenAQ natural key |
| `station_hash` | STRING | MD5 of location_id + name + city + country — the change detector |
| `active_flag` | BOOLEAN | `true` = current version |
| `effective_start` | TIMESTAMP | When this version became active |
| `effective_end` | TIMESTAMP | `9999-12-31` while active; expiry timestamp once superseded |

**MERGE logic**

```
Step 1 — expire changed rows
  MATCH  target.location_id  = source.location_id
     AND target.active_flag  = true
     AND target.station_hash <> source.station_hash
  SET    active_flag = false
         effective_end = current_timestamp()

Step 2 — insert new versions
  Anti-join source against the active target set
  INSERT unmatched rows with active_flag = true, effective_end = 9999-12-31
```

Comparing one hash rather than every attribute keeps the change detection O(1) in table width — adding a column to the dimension does not change the comparison cost.

### `dim_country` — SCD Type 1

Overwrites in place. Country GDP and health expenditure are annual reference values; analysts want the current enrichment, and historical series are better sourced from the World Bank directly than reconstructed from dimension history.

### `dim_date` and `dim_pollutant` — Type 0

`dim_date` is a pre-generated spine from 2015-01-01 to 2030-12-31 — Power BI time intelligence requires a complete, gap-free calendar. `dim_pollutant` holds the five criteria pollutants with WHO 2021 annual guidelines, updated manually if WHO revises them.

---

## Surrogate key strategy

All surrogate keys are CRC32 hashes of the natural key:

```python
F.crc32(F.col("country_code")).cast(IntegerType())
F.crc32(F.col("location_id").cast("string")).cast(IntegerType())
```

**Why CRC32 rather than a sequence generator?**
- Deterministic — the same natural key yields the same surrogate key on every run and every executor, so re-running a load is idempotent.
- No distributed counter, so no coordination overhead or shuffle to assign keys.
- Collision probability is negligible at dimension cardinality.

Trade-off: the keys are not monotonically increasing, so they carry no insertion-order meaning — which is fine, since ordering is carried by `effective_start` and `date_key`.

---

## Partitioning strategy

| Table | Partition columns | Rationale |
|---|---|---|
| `bronze.raw_openaq_readings` | `ingestion_date` | Append-only; date partitions make retention cleanup trivial |
| `silver.silver_readings` | `country_code`, `year_month` | Country-scoped time-range queries are the primary access pattern |
| `gold.fact_readings` | `year_month` | Reporting always filters by period; station filtering is served by Z-Order |

---

## AQI categories (WHO 2021)

Applied to PM2.5 readings only. Other pollutants carry `aqi_category = 'N/A'` — which is why 649 of 894 rows show N/A.

| Category | PM2.5 range (µg/m³) | Health implication |
|---|---|---|
| Good | 0 – 12.0 | Air quality satisfactory |
| Moderate | 12.1 – 35.4 | Acceptable; sensitive groups may be affected |
| Unhealthy for Sensitive | 35.5 – 55.4 | Sensitive groups may experience health effects |
| Unhealthy | 55.5 – 150.4 | Everyone may experience health effects |
| Hazardous | > 150.4 | Health alert — serious effects for the whole population |

Current distribution: N/A 649 · Good 137 · Moderate 72 · Unhealthy 17 · Hazardous 11 · Unhealthy for Sensitive 8.

---

## WHO exceedance flag

Applied to **all** pollutants in `fact_readings`, using each pollutant's own guideline:

```python
exceeds_who_guideline = value > who_guideline_value
```

| Pollutant | WHO annual guideline | Unit |
|---|---|---|
| PM2.5 | 15.0 | µg/m³ |
| PM10 | 45.0 | µg/m³ |
| NO₂ | 10.0 | µg/m³ |
| CO | 4000.0 | µg/m³ |
| O₃ | 60.0 | µg/m³ |

**274 of 894 readings (30.6%) breach their pollutant's guideline.** By country and pollutant, the largest breaches are:

| Country | Pollutant | Average | Exceedance factor | Readings over limit |
|---|---|---|---|---|
| India | PM2.5 | 173.24 | 11.5× | 100% |
| Mongolia | PM2.5 | 131.40 | 8.8× | 100% |
| India | NO₂ | 75.24 | 7.5× | 100% |
| India | PM10 | 279.97 | 6.2× | 100% |
| China | NO₂ | 61.83 | 6.2× | 100% |
| Mongolia | PM10 | 230.62 | 5.1× | 88% |
| Brazil | NO₂ | 47.73 | 4.8× | 100% |
| China | PM2.5 | 61.00 | 4.1× | 87% |

---

## Real-time (KQL) model

The Eventhouse tables mirror the Silver row shape rather than the star schema — the speed layer answers "what is happening now", so conformed dimension joins are not the point.

| Table | Retention | Populated by |
|---|---|---|
| `raw_readings` | 30 days | Eventstream `es_openaq_realtime` |
| `silver_readings` | 365 days | Update policy running `TransformRawReadings()` transactionally on `raw_readings` ingestion |

Both carry station identity, coordinates, `parameter`, `value`, `unit`, and the two timestamps; `silver_readings` adds `aqi_category` and `exceeds_who_guideline`, computed by the same WHO thresholds used in the batch path so the two layers cannot disagree. Full DDL in [`../kql/schema_create.kql`](../kql/schema_create.kql).

---

## Published snapshot contract

`08_export_to_streamlit.ipynb` writes newline-delimited JSON (one object per line) to [`../streamlit/data/`](../streamlit/data/). The Streamlit app parses line-by-line, not with a whole-file `json.load`.

| File | Shape | Columns |
|---|---|---|
| `fact_readings.json` | JSONL, 894 rows | `location_id, parameter, value, unit, aqi_category, exceeds_who_guideline, reading_date, continent, country_sk` |
| `dim_country.json` | JSONL, 23 rows | `country_sk, country_code, country_name, continent` |
| `dim_station.json` | JSONL, 308 rows | `station_sk, location_id, location_name, city, country_code, latitude, longitude` |
| `fact_aqi_predictions.json` | JSONL, 245 rows | `location_id, country_sk, pm25, predicted_aqi_class` |
| `kql_stats.json` | Single JSON object | `events_sent_this_run, skipped, errors, exported_at, pipeline, frequency, status` |

The export is a deliberate column subset of Gold — enough to power the public dashboard, without publishing the full fact schema.
