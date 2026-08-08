# Data Model

## Gold Layer — Star Schema

### Entity Relationship

```
                    ┌─────────────┐
                    │  dim_date   │
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
│──────────────│    │─────────────────────────────│    │─────────────────│
│ station_sk PK│◄───│ station_sk FK               │───►│ pollutant_sk PK │
│ location_id  │    │ country_sk FK               │    │ pollutant_code  │
│ location_name│    │ pollutant_sk FK             │    │ pollutant_name  │
│ city         │    │ date_key FK                 │    │ standard_unit   │
│ country_code │    │ location_id                 │    │ who_guideline   │
│ latitude     │    │ parameter                   │    │ description     │
│ longitude    │    │ value                       │    └─────────────────┘
│ station_hash │    │ unit                        │
│ active_flag  │    │ aqi_category                │    ┌─────────────────┐
│ effective_start   │ exceeds_who_guideline        │    │  dim_country    │
│ effective_end│    │ reading_ts                  │◄───│─────────────────│
└──────────────┘    │ reading_date                │    │ country_sk PK   │
                    │ reading_hour                │    │ country_code    │
                    │ year_month                  │    │ country_name    │
                    │ is_recent                   │    │ continent       │
                    │ latitude                    │    │ gdp_per_capita  │
                    │ longitude                   │    │ health_exp_pct  │
                    │ continent                   │    │ population      │
                    │ source_system               │    │ scd_updated_ts  │
                    │ ingestion_ts                │    └─────────────────┘
                    └─────────────────────────────┘
```

---

## SCD Implementation

### dim_station — SCD Type 2

Tracks changes to station name and city over time.

| Column | Type | Description |
|---|---|---|
| `station_sk` | INT | CRC32 surrogate key — deterministic from location_id |
| `location_id` | INT | OpenAQ natural key |
| `station_hash` | STRING | MD5 of location_id + name + city + country — change detector |
| `active_flag` | BOOLEAN | True = current version |
| `effective_start` | TIMESTAMP | When this version became active |
| `effective_end` | TIMESTAMP | 9999-12-31 = currently active; otherwise expiry timestamp |

**MERGE logic:**
```
Step 1 — Expire changed rows:
  MATCH: location_id = source.location_id
         AND active_flag = true
         AND station_hash <> source.station_hash
  ACTION: active_flag = false, effective_end = now()

Step 2 — Insert new/changed rows:
  Anti-join source against active target
  INSERT all unmatched rows with active_flag=true, effective_end=9999-12-31
```

### dim_country — SCD Type 1

Overwrites current values — no history kept.

**Rationale:** Country GDP and health expenditure are annual reference values. Analysts always want the current enrichment, not point-in-time historical GDP.

### dim_date — Type 0

Pre-generated spine from 2015-01-01 to 2030-12-31. Never changes.

### dim_pollutant — Type 0

5 rows (pm25, pm10, no2, co, o3) with WHO 2021 annual guidelines. Updated manually when WHO revises guidelines.

---

## Surrogate Key Strategy

All surrogate keys use **CRC32 hash** of the natural key:

```python
F.crc32(F.col("country_code")).cast(IntegerType())
F.crc32(F.col("location_id").cast("string")).cast(IntegerType())
```

**Why CRC32 over sequence generators?**
- Deterministic: same natural key always produces same SK across all runs
- No distributed counter needed — avoids coordination overhead in Spark
- Collision risk is acceptable for dimension tables at this scale

---

## Partitioning Strategy

| Table | Partition Columns | Rationale |
|---|---|---|
| `bronze.raw_openaq_readings` | `ingestion_date` | Append-only; date-based cleanup |
| `silver.silver_readings` | `country_code`, `year_month` | Country-level queries are primary access pattern |
| `gold.fact_readings` | `year_month` | Time-range reporting queries always filter by month |

---

## AQI Category Definitions (WHO 2021)

Applied to PM2.5 readings only:

| Category | PM2.5 Range (µg/m³) | Health Implication |
|---|---|---|
| Good | 0–12.0 | Air quality satisfactory |
| Moderate | 12.1–35.4 | Acceptable; some pollutants concern sensitive groups |
| Unhealthy for Sensitive | 35.5–55.4 | Sensitive groups may experience health effects |
| Unhealthy | 55.5–150.4 | Everyone may experience health effects |
| Hazardous | >150.4 | Health alert — everyone may experience serious effects |

---

## WHO Exceedance Flag

Applied to all pollutants in `fact_readings`:

```python
exceeds_who_guideline = value > who_guideline_value
```

| Pollutant | WHO Annual Guideline | Unit |
|---|---|---|
| PM2.5 | 15.0 | µg/m³ |
| PM10 | 45.0 | µg/m³ |
| NO2 | 10.0 | µg/m³ |
| CO | 4000.0 | µg/m³ |
| O3 | 60.0 | µg/m³ |

**Key finding from current dataset:**
- India: 100% PM2.5 exceedance (avg 175.83 µg/m³ — 11.7x WHO guideline)
- Mongolia: 100% PM2.5 exceedance (avg 131.4 µg/m³ — 8.8x WHO guideline)
- India: 100% NO2 exceedance (avg 81.02 µg/m³ — 8.1x WHO guideline)
