# Spark Optimization Patterns

All optimization patterns applied in the GlobalWatch pipeline with code examples and interview talking points.

---

## 1. Adaptive Query Execution (AQE)

**What:** Spark dynamically re-optimizes the execution plan at runtime based on actual data statistics gathered during the job.

**Why:** Static query planning assumes uniform data distribution. AQE handles real-world skew and small partitions automatically.

**Applied in:** All three notebooks (Bronze, Silver, Gold)

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**Three AQE features used:**

| Feature | What it does | Benefit |
|---|---|---|
| `coalescePartitions` | Merges small shuffle output partitions | Prevents 200 tiny files after joins |
| `skewJoin` | Splits oversized partitions | Prevents OOM on Beijing/Delhi station data |
| `adaptiveEnabled` | Master switch — enables runtime re-planning | Covers both above + join strategy switching |

**Interview line:** "We enable AQE on all notebooks. The key benefit in GlobalWatch is coalescePartitions — after joining Silver to dimensions, we'd normally get 200 shuffle partitions. AQE detects that most are tiny and coalesces them automatically without us manually tuning spark.sql.shuffle.partitions."

---

## 2. Broadcast Join

**What:** Copies a small table to every executor so joins happen locally without shuffling the large table.

**Why:** For dimension tables under 50MB, broadcast eliminates the most expensive Spark operation — the shuffle.

**Applied in:** Gold notebook — fact_readings build

```python
# Set broadcast threshold — tables under 50MB auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))

# Explicit broadcast hint — forces broadcast even if AQE disagrees
df_fact = df_silver \
    .join(F.broadcast(df_country),   on="country_code", how="left") \
    .join(F.broadcast(df_pollutant), on=...,             how="left") \
    .join(F.broadcast(df_station),   on="location_id",   how="left")
```

**Table sizes in GlobalWatch:**

| Table | Rows (09 Aug 2026) | Approx Size | Strategy |
|---|---|---|---|
| dim_country | 23 | ~2 KB | Broadcast |
| dim_pollutant | 5 | <1 KB | Broadcast |
| dim_station | 308 | ~30 KB | Broadcast |
| fact_readings | 894 and growing | Grows over time | Build side (never broadcast) |

**Interview line:** "dim_country has 23 rows and dim_pollutant has 5 — we explicitly broadcast both. At production scale with billions of fact rows, not broadcasting a 23-row table would cause an unnecessary multi-terabyte shuffle. The explicit hint forces the right plan even when AQE's statistics are stale."

---

## 3. Salting for Skewed Joins

**What:** Artificially distribute skewed keys across multiple partitions by appending a random salt value.

**Why:** Some OpenAQ stations (Beijing, Delhi, Mumbai) have 100x more readings than average. Without salting, one executor handles all data for that station → OOM or straggler task.

**Applied in:** Silver transform — station dimension join

```python
SALT_BUCKETS = 10

# Salt the large fact side
df_readings_salted = df_readings \
    .withColumn("salt", (F.rand() * SALT_BUCKETS).cast("int")) \
    .withColumn("station_id_salted",
                F.concat(F.col("station_id"), F.lit("_"), F.col("salt")))

# Explode the small dimension side to match all salt values
df_station_exploded = df_station \
    .crossJoin(spark.range(SALT_BUCKETS)
               .withColumnRenamed("id", "salt")) \
    .withColumn("station_id_salted",
                F.concat(F.col("station_id"), F.lit("_"), F.col("salt")))

# Join on salted key — even distribution across executors
df_joined = df_readings_salted \
    .join(F.broadcast(df_station_exploded),
          on="station_id_salted", how="left") \
    .drop("salt", "station_id_salted")
```

**Interview line:** "Beijing and Delhi stations have 100x more readings than the average station. Without salting, the executor handling station_id=201 would receive 100x the data and either OOM or become a straggler that holds up the entire stage. Salting distributes that data across 10 executors — each handles 1/10th of the Beijing data."

---

## 4. Partitioning Strategy

**What:** Physically organize Delta files on disk so filters prune irrelevant files at query time.

**Why:** Without partitioning, every query scans all files. With partitioning, Spark skips entire directories that don't match the filter.

```python
# Silver — partitioned by country_code + year_month
# Query: "Show me India readings for Aug 2026"
# Without partitioning: scan every row in the table
# With partitioning: scan only the IN/2026-08/ directory
df_silver.write \
    .partitionBy("country_code", "year_month") \
    .saveAsTable("silver_readings")

# Gold fact — partitioned by year_month
# Query: "Show me all readings for Q3 2026"
# Scans only 2026-07/, 2026-08/, 2026-09/ directories
df_fact.write \
    .partitionBy("year_month") \
    .saveAsTable("fact_readings")
```

**Interview line:** "We partition Silver by country_code + year_month because the dominant query pattern is country-filtered time-range analysis. A query for India in August 2026 skips every other country directory — in the current snapshot that means touching roughly 40 of 894 rows instead of all of them, and the ratio only improves as the table grows."

---

## 5. Z-Order (Delta File-Level Clustering)

**What:** Delta-specific optimization that co-locates rows with similar values in the same Parquet files using a space-filling Z-order curve.

**Why:** Even with partition pruning, queries filtering on non-partition columns still scan all files within a partition. Z-Order reduces files scanned further.

**Applied in:** Gold — fact_readings

```python
spark.sql(f"""
    OPTIMIZE {GOLD_DB}.fact_readings
    ZORDER BY (location_id, reading_ts)
""")
```

**When Z-Order helps:**

| Query | Without Z-Order | With Z-Order |
|---|---|---|
| `WHERE location_id = 6207952` | Scan every file in the partition | Scan 1–2 files |
| `WHERE location_id = 6207952 AND reading_ts >= '2026-01-01'` | Scan every file in the partition | Scan 1 file |
| `WHERE country_code = 'IN'` | Not helped (partition pruning handles this) | N/A |

**Interview line:** "Z-Order is Delta's answer to multi-dimensional clustering. We Z-order on location_id + reading_ts because our Power BI reports and RTI dashboards always filter by station and time range together. The Z-order curve co-locates data for nearby station IDs and timestamps in the same files — Data Skipping then eliminates files that can't contain the query's rows."

---

## 6. V-Order (Fabric Direct Lake Optimization)

**What:** Microsoft Fabric's proprietary Parquet write optimization that sorts data within row groups by value distribution.

**Why:** Direct Lake mode in Power BI reads Parquet files from OneLake directly into memory. V-Ordered files allow the VertiPaq engine to compress and scan data faster.

**Applied in:** Gold — all tables (enabled by default in Fabric Spark runtime)

```python
# V-Order is ON by default in Fabric Spark
# Explicitly confirm it's enabled:
spark.conf.set("spark.ms.vorder.enabled", "true")

df_fact.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .saveAsTable(f"{GOLD_DB}.fact_readings")
```

**Direct Lake modes:**

| Mode | Trigger | Performance |
|---|---|---|
| Direct Lake (V-Ordered) | Gold tables with V-Order ON | Fastest — in-memory VertiPaq scan |
| Direct Lake (fallback) | V-Order OFF or large tables | Slower — row group scan |
| DirectQuery | Fallback when Direct Lake fails | Slowest — live SQL query |

**Interview line:** "V-Order is mandatory for Direct Lake performance. Without it, Power BI's VertiPaq engine can't efficiently load the Parquet row groups into its columnar store — it falls back to a slower scan mode. We apply V-Order on all Gold tables since they're the Direct Lake serving layer."

---

## 7. Delta MERGE for SCD Type 2

**What:** Atomic upsert operation — matches rows, updates matched rows, inserts unmatched rows in a single transaction.

**Why:** SCD Type 2 requires expiring old rows and inserting new ones. Two separate operations (UPDATE then INSERT) risk partial failures. MERGE is atomic.

**Applied in:** dim_station — Gold notebook

```python
from delta.tables import DeltaTable

# Step 1: Expire changed rows
DeltaTable.forName(spark, f"{GOLD_DB}.dim_station").alias("target") \
    .merge(
        df_stations_src.alias("source"),
        """target.location_id = source.location_id
           AND target.active_flag = true
           AND target.station_hash <> source.station_hash"""
    ) \
    .whenMatchedUpdate(set={
        "active_flag":   F.lit(False),
        "effective_end": F.current_timestamp()
    }) \
    .execute()

# Step 2: Insert new/changed rows
df_to_insert.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(f"{GOLD_DB}.dim_station")
```

**Interview line:** "We use Delta MERGE for SCD2 because it's atomic — the expire and insert happen in a single transaction. The station_hash column is key: instead of comparing every attribute column, we compare one MD5 hash. If the hash changes, we know something changed. This is O(1) comparison regardless of how many columns the station has."

---

## 8. Schema Evolution (mergeSchema)

**What:** Delta automatically updates the table schema when new columns appear in incoming data.

**Why:** OpenAQ API occasionally adds new fields. Without schema evolution, the write fails. With it, Delta adds the new column and backfills NULL for existing rows.

**Applied in:** Bronze — raw_openaq_readings write

```python
df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \  # handles new API fields
    .partitionBy("ingestion_date") \
    .saveAsTable(f"{DB}.raw_openaq_readings")
```

**Interview line:** "We enable mergeSchema on Bronze writes because OpenAQ v3 is an evolving API. When they add a new field, we don't want our pipeline to fail — we want to capture it and handle it in Silver. Bronze is our raw landing zone: capture everything, validate later."

---

## Summary Table

| Optimization | Notebook | Interview Trigger Question |
|---|---|---|
| AQE | All | "How do you handle data skew in Spark?" |
| Broadcast join | Gold | "How do you optimize joins in Spark?" |
| Salting | Silver | "What causes OOM in Spark and how do you fix it?" |
| Partitioning | Silver + Gold | "How do you design partitions in Delta Lake?" |
| Z-Order | Gold | "What is Z-Order and when do you use it?" |
| V-Order | Gold | "What is Direct Lake and how does it work?" |
| Delta MERGE | Gold | "How do you implement SCD Type 2 in Spark?" |
| mergeSchema | Bronze | "How do you handle schema drift in pipelines?" |
