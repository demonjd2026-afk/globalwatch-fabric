# Notebooks

PySpark notebooks for the GlobalWatch medallion pipeline.
Run in order — each notebook depends on the previous layer.

| # | Notebook | Layer | Description |
|---|---|---|---|
| 01 | `01_bronze_ingest_openaq.ipynb` | Bronze | OpenAQ API ingestion — watermark control, schema enforcement, Delta write |
| 04 | `04_silver_transform.ipynb` | Silver | DQ filters, AQI categorization, partitioned Delta write |
| 05 | `05_gold_star_schema.ipynb` | Gold | Star schema — dim_date, dim_pollutant, dim_country (SCD1), dim_station (SCD2), fact_readings (V-Order + Z-Order) |

## Prerequisites

- Fabric workspace `globalwatch-dev` with three lakehouses attached
- Environment `globalwatch-env` with `spark.openaq.api.key` Spark property set
- Silver notebook must be attached to `silver_globalwatch` lakehouse
- Gold notebook must be attached to `gold_globalwatch` lakehouse

## Execution order

```
01_bronze_ingest_openaq
        ↓
04_silver_transform
        ↓
05_gold_star_schema
```

## Key concepts demonstrated

- Watermark-based incremental ingestion
- Cross-lakehouse reads via 3-part table reference
- AQE (Adaptive Query Execution) configuration
- Broadcast join for small dimensions
- Delta MERGE for SCD Type 2
- V-Order write for Direct Lake compatibility
- Z-Order for query optimization
- Row count assertions for data quality
