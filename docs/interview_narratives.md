# Interview Narratives

STAR-format stories for each phase of GlobalWatch. Use these when interviewers ask behavioural or technical questions.

---

## "Tell me about a data engineering project you built end-to-end"

**Situation:** I wanted to build a production-grade portfolio project on Microsoft Fabric that demonstrates the full data engineering lifecycle — ingestion, transformation, serving, and AI — using a globally unique dataset.

**Task:** Design and implement a Lambda architecture platform that ingests real-time and batch air quality data from 10,000+ stations across 90 countries, processes it through a medallion lakehouse, and serves it via Direct Lake Power BI and a Claude-powered AI agent.

**Action:** Built GlobalWatch on Microsoft Fabric — Fabric Eventstream for real-time OpenAQ ingestion, Fabric Data Factory watermark pipelines for batch WAQI data, PySpark notebooks with AQE, broadcast joins, and SCD Type 2 via Delta MERGE for the Bronze→Silver→Gold medallion. Added a KQL Database with update policies for the real-time layer, Data Activator for PM2.5 hazard alerting, and a Claude Sonnet tool-use agent for natural language querying.

**Result:** A fully deployed pipeline ingesting air quality readings from 9 countries, with a star schema Gold layer showing India at 100% PM2.5 WHO exceedance (avg 175 µg/m³ vs WHO guideline of 15), and an AI agent that translates natural language to KQL and SQL against live and historical data.

---

## "How did you implement incremental ingestion?"

**Situation:** The OpenAQ API returns millions of historical readings. Pulling all data on every run would be expensive and slow.

**Task:** Implement an incremental pattern that only pulls new data since the last successful run.

**Action:** Created a `watermark_control` Delta table in Bronze with columns `source_name`, `last_loaded_date`, `last_loaded_ts`. At the start of each pipeline run, we read the watermark, pass `last_loaded_date` as a filter to the API call, write only new records to Bronze, then UPDATE the watermark to the current timestamp after a successful write.

**Result:** Each run only processes net-new records — no full reloads, no duplicate data, and if a run fails mid-way the watermark isn't updated so the next run picks up from the last successful point.

---

## "How did you handle SCD Type 2 in your pipeline?"

**Situation:** Air quality stations occasionally change their name or get reassigned to a different city district. We needed to track these changes historically for accurate point-in-time reporting.

**Task:** Implement SCD Type 2 on `dim_station` so analysts can see what a station was called at any point in time.

**Action:** Used Delta MERGE with a hash-based change detection strategy. Added a `station_hash` column (MD5 of location_id + name + city + country). On each run: first MERGE expires rows where the hash changed by setting `active_flag=False` and `effective_end=now()`. Then insert new rows with `active_flag=True` and `effective_end=9999-12-31`. The hash comparison means we only compare one column instead of all attributes — O(1) regardless of table width.

**Result:** dim_station correctly tracks station history. Active stations show `effective_end=9999-12-31`. When a station is renamed, the old row gets expired and a new row is inserted atomically — no partial failures possible with Delta MERGE.

---

## "How did you optimize Spark joins?"

**Situation:** The Gold layer joins a 344-row fact table to dimension tables of 9, 5, and 121 rows respectively. At production scale with billions of fact rows, unoptimized joins would cause massive shuffles.

**Task:** Eliminate shuffle for dimension joins.

**Action:** Set `autoBroadcastJoinThreshold` to 50MB and applied explicit `F.broadcast()` hints on all three dimension joins. dim_country (9 rows), dim_pollutant (5 rows), and dim_station (121 rows) are all copied to every executor — joins happen locally with zero data movement.

**Result:** No shuffle stage in the Gold fact build. At scale, this saves minutes of shuffle time and eliminates the risk of OOM on executor memory during the join phase.

---

## "What is Direct Lake and how did you use it?"

**Situation:** The GlobalWatch Power BI report needed sub-second query performance on the Gold star schema without the complexity of scheduled refreshes.

**Task:** Configure the Power BI semantic model to read directly from OneLake Delta files.

**Action:** Wrote all Gold tables with V-Order enabled (Fabric's proprietary Parquet optimization) and created a Direct Lake semantic model pointing to the Gold lakehouse. Direct Lake reads Delta Parquet files directly into VertiPaq's in-memory columnar store — no import copy, no DirectQuery live SQL.

**Result:** Power BI reports query in-memory speeds without any scheduled refresh. When fact_readings is updated by the daily pipeline, the semantic model picks up the new Delta snapshot automatically. V-Order is the critical enabler — without it, Direct Lake falls back to a slower scan mode.

---

## "Have you worked with real-time data?"

**Situation:** The GlobalWatch operational dashboard needed live AQI readings updated every few seconds — the batch Gold layer refreshes daily so it can't serve this need.

**Task:** Build a real-time path from the OpenAQ API to a live dashboard.

**Action:** Set up a Fabric Eventstream with a Custom Endpoint that receives OpenAQ readings every 5 seconds. The stream lands in a KQL Database with a KQL update policy that automatically transforms raw readings into AQI categories as they arrive — no separate streaming job needed. A Data Activator rule monitors the KQL stream and sends a Teams alert when PM2.5 exceeds 150 µg/m³ (WHO Hazardous threshold) for 3 consecutive readings from the same station.

**Result:** Live RTI dashboard with auto-refresh showing real-time AQI by country, plus automated hazard alerts — with zero custom streaming code, using only Fabric-native components.

---

## "Tell me about an AI or LLM integration you built"

**Situation:** Non-technical stakeholders wanted to query GlobalWatch data without writing KQL or SQL.

**Task:** Build a natural language interface that routes questions to the correct data layer.

**Action:** Built a Streamlit app powered by Claude Sonnet with three tools: `query_kql` for real-time questions (last 24h), `query_gold_sql` for historical trend questions, and `get_country_health_context` for economic enrichment. The agent decides which tool to call based on the question, generates the query, executes it against the live endpoint, and returns a grounded natural language answer. API key stored in Streamlit secrets — not hardcoded.

**Result:** Stakeholders can ask "Which Asian cities had hazardous PM2.5 in the last 6 hours?" and get a live answer pulled from the KQL Database, or "Show me India's PM2.5 trend vs GDP per capita" and get a Gold SQL query result with World Bank enrichment — all through a chat interface.

---

## "How did you handle API rate limits?"

**Situation:** OpenAQ API free tier enforces rate limits. Hitting the limit mid-ingestion returns 429 errors and loses data.

**Task:** Build rate-limit-safe ingestion without over-engineering.

**Action:** Added `time.sleep(1)` between sensor-level API calls and `time.sleep(0.5)` between location iterations. Wrapped all API calls in try/except to log and skip individual failures rather than crashing the pipeline. Added a 429 detection check — if a page returns 429, the loop breaks gracefully and the watermark only updates if at least some records were written.

**Result:** Zero pipeline crashes from rate limits. The 429 on page 3 of the initial test was caught gracefully — 335 records were still written successfully for pages 1 and 2, and the next run picks up where it left off via the watermark.

---

## "How did you build your real-time KQL pipeline?"

**Situation:** GlobalWatch needed a real-time layer that could process incoming air quality readings and automatically classify them by AQI category without a separate streaming compute job.

**Task:** Stream OpenAQ readings into a KQL Database and apply transformations natively as data arrives.

**Action:** Created a KQL Eventhouse with two tables: `raw_readings` (landing zone, 30-day retention) and `silver_readings` (365-day retention). Wrote a KQL function `TransformRawReadings()` that applies AQI categorization logic and WHO exceedance flags. Attached it as an update policy on `raw_readings` with `IsEnabled=true, IsTransactional=true` — so every new row landing in raw automatically triggers the function and writes a transformed record to silver in the same transaction. The Eventstream custom endpoint (`es_openaq_realtime`) routes the OpenAQ feed directly into `kql-raw-readings`.

**Result:** 315 events streamed end-to-end within the first run. No Spark streaming job needed — KQL update policies handle the transformation natively, with transactional guarantees meaning raw and silver are always in sync. This is the Fabric-native alternative to Structured Streaming for sub-minute latency use cases.

---

## "What is a KQL update policy and when would you use it?"

**Situation:** Incoming raw air quality readings needed AQI categorization applied in real time — but spinning up a separate Spark Structured Streaming job just for a column derivation felt like overengineering.

**Task:** Find a Fabric-native way to apply a transformation automatically as each row arrives in the KQL database.

**Action:** Used a KQL update policy — a mechanism where you define a KQL function (the transformation) and attach it to a destination table. When a row lands in the source table, the policy fires the function and inserts the result into the destination table, atomically. The key setting is `IsTransactional=true` — if the function fails, the source write also rolls back, so you never get a raw row without a corresponding silver row.

**Result:** Zero-code, zero-latency real-time transformation. The update policy replaces what would otherwise be a Spark streaming pipeline with complex checkpoint management. It's the right tool when your transformation is a column derivation or lookup — not when you need windowing, aggregation, or ML scoring (those still need Spark).

---

## "How did you set up automated alerting in your pipeline?"

**Situation:** The GlobalWatch dashboard showed PM2.5 readings but there was no mechanism to proactively notify anyone when a hazardous reading was detected — analysts had to check the dashboard manually.

**Task:** Implement automated alerting that triggers when PM2.5 exceeds the WHO Hazardous threshold (150 µg/m³) without any custom polling code.

**Action:** Used Fabric Data Activator connected directly to the `es_openaq_realtime` Eventstream. Defined a `pm25_station` object tracking the `location_id` field as the instance key. Created a rule `pm25_hazard_alert` that fires when `value > 150` and sends an email with station name, city, country, and current reading. No code — configured entirely through Data Activator's visual rule builder. The alert is bound to the live stream, not a batch query, so detection latency is seconds not hours.

**Result:** Alert confirmed firing — email received at Aug 09 2026 6:08 UTC with subject "[Fabric Activator] ⚠️ GlobalWatch PM2.5 Hazard Alert". 5 of 98 monitored station IDs actively triggered. This demonstrates the full observability loop: real-time ingest → KQL transformation → automated action, entirely Fabric-native with no external infrastructure.

---

## "Have you worked with Fabric Real-Time Intelligence end-to-end?"

**Situation:** Most portfolio projects demonstrate batch medallion pipelines. Interviewers at companies using Fabric increasingly ask about Real-Time Intelligence (RTI) — Eventstream, KQL, and Data Activator — as these are the differentiating Fabric features vs. pure Databricks stacks.

**Task:** Build and demonstrate a complete RTI pipeline from live API source through to automated action.

**Action:** Built the full RTI stack for GlobalWatch: (1) Fabric Eventstream with a custom endpoint receives OpenAQ readings via the `07_streaming_openaq_eventstream.ipynb` notebook posting JSON payloads over the Azure Event Hubs SDK. (2) Eventstream routes to `kql-raw-readings` in the `globalwatch_eventhouse` KQL Database using Event processing mode. (3) A KQL update policy transforms raw → silver readings with AQI categorization automatically. (4) Data Activator monitors the same Eventstream and fires email alerts when PM2.5 exceeds 150 µg/m³. The KQL Queryset also includes analytical queries (e.g., `silver_readings | summarize count() by country_code, parameter, aqi_category | order by count_ desc | take 10`) for dashboard consumption.

**Result:** 315 events end-to-end in the first streaming run. Top real-time findings: Netherlands (NL) dominates event count — 27 PM10, 23 NO2, 20 PM25 readings; Chile (CL) and Ghana (GH) also active. Data Activator alert confirmed. This is a complete, working RTI reference implementation — not a tutorial replica.
