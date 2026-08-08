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
