# Interview Narratives

STAR-format stories for each phase of GlobalWatch. Use these when interviewers ask behavioural or technical questions.

> Figures quoted as "the dataset" reflect the Gold snapshot of **09 Aug 2026** — 894 fact rows, 303 stations, 23 countries, 274 WHO exceedances. Figures tied to a specific screenshot or run are labelled as such; they describe that run, not the current state.

---

## "Tell me about a data engineering project you built end-to-end"

**Situation:** I wanted to build a production-grade portfolio project on Microsoft Fabric that demonstrates the full data engineering lifecycle — ingestion, transformation, serving, and AI — using a globally unique dataset.

**Task:** Design and implement a Lambda architecture platform that ingests real-time and batch air quality data across 70 targeted country codes, processes it through a medallion lakehouse, scores it with a tracked model, and serves it via Direct Lake Power BI and a natural-language interface.

**Action:** Built GlobalWatch on Microsoft Fabric — Fabric Eventstream for real-time OpenAQ ingestion, Fabric Data Factory watermark pipelines for batch WAQI data, PySpark notebooks with AQE, broadcast joins, and SCD Type 2 via Delta MERGE for the Bronze→Silver→Gold medallion. Added a KQL Database with update policies for the real-time layer, Data Activator for PM2.5 hazard alerting, and a Claude Sonnet tool-use agent for natural language querying.

**Result:** A deployed pipeline currently holding 894 readings from 303 stations across 23 countries, with a star schema Gold layer showing India at 100% PM2.5 WHO exceedance (avg 173.24 µg/m³ against a guideline of 15) and 30.6% of all readings breaching their pollutant's WHO limit. It runs on a schedule — batch daily, streaming hourly — with a Random Forest scoring PM2.5 readings, a Data Activator email alert on hazardous conditions, a Direct Lake Power BI report with continent-level RLS, and a public Streamlit app with a Claude-powered assistant grounded on the published Gold snapshot.

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

**Situation:** The Gold layer joins the fact table to three dimensions that are tiny by comparison — currently 23, 5 and 308 rows. At production scale with billions of fact rows, unoptimized joins would cause massive shuffles.

**Task:** Eliminate shuffle for dimension joins.

**Action:** Set `autoBroadcastJoinThreshold` to 50MB and applied explicit `F.broadcast()` hints on all three dimension joins. dim_country (23 rows), dim_pollutant (5 rows) and dim_station (308 rows) are all copied to every executor — joins happen locally with zero data movement. The explicit hint matters because it holds even when AQE's statistics are stale.

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

**Action:** Built a Streamlit app powered by Claude Sonnet 4.6 through the Anthropic Messages API. The design decision worth explaining is *grounding*: rather than giving the model query access, the app pre-computes aggregates from the published Gold snapshot — per-country PM2.5 averages, WHO exceedance counts, AQI distribution, ML prediction distribution, top polluted stations, and an architecture summary — and injects them as the system prompt on every turn. The API key lives in Streamlit secrets, never in source.

**Result:** Stakeholders ask "which country has the worst air quality?" or "how many stations exceed WHO guidelines?" and get an answer citing the real numbers from the latest pipeline export, with no hallucinated figures — because every number available to the model came from the Gold layer. The honest limitation is that it cannot answer outside those aggregates; the three-tool design (`query_kql`, `query_gold_sql`, `get_country_health_context`) is specified in TECH_SPEC as the next step once the agent has network access to the KQL and Lakehouse SQL endpoints. Being able to say precisely what is built, what is designed, and why the boundary sits where it does is usually more convincing than claiming the full agent.

---

## "How did you handle API rate limits?"

**Situation:** OpenAQ API free tier enforces rate limits. Hitting the limit mid-ingestion returns 429 errors and loses data.

**Task:** Build rate-limit-safe ingestion without over-engineering.

**Action:** The first version used simple `time.sleep()` calls between requests — safe, but it made 70-country coverage impractically slow. The current version uses a `ThreadPoolExecutor` with 10 workers and a `Semaphore` that holds total throughput to the 60 req/min ceiling, so concurrency and the rate limit are decoupled. I also switched from `/sensors/{id}/measurements` to `/locations/{id}/latest` — one call per location instead of one per sensor, which is the change that actually made the breadth affordable. Every call is wrapped in try/except so an individual failure is logged and skipped rather than crashing the run, and the watermark only advances if records were written.

**Result:** Zero pipeline crashes from rate limits, and coverage went from a handful of countries to 23 with sensor data returning across 70 targeted country codes. The lesson I'd give: rate limits are a throughput budget, not a reason to go serial — the fix is bounding concurrency against the budget, plus reducing the number of calls each unit of work needs.

---

## "How did you build your real-time KQL pipeline?"

**Situation:** GlobalWatch needed a real-time layer that could process incoming air quality readings and automatically classify them by AQI category without a separate streaming compute job.

**Task:** Stream OpenAQ readings into a KQL Database and apply transformations natively as data arrives.

**Action:** Created a KQL Eventhouse with two tables: `raw_readings` (landing zone, 30-day retention) and `silver_readings` (365-day retention). Wrote a KQL function `TransformRawReadings()` that applies AQI categorization logic and WHO exceedance flags. Attached it as an update policy on `raw_readings` with `IsEnabled=true, IsTransactional=true` — so every new row landing in raw automatically triggers the function and writes a transformed record to silver in the same transaction. The Eventstream custom endpoint (`es_openaq_realtime`) routes the OpenAQ feed directly into `kql-raw-readings`.

**Result:** 315 events streamed end-to-end on the first run; the hourly pipeline now dispatches around 5,056 events per run. No Spark streaming job needed — KQL update policies handle the transformation natively, with transactional guarantees meaning raw and silver are always in sync. This is the Fabric-native alternative to Structured Streaming for sub-minute latency use cases.

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

---

## "Have you used MLflow in your projects?"

**Situation:** GlobalWatch needed a way to train, track, and serve an AQI prediction model — and interviewers increasingly ask about ML lifecycle management, not just model accuracy.

**Task:** Train a classifier on air quality data, track it with MLflow, register it in the model registry, and apply it to the Gold layer.

**Action:** Built `06_ml_aqi_prediction.ipynb` with 7 cells covering the full ML lifecycle. Pivoted Silver data from long to wide format (one row per station+timestamp, one column per pollutant). Created WHO PM2.5 threshold-based labels (0=Good → 4=Hazardous). Trained a Random Forest classifier using Spark MLlib with a Pipeline (VectorAssembler → RandomForestClassifier). Logged hyperparameters (num_trees=100, max_depth=5), metrics (accuracy, train/test rows), and the model artifact to MLflow. Registered as `globalwatch_aqi_classifier v1` in the MLflow Model Registry. Loaded the registered model and applied it to Gold `fact_readings`, writing the scored rows to a new `fact_aqi_predictions` Delta table with `mode=overwrite` so re-runs are idempotent.

**Result:** 96.15% accuracy on the held-out test set. Feature importance showed PM2.5 dominating at 76.32%, followed by PM10 at 12.91%. The current snapshot holds 245 predictions across 206 stations — Good 130, Moderate 87, Unhealthy 20, Hazardous 8, consistent with real-world air quality data skewed toward cleaner readings. Worth saying out loud in an interview: PM2.5 is by construction the dominant signal for a PM2.5-derived label, so 96% demonstrates a working MLflow lifecycle more than a hard prediction problem — the transferable part is the pattern of track → register → load by URI → score → persist.

---

## "Why Random Forest over other algorithms for this problem?"

**Situation:** Given 164 pivoted rows across 5 features with significant class imbalance (134 Good vs 5 Hazardous), algorithm choice matters.

**Task:** Pick an algorithm that handles imbalance, small data, and mixed feature scales without extensive tuning.

**Action:** Chose Random Forest because: (1) ensemble of trees handles class imbalance better than a single decision tree — minority classes like Hazardous get represented across different tree splits; (2) no feature scaling needed unlike SVM or logistic regression — PM2.5 ranges 0-300 while O3 ranges 0-0.1, but RF handles this natively; (3) built-in feature importance via Gini impurity — directly answers "which pollutant matters most?"; (4) robust to overfitting with maxDepth=5 cap on a 164-row dataset. XGBoost would also work but requires pip install in the Fabric environment — RF is available natively in Spark MLlib.

**Result:** 96.15% accuracy with zero preprocessing beyond pivot and null-fill. PM2.5 confirmed as the dominant predictor (76%) — aligns with WHO's PM2.5-centric AQI framework.

---

## "How did you apply the ML model to production data?"

**Situation:** A trained model sitting in MLflow has no value unless it enriches the serving layer that downstream consumers query.

**Task:** Load the registered model and apply it to the Gold lakehouse `fact_readings` table without retraining.

**Action:** Used `mlflow.spark.load_model("models:/globalwatch_aqi_classifier/1")` to load the registered PipelineModel directly from the MLflow Model Registry. Filtered Gold `fact_readings` to PM2.5 rows only, reshaped to match the training schema (pm25, pm10, no2, o3, co columns), ran `loaded_model.transform()` to generate predictions, then mapped numeric predictions back to readable labels (Good/Moderate/Unhealthy/Hazardous). Wrote the result as `fact_aqi_predictions` Delta table in the Gold lakehouse using `saveAsTable` with `mode=overwrite` for idempotency.

**Result:** PM2.5 station predictions persisted to Gold — 245 rows over 206 stations in the current snapshot. The pattern — register → load → transform → write — is the standard MLflow inference pattern, applicable to any Spark-based ML deployment on Fabric, Databricks or EMR alike.

---

## "Did you use the Fabric Data Agent?"

**Situation:** GlobalWatch's architecture called for a native Fabric Data Agent to enable natural language querying over the Gold lakehouse and KQL database — a key differentiator of the Microsoft Fabric RTI stack.

**Task:** Implement conversational AI querying over air quality data without requiring users to know SQL or KQL.

**Action:** Attempted to provision a native Fabric Data Agent (`globalwatch_agent`) in the globalwatch-dev workspace. Encountered a SKU limitation — Data Agent requires F64+ capacity; the trial runs on a lower SKU. Rather than leaving the capability undocumented, built `07_data_agent_simulation.ipynb` to demonstrate the NL→SQL pattern the Data Agent uses internally: defined 3 natural language questions, mapped each to an equivalent SQL query, executed them against the Gold lakehouse, and surfaced the results. Findings from that run (per `screenshots/29_data_agent_query_results.png`): India highest PM2.5 at 175.83 µg/m³, 5 hazardous readings detected, and a CO reading of 8,720 ppb flagged as an extreme outlier worth investigating. The current snapshot shows India at 173.24 µg/m³ and 11 hazardous readings — the figures move with each run.

**Result:** The simulation proves architectural understanding of the Data Agent pattern. In production on F64+, the native Data Agent would replace the manual NL→SQL mapping with an LLM-powered query translator connected directly to the semantic model and KQL database — same queries, zero code. This is a common interview scenario — knowing *why* a feature isn't available and *how* to work around it demonstrates real-world engineering judgment over tutorial-following.

---

## "How does Fabric Data Agent work under the hood?"

**Situation:** Interviewers at Microsoft-stack companies often ask about Data Agent internals to assess depth of Fabric knowledge.

**Task:** Explain the Data Agent architecture beyond "it answers questions in natural language."

**Action:** Fabric Data Agent is an LLM-powered query translator that sits on top of a semantic model or KQL database. When a user asks a natural language question, the agent: (1) interprets the question using an LLM (GPT-4 in the backend); (2) maps it to the available schema — tables, columns, measures, relationships in the semantic model; (3) generates a DAX query (for Power BI semantic models) or KQL query (for Eventhouses); (4) executes the query against the Direct Lake or KQL engine; (5) formats the result as a natural language answer with supporting data. The agent is stateful within a session — follow-up questions like "now filter to India only" resolve correctly because the agent maintains conversation context.

**Result:** Understanding this flow means you can debug Data Agent failures (schema not exposed → agent can't generate correct query), optimize it (add descriptions to semantic model columns → better NL interpretation), and explain its limitations (complex multi-hop reasoning across joins still fails occasionally — better to pre-compute in Gold).

---

## "How did you orchestrate your data pipelines?"

**Situation:** GlobalWatch had 5 notebooks covering Bronze ingestion, Silver transformation, Gold star schema, ML prediction, and Data Agent simulation — each needed to run in dependency order daily.

**Task:** Build an orchestration layer that chains all notebooks, handles failures gracefully, and runs on a schedule without manual intervention.

**Action:** Created two Fabric Data Factory pipelines. `pl_batch_globalwatch` chains 5 notebook activities in sequence: Bronze_Ingest → Silver_Transform → Gold_Star_Schema → ML_AQI_Prediction → Data_Agent_Simulation. Each activity has retry=2, retry interval=60s, timeout=1hr. Green arrows between activities enforce on-success dependencies — Silver only runs if Bronze succeeds, Gold only if Silver succeeds, and so on. Scheduled daily at 02:00 AM IST with failure email notifications to jaydolai@zohomail.in. `pl_realtime_globalwatch` runs `Stream_To_Eventstream` notebook hourly with retry=3, interval=30s — shorter timeout (30 mins) since streaming runs are faster than full batch.

**Result:** Two validated pipelines — batch validated with zero errors in Fabric pipeline validator. The separation of batch and real-time into two pipelines follows Lambda architecture principles: batch handles full medallion refresh + ML, real-time handles continuous stream ingestion independently. In production, `pl_batch_globalwatch` would be triggered after `pl_realtime_globalwatch` completes its last hourly run, ensuring Gold reflects the latest streamed data before ML scoring.

---

## "How would you implement CI/CD for Fabric in production?"

**Situation:** GlobalWatch runs on a personal Zoho trial tenant which blocks Git integration (GitHub OAuth disabled, Azure DevOps requires organizational account). In production at Optum or any enterprise, CI/CD is a hard requirement.

**Task:** Design and explain the production CI/CD setup even where it cannot be demonstrated live.

**Action:** The production setup would use Fabric's native Git integration connected to Azure DevOps (enterprise standard). The workspace connects to a feature branch — developers commit notebook and pipeline changes to feature branches, raise PRs to main. Fabric automatically syncs workspace items on merge. A three-stage deployment pipeline (Dev → Test → Prod) promotes items across workspaces: Dev workspace for development and testing, Test workspace for UAT and data quality validation, Prod workspace for live serving. Each stage has its own capacity and lakehouse — connection strings and secrets managed via Fabric environment variables or Azure Key Vault. Pipeline runs in Test validate Silver DQ rules and Gold assertion counts before promotion to Prod is allowed.

**Result:** The architecture is documented and the limitation is a tenant constraint, not a design gap. The GitHub repo (`demonjd2026-afk/globalwatch-fabric`) serves as the source of truth for all notebooks, pipelines, and docs — matching what a Git-integrated Fabric workspace would track automatically.

---

## "What is the difference between Fabric deployment pipeline and Git integration?"

**Situation:** Interviewers frequently conflate these two Fabric CI/CD features.

**Task:** Explain both clearly and when to use each.

**Action:** Git integration connects a Fabric workspace to a Git branch — it tracks item definitions (notebooks, pipelines, semantic models) as code and syncs changes bidirectionally. It's the developer workflow tool — commit, push, PR, merge. Deployment pipeline promotes items across environments (Dev → Test → Prod) — it's the release management tool. They work together: Git integration manages the code lifecycle within an environment; deployment pipeline moves promoted, tested items across environments. A typical flow: developer commits notebook changes to feature branch (Git) → PR merged to main → Dev workspace syncs (Git) → deployment pipeline promotes Dev → Test → Test team validates → promotes Test → Prod. Git integration without deployment pipeline means you're doing version control but no environment promotion. Deployment pipeline without Git integration means you're promoting items but not tracking their history.

**Result:** Understanding this distinction demonstrates senior-level Fabric knowledge — most tutorials cover only one of the two, not how they compose.

---

## "Did you run your pipelines end-to-end successfully?"

**Situation:** Building pipelines is one thing — proving they run cleanly in production mode is another. Pipeline execution has different constraints than interactive notebook runs.

**Task:** Get both batch and realtime pipelines running successfully via Fabric Data Factory, not just interactively.

**Action:** Hit two pipeline-specific issues during execution. First: `%pip magic command is disabled` — pip installs work in interactive sessions but not pipeline-triggered runs. Fixed by adding `azure-eventhub` to `globalwatch-env` as a PyPI library via External repositories, then removing the `%pip install` cell from the streaming notebook. Second: KQL outbound network resolution fails in pipeline sandbox on trial capacity — the verification cell that queries the KQL endpoint via HTTP was blocked. Fixed by wrapping it in a skip with a comment explaining it must be run manually in interactive mode. After both fixes, `pl_realtime_globalwatch` ran successfully in 1m 44s.

**Result:** `Stream_To_Eventstream` activity — Succeeded. KQL `raw_readings` count grew from 315 → 2,205 events confirming end-to-end data flow: notebook → Eventstream → KQL raw_readings → update policy → silver_readings. These are real production debugging skills — knowing the difference between interactive and pipeline execution environments, and how to fix library management and network access issues specific to Fabric pipeline mode.
