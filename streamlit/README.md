# GlobalWatch — Streamlit App

The public face of the GlobalWatch platform: a dark-themed air quality dashboard plus a Claude-powered assistant, both reading Gold-layer snapshots published from Microsoft Fabric.

**Live:** <https://globalwatch-fabric.streamlit.app/>

---

## Pages

### 📊 Dashboard

| Element | Detail |
|---|---|
| KPI row | Total readings, active stations, countries, WHO exceedances, hazardous readings, real-time events per run |
| Global map | Plotly `Scattermap` on `carto-darkmatter`; bubble size and colour scale with station PM2.5 |
| PM2.5 by country | Horizontal bar with a dashed WHO limit line at 15 µg/m³ |
| AQI distribution | Donut coloured by category |
| ML predictions | Predicted AQI class counts from the Random Forest (96.15% accuracy) |
| Top 10 polluted | Table of the highest peak PM2.5 stations, AQI column colour-coded |

### 🤖 AI Agent

Natural-language Q&A over the same data, powered by Claude.

```
User question
     │
     ▼
App builds a DATA_CONTEXT system prompt from the loaded snapshot:
  · total readings, stations, country list
  · average PM2.5 per country
  · WHO exceedance counts per country
  · AQI category distribution
  · ML prediction distribution
  · top 5 polluted stations
  · platform architecture summary + WHO guideline
     │
     ▼
POST https://api.anthropic.com/v1/messages
model: claude-sonnet-4-6 · max_tokens: 1000 · anthropic-version: 2023-06-01
     │
     ▼
Answer citing the real numbers from the latest pipeline export
```

This is **one grounded API call per turn, not a multi-tool agent**. The aggregates are computed in the app and injected as the system prompt, which means every figure the assistant states is traceable to the published snapshot — and equally, that it cannot answer questions outside those aggregates. The three-tool design against live KQL and Lakehouse SQL endpoints is specified in [`../TECH_SPEC.md`](../TECH_SPEC.md#8-ai-and-natural-language-specification) as the next step.

Six suggested prompts are provided in the UI; conversation history is kept in `st.session_state` and sent in full on each turn (the Messages API is stateless).

---

## Where the data comes from

The app does **not** connect to Fabric. It reads JSONL snapshots from this repository over `raw.githubusercontent.com`, cached for 30 minutes (`@st.cache_data(ttl=1800)`):

| File | Written by | Rows (09 Aug 2026) |
|---|---|---|
| `data/fact_readings.json` | `08_export_to_streamlit.ipynb` | 894 |
| `data/dim_country.json` | `08_export_to_streamlit.ipynb` | 23 |
| `data/dim_station.json` | `08_export_to_streamlit.ipynb` | 308 |
| `data/fact_aqi_predictions.json` | `08_export_to_streamlit.ipynb` | 245 |
| `data/kql_stats.json` | `07_streaming_openaq_eventstream.ipynb` | single object |

The four Gold extracts are newline-delimited JSON — one object per line — so they are parsed line-by-line rather than with `json.load`. `kql_stats.json` is a single JSON object holding the real-time run statistics (`events_sent_this_run`, `exported_at`, `pipeline`, `frequency`, `status`).

**Freshness follows the pipeline:** `pl_batch_globalwatch` refreshes the four Gold extracts daily at 02:00 IST; `pl_realtime_globalwatch` refreshes `kql_stats.json` hourly.

Serving through GitHub is a deliberate choice — the app is free to host, needs no inbound path into the Fabric workspace, holds no credential other than the LLM key, and every published number carries a versioned commit.

---

## Upstream platform

Built on Microsoft Fabric:

- Bronze → Silver → Gold medallion lakehouse in PySpark (AQE, broadcast joins, salting, partitioning, Z-Order, V-Order, Delta MERGE SCD2)
- KQL Eventhouse fed by a Fabric Eventstream, with an update policy applying AQI categorisation on ingestion
- Data Activator emailing a hazard alert above 150 µg/m³ PM2.5
- Spark MLlib Random Forest tracked and registered in MLflow, scoring Gold PM2.5 readings
- Direct Lake Power BI semantic model with continent-level row-level security

Full detail in the [repository README](../README.md).

---

## Run locally

```bash
cd streamlit
pip install -r requirements.txt
streamlit run app.py
```

Dependencies: `streamlit`, `pandas`, `plotly`, `requests`.

The Dashboard page works with no configuration — it pulls the published snapshots straight from GitHub. The AI Agent page needs an Anthropic API key in `streamlit/.streamlit/secrets.toml` (git-ignored):

```toml
ANTHROPIC_API_KEY = "sk-ant-..."
```

Without it, the agent page renders but each question returns an API error message rather than an answer.

---

## Deploy to Streamlit Cloud

1. Push to GitHub.
2. Create the app at [share.streamlit.io](https://share.streamlit.io) pointing at this repository.
3. Main file path: `streamlit/app.py`.
4. **Settings → Secrets** → add `ANTHROPIC_API_KEY`.
5. Deploy. Subsequent pipeline runs update the app's data automatically — no redeploy needed, since the data lives in the repository rather than in the build.

---

## Links

- Live app — <https://globalwatch-fabric.streamlit.app/>
- Repository — [demonjd2026-afk/globalwatch-fabric](https://github.com/demonjd2026-afk/globalwatch-fabric)
- Architecture — [`../docs/architecture.md`](../docs/architecture.md)
- Data model — [`../docs/data_model.md`](../docs/data_model.md)
