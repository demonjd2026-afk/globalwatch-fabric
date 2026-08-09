# GlobalWatch — Air Quality Intelligence Platform

A Streamlit app for the GlobalWatch portfolio project built on Microsoft Fabric.

## Features

- **📊 Dashboard** — KPI cards, PM2.5 bar chart, AQI donut, station map, ML predictions
- **🤖 AI Agent** — Natural language Q&A powered by Claude AI over real air quality data

## Architecture

Built on Microsoft Fabric:
- Bronze/Silver/Gold Lakehouse medallion
- KQL Eventhouse (2,205 real-time events)
- Random Forest ML model (96.15% accuracy)
- Direct Lake Power BI + RLS

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Deploy to Streamlit Cloud

1. Push to GitHub
2. Connect at share.streamlit.io
3. Set main file: `app.py`

## Data

JSON snapshots exported from Microsoft Fabric Gold lakehouse.
Updated daily via `pl_batch_globalwatch` pipeline.

## GitHub

[demonjd2026-afk/globalwatch-fabric](https://github.com/demonjd2026-afk/globalwatch-fabric)
