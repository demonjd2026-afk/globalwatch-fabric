import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import requests

st.set_page_config(
    page_title="GlobalWatch — Air Quality Intelligence",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap');

html, body, [class*="css"] { font-family: 'Space Grotesk', sans-serif; }

/* ── App background — subtle radial glow ── */
.stApp {
    background:
        radial-gradient(ellipse 80% 50% at 50% -10%, rgba(0, 136, 204, 0.12), transparent),
        radial-gradient(ellipse 60% 40% at 90% 110%, rgba(0, 212, 255, 0.05), transparent),
        #0a0e1a;
    color: #e2e8f0;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: rgba(15, 22, 41, 0.85);
    backdrop-filter: blur(12px);
    border-right: 1px solid rgba(30, 58, 95, 0.5);
}

/* ── Glassmorphism KPI cards ── */
.metric-card {
    background: linear-gradient(135deg, rgba(15, 22, 41, 0.7), rgba(26, 37, 64, 0.7));
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    border: 1px solid rgba(0, 212, 255, 0.15);
    border-radius: 16px;
    padding: 20px 12px;
    text-align: center;
    position: relative;
    overflow: hidden;
    height: 104px;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    transition: all 0.25s ease;
    box-shadow: 0 4px 24px rgba(0, 0, 0, 0.3);
}
.metric-card:hover {
    transform: translateY(-3px);
    border-color: rgba(0, 212, 255, 0.45);
    box-shadow: 0 8px 32px rgba(0, 136, 204, 0.25);
}
.metric-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent, #00d4ff, transparent);
}
.metric-value {
    font-size: 1.9rem;
    font-weight: 700;
    background: linear-gradient(135deg, #00d4ff, #66e5ff);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    line-height: 1;
    font-family: 'JetBrains Mono', monospace;
}
.metric-label {
    font-size: 0.62rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-top: 8px;
    font-weight: 500;
}

/* ── Chart card containers ── */
.element-container:has(.js-plotly-plot) {
    background: rgba(15, 22, 41, 0.55);
    border: 1px solid rgba(30, 58, 95, 0.45);
    border-radius: 16px;
    padding: 8px;
    box-shadow: 0 4px 24px rgba(0, 0, 0, 0.25);
    transition: border-color 0.25s ease;
}
.element-container:has(.js-plotly-plot):hover {
    border-color: rgba(0, 212, 255, 0.3);
}
.js-plotly-plot { border-radius: 12px; overflow: hidden; }

/* ── Section headers ── */
.section-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin: 32px 0 16px;
}
.section-header .accent {
    width: 4px;
    height: 22px;
    background: linear-gradient(180deg, #00d4ff, #0088cc);
    border-radius: 2px;
}
.section-header .title {
    font-size: 1.15rem;
    font-weight: 600;
    color: #e2e8f0;
}
.section-gap { margin: 28px 0; }

/* ── Dataframe styling ── */
[data-testid="stDataFrame"] {
    background: rgba(15, 22, 41, 0.55);
    border: 1px solid rgba(30, 58, 95, 0.45);
    border-radius: 16px;
    padding: 8px;
}

/* ── Chat messages ── */
.chat-user {
    background: linear-gradient(135deg, rgba(30, 58, 95, 0.85), rgba(37, 99, 235, 0.25));
    backdrop-filter: blur(8px);
    border-radius: 14px 14px 4px 14px;
    padding: 14px 18px;
    margin: 10px 0;
    margin-left: 18%;
    border: 1px solid rgba(37, 99, 235, 0.5);
    color: #e2e8f0;
}
.chat-agent {
    background: rgba(15, 22, 41, 0.75);
    backdrop-filter: blur(8px);
    border-radius: 14px 14px 14px 4px;
    padding: 14px 18px;
    margin: 10px 0;
    margin-right: 18%;
    border: 1px solid rgba(30, 45, 74, 0.8);
    color: #e2e8f0;
}

h1, h2, h3 { color: #e2e8f0 !important; }

/* ── Inputs ── */
.stTextInput > div > div > input {
    background: rgba(15, 22, 41, 0.75);
    border: 1px solid rgba(30, 58, 95, 0.6);
    color: #e2e8f0;
    border-radius: 10px;
    padding: 10px 14px;
}
.stTextInput > div > div > input:focus {
    border-color: #00d4ff;
    box-shadow: 0 0 0 2px rgba(0, 212, 255, 0.15);
}

/* ── Buttons ── */
.stButton > button {
    background: linear-gradient(135deg, #0088cc, #00d4ff);
    color: #0a0e1a;
    font-weight: 600;
    border: none;
    border-radius: 10px;
    padding: 8px 24px;
    transition: all 0.2s ease;
}
.stButton > button:hover {
    transform: translateY(-1px);
    box-shadow: 0 6px 20px rgba(0, 212, 255, 0.35);
}

/* ── Modebar dark ── */
.js-plotly-plot .plotly .modebar { background: transparent !important; }
.js-plotly-plot .plotly .modebar-btn path { fill: #475569 !important; }
.js-plotly-plot .plotly .modebar-btn:hover path { fill: #00d4ff !important; }

/* ── Radio nav in sidebar ── */
[data-testid="stSidebar"] .stRadio label {
    padding: 6px 10px;
    border-radius: 8px;
    transition: background 0.2s ease;
}
[data-testid="stSidebar"] .stRadio label:hover {
    background: rgba(0, 212, 255, 0.08);
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: #0a0e1a; }
::-webkit-scrollbar-thumb { background: #1e3a5f; border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: #00d4ff; }
</style>
""", unsafe_allow_html=True)

# ── Plotly dark theme helper ──────────────────────────────────
CHART_DEFAULTS = dict(
    paper_bgcolor="#0f1629",
    plot_bgcolor="#0f1629",
    font=dict(color="#94a3b8", family="Space Grotesk"),
    title_font=dict(size=14, color="#94a3b8"),
    modebar=dict(
        bgcolor="#0f1629",
        color="#475569",
        activecolor="#00d4ff",
    ),
)

PLOTLY_CONFIG = {
    "displaylogo": False,
    "modeBarButtonsToRemove": ["toImage", "sendDataToCloud"],
}

# ── Data loading ──────────────────────────────────────────────
GITHUB_RAW = "https://raw.githubusercontent.com/demonjd2026-afk/globalwatch-fabric/main/streamlit/data"

@st.cache_data(ttl=1800)
def load_data():
    def read_jsonl(url):
        r = requests.get(url, timeout=30)
        rows = [json.loads(line) for line in r.text.strip().split("\n") if line.strip()]
        return pd.DataFrame(rows)

    fact    = read_jsonl(f"{GITHUB_RAW}/fact_readings.json")
    country = read_jsonl(f"{GITHUB_RAW}/dim_country.json")
    station = read_jsonl(f"{GITHUB_RAW}/dim_station.json")
    pred    = read_jsonl(f"{GITHUB_RAW}/fact_aqi_predictions.json")

    fact = fact.merge(country[["country_sk","country_name","country_code","continent"]],
                      on="country_sk", how="left")
    pred = pred.merge(country[["country_sk","country_name","country_code"]],
                      on="country_sk", how="left")
    return fact, country, station, pred

@st.cache_data(ttl=1800)
def load_kql_stats():
    try:
        r = requests.get(f"{GITHUB_RAW}/kql_stats.json", timeout=30)
        return r.json() if r.status_code == 200 else {}
    except:
        return {"events_sent_this_run": 0, "exported_at": "N/A", "status": "N/A"}

fact_df, country_df, station_df, pred_df = load_data()
kql_stats = load_kql_stats()

# ── Color map ─────────────────────────────────────────────────
AQI_COLORS = {
    "Good": "#22c55e",
    "Moderate": "#f59e0b",
    "Unhealthy for Sensitive": "#f97316",
    "Unhealthy": "#ef4444",
    "Hazardous": "#dc2626",
    "N/A": "#334155"
}

def aqi_color(val):
    return AQI_COLORS.get(val, "#334155")

def pm25_color(v):
    if v > 150: return "#dc2626"
    if v > 55:  return "#ef4444"
    if v > 35:  return "#f59e0b"
    return "#22c55e"

# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='text-align:center; padding: 8px 0 20px'>
        <div style='font-size:2rem'>🌍</div>
        <div style='font-size:1.1rem; font-weight:700; color:#00d4ff'>GlobalWatch</div>
        <div style='font-size:0.7rem; color:#64748b; letter-spacing:0.1em'>AIR QUALITY INTELLIGENCE</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("---")
    page = st.radio("Navigate", ["📊 Dashboard", "🤖 AI Agent"], label_visibility="collapsed")
    st.markdown("---")
    st.markdown(f"""
    <div style='font-size:0.72rem; color:#64748b; line-height:1.8'>
        <b style='color:#94a3b8'>Data source</b><br>OpenAQ v3 API<br>
        <b style='color:#94a3b8'>Pipeline</b><br>Microsoft Fabric<br>Bronze → Silver → Gold<br>
        <b style='color:#94a3b8'>ML Model</b><br>Random Forest · 96.15% accuracy<br>
        <b style='color:#94a3b8'>Last updated</b><br>{pd.Timestamp.now().strftime("%d %b %Y")}
    </div>
    """, unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("""
    <div style='font-size:0.7rem; color:#334155'>
        Built on Microsoft Fabric<br>Lakehouse + KQL + Eventstream<br>
        <a href='https://github.com/demonjd2026-afk/globalwatch-fabric' style='color:#0088cc'>GitHub →</a>
    </div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# PAGE 1 — DASHBOARD
# ══════════════════════════════════════════════════════════════
if "Dashboard" in page:

    st.markdown("""
    <div style='display:flex; align-items:center; gap:10px; margin-bottom:10px'>
        <span style='display:inline-flex; align-items:center; gap:6px;
                     background:rgba(34,197,94,0.12); border:1px solid rgba(34,197,94,0.35);
                     color:#22c55e; font-size:0.68rem; font-weight:600; padding:4px 12px;
                     border-radius:999px; letter-spacing:0.08em; font-family:JetBrains Mono'>
            <span style='width:7px; height:7px; background:#22c55e; border-radius:50%;
                         box-shadow:0 0 8px #22c55e; display:inline-block'></span>
            LIVE
        </span>
        <span style='font-size:0.72rem; color:#00d4ff; text-transform:uppercase;
                     letter-spacing:0.15em; font-family:JetBrains Mono'>
            AIR QUALITY INTELLIGENCE PLATFORM
        </span>
    </div>
    <h1 style='font-size:2.3rem; margin:0 0 6px; font-weight:700; line-height:1.1'>
        World Air Quality
        <span style='background:linear-gradient(135deg,#00d4ff,#66e5ff);
                     -webkit-background-clip:text; -webkit-text-fill-color:transparent'>Dashboard</span>
    </h1>
    <p style='color:#64748b; margin:0 0 28px; font-size:0.92rem'>
        Real-time air quality intelligence powered by Microsoft Fabric · OpenAQ v3 · WHO guideline tracking
    </p>
    """, unsafe_allow_html=True)

    # ── KPI Cards — equal size ──
    total     = len(fact_df)
    stations  = fact_df["location_id"].nunique()
    countries = fact_df["country_name"].nunique()
    hazardous = len(fact_df[fact_df["aqi_category"] == "Hazardous"])
    who_exc   = int(fact_df["exceeds_who_guideline"].sum())
    rt_events = kql_stats.get("events_sent_this_run", 0)
    rt_time   = kql_stats.get("exported_at", "N/A")

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    for col, val, label in [
        (c1, total,     "Total Readings"),
        (c2, stations,  "Active Stations"),
        (c3, countries, "Countries"),
        (c4, who_exc,   "WHO Exceedances"),
        (c5, hazardous, "Hazardous"),
        (c6, rt_events, "⚡ RT Events/Run"),
    ]:
        with col:
            st.markdown(f"""
            <div class='metric-card'>
                <div class='metric-value'>{val}</div>
                <div class='metric-label'>{label}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown(f"""
    <div style='text-align:right; font-size:0.7rem; color:#334155; margin-top:6px; margin-bottom:28px'>
        ⚡ Real-time stream last synced: {rt_time}
    </div>
    """, unsafe_allow_html=True)

    # ── Map — full width ──
    map_df = station_df.merge(
        fact_df[fact_df["parameter"]=="pm25"]
            .groupby("location_id")["value"].mean().reset_index(),
        on="location_id", how="left"
    ).dropna(subset=["latitude","longitude","value"])

    fig_map = go.Figure(go.Scattermap(
        lat=map_df["latitude"],
        lon=map_df["longitude"],
        mode="markers",
        marker=dict(
            size=[max(6, min(30, v/5)) for v in map_df["value"]],
            color=[pm25_color(v) for v in map_df["value"]],
            opacity=0.85,
        ),
        text=map_df["location_name"],
        customdata=map_df[["city","country_code","value"]],
        hovertemplate=(
            "<b>%{text}</b><br>"
            "City: %{customdata[0]}<br>"
            "Country: %{customdata[1]}<br>"
            "PM2.5: %{customdata[2]:.1f} µg/m³"
            "<extra></extra>"
        ),
    ))
    fig_map.update_layout(
        **CHART_DEFAULTS,
        title="🗺️ PM2.5 by Station — Global View (bubble size = concentration)",
        map=dict(style="carto-darkmatter", zoom=1, center=dict(lat=20, lon=10)),
        margin=dict(l=10, r=10, t=50, b=10),
        height=420,
        showlegend=False,
    )
    st.plotly_chart(fig_map, config=PLOTLY_CONFIG, theme=None, width='stretch')

    st.markdown("<div class='section-gap'></div>", unsafe_allow_html=True)

    # ── Bar chart + Donut ──
    col_left, col_right = st.columns([3, 2])

    with col_left:
        pm25 = fact_df[fact_df["parameter"]=="pm25"]\
            .groupby("country_name")["value"].mean().reset_index()
        pm25.columns = ["Country", "Avg PM2.5"]
        pm25 = pm25.sort_values("Avg PM2.5", ascending=True)

        fig_bar = go.Figure(go.Bar(
            x=pm25["Avg PM2.5"],
            y=pm25["Country"],
            orientation="h",
            marker_color=[pm25_color(v) for v in pm25["Avg PM2.5"]],
            hovertemplate="<b>%{y}</b><br>Avg PM2.5: %{x:.1f} µg/m³<extra></extra>",
        ))
        fig_bar.add_vline(
            x=15, line_dash="dash", line_color="#00d4ff",
            annotation_text="WHO limit",
            annotation_font_color="#00d4ff",
            annotation_font_size=10,
        )
        fig_bar.update_layout(
            **CHART_DEFAULTS,
            title="Average PM2.5 by Country (µg/m³)",
            xaxis=dict(gridcolor="#1e2d4a", color="#64748b",
                       title=dict(text="Avg PM2.5 (µg/m³)", standoff=10),
                       automargin=True),
            yaxis=dict(color="#94a3b8", automargin=True,
                       tickmode="linear", dtick=1,
                       ticksuffix="   ", tickfont=dict(size=11)),
            margin=dict(l=10, r=20, t=50, b=40),
            height=max(420, len(pm25) * 24),
            showlegend=False,
        )
        st.plotly_chart(fig_bar, config=PLOTLY_CONFIG, theme=None, width='stretch')

    with col_right:
        aqi_counts = fact_df["aqi_category"].value_counts().reset_index()
        aqi_counts.columns = ["AQI Category", "Count"]
        colors = [aqi_color(c) for c in aqi_counts["AQI Category"]]

        fig_donut = go.Figure(go.Pie(
            labels=aqi_counts["AQI Category"],
            values=aqi_counts["Count"],
            hole=0.6,
            marker_colors=colors,
            textinfo="percent",
            textfont=dict(color="#e2e8f0", size=11),
            hovertemplate="<b>%{label}</b><br>Count: %{value}<br>%{percent}<extra></extra>",
        ))
        fig_donut.update_layout(
            **CHART_DEFAULTS,
            title="AQI Category Distribution",
            showlegend=True,
            legend=dict(
                font=dict(color="#cbd5e1", size=11),
                bgcolor="rgba(10,14,26,0.85)",
                bordercolor="rgba(0,212,255,0.25)",
                borderwidth=1,
                orientation="h",
                yanchor="top", y=-0.05,
                xanchor="center", x=0.5,
            ),
            margin=dict(l=10, r=10, t=50, b=10),
            height=420,
        )
        st.plotly_chart(fig_donut, config=PLOTLY_CONFIG, theme=None, width='stretch')

    st.markdown("<div class='section-gap'></div>", unsafe_allow_html=True)

    # ── ML Predictions ──
    pred_counts = pred_df["predicted_aqi_class"].value_counts().reset_index()
    pred_counts.columns = ["Predicted AQI", "Stations"]

    fig_pred = go.Figure(go.Bar(
        x=pred_counts["Predicted AQI"],
        y=pred_counts["Stations"],
        marker_color=[aqi_color(c) for c in pred_counts["Predicted AQI"]],
        text=pred_counts["Stations"],
        textposition="outside",
        textfont=dict(color="#e2e8f0", size=13),
        cliponaxis=False,
        hovertemplate="<b>%{x}</b><br>Stations: %{y}<extra></extra>",
    ))
    fig_pred.update_layout(
        **CHART_DEFAULTS,
        title="ML-Predicted AQI Classes (Random Forest · 96.15% accuracy)",
        xaxis=dict(color="#94a3b8", gridcolor="#1e2d4a", automargin=True,
                   showgrid=False),
        yaxis=dict(color="#64748b", gridcolor="#1e2d4a", automargin=True,
                   range=[0, pred_counts["Stations"].max() * 1.18]),
        margin=dict(l=10, r=10, t=60, b=40),
        height=340,
        showlegend=False,
    )
    st.plotly_chart(fig_pred, config=PLOTLY_CONFIG, theme=None, width='stretch')

    st.markdown("<div class='section-gap'></div>", unsafe_allow_html=True)

    # ── Top 10 Most Polluted Stations ──
    st.markdown("""
    <div class='section-header'>
        <div class='accent'></div>
        <div class='title'>🚨 Top 10 Most Polluted Stations</div>
    </div>
    """, unsafe_allow_html=True)
    top_stations = fact_df[fact_df["parameter"]=="pm25"] \
        .merge(station_df[["location_id","location_name","city"]], on="location_id", how="left") \
        .groupby(["location_name","city","country_name","aqi_category"])["value"].max().reset_index() \
        .nlargest(10, "value") \
        .rename(columns={
            "location_name": "Station",
            "city": "City",
            "country_name": "Country",
            "value": "PM2.5 (µg/m³)",
            "aqi_category": "AQI"
        })
    top_stations["PM2.5 (µg/m³)"] = top_stations["PM2.5 (µg/m³)"].round(1)

    def color_aqi(val):
        c = AQI_COLORS.get(val, "#64748b")
        return f"color: {c}; font-weight: 600"

    st.dataframe(
        top_stations.style.map(color_aqi, subset=["AQI"]),
        width='stretch',
        hide_index=True,
    )


# ══════════════════════════════════════════════════════════════
# PAGE 2 — AI AGENT
# ══════════════════════════════════════════════════════════════
else:
    st.markdown("""
    <div style='display:flex; align-items:center; gap:10px; margin-bottom:10px'>
        <span style='display:inline-flex; align-items:center; gap:6px;
                     background:rgba(0,212,255,0.1); border:1px solid rgba(0,212,255,0.35);
                     color:#00d4ff; font-size:0.68rem; font-weight:600; padding:4px 12px;
                     border-radius:999px; letter-spacing:0.08em; font-family:JetBrains Mono'>
            🤖 AI POWERED
        </span>
        <span style='font-size:0.72rem; color:#00d4ff; text-transform:uppercase;
                     letter-spacing:0.15em; font-family:JetBrains Mono'>
            NATURAL LANGUAGE AGENT
        </span>
    </div>
    <h1 style='font-size:2.3rem; margin:0 0 6px; font-weight:700; line-height:1.1'>
        GlobalWatch
        <span style='background:linear-gradient(135deg,#00d4ff,#66e5ff);
                     -webkit-background-clip:text; -webkit-text-fill-color:transparent'>AI Agent</span>
    </h1>
    <p style='color:#64748b; margin:0 0 24px; font-size:0.92rem'>
        Ask questions about air quality data in plain English — powered by Claude
    </p>
    """, unsafe_allow_html=True)

    pm25_by_country = fact_df[fact_df["parameter"]=="pm25"] \
        .groupby("country_name")["value"].mean().round(2).to_dict()
    who_by_country  = fact_df.groupby("country_name")["exceeds_who_guideline"].sum().to_dict()
    aqi_dist        = fact_df["aqi_category"].value_counts().to_dict()
    pred_dist       = pred_df["predicted_aqi_class"].value_counts().to_dict()

    DATA_CONTEXT = f"""
You are GlobalWatch AI Agent, an expert on air quality data from Microsoft Fabric.

KEY FACTS:
- Total readings: {len(fact_df)}
- Active stations: {fact_df['location_id'].nunique()}
- Countries monitored: {fact_df['country_name'].nunique()} ({', '.join(fact_df['country_name'].dropna().unique())})
- Data source: OpenAQ v3 API ingested via Fabric Eventstream + batch pipelines

AVERAGE PM2.5 BY COUNTRY (µg/m³):
{json.dumps(pm25_by_country, indent=2)}

WHO GUIDELINE EXCEEDANCES BY COUNTRY:
{json.dumps({k: int(v) for k,v in who_by_country.items()}, indent=2)}

AQI CATEGORY DISTRIBUTION:
{json.dumps(aqi_dist, indent=2)}

ML MODEL PREDICTIONS (Random Forest, 96.15% accuracy):
{json.dumps(pred_dist, indent=2)}

TOP POLLUTED STATIONS (PM2.5):
{fact_df[fact_df['parameter']=='pm25'].merge(station_df[['location_id','location_name','city']], on='location_id', how='left').nlargest(5,'value')[['location_name','city','country_name','value']].to_string(index=False)}

ARCHITECTURE:
- Ingestion: Fabric Eventstream (real-time) + Data Factory (batch)
- Storage: Bronze/Silver/Gold Lakehouse medallion on OneLake
- Processing: Apache Spark with AQE, Delta Lake MERGE, SCD2
- Real-time: KQL Eventhouse with update policies
- ML: Random Forest via Spark MLlib + MLflow tracking
- Serving: Direct Lake Power BI + RLS by continent
- Orchestration: pl_batch_globalwatch (daily) + pl_realtime_globalwatch (every 30 mins)

WHO PM2.5 GUIDELINE: 15 µg/m³ annual mean
Answer concisely and cite specific numbers.
"""

    st.markdown("**Try asking:**")
    cols = st.columns(3)
    suggestions = [
        "Which country has the worst air quality?",
        "How many stations exceed WHO guidelines?",
        "Explain the Fabric pipeline architecture",
        "What does the ML model predict for India?",
        "Compare PM2.5 across Asian countries",
        "What is a KQL update policy?",
    ]
    for i, sug in enumerate(suggestions):
        with cols[i % 3]:
            if st.button(sug, key=f"sug_{i}", width='stretch'):
                st.session_state.setdefault("messages", [])
                st.session_state["pending_question"] = sug

    st.markdown("---")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    if "pending_question" in st.session_state:
        q = st.session_state.pop("pending_question")
        st.session_state.messages.append({"role": "user", "content": q})

    for msg in st.session_state.messages:
        if msg["role"] == "user":
            st.markdown(f"""
            <div class='chat-user'>
                <span style='font-size:0.7rem; color:#94a3b8'>You</span><br>
                {msg['content']}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class='chat-agent'>
                <span style='font-size:0.7rem; color:#00d4ff'>🌍 GlobalWatch Agent</span><br>
                {msg['content']}
            </div>
            """, unsafe_allow_html=True)

    msgs = st.session_state.messages
    if msgs and msgs[-1]["role"] == "user":
        with st.spinner("Analyzing air quality data..."):
            try:
                response = requests.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "Content-Type": "application/json",
                        "x-api-key": st.secrets.get("ANTHROPIC_API_KEY", ""),
                        "anthropic-version": "2023-06-01"
                    },
                    json={
                        "model": "claude-sonnet-4-6",
                        "max_tokens": 1000,
                        "system": DATA_CONTEXT,
                        "messages": [{"role": m["role"], "content": m["content"]} for m in msgs]
                    },
                    timeout=30
                )
                resp_json = response.json()
                if "content" in resp_json:
                    answer = resp_json["content"][0]["text"]
                elif "error" in resp_json:
                    answer = f"API Error: {resp_json['error']['message']}"
                else:
                    answer = f"Unexpected response: {str(resp_json)[:200]}"
            except Exception as e:
                answer = f"Unable to connect to AI service. Error: {str(e)}"

        st.session_state.messages.append({"role": "assistant", "content": answer})
        st.rerun()

    with st.form("chat_form", clear_on_submit=True):
        col_input, col_btn = st.columns([5, 1])
        with col_input:
            user_input = st.text_input(
                "Ask about air quality...",
                placeholder="e.g. Which country has the highest PM2.5?",
                label_visibility="collapsed"
            )
        with col_btn:
            submitted = st.form_submit_button("Send →")

    if submitted and user_input.strip():
        st.session_state.messages.append({"role": "user", "content": user_input.strip()})
        st.rerun()

    if st.button("🗑️ Clear conversation", type="secondary"):
        st.session_state.messages = []
        st.rerun()
