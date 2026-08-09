import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import requests

# ── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title="GlobalWatch — Air Quality Intelligence",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'Space Grotesk', sans-serif;
}

/* Dark atmospheric background */
.stApp {
    background: #0a0e1a;
    color: #e2e8f0;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0f1629;
    border-right: 1px solid #1e2d4a;
}

/* Metric cards */
.metric-card {
    background: linear-gradient(135deg, #0f1629 0%, #1a2540 100%);
    border: 1px solid #1e3a5f;
    border-radius: 12px;
    padding: 20px 24px;
    text-align: center;
    position: relative;
    overflow: hidden;
}
.metric-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, #00d4ff, #0088cc);
}
.metric-value {
    font-size: 2.4rem;
    font-weight: 700;
    color: #00d4ff;
    line-height: 1;
    font-family: 'JetBrains Mono', monospace;
}
.metric-label {
    font-size: 0.75rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-top: 6px;
}

/* AQI badges */
.aqi-good { color: #22c55e; font-weight: 600; }
.aqi-moderate { color: #f59e0b; font-weight: 600; }
.aqi-unhealthy-sensitive { color: #f97316; font-weight: 600; }
.aqi-unhealthy { color: #ef4444; font-weight: 600; }
.aqi-hazardous { color: #dc2626; font-weight: 700; }

/* Chat messages */
.chat-user {
    background: #1e3a5f;
    border-radius: 12px 12px 4px 12px;
    padding: 12px 16px;
    margin: 8px 0;
    margin-left: 20%;
    border: 1px solid #2563eb;
}
.chat-agent {
    background: #0f1629;
    border-radius: 12px 12px 12px 4px;
    padding: 12px 16px;
    margin: 8px 0;
    margin-right: 20%;
    border: 1px solid #1e2d4a;
}

/* Header */
.app-header {
    background: linear-gradient(135deg, #0a0e1a 0%, #0f1e35 100%);
    border-bottom: 1px solid #1e3a5f;
    padding: 16px 0;
    margin-bottom: 24px;
}

/* Section headers */
h1, h2, h3 {
    color: #e2e8f0 !important;
}

/* Plotly chart background */
.js-plotly-plot {
    border-radius: 12px;
    overflow: hidden;
}

/* Input styling */
.stTextInput > div > div > input {
    background: #0f1629;
    border: 1px solid #1e3a5f;
    color: #e2e8f0;
    border-radius: 8px;
}

/* Button */
.stButton > button {
    background: linear-gradient(135deg, #0088cc, #00d4ff);
    color: #0a0e1a;
    font-weight: 600;
    border: none;
    border-radius: 8px;
    padding: 8px 24px;
}
.stButton > button:hover {
    background: linear-gradient(135deg, #00d4ff, #0088cc);
    transform: translateY(-1px);
}

/* Tab styling */
.stTabs [data-baseweb="tab-list"] {
    background: #0f1629;
    border-bottom: 1px solid #1e2d4a;
    gap: 4px;
}
.stTabs [data-baseweb="tab"] {
    color: #64748b;
    font-weight: 500;
}
.stTabs [aria-selected="true"] {
    color: #00d4ff !important;
    border-bottom-color: #00d4ff !important;
}
</style>
""", unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────
GITHUB_RAW = "https://raw.githubusercontent.com/demonjd2026-afk/globalwatch-fabric/main/streamlit/data"

@st.cache_data(ttl=1800)  # refresh every 30 minutes — matches real-time pipeline
def load_data():
    def read_jsonl(url):
        r = requests.get(url, timeout=30)
        rows = [json.loads(line) for line in r.text.strip().split("\n") if line.strip()]
        return pd.DataFrame(rows)

    fact     = read_jsonl(f"{GITHUB_RAW}/fact_readings.json")
    country  = read_jsonl(f"{GITHUB_RAW}/dim_country.json")
    station  = read_jsonl(f"{GITHUB_RAW}/dim_station.json")
    pred     = read_jsonl(f"{GITHUB_RAW}/fact_aqi_predictions.json")

    fact = fact.merge(country[["country_sk","country_name","country_code","continent"]],
                      on="country_sk", how="left")
    pred = pred.merge(country[["country_sk","country_name","country_code"]],
                      on="country_sk", how="left")

    return fact, country, station, pred

@st.cache_data(ttl=1800)  # refresh every 30 minutes
def load_kql_stats():
    try:
        r = requests.get(f"{GITHUB_RAW}/kql_stats.json", timeout=30)
        return r.json() if r.status_code == 200 else {}
    except:
        return {
            "events_sent_this_run": 0,
            "exported_at": "N/A",
            "status": "N/A"
        }

fact_df, country_df, station_df, pred_df = load_data()
kql_stats = load_kql_stats()


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
    st.markdown("""
    <div style='font-size:0.72rem; color:#64748b; line-height:1.8'>
        <b style='color:#94a3b8'>Data source</b><br>
        OpenAQ v3 API<br>
        <b style='color:#94a3b8'>Pipeline</b><br>
        Microsoft Fabric<br>
        Bronze → Silver → Gold<br>
        <b style='color:#94a3b8'>ML Model</b><br>
        Random Forest<br>
        96.15% accuracy<br>
        <b style='color:#94a3b8'>Last updated</b><br>
        {last_updated}
    </div>
    """.format(last_updated=pd.Timestamp.now().strftime("%d %b %Y")), unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("""
    <div style='font-size:0.7rem; color:#334155'>
        Built on Microsoft Fabric<br>
        Lakehouse + KQL + Eventstream<br>
        <a href='https://github.com/demonjd2026-afk/globalwatch-fabric'
           style='color:#0088cc'>GitHub →</a>
    </div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# PAGE 1 — DASHBOARD
# ══════════════════════════════════════════════════════════════
if "Dashboard" in page:

    st.markdown("""
    <div style='margin-bottom:8px'>
        <span style='font-size:0.75rem; color:#00d4ff; text-transform:uppercase;
                     letter-spacing:0.15em; font-family:JetBrains Mono'>
            LIVE AIR QUALITY INTELLIGENCE PLATFORM
        </span>
    </div>
    <h1 style='font-size:2rem; margin:0 0 4px; font-weight:700'>
        World Air Quality Dashboard
    </h1>
    <p style='color:#64748b; margin:0 0 24px; font-size:0.9rem'>
        Real-time monitoring across {country_count} countries · {station_count} active stations · WHO guideline tracking
    </p>
    """.format(
        country_count=fact_df["country_name"].nunique(),
        station_count=fact_df["location_id"].nunique()
    ), unsafe_allow_html=True)

    # ── KPI Cards ──
    total   = len(fact_df)
    stations = fact_df["location_id"].nunique()
    countries = fact_df["country_name"].nunique()
    hazardous = len(fact_df[fact_df["aqi_category"] == "Hazardous"])
    who_exc  = fact_df["exceeds_who_guideline"].sum()
    rt_events = kql_stats.get("events_sent_this_run", 0)
    rt_exported = kql_stats.get("exported_at", "N/A")

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    for col, val, label in [
        (c1, total, "Total Readings"),
        (c2, stations, "Active Stations"),
        (c3, countries, "Countries"),
        (c4, int(who_exc), "WHO Exceedances"),
        (c5, hazardous, "Hazardous Readings"),
        (c6, rt_events, "⚡ RT Events/Run"),
    ]:
        with col:
            st.markdown(f"""
            <div class='metric-card'>
                <div class='metric-value'>{val}</div>
                <div class='metric-label'>{label}</div>
            </div>
            """, unsafe_allow_html=True)

    # Real-time sync info
    st.markdown(f"""
    <div style='text-align:right; font-size:0.7rem; color:#334155; margin-top:4px'>
        ⚡ Real-time stream last synced: {rt_exported}
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Row 2: Full width map ──
    map_df = station_df.merge(
        fact_df[fact_df["parameter"]=="pm25"].groupby("location_id")["value"].mean().reset_index(),
        on="location_id", how="left"
    ).dropna(subset=["latitude","longitude","value"])

    fig_map = px.scatter_mapbox(
        map_df,
        lat="latitude", lon="longitude",
        size="value", color="value",
        hover_name="location_name",
        hover_data={"city": True, "country_code": True, "value": ":.1f"},
        color_continuous_scale=["#22c55e","#f59e0b","#ef4444","#dc2626"],
        size_max=20,
        zoom=1,
        title="🗺️ PM2.5 by Station — Global View (bubble size = concentration)",
        mapbox_style="carto-darkmatter",
    )
    fig_map.update_layout(
        paper_bgcolor="#0f1629",
        font=dict(color="#e2e8f0", family="Space Grotesk"),
        title_font=dict(size=14, color="#94a3b8"),
        coloraxis_showscale=False,
        coloraxis_colorbar=dict(bgcolor="#0f1629"),
        margin=dict(l=0, r=0, t=40, b=0),
        height=420,
    )
    st.plotly_chart(fig_map, use_container_width=True)

    # ── Row 3: Bar chart + Donut ──
    col_left, col_right = st.columns([3, 2])

    with col_left:
        pm25 = fact_df[fact_df["parameter"] == "pm25"].groupby("country_name")["value"].mean().reset_index()
        pm25.columns = ["Country", "Avg PM2.5 (µg/m³)"]
        pm25 = pm25.sort_values("Avg PM2.5 (µg/m³)", ascending=True)

        fig_bar = px.bar(
            pm25, x="Avg PM2.5 (µg/m³)", y="Country",
            orientation="h",
            title="Average PM2.5 by Country",
            color="Avg PM2.5 (µg/m³)",
            color_continuous_scale=["#22c55e", "#f59e0b", "#ef4444", "#dc2626"],
        )
        fig_bar.update_layout(
            plot_bgcolor="#0f1629", paper_bgcolor="#0f1629",
            font=dict(color="#e2e8f0", family="Space Grotesk"),
            title_font=dict(size=14, color="#94a3b8"),
            coloraxis_showscale=False,
            margin=dict(l=0, r=10, t=40, b=10),
            xaxis=dict(gridcolor="#1e2d4a", color="#64748b"),
            yaxis=dict(color="#94a3b8"),
            height=320,
        )
        fig_bar.add_vline(x=15, line_dash="dash", line_color="#00d4ff",
                          annotation_text="WHO limit", annotation_font_color="#00d4ff")
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_right:
        aqi_counts = fact_df["aqi_category"].value_counts().reset_index()
        aqi_counts.columns = ["AQI Category", "Count"]

        color_map = {
            "Good": "#22c55e", "Moderate": "#f59e0b",
            "Unhealthy for Sensitive": "#f97316",
            "Unhealthy": "#ef4444", "Hazardous": "#dc2626", "N/A": "#334155"
        }
        colors = [color_map.get(c, "#334155") for c in aqi_counts["AQI Category"]]

        fig_donut = go.Figure(go.Pie(
            labels=aqi_counts["AQI Category"],
            values=aqi_counts["Count"],
            hole=0.6,
            marker_colors=colors,
            textinfo="percent",
            textfont=dict(color="#e2e8f0", size=11),
        ))
        fig_donut.update_layout(
            title="AQI Category Distribution",
            title_font=dict(size=14, color="#94a3b8"),
            plot_bgcolor="#0f1629", paper_bgcolor="#0f1629",
            font=dict(color="#e2e8f0", family="Space Grotesk"),
            legend=dict(
                font=dict(color="#94a3b8", size=10),
                bgcolor="#0f1629",
                bordercolor="#1e2d4a",
                borderwidth=1,
            ),
            margin=dict(l=0, r=0, t=40, b=10),
            height=320,
        )
        st.plotly_chart(fig_donut, use_container_width=True)

    # ── Row 3: ML predictions full width ──
    pred_counts = pred_df["predicted_aqi_class"].value_counts().reset_index()
    pred_counts.columns = ["Predicted AQI", "Stations"]
    pred_colors = [color_map.get(c, "#334155") for c in pred_counts["Predicted AQI"]]

    fig_pred = go.Figure(go.Bar(
        x=pred_counts["Predicted AQI"],
        y=pred_counts["Stations"],
        marker_color=pred_colors,
        text=pred_counts["Stations"],
        textposition="outside",
        textfont=dict(color="#94a3b8"),
    ))
    fig_pred.update_layout(
        title="ML-Predicted AQI Classes",
        title_font=dict(size=14, color="#94a3b8"),
        plot_bgcolor="#0f1629", paper_bgcolor="#0f1629",
        font=dict(color="#e2e8f0", family="Space Grotesk"),
        xaxis=dict(color="#64748b", gridcolor="#1e2d4a"),
        yaxis=dict(color="#64748b", gridcolor="#1e2d4a"),
        margin=dict(l=0, r=0, t=40, b=10),
        height=300,
        showlegend=False,
    )
    st.plotly_chart(fig_pred, use_container_width=True)

    # ── Row 4: Top polluted stations ──
    st.markdown("### 🚨 Top 10 Most Polluted Stations")
    top_stations = fact_df[fact_df["parameter"]=="pm25"] \
        .merge(station_df[["location_id","location_name","city"]], on="location_id", how="left") \
        .nlargest(10, "value")[["location_name","city","country_name","value","aqi_category"]] \
        .rename(columns={"location_name":"Station","city":"City",
                          "country_name":"Country","value":"PM2.5 (µg/m³)","aqi_category":"AQI"})

    def color_aqi(val):
        colors = {"Good":"#22c55e","Moderate":"#f59e0b","Unhealthy":"#ef4444",
                  "Hazardous":"#dc2626","N/A":"#64748b"}
        c = colors.get(val, "#64748b")
        return f"color: {c}; font-weight: 600"

    st.dataframe(
        top_stations.style.map(color_aqi, subset=["AQI"]),
        use_container_width=True,
        hide_index=True,
    )


# ══════════════════════════════════════════════════════════════
# PAGE 2 — AI AGENT
# ══════════════════════════════════════════════════════════════
else:
    st.markdown("""
    <div style='margin-bottom:8px'>
        <span style='font-size:0.75rem; color:#00d4ff; text-transform:uppercase;
                     letter-spacing:0.15em; font-family:JetBrains Mono'>
            NATURAL LANGUAGE AIR QUALITY AGENT
        </span>
    </div>
    <h1 style='font-size:2rem; margin:0 0 4px; font-weight:700'>
        GlobalWatch AI Agent
    </h1>
    <p style='color:#64748b; margin:0 0 24px; font-size:0.9rem'>
        Ask questions about air quality data in plain English
    </p>
    """, unsafe_allow_html=True)

    # Build data context for Claude
    pm25_by_country = fact_df[fact_df["parameter"]=="pm25"] \
        .groupby("country_name")["value"].mean().round(2).to_dict()
    who_by_country = fact_df.groupby("country_name")["exceeds_who_guideline"].sum().to_dict()
    aqi_dist = fact_df["aqi_category"].value_counts().to_dict()
    pred_dist = pred_df["predicted_aqi_class"].value_counts().to_dict()

    DATA_CONTEXT = f"""
You are GlobalWatch AI Agent, an expert on air quality data from Microsoft Fabric.
You have access to real air quality data from the GlobalWatch platform built on Microsoft Fabric.

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

ARCHITECTURE (for technical questions):
- Ingestion: Fabric Eventstream (real-time) + Data Factory (batch)
- Storage: Bronze/Silver/Gold Lakehouse medallion on OneLake
- Processing: Apache Spark with AQE, Delta Lake MERGE, SCD2
- Real-time: KQL Eventhouse with update policies (2,205 events)
- ML: Random Forest classifier via Spark MLlib + MLflow tracking
- Serving: Direct Lake Power BI + RLS by continent
- Orchestration: pl_batch_globalwatch (daily) + pl_realtime_globalwatch (hourly)

WHO PM2.5 GUIDELINE: 15 µg/m³ annual mean
Answer questions accurately based on this data. Be concise but insightful.
For technical questions about the pipeline architecture, explain clearly.
Always cite specific numbers from the data.
"""

    # Suggested questions
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
            if st.button(sug, key=f"sug_{i}", use_container_width=True):
                st.session_state.setdefault("messages", [])
                st.session_state["pending_question"] = sug

    st.markdown("---")

    # Chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Handle suggested question
    if "pending_question" in st.session_state:
        q = st.session_state.pop("pending_question")
        st.session_state.messages.append({"role": "user", "content": q})

    # Display chat history
    for msg in st.session_state.messages:
        if msg["role"] == "user":
            st.markdown(f"""
            <div class='chat-user'>
                <span style='font-size:0.7rem; color:#64748b'>You</span><br>
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

    # Get AI response for last unanswered user message
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
                        "messages": [
                            {"role": m["role"], "content": m["content"]}
                            for m in msgs
                        ]
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

    # Input box
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
