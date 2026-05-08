"""
Part 4 — Real-Time Flight Analytics Dashboard
Aviation-themed dark dashboard powered by Plotly + Streamlit.
Reads from HBase via Thrift (happybase) and auto-refreshes every 15 seconds.

Sections:
  Header  — Live KPI cards (flights, countries, avg altitude, avg velocity,
             pipeline throughput, active alerts)
  Tab 1   — Global Traffic Map  (Plotly scatter_geo, altitude colour scale)
  Tab 2   — Traffic Analytics   (top-N countries, altitude / velocity distributions,
             country sliding-window trend, heatmap)
  Tab 3   — Fleet Intelligence  (top operators, aircraft models, manufacturer share)
  Tab 4   — Anomaly Centre      (alert table, descent-rate histogram, severity gauge)
"""

import os
import time

import happybase
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
HBASE_HOST       = os.getenv("HBASE_HOST", "localhost")
REFRESH_INTERVAL = 15      # seconds
TOP_N            = 20
MAX_ALERTS       = 200
MAX_AGG_ROWS     = 5000

# ---------------------------------------------------------------------------
# Page config & global CSS
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Flight Analytics | CS523",
    page_icon="✈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
/* ── Global dark aviation theme ── */
[data-testid="stAppViewContainer"] {
    background: #0a0e1a;
    color: #e0e6f0;
}
[data-testid="stHeader"] { background: transparent; }
[data-testid="stSidebar"] { background: #0d1220; }

/* ── KPI cards ── */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(6, 1fr);
    gap: 12px;
    margin-bottom: 18px;
}
.kpi-card {
    background: linear-gradient(135deg, #111827 0%, #1a2340 100%);
    border: 1px solid #1e3a5f;
    border-radius: 10px;
    padding: 16px 14px 12px;
    text-align: center;
}
.kpi-label {
    font-size: 0.70rem;
    color: #7ca0c8;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 6px;
}
.kpi-value {
    font-size: 1.7rem;
    font-weight: 700;
    color: #38bdf8;
    line-height: 1;
}
.kpi-sub {
    font-size: 0.68rem;
    color: #4a6a8a;
    margin-top: 4px;
}

/* ── Section headers ── */
.section-header {
    font-size: 0.78rem;
    font-weight: 600;
    color: #38bdf8;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    border-bottom: 1px solid #1e3a5f;
    padding-bottom: 6px;
    margin: 18px 0 10px;
}

/* ── Tabs ── */
[data-testid="stTabs"] button {
    color: #7ca0c8 !important;
    font-size: 0.82rem;
    font-weight: 600;
    letter-spacing: 0.06em;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color: #38bdf8 !important;
    border-bottom: 2px solid #38bdf8;
}

/* ── Dataframe ── */
[data-testid="stDataFrame"] { border: 1px solid #1e3a5f; border-radius: 8px; }

/* ── Alert badge ── */
.alert-badge {
    display: inline-block;
    background: #7f1d1d;
    color: #fca5a5;
    font-size: 0.7rem;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 999px;
    text-transform: uppercase;
}
.ok-badge {
    display: inline-block;
    background: #14532d;
    color: #86efac;
    font-size: 0.7rem;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 999px;
    text-transform: uppercase;
}

/* ── Plotly chart backgrounds ── */
.js-plotly-plot { border-radius: 10px; }

/* ── Pipeline label ── */
.pipeline-label {
    font-size: 0.68rem;
    color: #4a6a8a;
    text-align: right;
    margin-bottom: 4px;
}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Plotly layout defaults (dark theme)
# ---------------------------------------------------------------------------
PLOTLY_BASE = dict(
    paper_bgcolor="#111827",
    plot_bgcolor="#111827",
    font=dict(color="#c9d8eb", family="Inter, sans-serif", size=12),
    margin=dict(l=12, r=12, t=36, b=12),
    coloraxis_colorbar=dict(
        tickfont=dict(color="#c9d8eb"),
        title_font=dict(color="#c9d8eb"),
    ),
)

GEO_BASE = dict(
    bgcolor="#0a0e1a",
    showland=True, landcolor="#1a2340",
    showocean=True, oceancolor="#070d1a",
    showlakes=True, lakecolor="#0d1a2e",
    showcountries=True, countrycolor="#1e3a5f",
    showcoastlines=True, coastlinecolor="#1e3a5f",
    showframe=False,
    projection_type="natural earth",
)

# ---------------------------------------------------------------------------
# HBase helpers
# ---------------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def get_conn():
    return happybase.Connection(HBASE_HOST)


def _decode(d, key, default=""):
    v = d.get(key, b"")
    return v.decode(errors="replace") if v else default


def _float(d, key):
    try:
        return float(_decode(d, key, "0") or "0")
    except (ValueError, TypeError):
        return 0.0


def _int(d, key):
    try:
        return int(_decode(d, key, "0") or "0")
    except (ValueError, TypeError):
        return 0


@st.cache_data(ttl=14, show_spinner=False)
def load_flights(_conn_id):
    conn = get_conn()
    records = []
    for key, d in conn.table("flights_live").scan():
        try:
            lat = _float(d, b"pos:lat")
            lon = _float(d, b"pos:lon")
            if lat == 0.0 and lon == 0.0:
                continue
            on_ground = _decode(d, b"meta:on_ground", "True") == "True"
            records.append({
                "icao24":    key.decode(errors="replace"),
                "callsign":  _decode(d, b"info:callsign").strip(),
                "country":   _decode(d, b"info:country"),
                "lat":       lat,
                "lon":       lon,
                "altitude":  _float(d, b"pos:baro_alt"),
                "velocity":  _float(d, b"pos:velocity"),
                "vrate":     _float(d, b"pos:vrate"),
                "track":     _float(d, b"pos:track"),
                "on_ground": on_ground,
                "operator":  _decode(d, b"enrich:operator"),
                "model":     _decode(d, b"enrich:model"),
                "mfr":       _decode(d, b"enrich:mfr"),
                "typecode":  _decode(d, b"enrich:typecode"),
            })
        except Exception:
            continue
    df = pd.DataFrame(records)
    if not df.empty:
        df = df[~df["on_ground"]]
        df = df[df["altitude"] > 0]
    return df


@st.cache_data(ttl=14, show_spinner=False)
def load_agg(_conn_id):
    conn = get_conn()
    records = []
    for key, d in conn.table("flights_agg").scan(limit=MAX_AGG_ROWS):
        try:
            parts = key.decode(errors="replace").split("|")
            win_end = _int(d, b"m:window_end")
            records.append({
                "country":   parts[0],
                "window_end": pd.Timestamp(win_end, unit="s"),
                "count":      _int(d,   b"m:flight_count"),
                "avg_alt":    _float(d, b"m:avg_alt"),
                "max_alt":    _float(d, b"m:max_alt"),
                "avg_vel":    _float(d, b"m:avg_velocity"),
            })
        except Exception:
            continue
    return pd.DataFrame(records)


@st.cache_data(ttl=14, show_spinner=False)
def load_alerts(_conn_id):
    conn = get_conn()
    records = []
    for key, d in conn.table("flights_alerts").scan(limit=MAX_ALERTS):
        try:
            parts = key.decode(errors="replace").split("|")
            ts = _int(d, b"a:ts")
            records.append({
                "icao24":    parts[0],
                "callsign":  _decode(d, b"a:callsign"),
                "operator":  _decode(d, b"a:operator"),
                "country":   _decode(d, b"a:country"),
                "vrate":     _float(d, b"a:vrate"),
                "altitude":  _float(d, b"a:alt"),
                "timestamp": pd.Timestamp(ts, unit="s") if ts else pd.NaT,
            })
        except Exception:
            continue
    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("timestamp", ascending=False)
    return df


# ---------------------------------------------------------------------------
# Header — Pipeline label
# ---------------------------------------------------------------------------
st.markdown(
    '<div class="pipeline-label">'
    'OpenSky API &nbsp;→&nbsp; Kafka (3 partitions) &nbsp;→&nbsp; '
    'PySpark Structured Streaming &nbsp;→&nbsp; HBase &nbsp;→&nbsp; Dashboard'
    '</div>',
    unsafe_allow_html=True,
)
st.markdown(
    "## ✈ Real-Time Global Flight Analytics",
    unsafe_allow_html=False,
)

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
conn_id = int(time.time() // REFRESH_INTERVAL)   # cache key rotates on refresh

try:
    flights_df = load_flights(conn_id)
    agg_df     = load_agg(conn_id)
    alerts_df  = load_alerts(conn_id)
    data_ok    = True
except Exception as exc:
    st.error(f"HBase connection error — {exc}")
    st.stop()

airborne   = len(flights_df)
n_countries = flights_df["country"].nunique() if not flights_df.empty else 0
avg_alt    = round(flights_df["altitude"].mean(),  0) if not flights_df.empty else 0
avg_vel    = round(flights_df["velocity"].mean(),  1) if not flights_df.empty else 0
n_alerts   = len(alerts_df)
n_enriched = int(flights_df["operator"].ne("").sum()) if not flights_df.empty else 0

# ---------------------------------------------------------------------------
# KPI Cards
# ---------------------------------------------------------------------------
st.markdown(f"""
<div class="kpi-grid">
  <div class="kpi-card">
    <div class="kpi-label">Airborne Flights</div>
    <div class="kpi-value">{airborne:,}</div>
    <div class="kpi-sub">tracked live</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Countries</div>
    <div class="kpi-value">{n_countries}</div>
    <div class="kpi-sub">origin nations</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Avg Altitude</div>
    <div class="kpi-value">{avg_alt:,.0f}</div>
    <div class="kpi-sub">metres</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Avg Velocity</div>
    <div class="kpi-value">{avg_vel}</div>
    <div class="kpi-sub">m/s &nbsp;(≈ {round(avg_vel*1.944, 0):.0f} kt)</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Enriched Flights</div>
    <div class="kpi-value">{n_enriched:,}</div>
    <div class="kpi-sub">with operator data</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">Active Alerts</div>
    <div class="kpi-value" style="color:{'#f87171' if n_alerts else '#34d399'}">{n_alerts}</div>
    <div class="kpi-sub">{'rapid descents' if n_alerts else 'all clear'}</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "  Global Traffic Map  ",
    "  Traffic Analytics  ",
    "  Fleet Intelligence  ",
    "  Anomaly Centre  ",
])

# ═══════════════════════════════════════════════════════════════════════════
# TAB 1 — GLOBAL TRAFFIC MAP
# ═══════════════════════════════════════════════════════════════════════════
with tab1:
    if flights_df.empty:
        st.info("Waiting for flight data from HBase...")
    else:
        c_left, c_right = st.columns([3, 1])

        with c_left:
            st.markdown('<div class="section-header">Live Airborne Flights — Altitude Colour Scale</div>',
                        unsafe_allow_html=True)

            fig_map = go.Figure()
            fig_map.add_trace(go.Scattergeo(
                lat=flights_df["lat"],
                lon=flights_df["lon"],
                mode="markers",
                marker=dict(
                    size=4,
                    color=flights_df["altitude"],
                    colorscale="Viridis",
                    cmin=0,
                    cmax=13000,
                    colorbar=dict(
                        title="Altitude (m)",
                        thickness=12,
                        len=0.7,
                        tickfont=dict(color="#c9d8eb", size=10),
                        title_font=dict(color="#c9d8eb", size=10),
                    ),
                    opacity=0.85,
                ),
                text=flights_df.apply(
                    lambda r: f"{r['callsign'] or r['icao24']}<br>"
                              f"Alt: {r['altitude']:.0f} m<br>"
                              f"Speed: {r['velocity']:.0f} m/s<br>"
                              f"{r['country']}",
                    axis=1,
                ),
                hoverinfo="text",
            ))
            fig_map.update_layout(
                **{**PLOTLY_BASE, "margin": dict(l=0, r=0, t=0, b=0)},
                height=520,
                geo=GEO_BASE,
            )
            st.plotly_chart(fig_map, use_container_width=True)

        with c_right:
            st.markdown('<div class="section-header">Altitude Distribution</div>',
                        unsafe_allow_html=True)
            alt_bins = pd.cut(
                flights_df["altitude"],
                bins=[0, 3000, 6000, 9000, 10000, 11000, 12000, 14000],
                labels=["<3k", "3-6k", "6-9k", "9-10k", "10-11k", "11-12k", ">12k"],
            ).value_counts().sort_index()

            fig_alt = go.Figure(go.Bar(
                x=alt_bins.values,
                y=alt_bins.index.astype(str),
                orientation="h",
                marker_color="#38bdf8",
                marker_line_width=0,
            ))
            fig_alt.update_layout(
                **PLOTLY_BASE,
                height=240,
                xaxis_title="Flights",
                yaxis_title="Altitude band (m)",
                showlegend=False,
            )
            st.plotly_chart(fig_alt, use_container_width=True)

            st.markdown('<div class="section-header">Speed Distribution</div>',
                        unsafe_allow_html=True)
            vel_data = flights_df["velocity"][flights_df["velocity"] > 20]
            fig_vel = go.Figure(go.Histogram(
                x=vel_data,
                nbinsx=30,
                marker_color="#818cf8",
                marker_line_width=0,
            ))
            fig_vel.update_layout(
                **PLOTLY_BASE,
                height=220,
                xaxis_title="Velocity (m/s)",
                yaxis_title="Count",
                showlegend=False,
                bargap=0.05,
            )
            st.plotly_chart(fig_vel, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 2 — TRAFFIC ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════
with tab2:
    if agg_df.empty:
        st.info("Waiting for windowed aggregations from HBase...")
    else:
        latest = (agg_df
                  .sort_values("window_end", ascending=False)
                  .groupby("country", as_index=False)
                  .first()
                  .sort_values("count", ascending=False)
                  .head(TOP_N))

        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown('<div class="section-header">Top Countries by Airborne Flight Count</div>',
                        unsafe_allow_html=True)
            fig_bar = go.Figure(go.Bar(
                x=latest["count"],
                y=latest["country"],
                orientation="h",
                marker=dict(
                    color=latest["count"],
                    colorscale="Blues",
                    line_width=0,
                ),
                text=latest["count"],
                textposition="outside",
                textfont=dict(color="#c9d8eb", size=10),
            ))
            fig_bar.update_layout(
                **PLOTLY_BASE,
                height=480,
                xaxis_title="Flights",
                yaxis=dict(autorange="reversed", tickfont=dict(size=10)),
                showlegend=False,
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        with col_b:
            st.markdown('<div class="section-header">Average Altitude by Country (Top 20)</div>',
                        unsafe_allow_html=True)
            fig_alt2 = px.scatter(
                latest,
                x="avg_vel",
                y="avg_alt",
                size="count",
                color="count",
                color_continuous_scale="Viridis",
                hover_name="country",
                hover_data={"count": True, "avg_alt": ":.0f", "avg_vel": ":.1f"},
                labels={"avg_vel": "Avg Velocity (m/s)", "avg_alt": "Avg Altitude (m)", "count": "Flights"},
                size_max=40,
            )
            fig_alt2.update_layout(
                **PLOTLY_BASE,
                height=480,
                showlegend=False,
            )
            st.plotly_chart(fig_alt2, use_container_width=True)

        # ── Sliding-window trend ──
        st.markdown('<div class="section-header">Sliding-Window Trend (5-min window, 1-min step)</div>',
                    unsafe_allow_html=True)
        top10 = latest.head(10)["country"].tolist()
        col_sel, _ = st.columns([2, 5])
        with col_sel:
            selected = st.multiselect(
                "Countries", top10, default=top10[:4],
                label_visibility="collapsed",
            )
        if selected:
            trend = agg_df[agg_df["country"].isin(selected)].sort_values("window_end")

            c1, c2 = st.columns(2)
            with c1:
                fig_tc = px.line(
                    trend, x="window_end", y="count", color="country",
                    labels={"window_end": "Window End", "count": "Flight Count", "country": ""},
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig_tc.update_layout(**PLOTLY_BASE, height=280, title="Flight Count Over Time")
                st.plotly_chart(fig_tc, use_container_width=True)

            with c2:
                fig_ta = px.line(
                    trend, x="window_end", y="avg_alt", color="country",
                    labels={"window_end": "Window End", "avg_alt": "Avg Altitude (m)", "country": ""},
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig_ta.update_layout(**PLOTLY_BASE, height=280, title="Avg Altitude Over Time (Moving Avg)")
                st.plotly_chart(fig_ta, use_container_width=True)

        # ── Country heatmap ──
        st.markdown('<div class="section-header">Traffic Intensity Heatmap — Flight Count per Country</div>',
                    unsafe_allow_html=True)
        heatmap_data = latest.head(15).copy()
        fig_choropleth = px.choropleth(
            heatmap_data,
            locations="country",
            locationmode="country names",
            color="count",
            color_continuous_scale="YlOrRd",
            hover_data={"avg_alt": ":.0f", "avg_vel": ":.1f"},
            labels={"count": "Flights", "avg_alt": "Avg Alt (m)", "avg_vel": "Avg Vel (m/s)"},
        )
        fig_choropleth.update_geos(**GEO_BASE)
        fig_choropleth.update_layout(
            **{**PLOTLY_BASE, "margin": dict(l=0, r=0, t=10, b=0)},
            height=340,
            geo=GEO_BASE,
        )
        st.plotly_chart(fig_choropleth, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 3 — FLEET INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════
with tab3:
    if flights_df.empty:
        st.info("Waiting for flight data...")
    else:
        enriched = flights_df[flights_df["operator"].ne("") & flights_df["model"].ne("")]

        if enriched.empty:
            st.info("Enrichment data not yet available — Spark join still processing.")
        else:
            col1, col2, col3 = st.columns(3)

            with col1:
                st.markdown('<div class="section-header">Top 15 Airlines / Operators</div>',
                            unsafe_allow_html=True)
                top_ops = (enriched["operator"]
                           .value_counts().head(15)
                           .reset_index()
                           .rename(columns={"index": "operator", "count": "flights"}))
                # newer pandas uses different column naming
                top_ops.columns = ["operator", "flights"]
                fig_ops = px.bar(
                    top_ops, x="flights", y="operator", orientation="h",
                    color="flights", color_continuous_scale="Blues",
                    labels={"flights": "Flights", "operator": ""},
                )
                fig_ops.update_layout(**PLOTLY_BASE, height=420, showlegend=False,
                                      yaxis=dict(autorange="reversed", tickfont=dict(size=9)))
                st.plotly_chart(fig_ops, use_container_width=True)

            with col2:
                st.markdown('<div class="section-header">Top Aircraft Models</div>',
                            unsafe_allow_html=True)
                top_models = (enriched["model"]
                              .value_counts().head(15)
                              .reset_index()
                              .rename(columns={"index": "model", "count": "flights"}))
                top_models.columns = ["model", "flights"]
                fig_models = px.bar(
                    top_models, x="flights", y="model", orientation="h",
                    color="flights", color_continuous_scale="Purples",
                    labels={"flights": "Flights", "model": ""},
                )
                fig_models.update_layout(**PLOTLY_BASE, height=420, showlegend=False,
                                         yaxis=dict(autorange="reversed", tickfont=dict(size=9)))
                st.plotly_chart(fig_models, use_container_width=True)

            with col3:
                st.markdown('<div class="section-header">Manufacturer Share</div>',
                            unsafe_allow_html=True)
                top_mfr = enriched["mfr"].value_counts().head(8).reset_index()
                top_mfr.columns = ["mfr", "count"]
                fig_mfr = go.Figure(go.Pie(
                    labels=top_mfr["mfr"],
                    values=top_mfr["count"],
                    hole=0.55,
                    marker=dict(colors=px.colors.qualitative.Set3),
                    textinfo="label+percent",
                    textfont=dict(size=10, color="#c9d8eb"),
                ))
                fig_mfr.update_layout(
                    **PLOTLY_BASE,
                    height=300,
                    showlegend=False,
                    annotations=[dict(
                        text=f"{len(enriched):,}<br>flights",
                        x=0.5, y=0.5,
                        font=dict(size=14, color="#38bdf8"),
                        showarrow=False,
                    )],
                )
                st.plotly_chart(fig_mfr, use_container_width=True)

                # Typecode breakdown
                st.markdown('<div class="section-header">Type Codes</div>',
                            unsafe_allow_html=True)
                top_type = enriched["typecode"].value_counts().head(10).reset_index()
                top_type.columns = ["typecode", "count"]
                fig_type = px.bar(
                    top_type, x="typecode", y="count",
                    color="count", color_continuous_scale="Teal",
                    labels={"typecode": "ICAO Type", "count": "Count"},
                )
                fig_type.update_layout(**PLOTLY_BASE, height=200, showlegend=False,
                                       xaxis_tickfont=dict(size=9))
                st.plotly_chart(fig_type, use_container_width=True)

            # ── Altitude vs Velocity scatter by operator ──
            st.markdown('<div class="section-header">Altitude vs Velocity by Operator (Top 8)</div>',
                        unsafe_allow_html=True)
            top8_ops = enriched["operator"].value_counts().head(8).index.tolist()
            scatter_df = enriched[enriched["operator"].isin(top8_ops)]
            fig_scatter = px.scatter(
                scatter_df,
                x="velocity", y="altitude",
                color="operator",
                opacity=0.55,
                size_max=6,
                color_discrete_sequence=px.colors.qualitative.Pastel,
                labels={"velocity": "Velocity (m/s)", "altitude": "Altitude (m)", "operator": "Operator"},
                hover_data=["callsign", "model", "country"],
            )
            fig_scatter.update_traces(marker=dict(size=4))
            fig_scatter.update_layout(**PLOTLY_BASE, height=320)
            st.plotly_chart(fig_scatter, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 4 — ANOMALY CENTRE
# ═══════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown('<div class="section-header">Rapid-Descent Detection (vertical rate < −15 m/s)</div>',
                unsafe_allow_html=True)

    if alerts_df.empty:
        st.markdown('<span class="ok-badge">ALL CLEAR</span> &nbsp; No rapid-descent events detected.',
                    unsafe_allow_html=True)
    else:
        col_g, col_h = st.columns([1, 3])

        with col_g:
            # Gauge — most severe vrate
            worst = alerts_df["vrate"].min()
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=abs(worst),
                title=dict(text="Worst Descent Rate<br>(m/s)", font=dict(color="#c9d8eb", size=12)),
                number=dict(suffix=" m/s", font=dict(color="#f87171", size=28)),
                gauge=dict(
                    axis=dict(range=[0, 60], tickcolor="#c9d8eb"),
                    bar=dict(color="#f87171"),
                    bgcolor="#1a2340",
                    bordercolor="#1e3a5f",
                    steps=[
                        dict(range=[0,  20], color="#14532d"),
                        dict(range=[20, 35], color="#713f12"),
                        dict(range=[35, 60], color="#7f1d1d"),
                    ],
                    threshold=dict(line=dict(color="#fbbf24", width=3), thickness=0.8, value=30),
                ),
            ))
            fig_gauge.update_layout(**PLOTLY_BASE, height=260)
            st.plotly_chart(fig_gauge, use_container_width=True)

            # Alert count by country
            st.markdown('<div class="section-header">Alerts by Country</div>',
                        unsafe_allow_html=True)
            ac = alerts_df["country"].value_counts().head(10).reset_index()
            ac.columns = ["country", "alerts"]
            fig_ac = px.bar(ac, x="alerts", y="country", orientation="h",
                            color="alerts", color_continuous_scale="Reds",
                            labels={"alerts": "Alerts", "country": ""})
            fig_ac.update_layout(**PLOTLY_BASE, height=260, showlegend=False,
                                 yaxis=dict(autorange="reversed", tickfont=dict(size=9)))
            st.plotly_chart(fig_ac, use_container_width=True)

        with col_h:
            # Descent-rate histogram
            st.markdown('<div class="section-header">Descent Rate Distribution</div>',
                        unsafe_allow_html=True)
            fig_hist = go.Figure(go.Histogram(
                x=alerts_df["vrate"],
                nbinsx=30,
                marker_color="#f87171",
                marker_line_width=0,
            ))
            fig_hist.add_vline(x=-15, line_dash="dash", line_color="#fbbf24",
                               annotation_text="Threshold −15 m/s",
                               annotation_font_color="#fbbf24",
                               annotation_position="top right")
            fig_hist.update_layout(
                **PLOTLY_BASE,
                height=220,
                xaxis_title="Vertical Rate (m/s)",
                yaxis_title="Events",
                showlegend=False,
            )
            st.plotly_chart(fig_hist, use_container_width=True)

            # Alert table
            st.markdown('<div class="section-header">Alert Event Log</div>',
                        unsafe_allow_html=True)
            display_alerts = alerts_df.copy()
            display_alerts["vrate_ft_min"] = (display_alerts["vrate"] * 196.85).round(0)
            display_alerts["altitude_ft"]  = (display_alerts["altitude"] * 3.281).round(0)
            display_alerts["severity"] = display_alerts["vrate"].apply(
                lambda v: "CRITICAL" if v < -30 else "WARNING"
            )
            st.dataframe(
                display_alerts[[
                    "timestamp", "severity", "callsign", "operator",
                    "country", "vrate", "vrate_ft_min", "altitude", "altitude_ft",
                ]].rename(columns={
                    "vrate":        "V-rate (m/s)",
                    "vrate_ft_min": "V-rate (fpm)",
                    "altitude":     "Alt (m)",
                    "altitude_ft":  "Alt (ft)",
                }),
                use_container_width=True,
                height=320,
            )

# ---------------------------------------------------------------------------
# Footer + auto-refresh
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown(
    f'<div class="pipeline-label">CS523 Big Data Technology — Maharishi International University &nbsp;|&nbsp; '
    f'Mercel Vubangsi · Alvin Leonald Kabwama &nbsp;|&nbsp; '
    f'Refreshing every {REFRESH_INTERVAL}s &nbsp;|&nbsp; '
    f'Last update: {pd.Timestamp.now().strftime("%H:%M:%S")}</div>',
    unsafe_allow_html=True,
)

time.sleep(REFRESH_INTERVAL)
st.rerun()
