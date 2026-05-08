"""
Part 4 — Visualisation Dashboard
Streamlit dashboard that reads from HBase via Thrift (happybase) and
auto-refreshes every 10 seconds.

Three tabs:
  Tab 1 — Live world map of airborne flights (from flights_live)
  Tab 2 — Country trend: sliding-window moving averages (from flights_agg)
  Tab 3 — Rapid-descent alert feed (from flights_alerts)
"""

import os
import time

import happybase
import pandas as pd
import streamlit as st

HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
REFRESH_INTERVAL = 10  # seconds
MAX_ALERT_ROWS = 50
TOP_N_COUNTRIES = 20

st.set_page_config(
    page_title="CS523 Flight Pipeline",
    page_icon="✈",
    layout="wide",
)


# ---------------------------------------------------------------------------
# HBase read helpers
# ---------------------------------------------------------------------------

@st.cache_resource
def get_connection():
    return happybase.Connection(HBASE_HOST)


def read_live_flights(conn):
    """Scan flights_live and return a DataFrame of current airborne flights."""
    table = conn.table("flights_live")
    records = []
    for key, data in table.scan():
        try:
            lat = float(data.get(b"pos:lat", b"0") or b"0")
            lon = float(data.get(b"pos:lon", b"0") or b"0")
            alt = float(data.get(b"pos:baro_alt", b"0") or b"0")
            on_ground = data.get(b"meta:on_ground", b"True").decode() == "True"
            if lat == 0.0 and lon == 0.0:
                continue
            if on_ground:
                continue
            records.append({
                "icao24":   key.decode(errors="replace"),
                "callsign": data.get(b"info:callsign", b"").decode(errors="replace").strip(),
                "country":  data.get(b"info:country",  b"").decode(errors="replace"),
                "lat":      lat,
                "lon":      lon,
                "altitude": alt,
                "velocity": float(data.get(b"pos:velocity", b"0") or b"0"),
                "operator": data.get(b"enrich:operator", b"").decode(errors="replace"),
                "model":    data.get(b"enrich:model",    b"").decode(errors="replace"),
            })
        except (ValueError, TypeError):
            continue
    return pd.DataFrame(records)


def read_agg(conn, country_filter=None):
    """
    Scan flights_agg and return the most recent window per country.
    If country_filter is given, prefix-scan that country only.
    """
    table = conn.table("flights_agg")
    row_prefix = country_filter.encode() if country_filter else None
    records = []
    kwargs = {"row_prefix": row_prefix} if row_prefix else {"limit": 2000}
    for key, data in table.scan(**kwargs):
        try:
            parts = key.decode(errors="replace").split("|")
            country = parts[0]
            win_end = int(data.get(b"m:window_end", b"0") or b"0")
            records.append({
                "country":      country,
                "window_end":   pd.Timestamp(win_end, unit="s"),
                "flight_count": int(data.get(b"m:flight_count", b"0") or b"0"),
                "avg_alt":      float(data.get(b"m:avg_alt",      b"0") or b"0"),
                "max_alt":      float(data.get(b"m:max_alt",      b"0") or b"0"),
                "avg_velocity": float(data.get(b"m:avg_velocity", b"0") or b"0"),
            })
        except (ValueError, TypeError):
            continue
    return pd.DataFrame(records)


def read_alerts(conn):
    """Read the latest MAX_ALERT_ROWS events from flights_alerts."""
    table = conn.table("flights_alerts")
    records = []
    for key, data in table.scan(limit=MAX_ALERT_ROWS):
        try:
            parts = key.decode(errors="replace").split("|")
            icao = parts[0]
            ts_raw = int(data.get(b"a:ts", b"0") or b"0")
            records.append({
                "icao24":    icao,
                "callsign":  data.get(b"a:callsign", b"").decode(errors="replace"),
                "operator":  data.get(b"a:operator", b"").decode(errors="replace"),
                "country":   data.get(b"a:country",  b"").decode(errors="replace"),
                "vrate_m_s": float(data.get(b"a:vrate", b"0") or b"0"),
                "altitude":  float(data.get(b"a:alt",   b"0") or b"0"),
                "timestamp": pd.Timestamp(ts_raw, unit="s") if ts_raw else None,
            })
        except (ValueError, TypeError):
            continue
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Dashboard layout
# ---------------------------------------------------------------------------

st.title("Real-Time Flight Analytics — CS523")
st.caption(f"Auto-refreshes every {REFRESH_INTERVAL} s  |  HBase host: {HBASE_HOST}")

tab1, tab2, tab3 = st.tabs(["Live Map", "Country Trends", "Anomaly Alerts"])

try:
    conn = get_connection()

    # --- Tab 1: Live world map ---
    with tab1:
        flights_df = read_live_flights(conn)
        col1, col2, col3 = st.columns(3)
        col1.metric("Airborne Flights", len(flights_df))
        col2.metric("Countries", flights_df["country"].nunique() if not flights_df.empty else 0)
        col3.metric("Avg Altitude (m)",
                    round(flights_df["altitude"].mean(), 0) if not flights_df.empty else 0)
        if not flights_df.empty:
            st.map(flights_df.rename(columns={"lat": "latitude", "lon": "longitude"}),
                   zoom=1)
            with st.expander("Raw data"):
                st.dataframe(flights_df, use_container_width=True)
        else:
            st.info("Waiting for flight data...")

    # --- Tab 2: Country trends ---
    with tab2:
        agg_df = read_agg(conn)
        if not agg_df.empty:
            # Latest snapshot per country for bar chart
            latest = (agg_df.sort_values("window_end", ascending=False)
                             .groupby("country", as_index=False)
                             .first()
                             .sort_values("flight_count", ascending=False)
                             .head(TOP_N_COUNTRIES))

            st.subheader(f"Top {TOP_N_COUNTRIES} Countries by Airborne Flight Count")
            st.bar_chart(latest.set_index("country")["flight_count"])

            st.subheader("Average Altitude Trend (select a country)")
            countries = sorted(agg_df["country"].unique().tolist())
            selected = st.selectbox("Country", countries)
            if selected:
                trend = (agg_df[agg_df["country"] == selected]
                         .sort_values("window_end")
                         .set_index("window_end")[["avg_alt", "avg_velocity"]])
                st.line_chart(trend)
        else:
            st.info("Waiting for aggregation data...")

    # --- Tab 3: Anomaly alerts ---
    with tab3:
        alerts_df = read_alerts(conn)
        st.subheader(f"Rapid-Descent Alerts (vertical rate < -15 m/s)")
        if not alerts_df.empty:
            st.metric("Total alerts", len(alerts_df))
            st.dataframe(
                alerts_df.sort_values("timestamp", ascending=False),
                use_container_width=True,
            )
        else:
            st.success("No rapid-descent alerts detected.")

except Exception as exc:
    st.error(f"HBase connection error: {exc}\n\nMake sure HBase Thrift is running on {HBASE_HOST}:9090")

# Auto-refresh
time.sleep(REFRESH_INTERVAL)
st.rerun()
