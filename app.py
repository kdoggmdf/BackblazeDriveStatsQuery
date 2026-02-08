import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# --- Page Config ---
st.set_page_config(page_title="Backblaze Drive Stats Query Tool", layout="wide")

@st.cache_resource
def get_con():
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        SET memory_limit='2GB';
        SET threads=2;
        SET http_timeout=60000;
        SET http_retries=5;
        SET s3_region='us-west-004';
        SET s3_endpoint='s3.us-west-004.backblazeb2.com';
        SET s3_access_key_id='0045f0571db506a0000000017';
        SET s3_secret_access_key='K004Fs/bgmTk5dgo6GAVm2Waj3Ka+TE';
        SET s3_use_ssl=true;
        SET s3_url_style='path';
        SET unsafe_enable_version_guessing = true;
    """)
    return con

con = get_con()

st.title("🛡️ Backblaze Drive Stats Query Tool")
st.caption("Comprehensive Reliability Lab | All-Time History (2013-Present)")

# --- Sidebar Navigation ---
mode = st.sidebar.radio("Analysis Mode", ["Single Model Research", "Reliability Arena (Compare)", "Fleet Top 10"])

# --- MODE 1: SINGLE MODEL ---
if mode == "Single Model Research":
    model_id = st.sidebar.text_input("Enter Drive Model", value="MG08ACA16TEY")
    if st.sidebar.button("Analyze Model"):
        try:
            with st.spinner(f"Analyzing {model_id}..."):
                stats = con.execute(f"SELECT COUNT(*) as days, SUM(failure) as fails, AVG(smart_9_raw) FILTER (WHERE failure = 1) as avg_age FROM iceberg_scan('s3://drivestats-iceberg/drivestats') WHERE model ILIKE '%{model_id}%'").df()
                fails_df = con.execute(f"SELECT date, serial_number, model, smart_9_raw as hours, failure FROM iceberg_scan('s3://drivestats-iceberg/drivestats') WHERE model ILIKE '%{model_id}%' AND failure = 1 ORDER BY date DESC").df()
                
            if stats['days'][0] > 0:
                afr = (stats['fails'][0] / (stats['days'][0] / 365.0)) * 100
                cols = st.columns(4)
                cols[0].metric("Total Drive Days", f"{int(stats['days'][0]):,}")
                cols[1].metric("Total Failures", int(stats['fails'][0]))
                cols[2].metric("Lifetime AFR", f"{afr:.2f}%")
                cols[3].metric("Avg Fail Age", f"{int(stats['avg_age'][0] or 0):,} hrs")
                
                st.divider()
                c1, c2 = st.columns(2)
                with c1:
                    st.plotly_chart(px.histogram(fails_df, x="date", title="Failure Spikes", color_discrete_sequence=['#e74c3c']), use_container_width=True)
                with c2:
                    st.plotly_chart(px.histogram(fails_df, x="hours", title="Age at Failure (Bathtub Curve)", color_discrete_sequence=['#3498db']), use_container_width=True)
            else:
                st.warning("No data found.")
        except Exception as e:
            st.error(f"Error: {e}")

# --- MODE 2: ARENA ---
elif mode == "Reliability Arena (Compare)":
    m1 = st.sidebar.text_input("Model A", value="MG08ACA16TEY")
    m2 = st.sidebar.text_input("Model B", value="WUH722222ALE6L4")
    if st.sidebar.button("Battle!"):
        try:
            with st.spinner("Comparing fleets..."):
                arena_df = con.execute(f"SELECT model, COUNT(*) as days, SUM(failure) as fails FROM iceberg_scan('s3://drivestats-iceberg/drivestats') WHERE model ILIKE '%{m1}%' OR model ILIKE '%{m2}%' GROUP BY model").df()
                arena_df['AFR %'] = (arena_df['fails'] / (arena_df['days'] / 365.0)) * 100
                st.table(arena_df)
                st.plotly_chart(px.bar(arena_df, x='model', y='AFR %', color='model', title="AFR Comparison (Lower is Better)"))
        except Exception as e:
            st.error(f"Error: {e}")

# --- MODE 3: TOP 10 ---
elif mode == "Fleet Top 10":
    if st.sidebar.button("Show Fleet Leaders"):
        try:
            with st.spinner("Finding most deployed drives..."):
                top_df = con.execute("SELECT model, COUNT(*) as drive_days, SUM(failure) as total_failures FROM iceberg_scan('s3://drivestats-iceberg/drivestats') GROUP BY model ORDER BY drive_days DESC LIMIT 10").df()
                top_df['AFR %'] = (top_df['total_failures'] / (top_df['drive_days'] / 365.0)) * 100
                st.subheader("Top 10 Most Deployed Models (Historical)")
                st.dataframe(top_df, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")

# --- SMART Guide (Always Visible) ---
st.divider()
st.subheader("🩺 The 'Predictive Five' S.M.A.R.T. Attributes")
smart_guide = pd.DataFrame({
    "ID": ["5", "187", "188", "197", "198"],
    "Attribute": ["Reallocated Sectors", "Uncorrectable Errors", "Command Timeout", "Pending Sectors", "Offline Uncorrectable"],
    "Risk": ["High", "Critical", "Moderate", "Critical", "Critical"]
})
st.table(smart_guide)
