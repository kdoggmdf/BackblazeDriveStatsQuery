import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# --- Page Config ---
st.set_page_config(page_title="Lifetime Drive Reliability Lab", layout="wide")

@st.cache_resource
def get_con():
    # Use an in-memory connection but limit its footprint
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    
    con.execute("""
        -- Optimization for Cloud Hosting
        SET memory_limit='2GB';
        SET threads=2;
        SET http_timeout=60000;
        SET http_retries=5;
        
        -- Backblaze B2 Config
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

st.title("🛡️ Lifetime Drive Reliability Lab")
st.markdown("Querying the full history of Backblaze Drive Stats via Apache Iceberg.")

# --- UI ---
model_id = st.sidebar.text_input("Enter Drive Model", value="MG08ACA16TEY")

if st.sidebar.button("Run Full Lifetime Analysis"):
    try:
        with st.spinner("Analyzing millions of records..."):
            # Aggregation is done on the DuckDB side to keep Python memory low
            stats_query = f"""
                SELECT 
                    COUNT(*) as total_drive_days,
                    SUM(failure) as total_failures,
                    MIN(date) as first_seen,
                    MAX(date) as last_seen,
                    AVG(smart_9_raw) FILTER (WHERE failure = 1) as avg_hours_at_fail
                FROM iceberg_scan('s3://drivestats-iceberg/drivestats')
                WHERE model ILIKE '%{model_id}%'
            """
            stats_df = con.execute(stats_query).df()
            
            # Specific failure events (only pulls rows where failure=1)
            fail_events_query = f"""
                SELECT date, serial_number, model, smart_9_raw as hours, failure
                FROM iceberg_scan('s3://drivestats-iceberg/drivestats')
                WHERE model ILIKE '%{model_id}%' AND failure = 1
                ORDER BY date DESC
            """
            fail_df = con.execute(fail_events_query).df()

        if stats_df['total_drive_days'][0] > 0:
            # Metrics
            days = stats_df['total_drive_days'][0]
            fails = stats_df['total_failures'][0]
            afr = (fails / (days / 365.0)) * 100 if days > 0 else 0
            
            st.header(f"Lifetime Stats: {model_id}")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Drive Days", f"{int(days):,}")
            c2.metric("Total Failures", int(fails))
            c3.metric("Lifetime AFR", f"{afr:.2f}%")
            c4.metric("Avg Failure Age", f"{int(stats_df['avg_hours_at_fail'][0] or 0):,} hrs")

            # --- Visuals ---
            [Image of the hard drive bathtub curve]
            st.divider()
            col_l, col_r = st.columns(2)
            
            with col_l:
                st.subheader("Timeline: Failure Density")
                if not fail_df.empty:
                    fig_timeline = px.histogram(fail_df, x="date", title="Failures per Day",
                                              color_discrete_sequence=['#e74c3c'])
                    st.plotly_chart(fig_timeline, use_container_width=True)

            with col_r:
                st.subheader("Age: Failure Distribution")
                if not fail_df.empty:
                    fig_age = px.histogram(fail_df, x="hours", nbins=30, title="At what age do they fail?",
                                         color_discrete_sequence=['#3498db'])
                    st.plotly_chart(fig_age, use_container_width=True)

            # --- S.M.A.R.T. Guide ---
            st.subheader("🩺 The 'Predictive Five' S.M.A.R.T. Attributes")
            smart_guide = pd.DataFrame({
                "ID": ["5", "187", "188", "197", "198"],
                "Attribute Name": ["Reallocated Sectors", "Uncorrectable Errors", "Command Timeout", "Pending Sectors
