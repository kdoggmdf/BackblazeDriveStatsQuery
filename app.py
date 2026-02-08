import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Backblaze Drive Stats Explorer", layout="wide")

st.title("💽 Backblaze Drive Stats (Live Iceberg)")
st.markdown("Querying the public Iceberg dataset directly from Backblaze B2.")

# Initialize DuckDB and install extensions
@st.cache_resource
def get_con():
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # Backblaze Public Read-Only Credentials
    con.execute("""
        SET s3_region='us-west-004';
        SET s3_endpoint='s3.us-west-004.backblazeb2.com';
        SET s3_access_key_id='0045f0571db506a0000000017';
        SET s3_secret_access_key='K004Fs/bgmTk5dgo6GAVm2Waj3Ka+TE';
    """)
    return con

con = get_con()

# --- Sidebar Filters ---
st.sidebar.header("Search Filters")
model_search = st.sidebar.text_input("Drive Model (e.g., ST4000DM000)", "ST4000DM000")
days_lookback = st.sidebar.slider("Days of History", 7, 365, 30)

# --- Query Logic ---
if model_search:
    with st.spinner(f"Querying Iceberg table for {model_search}..."):
        # Note: iceberg_scan handles the metadata-to-data mapping automatically
        query = f"""
            SELECT date, serial_number, model, capacity_bytes, failure
            FROM iceberg_scan('s3://drivestats-iceberg/drivestats')
            WHERE model LIKE '%{model_search}%'
              AND date >= (SELECT MAX(date) FROM iceberg_scan('s3://drivestats-iceberg/drivestats')) - INTERVAL {days_lookback} DAY
            ORDER BY date DESC
        """
        df = con.execute(query).df()

    if not df.empty:
        # Layout metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Rows", len(df))
        col2.metric("Unique Drives", df['serial_number'].nunique())
        col3.metric("Failures Found", df['failure'].sum())

        # Simple Visuals
        st.subheader("Drive Count over Time")
        daily_counts = df.groupby('date').size().reset_index(name='count')
        fig = px.line(daily_counts, x='date', y='count', title=f"Active {model_search} Drives")
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Raw Data Preview")
        st.dataframe(df, use_container_width=True)
    else:
        st.warning("No data found for that model in the selected timeframe.")
