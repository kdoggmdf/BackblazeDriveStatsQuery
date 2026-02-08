import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# --- Page Config ---
st.set_page_config(page_title="Backblaze Drive Stats Explorer", layout="wide")

st.title("💽 Backblaze Drive Stats Explorer")
st.markdown("""
This tool queries the **Apache Iceberg** dataset hosted by Backblaze. 
It pulls data directly from B2 storage using DuckDB.
""")

# --- Database Connection ---
@st.cache_resource
def get_duckdb_connection():
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # Crucial Fixes for Streamlit Cloud:
    # 1. Use SSL=true
    # 2. Use Path-Style URLS
    # 3. Explicitly set the region
    con.execute("""
        SET s3_region='us-west-004';
        SET s3_endpoint='s3.us-west-004.backblazeb2.com';
        SET s3_access_key_id='0045f0571db506a0000000017';
        SET s3_secret_access_key='K004Fs/bgmTk5dgo6GAVm2Waj3Ka+TE';
        SET s3_use_ssl=true;
        SET s3_url_style='path';
    """)
    return con

con = get_duckdb_connection()

# --- Sidebar Inputs ---
st.sidebar.header("Query Settings")

# Common models to make testing easier
popular_models = ["ST4000DM000", "ST8000NM0055", "WDC WD120EDAZ", "HGST HMS5C4040ALE640"]
selected_model = st.sidebar.selectbox("Quick Select Model", ["Custom..."] + popular_models)

if selected_model == "Custom...":
    model_id = st.sidebar.text_input("Enter Drive Model", value="ST4000DM000")
else:
    model_id = selected_model

limit = st.sidebar.slider("Number of records to fetch", 100, 10000, 1000)

# --- Data Execution ---
if st.sidebar.button("Run Query"):
    try:
        with st.spinner(f"Scanning Iceberg tables for {model_id}..."):
            # Simplified query to avoid subquery overhead/timeouts
            sql = f"""
                SELECT 
                    date, 
                    serial_number, 
                    model, 
                    capacity_bytes / (1024*1024*1024*1024) as capacity_tb, 
                    failure,
                    smart_9_raw as power_on_hours
                FROM iceberg_scan('s3://drivestats-iceberg/drivestats')
                WHERE model = '{model_id}'
                ORDER BY date DESC
                LIMIT {limit}
            """
            df = con.execute(sql).df()

        if not df.empty:
            # --- Visuals ---
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Records Found", len(df))
            with col2:
                st.metric("Failures in Sample", df['failure'].sum())
            with col3:
                st.metric("Avg Capacity (TB)", round(df['capacity_tb'].mean(), 1))

            st.divider()

            # Simple Chart: Power on Hours vs Date (Health check)
            st.subheader("Visual: Power On Hours Trend")
            fig = px.scatter(df, x="date", y="power_on_hours", color="failure",
                             title=f"Usage Profile for {model_id}",
                             labels={"power_on_hours": "Power On Hours", "date": "Date"})
            st.plotly_chart(fig, use_container_width=True)

            # Text Data
            st.subheader("Raw Data Preview")
            st.dataframe(df, use_container_width=True)
            
        else:
            st.warning("No data found for that model. Check spelling and case sensitivity.")

    except Exception as e:
        st.error("The query failed to connect to Backblaze.")
        st.exception(e)
else:
    st.info("Click 'Run Query' in the sidebar to load data.")

# --- Footer ---
st.caption("Data provided by Backblaze Drive Stats. Query powered by DuckDB + Iceberg.")
