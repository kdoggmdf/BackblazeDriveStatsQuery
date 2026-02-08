import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# --- Page Config ---
st.set_page_config(page_title="Backblaze Drive Stats", layout="wide")

st.title("💽 Backblaze Drive Stats Explorer")
st.markdown("Querying the public Iceberg dataset directly from Backblaze B2.")

# --- Database Connection ---
@st.cache_resource
def get_duckdb_connection():
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # Configure the connection for Backblaze B2
    con.execute("""
        SET s3_region='us-west-004';
        SET s3_endpoint='s3.us-west-004.backblazeb2.com';
        SET s3_access_key_id='0045f0571db506a0000000017';
        SET s3_secret_access_key='K004Fs/bgmTk5dgo6GAVm2Waj3Ka+TE';
        SET s3_use_ssl=true;
        SET s3_url_style='path';
        
        -- THE CRITICAL FIX:
        -- Tells DuckDB to look for the latest metadata.json file automatically
        SET unsafe_enable_version_guessing = true;
        
        -- Jitter protection for cloud hosting
        SET http_timeout=30000;
        SET http_retries=3;
    """)
    return con

con = get_duckdb_connection()

# --- Sidebar ---
st.sidebar.header("Filter Settings")
# Using a common model as default to ensure results
model_id = st.sidebar.text_input("Drive Model", value="ST4000DM000")
limit = st.sidebar.slider("Sample Size", 100, 5000, 1000)

if st.sidebar.button("Search Database"):
    try:
        with st.spinner(f"Scanning Iceberg metadata for {model_id}..."):
            # We query the scan function directly
            sql = f"""
                SELECT 
                    date, 
                    serial_number, 
                    model, 
                    capacity_bytes / 1e12 as capacity_tb, 
                    failure
                FROM iceberg_scan('s3://drivestats-iceberg/drivestats')
                WHERE model = '{model_id}'
                ORDER BY date DESC
                LIMIT {limit}
            """
            df = con.execute(sql).df()

        if not df.empty:
            # Metrics
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Records", len(df))
            c2.metric("Failures Found", df['failure'].sum())
            c3.metric("Avg Capacity (TB)", f"{df['capacity_tb'].mean():.1f}")

            # Visuals
            st.subheader("Visual: Activity by Date")
            # Creating a simple timeline of drive reports
            timeline = df.groupby('date').size().reset_index(name='count')
            fig = px.bar(timeline, x='date', y='count', title="Daily Report Density")
            st.plotly_chart(fig, use_container_width=True)

            # Table
            st.subheader("Data Preview")
            st.dataframe(df, use_container_width=True)
        else:
            st.warning("No data found. Note: Model names are case-sensitive.")

    except Exception as e:
        st.error("The query engine failed.")
        st.exception(e)
else:
    st.info("Enter a model (e.g., ST4000DM000) and click Search.")
