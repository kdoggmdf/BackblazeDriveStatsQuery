import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Backblaze Drive Stats", layout="wide")

st.title("📊 Backblaze Drive Stats Analyzer")

@st.cache_resource
def get_con():
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    con.execute("""
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

# --- Search Interface ---
st.sidebar.header("Search Parameters")
# Using the model you mentioned as default
model_input = st.sidebar.text_input("Drive Model", value="HUH721212ALN604")
lookback_limit = st.sidebar.number_input("Max Records to Scan", value=20000)

if st.sidebar.button("Analyze Model"):
    try:
        with st.spinner(f"Searching for {model_input}..."):
            # 1. Use ILIKE for case-insensitive partial matching
            # 2. Calculate AFR: (Failures / Total Drive Days) * 365 * 100
            query = f"""
                SELECT 
                    date,
                    model,
                    capacity_bytes / 1e12 as capacity_tb,
                    failure,
                    smart_9_raw as power_on_hours
                FROM iceberg_scan('s3://drivestats-iceberg/drivestats')
                WHERE model ILIKE '%{model_input}%'
                ORDER BY date DESC
                LIMIT {lookback_limit}
            """
            df = con.execute(query).df()

        if not df.empty:
            # --- Calculations ---
            total_records = len(df)
            total_failures = df['failure'].sum()
            # Standard Backblaze AFR Calculation
            afr = (total_failures / total_records) * 365 * 100 if total_records > 0 else 0
            
            # --- Metrics ---
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Exact Model Found", df['model'].iloc[0])
            m2.metric("Total Drive-Days", total_records)
            m3.metric("Failures", int(total_failures))
            m4.metric("Est. Annual Failure Rate (AFR)", f"{afr:.2f}%")

            # --- Visuals ---
            col_left, col_right = st.columns(2)
            
            with col_left:
                st.subheader("Reliability: Power On Hours vs Failure")
                # Visualizing when failures occur relative to drive age
                fig_age = px.scatter(df, x="power_on_hours", y="failure", 
                                   color="failure", 
                                   title="Failure Events by Drive Age (Hours)",
                                   color_continuous_scale=["#2ecc71", "#e74c3c"])
                st.plotly_chart(fig_age, use_container_width=True)

            with col_right:
                st.subheader("Data Density (Sample Window)")
                # Showing the timeframe the data covers
                fig_time = px.histogram(df, x="date", title="Reports Collected per Day")
                st.plotly_chart(fig_time, use_container_width=True)

            st.subheader("Recent Data Points")
            st.dataframe(df.head(100), use_container_width=True)
            
        else:
            st.warning(f"No results found for '{model_input}'. Try a shorter version of the model name.")

    except Exception as e:
        st.error("Query Error")
        st.exception(e)
else:
    st.info("Enter a model name in the sidebar and click Analyze.")
