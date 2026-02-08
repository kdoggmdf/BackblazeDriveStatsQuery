import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# --- Page Config ---
st.set_page_config(page_title="Drive Reliability Lab", layout="wide")

@st.cache_resource
def get_con():
    # Use an in-memory connection but limit its footprint for Streamlit Cloud
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

st.title("🛡️ Lifetime Drive Reliability Lab")
st.caption("Analyzing full historical data (2013-Present) | Backblaze B2 + Apache Iceberg")

# --- Sidebar ---
st.sidebar.header("Research Parameters")
model_id = st.sidebar.text_input("Enter Drive Model", value="MG08ACA16TEY")

if st.sidebar.button("Run Full Lifetime Analysis"):
    try:
        with st.spinner(f"Aggregating all-time data for {model_id}..."):
            # 1. Main Aggregate Stats
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
            
            # 2. Extract specific failure events
            fail_events_query = f"""
                SELECT date, serial_number, model, smart_9_raw as hours, failure
                FROM iceberg_scan('s3://drivestats-iceberg/drivestats')
                WHERE model ILIKE '%{model_id}%' AND failure = 1
                ORDER BY date DESC
            """
            fail_df = con.execute(fail_events_query).df()

        if not stats_df.empty and stats_df['total_drive_days'][0] > 0:
            # --- Metrics Summary ---
            days = stats_df['total_drive_days'][0]
            fails = stats_df['total_failures'][0]
            afr = (fails / (days / 365.0)) * 100 if days > 0 else 0
            
            st.header(f"Results for {model_id}")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Drive Days", f"{int(days):,}")
            m2.metric("Total Failures", int(fails))
            m3.metric("Lifetime AFR", f"{afr:.2f}%")
            m4.metric("Avg Hours at Failure", f"{int(stats_df['avg_hours_at_fail'][0] or 0):,}")

            # --- Visual Section ---
            st.divider()
            col_l, col_r = st.columns(2)
            
            with col_l:
                st.subheader("Timeline: Failure Density")
                if not fail_df.empty:
                    fig_timeline = px.histogram(fail_df, x="date", title="Failure Events per Day",
                                              labels={'date': 'Date', 'count': 'Failures'},
                                              color_discrete_sequence=['#e74c3c'])
                    st.plotly_chart(fig_timeline, use_container_width=True)
                else:
                    st.info("No failure events recorded.")

            with col_r:
                st.subheader("Age: Failure Distribution")
                if not fail_df.empty:
                    fig_age = px.histogram(fail_df, x="hours", nbins=30, title="At what age do they die?",
                                         labels={'hours': 'Power On Hours'}, color_discrete_sequence=['#3498db'])
                    st.plotly_chart(fig_age, use_container_width=True)

            # --- S.M.A.R.T. Predictive Guide ---
            st.divider()
            st.subheader("🩺 The 'Predictive Five' S.M.A.R.T. Attributes")
            
            smart_guide = pd.DataFrame({
                "ID": ["5", "187", "188", "197", "198"],
                "Attribute": ["Reallocated Sectors", "Uncorrectable Errors", "Command Timeout", "Pending Sectors", "Offline Uncorrectable"],
                "Predictive Strength": ["High", "Critical", "Moderate", "Critical", "Critical"]
            })
            st.table(smart_guide)

            # --- Raw Failure Log ---
            st.subheader("📜 Raw Failure Log")
            if not fail_df.empty:
                st.dataframe(fail_df, use_container_width=True)
                csv = fail_df.to_csv(index=False).encode('utf-8')
                st.download_button("📥 Download Failure Data as CSV", data=csv, file_name=f"{model_id}_failures.csv", mime='text/csv')
            else:
                st.write("No raw failure rows found.")

        else:
            st.warning("No data found. Try searching for a partial model name (e.g. 'MG08').")

    except Exception as e:
        st.error(f"Engine Error: {e}")
