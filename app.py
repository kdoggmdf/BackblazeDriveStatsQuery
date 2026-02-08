import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Drive Reliability Lab", layout="wide")

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

st.title("🔬 Drive Reliability Research Lab")
st.caption("Powered by Backblaze Drive Stats + Apache Iceberg")

# --- Sidebar ---
model_input = st.sidebar.text_input("Enter Drive Model", value="HUH721212ALN604")
# Research needs more data, so we'll increase the scan limit
scan_limit = st.sidebar.slider("Scan Depth (Rows)", 10000, 100000, 30000)

if st.sidebar.button("Generate Reliability Report"):
    try:
        with st.spinner("Analyzing fleet data..."):
            # Query focused on research metrics
            query = f"""
                SELECT 
                    date,
                    serial_number,
                    model,
                    failure,
                    smart_9_raw as hours
                FROM iceberg_scan('s3://drivestats-iceberg/drivestats')
                WHERE model ILIKE '%{model_input}%'
                ORDER BY date DESC
                LIMIT {scan_limit}
            """
            df = con.execute(query).df()

        if not df.empty:
            # --- RESEARCH CALCULATIONS ---
            unique_drives = df['serial_number'].nunique()
            total_drive_days = len(df)
            total_failures = df['failure'].sum()
            
            # AFR Formula: (Failures / (Drive Days / 365)) * 100
            afr = (total_failures / (total_drive_days / 365.0)) * 100 if total_drive_days > 0 else 0
            
            # Failure Timing (Hours before failure)
            failure_data = df[df['failure'] == 1]['hours']
            avg_failure_hours = failure_data.mean() if not failure_data.empty else 0

            # --- TOP LEVEL METRICS ---
            st.subheader(f"Reliability Profile: {df['model'].iloc[0]}")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Drives Deployed (Sample)", f"{unique_drives:,}")
            m2.metric("Total Failures", int(total_failures))
            m3.metric("Annual Failure Rate (AFR)", f"{afr:.2f}%")
            m4.metric("Avg. Hours at Failure", f"{int(avg_failure_hours):,}" if avg_failure_hours > 0 else "N/A")

            # --- VISUALS ---
            col_a, col_b = st.columns(2)

            with col_a:
                st.write("**Failure Distribution (Age in Hours)**")
                if not failure_data.empty:
                    fig_hist = px.histogram(failure_data, x="hours", 
                                          nbins=30, 
                                          title="When do they die?",
                                          labels={'hours': 'Power On Hours'},
                                          color_discrete_sequence=['#e74c3c'])
                    st.plotly_chart(fig_hist, use_container_width=True)
                else:
                    st.info("No failure events in this sample to map timing.")

            with col_b:
                st.write("**Fleet Activity Timeline**")
                daily_active = df.groupby('date')['serial_number'].nunique().reset_index()
                fig_line = px.line(daily_active, x='date', y='serial_number', 
                                 title="Active Drive Count Over Time")
                st.plotly_chart(fig_line, use_container_width=True)

            # --- PREDICTIVE S.M.A.R.T. SECTION ---
            st.divider()
            st.subheader("Critical Health Indicators")
            st.markdown("""
            Research shows these 5 S.M.A.R.T. stats are the strongest predictors of failure. 
            If your drive has values > 0 here, it is likely in a 'proactive failure' state.
            """)
            
            # Example of how to pull specific SMART stats for research
            st.table(pd.DataFrame({
                "SMART ID": ["5", "187", "188", "197", "198"],
                "Description": [
                    "Reallocated Sector Count", 
                    "Reported Uncorrectable Errors", 
                    "Command Timeout", 
                    "Current Pending Sector Count", 
                    "Offline Uncorrectable"
                ],
                "Risk Level": ["High", "Critical", "Moderate", "Critical", "Critical"]
            }))

        else:
            st.warning("No data found for this model.")

    except Exception as e:
        st.error("Engine Error")
        st.exception(e)
