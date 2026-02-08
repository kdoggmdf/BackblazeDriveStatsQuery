import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="All-Time Drive Reliability", layout="wide")

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

st.title("🛡️ Lifetime Drive Reliability Lab")
st.caption("Analyzing the full Backblaze history (2013–Present) via Iceberg")

model_id = st.sidebar.text_input("Drive Model", value="MG08ACA16TEY")

if st.sidebar.button("Run Full Lifetime Analysis"):
    try:
        with st.spinner(f"Aggregating all-time data for {model_id}..."):
            # Instead of SELECT *, we aggregate in SQL to save memory/time
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
            
            # Get failure distribution for the histogram
            fail_dist_query = f"""
                SELECT smart_9_raw as hours
                FROM iceberg_scan('s3://drivestats-iceberg/drivestats')
                WHERE model ILIKE '%{model_id}%' AND failure = 1
            """
            fail_df = con.execute(fail_dist_query).df()

        if stats_df['total_drive_days'][0] > 0:
            # --- Metrics ---
            days = stats_df['total_drive_days'][0]
            fails = stats_df['total_failures'][0]
            afr = (fails / (days / 365.0)) * 100
            
            st.header(f"Lifetime Stats: {model_id}")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Drive Days", f"{int(days):,}")
            col2.metric("Lifetime Failures", int(fails))
            col3.metric("Lifetime AFR", f"{afr:.2f}%")
            col4.metric("Avg Failure Age", f"{int(stats_df['avg_hours_at_fail'][0] or 0):,} hrs")

            # --- Visuals ---
            st.divider()
            c_left, c_right = st.columns(2)
            
            with c_left:
                st.subheader("Failure Timeline (Infant vs Old Age)")
                if not fail_df.empty:
                    fig = px.histogram(fail_df, x="hours", nbins=40, title="At what age do they fail?",
                                     labels={'hours': 'Power On Hours'}, color_discrete_sequence=['#ef553b'])
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No failures found for this model in the lifetime data.")

            with c_right:
                # Bathtub Curve Context
                st.subheader("Reliability Context")
                st.write(f"**First Observed:** {stats_df['first_seen'][0]}")
                st.write(f"**Last Observed:** {stats_df['last_seen'][0]}")
                
                # Dynamic Insight
                if afr > 2.0:
                    st.error("⚠️ High Risk: This model's lifetime failure rate is significantly above the 1.3% fleet average.")
                elif afr < 0.8:
                    st.success("✅ Top Tier: This model is exceptionally reliable compared to the fleet average.")
                else:
                    st.info("📊 Average: This model performs within normal operational parameters.")

        else:
            st.warning("No data found for this model. Try a partial name like 'MG08'.")

    except Exception as e:
        st.error(f"Engine Error: {e}")
