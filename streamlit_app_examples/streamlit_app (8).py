# streamlit_app.py (Updated with PDF page navigation)
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from utils import *  # Import shared stuff

st.set_page_config(
    page_title="Apple Line Production — Bins & Weight Confirmation",
    layout="wide"
)

st.markdown("""
<style>
.block-container {
    max-width: none !important;
    padding-left: 1rem;
    padding-right: 1rem;
    padding-top: 1rem;
}
.reportview-container {
    max-width: 100% !important;
    width: 100% !important;
}
.main {
    max-width: 100% !important;
    padding: 1rem;
}
section[data-testid="stSidebar"] {
    min-width: 300px;
    max-width: 360px;
}

/* HIDE SIDEBAR PAGE NAVIGATION ON MAIN PAGE ONLY */
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] {
    display: none;
}

/* Smaller main title - 18px */
h1 {
    font-size: 18px !important;
    font-weight: 600;
    margin-bottom: 1.5rem;
    line-height: 1.3;
}
input[type="number"]::-webkit-inner-spin-button,
input[type="number"]::-webkit-outer-spin-button {
    -webkit-appearance: none;
    margin: 0;
}
input[type="number"] {
    -moz-appearance: textfield;
}
/* Make all buttons blue (except danger zone) */
div.stButton > button {
    background-color: #007bff !important;
    color: white !important;
    border: 1px solid #0069d9 !important;
}
div.stButton > button:hover {
    background-color: #0069d9 !important;
    border-color: #0062cc !important;
}
div.stButton > button:active {
    background-color: #0062cc !important;
}
</style>
""", unsafe_allow_html=True)

st.title("Apple Line Production — Bins & Weight Confirmation")

ptrun_df = load_ptrun()
rec_df = load_recommendation()
tare_details_df = load_tare_details()

ptrun_df["RUN_DATE"] = pd.to_datetime(ptrun_df["RUN_DATE"]).dt.date

# Create join keys
ptrun_df["JOIN_KEY"] = (
    ptrun_df["RUN_DATE"].astype(str) + "-" +
    ptrun_df["SHIFT"].astype(str) + "-" +
    ptrun_df["GROWER"].astype(str).str.zfill(4)
)
rec_df["JOIN_KEY"] = (
    rec_df["RUN_DATE"].astype(str) + "-" +
    rec_df["SHIFT"].astype(str) + "-" +
    rec_df["GROWER_CODE"].astype(str).str.zfill(4)
)
tare_details_df["JOIN_KEY"] = (
    tare_details_df["RUN_DATE"].astype(str) + "-" +
    tare_details_df["SHIFT"].astype(str) + "-" +
    tare_details_df["GROWER_CODE"].astype(str).str.zfill(4)
)

# Check for query param to pre-select a run
query_params = st.query_params
pre_selected_run = query_params.get("run", [None])[0]

# ========================
# Sidebar Filters
# ========================
st.sidebar.header("Filters")

default_date = ptrun_df["RUN_DATE"].max()
selected_date = st.sidebar.date_input("Run Date", value=default_date)

filtered_ptrun = ptrun_df[ptrun_df["RUN_DATE"] == selected_date].copy()

available_shifts = sorted(filtered_ptrun["SHIFT"].dropna().unique())
selected_shift = st.sidebar.multiselect(
    "Shift",
    options=available_shifts,
    default=available_shifts
)
if selected_shift:
    filtered_ptrun = filtered_ptrun[filtered_ptrun["SHIFT"].isin(selected_shift)]

available_growers = sorted(filtered_ptrun["GROWER"].unique())
selected_grower = st.sidebar.multiselect(
    "Grower Code",
    options=available_growers,
    default=available_growers
)
if selected_grower:
    filtered_ptrun = filtered_ptrun[filtered_ptrun["GROWER"].isin(selected_grower)]

# If pre-selected run, filter to it (overrides other filters for simplicity)
if pre_selected_run:
    filtered_ptrun = filtered_ptrun[filtered_ptrun["UNIQUE_RUN_KEY"] == pre_selected_run]
    if filtered_ptrun.empty:
        st.error("Pre-selected run not found or not matching filters.")
        st.stop()

filter_desc = f"**Showing {len(filtered_ptrun)} runs** for {selected_date}"
if selected_shift:
    filter_desc += f", Shift(s): {', '.join(map(str, selected_shift))}"
if selected_grower:
    filter_desc += f", Grower(s): {', '.join(map(str, selected_grower))}"
st.write(filter_desc)

if filtered_ptrun.empty:
    st.info("No runs found with the selected filters.")
    st.stop()

# Join recommendation
display_df = filtered_ptrun.merge(
    rec_df,
    on="JOIN_KEY",
    how="left",
    suffixes=("", "_rec")
)

# Fill missing values
display_df["REC_BIN_COUNT"] = display_df["REC_BIN_COUNT"].fillna(0).astype(int)
display_df["REC_NET_WEIGHT"] = display_df["REC_NET_WEIGHT"].fillna(0).astype(int)
display_df["REC_TARE_WEIGHT"] = display_df["REC_TARE_WEIGHT"].fillna(0).astype(int)
display_df["REC_AVG_TARE_PER_BIN"] = display_df["REC_AVG_TARE_PER_BIN"].fillna(0).astype(float)
display_df["REC_GROSS_WEIGHT"] = display_df["REC_GROSS_WEIGHT"].fillna(0).astype(int)
display_df["REC_DUMPER_HMS"] = display_df["REC_DUMPER_HMS"].fillna("00:00:00")

main_df = display_df.rename(columns=renamed_columns)[list(renamed_columns.values())].copy()
main_df["Run Date"] = main_df["Run Date"].astype(str)
main_df['Bin Type'] = main_df['Bin Type'].fillna('None')
main_df['Tare Breakdown'] = main_df['Tare Breakdown'].fillna('')
main_df['Bin Type'] = main_df.apply(lambda r: ', '.join([part.split('=')[0].strip() for part in r['Tare Breakdown'].split(', ') if '=' in part]) if r['Bin Type'] == 'None' and r['Tare Breakdown'] else r['Bin Type'], axis=1)

styled_main = main_df.style.apply(highlight_unsubmitted, axis=1)

selection = st.dataframe(
    styled_main,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    key="main_table"
)

st.caption("💡 **Yellow row** = No submission yet. **Raw Bins** = total dumped for grower/shift (all lines). **Actual Bins** = verified value for this run. Click a row to verify/submit.")

# Auto-select the only row if pre-selected
selected_rows = st.session_state.get("main_table", {}).get("selection", {}).get("rows", [])
if pre_selected_run and not selected_rows and not display_df.empty:
    selected_rows = [0]

if selected_rows:
    idx = selected_rows[0]
    row = display_df.iloc[idx]
    st.session_state.selected_run_key = row['UNIQUE_RUN_KEY']
    st.session_state.selected_run_data = row.to_dict()

    st.subheader(f"Run {row['RUN_NUMBER']} — {row['VARIETY_USER_CD']} — Grower {row['GROWER']} — {row['PACK_LINE']} — Pool {row['POOL']} — Date {row['RUN_DATE']}")

    # Navigation buttons - now 6 columns including PDF
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        if st.button("📦 Bins & Breakdown", use_container_width=True, type="primary"):
            st.switch_page("pages/1_Bins_Breakdown.py")
    with col2:
        if st.button("📊 Stamper", use_container_width=True, type="primary"):
            st.switch_page("pages/2_Stamper.py")
    with col3:
        if st.button("⚙️ Sizer", use_container_width=True, type="primary"):
            st.switch_page("pages/3_Sizer.py")
    with col4:
        if st.button("🍎 Cull", use_container_width=True, type="primary"):
            st.switch_page("pages/4_Cull_Analysis.py")
    with col5:
        if st.button("🔬 Pressure", use_container_width=True, type="primary"):
            st.switch_page("pages/5_Pressure.py")
    with col6:
        if st.button("📄 PDF Report", use_container_width=True, type="primary"):
            st.switch_page("pages/6_PDF.py")
