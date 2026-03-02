# pages/4_Cull_Analysis.py
import streamlit as st
import pandas as pd
from datetime import datetime
from utils import *

st.set_page_config(page_title="Cull Analysis", layout="wide")

if 'selected_run_key' not in st.session_state:
    st.error("⚠️ No run selected")
    if st.button("← Back to Main"):
        st.switch_page("streamlit_app.py")
    st.stop()

row = pd.Series(st.session_state.selected_run_data)
run_key_suffix = f"*{st.session_state.selected_run_key}"

st.title("🍎 Cull Analysis")
st.subheader(f"Run {row['RUN_NUMBER']} — {row['VARIETY_USER_CD']} — Grower {row['GROWER']} — {row['PACK_LINE']} — Pool {row['POOL']} — Date {row['RUN_DATE']}")

st.markdown("#### **Cull Analysis**")
potential_df = load_potential_culls(row.get("RUN_DATE", row.get("Run Date")), row.get("GROWER", row.get("Grower")))
potential_options = ["Manual Entry"] + [f"{row['Id']} - {row['DATE_YY_MM_DD']} - Shift {row['PACKING_LINE_ORDER']} - Grower {row['GROWER_NUMBER']} - {row['VARIETY']}" for _, row in potential_df.iterrows()] if not potential_df.empty else ["Manual Entry"]
existing_cull_header = load_existing_cull_header(row.get("UNIQUE_RUN_KEY", row.get("Unique Run Key")))
existing_cull_defects = load_existing_cull_defects(row.get("UNIQUE_RUN_KEY", row.get("Unique Run Key")))
default_selection = "Manual Entry"
if not existing_cull_header.empty:
    default_cull_id = existing_cull_header.iloc[0].get("CULL_ID")
    if default_cull_id is not None:
        default_option = next((opt for opt in potential_options if opt.startswith(str(default_cull_id) + ' - ')), "Manual Entry")
        default_selection = default_option
possible_defects = [
    "81 Visible Watercore",
    "79 Thrip",
    "78 Sunburn",
    "77 Stink Bug",
    "76 Stain",
    "75 Splits",
    "74 Shrivel",
    "73 Scale",
    "72 Scald",
    "71 Russet or Frost",
    "70 Puncture",
    "69 Pandemis Leafroller",
    "68 Other Insect",
    "67 Off Shape",
    "66 Low Color",
    "65 Limb Rub",
    "64 Lenticel Decay",
    "62 Hail",
    "61 Decay",
    "60 Cut Worm",
    "59 Codling Moth",
    "57 Clipper Damage",
    "56 Campyloma",
    "55 Calcium",
    "54Bruise-Thinning",
    "53 Bruise-Picking",
    "52 Bitterpit",
    "51 Bird Peck"
]
if f"cull_bin_temp{run_key_suffix}" not in st.session_state:
    if not existing_cull_header.empty:
        st.session_state[f"cull_bin_temp{run_key_suffix}"] = float(existing_cull_header.iloc[0].get("BIN_TEMP") ) if pd.notnull(existing_cull_header.iloc[0].get("BIN_TEMP")) else 0.0
        st.session_state[f"cull_tub_temp{run_key_suffix}"] = float(existing_cull_header.iloc[0].get("TUB_TEMP") ) if pd.notnull(existing_cull_header.iloc[0].get("TUB_TEMP")) else 0.0
        st.session_state[f"cull_cmi_inspector{run_key_suffix}"] = existing_cull_header.iloc[0].get("CMI_INSPECTOR") if pd.notnull(existing_cull_header.iloc[0].get("CMI_INSPECTOR")) else ""
        for defect in possible_defects:
            st.session_state[f"cull_defect_count_{defect.replace(' ', '_').replace('-', '_')}{run_key_suffix}"] = 0
        if not existing_cull_defects.empty:
            for _, d_row in existing_cull_defects.iterrows():
                defect_type = d_row["DEFECT_TYPE"]
                if defect_type in possible_defects:
                    defect_key = f"cull_defect_count_{defect_type.replace(' ', '_').replace('-', '_')}{run_key_suffix}"
                    st.session_state[defect_key] = int(d_row["COUNT_INT"]) if pd.notnull(d_row["COUNT_INT"]) else 0
    else:
        st.session_state[f"cull_bin_temp{run_key_suffix}"] = 0.0
        st.session_state[f"cull_tub_temp{run_key_suffix}"] = 0.0
        st.session_state[f"cull_cmi_inspector{run_key_suffix}"] = ""
        for defect in possible_defects:
            st.session_state[f"cull_defect_count_{defect.replace(' ', '_').replace('-', '_')}{run_key_suffix}"] = 0
previous_cull_key = f"previous_cull_option{run_key_suffix}"
if previous_cull_key not in st.session_state:
    st.session_state[previous_cull_key] = default_selection
previous_cull_option = st.session_state[previous_cull_key]
selected_cull_option = st.selectbox("Select Cull Inspection", options=potential_options, index=potential_options.index(default_selection))
if selected_cull_option != previous_cull_option:
    st.session_state[previous_cull_key] = selected_cull_option
    if selected_cull_option == "Manual Entry":
        bin_temp = 0.0
        tub_temp = 0.0
        cmi_inspector = ""
        temp_defect_counts = {defect: 0 for defect in possible_defects}
        if not existing_cull_header.empty:
            header = existing_cull_header.iloc[0]
            bin_temp = float(header.get("BIN_TEMP")) if pd.notnull(header.get("BIN_TEMP")) else 0.0
            tub_temp = float(header.get("TUB_TEMP")) if pd.notnull(header.get("TUB_TEMP")) else 0.0
            cmi_inspector = header.get("CMI_INSPECTOR") if pd.notnull(header.get("CMI_INSPECTOR")) else ""
            if not existing_cull_defects.empty:
                for _, d_row in existing_cull_defects.iterrows():
                    defect_type = d_row["DEFECT_TYPE"]
                    if defect_type in temp_defect_counts:
                        temp_defect_counts[defect_type] = d_row["COUNT_INT"] if pd.notnull(d_row["COUNT_INT"]) else 0
        st.session_state[f"cull_bin_temp{run_key_suffix}"] = bin_temp
        st.session_state[f"cull_tub_temp{run_key_suffix}"] = tub_temp
        st.session_state[f"cull_cmi_inspector{run_key_suffix}"] = cmi_inspector
        for defect in possible_defects:
            defect_key = f"cull_defect_count_{defect.replace(' ', '_').replace('-', '_')}{run_key_suffix}"
            st.session_state[defect_key] = temp_defect_counts[defect]
    else:
        selected_cull = selected_cull_option.split(' - ')[0]
        bin_temp = 0.0
        tub_temp = 0.0
        cmi_inspector = ""
        temp_defect_counts = {defect: 0 for defect in possible_defects}
        header_df = load_cull_header(selected_cull)
        if not header_df.empty:
            header = header_df.iloc[0]
            bin_temp = float(header.get("BIN_TEMP")) if pd.notnull(header.get("BIN_TEMP")) else 0.0
            tub_temp = float(header.get("TUB_TEMP")) if pd.notnull(header.get("TUB_TEMP")) else 0.0
            cmi_inspector = header.get("CMI_INSPECTOR") if pd.notnull(header.get("CMI_INSPECTOR")) else ""
            defects_df = load_cull_defects(selected_cull)
            for _, d_row in defects_df.iterrows():
                defect_type = d_row["DEFECT_TYPE"]
                if defect_type in temp_defect_counts:
                    temp_defect_counts[defect_type] = d_row["COUNT_INT"] if pd.notnull(d_row["COUNT_INT"]) else 0
        st.session_state[f"cull_bin_temp{run_key_suffix}"] = bin_temp
        st.session_state[f"cull_tub_temp{run_key_suffix}"] = tub_temp
        st.session_state[f"cull_cmi_inspector{run_key_suffix}"] = cmi_inspector
        for defect in possible_defects:
            defect_key = f"cull_defect_count_{defect.replace(' ', '_').replace('-', '_')}{run_key_suffix}"
            st.session_state[defect_key] = temp_defect_counts[defect]
    st.rerun()
st.markdown("**Header Details**")
st.number_input("Bin Temp", min_value=0.0, step=0.1, value=st.session_state.get(f"cull_bin_temp{run_key_suffix}", 0.0), key=f"cull_bin_temp{run_key_suffix}")
st.number_input("Tub Temp", min_value=0.0, step=0.1, value=st.session_state.get(f"cull_tub_temp{run_key_suffix}", 0.0), key=f"cull_tub_temp{run_key_suffix}")
st.text_input("CMI Inspector", value=st.session_state.get(f"cull_cmi_inspector{run_key_suffix}", ""), key=f"cull_cmi_inspector{run_key_suffix}")
st.markdown("**Defects**")
with st.form(key=f"cull_defects_form{run_key_suffix}"):
    for defect in possible_defects:
        col1, col2 = st.columns([4, 2])
        with col1:
            st.write(defect)
        with col2:
            defect_key = f"cull_defect_count_{defect.replace(' ', '_').replace('-', '_')}{run_key_suffix}"
            st.number_input("", min_value=0, step=1, value=st.session_state.get(defect_key, 0), key=defect_key, label_visibility="collapsed")
    defects_submitted = st.form_submit_button("Confirm Defects")
    if defects_submitted:
        st.rerun()
current_defect_sum = 0
for defect in possible_defects:
    defect_key = f"cull_defect_count_{defect.replace(' ', '_').replace('-', '_')}{run_key_suffix}"
    current_defect_sum += st.session_state.get(defect_key, 0)
st.write(f"Total Defect Count: {current_defect_sum}")

selected_cull = None if selected_cull_option == "Manual Entry" else selected_cull_option.split(' - ')[0]

if st.button("💾 Save Cull Data", type="primary"):
    with st.spinner("Saving..."):
        try:
            session.sql("BEGIN").collect()
            unique_run_key = row.get("UNIQUE_RUN_KEY", row.get("Unique Run Key"))
            cull_header_data = {
                'UNIQUE_RUN_KEY': unique_run_key,
                'CULL_ID': selected_cull,
                'DATE_YY_MM_DD': row['RUN_DATE'],
                'SHIFT': row['SHIFT'],
                'GROWER_NUMBER': row['GROWER'],
                'COMPUTECH_ABBR': row['VARIETY_USER_CD'],
                'BIN_TEMP': st.session_state[f"cull_bin_temp{run_key_suffix}"],
                'TUB_TEMP': st.session_state[f"cull_tub_temp{run_key_suffix}"],
                'CMI_INSPECTOR': st.session_state[f"cull_cmi_inspector{run_key_suffix}"],
                'LAST_UPDATED_BY': session.sql("SELECT CURRENT_USER()").collect()[0][0],
                'LAST_UPDATED_AT': datetime.now()
            }
            upsert_single_row(session, "FROSTY.APP.PTRUN_CULL_HEADER", cull_header_data)
            session.sql("DELETE FROM FROSTY.APP.PTRUN_CULL_DEFECT WHERE UNIQUE_RUN_KEY = ?", params=[unique_run_key]).collect()
            for defect in possible_defects:
                key = f"cull_defect_count_{defect.replace(' ', '_').replace('-', '_')}{run_key_suffix}"
                count = st.session_state.get(key, 0)
                if count > 0:
                    session.sql("""
                        INSERT INTO FROSTY.APP.PTRUN_CULL_DEFECT (UNIQUE_RUN_KEY, DEFECT_TYPE, COUNT_INT)
                        VALUES (?, ?, ?)
                    """, params=[unique_run_key, defect, count]).collect()
            session.sql("COMMIT").collect()

            # Refresh materialized table for PowerBI
            session.sql("DELETE FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER_MAT WHERE UNIQUE_RUN_KEY = ?", params=[unique_run_key]).collect()
            session.sql("""
                INSERT INTO FROSTY.APP.POWERBI_PRODUCTION_HEADER_MAT
                SELECT * FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER
                WHERE UNIQUE_RUN_KEY = ?
            """, params=[unique_run_key]).collect()
            session.sql("COMMIT").collect()
            
            st.success("✅ Cull data saved!")
            # Clear session states
            keys_to_delete = [k for k in st.session_state if k.endswith(run_key_suffix)]
            for k in keys_to_delete:
                del st.session_state[k]
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            session.sql("ROLLBACK").collect()
            st.error(f"Error: {str(e)}")

col1, col2 = st.columns([1, 1])
with col1:
    if st.button("← Sizer"):
        st.switch_page("pages/3_Sizer.py")
with col2:
    if st.button("Next: Pressure →"):
        st.switch_page("pages/5_Pressure.py")