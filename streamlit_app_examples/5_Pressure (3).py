# pages/5_Pressure.py
import streamlit as st
import pandas as pd
from datetime import datetime
from utils import *

st.set_page_config(page_title="Pressure", layout="wide")  # Unique title to avoid pathname conflict

if 'selected_run_key' not in st.session_state:
    st.error("⚠️ No run selected")
    if st.button("← Back to Main"):
        st.switch_page("streamlit_app.py")
    st.stop()

row = pd.Series(st.session_state.selected_run_data)
run_key_suffix = f"*{st.session_state.selected_run_key}"

st.title("🔬 Pressure")
st.subheader(f"Run {row['RUN_NUMBER']} — {row['VARIETY_USER_CD']} — Grower {row['GROWER']} — {row['PACK_LINE']} — Pool {row['POOL']} — Date {row['RUN_DATE']}")

st.warning("All recommended data is derived from raw sources and may be approximate. Please verify and confirm values.")

# Pressure Analysis Section
st.markdown("#### **Pressure Analysis**")
potential_pressures = load_potential_pressures(row.get("RUN_DATE", row.get("Run Date")), row.get("GROWER", row.get("Grower")))
potential_pressure_options = ["Manual Entry"] + [f"{row['ID']} - {row['DATE_YY_MM_DD']} - Shift {row['SHIFT']} - Grower {row['GROWER_NUMBER']} - {row['VARIETY']}" for _, row in potential_pressures.iterrows()] if not potential_pressures.empty else ["Manual Entry"]
existing_pressure_header = load_existing_pressure_header(row.get("UNIQUE_RUN_KEY", row.get("Unique Run Key")))
existing_pressure_details = load_existing_pressure_details(row.get("UNIQUE_RUN_KEY", row.get("Unique Run Key")))
default_selection = "Manual Entry"
if not existing_pressure_header.empty:
    default_pressure_id = existing_pressure_header.iloc[0].get("PRESSURE_ID")
    if default_pressure_id is not None:
        default_option = next((opt for opt in potential_pressure_options if opt.startswith(str(default_pressure_id) + ' - ')), "Manual Entry")
        default_selection = default_option
possible_fruit_sizes = [56, 64, 72, 80, 88, 100, 113, 125, 138, 150]
if f"pressure_cmi_inspector{run_key_suffix}" not in st.session_state:
    if not existing_pressure_header.empty:
        st.session_state[f"pressure_cmi_inspector{run_key_suffix}"] = existing_pressure_header.iloc[0].get("CMI_INSPECTOR") if pd.notnull(existing_pressure_header.iloc[0].get("CMI_INSPECTOR")) else ""
        for size in possible_fruit_sizes:
            st.session_state[f"pressure_{size}{run_key_suffix}"] = 0.0
        if not existing_pressure_details.empty:
            for _, p_row in existing_pressure_details.iterrows():
                size = int(p_row["FRUIT_SIZE_INT"]) if pd.notnull(p_row["FRUIT_SIZE_INT"]) else None
                if size in possible_fruit_sizes:
                    st.session_state[f"pressure_{size}{run_key_suffix}"] = float(p_row["PRESSURE_DEC"]) if pd.notnull(p_row["PRESSURE_DEC"]) else 0.0
    else:
        st.session_state[f"pressure_cmi_inspector{run_key_suffix}"] = ""
        for size in possible_fruit_sizes:
            st.session_state[f"pressure_{size}{run_key_suffix}"] = 0.0
previous_pressure_key = f"previous_pressure_option{run_key_suffix}"
if previous_pressure_key not in st.session_state:
    st.session_state[previous_pressure_key] = default_selection
previous_pressure_option = st.session_state[previous_pressure_key]
selected_pressure_option = st.selectbox("Select Pressure Inspection", options=potential_pressure_options, index=potential_pressure_options.index(default_selection))
if selected_pressure_option != previous_pressure_option:
    st.session_state[previous_pressure_key] = selected_pressure_option
    if selected_pressure_option == "Manual Entry":
        cmi_inspector = ""
        temp_pressure_values = {size: 0.0 for size in possible_fruit_sizes}
        if not existing_pressure_header.empty:
            header = existing_pressure_header.iloc[0]
            cmi_inspector = header.get("CMI_INSPECTOR") if pd.notnull(header.get("CMI_INSPECTOR")) else ""
            if not existing_pressure_details.empty:
                for _, p_row in existing_pressure_details.iterrows():
                    size = int(p_row["FRUIT_SIZE_INT"]) if pd.notnull(p_row["FRUIT_SIZE_INT"]) else None
                    if size in temp_pressure_values:
                        temp_pressure_values[size] = p_row["PRESSURE_DEC"] if pd.notnull(p_row["PRESSURE_DEC"]) else 0.0
        st.session_state[f"pressure_cmi_inspector{run_key_suffix}"] = cmi_inspector
        for size in possible_fruit_sizes:
            st.session_state[f"pressure_{size}{run_key_suffix}"] = temp_pressure_values[size]
    else:
        selected_pressure = selected_pressure_option.split(' - ')[0]
        cmi_inspector = ""
        temp_pressure_values = {size: 0.0 for size in possible_fruit_sizes}
        header_df = load_pressure_header(selected_pressure)
        if not header_df.empty:
            header = header_df.iloc[0]
            cmi_inspector = header.get("CMI_INSPECTOR") if pd.notnull(header.get("CMI_INSPECTOR")) else ""
            pressure_details_df = load_pressure_details(selected_pressure)
            for _, p_row in pressure_details_df.iterrows():
                size = int(p_row["FRUIT_SIZE_INT"]) if pd.notnull(p_row["FRUIT_SIZE_INT"]) else None
                if size in temp_pressure_values:
                    temp_pressure_values[size] = p_row["PRESSURE_DEC"] if pd.notnull(p_row["PRESSURE_DEC"]) else 0.0
        st.session_state[f"pressure_cmi_inspector{run_key_suffix}"] = cmi_inspector
        for size in possible_fruit_sizes:
            st.session_state[f"pressure_{size}{run_key_suffix}"] = temp_pressure_values[size]
    st.rerun()
st.markdown("**Header Details**")
st.text_input("CMI Inspector", value=st.session_state.get(f"pressure_cmi_inspector{run_key_suffix}", ""), key=f"pressure_cmi_inspector{run_key_suffix}")
st.markdown("**Pressures**")
with st.form(key=f"pressure_form{run_key_suffix}"):
    for size in possible_fruit_sizes:
        col1, col2 = st.columns([4, 2])
        with col1:
            st.write(size)
        with col2:
            pressure_key = f"pressure_{size}{run_key_suffix}"
            st.number_input("", min_value=0.0, step=0.1, value=st.session_state.get(pressure_key, 0.0), key=pressure_key, label_visibility="collapsed")
    pressures_submitted = st.form_submit_button("Confirm Pressures")
    if pressures_submitted:
        st.rerun()

selected_pressure = None if selected_pressure_option == "Manual Entry" else selected_pressure_option.split(' - ')[0]

if st.button("💾 Save Pressure Data", type="primary"):
    with st.spinner("Saving..."):
        try:
            session.sql("BEGIN").collect()
            unique_run_key = row.get("UNIQUE_RUN_KEY", row.get("Unique Run Key"))
            pressure_header_data = {
                'UNIQUE_RUN_KEY': unique_run_key,
                'PRESSURE_ID': selected_pressure,
                'DATE_YY_MM_DD': row['RUN_DATE'],
                'SHIFT': row['SHIFT'],
                'GROWER_NUMBER': row['GROWER'],
                'COMPUTECH_ABBR': row['VARIETY_USER_CD'],
                'CMI_INSPECTOR': st.session_state[f"pressure_cmi_inspector{run_key_suffix}"],
                'LAST_UPDATED_BY': session.sql("SELECT CURRENT_USER()").collect()[0][0],
                'LAST_UPDATED_AT': datetime.now()
            }
            upsert_single_row(session, "FROSTY.APP.PTRUN_PRESSURE_HEADER", pressure_header_data)
            session.sql("DELETE FROM FROSTY.APP.PTRUN_PRESSURE_DETAIL WHERE UNIQUE_RUN_KEY = ?", params=[unique_run_key]).collect()
            for size in possible_fruit_sizes:
                key = f"pressure_{size}{run_key_suffix}"
                value = st.session_state.get(key, 0.0)
                if value > 0:
                    session.sql("""
                        INSERT INTO FROSTY.APP.PTRUN_PRESSURE_DETAIL (UNIQUE_RUN_KEY, FRUIT_SIZE_INT, PRESSURE_DEC)
                        VALUES (?, ?, ?)
                    """, params=[unique_run_key, size, value]).collect()
            session.sql("COMMIT").collect()


            # Refresh materialized table for PowerBI
            session.sql("DELETE FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER_MAT WHERE UNIQUE_RUN_KEY = ?", params=[unique_run_key]).collect()
            session.sql("""
                INSERT INTO FROSTY.APP.POWERBI_PRODUCTION_HEADER_MAT
                SELECT * FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER
                WHERE UNIQUE_RUN_KEY = ?
            """, params=[unique_run_key]).collect()
            session.sql("COMMIT").collect()
            
            # Set success flag before clearing
            st.session_state['save_success'] = True

            
            
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
    if st.button("← Cull"):
        st.switch_page("pages/4_Cull_Analysis.py")
with col2:
    if st.button("Back to Main →"):
        st.switch_page("streamlit_app.py")