# pages/2_Stamper.py
import streamlit as st
import pandas as pd
from datetime import datetime
from utils import *

st.set_page_config(page_title="Stamper", layout="wide")

# Check if we're just passing through after a reset
if st.session_state.get('return_to_sizer_after_reset', False):
    # Restore the run selection that was active
    if 'reset_run_key' in st.session_state:
        st.session_state.selected_run_key = st.session_state['reset_run_key']
        st.session_state.selected_run_data = st.session_state['reset_run_data']
        # Clean up temporary flags
        del st.session_state['reset_run_key']
        del st.session_state['reset_run_data']
    # Immediately bounce back to Sizer
    st.switch_page("pages/3_Sizer.py")

if 'selected_run_key' not in st.session_state:
    st.error("⚠️ No run selected")
    if st.button("← Back to Main"):
        st.switch_page("streamlit_app.py")
    st.stop()

row = pd.Series(st.session_state.selected_run_data)
run_key_suffix = f"*{st.session_state.selected_run_key}"

st.title("📊 Stamper")
st.subheader(f"Run {row['RUN_NUMBER']} — {row['VARIETY_USER_CD']} — Grower {row['GROWER']} — {row['PACK_LINE']} — Pool {row['POOL']} — Date {row['RUN_DATE']}")

st.info("**Raw Bins** = total dumped for this grower on this shift (across all lines). Use as starting point for verification.")
st.warning("All recommended data is derived from raw sources and may be approximate. Please verify and confirm values.")

# Stamper details
st.markdown("**Stamper Details**")
stamper_df = load_stamper_data(row.get('GROWER', row.get('Grower')), row.get('RUN_DATE', row.get('Run Date')))
raw_stamper_weight = 0.0
if stamper_df.empty:
    st.info("No stamper data found for this grower.")
else:
    st.write(f"Found {len(stamper_df)} stamper entries. Green = high probability match (variety & shift). Yellow = partial match. Select with checkboxes:")
    if 'SELECT' not in stamper_df.columns:
        stamper_df['SELECT'] = False
    stamper_session_key = f"edited_stamper_df{run_key_suffix}"
    if stamper_session_key not in st.session_state:
        df_copy = stamper_df.copy()
        for idx, r in df_copy.iterrows():
            variety_match = row['VARIETY_USER_CD'].lower() in str(r['NAME_VAR']).lower()
            shift_match = str(row['SHIFT']) == str(r['SHIFT1'])
            if variety_match and shift_match:
                df_copy.at[idx, 'SELECT'] = True
        st.session_state[stamper_session_key] = df_copy
    def highlight_stamper_matches(r):
        variety_match = row['VARIETY_USER_CD'].lower() in str(r['NAME_VAR']).lower()
        shift_match = str(row['SHIFT']) == str(r['SHIFT1'])
        if variety_match and shift_match:
            return 'lightgreen'
        elif variety_match or shift_match:
            return '#fffacd'
        return 'transparent'
    with st.form(key=f"stamper_form{run_key_suffix}"):
        for idx, r in st.session_state[stamper_session_key].iterrows():
            color = highlight_stamper_matches(r)
            st.markdown(f'<div style="background-color: {color}; padding: 10px; border-radius: 5px; margin-bottom: 5px;">', unsafe_allow_html=True)
            cols = st.columns([1.5, 1, 2, 1.5, 2, 2, 1])
            cols[0].write(r['D'])
            cols[1].write(r['SHIFT1'])
            cols[2].write(r['DATE_SHIFT_KEY'])
            cols[3].write(r.get('GROWERID_GRO', ''))
            cols[4].write(r['NAME_VAR'])
            cols[5].write(r['STAMPER_WEIGHT'])
            checkbox_key = f"stamper_cb_{idx}{run_key_suffix}"
            cols[6].checkbox("Select", value=r['SELECT'], key=checkbox_key, label_visibility="collapsed")
            st.markdown('</div>', unsafe_allow_html=True)
        stamper_submitted = st.form_submit_button("Confirm Stamper Selections")
        if stamper_submitted:
            for idx in range(len(st.session_state[stamper_session_key])):
                checkbox_key = f"stamper_cb_{idx}{run_key_suffix}"
                st.session_state[stamper_session_key].at[idx, 'SELECT'] = st.session_state[checkbox_key]
    selected_stamper_rows = st.session_state[stamper_session_key][st.session_state[stamper_session_key]['SELECT']]
    raw_stamper_weight = float(selected_stamper_rows['STAMPER_WEIGHT'].sum()) if not selected_stamper_rows.empty else 0.0

default_stamper_weight = float(row["ACTUAL_STAMPER_WEIGHT"]) if row["ACTUAL_STAMPER_WEIGHT"] > 0 else raw_stamper_weight
verified_stamper_key = f"verified_stamper_weight{run_key_suffix}"
if verified_stamper_key not in st.session_state:
    st.session_state[verified_stamper_key] = default_stamper_weight

col1, col2 = st.columns(2)
with col1:
    st.markdown(f"<span style='background-color: yellow; color: black;'>Raw Recommendation: {raw_stamper_weight} lbs</span>", unsafe_allow_html=True)
with col2:
    verified_stamper_weight = st.number_input("Verified Stamper Weight", min_value=0.0, step=0.1, format="%.2f", key=verified_stamper_key, label_visibility="visible")

# Show success message if just saved (at bottom)
if st.session_state.get('save_success'):
    st.success("✅ Stamper data saved successfully!")
    del st.session_state['save_success']

if st.button("💾 Save Stamper Data", type="primary"):
    with st.spinner("Saving..."):
        try:
            session.sql("BEGIN").collect()
            unique_run_key = row.get("UNIQUE_RUN_KEY", row.get("Unique Run Key"))
            bin_input_data = {
                'UNIQUE_RUN_KEY': unique_run_key,
                'ACTUAL_STAMPER_WEIGHT': verified_stamper_weight,
                'LAST_UPDATED_BY': session.sql("SELECT CURRENT_USER()").collect()[0][0],
                'LAST_UPDATED_AT': datetime.now()
            }
            upsert_single_row(session, "FROSTY.APP.PTRUN_BIN_INPUT", bin_input_data)
            session.sql("COMMIT").collect()

            # Refresh materialized table for PowerBI
            session.sql("DELETE FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER_MAT WHERE UNIQUE_RUN_KEY = ?", params=[unique_run_key]).collect()
            session.sql("""
                INSERT INTO FROSTY.APP.POWERBI_PRODUCTION_HEADER_MAT
                SELECT * FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER
                WHERE UNIQUE_RUN_KEY = ?
            """, params=[unique_run_key]).collect()
            session.sql("COMMIT").collect()
            
            # REFRESH session state with updated data from database
            updated_row = session.sql("""
                SELECT * FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER
                WHERE UNIQUE_RUN_KEY = ?
            """, params=[unique_run_key]).to_pandas()
            if not updated_row.empty:
                st.session_state.selected_run_data = updated_row.iloc[0].to_dict()
            
            # Set success flag before clearing
            st.session_state['save_success'] = True
            
            # Clear session states for this run (except the core selection)
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
    if st.button("← Bins & Breakdown"):
        st.switch_page("pages/1_Bins_Breakdown.py")
with col2:
    if st.button("Next: Sizer →"):
        st.switch_page("pages/3_Sizer.py")