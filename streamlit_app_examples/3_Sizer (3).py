import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from utils import *
st.set_page_config(page_title="Sizer", layout="wide")
if 'selected_run_key' not in st.session_state:
    st.error("⚠️ No run selected")
    if st.button("← Back to Main"):
        st.switch_page("streamlit_app.py")
    st.stop()
row = pd.Series(st.session_state.selected_run_data)
run_key_suffix = f"*{st.session_state.selected_run_key}"

# Check if we just reset and need to return
if st.session_state.get('return_to_sizer_after_reset', False):
    # Clear the return flag
    del st.session_state['return_to_sizer_after_reset']
    # Show success message
    st.success("✅ Reset complete! Size/grade selections restored to defaults.")
    # Continue loading page normally - it will now have fresh data

st.title("⚙️ Sizer")
st.subheader(f"Run {row['RUN_NUMBER']} — {row['VARIETY_USER_CD']} — Grower {row['GROWER']} — {row['PACK_LINE']} — Pool {row['POOL']} — Date {row['RUN_DATE']}")
# Sizer lookup date
default_date = row.get('RUN_DATE', row.get('Run Date'))
lookup_date_key = f"sizer_lookup_date{run_key_suffix}"
if lookup_date_key not in st.session_state:
    st.session_state[lookup_date_key] = default_date
lookup_date = st.date_input("Sizer Data Lookup Date", value=st.session_state[lookup_date_key], key=lookup_date_key)
# Sizer details
st.markdown("**Sizer Details**")
sizer_df = load_sizer_headers(lookup_date)
raw_sizer_weight = 0.0
if sizer_df.empty:
    st.info("No sizer runs found for this date.")
else:
    st.write(f"Found {len(sizer_df)} sizer entries for the day. Green = high probability match (grower & variety). Yellow = partial match (grower or variety). Select with checkboxes:")
    if 'SELECT' not in sizer_df.columns:
        sizer_df['SELECT'] = False
    sizer_session_key = f"edited_sizer_df{run_key_suffix}"
    if sizer_session_key not in st.session_state:
        existing_keys = row.get("SIZER_FOREIGN_KEYS", "")
        df_copy = sizer_df.copy()
        if existing_keys:
            for key in existing_keys.split(','):
                if '/EventId:' in key:
                    batch_id = key.split('/EventId:')[0].split('BatchID:')[1]
                    event_id = key.split('/EventId:')[1]
                    mask = (df_copy['BATCH_ID'] == batch_id) & (df_copy['EVENT_ID'] == event_id)
                    df_copy.loc[mask, 'SELECT'] = True
        st.session_state[sizer_session_key] = df_copy
    def highlight_matches(r):
        grower_match = str(r['GROWER_CODE']) == str(row['GROWER'])
        variety_match = row['VARIETY_USER_CD'].lower() in str(r['VARIETY_NAME']).lower()
        if grower_match and variety_match:
            return 'lightgreen'
        elif grower_match or variety_match:
            return '#fffacd'
        return 'transparent'
    with st.form(key=f"sizer_form{run_key_suffix}"):
        for idx, r in st.session_state[sizer_session_key].iterrows():
            color = highlight_matches(r)
            st.markdown(f'<div style="background-color: {color}; padding: 10px; border-radius: 5px; margin-bottom: 5px;">', unsafe_allow_html=True)
            cols = st.columns([1.2, 2, 2.5, 2.5, 1.5, 1.5, 1.5, 1.2, 1])
            cols[0].write(r['GROWER_CODE'])
            cols[1].write(r['VARIETY_NAME'])
            cols[2].write(r['START_TIME'])
            cols[3].write(r['END_TIME'])
            cols[4].write(r['BATCH_ID'])
            cols[5].write(r['EVENT_ID'])
            cols[6].write(r['EVENT_TYPE'])
            cols[7].write(r['SHIFT'])
            checkbox_key = f"sizer_cb_{idx}{run_key_suffix}"
            cols[8].checkbox("Select", value=r['SELECT'], key=checkbox_key, label_visibility="collapsed")
            st.markdown('</div>', unsafe_allow_html=True)
        sizer_submitted = st.form_submit_button("Confirm Sizer Selections")
        if sizer_submitted:
            for idx in range(len(st.session_state[sizer_session_key])):
                checkbox_key = f"sizer_cb_{idx}{run_key_suffix}"
                st.session_state[sizer_session_key].at[idx, 'SELECT'] = st.session_state[checkbox_key]
            # Clear the size-grade overrides session state so it gets recreated with new event IDs
            sg_session_key = f"edited_size_grade_df{run_key_suffix}"
            if sg_session_key in st.session_state:
                del st.session_state[sg_session_key]
            group_expand_key = f"group_expand{run_key_suffix}"
            if group_expand_key in st.session_state:
                del st.session_state[group_expand_key]
            st.rerun()
    selected_rows = st.session_state[sizer_session_key][st.session_state[sizer_session_key]['SELECT']]
    selected_pairs = [f"BatchID:{r['BATCH_ID']}/EventId:{r['EVENT_ID']}" for _, r in selected_rows.iterrows()]
    verified_sizer_foreign_keys = ','.join(selected_pairs) if selected_pairs else ""
    event_ids = selected_rows['EVENT_ID'].tolist()
    drops_df = load_sizer_drops(event_ids)
# Sizer Size-Grade Overrides (grouped, condensed, with headers and spacing)
st.markdown("**Sizer Size-Grade Overrides**")
# Check if we have saved data for this run
existing_sizer_packed = load_existing_sizer_packed(row['UNIQUE_RUN_KEY'])
has_saved_sizer_data = not existing_sizer_packed.empty
# Show status indicator and reset button
status_col, reset_col = st.columns([4, 1])
with status_col:
    if has_saved_sizer_data:
        st.success("📁 **Loaded from saved selections.** Click 'Reset to Raw' to restore defaults (you must Save after to keep changes).")
    else:
        st.info("🔄 **Using default auto-selections.** Modify as needed and click Save to store your choices.")
with reset_col:
    if has_saved_sizer_data:
        reset_button_key = f"reset_sizer_packed_btn{run_key_suffix}"
        if st.button("🔄 Reset to Raw", key=reset_button_key, help="Reset selections to defaults while keeping sizer runs"):
            unique_run_key_to_reset = row.get('UNIQUE_RUN_KEY')
            
            if unique_run_key_to_reset and len(str(unique_run_key_to_reset).strip()) > 0:
                # Delete saved overrides from database
                session.sql("""
                    DELETE FROM FROSTY.APP.PTRUN_SIZER_PACKED
                    WHERE UNIQUE_RUN_KEY = ?
                """, params=[unique_run_key_to_reset]).collect()
                
                session.sql("COMMIT").collect()
                
                # Clear size/grade overrides
                sg_session_key_to_clear = f"edited_size_grade_df{run_key_suffix}"
                if sg_session_key_to_clear in st.session_state:
                    del st.session_state[sg_session_key_to_clear]
                
                # Clear group expansion state
                group_expand_key_to_clear = f"group_expand{run_key_suffix}"
                if group_expand_key_to_clear in st.session_state:
                    del st.session_state[group_expand_key_to_clear]
                
                # Clear cache
                st.cache_data.clear()
                
                # Set flag to trigger auto-return after page switch
                st.session_state['return_to_sizer_after_reset'] = True
                st.session_state['reset_run_key'] = st.session_state.selected_run_key
                st.session_state['reset_run_data'] = st.session_state.selected_run_data
                
                # Navigate away then back to force clean reload
                st.switch_page("pages/2_Stamper.py")
            else:
                st.error("Cannot reset: No valid run key found.")
if drops_df.empty:
    st.info("No drop data found for selected sizer events.")
else:
    agg_drops = drops_df.groupby(['EVENT_ID', 'QUALITY_NAME', 'GRADE_NAME', 'SIZE_NAME'], as_index=False).agg({'weight_dec': 'sum'})
    agg_drops['TOTAL_WEIGHT_LBS'] = round(agg_drops['weight_dec'] * 0.00220462, 2)
    unique_sg_df = agg_drops[['EVENT_ID', 'QUALITY_NAME', 'GRADE_NAME', 'SIZE_NAME', 'TOTAL_WEIGHT_LBS']].copy()
    unique_sg_df['QUALITY_NAME'] = unique_sg_df['QUALITY_NAME'].str.upper().str.strip()
    unique_sg_df['INCLUDE'] = False # Will be set properly in session state initialization
    quality_order = ['GOOD', 'MODERATE', 'QBAD', 'BAD', 'BAD IB', 'BAD PRESSURE']
    unique_sg_df['QUALITY_SORT'] = pd.Categorical(unique_sg_df['QUALITY_NAME'], categories=quality_order, ordered=True)
    unique_sg_df['SIZE_NUM'] = pd.to_numeric(unique_sg_df['SIZE_NAME'], errors='coerce').fillna(0)
    unique_sg_df = unique_sg_df.sort_values(by=['QUALITY_SORT', 'GRADE_NAME', 'SIZE_NUM'], ascending=[True, False, False])
    sg_session_key = f"edited_size_grade_df{run_key_suffix}"
    if sg_session_key not in st.session_state:
        df_copy = unique_sg_df.copy()
        # Initialize INCLUDE column to False
        df_copy['INCLUDE'] = False
        # Check if we have saved data to load
        saved_sizer_packed = load_existing_sizer_packed(row['UNIQUE_RUN_KEY'])
        if not saved_sizer_packed.empty:
            # LOAD FROM SAVED DATA (PTRUN_SIZER_PACKED)
            saved_sizer_packed['QUALITY_NAME'] = saved_sizer_packed['QUALITY_NAME'].str.upper().str.strip()
            saved_sizer_packed['GRADE_NAME'] = saved_sizer_packed['GRADE_NAME'].str.upper().str.strip()
            saved_sizer_packed['SIZE_NAME'] = saved_sizer_packed['SIZE_NAME'].astype(str).str.strip()
            # Create lookup dictionary: (quality, grade, size) -> is_packed
            saved_lookup = {}
            for _, saved_row in saved_sizer_packed.iterrows():
                lookup_key = (
                    saved_row['QUALITY_NAME'],
                    saved_row['GRADE_NAME'],
                    saved_row['SIZE_NAME']
                )
                saved_lookup[lookup_key] = bool(saved_row['IS_PACKED'])
            # Apply saved selections to dataframe
            for idx, r in df_copy.iterrows():
                quality = str(r['QUALITY_NAME']).upper().strip()
                grade = str(r['GRADE_NAME']).upper().strip()
                size = str(r['SIZE_NAME']).strip()
                lookup_key = (quality, grade, size)
                if lookup_key in saved_lookup:
                    df_copy.at[idx, 'INCLUDE'] = saved_lookup[lookup_key]
                else:
                    # New item not in saved data - default to False
                    df_copy.at[idx, 'INCLUDE'] = False
        else:
            # LOAD FROM RAW DATA (apply auto-selection)
            # Define excluded grades (no auto-selection)
            excluded_grades = [
                'C', 'CULLS', 'DECAY', 'PEELER', 'RECYCLE', 'SLICE',
                'UDER COL', 'UNDER CO', 'UNDER COL', 'UNDER COLOR',
                'UNDER CO SLICE', 'UNDER COLOR SLICE'
            ]
            excluded_grades_upper = [g.upper().strip() for g in excluded_grades]
            # Define included qualities (whitelist - ONLY these get auto-selected)
            included_qualities = ['GOOD', 'MODERATE']
            included_qualities_upper = [q.upper().strip() for q in included_qualities]
            # Apply auto-selection rules
            for idx, r in df_copy.iterrows():
                size_num = r['SIZE_NUM']
                grade = str(r['GRADE_NAME']).upper().strip()
                quality = str(r['QUALITY_NAME']).upper().strip()
                # Auto-select ONLY if ALL conditions are met:
                # 1. Quality is in whitelist (GOOD or MODERATE)
                # 2. Grade is NOT in excluded list
                # 3. Size is between 56 and 198 (inclusive)
                should_select = (
                    quality in included_qualities_upper
                    and grade not in excluded_grades_upper
                    and 56 <= size_num <= 198
                )
                df_copy.at[idx, 'INCLUDE'] = should_select
        st.session_state[sg_session_key] = df_copy
    # Group by Quality and Grade
    grouped = st.session_state[sg_session_key].groupby(['QUALITY_NAME', 'GRADE_NAME'])
    # Session state for group expansions
    group_expand_key = f"group_expand{run_key_suffix}"
    if group_expand_key not in st.session_state:
        st.session_state[group_expand_key] = {}

    for (quality, grade), group_df in grouped:
        key = f"{quality}_{grade}"
        # Always expand groups
        st.session_state[group_expand_key][key] = True
    
    with st.form(key=f"sizer_overrides_form{run_key_suffix}"):
        for (quality, grade), group_df in grouped:
            group_key = f"{quality}_{grade}"
            expander_label = f"{quality} - {grade} (Total Weight: {group_df['TOTAL_WEIGHT_LBS'].sum():.2f} lbs)"
            with st.expander(expander_label, expanded=st.session_state[group_expand_key][group_key]):
                # List individual sizes
                st.markdown("**Individual Sizes (override if needed):**")
                for idx, r in group_df.iterrows():
                    row_cols = st.columns([1.5, 2, 2, 2, 1.2])
                    row_cols[0].write(r['EVENT_ID'])
                    row_cols[1].write(r['SIZE_NAME'])
                    row_cols[2].write(f"{r['TOTAL_WEIGHT_LBS']:.2f}")
                    ck = f"sg_cb_{idx}{run_key_suffix}"
                    row_cols[3].checkbox("Packed", value=st.session_state[sg_session_key].at[idx, 'INCLUDE'], key=ck, label_visibility="collapsed")
        submitted = st.form_submit_button("Calculate Packed Weight")
        if submitted:
            for (quality, grade), group_df in grouped:
                for idx, r in group_df.iterrows():
                    ck = f"sg_cb_{idx}{run_key_suffix}"
                    st.session_state[sg_session_key].at[idx, 'INCLUDE'] = st.session_state[ck]
            
            # Force a rerun to refresh the page with updated session state before calculations
            st.rerun()
    
    # Move these calculations OUTSIDE the form and submit block
    filtered_drops = agg_drops.merge(
        st.session_state[sg_session_key][['EVENT_ID', 'QUALITY_NAME', 'GRADE_NAME', 'SIZE_NAME', 'INCLUDE']],
        on=['EVENT_ID', 'QUALITY_NAME', 'GRADE_NAME', 'SIZE_NAME']
    )
    filtered_drops = filtered_drops[filtered_drops['INCLUDE']]
    raw_sizer_weight = round(filtered_drops['weight_dec'].sum() * 0.00220462, 2) if not filtered_drops.empty else 0.0
    initial_weight = round(agg_drops['weight_dec'].sum() * 0.00220462, 2)
    
    st.write(f"INITIAL WEIGHT: {initial_weight:.2f} lbs")
    st.write(f"PACKABLE WEIGHT: {raw_sizer_weight:.2f} lbs")
    
    # Update the verified sizer weight session state with the calculated value
    verified_sizer_key = f"verified_sizer_weight{run_key_suffix}"
    st.session_state[verified_sizer_key] = raw_sizer_weight
# Verified sizer weight input
default_sizer_weight = float(row["ACTUAL_SIZER_WEIGHT"]) if row["ACTUAL_SIZER_WEIGHT"] > 0 else raw_sizer_weight
verified_sizer_key = f"verified_sizer_weight{run_key_suffix}"
if verified_sizer_key not in st.session_state:
    st.session_state[verified_sizer_key] = default_sizer_weight
col1, col2 = st.columns(2)
with col1:
    st.markdown(f"<span style='background-color: yellow; color: black;'>Raw Recommendation: {raw_sizer_weight} lbs</span>", unsafe_allow_html=True)
with col2:
    verified_sizer_weight = st.number_input("Verified Sizer Weight", min_value=0.0, step=0.1, format="%.2f", key=verified_sizer_key, label_visibility="visible")
# Grower Contacted
st.markdown("**Grower Contacted?**")
default_contacted = row["GROWER_CONTACTED"] if pd.notna(row["GROWER_CONTACTED"]) else ""
verified_grower_contacted = st.text_input("Enter details (e.g., 'Yes, talked to John')", value=default_contacted, key=f"verified_grower_contacted{run_key_suffix}")
# Comments
st.markdown("**Comments**")
default_comments = row["COMMENTS"] if pd.notna(row["COMMENTS"]) else ""
verified_comments = st.text_area("Enter comments", value=default_comments, key=f"verified_comments{run_key_suffix}")
if st.button("💾 Save Sizer Data", type="primary"):
    with st.spinner("Saving..."):
        try:
            session.sql("BEGIN").collect()
            unique_run_key = row.get("UNIQUE_RUN_KEY", row.get("Unique Run Key"))
            bin_input_data = {
                'UNIQUE_RUN_KEY': unique_run_key,
                'ACTUAL_SIZER_WEIGHT': float(verified_sizer_weight), # Cast to float
                'SIZER_FOREIGN_KEYS': verified_sizer_foreign_keys,
                'GROWER_CONTACTED': verified_grower_contacted,
                'COMMENTS': verified_comments,
                'LAST_UPDATED_BY': session.sql("SELECT CURRENT_USER()").collect()[0][0],
                'LAST_UPDATED_AT': datetime.now()
            }
            upsert_single_row(session, "FROSTY.APP.PTRUN_BIN_INPUT", bin_input_data)
            session.sql("DELETE FROM FROSTY.APP.PTRUN_SIZER_DROP_SNAPSHOT WHERE UNIQUE_RUN_KEY = ?", params=[unique_run_key]).collect()
            if verified_sizer_foreign_keys:
                event_ids = [pair.split('/EventId:')[1] for pair in verified_sizer_foreign_keys.split(',') if '/EventId:' in pair]
                if event_ids:
                    event_ids = [int(eid) for eid in event_ids] # Cast to int
                    event_placeholders = ','.join(['?'] * len(event_ids))
                    session.sql(f"""
                        INSERT INTO FROSTY.APP.PTRUN_SIZER_DROP_SNAPSHOT (
                            UNIQUE_RUN_KEY, BATCH_ID, EVENT_ID, DROPSUMMARY_ID, SUMMARY_GROUP_ID, DROP_ID,
                            FRUIT_COUNT, WEIGHT_GRAMS, AVE_FRUIT_WEIGHT, GRADE_INDEX, GRADE_NAME, SIZE_INDEX,
                            SIZE_NAME, MIN_WEIGHT_GRAMS, PRODUCT_ID, PRODUCT_NAME, QUALITY_INDEX, QUALITY_NAME,
                            SUMMARY_GROUP_NAME, ORDER_ID, VARIETY_SIZING_MAP_ID, SIZING_MAP_NAME,
                            FRUIT_COUNT_NON_REJECT, WEIGHT_GRAMS_NON_REJECT, WEIGHT, COUNT, SIZE_NUM,
                            PACKOUT_GROUP, WEIGHT_DEC
                        )
                        SELECT
                            ?,
                            TO_NUMBER(h."BatchID"),
                            TO_NUMBER(ds."EventId"),
                            TO_NUMBER(ds."DropSummaryId"),
                            TO_NUMBER(ds."SummaryGroupId"),
                            TO_NUMBER(ds."DropId"),
                            TO_NUMBER(ds."FruitCount"),
                            TO_NUMBER(ds."WeightGrams"),
                            TO_NUMBER(ds."AveFruitWeight"),
                            TO_NUMBER(ds."GradeIndex"),
                            ds."GradeName",
                            TO_NUMBER(ds."SizeIndex"),
                            ds."SizeName",
                            TO_NUMBER(ds."MinWeightGrams"),
                            TO_NUMBER(ds."ProductId"),
                            ds."ProductName",
                            TO_NUMBER(ds."QualityIndex"),
                            ds."QualityName",
                            ds."SummaryGroupName",
                            TO_NUMBER(ds."OrderId"),
                            TO_NUMBER(ds."VarietySizingMapId"),
                            ds."SizingMapName",
                            TO_NUMBER(ds."FruitCountNonReject"),
                            TO_NUMBER(ds."WeightGramsNonReject"),
                            ds.WEIGHT,
                            ds."COUNT",
                            ds."SizeNum",
                            ds.PACKOUT_GROUP,
                            ds."weight_dec"
                        FROM FROSTY.STAGING.DQ_APPLE_SIZER_DROPSUMMARY_03 ds
                        JOIN FROSTY.STAGING.DQ_APPLE_SIZER_HEADER_VIEW_03 h ON ds."EventId" = h."EventId"
                        WHERE ds."EventId" IN ({event_placeholders})
                    """, params=[unique_run_key] + event_ids).collect()
            session.sql("DELETE FROM FROSTY.APP.PTRUN_SIZER_PACKED WHERE UNIQUE_RUN_KEY = ?", params=[unique_run_key]).collect()
            sg_session_key = f"edited_size_grade_df{run_key_suffix}"
            if sg_session_key in st.session_state:
                sizer_packed_df = st.session_state[sg_session_key]
                packed_values = []
                now = datetime.now()
                for _, r in sizer_packed_df.iterrows():
                    packed_values.append((
                        unique_run_key,
                        r['QUALITY_NAME'],
                        r['GRADE_NAME'],
                        r['SIZE_NAME'],
                        r['INCLUDE'],
                        now
                    ))
                if packed_values:
                    bulk_insert(session, "FROSTY.APP.PTRUN_SIZER_PACKED",
                        ['UNIQUE_RUN_KEY', 'QUALITY_NAME', 'GRADE_NAME', 'SIZE_NAME', 'IS_PACKED', 'LAST_UPDATED_AT'],
                        packed_values)
            session.sql("COMMIT").collect()
            # Refresh materialized table for PowerBI
            session.sql("DELETE FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER_MAT WHERE UNIQUE_RUN_KEY = ?", params=[unique_run_key]).collect()
            session.sql("""
                INSERT INTO FROSTY.APP.POWERBI_PRODUCTION_HEADER_MAT
                SELECT * FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER
                WHERE UNIQUE_RUN_KEY = ?
            """, params=[unique_run_key]).collect()
            session.sql("COMMIT").collect()
            
            st.success("✅ Sizer data saved!")
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
    if st.button("← Stamper"):
        st.switch_page("pages/2_Stamper.py")
with col2:
    if st.button("Next: Cull →"):
        st.switch_page("pages/4_Cull_Analysis.py")