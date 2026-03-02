# pages/1_Bins_Breakdown.py
import streamlit as st
import pandas as pd
from datetime import datetime
from utils import *

st.set_page_config(page_title="Bins & Breakdown", layout="wide")

if 'selected_run_key' not in st.session_state:
    st.error("⚠️ No run selected")
    if st.button("← Back to Main"):
        st.switch_page("streamlit_app.py")
    st.stop()

row = pd.Series(st.session_state.selected_run_data)
run_key_suffix = f"*{st.session_state.selected_run_key}"

tare_details_df = load_tare_details()  # Load it here
# Create JOIN_KEY, matching main.py
tare_details_df["JOIN_KEY"] = (
    tare_details_df["RUN_DATE"].astype(str) + "-" +
    tare_details_df["SHIFT"].astype(str) + "-" +
    tare_details_df["GROWER_CODE"].astype(str).str.zfill(4)
)

st.title("📦 Bins & Breakdown")
st.subheader(f"Run {row['RUN_NUMBER']} — {row['VARIETY_USER_CD']} — Grower {row['GROWER']} — {row['PACK_LINE']} — Pool {row['POOL']} — Date {row['RUN_DATE']}")

st.info("**Raw Bins** = total dumped for this grower on this shift (across all lines). Use as starting point for verification.")
st.warning("All recommended data is derived from raw sources and may be approximate. Please verify and confirm values.")

st.markdown("#### **Submit Verified Values**")
default_bins = int(row["BINS_SUBMITTED"]) if row["BINS_SUBMITTED"] > 0 else int(row["REC_BIN_COUNT"])
default_net = int(row["ACTUAL_NET"]) if row["ACTUAL_NET"] > 0 else int(row["REC_NET_WEIGHT"])
default_gross = int(row["ACTUAL_GROSS"]) if row["ACTUAL_GROSS"] > 0 else int(row["REC_GROSS_WEIGHT"])
default_bin_type = row.get("BIN_TYPE")
if default_bin_type is None or default_bin_type not in TARE_LOOKUP:
    default_bin_type = list(TARE_LOOKUP.keys())[0]

col1, col2 = st.columns(2)
with col1:
    st.markdown(f"<span style='background-color: yellow; color: black;'>Raw Bins Recommendation: {int(row['REC_BIN_COUNT'])} bins</span>", unsafe_allow_html=True)
with col2:
    verified_bins = st.number_input("Verified Bins Submitted (Actual Bins)", value=default_bins, min_value=0, step=1, key=f"verified_bins{run_key_suffix}", label_visibility="collapsed")

col1, col2 = st.columns(2)
with col1:
    st.markdown(f"<span style='background-color: yellow; color: black;'>Raw Recommendation: {int(row['REC_GROSS_WEIGHT'])} lbs</span>", unsafe_allow_html=True)
with col2:
    verified_gross = st.number_input("Verified Actual Gross", value=default_gross, min_value=0, step=1, key=f"verified_gross{run_key_suffix}", label_visibility="collapsed")

raw_tare_groups = tare_details_df[tare_details_df["JOIN_KEY"] == row["JOIN_KEY"]].to_dict('records')
is_possible_split = len(raw_tare_groups) > 1
default_tare_per_bin = 0
if row["ACTUAL_TARE"] > 0 and default_bins > 0:
    default_tare_per_bin = round(row["ACTUAL_TARE"] / default_bins)
elif row["REC_AVG_TARE_PER_BIN"] > 0:
    default_tare_per_bin = round(row["REC_AVG_TARE_PER_BIN"])
inferred_bin_type = None
if len(raw_tare_groups) == 1:
    inferred_bin_type = find_closest_type(raw_tare_groups[0]['TARE'])
elif default_tare_per_bin > 0:
    inferred_bin_type = find_closest_type(default_tare_per_bin)
if inferred_bin_type and (default_bin_type not in TARE_LOOKUP or default_bin_type == list(TARE_LOOKUP.keys())[0]):
    default_bin_type = inferred_bin_type

display_to_key = {f"{k} ({v} lbs)": k for k, v in TARE_LOOKUP.items()}
formatted_options = list(display_to_key.keys())

col1, col2 = st.columns(2)
with col1:
    st.markdown(f"<span style='background-color: yellow; color: black;'>Raw Recommendation: {round(row['REC_AVG_TARE_PER_BIN'], 2)} lbs/bin (total {int(row['REC_TARE_WEIGHT'])} lbs).</span>", unsafe_allow_html=True)
split_tare = st.checkbox("Split by bin type/tare?", value=is_possible_split or (row.get("TARE_BREAKDOWN") and ' ' in row["TARE_BREAKDOWN"]), key=f"split_tare{run_key_suffix}")

verified_total_tare = 0
verified_bin_type = None
verified_tare_breakdown = None
math_str = ""
current_bin_sum = 0
if not split_tare:
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"<span style='background-color: yellow; color: black;'>Recommended Bin Type: {default_bin_type} ({TARE_LOOKUP.get(default_bin_type, 0)} lbs)</span>", unsafe_allow_html=True)
    with col2:
        default_display = f"{default_bin_type} ({TARE_LOOKUP.get(default_bin_type, 0)} lbs)"
        default_index = formatted_options.index(default_display) if default_display in formatted_options else 0
        selected_display = st.selectbox("Verified Bin Type", options=formatted_options, index=default_index, key=f"verified_bin_type{run_key_suffix}", label_visibility="collapsed")
        verified_bin_type = display_to_key[selected_display]
        verified_tare_per_bin = TARE_LOOKUP[verified_bin_type]
        verified_total_tare = verified_tare_per_bin * verified_bins
        verified_tare_breakdown = f"{verified_bin_type}={verified_bins}@{verified_tare_per_bin}"
        math_str = f" ({verified_bins} bins x {verified_tare_per_bin} lbs/bin)"
        current_bin_sum = verified_bins
else:
    tare_group_key = f"tare_groups{run_key_suffix}"
    if tare_group_key not in st.session_state:
        st.session_state[tare_group_key] = []
    tare_groups = st.session_state[tare_group_key]
    if not tare_groups:
        default_tare_breakdown = row.get("TARE_BREAKDOWN", "")
        if default_tare_breakdown:
            parts = default_tare_breakdown.split(', ') if ', ' in default_tare_breakdown else default_tare_breakdown.split(' ')
            for part in parts:
                if '=' in part and '@' in part:
                    type_count, tare_str = part.split('@')
                    type_name, count_str = type_count.split('=')
                    try:
                        count = int(count_str.strip())
                        tare = int(tare_str.strip())
                        matched_type = type_name if type_name in TARE_LOOKUP and tare == TARE_LOOKUP[type_name] else find_closest_type(tare)
                        tare_groups.append({'type': matched_type, 'count': count, 'tare': TARE_LOOKUP[matched_type]})
                    except ValueError:
                        pass
        elif raw_tare_groups:
            for group in raw_tare_groups:
                matched_type = find_closest_type(group['TARE'])
                tare_groups.append({'type': matched_type, 'count': group['BIN_COUNT'], 'tare': TARE_LOOKUP[matched_type]})
        st.session_state[tare_group_key] = tare_groups
    current_tare_sum = 0
    for i in range(len(st.session_state[tare_group_key])):
        col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 1])
        with col1:
            def update_tare(i=i):
                new_display = st.session_state[f"tare_type_{i}{run_key_suffix}"]
                new_type = display_to_key[new_display]
                new_tare_value = TARE_LOOKUP[new_type]
                st.session_state[tare_group_key][i]['type'] = new_type
                st.session_state[tare_group_key][i]['tare'] = new_tare_value
                st.rerun()
            try:
                default_display = f"{st.session_state[tare_group_key][i]['type']} ({st.session_state[tare_group_key][i]['tare']} lbs)"
                default_index = formatted_options.index(default_display)
            except ValueError:
                default_index = 0
                default_key = list(TARE_LOOKUP.keys())[0]
                st.session_state[tare_group_key][i]['type'] = default_key
                st.session_state[tare_group_key][i]['tare'] = TARE_LOOKUP[default_key]
            new_display = st.selectbox(f"Type {i+1}", options=formatted_options, index=default_index, key=f"tare_type_{i}{run_key_suffix}", on_change=update_tare)
            new_type = display_to_key[new_display]
            st.session_state[tare_group_key][i]['type'] = new_type
        with col2:
            new_count = st.number_input(f"Count {i+1}", value=st.session_state[tare_group_key][i]['count'], min_value=0, step=1, key=f"tare_count_{i}{run_key_suffix}")
            st.session_state[tare_group_key][i]['count'] = new_count
            current_bin_sum += new_count
        with col3:
            new_tare = TARE_LOOKUP[new_type]
            st.session_state[tare_group_key][i]['tare'] = new_tare
            st.write(f"Tare Per Bin: {new_tare}")
            current_tare_sum += new_count * new_tare
        with col4:
            st.write(f"Subtotal: {new_count * new_tare} lbs")
        with col5:
            if st.button("Remove", key=f"tare_remove_{i}{run_key_suffix}"):
                del st.session_state[tare_group_key][i]
                st.rerun()
    if st.button("Add Bin Type Group"):
        first_type = list(TARE_LOOKUP.keys())[0]
        st.session_state[tare_group_key].append({'type': first_type, 'count': 0, 'tare': TARE_LOOKUP[first_type]})
        st.rerun()
    verified_total_tare = current_tare_sum
    verified_tare_breakdown = ', '.join(f"{group['type']}={group['count']}@{group['tare']}" for group in st.session_state[tare_group_key] if group['count'] > 0)
    if st.session_state[tare_group_key]:
        bin_diff = verified_bins - current_bin_sum
        if bin_diff > 0:
            st.warning(f"Group bin counts sum to {current_bin_sum}; need {bin_diff} more to match verified bins ({verified_bins}).")
        elif bin_diff < 0:
            st.warning(f"Group bin counts sum to {current_bin_sum}; over by {-bin_diff} compared to verified bins ({verified_bins}).")
        else:
            st.success(f"Group bin counts match verified bins ({verified_bins}).")

st.write(f"Verified Total Tare: {verified_total_tare} lbs{math_str}")

st.markdown("**Calculated Dumper Runtime from Raw Data**")
st.write(row["REC_DUMPER_HMS"])

st.markdown("**Verified Start Time**")
col1, col2 = st.columns(2)
with col1:
    if pd.notna(row["REC_FIRST_DUMP_TIME"]):
        st.markdown(f"<span style='background-color: yellow; color: black;'>Recommendation: {row['REC_FIRST_DUMP_TIME'].time().strftime('%H:%M')}</span>", unsafe_allow_html=True)
default_start_time = None
if pd.notna(row["FIRST_DUMP_TIME"]):
    default_start_time = row["FIRST_DUMP_TIME"].time()
elif pd.notna(row["REC_FIRST_DUMP_TIME"]):
    default_start_time = row["REC_FIRST_DUMP_TIME"].time()
verified_time_start = st.time_input("Start Time", value=default_start_time or datetime.min.time(), key=f"verified_time_start{run_key_suffix}")

st.markdown("**Verified End Time**")
col1, col2 = st.columns(2)
with col1:
    if pd.notna(row["REC_LAST_DUMP_TIME"]):
        st.markdown(f"<span style='background-color: yellow; color: black;'>Recommendation: {row['REC_LAST_DUMP_TIME'].time().strftime('%H:%M')}</span>", unsafe_allow_html=True)
default_end_time = None
if pd.notna(row["LAST_DUMP_TIME"]):
    default_end_time = row["LAST_DUMP_TIME"].time()
elif pd.notna(row["REC_LAST_DUMP_TIME"]):
    default_end_time = row["REC_LAST_DUMP_TIME"].time()
verified_time_end = st.time_input("End Time", value=default_end_time or datetime.min.time(), key=f"verified_time_end{run_key_suffix}")

verified_start_dt = datetime.combine(row.get("RUN_DATE", row.get("Run Date", datetime.today().date())), verified_time_start) if verified_time_start else None
verified_end_dt = datetime.combine(row.get("RUN_DATE", row.get("Run Date", datetime.today().date())), verified_time_end) if verified_time_end else None

# Block breakdown
st.markdown("**Block Breakdown**")
block_key = f"blocks{run_key_suffix}"
if block_key not in st.session_state:
    st.session_state[block_key] = []
blocks = st.session_state[block_key]
if not blocks:
    default_breakdown = row.get("BLOCK_BREAKDOWN", "")
    if default_breakdown and default_breakdown != "BLOCK: N/A":
        if default_breakdown.startswith("BLOCK: "):
            inner = default_breakdown[7:].strip()
            parts = inner.split(', ')
            for part in parts:
                if '=' in part:
                    block_id, bins_str = part.split('=')
                    block_id = block_id.lstrip('#').strip()
                    bins_str = bins_str.replace(' Bins', '').strip()
                    try:
                        bins = int(bins_str)
                        blocks.append({'id': block_id, 'bins': bins})
                    except ValueError:
                        pass
        else:
            parts = default_breakdown.split(', ') if ', ' in default_breakdown else default_breakdown.split(' ')
            for part in parts:
                if '=' in part:
                    name, bins_str = part.split('=')
                    name = name.strip()
                    if name.startswith('BLOCK #'):
                        block_id = name[7:].strip()
                    else:
                        block_id = name
                    bins_str = bins_str.replace(' Bins', '').strip()
                    try:
                        bins = int(bins_str)
                        blocks.append({'id': block_id, 'bins': bins})
                    except ValueError:
                        pass
    st.session_state[block_key] = blocks
for block in st.session_state[block_key]:
    if 'id' not in block:
        if 'name' in block:
            name = block.pop('name')
            if name.startswith('BLOCK #'):
                block['id'] = name[7:].strip()
            else:
                block['id'] = name.strip()
        else:
            block['id'] = 'N/A'
current_sum = 0
for i in range(len(st.session_state[block_key])):
    col1, col2, col3, col4 = st.columns([1.5, 3, 2, 2])
    with col1:
        st.write("BLOCK")
    with col2:
        new_id = st.text_input(f"ID {i+1}", value=st.session_state[block_key][i]['id'], key=f"block_id_{i}{run_key_suffix}", label_visibility="collapsed")
        st.session_state[block_key][i]['id'] = new_id
    with col3:
        new_bins = st.number_input(f"Bins {i+1}", value=st.session_state[block_key][i]['bins'], min_value=0, step=1, key=f"block_bins_{i}{run_key_suffix}", label_visibility="collapsed")
        st.session_state[block_key][i]['bins'] = new_bins
        current_sum += new_bins
    with col4:
        if st.button("Remove", key=f"remove_{i}{run_key_suffix}"):
            del st.session_state[block_key][i]
            st.rerun()
if st.button("Add Block"):
    st.session_state[block_key].append({'id': 'N/A', 'bins': 0})
    st.rerun()
if st.session_state[block_key]:
    diff = verified_bins - current_sum
    st.write(f"Current block sum: {current_sum} bins")
    if diff > 0:
        st.warning(f"You need {diff} more bins to match verified bins ({verified_bins}).")
    elif diff < 0:
        st.warning(f"You are over by {-diff} bins compared to verified bins ({verified_bins}).")
    else:
        st.success(f"Block sums match verified bins ({verified_bins}).")
verified_block_breakdown = "BLOCK: N/A"
if st.session_state[block_key]:
    inner = ', '.join(f"#{block['id']}={block['bins']} Bins" for block in st.session_state[block_key] if block['bins'] > 0)
    if inner:
        verified_block_breakdown = f"BLOCK: {inner}"

# Pick breakdown
st.markdown("**Pick Breakdown**")
pick_key = f"picks{run_key_suffix}"
if pick_key not in st.session_state:
    st.session_state[pick_key] = []
picks = st.session_state[pick_key]
if not picks:
    default_pick_breakdown = row.get("PICK_BREAKDOWN", "")
    if default_pick_breakdown and default_pick_breakdown != "PICK: N/A":
        if default_pick_breakdown.startswith("PICK: "):
            inner = default_pick_breakdown[6:].strip()
            parts = inner.split(', ')
        else:
            parts = default_pick_breakdown.split(', ') if ', ' in default_pick_breakdown else default_pick_breakdown.split(' ')
        for part in parts:
            if '=' in part:
                pick_id, bins_str = part.split('=')
                bins_str = bins_str.replace(' Bins', '').strip()
                try:
                    bins = int(bins_str)
                    picks.append({'id': pick_id.strip(), 'bins': bins})
                except ValueError:
                    pass
    st.session_state[pick_key] = picks
current_pick_sum = 0
for i in range(len(st.session_state[pick_key])):
    col1, col2, col3, col4 = st.columns([1.5, 3, 2, 2])
    with col1:
        st.write("PICK")
    with col2:
        new_id = st.text_input(f"Type {i+1}", value=st.session_state[pick_key][i]['id'], key=f"pick_id_{i}{run_key_suffix}", label_visibility="collapsed")
        st.session_state[pick_key][i]['id'] = new_id
    with col3:
        new_bins = st.number_input(f"Bins {i+1}", value=st.session_state[pick_key][i]['bins'], min_value=0, step=1, key=f"pick_bins_{i}{run_key_suffix}", label_visibility="collapsed")
        st.session_state[pick_key][i]['bins'] = new_bins
        current_pick_sum += new_bins
    with col4:
        if st.button("Remove", key=f"pick_remove_{i}{run_key_suffix}"):
            del st.session_state[pick_key][i]
            st.rerun()
if st.button("Add Pick Order"):
    st.session_state[pick_key].append({'id': 'N/A', 'bins': 0})
    st.rerun()
if st.session_state[pick_key]:
    pick_diff = verified_bins - current_pick_sum
    st.write(f"Current pick sum: {current_pick_sum} bins")
    if pick_diff > 0:
        st.warning(f"You need {pick_diff} more bins to match verified bins ({verified_bins}).")
    elif pick_diff < 0:
        st.warning(f"You are over by {-pick_diff} bins compared to verified bins ({verified_bins}).")
    else:
        st.success(f"Pick sums match verified bins ({verified_bins}).")
verified_pick_breakdown = "PICK: N/A"
if st.session_state[pick_key]:
    inner = ', '.join(f"{pick['id']}={pick['bins']} Bins" for pick in st.session_state[pick_key] if pick['bins'] > 0)
    if inner:
        verified_pick_breakdown = f"PICK: {inner}"

verified_net = verified_gross - verified_total_tare if verified_gross >= verified_total_tare else 0
is_block_valid = len(st.session_state.get(block_key, [])) == 0 or current_sum == verified_bins
is_tare_valid = not split_tare or current_bin_sum == verified_bins
is_pick_valid = len(st.session_state.get(pick_key, [])) == 0 or current_pick_sum == verified_bins

# Show success message if just saved (at bottom)
if st.session_state.get('save_success'):
    st.success("✅ Bins data saved successfully!")
    del st.session_state['save_success']

if st.button("💾 Save Bins Data", type="primary", disabled=not (is_block_valid and is_tare_valid and is_pick_valid)):
    with st.spinner("Saving..."):
        try:
            session.sql("BEGIN").collect()
            unique_run_key = row.get("UNIQUE_RUN_KEY", row.get("Unique Run Key"))
            bin_input_data = {
                'UNIQUE_RUN_KEY': unique_run_key,
                'BINS_SUBMITTED': verified_bins,
                'ACTUAL_NET': verified_net,
                'ACTUAL_TARE': verified_total_tare,
                'ACTUAL_GROSS': verified_gross,
                'FIRST_DUMP_TIME': verified_start_dt,
                'LAST_DUMP_TIME': verified_end_dt,
                'BIN_TYPE': verified_bin_type if not split_tare else None,
                'BLOCK_BREAKDOWN': verified_block_breakdown,
                'TARE_BREAKDOWN': verified_tare_breakdown,
                'PICK_BREAKDOWN': verified_pick_breakdown,
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
    if st.button("← Back to Main"):
        st.switch_page("streamlit_app.py")
with col2:
    if st.button("Next: Stamper →"):
        st.switch_page("pages/2_Stamper.py")