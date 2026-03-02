import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session
session = get_active_session()

# TARE_LOOKUP
TARE_LOOKUP = {
    "1 - MCD WOOD": 110,
    "6 - MCD PLASTIC": 102,
    "E - ORG. PLASTIC": 102,
    "G - CFP WOOD": 112,
    "H - CFP PLASTIC": 90,
    "N - BARDIN 25 BOX": 105,
    "W - ORG. WOOD": 110,
    "Z - GWR PLASTIC": 91
}

def find_closest_type(tare_value):
    if tare_value in TARE_LOOKUP.values():
        return next(k for k, v in TARE_LOOKUP.items() if v == tare_value)
    else:
        return min(TARE_LOOKUP, key=lambda k: abs(TARE_LOOKUP[k] - tare_value))

@st.cache_data(ttl=60)
def load_ptrun():
    return session.sql("""
        SELECT
            "RUN_DATE",
            "UNIQUE_RUN_KEY",
            "RUN_NUMBER",
            "SHIFT",
            "PACK_LINE",
            "GROWER",
            "VARIETY_USER_CD",
            "POOL",
            COALESCE(BINS_SUBMITTED, 0) AS BINS_SUBMITTED,
            COALESCE(ACTUAL_NET, 0) AS ACTUAL_NET,
            COALESCE(ACTUAL_TARE, 0) AS ACTUAL_TARE,
            COALESCE(ACTUAL_GROSS, 0) AS ACTUAL_GROSS,
            FIRST_DUMP_TIME,
            LAST_DUMP_TIME,
            SUBMITTED_BY,
            BIN_TYPE,
            BLOCK_BREAKDOWN,
            TARE_BREAKDOWN,
            PICK_BREAKDOWN,
            COALESCE(ACTUAL_SIZER_WEIGHT, 0) AS ACTUAL_SIZER_WEIGHT,
            COALESCE(ACTUAL_STAMPER_WEIGHT, 0) AS ACTUAL_STAMPER_WEIGHT,
            SIZER_FOREIGN_KEYS,
            COALESCE(GROWER_CONTACTED, '') AS GROWER_CONTACTED,
            COALESCE(COMMENTS, '') AS COMMENTS
        FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER
        ORDER BY "RUN_DATE" DESC, "RUN_NUMBER" DESC
    """).to_pandas()

@st.cache_data(ttl=60)
def load_recommendation():
    return session.sql("""
        SELECT
            RUN_DATE,
            SHIFT,
            GROWER_CODE,
            REC_BIN_COUNT,
            REC_NET_WEIGHT,
            REC_TARE_WEIGHT,
            REC_AVG_TARE_PER_BIN,
            REC_GROSS_WEIGHT,
            REC_DUMPER_HMS,
            REC_FIRST_DUMP_TIME,
            REC_LAST_DUMP_TIME
        FROM FROSTY.APP.V_BINDUMP_AGG
    """).to_pandas()

@st.cache_data(ttl=60)
def load_tare_details():
    return session.sql("""
        SELECT
            RUN_DATE,
            SHIFT,
            GROWER_CODE,
            TARE,
            BIN_COUNT
        FROM FROSTY.APP.V_BINDUMP_TARE_DETAILS
    """).to_pandas()

@st.cache_data(ttl=60)
def load_sizer_headers(run_date):
    tomorrow = run_date + timedelta(days=1)
    return session.sql("""
        SELECT
            "GrowerCode" AS GROWER_CODE,
            "VarietyName" AS VARIETY_NAME,
            "StartTime" AS START_TIME,
            "EndTime" AS END_TIME,
            "BatchID" AS BATCH_ID,
            "EventId" AS EVENT_ID,
            "EventType" AS EVENT_TYPE,
            "SHIFT_NAME" AS SHIFT
        FROM FROSTY.STAGING.DQ_APPLE_SIZER_HEADER_VIEW_03
        WHERE
            DATE("StartTime") IN (?, ?)
        ORDER BY "StartTime" DESC
    """, params=[run_date, tomorrow]).to_pandas()

@st.cache_data(ttl=60)
def load_stamper_data(grower, run_date):
    tomorrow = run_date + timedelta(days=1)
    return session.sql("""
        SELECT "D", SHIFT1, DATE_SHIFT_KEY, growerid_gro, NAME_VAR, SUM(WEIGHT_PRD) AS "STAMPER_WEIGHT"
        FROM FROSTY.STAGING."03_AIRPORT_STAMPERS_SHIFT_01"
        WHERE growerid_gro = ? AND "D" IN (?, ?)
        GROUP BY 1,2,3,4,5
    """, params=[grower, run_date, tomorrow]).to_pandas()

@st.cache_data(ttl=60)
def load_potential_culls(run_date, grower):
    return session.sql("""
        SELECT DISTINCT "Id", DATE_YY_MM_DD, GROWER_NUMBER, VARIETY, PACKING_LINE_ORDER
        FROM FROSTY.APP.AGROFRESH_CULLS_UNPIVOTED
        WHERE DATE_YY_MM_DD = ? AND GROWER_NUMBER = ?
    """, params=[run_date, grower]).to_pandas()

@st.cache_data(ttl=60)
def load_cull_header(cull_id):
    return session.sql("""
        SELECT DISTINCT
            "Id",
            DATE_YY_MM_DD,
            PACKING_LINE_ORDER AS "SHIFT",
            GROWER_NUMBER,
            COMPUTECH_ABBR,
            "Bin Temp" AS BIN_TEMP,
            "Tub Temp" AS TUB_TEMP,
            "CMI Inspector" AS CMI_INSPECTOR
        FROM FROSTY.APP.AGROFRESH_CULLS_UNPIVOTED
        WHERE "Id" = ?
    """, params=[cull_id]).to_pandas()

@st.cache_data(ttl=60)
def load_cull_defects(cull_id):
    return session.sql("""
        SELECT DEFECT_TYPE, TRY_CAST("count" AS INTEGER) AS COUNT_INT
        FROM FROSTY.APP.AGROFRESH_CULLS_UNPIVOTED
        WHERE "Id" = ?
    """, params=[cull_id]).to_pandas()

@st.cache_data(ttl=60)
def load_existing_cull_header(unique_run_key):
    return session.sql("""
        SELECT *
        FROM FROSTY.APP.PTRUN_CULL_HEADER
        WHERE UNIQUE_RUN_KEY = ?
    """, params=[unique_run_key]).to_pandas()

@st.cache_data(ttl=60)
def load_existing_cull_defects(unique_run_key):
    return session.sql("""
        SELECT DEFECT_TYPE, COUNT_INT
        FROM FROSTY.APP.PTRUN_CULL_DEFECT
        WHERE UNIQUE_RUN_KEY = ?
    """, params=[unique_run_key]).to_pandas()

@st.cache_data(ttl=60)
def load_potential_pressures(run_date, grower):
    next_day = run_date + timedelta(days=1)
    return session.sql("""
        SELECT MAX("Id") AS ID, DATE_YY_MM_DD, GROWER_NUMBER, VARIETY, SHIFT, MAX("Inspector") AS INSPECTOR
        FROM FROSTY.APP.PTRUN_AGROFRESH_PRESSURE_VIEW_PBIX
        WHERE GROWER_NUMBER = ?
        AND (
            DATE_YY_MM_DD = ?
            OR (
                DATE_YY_MM_DD = ?
                AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', "Date (UTC)")::TIME < '04:00:00'
            )
        )
        GROUP BY DATE_YY_MM_DD, GROWER_NUMBER, VARIETY, SHIFT
    """, params=[grower, run_date, next_day]).to_pandas()

@st.cache_data(ttl=60)
def load_pressure_header(pressure_id):
    return session.sql("""
        SELECT DISTINCT
            "Id" AS ID,
            DATE_YY_MM_DD,
            SHIFT AS "SHIFT",
            GROWER_NUMBER,
            COMPUTECH_ABBR,
            "Inspector" AS CMI_INSPECTOR
        FROM FROSTY.APP.PTRUN_AGROFRESH_PRESSURE_VIEW_PBIX
        WHERE "Id" = ?
    """, params=[pressure_id]).to_pandas()

@st.cache_data(ttl=60)
def load_pressure_details(pressure_id):
    return session.sql("""
        SELECT TRY_CAST("Fruit Size " AS INTEGER) AS FRUIT_SIZE_INT, TRY_CAST("Pressure" AS FLOAT) AS PRESSURE_DEC
        FROM FROSTY.APP.PTRUN_AGROFRESH_PRESSURE_VIEW_PBIX
        WHERE DATE_YY_MM_DD = (SELECT DATE_YY_MM_DD FROM FROSTY.APP.PTRUN_AGROFRESH_PRESSURE_VIEW_PBIX WHERE "Id" = ?)
        AND GROWER_NUMBER = (SELECT GROWER_NUMBER FROM FROSTY.APP.PTRUN_AGROFRESH_PRESSURE_VIEW_PBIX WHERE "Id" = ?)
        AND SHIFT = (SELECT SHIFT FROM FROSTY.APP.PTRUN_AGROFRESH_PRESSURE_VIEW_PBIX WHERE "Id" = ?)
        AND VARIETY = (SELECT VARIETY FROM FROSTY.APP.PTRUN_AGROFRESH_PRESSURE_VIEW_PBIX WHERE "Id" = ?)
        AND "Inspector" = (SELECT "Inspector" FROM FROSTY.APP.PTRUN_AGROFRESH_PRESSURE_VIEW_PBIX WHERE "Id" = ?)
    """, params=[pressure_id] * 5).to_pandas()

@st.cache_data(ttl=60)
def load_existing_pressure_header(unique_run_key):
    return session.sql("""
        SELECT *
        FROM FROSTY.APP.PTRUN_PRESSURE_HEADER
        WHERE UNIQUE_RUN_KEY = ?
    """, params=[unique_run_key]).to_pandas()

@st.cache_data(ttl=60)
def load_existing_pressure_details(unique_run_key):
    return session.sql("""
        SELECT FRUIT_SIZE_INT, PRESSURE_DEC
        FROM FROSTY.APP.PTRUN_PRESSURE_DETAIL
        WHERE UNIQUE_RUN_KEY = ?
    """, params=[unique_run_key]).to_pandas()

@st.cache_data(ttl=60)
def load_sizer_drops(event_ids):
    if not event_ids:
        return pd.DataFrame()
    event_placeholders = ','.join(['?' ] * len(event_ids))
    return session.sql(f"""
        SELECT
            "EventId" AS EVENT_ID,
            TRIM("QualityName") AS QUALITY_NAME,
            "SizeName" AS SIZE_NAME,
            "GradeName" AS GRADE_NAME,
            "weight_dec"
        FROM FROSTY.STAGING.DQ_APPLE_SIZER_DROPSUMMARY_03
        WHERE "EventId" IN ({event_placeholders})
    """, params=event_ids).to_pandas()

def load_existing_sizer_packed(unique_run_key):
    return session.sql("""
        SELECT QUALITY_NAME, GRADE_NAME, SIZE_NAME, IS_PACKED
        FROM FROSTY.APP.PTRUN_SIZER_PACKED
        WHERE UNIQUE_RUN_KEY = ?
    """, params=[unique_run_key]).to_pandas()

def upsert_single_row(session, table_name, data_dict, key_column='UNIQUE_RUN_KEY'):
    if not data_dict:
        return 0
    key_value = data_dict.get(key_column)
    if not key_value or len(str(key_value).strip()) == 0:
        raise ValueError("Key value is empty")
    merge_sql = f"""
        MERGE INTO {table_name} t
        USING (SELECT ? AS {key_column}) s
        ON t.{key_column} = s.{key_column}
        WHEN MATCHED THEN UPDATE SET
    """
    update_sets = []
    params = [key_value]
    for col, val in data_dict.items():
        if col != key_column:
            update_sets.append(f"t.{col} = ?")
            params.append(val)
    if not update_sets:
        return 0
    merge_sql += ", ".join(update_sets)
    merge_sql += """
        WHEN NOT MATCHED THEN INSERT (
    """
    insert_columns = [key_column] + [col for col in data_dict if col != key_column]
    merge_sql += ", ".join(insert_columns)
    merge_sql += """
        ) VALUES (
    """
    merge_sql += ", ".join(["?" for _ in insert_columns])
    merge_sql += ")"
    params += [key_value] + [data_dict[col] for col in data_dict if col != key_column]
    session.sql(merge_sql, params=params).collect()
    return 1

def bulk_insert(session, table_name, columns, values_list):
    if not values_list:
        return 0
    placeholders = ', '.join(['(' + ', '.join(['?' for _ in columns]) + ')' for _ in values_list])
    flat_values = [val for row in values_list for val in row]
    columns_str = ', '.join(columns)
    session.sql(f"INSERT INTO {table_name} ({columns_str}) VALUES {placeholders}", params=flat_values).collect()
    return len(values_list)

renamed_columns = {
    "RUN_DATE": "Run Date",
    "PACK_LINE": "Packline",
    "SHIFT": "Shift",
    "RUN_NUMBER": "Run",
    "GROWER": "Grower",
    "VARIETY_USER_CD": "Variety",
    "POOL": "Pool",
    "REC_BIN_COUNT": "Raw Bins",
    "BINS_SUBMITTED": "Actual Bins",
    "ACTUAL_NET": "Actual Net",
    "ACTUAL_TARE": "Actual Tare",
    "ACTUAL_GROSS": "Actual Gross",
    "ACTUAL_SIZER_WEIGHT": "Actual Sizer Weight",
    "ACTUAL_STAMPER_WEIGHT": "Actual Stamper Weight",
    "BIN_TYPE": "Bin Type",
    "BLOCK_BREAKDOWN": "Block Breakdown",
    "TARE_BREAKDOWN": "Tare Breakdown",
    "PICK_BREAKDOWN": "Pick Breakdown",
    "FIRST_DUMP_TIME": "First Dump Time",
    "LAST_DUMP_TIME": "Last Dump Time",
    "SUBMITTED_BY": "Submitted By",
    "COMMENTS": "Comments"
}

def highlight_unsubmitted(row):
    if row["Actual Bins"] == 0:
        return ['background-color: #fff3cd; color: black;' for _ in row]
    return [''] * len(row)