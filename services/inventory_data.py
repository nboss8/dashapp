"""
Pallet Inventory - data fetching for pivot, SKU detail, changes today, filter options.
All queries use DT_INV_ON_HAND_SKU_GRAIN (via inv_on_hand_pivot) or inv_changes_today.
"""
import os
import time
import logging
from datetime import datetime

import pandas as pd
from utils.table_helpers import _normalize_df_columns

from services.snowflake_service import query
from services.cache_manager import register_report

DBT_SCHEMA = os.getenv("SNOWFLAKE_DATABASE", "FROSTY") + "." + os.getenv("DBT_SCHEMA", "DBT_DEV_DBT_DEV")
logger = logging.getLogger(__name__)

# Filter options cache: 60s TTL
_FILTER_OPTIONS_CACHE = None
_FILTER_OPTIONS_TS = 0
_FILTER_CACHE_TTL = 60

INV_COL_MAP = {
    "group_category": ["GROUP_CATEGORY"],
    "final_stage_status": ["FINAL_STAGE_STATUS"],
    "variety_abbr": ["VARIETY_ABBR"],
    "sku": ["SKU"],
    "week_bucket": ["WEEK_BUCKET"],
    "week_bucket_num": ["WEEK_BUCKET_NUM"],
    "pack_abbr": ["PACK_ABBR"],
    "grade_abbr": ["GRADE_ABBR"],
    "size_abbr": ["SIZE_ABBR"],
    "pool": ["POOL"],
    "process_code": ['"Process Code"'],
    "cartons": ["CARTONS"],
    "eq_on_hand": ["EQ_ON_HAND"],
    "cartons_avail": ["CARTONS_AVAIL"],
    "change_type": ["change_type", "CHANGE_TYPE"],
}

_INV_SKU_MAX_ROWS = 2000


def _build_where(filters):
    """Build WHERE clause and params from filter dict. None/empty = no filter for that dim."""
    if not filters:
        return "", []
    wheres = []
    params = []
    mapping = [
        ("group_category", "GROUP_CATEGORY"),
        ("variety", "VARIETY_ABBR"),
        ("pack", "PACK_ABBR"),
        ("grade", "GRADE_ABBR"),
        ("size", "SIZE_ABBR"),
        ("pool", "POOL"),
        ("process_code", '"Process Code"'),
        ("final_stage_status", "FINAL_STAGE_STATUS"),
        # Week age filter (used when clicking pivot cells)
        ("week_bucket", "WEEK_BUCKET"),
    ]
    for key, col in mapping:
        v = filters.get(key)
        if v and str(v).strip():
            wheres.append(f"{col} = %s")
            params.append(str(v).strip())
    return " AND ".join(wheres) if wheres else "", params


def get_pivot_data(filters=None, use_eq=False):
    """
    Variety x week aggregated data for pivot.
    use_eq: if True, primary measure is EQ_ON_HAND; else CARTONS.
    """
    filters = filters or {}
    where_clause, params = _build_where(filters)
    where_sql = f"WHERE {where_clause}" if where_clause else ""
    measure = "EQ_ON_HAND" if use_eq else "CARTONS"
    sql = f"""
    SELECT VARIETY_ABBR, WEEK_BUCKET, WEEK_BUCKET_NUM,
           SUM(CARTONS) AS CARTONS, SUM(EQ_ON_HAND) AS EQ_ON_HAND
    FROM {DBT_SCHEMA}.inv_on_hand_pivot
    {where_sql}
    GROUP BY VARIETY_ABBR, WEEK_BUCKET, WEEK_BUCKET_NUM
    ORDER BY VARIETY_ABBR, WEEK_BUCKET_NUM NULLS LAST, WEEK_BUCKET
    """
    logger.info("[INV] get_pivot_data filters=%s where=%s params=%s", filters, where_clause, params)
    df = query(sql, params=params)
    df = _normalize_df_columns(df, INV_COL_MAP) if df is not None else pd.DataFrame()
    return df


def get_sku_detail(filters=None, page=1, page_size=50, use_eq=False):
    """
    Paginated SKU list — one row per SKU with totals. use_eq: if True, primary measure is EQ_ON_HAND.
    Returns df with SKU, VARIETY_ABBR, CARTONS, EQ_ON_HAND.
    """
    filters = filters or {}
    where_clause, params = _build_where(filters)
    where_sql = f"WHERE {where_clause}" if where_clause else ""
    offset = (page - 1) * page_size if page_size else 0
    sql = f"""
    SELECT SKU, VARIETY_ABBR, SUM(CARTONS) AS CARTONS, SUM(EQ_ON_HAND) AS EQ_ON_HAND
    FROM {DBT_SCHEMA}.inv_on_hand_pivot
    {where_sql}
    GROUP BY SKU, VARIETY_ABBR
    ORDER BY VARIETY_ABBR, SKU"""
    if page_size is not None:
        sql += """
    LIMIT %s OFFSET %s"""
        params = list(params) + [page_size, offset]
    else:
        params = list(params)
    logger.info("[INV] get_sku_detail filters=%s page=%s page_size=%s where=%s params=%s", filters, page, page_size, where_clause, params)
    df = query(sql, params=params)
    return _normalize_df_columns(df, INV_COL_MAP) if df is not None else pd.DataFrame()


def get_sku_total_count(filters=None):
    """Total SKU row count for pagination."""
    filters = filters or {}
    where_clause, params = _build_where(filters)
    where_sql = f"WHERE {where_clause}" if where_clause else ""
    sql = f"""
    SELECT COUNT(*) AS cnt
    FROM (
      SELECT 1
      FROM {DBT_SCHEMA}.inv_on_hand_pivot
      {where_sql}
      GROUP BY SKU, VARIETY_ABBR
    ) t
    """
    logger.info("[INV] get_sku_total_count filters=%s where=%s params=%s", filters, where_clause, params)
    df = query(sql, params=params)
    df = _normalize_df_columns(df, INV_COL_MAP) if df is not None else pd.DataFrame()
    if df.empty:
        return 0
    try:
        if "cnt" in df.columns:
            return int(df["cnt"].iloc[0])
        return int(df.iloc[0,0])
    except:
        return 0


def get_changes_today(filters=None):
    """Packed, Shipped, Staged totals for today. Respects filters (Option A).
    Returns df with change_type, cartons, eq_on_hand.
    """
    filters = filters or {}
    # Week filters apply only to on-hand; inv_changes_today does not have week buckets
    if "week_bucket" in filters:
        filters = {k: v for k, v in filters.items() if k != "week_bucket"}
    where_clause, params = _build_where(filters)
    where_sql = f"AND {where_clause}" if where_clause else ""
    sql = (
        "SELECT change_type, SUM(CARTONS) AS cartons, "
        "SUM(EQ_ON_HAND) AS eq_on_hand "
        f"FROM {DBT_SCHEMA}.inv_changes_today "
        "WHERE 1=1 "
        f"{where_sql} "
        "GROUP BY change_type "
        "ORDER BY change_type"
    )
    logger.info("[INV] get_changes_today filters=%s where=%s params=%s", filters, where_clause, params)
    df = query(sql, params=params)
    df = _normalize_df_columns(df, INV_COL_MAP) if df is not None else pd.DataFrame()
    return df


def get_sku_all(filters=None, use_eq=False, max_rows=2000):
    """
    Fetch full SKU list for caching (no pagination). Capped at max_rows.
    Returns (df, total_count). If total > max_rows, df has max_rows rows.
    """
    where_clause, params = _build_where(filters or {})
    where_sql = f"WHERE {where_clause}" if where_clause else ""
    total = get_sku_total_count(filters or {})
    params = list(params) + [max_rows]
    sql = f"""
    SELECT SKU, VARIETY_ABBR, SUM(CARTONS) AS CARTONS, SUM(EQ_ON_HAND) AS EQ_ON_HAND
    FROM {DBT_SCHEMA}.inv_on_hand_pivot
    {where_sql}
    GROUP BY SKU, VARIETY_ABBR
    ORDER BY VARIETY_ABBR, SKU
    LIMIT %s
    """
    df = query(sql, params=params)
    df = _normalize_df_columns(df, INV_COL_MAP) if df is not None else pd.DataFrame()
    return df, total


# Max rows for pre-fetched SKU-by-week cache (used for instant pivot-cell filtering)
_SKU_BY_WEEK_MAX_ROWS = 5000

# Dimensions in cache key (coarse) vs in-memory (fine). Only group + stage trigger cache rebuild.
IN_CACHE_KEY_DIMS = frozenset(("group_category", "final_stage_status"))
IN_MEMORY_FILTER_DIMS = frozenset(("variety", "week_bucket", "pack", "grade", "size"))


def get_sku_all_by_week(filters=None, use_eq=False, max_rows=None):
    """
    Fetch SKU-level rows at (SKU, variety, week_bucket, pack, grade, size) grain for in-memory filtering.
    Use base (cache-key) filters only. Returns DataFrame with sku, variety_abbr, week_bucket, pack_abbr,
    grade_abbr, size_abbr, cartons, eq_on_hand.
    """
    max_rows = max_rows or _SKU_BY_WEEK_MAX_ROWS
    f = _base_filters_only(filters)
    where_clause, params = _build_where(f)
    where_sql = f"WHERE {where_clause}" if where_clause else ""
    params = list(params) + [max_rows]
    sql = f"""
    SELECT SKU, VARIETY_ABBR, WEEK_BUCKET, WEEK_BUCKET_NUM,
           PACK_ABBR, GRADE_ABBR, SIZE_ABBR,
           SUM(CARTONS) AS CARTONS, SUM(EQ_ON_HAND) AS EQ_ON_HAND
    FROM {DBT_SCHEMA}.inv_on_hand_pivot
    {where_sql}
    GROUP BY SKU, VARIETY_ABBR, WEEK_BUCKET, WEEK_BUCKET_NUM, PACK_ABBR, GRADE_ABBR, SIZE_ABBR
    ORDER BY VARIETY_ABBR, SKU, WEEK_BUCKET_NUM NULLS LAST
    LIMIT %s
    """
    df = query(sql, params=params)
    df = _normalize_df_columns(df, INV_COL_MAP) if df is not None else pd.DataFrame()
    return df


def _base_filters_only(filters):
    """Return only filters that are part of the cache key (group, stage)."""
    if not filters:
        return {}
    return {k: v for k, v in (filters or {}).items() if k in IN_CACHE_KEY_DIMS and v}


def get_changes_today_detail(filters=None):
    """
    Raw change rows (change_type, pack_abbr, grade_abbr, size_abbr, variety_abbr, cartons, eq_on_hand).
    Use base filters only. Callback filters in memory and aggregates to Packed/Shipped/Staged.
    """
    filters = _base_filters_only(filters or {})
    if "week_bucket" in (filters or {}):
        filters = {k: v for k, v in filters.items() if k != "week_bucket"}
    where_clause, params = _build_where(filters)
    where_sql = f"AND {where_clause}" if where_clause else ""
    sql = (
        "SELECT change_type, VARIETY_ABBR, PACK_ABBR, GRADE_ABBR, SIZE_ABBR, "
        "SUM(CARTONS) AS cartons, SUM(EQ_ON_HAND) AS eq_on_hand "
        f"FROM {DBT_SCHEMA}.inv_changes_today "
        "WHERE 1=1 "
        f"{where_sql} "
        "GROUP BY change_type, VARIETY_ABBR, PACK_ABBR, GRADE_ABBR, SIZE_ABBR "
        "ORDER BY change_type"
    )
    df = query(sql, params=params)
    return _normalize_df_columns(df, INV_COL_MAP) if df is not None else pd.DataFrame()


def filters_to_cache_key(filters, include_detail_dims=False):
    """Convert filters dict to immutable cache key tuple. Only group_category and final_stage_status
    are in the cache key; variety, pack, grade, size, week_bucket are filtered in-memory."""
    if not filters:
        return ()
    if include_detail_dims:
        items = [(k, v) for k, v in filters.items() if v is not None and str(v).strip()]
    else:
        items = [
            (k, v)
            for k, v in filters.items()
            if v is not None and str(v).strip() and k in IN_CACHE_KEY_DIMS
        ]
    return tuple(sorted(items))


def inv_cache_identifier(filters, use_eq):
    """Convert (filters, use_eq) to a stable string for cache_manager. ({}, False)->'default', ({}, True)->'default_eq', else 'k1=v1|k2=v2:cartons' or ':eq'."""
    fkey = filters_to_cache_key(filters or {})
    if not fkey and not use_eq:
        return "default"
    if not fkey and use_eq:
        return "default_eq"
    parts = "|".join(f"{k}={v}" for k, v in fkey)
    suffix = "eq" if use_eq else "cartons"
    return f"{parts}:{suffix}"


def inv_cache_identifier_decode(identifier):
    """Decode cache_manager identifier to (filters_dict, use_eq)."""
    if identifier == "default":
        return {}, False
    if identifier == "default_eq":
        return {}, True
    try:
        if ":" in identifier:
            rest, suffix = identifier.rsplit(":", 1)
            use_eq = suffix.lower() == "eq"
        else:
            rest, use_eq = identifier, False
        filters = {}
        if rest:
            for part in rest.split("|"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    filters[k.strip()] = v.strip()
        return filters, use_eq
    except Exception:
        return {}, False


def build_inv_payload(filters, use_eq):
    """Build inventory payload for caching. Uses base filters (group, stage) only.
    pivot_df and total are derived in callback from filtered sku_all_df.
    Fine filters (variety, pack, grade, size, week_bucket) applied in-memory."""
    _start = time.perf_counter()
    f = _base_filters_only(filters or {})
    changes_detail_df = get_changes_today_detail(f)
    sku_all_df = get_sku_all_by_week(f, use_eq=use_eq)
    return {
        "changes_detail_df": changes_detail_df,
        "sku_all_df": sku_all_df,
        "_cached_at": datetime.now().isoformat(),
        "_cached_duration_seconds": round(time.perf_counter() - _start, 2),
    }


def build_inv_payload_for_cache(identifier):
    """Build inventory payload for cache_manager. Decodes identifier to (filters, use_eq) and calls build_inv_payload."""
    filters, use_eq = inv_cache_identifier_decode(identifier)
    return build_inv_payload(filters, use_eq)


def get_inv_cache_options():
    """Return options for cache_manager prewarm (default and default_eq)."""
    return [
        {"label": "Default (cartons)", "value": "default"},
        {"label": "Default (EQ)", "value": "default_eq"},
    ]


def _fetch_filter_options():
    """Fetch distinct values for each filter dimension from inv_on_hand_pivot."""
    cols = [
        ("GROUP_CATEGORY", "group_category"),
        ("VARIETY_ABBR", "variety"),
        ("PACK_ABBR", "pack"),
        ("GRADE_ABBR", "grade"),
        ("SIZE_ABBR", "size"),
        ("POOL", "pool"),
        ('"Process Code"', "process_code"),
        ("FINAL_STAGE_STATUS", "final_stage_status"),
    ]
    result = {}
    for sql_col, key in cols:
        try:
            df = query(
                f"SELECT DISTINCT {sql_col} AS v FROM {DBT_SCHEMA}.inv_on_hand_pivot "
                f"WHERE {sql_col} IS NOT NULL AND TRIM(CAST({sql_col} AS VARCHAR)) != '' "
                "ORDER BY v",
            )
            opts = [{"label": "All", "value": "ALL"}]
            # Snowflake uppercases unquoted identifiers; handle both 'v' and 'V'
            col_name = None
            if df is not None and not df.empty:
                if "v" in df.columns:
                    col_name = "v"
                elif "V" in df.columns:
                    col_name = "V"
            if col_name is not None:
                for v in df[col_name].dropna().unique():
                    s = str(v).strip()
                    if s:
                        opts.append({"label": s, "value": s})
            result[key] = opts
        except Exception:
            result[key] = [{"label": "All", "value": "ALL"}]
    return result


def get_filter_options():
    """Filter dropdown options. Cached 60s."""
    global _FILTER_OPTIONS_CACHE, _FILTER_OPTIONS_TS
    now = time.time()
    if _FILTER_OPTIONS_CACHE is not None and (now - _FILTER_OPTIONS_TS) < _FILTER_CACHE_TTL:
        return _FILTER_OPTIONS_CACHE
    opts = _fetch_filter_options()
    # Fallback: if variety has only "All", try pivot data (e.g. when filter-options query fails)
    try:
        if len(opts.get("variety", [])) <= 1:
            df = get_pivot_data({}, use_eq=False)
            if df is not None and not df.empty:
                col = "variety_abbr" if "variety_abbr" in df.columns else "VARIETY_ABBR"
                if col in df.columns:
                    variety_opts = [{"label": "All", "value": "ALL"}]
                    for v in df[col].dropna().unique():
                        s = str(v).strip()
                        if s:
                            variety_opts.append({"label": s, "value": s})
                    opts["variety"] = variety_opts
    except Exception:
        pass
    _FILTER_OPTIONS_CACHE = opts
    _FILTER_OPTIONS_TS = now
    return _FILTER_OPTIONS_CACHE


def get_inv_cache_status():
    """Read-only status for Caching page. Returns {cache_key_str: cached_at_str} from central cache_manager."""
    from services.cache_manager import get_cache_status
    inv_entries = get_cache_status().get("inventory", {})
    result = {}
    for k, v in inv_entries.items():
        label = "default (eqs)" if k == "default_eq" else "default (cartons)" if k == "default" else k
        result[label] = str(v) if v else "-"
    return result


def _apply_fine_filters_to_df(df, filters, variety_col="variety_abbr", week_col="week_bucket"):
    """Filter df by fine (in-memory) dims: variety, week_bucket, pack, grade, size."""
    if df is None or df.empty:
        return df
    f = filters or {}
    df = df.copy()
    col_map = {
        "variety": variety_col if variety_col in df.columns else "VARIETY_ABBR",
        "week_bucket": week_col if week_col in df.columns else "WEEK_BUCKET",
        "pack": "pack_abbr" if "pack_abbr" in df.columns else "PACK_ABBR",
        "grade": "grade_abbr" if "grade_abbr" in df.columns else "GRADE_ABBR",
        "size": "size_abbr" if "size_abbr" in df.columns else "SIZE_ABBR",
    }
    for key in IN_MEMORY_FILTER_DIMS:
        val = f.get(key)
        if not val or str(val).strip() == "ALL":
            continue
        col = col_map.get(key)
        if col and col in df.columns:
            df = df[df[col].astype(str).str.strip() == str(val).strip()]
    return df


def derive_changes_from_detail(changes_detail_df, use_eq):
    """Aggregate changes_detail_df to change_type totals. Returns df with change_type, cartons, eq_on_hand."""
    if changes_detail_df is None or changes_detail_df.empty:
        return pd.DataFrame()
    measure = "eq_on_hand" if use_eq else "cartons"
    if "change_type" not in changes_detail_df.columns:
        return pd.DataFrame()
    agg = changes_detail_df.groupby("change_type", as_index=False).agg(
        {"cartons": "sum", "eq_on_hand": "sum"}
    )
    return agg


def filters_from_store(store):
    """Convert dcc.Store filter values to dict. store keys: group_category, variety, etc."""
    if not store:
        return {}
    return {
        k: (v if v and v != "ALL" else None)
        for k, v in store.items()
        if k in (
            "group_category",
            "variety",
            "pack",
            "grade",
            "size",
            "pool",
            "process_code",
            "final_stage_status",
            "week_bucket",
        )
    }


register_report(
    build_inv_payload_for_cache,
    get_options_func=get_inv_cache_options,
    prewarm_all_options=True,
    historical_refresh_keys=["default", "default_eq"],
)
