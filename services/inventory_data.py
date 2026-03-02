"""
Pallet Inventory - data fetching for pivot, SKU detail, changes today, filter options.
All queries use DT_INV_ON_HAND_SKU_GRAIN (via inv_on_hand_pivot) or inv_changes_today.
"""
import os
import threading
import time
from datetime import datetime

import pandas as pd

from services.snowflake_service import query

DBT_SCHEMA = os.getenv("SNOWFLAKE_DATABASE", "FROSTY") + "." + os.getenv("DBT_SCHEMA", "DBT_DEV")

# Filter options cache: 60s TTL
_FILTER_OPTIONS_CACHE = None
_FILTER_OPTIONS_TS = 0
_FILTER_CACHE_TTL = 60

# Inventory data cache (exported for callbacks)
inv_cache = {}
inv_cache_lock = threading.Lock()
_INV_CACHE_MAX_SIZE = 20
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
    ]
    for key, col in mapping:
        v = filters.get(key)
        if v and str(v).strip():
            wheres.append(f"{col} = %s")
            params.append(str(v).strip())
    return (" AND " + " AND ".join(wheres)) if wheres else "", params


def get_pivot_data(filters=None, use_eq=False):
    """
    Variety x week aggregated data for pivot.
    use_eq: if True, primary measure is EQ_ON_HAND; else CARTONS.
    """
    where_clause, params = _build_where(filters or {})
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
    return query(sql, params=params)


def get_sku_detail(filters=None, page=1, page_size=50, use_eq=False):
    """
    Paginated SKU list — one row per SKU with totals. use_eq: if True, primary measure is EQ_ON_HAND.
    Returns df with SKU, VARIETY_ABBR, CARTONS, EQ_ON_HAND.
    """
    where_clause, params = _build_where(filters or {})
    where_sql = f"WHERE {where_clause}" if where_clause else ""
    offset = (page - 1) * page_size
    params = list(params) + [page_size, offset]
    sql = f"""
    SELECT SKU, VARIETY_ABBR, SUM(CARTONS) AS CARTONS, SUM(EQ_ON_HAND) AS EQ_ON_HAND
    FROM {DBT_SCHEMA}.inv_on_hand_pivot
    {where_sql}
    GROUP BY SKU, VARIETY_ABBR
    ORDER BY VARIETY_ABBR, SKU
    LIMIT %s OFFSET %s
    """
    return query(sql, params=params)


def get_sku_total_count(filters=None):
    """Total SKU row count for pagination."""
    where_clause, params = _build_where(filters or {})
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
    df = query(sql, params=params)
    if df.empty:
        return 0
    return int(df.iloc[0]["cnt"]) if "cnt" in df.columns else int(df.iloc[0, 0])


def get_changes_today(filters=None):
    """
    Packed, Shipped, Staged totals for today. Respects filters (Option A).
    Returns df with change_type, cartons, eq_on_hand.
    """
    where_clause, params = _build_where(filters or {})
    where_sql = f"AND {where_clause}" if where_clause else ""
    sql = f"""
    SELECT change_type, SUM(CARTONS) AS cartons, SUM(EQ_ON_HAND) AS eq_on_hand
    FROM {DBT_SCHEMA}.inv_changes_today
    WHERE 1=1 {where_sql}
    GROUP BY change_type
    ORDER BY change_type
    """
    return query(sql, params=params)


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
    return df, total


def filters_to_cache_key(filters):
    """Convert filters dict to immutable cache key tuple."""
    if not filters:
        return ()
    items = [(k, v) for k, v in filters.items() if v is not None and str(v).strip()]
    return tuple(sorted(items))


def build_inv_payload(filters, use_eq):
    """Build full inventory payload for caching."""
    _start = time.perf_counter()
    f = filters or {}
    changes_df = get_changes_today(f)
    pivot_df = get_pivot_data(f, use_eq=use_eq)
    sku_df, total = get_sku_all(f, use_eq=use_eq, max_rows=_INV_SKU_MAX_ROWS)
    return {
        "changes_df": changes_df,
        "pivot_df": pivot_df,
        "sku_full_df": sku_df,
        "total": total,
        "_cached_at": datetime.now().isoformat(),
        "_cached_duration_seconds": round(time.perf_counter() - _start, 2),
    }


def _evict_inv_cache_if_needed():
    """LRU eviction when over max size. Requires lock held."""
    if len(inv_cache) >= _INV_CACHE_MAX_SIZE:
        # Remove oldest entry (first in dict iteration for Python 3.7+)
        oldest = next(iter(inv_cache))
        del inv_cache[oldest]


def _refresh_inv_cache_default():
    """Pre-warm cache for default case (all filters ALL, cartons)."""
    try:
        default_filters = {}
        payload = build_inv_payload(default_filters, use_eq=False)
        key = (filters_to_cache_key(default_filters), False)
        with inv_cache_lock:
            inv_cache[key] = payload
        # Also pre-warm EQs for default
        payload_eq = build_inv_payload(default_filters, use_eq=True)
        key_eq = (filters_to_cache_key(default_filters), True)
        with inv_cache_lock:
            inv_cache[key_eq] = payload_eq
    except Exception as e:
        print(f"Inventory cache refresh error: {e}")


def _inv_background_worker():
    """Daemon thread: refresh default inventory cache every 15 min."""
    _refresh_inv_cache_default()
    while True:
        time.sleep(900)
        _refresh_inv_cache_default()


_inv_background_thread = threading.Thread(target=_inv_background_worker, daemon=True)
_inv_background_thread.start()


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
        df = query(
            f"SELECT DISTINCT {sql_col} AS v FROM {DBT_SCHEMA}.inv_on_hand_pivot "
            f"WHERE {sql_col} IS NOT NULL AND TRIM(CAST({sql_col} AS VARCHAR)) != '' "
            f"ORDER BY v",
        )
        opts = [{"label": "All", "value": "ALL"}]
        if not df.empty and "v" in df.columns:
            for v in df["v"].dropna().unique():
                s = str(v).strip()
                if s:
                    opts.append({"label": s, "value": s})
        result[key] = opts
    return result


def get_filter_options():
    """Filter dropdown options. Cached 60s."""
    global _FILTER_OPTIONS_CACHE, _FILTER_OPTIONS_TS
    now = time.time()
    if _FILTER_OPTIONS_CACHE is not None and (now - _FILTER_OPTIONS_TS) < _FILTER_CACHE_TTL:
        return _FILTER_OPTIONS_CACHE
    _FILTER_OPTIONS_CACHE = _fetch_filter_options()
    _FILTER_OPTIONS_TS = now
    return _FILTER_OPTIONS_CACHE


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
        )
    }
