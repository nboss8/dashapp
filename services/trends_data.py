"""
Packed Inventory Trends - data layer for line charts, YOY, aging risk.
Source: DT_CARTON_DAILY_SNAPSHOT_EQ. Cache: 1/day, filter in-memory.
"""
import os
import time
import logging
from datetime import datetime

import pandas as pd

from services.snowflake_service import query
from services.cache_manager import register_report

DBT_SCHEMA = os.getenv("SNOWFLAKE_DATABASE", "FROSTY") + "." + os.getenv("DBT_SCHEMA", "DBT_DEV_DBT_DEV")
logger = logging.getLogger(__name__)

TRENDS_COL_MAP = {
    "date": ["DATE"],
    "group_category": ["GROUP_CATEGORY"],
    "crop_year": ["CROP_YEAR"],
    "variety_abbr": ["VARIETY_ABBR"],
    "source": ["SOURCE"],
    "report_group": ["REPORT_GROUP"],
    "pool": ["POOL"],
    "grower_number": ["GROWER_NUMBER"],
    "on_hand_end_of_day": ["ON_HAND_END_OF_DAY"],
    "on_hand_end_of_day_eq": ["ON_HAND_END_OF_DAY_EQ"],
    "packed_that_day": ["PACKED_THAT_DAY"],
    "shipped_that_day_net": ["SHIPPED_THAT_DAY_NET"],
    "avg_age_on_hand_days": ["AVG_AGE_ON_HAND_DAYS"],
    "risk_weighted_on_hand": ["RISK_WEIGHTED_ON_HAND"],
}

_FILTER_OPTIONS_CACHE = None
_FILTER_OPTIONS_TS = 0
_FILTER_CACHE_TTL = 60


def get_trends_data():
    """
    Fetch pre-aggregated trends from DT. Grain: DATE, CROP_YEAR, SOURCE, VARIETY_ABBR, REPORT_GROUP.
    Returns ~50k-200k rows (not raw 16M).
    """
    sql = f"""
    SELECT
        DATE,
        GROUP_CATEGORY,
        CROP_YEAR,
        SOURCE,
        VARIETY_ABBR,
        REPORT_GROUP,
        POOL,
        GROWER_NUMBER,
        SUM(ON_HAND_END_OF_DAY) AS ON_HAND_END_OF_DAY,
        SUM(ON_HAND_END_OF_DAY_EQ) AS ON_HAND_END_OF_DAY_EQ,
        SUM(PACKED_THAT_DAY) AS PACKED_THAT_DAY,
        SUM(SHIPPED_THAT_DAY_NET) AS SHIPPED_THAT_DAY_NET,
        SUM(ON_HAND_END_OF_DAY * COALESCE(AVG_AGE_ON_HAND_DAYS, 0)) / NULLIF(SUM(ON_HAND_END_OF_DAY), 0) AS AVG_AGE_ON_HAND_DAYS,
        SUM(ON_HAND_END_OF_DAY * (1 + COALESCE(AVG_AGE_ON_HAND_DAYS, 0) / 30.0)) AS RISK_WEIGHTED_ON_HAND
    FROM {DBT_SCHEMA}.DT_CARTON_DAILY_SNAPSHOT_EQ
    WHERE DATE >= DATEADD(year, -2, CURRENT_DATE)
    GROUP BY DATE, GROUP_CATEGORY, CROP_YEAR, SOURCE, VARIETY_ABBR, REPORT_GROUP, POOL, GROWER_NUMBER
    ORDER BY DATE, GROUP_CATEGORY, CROP_YEAR, SOURCE, VARIETY_ABBR, REPORT_GROUP, POOL, GROWER_NUMBER
    """
    logger.info("[TRENDS] Fetching aggregated trends data")
    df = query(sql)
    if df is not None and not df.empty:
        for sql_col, keys in TRENDS_COL_MAP.items():
            for k in keys:
                if k in df.columns and k != sql_col:
                    df = df.rename(columns={k: sql_col})
    return df if df is not None and not df.empty else pd.DataFrame()


# CROP = GROUP_CATEGORY. Fixed options with friendly labels.
CROP_OPTIONS = [
    {"label": "APPLES", "value": "AP"},
    {"label": "ORGANIC APPLES", "value": "OA"},
    {"label": "CHERRIES", "value": "CH"},
    {"label": "ORGANIC CHERRIES", "value": "OC"},
]


def get_filter_options():
    """Filter dropdown options. Cached 60s."""
    global _FILTER_OPTIONS_CACHE, _FILTER_OPTIONS_TS
    now = time.time()
    if _FILTER_OPTIONS_CACHE is not None and (now - _FILTER_OPTIONS_TS) < _FILTER_CACHE_TTL:
        return _FILTER_OPTIONS_CACHE
    cols = [
        ("SOURCE", "source"),
        ("CROP_YEAR", "crop_year"),
        ("VARIETY_ABBR", "variety_abbr"),
        ("REPORT_GROUP", "report_group"),
        ("POOL", "pool"),
        ("GROWER_NUMBER", "grower_number"),
    ]
    result = {}
    for sql_col, key in cols:
        try:
            df = query(
                f"SELECT DISTINCT {sql_col} AS v FROM {DBT_SCHEMA}.DT_CARTON_DAILY_SNAPSHOT_EQ "
                f"WHERE {sql_col} IS NOT NULL AND TRIM(CAST({sql_col} AS VARCHAR)) != '' "
                "ORDER BY v"
            )
            opts = [{"label": "All", "value": "ALL"}]
            col_name = "v" if df is not None and "v" in df.columns else "V"
            if df is not None and not df.empty and col_name in df.columns:
                for v in df[col_name].dropna().unique():
                    s = str(v).strip()
                    if s:
                        opts.append({"label": s, "value": s})
            result[key] = opts
        except Exception as e:
            logger.warning("[TRENDS] Filter options for %s failed: %s", key, e)
            result[key] = [{"label": "All", "value": "ALL"}]
    result["group_category"] = CROP_OPTIONS
    _FILTER_OPTIONS_CACHE = result
    _FILTER_OPTIONS_TS = now
    return result


def _apply_filters(df, filters):
    """Filter trends_df in-memory by slicer values. group_category supports multi-select (list)."""
    if df is None or df.empty:
        return df
    df = df.copy()
    if filters:
        # Multi-select: group_category (CROP)
        v = filters.get("group_category")
        if v is not None and v != "ALL":
            selected = v if isinstance(v, list) else [v] if v else []
            selected = [str(x).strip() for x in selected if x and str(x).strip()]
            if selected and "group_category" in df.columns:
                df = df[df["group_category"].astype(str).str.strip().isin(selected)]
        # Single-select filters
        mapping = {
            "source": "source",
            "crop_year": "crop_year",
            "variety_abbr": "variety_abbr",
            "report_group": "report_group",
            "pool": "pool",
            "grower_number": "grower_number",
        }
        for fkey, col in mapping.items():
            v = filters.get(fkey)
            if v and str(v).strip() and str(v).upper() != "ALL":
                if col in df.columns:
                    df = df[df[col].astype(str).str.strip() == str(v).strip()]
    return df


def build_trends_payload():
    """Build payload for cache. Aggregated trends + filter options."""
    _start = time.perf_counter()
    trends_df = get_trends_data()
    filter_opts = get_filter_options()
    payload = {
        "trends_df": trends_df,
        "filter_opts": filter_opts,
        "_cached_at": datetime.now().isoformat(),
        "_cached_duration_seconds": round(time.perf_counter() - _start, 2),
    }
    return payload


def build_trends_payload_for_cache(cache_key):
    """Cache manager entry point. cache_key ignored (single default payload)."""
    return build_trends_payload()


def get_trends_cache_options():
    return [{"label": "Default", "value": "default"}]


def compute_risk_kpis(df):
    """Return high_risk_cartons (age>=42d), critical_cartons (age>=56d)."""
    if df is None or df.empty:
        return {"high_risk_cartons": 0, "critical_cartons": 0}
    col = "avg_age_on_hand_days" if "avg_age_on_hand_days" in df.columns else "AVG_AGE_ON_HAND_DAYS"
    on_hand = "on_hand_end_of_day" if "on_hand_end_of_day" in df.columns else "ON_HAND_END_OF_DAY"
    if col not in df.columns or on_hand not in df.columns:
        return {"high_risk_cartons": 0, "critical_cartons": 0}
    high = int(df.loc[df[col] >= 42, on_hand].sum() or 0)
    critical = int(df.loc[df[col] >= 56, on_hand].sum() or 0)
    return {"high_risk_cartons": high, "critical_cartons": critical}


def _chart_layout():
    return dict(
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        font=dict(color="#fff", size=12),
        showlegend=True,
        legend=dict(orientation="h", y=1.12, yanchor="bottom", x=0.5, xanchor="center", font=dict(size=11)),
        margin=dict(l=50, r=30, t=70, b=50),
        height=340,
        xaxis=dict(gridcolor="#333", showgrid=True),
        yaxis=dict(gridcolor="#333", showgrid=True),
        hovermode="x unified",
    )


def build_on_hand_line_chart(df, filters, yoy_mode="none", use_eq=False):
    """Line chart: on-hand over time. yoy_mode: none, last_year, two_years. use_eq: show EQs instead of cartons."""
    import plotly.graph_objects as go

    fig = go.Figure(layout=_chart_layout())
    if df is None or df.empty:
        fig.add_annotation(
            text="No data", xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False, font=dict(color="#666", size=18),
        )
        return fig

    on_hand = (
        "on_hand_end_of_day_eq" if use_eq and "on_hand_end_of_day_eq" in df.columns
        else "on_hand_end_of_day" if "on_hand_end_of_day" in df.columns
        else "ON_HAND_END_OF_DAY"
    )
    date_col = "date" if "date" in df.columns else "DATE"
    crop_col = "crop_year" if "crop_year" in df.columns else "CROP_YEAR"

    agg = df.groupby(date_col, as_index=False)[on_hand].sum()
    agg[date_col] = pd.to_datetime(agg[date_col])

    if yoy_mode == "none":
        fig.add_trace(
            go.Scatter(
                x=agg[date_col], y=agg[on_hand],
                mode="lines", line=dict(color="#64B5F6", width=2),
                name="On-hand",
            )
        )
    else:
        years = agg[date_col].dt.year.unique()
        if len(years) < 2 and yoy_mode != "none":
            fig.add_trace(
                go.Scatter(x=agg[date_col], y=agg[on_hand], mode="lines",
                           line=dict(color="#64B5F6", width=2), name="On-hand")
            )
        else:
            years_sorted = sorted(years, reverse=True)
            colors = ["#64B5F6", "#81C784", "#FFB74D"]
            for i, yr in enumerate(years_sorted[:3] if yoy_mode == "two_years" else years_sorted[:2]):
                sub = agg[agg[date_col].dt.year == yr]
                sub = sub.sort_values(date_col)
                sub = sub.copy()
                sub["day_of_year"] = sub[date_col].dt.dayofyear
                fig.add_trace(
                    go.Scatter(
                        x=sub["day_of_year"], y=sub[on_hand],
                        mode="lines", line=dict(color=colors[i % len(colors)], width=2),
                        name=str(int(yr)),
                    )
                )
            fig.update_layout(xaxis_title="Day of year")

    unit = "EQs" if use_eq else "Cartons"
    fig.update_layout(
        title=dict(text=f"On-Hand End of Day ({unit})", font=dict(size=14)),
        yaxis_title=unit,
    )
    return fig


def build_aging_stack_chart(df, filters, use_eq=False):
    """Stacked area: inventory by age bucket over time. use_eq: show EQs instead of cartons."""
    import plotly.graph_objects as go
    import numpy as np

    fig = go.Figure(layout=_chart_layout())
    if df is None or df.empty:
        fig.add_annotation(
            text="No data", xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False, font=dict(color="#666", size=18),
        )
        return fig

    date_col = "date" if "date" in df.columns else "DATE"
    on_hand = (
        "on_hand_end_of_day_eq" if use_eq and "on_hand_end_of_day_eq" in df.columns
        else "on_hand_end_of_day" if "on_hand_end_of_day" in df.columns
        else "ON_HAND_END_OF_DAY"
    )
    age_col = "avg_age_on_hand_days" if "avg_age_on_hand_days" in df.columns else "AVG_AGE_ON_HAND_DAYS"

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df["age_bucket"] = pd.cut(
        df[age_col].fillna(0),
        bins=[-0.1, 14, 28, 42, 56, 999],
        labels=["0–2w", "2–4w", "4–6w", "6–8w", "8+w"],
    )
    df["age_bucket"] = df["age_bucket"].astype(str)

    pivot = df.pivot_table(
        index=date_col, columns="age_bucket", values=on_hand, aggfunc="sum", fill_value=0
    )
    cols = ["0–2w", "2–4w", "4–6w", "6–8w", "8+w"]
    cols = [c for c in cols if c in pivot.columns]
    if not cols:
        fig.add_annotation(text="No age data", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    colors = ["#4CAF50", "#8BC34A", "#FFC107", "#FF9800", "#f44336"]
    for i, c in enumerate(cols):
        if c in pivot.columns:
            r, g, b = _hex_to_rgb(colors[i % len(colors)])
            fig.add_trace(
                go.Scatter(
                    x=pivot.index, y=pivot[c],
                    mode="lines", fill="tonexty",
                    name=c, line=dict(width=0.5),
                    fillcolor=f"rgba({r},{g},{b},0.7)",
                )
            )

    unit = "EQs" if use_eq else "Cartons"
    fig.update_layout(
        title=dict(text="Inventory by Age (Higher = Higher Risk)", font=dict(size=14)),
        yaxis_title=unit,
        barmode="stack",
    )
    return fig


def _hex_to_rgb(hex_color):
    """Convert #RRGGBB to (r,g,b) 0-255."""
    h = hex_color.lstrip("#")
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))


register_report(
    build_trends_payload_for_cache,
    get_options_func=get_trends_cache_options,
    prewarm_all_options=False,
    historical_refresh_keys=["default"],
)
