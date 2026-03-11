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
    "age_weighted_cartons": ["AGE_WEIGHTED_CARTONS"],
    "age_weighted_eq": ["AGE_WEIGHTED_EQ"],
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
        SUM(ON_HAND_END_OF_DAY * COALESCE(AVG_AGE_ON_HAND_DAYS, 0)) AS AGE_WEIGHTED_CARTONS,
        SUM(ON_HAND_END_OF_DAY_EQ * COALESCE(AVG_AGE_ON_HAND_DAYS, 0)) AS AGE_WEIGHTED_EQ,
        SUM(ON_HAND_END_OF_DAY * (1 + COALESCE(AVG_AGE_ON_HAND_DAYS, 0) / 30.0)) AS RISK_WEIGHTED_ON_HAND
    FROM {DBT_SCHEMA}.DT_CARTON_DAILY_SNAPSHOT_EQ
    WHERE DATE >= DATEADD(year, -2, CURRENT_DATE)
    GROUP BY DATE, GROUP_CATEGORY, CROP_YEAR, SOURCE, VARIETY_ABBR, REPORT_GROUP, POOL, GROWER_NUMBER
    ORDER BY DATE, GROUP_CATEGORY, CROP_YEAR, SOURCE, VARIETY_ABBR, REPORT_GROUP, POOL, GROWER_NUMBER
    """
    logger.info("[TRENDS] Fetching aggregated trends data")
    df = query(sql)
    logger.info(
        "[TRENDS] Query returned %s rows, columns: %s",
        len(df) if df is not None else 0,
        list(df.columns) if df is not None else [],
    )
    if df is not None and not df.empty:
        for sql_col, keys in TRENDS_COL_MAP.items():
            for k in keys:
                if k in df.columns and k != sql_col:
                    df = df.rename(columns={k: sql_col})
        df["on_hand_end_of_day"] = pd.to_numeric(df["on_hand_end_of_day"], errors="coerce").fillna(0).astype(float)
        df["on_hand_end_of_day_eq"] = pd.to_numeric(df["on_hand_end_of_day_eq"], errors="coerce").fillna(0).astype(float)
        df["age_weighted_cartons"] = pd.to_numeric(df["age_weighted_cartons"], errors="coerce").fillna(0).astype(float)
        df["age_weighted_eq"] = pd.to_numeric(df["age_weighted_eq"], errors="coerce").fillna(0).astype(float)
        df["avg_age_on_hand_days"] = (df["age_weighted_cartons"] / df["on_hand_end_of_day"].replace(0, pd.NA)).fillna(0)
        # Parse date so cache stores real calendar dates (avoids 1970 YOY bug after deserialization)
        date_key = "date" if "date" in df.columns else "DATE"
        df[date_key] = pd.to_datetime(df[date_key], errors="coerce")
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


def _chart_layout_no_axes():
    """Layout for make_subplots figures: no xaxis/yaxis so we don't overwrite subplot axes."""
    return dict(
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        font=dict(color="#fff", size=12),
        showlegend=True,
        legend=dict(orientation="h", y=1.12, yanchor="bottom", x=0.5, xanchor="center", font=dict(size=11)),
        margin=dict(l=50, r=30, t=70, b=50),
        height=340,
        hovermode="x unified",
    )


def build_cartons_by_day_of_year_figure(df, use_eq=False, yoy_mode="none"):
    """Simple chart: X = Day of year (1-366), Y = Cartons (or EQs) summed.
    yoy_mode: none = single line (all years combined by day_of_year);
    last_year = one line per year (2 years); two_years = one line per year (up to 3 years)."""
    import plotly.graph_objects as go

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
    if on_hand not in df.columns:
        fig.add_annotation(text="No on-hand column", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    work = df.copy()
    work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    logger.info("[DEBUG CHART] build_cartons_by_day_of_year: date_col=%s on_hand=%s df.shape=%s", date_col, on_hand, work.shape)
    logger.info("[DEBUG CHART] df sample (date, %s): %s", on_hand, work[[date_col, on_hand]].head(5).to_dict())
    work = work.dropna(subset=[date_col])
    if work.empty:
        fig.add_annotation(text="No valid dates", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig
    work["_year"] = work[date_col].dt.year
    work["_day_of_year"] = work[date_col].dt.dayofyear
    agg = work.groupby(["_year", "_day_of_year"], as_index=False)[on_hand].sum()
    logger.info("[DEBUG CHART] agg shape=%s, head: %s, sum(%s)=%s", agg.shape, agg.head(5).to_dict(), on_hand, agg[on_hand].sum())

    unit = "EQs" if use_eq else "Cartons"
    if yoy_mode == "none":
        # Single line: aggregate all years by day_of_year
        by_day = agg.groupby("_day_of_year", as_index=False)[on_hand].sum()
        by_day = by_day.sort_values("_day_of_year").reset_index(drop=True)
        x_vals = by_day["_day_of_year"].astype(int).tolist()
        y_vals = by_day[on_hand].astype(float).tolist()
        fig.add_trace(
            go.Scatter(
                x=x_vals, y=y_vals,
                mode="lines", line=dict(color="#64B5F6", width=2),
                name="On-hand",
            )
        )
    else:
        years = sorted(agg["_year"].unique(), reverse=True)
        n_years = 3 if yoy_mode == "two_years" else 2
        colors = ["#64B5F6", "#81C784", "#FFB74D"]
        for i, yr in enumerate(years[:n_years]):
            sub = agg[agg["_year"] == yr].sort_values("_day_of_year").reset_index(drop=True)
            if sub.empty:
                continue
            x_vals = sub["_day_of_year"].astype(int).tolist()
            y_vals = sub[on_hand].astype(float).tolist()
            fig.add_trace(
                go.Scatter(
                    x=x_vals, y=y_vals,
                    mode="lines", line=dict(color=colors[i % len(colors)], width=2),
                    name=str(int(yr)),
                )
            )
        fig.update_layout(xaxis_title="Day of year")

    fig.update_layout(
        title=dict(text=f"On-Hand by Day of Year ({unit})", font=dict(size=14)),
        yaxis_title=unit,
    )
    return fig


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
    logger.info("[DEBUG CHART] build_on_hand_line_chart: date_col=%s on_hand=%s df.shape=%s", date_col, on_hand, df.shape)

    agg = df.groupby(date_col, as_index=False)[on_hand].sum()
    agg[date_col] = pd.to_datetime(agg[date_col])
    agg = agg.sort_values(date_col).reset_index(drop=True)

    # Use summed column by name (ensure we plot the column, not index)
    y_col = on_hand if on_hand in agg.columns else (agg.columns[1] if len(agg.columns) > 1 else on_hand)
    logger.info(
        "[TRENDS CHART] build_on_hand_line_chart agg.head(3) (columns=%s): %s",
        list(agg.columns),
        agg.head(3).to_dict() if not agg.empty else {},
    )

    if yoy_mode == "none":
        fig.add_trace(
            go.Scatter(
                x=agg[date_col].tolist(), y=agg[y_col].astype(float).tolist(),
                mode="lines", line=dict(color="#64B5F6", width=2),
                name="On-hand",
            )
        )
    else:
        years = agg[date_col].dt.year.unique()
        logger.info(
            "[TRENDS CHART] YOY branch: years=%s, len(years)=%s",
            years.tolist() if hasattr(years, "tolist") else list(years),
            len(years),
        )
        if len(years) < 2 and yoy_mode != "none":
            fig.add_trace(
                go.Scatter(
                    x=agg[date_col].tolist(), y=agg[y_col].astype(float).tolist(),
                    mode="lines", line=dict(color="#64B5F6", width=2),
                    name="On-hand",
                )
            )
        else:
            years_sorted = sorted(years, reverse=True)
            colors = ["#64B5F6", "#81C784", "#FFB74D"]
            for i, yr in enumerate(years_sorted[:3] if yoy_mode == "two_years" else years_sorted[:2]):
                sub = agg[agg[date_col].dt.year == yr].sort_values(date_col).copy()
                if sub.empty:
                    logger.warning("[TRENDS CHART] YOY year %s has no rows (years=%s)", yr, years.tolist())
                    continue
                sub["day_of_year"] = sub[date_col].dt.dayofyear
                x_vals = sub["day_of_year"].astype(int).tolist()
                y_vals = sub[y_col].astype(float).tolist()
                fig.add_trace(
                    go.Scatter(
                        x=x_vals, y=y_vals,
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


def build_on_hand_with_age_chart(df, filters, yoy_mode="none", use_eq=False):
    """Duplicate of On-Hand Over Time with Avg Age added. Uses same aggregation as build_on_hand_line_chart.
    Dual axis: On Hand (left), Avg Age (right). Built to match the working top chart."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    if df is None or df.empty:
        fig = go.Figure(layout=_chart_layout())
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
    weight_col = "age_weighted_eq" if use_eq else "age_weighted_cartons"
    if weight_col not in df.columns or on_hand not in df.columns:
        fig = go.Figure(layout=_chart_layout())
        fig.add_annotation(text="No age data", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    date_col = "date" if "date" in df.columns else "DATE"
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df["_vol"] = pd.to_numeric(df[on_hand], errors="coerce").fillna(0).astype(float)
    df["_weighted"] = pd.to_numeric(df[weight_col], errors="coerce").fillna(0).astype(float)
    agg = df.groupby(date_col, as_index=False).agg(
        on_hand_sum=("_vol", "sum"),
        avg_age_sum=("_weighted", "sum"),
    )
    agg["weighted_avg_age"] = (agg["avg_age_sum"] / agg["on_hand_sum"].replace(0, pd.NA)).fillna(0)
    agg = agg.sort_values(date_col).reset_index(drop=True)

    unit = "EQs" if use_eq else "Cartons"
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    layout = _chart_layout_no_axes()

    if yoy_mode == "none":
        x_vals = agg[date_col].tolist()
        fig.add_trace(
            go.Scatter(
                x=x_vals, y=agg["on_hand_sum"].astype(float).tolist(),
                mode="lines", line=dict(color="#64B5F6", width=2),
                name=f"On Hand ({unit})",
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=x_vals, y=agg["weighted_avg_age"].astype(float).tolist(),
                mode="lines", line=dict(color="#FFB74D", width=2),
                name="Avg Age (days)",
            ),
            secondary_y=True,
        )
    else:
        years = agg[date_col].dt.year.unique()
        years_sorted = sorted(years, reverse=True)
        colors_oh = ["#64B5F6", "#81C784", "#4CAF50"]
        colors_age = ["#FFB74D", "#FF9800", "#f44336"]
        for i, yr in enumerate(years_sorted[:3] if yoy_mode == "two_years" else years_sorted[:2]):
            sub = agg[agg[date_col].dt.year == yr].sort_values(date_col).copy()
            sub["day_of_year"] = sub[date_col].dt.dayofyear
            x_vals = sub["day_of_year"].astype(int).tolist()
            fig.add_trace(
                go.Scatter(
                    x=x_vals, y=sub["on_hand_sum"].astype(float).tolist(),
                    mode="lines", line=dict(color=colors_oh[i % len(colors_oh)], width=2),
                    name=f"{int(yr)} On Hand",
                ),
                secondary_y=False,
            )
            fig.add_trace(
                go.Scatter(
                    x=x_vals, y=sub["weighted_avg_age"].astype(float).tolist(),
                    mode="lines", line=dict(color=colors_age[i % len(colors_age)], width=2, dash="dash"),
                    name=f"{int(yr)} Avg Age",
                ),
                secondary_y=True,
            )
        fig.update_layout(xaxis_title="Day of year")

    fig.update_layout(
        **layout,
        title=dict(text=f"On-Hand Over Time + Avg Age ({unit})", font=dict(size=14)),
    )
    fig.update_yaxes(title_text=unit, secondary_y=False, gridcolor="#333")
    fig.update_yaxes(title_text="Avg Age (days)", secondary_y=True, gridcolor="rgba(51,51,51,0.3)")
    return fig


def build_aging_stack_chart(df, filters, use_eq=False):
    """Stacked area: inventory by age bucket over time. use_eq: show EQs instead of cartons."""
    import plotly.graph_objects as go

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
    weight_col = "age_weighted_eq" if use_eq else "age_weighted_cartons"
    if weight_col not in df.columns or on_hand not in df.columns:
        fig.add_annotation(text="No age data", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df["_avg_age"] = (df[weight_col] / df[on_hand].replace(0, pd.NA)).fillna(0).clip(lower=0)
    df["age_bucket"] = pd.cut(
        df["_avg_age"],
        bins=[-0.1, 14, 28, 42, 56, 999],
        labels=["0–2w", "2–4w", "4–6w", "6–8w", "8+w"],
    )
    df["age_bucket"] = df["age_bucket"].astype(str)

    pivot = df.pivot_table(
        index=date_col, columns="age_bucket", values=on_hand, aggfunc="sum", fill_value=0
    )
    logger.info(
        "[TRENDS CHART] build_aging_stack_chart pivot: shape=%s, columns=%s",
        pivot.shape,
        list(pivot.columns) if hasattr(pivot, "columns") else None,
    )
    if not pivot.empty and hasattr(pivot, "columns"):
        logger.info(
            "[TRENDS CHART] pivot head row sums: %s",
            pivot.head(3).sum(axis=1).tolist(),
        )
        logger.info(
            "[TRENDS CHART] pivot tail row sums: %s",
            pivot.tail(3).sum(axis=1).tolist(),
        )
    cols = ["0–2w", "2–4w", "4–6w", "6–8w", "8+w"]
    cols = [c for c in cols if c in pivot.columns]
    if not cols:
        fig.add_annotation(text="No age data", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    colors = ["#4CAF50", "#8BC34A", "#FFC107", "#FF9800", "#f44336"]
    x_vals = pivot.index.tolist()
    for i, c in enumerate(cols):
        if c in pivot.columns:
            r, g, b = _hex_to_rgb(colors[i % len(colors)])
            fig.add_trace(
                go.Scatter(
                    x=x_vals, y=pivot[c].astype(float).tolist(),
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


def build_avg_age_line_chart(df, filters, yoy_mode="none", use_eq=False):
    """Line chart: average age of cartons on hand over time. Age in days. yoy_mode: none, last_year, two_years."""
    import plotly.graph_objects as go

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
    weight_col = "age_weighted_eq" if use_eq else "age_weighted_cartons"
    if weight_col not in df.columns or on_hand not in df.columns:
        fig.add_annotation(text="No age data", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df["_vol"] = pd.to_numeric(df[on_hand], errors="coerce").fillna(0).astype(float)
    df["_weighted"] = pd.to_numeric(df[weight_col], errors="coerce").fillna(0).astype(float)
    agg = df.groupby(date_col, as_index=False).agg(
        avg_age=("_weighted", "sum"),
        total=("_vol", "sum"),
    )
    agg["weighted_avg_age"] = (agg["avg_age"] / agg["total"].replace(0, pd.NA)).fillna(0)
    agg = agg.sort_values(date_col).reset_index(drop=True)

    if yoy_mode == "none":
        fig.add_trace(
            go.Scatter(
                x=agg[date_col].tolist(), y=agg["weighted_avg_age"].astype(float).tolist(),
                mode="lines", line=dict(color="#FFB74D", width=2),
                name="Avg Age (days)",
            )
        )
    else:
        years = agg[date_col].dt.year.unique()
        years_sorted = sorted(years, reverse=True)
        colors = ["#FFB74D", "#81C784", "#64B5F6"]
        for i, yr in enumerate(years_sorted[:3] if yoy_mode == "two_years" else years_sorted[:2]):
            sub = agg[agg[date_col].dt.year == yr].sort_values(date_col).copy()
            sub["day_of_year"] = sub[date_col].dt.dayofyear
            x_vals = sub["day_of_year"].astype(int).tolist()
            y_vals = sub["weighted_avg_age"].astype(float).tolist()
            fig.add_trace(
                go.Scatter(
                    x=x_vals, y=y_vals,
                    mode="lines", line=dict(color=colors[i % len(colors)], width=2),
                    name=str(int(yr)),
                )
            )
        fig.update_layout(xaxis_title="Day of year")

    fig.update_layout(
        title=dict(text="Average Age of Cartons On Hand (days)", font=dict(size=14)),
        yaxis_title="Days",
    )
    return fig


def build_on_hand_vs_avg_age_chart(df, filters, yoy_mode="none", use_eq=False):
    """Dual y-axis: on-hand volume (left) + average age in days (right). One chart, two lines.
    Uses make_subplots(secondary_y=True) so both Cartons and EQs render correctly."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    if df is None or df.empty:
        fig = go.Figure(layout=_chart_layout())
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
    weight_col = "age_weighted_eq" if use_eq else "age_weighted_cartons"
    if weight_col not in df.columns or on_hand not in df.columns:
        fig = go.Figure(layout=_chart_layout())
        fig.add_annotation(text="No age data", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df["_vol"] = pd.to_numeric(df[on_hand], errors="coerce").fillna(0).astype(float)
    df["_weighted"] = pd.to_numeric(df[weight_col], errors="coerce").fillna(0).astype(float)
    agg = df.groupby(date_col, as_index=False).agg(
        on_hand_sum=("_vol", "sum"),
        avg_age_sum=("_weighted", "sum"),
    )
    agg["weighted_avg_age"] = (agg["avg_age_sum"] / agg["on_hand_sum"].replace(0, pd.NA)).fillna(0)
    agg = agg.sort_values(date_col).reset_index(drop=True)

    unit = "EQs" if use_eq else "Cartons"
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    layout = _chart_layout_no_axes()

    # Add Age first (primary/left), On Hand second (secondary/right), then swap axis sides
    # so On Hand appears on left and Age on right (fixes Plotly trace/axis swap bug)
    if yoy_mode == "none":
        x_vals = agg[date_col].tolist()
        fig.add_trace(
            go.Scatter(
                x=x_vals, y=agg["weighted_avg_age"].astype(float).tolist(),
                mode="lines", line=dict(color="#FFB74D", width=2),
                name="Avg Age (days)",
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=x_vals, y=agg["on_hand_sum"].astype(float).tolist(),
                mode="lines", line=dict(color="#64B5F6", width=2),
                name=f"On Hand ({unit})",
            ),
            secondary_y=True,
        )
    else:
        years = agg[date_col].dt.year.unique()
        years_sorted = sorted(years, reverse=True)
        colors_oh = ["#64B5F6", "#81C784", "#4CAF50"]
        colors_age = ["#FFB74D", "#FF9800", "#f44336"]
        for i, yr in enumerate(years_sorted[:3] if yoy_mode == "two_years" else years_sorted[:2]):
            sub = agg[agg[date_col].dt.year == yr].sort_values(date_col).copy()
            sub["day_of_year"] = sub[date_col].dt.dayofyear
            x_vals = sub["day_of_year"].astype(int).tolist()
            fig.add_trace(
                go.Scatter(
                    x=x_vals, y=sub["weighted_avg_age"].astype(float).tolist(),
                    mode="lines", line=dict(color=colors_age[i % len(colors_age)], width=2, dash="dash"),
                    name=f"{int(yr)} Avg Age",
                ),
                secondary_y=False,
            )
            fig.add_trace(
                go.Scatter(
                    x=x_vals, y=sub["on_hand_sum"].astype(float).tolist(),
                    mode="lines", line=dict(color=colors_oh[i % len(colors_oh)], width=2),
                    name=f"{int(yr)} On Hand",
                ),
                secondary_y=True,
            )
        fig.update_layout(xaxis_title="Day of year")

    fig.update_layout(
        **layout,
        title=dict(text="On Hand vs Average Age (days)", font=dict(size=14)),
        yaxis=dict(side="right", title=dict(text="Avg Age (days)"), gridcolor="rgba(51,51,51,0.3)"),
        yaxis2=dict(side="left", title=dict(text=unit), gridcolor="#333"),
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
