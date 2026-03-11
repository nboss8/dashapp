"""
Packed Inventory Trends - callbacks.
"""
import logging

import pandas as pd
from dash import callback, Input, Output, State, no_update, dcc, ctx

from services.trends_data import (
    _apply_filters,
    build_on_hand_line_chart,
    build_aging_stack_chart,
    build_avg_age_line_chart,
    build_on_hand_vs_avg_age_chart,
    build_on_hand_with_age_chart,
)
from services.cache_manager import get_cached_data

logger = logging.getLogger(__name__)


def _store_from_dropdowns(crop_sel, source, crop_year, variety, report_group, pool, grower):
    crop_val = crop_sel if (isinstance(crop_sel, list) and crop_sel) else None
    return {
        "source": None if source == "ALL" else source,
        "group_category": crop_val,
        "crop_year": None if crop_year == "ALL" else crop_year,
        "variety_abbr": None if variety == "ALL" else variety,
        "report_group": None if report_group == "ALL" else report_group,
        "pool": None if pool == "ALL" else pool,
        "grower_number": None if grower == "ALL" else grower,
    }


@callback(
    [
        Output("trends-filter-source", "options"),
        Output("trends-filter-crop-year", "options"),
        Output("trends-filter-variety", "options"),
        Output("trends-filter-report-group", "options"),
        Output("trends-filter-pool", "options"),
        Output("trends-filter-grower", "options"),
    ],
    Input("trends-interval", "n_intervals"),
)
def _load_filter_options(_n):
    """Load filter dropdown options from cache (instant when cache warm)."""
    default = [{"label": "All", "value": "ALL"}]
    try:
        payload = get_cached_data("trends", "default")
        opts = payload.get("filter_opts") or {}
        return (
            opts.get("source", default),
            opts.get("crop_year", default),
            opts.get("variety_abbr", default),
            opts.get("report_group", default),
            opts.get("pool", default),
            opts.get("grower_number", default),
        )
    except Exception as e:
        logger.warning("Trends filter options failed (cache not ready): %s", e)
        return default, default, default, default, default, default


@callback(
    Output("trends-filters-store", "data"),
    Input("trends-filter-crop", "value"),
    Input("trends-filter-source", "value"),
    Input("trends-filter-crop-year", "value"),
    Input("trends-filter-variety", "value"),
    Input("trends-filter-report-group", "value"),
    Input("trends-filter-pool", "value"),
    Input("trends-filter-grower", "value"),
)
def _sync_filters(crop_sel, source, crop_year, variety, report_group, pool, grower):
    return _store_from_dropdowns(crop_sel, source, crop_year, variety, report_group, pool, grower)


def _empty_figure(message="No data"):
    """Return a minimal valid Plotly figure for dcc.Graph (avoids 500 on callback error)."""
    import plotly.graph_objects as go
    fig = go.Figure(layout=dict(
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        font=dict(color="#fff", size=12),
        height=340,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    ))
    fig.add_annotation(
        text=message, xref="paper", yref="paper",
        x=0.5, y=0.5, showarrow=False, font=dict(color="#888", size=14),
    )
    return fig


@callback(
    Output("trends-line-chart", "figure"),
    Output("trends-aging-chart", "figure"),
    Output("trends-avg-age-chart", "figure"),
    Output("trends-on-hand-vs-age-chart", "figure"),
    Output("trends-on-hand-with-age-chart", "figure"),
    Input("trends-interval", "n_intervals"),
    Input("trends-filters-store", "data"),
    Input("trends-yoy", "value"),
    Input("trends-metric-toggle", "value"),
)
def _update_trends(_n, filters, yoy_mode, metric):
    """Main update: cached payload, filter in-memory, build charts."""
    yoy = yoy_mode or "none"
    use_eq = metric == "eqs"
    filters = filters or {}
    empty_fig = _empty_figure("Loading…")
    try:
        payload = get_cached_data("trends", "default")
    except Exception as e:
        logger.exception("Trends cache fetch failed: %s", e)
        return empty_fig, empty_fig, empty_fig, empty_fig, empty_fig
    trends_df = payload.get("trends_df")
    if trends_df is None or not isinstance(trends_df, pd.DataFrame):
        trends_df = pd.DataFrame()
    logger.info(
        "[TRENDS] Payload trends_df: rows=%s, columns=%s",
        len(trends_df),
        list(trends_df.columns) if hasattr(trends_df, "columns") else None,
    )
    if not trends_df.empty and hasattr(trends_df, "columns"):
        sample_cols = [c for c in ["date", "on_hand_end_of_day"] if c in trends_df.columns]
        if sample_cols:
            logger.info(
                "[TRENDS] Sample (first 5): %s",
                trends_df.head(5)[sample_cols].to_dict() if sample_cols else None,
            )
        if "on_hand_end_of_day" in trends_df.columns:
            payload_sum = trends_df["on_hand_end_of_day"].sum()
            date_key = "date" if "date" in trends_df.columns else "DATE"
            per_date = trends_df.groupby(date_key)["on_hand_end_of_day"].sum()
            logger.info(
                "[TRENDS] Payload totals: sum=%s, per_date head=%s tail=%s",
                payload_sum,
                per_date.head(5).tolist() if not per_date.empty else None,
                per_date.tail(5).tolist() if not per_date.empty else None,
            )
    filtered = _apply_filters(trends_df, filters)
    logger.info("[TRENDS] After filters: len(trends_df)=%s len(filtered)=%s", len(trends_df), len(filtered))
    try:
        fig_line = build_on_hand_line_chart(filtered, filters, yoy, use_eq=use_eq)
        fig_aging = build_aging_stack_chart(filtered, filters, use_eq=use_eq)
        fig_avg_age = build_avg_age_line_chart(filtered, filters, yoy, use_eq=use_eq)
        fig_on_hand_vs_age = build_on_hand_vs_avg_age_chart(filtered, filters, yoy, use_eq=use_eq)
        fig_on_hand_with_age = build_on_hand_with_age_chart(filtered, filters, yoy, use_eq=use_eq)
        return fig_line, fig_aging, fig_avg_age, fig_on_hand_vs_age, fig_on_hand_with_age
    except Exception as e:
        logger.exception("Trends chart build failed: %s", e)
        err_fig = _empty_figure("Error loading chart")
        return err_fig, err_fig, err_fig, err_fig, err_fig


@callback(
    Output("trends-filter-crop", "value"),
    Output("trends-filter-source", "value"),
    Output("trends-filter-crop-year", "value"),
    Output("trends-filter-variety", "value"),
    Output("trends-filter-report-group", "value"),
    Output("trends-filter-pool", "value"),
    Output("trends-filter-grower", "value"),
    Output("trends-filters-store", "data", allow_duplicate=True),
    Input("trends-clear-filters", "n_clicks"),
    prevent_initial_call=True,
)
def _clear_filters(n_clicks):
    if not n_clicks:
        return no_update, no_update, no_update, no_update, no_update, no_update, no_update, no_update
    return [], "ALL", "ALL", "ALL", "ALL", "ALL", "ALL", {}


@callback(
    Output("trends-csv-download", "data"),
    Input("trends-export-btn", "n_clicks"),
    State("trends-filters-store", "data"),
    prevent_initial_call=True,
)
def _export_csv(n_clicks, filters):
    if not n_clicks:
        return no_update
    try:
        payload = get_cached_data("trends", "default")
        trends_df = payload.get("trends_df")
        if trends_df is None or not isinstance(trends_df, pd.DataFrame) or trends_df.empty:
            return no_update
        filtered = _apply_filters(trends_df, filters or {})
        return dcc.send_data_frame(
            filtered.to_csv,
            "packed_inventory_trends.csv",
            index=False,
        )
    except Exception as e:
        logger.exception("Trends export failed: %s", e)
        return no_update


@callback(
    Output("trends-expanded-tile", "data"),
    Input("trends-expand-line", "n_clicks"),
    Input("trends-expand-aging", "n_clicks"),
    State("trends-expanded-tile", "data"),
    prevent_initial_call=True,
)
def _toggle_expand(_line_clicks, _aging_clicks, expanded):
    """Toggle fullscreen for chart tiles."""
    tid = ctx.triggered_id
    if not tid:
        return no_update
    if tid == "trends-expand-line":
        return None if expanded == "line" else "line"
    if tid == "trends-expand-aging":
        return None if expanded == "aging" else "aging"
    return no_update


@callback(
    Output("trends-tile-line", "className"),
    Output("trends-tile-aging", "className"),
    Input("trends-expanded-tile", "data"),
)
def _apply_expanded_class(expanded):
    base = "trends-tile-wrapper"
    line_cls = f"{base} trends-tile-expanded" if expanded == "line" else base
    aging_cls = f"{base} trends-tile-expanded" if expanded == "aging" else base
    return line_cls, aging_cls


@callback(
    Output("trends-clear-filters", "color"),
    Output("trends-clear-filters", "outline"),
    Input("trends-filters-store", "data"),
    prevent_initial_call=True,
)
def _update_clear_button(filters):
    if not filters:
        return "secondary", True
    crop_active = filters.get("group_category") and (
        isinstance(filters["group_category"], list) and len(filters["group_category"]) > 0
    )
    others_active = any(filters.get(k) for k in ["source", "crop_year", "variety_abbr", "report_group", "pool", "grower_number"])
    active = crop_active or others_active
    return ("primary", False) if active else ("secondary", True)
