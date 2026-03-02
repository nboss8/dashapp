"""
Production Intra Day KPIs - all callbacks.
Imported by pages/production_intra_day_kpis for registration.
"""
import pandas as pd
from dash import callback, Input, Output, State, html, no_update, ALL, ctx, dcc
from utils.table_helpers import _normalize_df_columns
from services.cache_manager import get_cached_data
from services.pidk_data import (
    get_run_totals,
    get_run_keys_for_shift,
    get_pidk_bph_chart_data,
    build_pidk_bph_chart_all_growers,
    get_sizer_events_with_event_ids,
    get_sizer_drops_for_event,
    get_sizer_drops_for_all_events,
    aggregate_sizer_drops_from_cache,
    filter_sizer_events_by_run_packdate,
    get_eq_data,
    get_employee_summary_data,
    build_sizer_matrix_table,
    filter_eq_by_run_or_packdate,
    filter_eq_by_classification,
    build_eq_matrix_table,
    eq_data_to_package_type_df,
    build_package_type_table,
    compute_employee_summary,
    build_employee_summary_table,
    build_run_totals_table,
    build_shift_totals_table,
    RUN_COL_MAP,
    SHIFT_COL_MAP,
)

# Layout constants used by callbacks
# ORDER MUST MATCH layout depth-first: run-totals, shift-totals, bph, sizer, employee, computech
_PIDK_TILE_IDS = ["run-totals", "shift-totals", "bph", "sizer", "employee", "computech"]
_PIDK_ICON_EXPAND = html.Img(
    src="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' fill='%23fff' viewBox='0 0 16 16'%3E%3Cpath d='M1.5 1a.5.5 0 0 0-.5.5v4a.5.5 0 0 1-1 0v-4A1.5 1.5 0 0 1 1.5 0h4a.5.5 0 0 1 0 1h-4zM10 .5a.5.5 0 0 1 .5-.5h4A1.5 1.5 0 0 1 16 1.5v4a.5.5 0 0 1-1 0v-4a.5.5 0 0 0-.5-.5h-4a.5.5 0 0 1-.5-.5zM.5 10a.5.5 0 0 1 .5.5v4a.5.5 0 0 0 .5.5h4a.5.5 0 0 1 0 1h-4A1.5 1.5 0 0 1 0 14.5v-4a.5.5 0 0 1 .5-.5zm15 0a.5.5 0 0 1 .5.5v4a1.5 1.5 0 0 1-1.5 1.5h-4a.5.5 0 0 1 0-1h4a.5.5 0 0 0 .5-.5v-4a.5.5 0 0 1 .5-.5z'/%3E%3C/svg%3E",
    style={"width": "14px", "height": "14px", "display": "inline-block", "verticalAlign": "middle"},
)
_PIDK_ICON_X = html.Img(
    src="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' fill='%23fff' viewBox='0 0 16 16'%3E%3Cpath d='M2.146 2.854a.5.5 0 1 1 .708-.708L8 7.293l5.146-5.147a.5.5 0 0 1 .708.708L8.707 8l5.147 5.146a.5.5 0 0 1-.708.708L8 8.707l-5.146 5.147a.5.5 0 0 1-.708-.708L7.293 8 2.146 2.854Z'/%3E%3C/svg%3E",
    style={"width": "14px", "height": "14px", "display": "inline-block", "verticalAlign": "middle"},
)


@callback(
    Output("pidk-day-label-dropdown", "value", allow_duplicate=True),
    Input("_pages_location", "pathname"),
    prevent_initial_call=True,
)
def set_default_today_on_load(pathname):
    if pathname == "/production/intra-day-kpis":
        return "TODAY"
    return no_update


@callback(
    Output("pidk-day-store", "data"),
    Output("pidk-selected-run", "data"),
    Output("pidk-selected-shift", "data"),
    Output("pidk-run-slicer", "value", allow_duplicate=True),
    Output("pidk-shift-slicer", "value", allow_duplicate=True),
    Input("pidk-day-label-dropdown", "value"),
    prevent_initial_call=True,
)
def update_day_store(day_label):
    return day_label or "TODAY", None, None, "ALL", "ALL"


@callback(
    Output("pidk-expanded-tile", "data"),
    Input({"type": "pidk-expand-btn", "index": ALL}, "n_clicks"),
    State("pidk-expanded-tile", "data"),
    prevent_initial_call=True,
)
def pidk_toggle_expand(n_clicks_list, expanded):
    tid = ctx.triggered_id
    if not tid or tid.get("type") != "pidk-expand-btn":
        return no_update
    clicked = tid.get("index")
    if expanded == clicked:
        return None
    return clicked


@callback(
    Output({"type": "pidk-tile-wrapper", "index": ALL}, "className"),
    Input("pidk-expanded-tile", "data"),
)
def pidk_apply_expanded_class(expanded):
    return [
        "pidk-tile-wrapper" + (" pidk-tile-expanded" if expanded == i else "")
        for i in _PIDK_TILE_IDS
    ]


@callback(
    Output({"type": "pidk-expand-btn", "index": ALL}, "children"),
    Input("pidk-expanded-tile", "data"),
)
def pidk_update_expand_button_labels(expanded):
    return [_PIDK_ICON_X if expanded == i else _PIDK_ICON_EXPAND for i in _PIDK_TILE_IDS]


def _get_row_val(row, *keys):
    """Case-insensitive row lookup for Snowflake column name variations."""
    if not row:
        return None
    for k in keys:
        if k in row:
            return row[k]
    low = {str(kk).lower(): (kk, row[kk]) for kk in row.keys()}
    for k in keys:
        k_low = str(k).lower()
        if k_low in low:
            return low[k_low][1]
    return None


def _safe_str(value):
    if value is None:
        return ""
    return str(value).strip()


def _run_option_value(row):
    return f"run|{_safe_str(_get_row_val(row, 'RUN_KEY', 'run_key'))}|{_safe_str(_get_row_val(row, 'PACKDATE_RUN_KEY', 'packdate_run_key'))}|{_safe_str(_get_row_val(row, 'LOT', 'Lot', 'lot'))}"


def _shift_option_value(row):
    return f"shift|{_safe_str(_get_row_val(row, 'PACKDATE_RUN_KEY', 'packdate_run_key'))}|{_safe_str(_get_row_val(row, 'SHIFT', 'Shift', 'shift'))}"


def _filter_run_rows(run_data, selected_run, selected_shift):
    if not run_data:
        return []
    if selected_run:
        run_key = _safe_str(selected_run.get("run_key"))
        packdate_run_key = _safe_str(selected_run.get("packdate_run_key"))
        lot = _safe_str(selected_run.get("lot"))
        out = []
        for row in run_data:
            row_run_key = _safe_str(_get_row_val(row, "RUN_KEY", "run_key"))
            row_pk = _safe_str(_get_row_val(row, "PACKDATE_RUN_KEY", "packdate_run_key"))
            row_lot = _safe_str(_get_row_val(row, "LOT", "Lot", "lot"))
            if run_key and row_run_key == run_key:
                out.append(row)
                continue
            if packdate_run_key and row_pk == packdate_run_key and lot and row_lot == lot:
                out.append(row)
        return out
    if selected_shift:
        packdate_run_key = _safe_str(selected_shift.get("packdate_run_key"))
        if not packdate_run_key:
            return run_data
        return [
            row for row in run_data
            if _safe_str(_get_row_val(row, "PACKDATE_RUN_KEY", "packdate_run_key")) == packdate_run_key
        ]
    return run_data


def _filter_shift_rows(shift_data, selected_run, selected_shift):
    if not shift_data:
        return []
    if selected_shift:
        packdate_run_key = _safe_str(selected_shift.get("packdate_run_key"))
        shift = _safe_str(selected_shift.get("shift"))
        out = []
        for row in shift_data:
            row_pk = _safe_str(_get_row_val(row, "PACKDATE_RUN_KEY", "packdate_run_key"))
            row_shift = _safe_str(_get_row_val(row, "SHIFT", "Shift", "shift"))
            if packdate_run_key and row_pk != packdate_run_key:
                continue
            if shift and row_shift and row_shift != shift:
                continue
            out.append(row)
        return out
    if selected_run:
        packdate_run_key = _safe_str(selected_run.get("packdate_run_key"))
        if not packdate_run_key:
            return shift_data
        return [
            row for row in shift_data
            if _safe_str(_get_row_val(row, "PACKDATE_RUN_KEY", "packdate_run_key")) == packdate_run_key
        ]
    return shift_data


@callback(
    Output("pidk-selected-run", "data", allow_duplicate=True),
    Output("pidk-selected-shift", "data", allow_duplicate=True),
    Output("pidk-shift-slicer", "value", allow_duplicate=True),
    Input("pidk-run-slicer", "value"),
    State("pidk-run-data", "data"),
    State("pidk-selected-run", "data"),
    State("pidk-selected-shift", "data"),
    prevent_initial_call=True,
)
def update_selection_from_run_slicer(value, run_data, selected_run, selected_shift):
    if value is None or value == "ALL" or not run_data:
        if selected_run is None and selected_shift is None:
            return no_update, no_update, no_update
        return None, None, "ALL"
    row = None
    for candidate in run_data:
        if _run_option_value(candidate) == value:
            row = candidate
            break
    # Backward-compatible fallback for any stale index values during rollout.
    if row is None:
        try:
            idx = int(value)
            if 0 <= idx < len(run_data):
                row = run_data[idx]
        except (ValueError, TypeError):
            row = None
    if row is None:
        return None, None, "ALL"
    run_filter = {
        "run_key": _get_row_val(row, "RUN_KEY", "run_key"),
        "packdate_run_key": _get_row_val(row, "PACKDATE_RUN_KEY", "packdate_run_key"),
        "lot": _get_row_val(row, "LOT", "Lot", "lot"),
        "shift": _get_row_val(row, "SHIFT", "Shift", "shift"),
        "run": _get_row_val(row, "RUN", "Run", "run"),
    }
    return run_filter, None, "ALL"


@callback(
    Output("pidk-selected-shift", "data", allow_duplicate=True),
    Output("pidk-selected-run", "data", allow_duplicate=True),
    Output("pidk-run-slicer", "value", allow_duplicate=True),
    Input("pidk-shift-slicer", "value"),
    State("pidk-shift-data", "data"),
    State("pidk-selected-run", "data"),
    State("pidk-selected-shift", "data"),
    prevent_initial_call=True,
)
def update_selection_from_shift_slicer(value, shift_data, selected_run, selected_shift):
    if value is None or value == "ALL" or not shift_data:
        # When shift is ALL: clear selected_shift only. Do NOT touch run slicer or
        # selected_run, otherwise run callback's programmatic shift->ALL triggers
        # this and wipes the user's run selection.
        if selected_shift is None:
            return no_update, no_update, no_update
        return None, no_update, no_update
    row = None
    for candidate in shift_data:
        if _shift_option_value(candidate) == value:
            row = candidate
            break
    # Backward-compatible fallback for any stale index values during rollout.
    if row is None:
        try:
            idx = int(value)
            if 0 <= idx < len(shift_data):
                row = shift_data[idx]
        except (ValueError, TypeError):
            row = None
    if row is None:
        return None, None, "ALL"
    shift_filter = {
        "packdate_run_key": _get_row_val(row, "PACKDATE_RUN_KEY", "packdate_run_key"),
        "shift": _get_row_val(row, "SHIFT", "Shift", "shift"),
    }
    return shift_filter, None, "ALL"


def _bph_from_cache(cached, selected_run, selected_shift):
    """Build BPH figure from cached bph_data when TODAY + filters."""
    bph_data = cached.get("bph_data") or {}
    if not bph_data:
        return None
    grower_dfs = []
    if selected_run:
        packdate_run_key = selected_run.get("packdate_run_key")
        lot = selected_run.get("lot")
        if packdate_run_key and lot:
            key = (str(packdate_run_key).strip(), str(lot).strip())
            chart_df = bph_data.get(key)
            # Fallback: handle lot formatting differences, e.g. "0043" vs "43".
            if (chart_df is None or chart_df.empty):
                lot_norm = str(lot).strip().lstrip("0") or "0"
                for (pk, lot_key), df in bph_data.items():
                    pk_norm = str(pk).strip()
                    lk_norm = str(lot_key).strip().lstrip("0") or "0"
                    if pk_norm == str(packdate_run_key).strip() and lk_norm == lot_norm:
                        chart_df = df
                        break
            if chart_df is not None and not chart_df.empty:
                grower_dfs.append((lot, chart_df))
    elif selected_shift:
        packdate_run_key = selected_shift.get("packdate_run_key")
        if packdate_run_key:
            for (pk, lot), chart_df in bph_data.items():
                if str(pk).strip() == str(packdate_run_key).strip() and chart_df is not None and not chart_df.empty:
                    grower_dfs.append((lot, chart_df))
    else:
        for (pk, lot), chart_df in bph_data.items():
            if chart_df is not None and not chart_df.empty:
                grower_dfs.append((lot, chart_df))
    return build_pidk_bph_chart_all_growers(grower_dfs)


def _bph_figure_live(day_label, selected_run, selected_shift):
    """Build BPH figure when cache miss or non-TODAY."""
    run_df = get_run_totals(day_label)
    run_df = _normalize_df_columns(run_df, RUN_COL_MAP)
    lot_col = "Lot" if "Lot" in run_df.columns else (run_df.columns[3] if len(run_df.columns) > 3 else None)
    if selected_run:
        run_key = selected_run.get("run_key")
        packdate_run_key = selected_run.get("packdate_run_key")
        lot = selected_run.get("lot")
        lots = [lot] if lot else []
        grower_dfs = []
        for g in lots:
            chart_df = get_pidk_bph_chart_data(day_label, g, run_key=run_key, packdate_run_key=packdate_run_key)
            if not chart_df.empty:
                grower_dfs.append((g, chart_df))
        return build_pidk_bph_chart_all_growers(grower_dfs)
    if selected_shift:
        packdate_run_key = selected_shift.get("packdate_run_key")
        lot_run_pairs = get_run_keys_for_shift(day_label, packdate_run_key)
        grower_dfs = []
        for lot, run_key in lot_run_pairs:
            chart_df = get_pidk_bph_chart_data(day_label, lot, run_key=run_key, packdate_run_key=packdate_run_key)
            if not chart_df.empty:
                grower_dfs.append((lot, chart_df))
        return build_pidk_bph_chart_all_growers(grower_dfs)
    if lot_col is None or run_df.empty:
        return build_pidk_bph_chart_all_growers([])
    lots = run_df[lot_col].dropna().astype(str).unique().tolist()
    grower_dfs = []
    for lot in lots:
        chart_df = get_pidk_bph_chart_data(day_label, lot)
        if not chart_df.empty:
            grower_dfs.append((lot, chart_df))
    return build_pidk_bph_chart_all_growers(grower_dfs)


def _sliced_label_text(selected_run, selected_shift):
    """Build 'Sliced by...' text for display in filtered visuals."""
    if selected_run:
        run = selected_run.get("run", "")
        lot = selected_run.get("lot", "")
        return f"Sliced by Run {run} · Lot {lot}" if run and lot else "Sliced by run"
    if selected_shift:
        shift = selected_shift.get("shift", "")
        return f"Sliced by Shift {shift}" if shift else "Sliced by shift"
    return ""


_NO_DATA_P = html.P("No data", style={"color": "#999", "textAlign": "center", "padding": "16px"})
_ERR_P = html.P("Error loading data", style={"color": "#FFC107", "textAlign": "center", "padding": "16px"})


def _pidk_fallback_outputs(error=False):
    """Return 18-tuple of safe fallback outputs when payload build fails or callback errors."""
    msg = _ERR_P if error else _NO_DATA_P
    wrapper = lambda x: html.Div(x, className="pidk-table-wrapper")
    empty_fig = build_pidk_bph_chart_all_growers([])
    sizer_opts = [{"label": "All", "value": "ALL"}]
    return (
        wrapper(msg),
        wrapper(msg),
        "Last updated: —",
        [],
        [],
        [{"label": "All", "value": "ALL"}],
        [{"label": "All", "value": "ALL"}],
        empty_fig,
        sizer_opts,
        "ALL",
        "",
        "",
        "",
        "",
        wrapper(html.P("Select an event or batch", style={"color": "#999", "textAlign": "center", "padding": "16px"})),
        wrapper(msg),
        wrapper(msg),
        wrapper(msg),
    )


@callback(
    Output("pidk-run-totals-table", "children"),
    Output("pidk-shift-totals-table", "children"),
    Output("pidk-last-updated", "children"),
    Output("pidk-run-data", "data"),
    Output("pidk-shift-data", "data"),
    Output("pidk-run-slicer", "options"),
    Output("pidk-shift-slicer", "options"),
    Output("pidk-bph-chart", "figure"),
    Output("pidk-sizer-event-dropdown", "options"),
    Output("pidk-sizer-event-dropdown", "value"),
    Output("pidk-sliced-label-bph", "children"),
    Output("pidk-sliced-label-sizer", "children"),
    Output("pidk-sliced-label-employee", "children"),
    Output("pidk-sliced-label-eq", "children"),
    Output("pidk-sizer-matrix", "children"),
    Output("pidk-eq-matrix", "children"),
    Output("pidk-package-type-table", "children"),
    Output("pidk-employee-summary", "children"),
    Input("pidk-interval", "n_intervals"),
    Input("pidk-day-store", "data"),
    Input("pidk-selected-run", "data"),
    Input("pidk-selected-shift", "data"),
    Input("pidk-sizer-event-dropdown", "value"),
    Input("pidk-sizer-packout-dropdown", "value"),
    Input("pidk-selected-package-type", "data"),
)
def update_pidk_all(
    _n_interval,
    day_label,
    selected_run,
    selected_shift,
    sizer_event_value,
    sizer_packout_value,
    selected_pkg,
):
    cache_key = day_label or "TODAY"
    if ctx.triggered_id == "pidk-interval" and cache_key != "TODAY":
        return (no_update,) * 18

    try:
        cached = get_cached_data("pidk", cache_key)

        run_data = cached.get("run_data") or []
        shift_data = cached.get("shift_data") or []
        last_updated = cached.get("last_updated") or "Last updated: —"
        filtered_run = _filter_run_rows(run_data, selected_run, selected_shift)
        filtered_shift = _filter_shift_rows(shift_data, selected_run, selected_shift)
        run_df = _normalize_df_columns(pd.DataFrame(filtered_run), RUN_COL_MAP)
        shift_df = _normalize_df_columns(pd.DataFrame(filtered_shift), SHIFT_COL_MAP)
        run_table = html.Div(build_run_totals_table(run_df), className="pidk-table-wrapper")
        shift_table = html.Div(build_shift_totals_table(shift_df), className="pidk-table-wrapper")

        run_opts = [{"label": "All", "value": "ALL"}]
        for i, row in enumerate(run_data):
            run_val = _get_row_val(row, "RUN", "Run", "run")
            lot_val = _get_row_val(row, "LOT", "Lot", "lot")
            label = f"Run {run_val} · Lot {lot_val}" if run_val and lot_val else f"Run {i + 1}"
            run_opts.append({"label": label, "value": _run_option_value(row)})
        shift_opts = [{"label": "All", "value": "ALL"}]
        for i, row in enumerate(shift_data):
            shift_val = _get_row_val(row, "SHIFT", "Shift", "shift")
            label = f"Shift {shift_val}" if shift_val else f"Shift {i + 1}"
            shift_opts.append({"label": label, "value": _shift_option_value(row)})

        if "bph_data" in cached:
            bph_fig = _bph_from_cache(cached, selected_run, selected_shift)
            if bph_fig is None:
                bph_fig = _bph_figure_live(cache_key, selected_run, selected_shift)
        else:
            bph_fig = _bph_figure_live(cache_key, selected_run, selected_shift)

        run_key = selected_run.get("run_key") if selected_run else None
        packdate_run_key = selected_run.get("packdate_run_key") if selected_run else (
            selected_shift.get("packdate_run_key") if selected_shift else None
        )
        if "sizer_events_full" in cached:
            events = filter_sizer_events_by_run_packdate(
                cached["sizer_events_full"], run_key=run_key, packdate_run_key=packdate_run_key
            )
            sizer_event_opts = [{"label": "All", "value": "ALL"}] + [
                {"label": e["label"], "value": e["event_id"]} for e in events
            ]
        else:
            events = get_sizer_events_with_event_ids(cache_key, run_key=run_key, packdate_run_key=packdate_run_key)
            sizer_event_opts = [{"label": "All", "value": "ALL"}] + [
                {"label": e["label"], "value": e["event_id"]} for e in events
            ]
        option_values = {opt["value"] for opt in sizer_event_opts}
        sizer_event_val = "ALL" if (sizer_event_value not in option_values) else no_update
        # Use "ALL" when unset/invalid so matrix loads on initial page refresh (avoids needing a second callback run)
        effective_sizer_value = sizer_event_value if (sizer_event_value and sizer_event_value in option_values) else ("ALL" if "ALL" in option_values else sizer_event_value)

        label_text = _sliced_label_text(selected_run, selected_shift)
        label_el = html.Span(label_text, style={"color": "#64B5F6", "fontSize": "0.75rem", "fontWeight": "600"}) if label_text else ""
        sliced_label_bph = label_el
        sliced_label_sizer = label_el
        sliced_label_employee = label_el
        sliced_label_eq = label_el

        if not effective_sizer_value or effective_sizer_value not in option_values:
            sizer_matrix_content = html.Div(
                html.P("Select an event or batch", style={"color": "#999", "textAlign": "center", "padding": "16px"}),
                className="pidk-table-wrapper",
            )
        else:
            event_id = effective_sizer_value
            packout_group = sizer_packout_value
            if "sizer_drops_by_event" in cached:
                events_f = filter_sizer_events_by_run_packdate(
                    cached["sizer_events_full"], run_key=run_key, packdate_run_key=packdate_run_key
                )
                if event_id == "ALL":
                    event_ids = [e["event_id"] for e in events_f]
                    drops_df = aggregate_sizer_drops_from_cache(cached["sizer_drops_by_event"], event_ids)
                else:
                    d = cached["sizer_drops_by_event"].get(str(event_id))
                    drops_df = d.copy() if (d is not None and not d.empty) else pd.DataFrame()
                if drops_df is not None and not drops_df.empty and packout_group and packout_group != "All":
                    if "PACKOUT_GROUP" in drops_df.columns:
                        drops_df = drops_df[drops_df["PACKOUT_GROUP"].astype(str).str.strip() == str(packout_group).strip()].copy()
                        if drops_df.empty:
                            sizer_matrix_content = html.Div(
                                html.P(f"No data for Packout Group = {packout_group}", style={"color": "#999", "textAlign": "center", "padding": "16px"}),
                                className="pidk-table-wrapper",
                            )
                        else:
                            sizer_matrix_content = html.Div(build_sizer_matrix_table(drops_df), className="pidk-table-wrapper")
                    else:
                        sizer_matrix_content = html.Div(build_sizer_matrix_table(drops_df), className="pidk-table-wrapper")
                else:
                    sizer_matrix_content = html.Div(build_sizer_matrix_table(drops_df), className="pidk-table-wrapper")
            else:
                if event_id == "ALL":
                    drops_df = get_sizer_drops_for_all_events(cache_key, run_key=run_key, packdate_run_key=packdate_run_key)
                else:
                    drops_df = get_sizer_drops_for_event(event_id)
                if drops_df is not None and not drops_df.empty and packout_group and packout_group != "All":
                    if "PACKOUT_GROUP" in drops_df.columns:
                        drops_df = drops_df[drops_df["PACKOUT_GROUP"].astype(str).str.strip() == str(packout_group).strip()].copy()
                        if drops_df.empty:
                            sizer_matrix_content = html.Div(
                                html.P(f"No data for Packout Group = {packout_group}", style={"color": "#999", "textAlign": "center", "padding": "16px"}),
                                className="pidk-table-wrapper",
                            )
                        else:
                            sizer_matrix_content = html.Div(build_sizer_matrix_table(drops_df), className="pidk-table-wrapper")
                    else:
                        sizer_matrix_content = html.Div(build_sizer_matrix_table(drops_df), className="pidk-table-wrapper")
                else:
                    sizer_matrix_content = html.Div(build_sizer_matrix_table(drops_df), className="pidk-table-wrapper")

        if "eq_df_full" in cached:
            eq_df = filter_eq_by_run_or_packdate(cached["eq_df_full"], run_key=run_key, packdate_run_key=packdate_run_key)
            eq_df = filter_eq_by_classification(eq_df, selected_pkg)
            eq_matrix_content = html.Div(build_eq_matrix_table(eq_df), className="pidk-table-wrapper")
            pkg_df = eq_data_to_package_type_df(
                filter_eq_by_run_or_packdate(cached["eq_df_full"], run_key=run_key, packdate_run_key=packdate_run_key)
            )
            pkg_table_content = html.Div(build_package_type_table(pkg_df, selected_package_type=selected_pkg), className="pidk-table-wrapper")
        else:
            eq_df = get_eq_data(cache_key, run_key=run_key, packdate_run_key=packdate_run_key)
            eq_df = filter_eq_by_classification(eq_df, selected_pkg)
            eq_matrix_content = html.Div(build_eq_matrix_table(eq_df), className="pidk-table-wrapper")
            pkg_df = eq_data_to_package_type_df(get_eq_data(cache_key, run_key=run_key, packdate_run_key=packdate_run_key))
            pkg_table_content = html.Div(build_package_type_table(pkg_df, selected_package_type=selected_pkg), className="pidk-table-wrapper")

        packdate_run_key_emp = selected_shift.get("packdate_run_key") if selected_shift else (selected_run.get("packdate_run_key") if selected_run else None)
        if "employee_df_full" in cached:
            emp_df = cached["employee_df_full"]
            if emp_df is not None and not emp_df.empty and packdate_run_key_emp:
                pk_col = "DATE_SHIFT_KEY" if "DATE_SHIFT_KEY" in emp_df.columns else None
                if pk_col:
                    emp_df = emp_df[emp_df[pk_col].astype(str).str.strip() == str(packdate_run_key_emp).strip()]
            summary = compute_employee_summary(emp_df)
            employee_content = html.Div(build_employee_summary_table(summary), className="pidk-table-wrapper")
        else:
            dt_df = get_employee_summary_data(cache_key, packdate_run_key=packdate_run_key_emp)
            summary = compute_employee_summary(dt_df)
            employee_content = html.Div(build_employee_summary_table(summary), className="pidk-table-wrapper")

        return (
            run_table,
            shift_table,
            last_updated,
            run_data,
            shift_data,
            run_opts,
            shift_opts,
            bph_fig,
            sizer_event_opts,
            sizer_event_val,
            sliced_label_bph,
            sliced_label_sizer,
            sliced_label_employee,
            sliced_label_eq,
            sizer_matrix_content,
            eq_matrix_content,
            pkg_table_content,
            employee_content,
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"PIDK update_pidk_all error: {e}")
        return _pidk_fallback_outputs(error=True)


@callback(
    Output("ag-grid-csv-download", "data"),
    Input("pidk-run-totals-export-btn", "n_clicks"),
    Input("pidk-shift-totals-export-btn", "n_clicks"),
    State("pidk-run-data", "data"),
    State("pidk-shift-data", "data"),
    prevent_initial_call=True,
)
def export_pidk_csv(run_clicks, shift_clicks, run_data, shift_data):
    trigger = ctx.triggered_id
    if not trigger:
        return no_update
    # Only fire on real button click (n_clicks > 0)
    if trigger == "pidk-run-totals-export-btn":
        if not run_clicks or not run_data:
            return no_update
        df = pd.DataFrame(run_data)
        return dcc.send_data_frame(df.to_csv, "run_totals.csv", index=False)
    if trigger == "pidk-shift-totals-export-btn":
        if not shift_clicks or not shift_data:
            return no_update
        df = pd.DataFrame(shift_data)
        return dcc.send_data_frame(df.to_csv, "shift_totals.csv", index=False)
    return no_update


@callback(
    Output("pidk-selected-package-type", "data"),
    Input("pidk-day-store", "data"),
    Input("pidk-selected-run", "data"),
    Input("pidk-selected-shift", "data"),
)
def reset_package_type_filter_on_context_change(day_label, selected_run, selected_shift):
    return None


@callback(
    Output("pidk-selected-package-type", "data", allow_duplicate=True),
    Input({"type": "pidk-pkg-filter-btn", "index": ALL}, "n_clicks"),
    State("pidk-selected-package-type", "data"),
    prevent_initial_call=True,
)
def update_package_type_filter(_n_clicks, current):
    tid = ctx.triggered_id
    if not tid or not isinstance(tid, dict) or tid.get("type") != "pidk-pkg-filter-btn":
        return no_update
    idx = tid.get("index")
    if idx is None:
        return no_update
    if idx == "All":
        return None
    if current == idx:
        return None
    return idx


