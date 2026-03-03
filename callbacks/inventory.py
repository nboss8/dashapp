"""
Pallet Inventory - all callbacks.
"""
import logging

import pandas as pd
import dash
from dash import callback, Input, Output, State, html, no_update, ctx, dcc

from components.ag_grid_table import create_ag_grid_table
from services.inventory_data import (
    get_pivot_data,
    get_sku_detail,
    get_sku_pallet_grain,
    get_filter_options,
    filters_from_store,
    inv_cache_identifier,
    _apply_fine_filters_to_df,
    _base_filters_only,
    derive_changes_from_detail,
)
from services.cache_manager import get_cached_data

_INV_DROPDOWN_IDS = [
    "inv-filter-group",
    "inv-filter-variety",
    "inv-filter-pack",
    "inv-filter-grade",
    "inv-filter-size",
    "inv-filter-stage",
]


def _store_from_dropdowns(g, v, p, gr, s, st, grower, run_type):
    return {
        "group_category": None if g == "ALL" else g,
        "variety": None if v == "ALL" else v,
        "pack": None if p == "ALL" else p,
        "grade": None if gr == "ALL" else gr,
        "size": None if s == "ALL" else s,
        "pool": None,
        "process_code": None,
        "final_stage_status": None if st == "ALL" else st,
        "grower_number": None if grower == "ALL" else grower,
        "run_type": None if run_type == "ALL" else run_type,
    }


def _sku_page_from_payload(payload, f, page, use_eq):
    """
    Get (sku_df_page, total, total_cartons, total_eq) for display. If payload has sku_all_df, filter in memory and paginate; else call get_sku_detail.
    """
    page_size = 50
    sku_all = payload.get("sku_all_df")
    if sku_all is not None and not sku_all.empty and isinstance(sku_all, pd.DataFrame):
        variety_col = "variety_abbr" if "variety_abbr" in sku_all.columns else "VARIETY_ABBR"
        week_col = "week_bucket" if "week_bucket" in sku_all.columns else "WEEK_BUCKET"
        df = sku_all.copy()
        if f.get("variety"):
            df = df[df[variety_col].astype(str) == str(f["variety"])]
        if f.get("week_bucket"):
            df = df[df[week_col].astype(str) == str(f["week_bucket"])]
        if df.empty:
            return pd.DataFrame(), 0, 0, 0.0
        grouped = df.groupby("sku", as_index=False).agg({"cartons": "sum", "eq_on_hand": "sum"})
        total = len(grouped)
        total_cartons = int(float(grouped["cartons"].sum() or 0))
        total_eq = float(grouped["eq_on_hand"].sum() or 0)
        start = (page - 1) * page_size
        end = start + page_size
        page_df = grouped.iloc[start:end]
        return page_df, total, total_cartons, total_eq
    sku_df = get_sku_detail(f, page=page, page_size=page_size, use_eq=use_eq)
    total = payload.get("total", 0)
    if sku_df is not None and not sku_df.empty:
        total_cartons = int(float(sku_df["cartons"].sum() or 0)) if "cartons" in sku_df.columns else 0
        total_eq = float(sku_df["eq_on_hand"].sum() or 0) if "eq_on_hand" in sku_df.columns else 0.0
    else:
        total_cartons, total_eq = 0, 0.0
    return sku_df, total, total_cartons, total_eq


def _build_pivot_table(df, use_eq):
    """Build Variety x Week pivot as HTML table. Expects normalized lowercase columns."""
    if df is None or df.empty:
        return html.P("No data available. Run the DT script and dbt models first.", className="text-center text-muted p-4")
    # Normalized df has lowercase: variety_abbr, week_bucket, cartons, eq_on_hand
    idx_col = "variety_abbr" if "variety_abbr" in df.columns else "VARIETY_ABBR"
    col_col = "week_bucket" if "week_bucket" in df.columns else "WEEK_BUCKET"
    measure = "eq_on_hand" if use_eq else "cartons"
    pivot = df.pivot_table(index=idx_col, columns=col_col, values=measure, aggfunc="sum", fill_value=0)
    pivot["Total"] = pivot.sum(axis=1)
    pivot = pivot.reset_index()
    cols = [idx_col] + [c for c in pivot.columns if c != idx_col]
    header_styles = []
    for c in cols:
        if c == idx_col:
            header_styles.append({**_th_style, "textAlign": "left"})
        else:
            header_styles.append({**_th_style, "textAlign": "center"})
    header = html.Tr([html.Th(c, style=header_styles[i]) for i, c in enumerate(cols)])
    rows = []
    for _, r in pivot.iterrows():
        variety_val = r[idx_col]
        row_cells = []
        for c in cols:
            val = r[c]
            if pd.notna(val) and isinstance(val, (int, float)):
                if use_eq:
                    display_val = f"{float(val):,.1f}"
                else:
                    display_val = f"{int(float(val)):,}"
            else:
                display_val = str(val) if pd.notna(val) else "0"
            label_style = {**_td_style, "textAlign": "left"}
            numeric_style = {**_td_style, "textAlign": "center"}
            if c == idx_col:
                row_cells.append(html.Td(display_val, style=label_style))
            elif c == "Total":
                row_cells.append(html.Td(display_val, style=numeric_style))
            else:
                # Clickable pivot cell: clicking filters SKU table to (variety, week_bucket)
                # Age color: 4w yellow, 5w orange, 6+w red
                week_num = int(c)
                num_val = int(float(display_val.replace(",", "") or 0))
                bg_color = "#1a1a1a"
                text_color = "#ddd"
                if num_val > 0 and week_num == 4:
                    bg_color = "#fff59d"
                    text_color = "#333"
                elif num_val > 0 and week_num == 5:
                    bg_color = "#ffcc80"
                    text_color = "#333"
                elif num_val > 0 and week_num >= 6:
                    bg_color = "#ffab91"
                    text_color = "#333"
                cell_style = {**numeric_style, "backgroundColor": bg_color}
                button_style = {
                    **numeric_style,
                    "width": "100%",
                    "backgroundColor": bg_color,
                    "color": text_color,
                    "border": "none",
                    "padding": 0,
                    "cursor": "pointer",
                }
                row_cells.append(
                    html.Td(
                        html.Button(
                            display_val,
                            id={
                                "type": "inv-pivot-cell",
                                "variety": str(variety_val),
                                "week_bucket": str(c),
                            },
                            n_clicks=0,
                            style=button_style,
                        ),
                        style=cell_style,
                    )
                )
        rows.append(html.Tr(row_cells))
    label_style = {**_td_style, "textAlign": "left", "fontWeight": "700", "padding": "6px 12px"}
    numeric_style = {**_td_style, "textAlign": "center", "fontWeight": "700", "padding": "6px 12px"}
    total_row = html.Tr([
        html.Td("Total", style=label_style)
    ] + [html.Td(f"{int(pivot[c].sum()):,}", style=numeric_style) for c in cols[1:]])
    rows.append(total_row)
    return html.Table([
        html.Thead(header),
        html.Tbody(rows),
    ], style={"width": "100%", "borderCollapse": "collapse", "fontSize": "0.85rem"})


_th_style = {"padding": "6px 8px", "textAlign": "center", "color": "#fff", "backgroundColor": "#2a2a2a", "border": "1px solid #444", "fontSize": "0.8rem"}
_td_style = {"padding": "4px 8px", "color": "#ddd", "backgroundColor": "#1a1a1a", "border": "1px solid #333", "fontSize": "0.8rem"}


def _build_pallets_drill_table(pallets_df, use_eq):
    """Build inline pallets sub-table for selected SKU (pallet_ticket, grower, pallet_source_flag, run_type, cartons/eq). Pallet Tr Code hidden."""
    if pallets_df is None or pallets_df.empty:
        return html.P("No pallets for this SKU", style={"color": "#999", "fontSize": "0.8rem", "padding": "8px"})
    cols = ["pallet_ticket", "grower_number", "pallet_source_flag", "run_type"]
    measure = "eq_on_hand" if use_eq else "cartons"
    cols = [c for c in cols if c in pallets_df.columns] + [measure]
    display = pallets_df[[c for c in cols if c in pallets_df.columns]].copy()
    for c in display.columns:
        if c not in (measure,):
            display[c] = display[c].fillna("—").astype(str)
    return create_ag_grid_table(
        display,
        id_prefix="inv-pallets-drill",
        export_filename="pallet_drill.csv",
        pinned_cols=2,
        preserve_numeric_columns=[measure],
    )


def _build_changes_display(changes_df, use_eq):
    """Extract Packed, Shipped, Staged values from changes df."""
    if changes_df is None or changes_df.empty:
        return "—", "—", "—"
    col = "eq_on_hand" if use_eq else "cartons"
    d = changes_df.set_index("change_type").to_dict("index")
    packed = int(d.get("Packed", {}).get(col, 0) or 0)
    shipped = int(d.get("Shipped", {}).get(col, 0) or 0)
    staged = int(d.get("Staged", {}).get(col, 0) or 0)
    return f"{packed:,}", f"{shipped:,}", f"{staged:,}"


@callback(
    [
        Output("inv-filter-group", "options"),
        Output("inv-filter-variety", "options"),
        Output("inv-filter-pack", "options"),
        Output("inv-filter-grade", "options"),
        Output("inv-filter-size", "options"),
        Output("inv-filter-stage", "options"),
        Output("inv-filter-grower", "options"),
        Output("inv-filter-run-type", "options"),
    ],
    Input("inv-interval", "n_intervals"),
)
def _load_inv_filter_options(_n_intervals):
    """Load filter dropdown options from cache when warm (instant), else fallback to get_filter_options."""
    default = [{"label": "All", "value": "ALL"}]
    run_opts = [{"label": "All", "value": "ALL"}, {"label": "Production", "value": "Production"}, {"label": "Repack", "value": "Repack"}]
    try:
        payload = get_cached_data("inventory", "default")
        opts = payload.get("filter_opts") or {}
        if opts:
            return (
                opts.get("group_category", default),
                opts.get("variety", default),
                opts.get("pack", default),
                opts.get("grade", default),
                opts.get("size", default),
                opts.get("final_stage_status", default),
                opts.get("grower_number", default),
                run_opts,
            )
    except Exception:
        pass
    try:
        opts = get_filter_options()
        return (
            opts.get("group_category", default),
            opts.get("variety", default),
            opts.get("pack", default),
            opts.get("grade", default),
            opts.get("size", default),
            opts.get("final_stage_status", default),
            opts.get("grower_number", default),
            run_opts,
        )
    except Exception as e:
        logging.getLogger(__name__).warning("Pallet Inventory: get_filter_options failed: %s", e)
        return default, default, default, default, default, default, default, run_opts


@callback(
    Output("inv-filters-store", "data"),
    Input("inv-filter-group", "value"),
    Input("inv-filter-variety", "value"),
    Input("inv-filter-pack", "value"),
    Input("inv-filter-grade", "value"),
    Input("inv-filter-size", "value"),
    Input("inv-filter-stage", "value"),
    Input("inv-filter-grower", "value"),
    Input("inv-filter-run-type", "value"),
)
def _sync_filters(g, v, p, gr, s, st, grower, run_type):
    return _store_from_dropdowns(g, v, p, gr, s, st, grower, run_type)


@callback(
    Output("inv-filters-store", "data", allow_duplicate=True),
    Output("inv-sku-page", "data", allow_duplicate=True),
    Input({"type": "inv-pivot-cell", "variety": dash.ALL, "week_bucket": dash.ALL}, "n_clicks"),
    State("inv-filters-store", "data"),
    prevent_initial_call=True,
)
def _pivot_cell_to_filters(_n_clicks, store):
    """When a pivot cell is clicked, filter to that (variety, week_bucket) and reset SKU page to 1."""
    tid = ctx.triggered_id
    if not tid or not isinstance(tid, dict):
        return no_update, no_update
    variety = tid.get("variety")
    week_bucket = tid.get("week_bucket")
    if not variety or week_bucket is None:
        return no_update, no_update
    new_store = dict(store or {})
    new_store["variety"] = variety
    new_store["week_bucket"] = week_bucket
    return new_store, 1


def _render_inventory_from_payload(payload, use_eq, sku_page, sku_df, total_override=None, total_cartons=None, total_eq=None):
    """Build UI outputs from payload + current SKU page. total_override/total_cartons/total_eq used when SKU list comes from in-memory filtered sku_all_df."""
    packed, shipped, staged = _build_changes_display(payload["changes_df"], use_eq)
    pivot_table = _build_pivot_table(payload["pivot_df"], use_eq)
    total = total_override if total_override is not None else payload["total"]
    page = sku_page or 1
    if sku_df is None or sku_df.empty:
        sku_table = html.P("No data available", className="text-center text-muted p-4")
    else:
        display_cols = ["sku", "eq_on_hand"] if use_eq else ["sku", "cartons"]
        sku_display = sku_df[display_cols] if all(c in sku_df.columns for c in display_cols) else sku_df
        sku_table = create_ag_grid_table(
            sku_display,
            id_prefix="inv-sku",
            export_filename="pallet_inventory_sku.csv",
            pinned_cols=1,
            preserve_numeric_columns=["eq_on_hand"] if use_eq else ["cartons"],
        )
    num_pages = max(1, (total + 49) // 50)
    if use_eq:
        val = total_eq or 0.0
        fmt = f"{val:,.1f}" if val % 1 else f"{int(val):,}"
        label = "Eq"
    else:
        val = total_cartons or 0
        fmt = f"{val:,}"
        label = "Cartons"
    totals_str = f" · {label}: {fmt}"
    pagination = html.Div([
        html.Span(
            f"Page {sku_page or 1} of {num_pages} ({total} SKUs){totals_str}",
            style={"color": "#aaa", "fontSize": "0.85rem"},
        ),
        html.Div([
            html.Button("Prev", id="inv-sku-prev", className="btn btn-sm btn-outline-secondary me-1", disabled=(sku_page or 1) <= 1),
            html.Button("Next", id="inv-sku-next", className="btn btn-sm btn-outline-secondary", disabled=(sku_page or 1) >= num_pages),
        ], className="d-inline ms-2"),
    ])
    return packed, shipped, staged, pivot_table, sku_table, pagination


@callback(
    Output("inv-packed-value", "children"),
    Output("inv-shipped-value", "children"),
    Output("inv-staged-value", "children"),
    Output("inv-pivot-table", "children"),
    Output("inv-sku-table", "children"),
    Output("inv-sku-pagination", "children"),
    Output("inv-filters-store", "data", allow_duplicate=True),
    Output("inv-sku-page", "data", allow_duplicate=True),
    Input("inv-interval", "n_intervals"),
    Input("inv-filters-store", "data"),
    Input("inv-metric-toggle", "value"),
    Input("inv-sku-page", "data"),
    prevent_initial_call=True,
)
def _update_inventory(_n_intervals, filters, metric, sku_page):
    # No pathname guard: when on Pallet Inventory page, all Inputs exist and callback fires
    use_eq = metric == "eqs"
    triggered_id = ctx.triggered_id
    metric_trigger = triggered_id == "inv-metric-toggle" if triggered_id else False
    if metric_trigger:
        f = {}
        page = 1
    else:
        f = filters_from_store(filters) if filters else {}
        page = sku_page or 1
    base_filters = _base_filters_only(f)
    identifier = inv_cache_identifier(base_filters, use_eq)
    payload = get_cached_data("inventory", identifier)
    sku_all = payload.get("sku_all_df")
    changes_detail = payload.get("changes_detail_df")
    filtered_sku = _apply_fine_filters_to_df(sku_all, f)
    if changes_detail is not None and not changes_detail.empty:
        filtered_changes = _apply_fine_filters_to_df(changes_detail, f)
        changes_df = derive_changes_from_detail(filtered_changes, use_eq)
    else:
        changes_df = payload.get("changes_df", pd.DataFrame())  # legacy payload
    effective_payload = {
        **payload,
        "changes_df": changes_df,
        "pivot_df": filtered_sku,
        "sku_all_df": filtered_sku,
    }
    sku_df, total, total_cartons, total_eq = _sku_page_from_payload(
        effective_payload, f, page, use_eq
    )
    visuals = _render_inventory_from_payload(
        effective_payload, use_eq, page, sku_df,
        total_override=total, total_cartons=total_cartons, total_eq=total_eq,
    )
    if metric_trigger:
        return (*visuals, {}, 1)
    else:
        return (*visuals, no_update, no_update)


@callback(
    Output("inv-sku-page", "data", allow_duplicate=True),
    Input("inv-sku-prev", "n_clicks"),
    Input("inv-sku-next", "n_clicks"),
    Input("inv-filter-group", "value"),
    Input("inv-filter-variety", "value"),
    Input("inv-filter-pack", "value"),
    Input("inv-filter-grade", "value"),
    Input("inv-filter-size", "value"),
    Input("inv-filter-stage", "value"),
    Input("inv-filter-grower", "value"),
    Input("inv-filter-run-type", "value"),
    State("inv-sku-page", "data"),
    prevent_initial_call=True,
)
def _sku_page_control(prev_clicks, next_clicks, g, v, p, gr, s, st, grower, run_type, page):
    tid = ctx.triggered_id
    if not tid:
        return no_update
    p = page or 1
    if tid in ("inv-filter-group", "inv-filter-variety", "inv-filter-pack", "inv-filter-grade", "inv-filter-size", "inv-filter-stage", "inv-filter-grower", "inv-filter-run-type"):
        return 1
    if tid == "inv-sku-prev" and p > 1:
        return p - 1
    if tid == "inv-sku-next":
        return p + 1
    return no_update


@callback(
    Output("inv-csv-download", "data"),
    Input("inv-export-pivot-btn", "n_clicks"),
    Input("inv-export-sku-btn", "n_clicks"),
    State("inv-filters-store", "data"),
    State("inv-metric-toggle", "value"),
    State("inv-sku-page", "data"),
    prevent_initial_call=True,
)
def _export_csv(pivot_clicks, sku_clicks, filters, metric, sku_page):
    tid = ctx.triggered_id
    if not tid or (not pivot_clicks and not sku_clicks):
        return no_update
    f = filters_from_store(filters) if filters else {}
    use_eq = metric == "eqs"
    if tid == "inv-export-pivot-btn":
        df = get_pivot_data(f, use_eq=use_eq)
        if df is None or df.empty:
            return no_update
        measure = "eq_on_hand" if use_eq else "cartons"
        idx_col = "variety_abbr" if "variety_abbr" in df.columns else "VARIETY_ABBR"
        col_col = "week_bucket" if "week_bucket" in df.columns else "WEEK_BUCKET"
        pivot = df.pivot_table(index=idx_col, columns=col_col, values=measure, aggfunc="sum", fill_value=0)
        pivot["Total"] = pivot.sum(axis=1)
        pivot = pivot.reset_index()
        return dcc.send_data_frame(pivot.to_csv, "pallet_inventory_pivot.csv", index=False)
    if tid == "inv-export-sku-btn":
        # Export at pallet grain so CSV includes pallet_ticket, grower_number, run_type
        df = get_sku_pallet_grain(f, use_eq=use_eq, max_rows=100_000)
        df = _apply_fine_filters_to_df(df, f)
        if df is None or df.empty:
            return no_update
        # Order columns: SKU/dims first, then pallet_ticket, grower_number, run_type, then measures
        prefer = ["sku", "variety_abbr", "week_bucket", "pack_abbr", "grade_abbr", "size_abbr",
                  "pallet_ticket", "grower_number", "pallet_source_flag", "run_type", "cartons", "eq_on_hand"]
        cols = [c for c in prefer if c in df.columns]
        cols += [c for c in df.columns if c not in cols]
        df = df[cols]
        return dcc.send_data_frame(df.to_csv, "pallet_inventory_sku.csv", index=False)
    return no_update


@callback(
    Output("inv-filter-group", "value"),
    Output("inv-filter-variety", "value"),
    Output("inv-filter-pack", "value"),
    Output("inv-filter-grade", "value"),
    Output("inv-filter-size", "value"),
    Output("inv-filter-stage", "value"),
    Output("inv-filter-grower", "value"),
    Output("inv-filter-run-type", "value"),
    Output("inv-filters-store", "data", allow_duplicate=True),
    Output("inv-sku-page", "data", allow_duplicate=True),
    Input("inv-clear-filters", "n_clicks"),
    prevent_initial_call=True,
)
def _clear_all_filters(n_clicks):
    """Reset all slicers to show full unfiltered view."""
    return (
        "ALL",
        "ALL",
        "ALL",
        "ALL",
        "ALL",
        "ALL",
        "ALL",
        "ALL",
        {},
        1,
    )


@callback(
    Output("inv-sku-drill-detail", "children"),
    Input("inv-sku-grid", "selectedRows"),
    State("inv-filters-store", "data"),
    State("inv-metric-toggle", "value"),
)
def _sku_drill_on_row_select(selected_rows, filters, metric):
    """When user selects an SKU row, show pallets behind it inline."""
    if not selected_rows or len(selected_rows) == 0:
        return html.P("Click an SKU row to see pallets behind it", style={"color": "#888", "fontSize": "0.8rem", "fontStyle": "italic", "padding": "8px"})
    row = selected_rows[0]
    sku = row.get("sku") or row.get("SKU")
    if not sku:
        return html.P("No SKU in selected row", style={"color": "#999", "fontSize": "0.8rem", "padding": "8px"})
    use_eq = metric == "eqs"
    f = filters_from_store(filters) if filters else {}
    base_filters = _base_filters_only(f)
    identifier = inv_cache_identifier(base_filters, use_eq)
    payload = get_cached_data("inventory", identifier)
    pallets_df = payload.get("pallets_df")
    if pallets_df is None or pallets_df.empty:
        return html.P(f"No pallet data for {sku}", style={"color": "#999", "fontSize": "0.8rem", "padding": "8px"})
    sku_col = "sku" if "sku" in pallets_df.columns else "SKU"
    if sku_col not in pallets_df.columns:
        return html.P("Pallet data has no SKU column", style={"color": "#999", "fontSize": "0.8rem", "padding": "8px"})
    filtered = pallets_df[pallets_df[sku_col].astype(str).str.strip() == str(sku).strip()]
    filtered = _apply_fine_filters_to_df(filtered, f)
    if filtered.empty:
        return html.P(f"No pallets for SKU {sku} with current filters", style={"color": "#999", "fontSize": "0.8rem", "padding": "8px"})
    return html.Div([
        html.P(f"Pallets for SKU {sku}", style={"color": "#64B5F6", "fontSize": "0.85rem", "fontWeight": "600", "marginBottom": "8px"}),
        _build_pallets_drill_table(filtered, use_eq),
    ], className="sku-drill-panel")


@callback(
    Output("inv-clear-filters", "color"),
    Output("inv-clear-filters", "outline"),
    Input("inv-filters-store", "data"),
    prevent_initial_call=True,
)
def _update_clear_button(filters):
    """Color Clear button if any filter active."""
    if not filters:
        return "secondary", True
    slicer_fields = ["group_category", "variety", "pack", "grade", "size", "final_stage_status", "grower_number", "run_type"]
    active = any(filters.get(field) is not None for field in slicer_fields)
    if active:
        return "primary", False
    return "secondary", True

#region Debug instrumentation (session 6d818d) - do not remove until verification
import json
import time

log_path = "debug-6d818d.log"
timestamp = int(time.time() * 1000)
log_entry = {
    "sessionId": "6d818d",
    "id": f"log_{timestamp}_inv_import",
    "timestamp": timestamp,
    "location": "callbacks/inventory.py:module_end",
    "message": "Inventory callbacks module fully parsed and loaded (syntax OK post-fix)",
    "data": {
        "line_256_indent": "confirmed_correct"
    },
    "runId": "post_indent_fix",
    "hypothesisId": "A"
}
with open(log_path, "a") as f:
    f.write(json.dumps(log_entry) + "\n")
#endregion

