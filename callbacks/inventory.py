"""
Pallet Inventory - all callbacks.
"""
import pandas as pd
from dash import callback, Input, Output, State, html, no_update, ctx, dcc

from components.ag_grid_table import create_ag_grid_table
from services.inventory_data import (
    get_pivot_data,
    get_sku_detail,
    get_sku_total_count,
    get_changes_today,
    filters_from_store,
    inv_cache,
    inv_cache_lock,
    build_inv_payload,
    filters_to_cache_key,
    _evict_inv_cache_if_needed,
)

_INV_DROPDOWN_IDS = [
    "inv-filter-group",
    "inv-filter-variety",
    "inv-filter-pack",
    "inv-filter-grade",
    "inv-filter-size",
    "inv-filter-stage",
]


def _store_from_dropdowns(g, v, p, gr, s, st):
    return {
        "group_category": None if g == "ALL" else g,
        "variety": None if v == "ALL" else v,
        "pack": None if p == "ALL" else p,
        "grade": None if gr == "ALL" else gr,
        "size": None if s == "ALL" else s,
        "pool": None,
        "process_code": None,
        "final_stage_status": None if st == "ALL" else st,
    }


def _build_pivot_table(df, use_eq):
    """Build Variety x Week pivot as HTML table."""
    if df is None or df.empty:
        return html.P("No data available. Run the DT script and dbt models first.", className="text-center text-muted p-4")
    measure = "EQ_ON_HAND" if use_eq else "CARTONS"
    pivot = df.pivot_table(index="VARIETY_ABBR", columns="WEEK_BUCKET", values=measure, aggfunc="sum", fill_value=0)
    pivot["Total"] = pivot.sum(axis=1)
    pivot = pivot.reset_index()
    cols = ["VARIETY_ABBR"] + [c for c in pivot.columns if c != "VARIETY_ABBR"]
    header = html.Tr([html.Th(c, style=_th_style) for c in cols])
    rows = []
    for _, r in pivot.iterrows():
        cells = [html.Td(str(r[c]) if pd.notna(r[c]) else "0", style=_td_style) for c in cols]
        rows.append(html.Tr(cells))
    total_row = html.Tr([html.Td("Total", style={**_td_style, "fontWeight": "600"})] + [html.Td(int(pivot[c].sum()), style=_td_style) for c in cols[1:]])
    rows.append(total_row)
    return html.Table([
        html.Thead(header),
        html.Tbody(rows),
    ], style={"width": "100%", "borderCollapse": "collapse", "fontSize": "0.85rem"})


_th_style = {"padding": "6px 8px", "textAlign": "center", "color": "#fff", "backgroundColor": "#2a2a2a", "border": "1px solid #444", "fontSize": "0.8rem"}
_td_style = {"padding": "4px 8px", "color": "#ddd", "backgroundColor": "#1a1a1a", "border": "1px solid #333", "fontSize": "0.8rem"}


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
    Output("inv-filters-store", "data"),
    Input("inv-filter-group", "value"),
    Input("inv-filter-variety", "value"),
    Input("inv-filter-pack", "value"),
    Input("inv-filter-grade", "value"),
    Input("inv-filter-size", "value"),
    Input("inv-filter-stage", "value"),
)
def _sync_filters(g, v, p, gr, s, st):
    return _store_from_dropdowns(g, v, p, gr, s, st)


def _render_inventory_from_payload(payload, use_eq, sku_page):
    """Build UI outputs from cached or fresh payload."""
    packed, shipped, staged = _build_changes_display(payload["changes_df"], use_eq)
    pivot_table = _build_pivot_table(payload["pivot_df"], use_eq)
    total = payload["total"]
    page = sku_page or 1
    page_size = 50
    sku_full = payload.get("sku_full_df")
    # Use cached SKU slice if we have full df and page is in range
    if sku_full is not None and not sku_full.empty:
        start = (page - 1) * page_size
        end = start + page_size
        if start < len(sku_full):
            sku_df = sku_full.iloc[start:end]
        else:
            sku_df = pd.DataFrame()
    else:
        sku_df = pd.DataFrame()
    if sku_df is None or sku_df.empty:
        sku_table = html.P("No data available", className="text-center text-muted p-4")
    else:
        display_cols = ["SKU", "CARTONS", "EQ_ON_HAND"]
        if use_eq:
            display_cols = ["SKU", "EQ_ON_HAND", "CARTONS"]
        sku_display = sku_df[display_cols] if all(c in sku_df.columns for c in display_cols) else sku_df
        sku_table = create_ag_grid_table(
            sku_display,
            id_prefix="inv-sku",
            export_filename="pallet_inventory_sku.csv",
            pinned_cols=1,
            page_size=50,
            pagination=True,
            preserve_numeric_columns=["CARTONS", "EQ_ON_HAND"],
        )
    num_pages = max(1, (total + 49) // 50)
    pagination = html.Div([
        html.Span(f"Page {sku_page or 1} of {num_pages} ({total} SKUs)", style={"color": "#aaa", "fontSize": "0.85rem"}),
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
    Input("inv-interval", "n_intervals"),
    Input("inv-filters-store", "data"),
    Input("inv-metric-toggle", "value"),
    Input("inv-sku-page", "data"),
)
def _update_inventory(_n_intervals, filters, metric, sku_page):
    # No pathname guard: when on Pallet Inventory page, all Inputs exist and callback fires
    use_eq = metric == "eqs"
    f = filters_from_store(filters) if filters else {}
    cache_key = (filters_to_cache_key(f), use_eq)
    with inv_cache_lock:
        cached = inv_cache.get(cache_key)
    if cached is not None:
        page = sku_page or 1
        total = cached["total"]
        sku_full = cached.get("sku_full_df")
        start = (page - 1) * 50
        # Page beyond cached SKU range: fetch this page live, keep changes/pivot from cache
        if sku_full is not None and not sku_full.empty and start < len(sku_full):
            return _render_inventory_from_payload(cached, use_eq, sku_page)
        # Beyond cache or no sku cache: fetch SKU page live
        sku_df_live = get_sku_detail(f, page=page, page_size=50, use_eq=use_eq)
        packed, shipped, staged = _build_changes_display(cached["changes_df"], use_eq)
        pivot_table = _build_pivot_table(cached["pivot_df"], use_eq)
        if sku_df_live is None or sku_df_live.empty:
            sku_table = html.P("No data available", className="text-center text-muted p-4")
        else:
            display_cols = ["SKU", "EQ_ON_HAND", "CARTONS"] if use_eq else ["SKU", "CARTONS", "EQ_ON_HAND"]
            sku_display = sku_df_live[display_cols] if all(c in sku_df_live.columns for c in display_cols) else sku_df_live
            sku_table = create_ag_grid_table(
                sku_display, id_prefix="inv-sku", export_filename="pallet_inventory_sku.csv",
                pinned_cols=1, page_size=50, pagination=True,
                preserve_numeric_columns=["CARTONS", "EQ_ON_HAND"],
            )
        num_pages = max(1, (total + 49) // 50)
        pagination = html.Div([
            html.Span(f"Page {page} of {num_pages} ({total} SKUs)", style={"color": "#aaa", "fontSize": "0.85rem"}),
            html.Div([
                html.Button("Prev", id="inv-sku-prev", className="btn btn-sm btn-outline-secondary me-1", disabled=page <= 1),
                html.Button("Next", id="inv-sku-next", className="btn btn-sm btn-outline-secondary", disabled=page >= num_pages),
            ], className="d-inline ms-2"),
        ])
        return packed, shipped, staged, pivot_table, sku_table, pagination
    payload = build_inv_payload(f, use_eq)
    with inv_cache_lock:
        _evict_inv_cache_if_needed()
        inv_cache[cache_key] = payload
    return _render_inventory_from_payload(payload, use_eq, sku_page)


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
    State("inv-sku-page", "data"),
    prevent_initial_call=True,
)
def _sku_page_control(prev_clicks, next_clicks, g, v, p, gr, s, st, page):
    tid = ctx.triggered_id
    if not tid:
        return no_update
    p = page or 1
    if tid in ("inv-filter-group", "inv-filter-variety", "inv-filter-pack", "inv-filter-grade", "inv-filter-size", "inv-filter-stage"):
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
        measure = "EQ_ON_HAND" if use_eq else "CARTONS"
        pivot = df.pivot_table(index="VARIETY_ABBR", columns="WEEK_BUCKET", values=measure, aggfunc="sum", fill_value=0)
        pivot["Total"] = pivot.sum(axis=1)
        pivot = pivot.reset_index()
        return dcc.send_data_frame(pivot.to_csv, "pallet_inventory_pivot.csv", index=False)
    if tid == "inv-export-sku-btn":
        df = get_sku_detail(f, page=sku_page or 1, page_size=10000, use_eq=use_eq)
        if df is None or df.empty:
            return no_update
        return dcc.send_data_frame(df.to_csv, "pallet_inventory_sku.csv", index=False)
    return no_update
