from dash import html
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import pandas as pd


def create_ag_grid_table(
    df: pd.DataFrame,
    id_prefix: str,
    export_filename: str,
    pinned_cols: int = 4,
    page_size: int = 12,
    hidden_columns: list = None,
    cell_class_rules: dict = None,
    cell_style_rules: dict = None,
    header_names: dict = None,
    preserve_numeric_columns: list = None,
    pagination: bool = False,
):
    """Reusable AG Grid table with dark theme, pinned columns, export, and mobile support."""
    if df is None or df.empty:
        return html.P("No data available", className="text-center text-muted p-5")

    hidden_columns = hidden_columns or []
    cell_class_rules = cell_class_rules or {}
    cell_style_rules = cell_style_rules or {}
    header_names = header_names or {}
    preserve_numeric_columns = set(preserve_numeric_columns or [])

    column_defs = []
    for i, col in enumerate(df.columns):
        header_label = header_names.get(col, col.replace("_", " ").title())
        col_def = {
            "field": col,
            "headerName": header_label,
            "sortable": True,
            "filter": True,
            "resizable": True,
            "minWidth": 95,
            "wrapHeaderText": True,
            "autoHeaderHeight": True,
        }
        if col == "sku":
            col_def["width"] = 250
        if col in hidden_columns:
            col_def["hide"] = True
        elif i < pinned_cols:
            col_def["pinned"] = "left"
            col_def["lockPinned"] = True
        if col in cell_class_rules:
            col_def["cellClassRules"] = cell_class_rules[col]
        if col in cell_style_rules:
            col_def["cellStyle"] = cell_style_rules[col]
        if col in preserve_numeric_columns:
            # Round to 1 decimal; show whole numbers without .0 (body must be inside function so return is legal)
            col_def["valueFormatter"] = {
                "function": "function(params) { var n = params.value; if (n == null || n === undefined) return '—'; n = Number(n); return (n % 1 === 0) ? String(n) : n.toFixed(1); }"
            }
        column_defs.append(col_def)

    # Only fill NaN with "—" for columns not used in cellClassRules, so numeric columns stay numbers
    display_df = df.copy()
    for c in display_df.columns:
        if c not in preserve_numeric_columns:
            display_df[c] = display_df[c].fillna("—")

    grid_opts = {
        "theme": "legacy",
        "domLayout": "autoHeight",
        "rowSelection": {"mode": "singleRow", "checkboxes": False, "enableClickSelection": True},
    }
    if pagination:
        grid_opts["pagination"] = True
        grid_opts["paginationPageSize"] = page_size
        grid_opts["paginationPageSizeSelector"] = False

    return dbc.Container([
        dbc.Row([
            dbc.Col(
                dag.AgGrid(
                    id=f"{id_prefix}-grid",
                    rowData=display_df.to_dict("records"),
                    columnDefs=column_defs,
                    defaultColDef={
                        "sortable": True,
                        "filter": True,
                        "resizable": True,
                        "suppressHeaderFilterButton": True,
                        "suppressHeaderMenuButton": True,
                        "wrapHeaderText": True,
                        "autoHeaderHeight": True,
                    },
                    columnSize="sizeToFit",
                    dashGridOptions=grid_opts,
                    className="ag-theme-alpine-dark",
                    style={"width": "100%"},
                ),
                width=12,
            )
        ]),
        dbc.Row([
            dbc.Col(
                dbc.Button(
                    "Export CSV",
                    id=f"{id_prefix}-export-btn",
                    color="secondary",
                    size="sm",
                    outline=True,
                    className="mt-2",
                ),
                width=12,
                className="text-end",
            )
        ]),
    ], fluid=True, className="p-0")
