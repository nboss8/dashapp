"""
Pallet Inventory - on-hand inventory by variety and week age.
Layout only. Data logic in services/inventory_data.py, callbacks in callbacks/inventory.py.
"""
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

from components.page_header import page_header

load_dotenv()
dash.register_page(__name__, path="/production/pallet-inventory", name="Pallet Inventory")

# Placeholder; real options loaded via callback when page renders (Snowflake ready)
_DEFAULT_OPTS = [{"label": "All", "value": "ALL"}]
opts = {k: _DEFAULT_OPTS for k in ["group_category", "variety", "pack", "grade", "size", "pool", "process_code", "final_stage_status"]}

_header_right = html.Div([
    html.Span("Show ", style={"color": "#aaa", "fontSize": "0.85rem", "marginRight": "6px"}),
    dcc.RadioItems(
        id="inv-metric-toggle",
        options=[{"label": "Cartons", "value": "cartons"}, {"label": "EQs", "value": "eqs"}],
        value="cartons",
        inline=True,
        style={"color": "#fff", "fontSize": "0.9rem"},
        inputStyle={"marginRight": "6px"},
        labelStyle={"marginRight": "12px", "color": "#fff"},
    ),
], className="d-flex align-items-center")

layout = html.Div([
    dcc.Interval(id="inv-interval", interval=900_000, n_intervals=0),
    dcc.Store(id="inv-filters-store", data={
        "group_category": None,
        "variety": None,
        "pack": None,
        "grade": None,
        "size": None,
        "pool": None,
        "process_code": None,
        "final_stage_status": None,
        "week_bucket": None,
    }),
    dcc.Store(id="inv-sku-page", data=1),
    dbc.Container([
        page_header("Pallet Inventory", "/", right_slot=_header_right),
        html.P(
            "On-hand inventory by variety and weeks age. Data refreshes every 15 min (dev).",
            style={"color": "#aaa", "fontSize": "0.85rem", "marginBottom": "12px"},
        ),
        # Filter bar
        dbc.Row([
            dbc.Col([html.Label("Group", style={"color": "#ccc", "fontSize": "0.8rem"}), dcc.Dropdown(id="inv-filter-group", options=opts["group_category"], value="ALL", clearable=False, className="inv-dropdown")], width=1),
            dbc.Col([html.Label("Variety", style={"color": "#ccc", "fontSize": "0.8rem"}), dcc.Dropdown(id="inv-filter-variety", options=opts["variety"], value="ALL", clearable=False, className="inv-dropdown")], width=1),
            dbc.Col([html.Label("Pack", style={"color": "#ccc", "fontSize": "0.8rem"}), dcc.Dropdown(id="inv-filter-pack", options=opts["pack"], value="ALL", clearable=False, className="inv-dropdown")], width=1),
            dbc.Col([html.Label("Grade", style={"color": "#ccc", "fontSize": "0.8rem"}), dcc.Dropdown(id="inv-filter-grade", options=opts["grade"], value="ALL", clearable=False, className="inv-dropdown")], width=1),
            dbc.Col([html.Label("Size", style={"color": "#ccc", "fontSize": "0.8rem"}), dcc.Dropdown(id="inv-filter-size", options=opts["size"], value="ALL", clearable=False, className="inv-dropdown")], width=1),
            dbc.Col([html.Label("Stage", style={"color": "#ccc", "fontSize": "0.8rem"}), dcc.Dropdown(id="inv-filter-stage", options=opts["final_stage_status"], value="ALL", clearable=False, className="inv-dropdown")], width=1),
        ], className="mb-3 g-2"),
        # Clear Filters button
        dbc.Row(
            dbc.Col(
                dbc.Button(
                    "Clear All Filters",
                    id="inv-clear-filters",
                    color="secondary",
                    size="sm",
                    outline=True,
                ),
                width=12,
                className="mb-3",
            ),
        ),
        # Hidden metric targets (values still computed for callbacks, but not shown as tiles)
        html.Div(
            [
                html.Div(id="inv-packed-value"),
                html.Div(id="inv-shipped-value"),
                html.Div(id="inv-staged-value"),
            ],
            style={"display": "none"},
        ),
        # Export buttons
        dcc.Download(id="inv-csv-download"),
        # Pivot and SKU
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Cartons, Variety by Weeks Age"),
                    dbc.CardBody([
                        html.Div(
                            dbc.Button(
                                "Export Pivot CSV",
                                id="inv-export-pivot-btn",
                                color="outline-light",
                                size="sm",
                            ),
                            className="text-end mb-2"
                        ),
                        dcc.Loading(
                            html.Div(id="inv-pivot-table", className="inv-table-wrapper"),
                            type="circle",
                            color="#64B5F6",
                            fullscreen=False,
                        ),
                    ]),
                ]),
            ], width=7),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("SKU Detail"),
                    dbc.CardBody([
                        html.Div(
                            dbc.Button(
                                "Export SKU CSV",
                                id="inv-export-sku-btn",
                                color="outline-light",
                                size="sm",
                            ),
                            className="text-end mb-2"
                        ),
                        dcc.Loading(
                            html.Div(id="inv-sku-table", className="inv-table-wrapper"),
                            type="circle",
                            color="#64B5F6",
                            fullscreen=False,
                        ),
                        html.Div(id="inv-sku-pagination", className="mt-2"),
                    ]),
                ]),
            ], width=5),
        ], className="g-3"),
    ], fluid=True, className="py-3"),
], className="pallet-root", style={"backgroundColor": "#1a1a1a", "minHeight": "100vh"})
