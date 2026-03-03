"""
Packed Inventory Trends - on-hand over time, YOY, aging risk.
Data: services/trends_data.py. Callbacks: callbacks/trends.py.
"""
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

from components.page_header import page_header

load_dotenv()
dash.register_page(__name__, path="/production/packed-inventory-trends", name="Packed Inventory Trends")

_DEFAULT_OPTS = [{"label": "All", "value": "ALL"}]
_CROP_OPTS = [
    {"label": "APPLES", "value": "AP"},
    {"label": "ORGANIC APPLES", "value": "OA"},
    {"label": "CHERRIES", "value": "CH"},
    {"label": "ORGANIC CHERRIES", "value": "OC"},
]
opts = {k: _DEFAULT_OPTS for k in ["source", "crop_year", "variety_abbr", "report_group", "pool", "grower_number"]}
opts["group_category"] = _CROP_OPTS

_header_right = html.Div([
    html.Span("Show ", style={"color": "#aaa", "fontSize": "0.85rem", "marginRight": "6px"}),
    dcc.RadioItems(
        id="trends-metric-toggle",
        options=[{"label": "Cartons", "value": "cartons"}, {"label": "EQs", "value": "eqs"}],
        value="cartons",
        inline=True,
        style={"color": "#fff", "fontSize": "0.9rem"},
        inputStyle={"marginRight": "6px"},
        labelStyle={"marginRight": "12px", "color": "#fff"},
    ),
    html.Span("Compare ", style={"color": "#aaa", "fontSize": "0.85rem", "marginLeft": "16px", "marginRight": "6px"}),
    dcc.Dropdown(
        id="trends-yoy",
        options=[
            {"label": "None", "value": "none"},
            {"label": "Last Year", "value": "last_year"},
            {"label": "Two Years", "value": "two_years"},
        ],
        value="none",
        clearable=False,
        style={"minWidth": "140px"},
        className="trends-dropdown",
    ),
], className="d-flex align-items-center")

layout = html.Div([
    dcc.Interval(id="trends-interval", interval=900_000, n_intervals=0),
    dcc.Store(id="trends-expanded-tile", data=None),
    dcc.Store(id="trends-filters-store", data={
        "source": None,
        "group_category": None,
        "crop_year": None,
        "variety_abbr": None,
        "report_group": None,
        "pool": None,
        "grower_number": None,
    }),
    dcc.Download(id="trends-csv-download"),
    dbc.Container([
        page_header("Packed Inventory Trends", "/", right_slot=_header_right),
        html.P(
            "On-hand inventory over time with YOY comparison and aging risk. Data refreshes daily.",
            style={"color": "#aaa", "fontSize": "0.85rem", "marginBottom": "12px"},
        ),
        # Slicers
        dbc.Row([
            dbc.Col([html.Label("Crop", style={"color": "#ccc", "fontSize": "0.8rem"}), dcc.Dropdown(id="trends-filter-crop", options=opts["group_category"], value=[], multi=True, placeholder="All crops", className="trends-dropdown")], width=2),
            dbc.Col([html.Label("Source", style={"color": "#ccc", "fontSize": "0.8rem"}), dcc.Dropdown(id="trends-filter-source", options=opts["source"], value="ALL", clearable=False, className="trends-dropdown")], width=2),
            dbc.Col([html.Label("Crop Year", style={"color": "#ccc", "fontSize": "0.8rem"}), dcc.Dropdown(id="trends-filter-crop-year", options=opts["crop_year"], value="ALL", clearable=False, className="trends-dropdown")], width=2),
            dbc.Col([html.Label("Variety", style={"color": "#ccc", "fontSize": "0.8rem"}), dcc.Dropdown(id="trends-filter-variety", options=opts["variety_abbr"], value="ALL", clearable=False, className="trends-dropdown")], width=2),
            dbc.Col([html.Label("Report Group", style={"color": "#ccc", "fontSize": "0.8rem"}), dcc.Dropdown(id="trends-filter-report-group", options=opts["report_group"], value="ALL", clearable=False, className="trends-dropdown")], width=2),
            dbc.Col([html.Label("Pool", style={"color": "#ccc", "fontSize": "0.8rem"}), dcc.Dropdown(id="trends-filter-pool", options=opts["pool"], value="ALL", clearable=False, className="trends-dropdown")], width=1),
            dbc.Col([html.Label("Grower", style={"color": "#ccc", "fontSize": "0.8rem"}), dcc.Dropdown(id="trends-filter-grower", options=opts["grower_number"], value="ALL", clearable=False, className="trends-dropdown")], width=1),
            dbc.Col([
                dbc.Button("Clear Filters", id="trends-clear-filters", color="secondary", size="sm", outline=True, className="mt-4"),
            ], width=2),
        ], className="mb-3 g-2"),
        # Charts
        dbc.Row([
            dbc.Col([
                html.Div([
                    dbc.Card([
                        dbc.CardHeader([
                            html.Span("On-Hand Over Time"),
                            html.Div([
                                dbc.Button("Export CSV", id="trends-export-btn", color="outline-light", size="sm", className="me-1"),
                                html.Button("⛶", id="trends-expand-line", className="trends-expand-btn", title="Expand chart"),
                            ], className="ms-auto d-flex align-items-center"),
                        ], className="d-flex align-items-center"),
                        dbc.CardBody([
                            dcc.Loading(
                                dcc.Graph(id="trends-line-chart", config={"displayModeBar": True, "displaylogo": False}),
                                type="circle", color="#64B5F6",
                            ),
                        ]),
                    ], className="trends-chart-card"),
                ], id="trends-tile-line", className="trends-tile-wrapper"),
            ], width=12),
        ], className="mb-3"),
        dbc.Row([
            dbc.Col([
                html.Div([
                    dbc.Card([
                        dbc.CardHeader([
                            html.Span("Inventory by Age (Higher = Higher Risk)"),
                            html.Button("⛶", id="trends-expand-aging", className="trends-expand-btn ms-auto", title="Expand chart"),
                        ], className="d-flex align-items-center"),
                        dbc.CardBody([
                            dcc.Loading(
                                dcc.Graph(id="trends-aging-chart", config={"displayModeBar": True, "displaylogo": False}),
                                type="circle", color="#64B5F6",
                            ),
                        ]),
                    ], className="trends-chart-card"),
                ], id="trends-tile-aging", className="trends-tile-wrapper"),
            ], width=12),
        ], className="mb-3"),
    ], fluid=True, className="py-3"),
], className="trends-root", style={"backgroundColor": "#1a1a1a", "minHeight": "100vh"})
