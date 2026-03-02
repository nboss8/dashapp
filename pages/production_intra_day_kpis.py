"""
Production Intra Day KPIs - PTRUN-driven report.
Layout only. Data logic in services/pidk_data.py, callbacks in callbacks/pidk.py.
"""
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

from components.page_header import page_header
from services.pidk_data import get_day_label_options

load_dotenv()
dash.register_page(__name__, path="/production/intra-day-kpis", name="Production Intra Day KPIs")

# Inline SVG icons (layout-only)
_PIDK_ICON_EXPAND = html.Img(
    src="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' fill='%23fff' viewBox='0 0 16 16'%3E%3Cpath d='M1.5 1a.5.5 0 0 0-.5.5v4a.5.5 0 0 1-1 0v-4A1.5 1.5 0 0 1 1.5 0h4a.5.5 0 0 1 0 1h-4zM10 .5a.5.5 0 0 1 .5-.5h4A1.5 1.5 0 0 1 16 1.5v4a.5.5 0 0 1-1 0v-4a.5.5 0 0 0-.5-.5h-4a.5.5 0 0 1-.5-.5zM.5 10a.5.5 0 0 1 .5.5v4a.5.5 0 0 0 .5.5h4a.5.5 0 0 1 0 1h-4A1.5 1.5 0 0 1 0 14.5v-4a.5.5 0 0 1 .5-.5zm15 0a.5.5 0 0 1 .5.5v4a1.5 1.5 0 0 1-1.5 1.5h-4a.5.5 0 0 1 0-1h4a.5.5 0 0 0 .5-.5v-4a.5.5 0 0 1 .5-.5z'/%3E%3C/svg%3E",
    style={"width": "14px", "height": "14px", "display": "inline-block", "verticalAlign": "middle"},
)

_header_right = dcc.Loading(
    [
        dcc.Dropdown(
            id="pidk-day-label-dropdown",
            options=get_day_label_options(),
            value="TODAY",
            clearable=False,
            placeholder="Select date",
            className="tv-date-dropdown",
            style={"minWidth": "140px"},
        ),
        html.P(id="pidk-last-updated", style={
            "color": "#fff", "margin": "0", "marginTop": "2px",
            "fontSize": "0.7rem", "textAlign": "right",
        }),
    ],
    type="circle",
    color="white",
    fullscreen=False,
    style={"minHeight": "28px"},
)

layout = html.Div([
    dcc.Interval(id="pidk-interval", interval=300_000, n_intervals=0),
    dcc.Store(id="pidk-day-store", data="TODAY"),
    dcc.Store(id="pidk-selected-run", data=None),
    dcc.Store(id="pidk-selected-shift", data=None),
    dcc.Store(id="pidk-selected-package-type", data=None),
    dcc.Store(id="pidk-expanded-tile", data=None),
    dcc.Store(id="pidk-run-data", data=[]),
    dcc.Store(id="pidk-shift-data", data=[]),
    dbc.Container([
        html.Div(
            page_header("Production Intra Day KPIs", "/", right_slot=_header_right),
            className="pidk-page-header-wrap",
        ),
        dbc.Row([
            dbc.Col([
                html.Div([
                    dbc.Card([
                        dbc.CardHeader([
                            html.Div(html.Span("Run Totals"), className="pidk-card-title"),
                            html.Div([
                                dbc.Button("Export CSV", id="pidk-run-totals-export-btn", color="secondary", size="sm", outline=True, className="me-1"),
                                html.Button(_PIDK_ICON_EXPAND, id={"type": "pidk-expand-btn", "index": "run-totals"}, className="pidk-expand-btn"),
                            ], className="pidk-card-actions d-flex align-items-center gap-1"),
                        ], className="pidk-card-header pidk-card-header-centered"),
                        dbc.CardBody([
                            html.Div([
                                html.Span("Slice by Run ", style={"color": "#aaa", "fontSize": "0.8rem", "marginRight": "6px"}),
                                dcc.Dropdown(
                                    id="pidk-run-slicer",
                                    options=[{"label": "All", "value": "ALL"}],
                                    value="ALL",
                                    clearable=False,
                                    className="tv-date-dropdown",
                                    style={"minWidth": "160px"},
                                ),
                            ], style={"marginBottom": "8px", "display": "flex", "alignItems": "center"}),
                            dcc.Loading(
                                html.Div(id="pidk-run-totals-table", className="pidk-table-wrapper"),
                                type="circle",
                                color="#64B5F6",
                                delay_show=180,
                                fullscreen=False,
                            ),
                        ], className="pidk-card-body p-0"),
                    ], className="pidk-table-card"),
                ], id={"type": "pidk-tile-wrapper", "index": "run-totals"}, className="pidk-tile-wrapper"),
            ], width=6),
            dbc.Col([
                html.Div([
                    dbc.Card([
                        dbc.CardHeader([
                            html.Div(html.Span("Shift Totals"), className="pidk-card-title"),
                            html.Div([
                                dbc.Button("Export CSV", id="pidk-shift-totals-export-btn", color="secondary", size="sm", outline=True, className="me-1"),
                                html.Button(_PIDK_ICON_EXPAND, id={"type": "pidk-expand-btn", "index": "shift-totals"}, className="pidk-expand-btn"),
                            ], className="pidk-card-actions d-flex align-items-center gap-1"),
                        ], className="pidk-card-header pidk-card-header-centered"),
                        dbc.CardBody([
                            html.Div([
                                html.Span("Slice by Shift ", style={"color": "#aaa", "fontSize": "0.8rem", "marginRight": "6px"}),
                                dcc.Dropdown(
                                    id="pidk-shift-slicer",
                                    options=[{"label": "All", "value": "ALL"}],
                                    value="ALL",
                                    clearable=False,
                                    className="tv-date-dropdown",
                                    style={"minWidth": "160px"},
                                ),
                            ], style={"marginBottom": "8px", "display": "flex", "alignItems": "center"}),
                            dcc.Loading(
                                html.Div(id="pidk-shift-totals-table", className="pidk-table-wrapper"),
                                type="circle",
                                color="#64B5F6",
                                delay_show=180,
                                fullscreen=False,
                            ),
                        ], className="pidk-card-body p-0"),
                    ], className="pidk-table-card"),
                ], id={"type": "pidk-tile-wrapper", "index": "shift-totals"}, className="pidk-tile-wrapper"),
            ], width=6),
        ]),
        dbc.Row([
            dbc.Col([
                html.Div([
                    dbc.Card([
                        dbc.CardHeader([
                            html.Div(html.Span("Bin Per Hour By Grower"), className="pidk-card-title"),
                            html.Button(_PIDK_ICON_EXPAND, id={"type": "pidk-expand-btn", "index": "bph"}, className="pidk-expand-btn"),
                        ], className="pidk-card-header pidk-card-header-centered"),
                        dbc.CardBody([
                            html.Div(id="pidk-sliced-label-bph", className="pidk-sliced-label", style={"marginBottom": "6px", "minHeight": "18px"}),
                            dcc.Loading(
                                html.Div(
                                    dcc.Graph(id="pidk-bph-chart", config={"displayModeBar": False, "displaylogo": False},
                                              style={"width": "100%", "height": "320px"}),
                                    className="pidk-bph-chart-wrapper pidk-table-wrapper",
                                ),
                                type="circle",
                                color="#64B5F6",
                                delay_show=180,
                                fullscreen=False,
                            ),
                        ], className="pidk-card-body"),
                    ], className="pidk-table-card"),
                ], id={"type": "pidk-tile-wrapper", "index": "bph"}, className="pidk-tile-wrapper"),
                html.Div([
                    dbc.Card([
                        dbc.CardHeader([
                            html.Div([
                                html.Span("Sizer Profile", style={"marginRight": "1rem"}),
                                html.Span("Event / Batch ", style={"color": "#aaa", "fontSize": "0.8rem", "marginRight": "6px"}),
                                dcc.Dropdown(
                                    id="pidk-sizer-event-dropdown",
                                    options=[],
                                    value=None,
                                    clearable=False,
                                    placeholder="Select event or batch",
                                    className="tv-date-dropdown",
                                    style={"minWidth": "140px"},
                                ),
                                html.Span("Packout Group ", style={"color": "#aaa", "fontSize": "0.8rem", "marginLeft": "12px", "marginRight": "6px"}),
                                dcc.Dropdown(
                                    id="pidk-sizer-packout-dropdown",
                                    options=[
                                        {"label": "packed", "value": "packed"},
                                        {"label": "Culls", "value": "Culls"},
                                        {"label": "All", "value": "All"},
                                    ],
                                    value="packed",
                                    clearable=False,
                                    className="tv-date-dropdown",
                                    style={"minWidth": "100px"},
                                ),
                            ], className="pidk-card-title d-flex flex-wrap align-items-center justify-content-center gap-2"),
                            html.Button(_PIDK_ICON_EXPAND, id={"type": "pidk-expand-btn", "index": "sizer"}, className="pidk-expand-btn"),
                        ], className="pidk-card-header pidk-card-header-centered"),
                        dbc.CardBody([
                            html.Div(id="pidk-sliced-label-sizer", className="pidk-sliced-label", style={"marginBottom": "6px", "minHeight": "18px"}),
                            dcc.Loading(
                                html.Div(id="pidk-sizer-matrix", className="pidk-table-wrapper pidk-sizer-matrix-wrapper"),
                                type="circle",
                                color="#64B5F6",
                                delay_show=180,
                                fullscreen=False,
                            ),
                        ], className="pidk-card-body"),
                    ], className="pidk-table-card"),
                ], id={"type": "pidk-tile-wrapper", "index": "sizer"}, className="pidk-tile-wrapper"),
            ], width=8, className="d-flex flex-column gap-3"),
            dbc.Col([
                html.Div([
                    dbc.Card([
                        dbc.CardHeader([
                            html.Div(html.Span("Employee Count"), className="pidk-card-title"),
                            html.Button(_PIDK_ICON_EXPAND, id={"type": "pidk-expand-btn", "index": "employee"}, className="pidk-expand-btn"),
                        ], className="pidk-card-header pidk-card-header-centered"),
                        dbc.CardBody([
                            html.Div(id="pidk-sliced-label-employee", className="pidk-sliced-label", style={"marginBottom": "6px", "minHeight": "18px"}),
                            dcc.Loading(
                                html.Div(id="pidk-employee-summary", className="pidk-table-wrapper"),
                                type="circle",
                                color="#64B5F6",
                                delay_show=180,
                                fullscreen=False,
                            ),
                        ], className="pidk-card-body p-0"),
                    ], className="pidk-table-card"),
                ], id={"type": "pidk-tile-wrapper", "index": "employee"}, className="pidk-tile-wrapper"),
                html.Div([
                    dbc.Card([
                        dbc.CardHeader([
                            html.Div(html.Span("Computech Carton Palletized"), className="pidk-card-title"),
                            html.Button(_PIDK_ICON_EXPAND, id={"type": "pidk-expand-btn", "index": "computech"}, className="pidk-expand-btn"),
                        ], className="pidk-card-header pidk-card-header-centered"),
                        dbc.CardBody([
                            html.Div(id="pidk-sliced-label-eq", className="pidk-sliced-label", style={"marginBottom": "6px", "minHeight": "18px"}),
                            dcc.Loading(
                                html.Div(id="pidk-eq-matrix", className="pidk-table-wrapper"),
                                type="circle",
                                color="#64B5F6",
                                delay_show=180,
                                fullscreen=False,
                            ),
                            html.Div("Package Type", style={
                                "color": "#ddd",
                                "fontSize": "0.8rem",
                                "fontWeight": "600",
                                "marginTop": "10px",
                                "marginBottom": "6px",
                            }),
                            dcc.Loading(
                                html.Div(id="pidk-package-type-table", className="pidk-table-wrapper"),
                                type="circle",
                                color="#64B5F6",
                                delay_show=180,
                                fullscreen=False,
                            ),
                        ], className="pidk-card-body p-0"),
                    ], className="pidk-table-card"),
                ], id={"type": "pidk-tile-wrapper", "index": "computech"}, className="pidk-tile-wrapper"),
            ], width=4, className="d-flex flex-column gap-3"),
        ], className="mt-2 g-2"),
    ], fluid=True, className="py-1"),
], className="tv-root pidk-root", style={"backgroundColor": "#1a1a1a", "minHeight": "100vh", "paddingTop": "0.15rem"})
