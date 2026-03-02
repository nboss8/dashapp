"""
TV Display - layout only. Data logic in services/tv_data.py, callbacks in callbacks/tv.py.
"""
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

from components.page_header import page_header
from services.tv_data import get_date_dropdown_options, _empty_figure

load_dotenv()
dash.register_page(__name__, path="/tv", name="TV Display")

layout = html.Div([
    dcc.Interval(id="tv-interval", interval=300_000, n_intervals=0),
    dcc.Store(id="tv-date-store", data=None),

    html.Div(id="tv-main-block", children=[
        page_header(
            html.H5(id="tv-header", className="tv-main-header", style={
                "color": "white", "margin": "0", "fontSize": "1.5rem", "fontWeight": "700",
                "textAlign": "center",
            }),
            back_href="/",
            right_slot=html.Div([
                html.Button("⛶ Fullscreen", id="tv-fullscreen-btn", className="tv-fullscreen-btn", title="Toggle fullscreen (like F11)"),
                dcc.Loading(
                    [
                        dcc.Dropdown(
                            id="tv-date-dropdown",
                            options=get_date_dropdown_options(),
                            value=None,
                            clearable=False,
                            placeholder="Select date",
                            className="tv-date-dropdown",
                            style={"minWidth": "140px"},
                        ),
                        html.P(id="tv-last-updated", style={
                            "color": "#fff", "margin": "0", "marginTop": "4px",
                            "fontSize": "0.75rem", "textAlign": "right"
                        }),
                    ],
                    type="circle",
                    color="white",
                    fullscreen=False,
                    style={"minHeight": "40px"},
                ),
            ], className="tv-header-right", style={"display": "flex", "alignItems": "center", "gap": "10px", "flexWrap": "wrap"}),
        ),

        html.Div([
            dbc.Row(id="tv-cards-row", className="g-2 mb-2 flex-shrink-0", children=[]),
            dbc.Row([
                dbc.Col(dcc.Graph(
                    id="tv-ppmh-chart",
                    figure=_empty_figure("Packs Per Man Hour"),
                    config={"displayModeBar": False, "displaylogo": False, "showLink": False},
                ), width=12),
            ], className="mb-1 tv-chart-row"),
            dbc.Row([
                dbc.Col(dcc.Graph(
                    id="tv-bph-chart",
                    figure=_empty_figure("Bins Per Hour"),
                    config={"displayModeBar": False, "displaylogo": False, "showLink": False},
                ), width=12),
            ], className="mb-3 tv-chart-row"),
            html.Div(id="tv-runs-section", className="tv-runs-section", children=[
                html.P("Loading…", style={"color": "#999", "textAlign": "center"}),
            ]),
        ], className="g-2", style={
            "flex": "1 1 0", "minHeight": 0, "display": "flex", "flexDirection": "column",
            "overflow": "hidden",
        }),

    ], style={
        "padding": "10px 14px",
        "flex": "1 1 0",
        "minHeight": 0,
        "overflow": "hidden",
        "overflowX": "hidden",
        "display": "flex",
        "flexDirection": "column",
        "boxSizing": "border-box",
    }),

], className="tv-root tv-page", style={"backgroundColor": "#1a1a1a", "height": "100vh", "maxHeight": "100vh", "overflow": "hidden", "overflowX": "hidden", "display": "flex", "flexDirection": "column"})
