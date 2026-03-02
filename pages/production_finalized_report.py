"""
Production Finalized Report — Interactive version of the Grower Production Summary PDF.
Layout only. Data logic in services/pfr_data.py, callbacks in callbacks/pfr.py.
"""
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from datetime import datetime
from dotenv import load_dotenv

from components.page_header import page_header

load_dotenv()
dash.register_page(__name__, path="/production/finalized-report", name="Production Finalized Report")

layout = html.Div([
    dcc.Interval(id="pfr-interval", interval=900_000, n_intervals=0),
    dbc.Container([
        page_header("Production Finalized Report", "/", right_slot=None),
        html.P(
            "View verified production summaries by date and grower/variety/pool. Same data as the PDF report.",
            style={"color": "#aaa", "marginBottom": "16px"},
        ),
        dbc.Row([
            dbc.Col([
                html.Label("Report Date", style={"color": "#ccc", "fontSize": "0.85rem"}),
                dcc.DatePickerSingle(
                    id="pfr-date",
                    date=datetime.now().date(),
                    display_format="YYYY-MM-DD",
                    style={"width": "100%"},
                ),
            ], width=2),
            dbc.Col([
                html.Label("Group (Grower — Variety — Pool)", style={"color": "#ccc", "fontSize": "0.85rem"}),
                dcc.Dropdown(id="pfr-group-dropdown", placeholder="Select a group", className="tv-date-dropdown"),
            ], width=4),
            dbc.Col([
                html.Label("\u00a0", style={"color": "#ccc", "fontSize": "0.85rem"}),
                html.Div([
                    dbc.Button("Generate PDF", id="pfr-generate-pdf-btn", color="primary", size="md", className="me-2 mb-1"),
                    dbc.Button("Generate All PDFs for Date", id="pfr-generate-all-pdf-btn", color="secondary", size="md", outline=True, className="mb-1"),
                ]),
            ], width=3, className="d-flex flex-column justify-content-end"),
        ], className="mb-4 g-3"),
        html.Div([
            html.Div(id="pfr-pdf-error", style={"color": "#e74c3c", "fontSize": "0.85rem", "marginTop": "4px"}),
            html.Div(id="pfr-zip-error", style={"color": "#e74c3c", "fontSize": "0.85rem", "marginTop": "4px"}),
        ]),
        html.Div(id="pfr-report-content"),
    ], fluid=True, className="py-4"),
], className="tv-root", style={"backgroundColor": "#1a1a1a", "minHeight": "100vh"})
