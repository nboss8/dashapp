import dash
from dash import html
import dash_bootstrap_components as dbc

from components.page_header import page_header

dash.register_page(__name__, path="/forms", name="Forms")

layout = dbc.Container([
    page_header("Forms", back_href="/"),
    dbc.Card([
        dbc.CardBody([
            html.P("In Development", className="text-muted text-center mb-0", style={"fontSize": "1.1rem"}),
        ], className="p-5 text-center"),
    ], style={"backgroundColor": "#1a1a1a", "borderColor": "#333"}),
], fluid=True)
