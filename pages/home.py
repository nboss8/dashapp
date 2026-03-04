import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

dash.register_page(__name__, path="/", name="Home")

layout = dbc.Container([
    dbc.Card([
        dbc.CardBody([
            html.H4("Columbia Fruit Analytics", className="mb-3"),
            html.P(
                "Inventory, production, and TV displays in one place.",
                className="text-muted mb-4",
            ),
            dcc.Link(
                dbc.Button("Open AI Assistant", color="primary", size="lg"),
                href="/ai-assistant",
                style={"textDecoration": "none"},
            ),
        ], className="p-5 text-center"),
    ], style={"backgroundColor": "#1a1a1a", "borderColor": "#333"}),
], fluid=True)
