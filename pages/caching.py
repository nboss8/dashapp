"""
Caching - view cache status for PIDK and other reports.
"""
import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

from components.page_header import page_header
from services.cache_manager import get_cache_status

load_dotenv()
dash.register_page(__name__, path="/caching", name="Caching")

layout = html.Div([
    dcc.Interval(id="caching-interval", interval=60_000, n_intervals=0),
    dbc.Container([
        page_header("Caching", "/"),
        html.Div(id="caching-status", className="mt-4"),
    ], fluid=True, className="py-3"),
], style={"backgroundColor": "#1a1a1a", "minHeight": "100vh", "color": "#ddd"})


@callback(
    Output("caching-status", "children"),
    Input("caching-interval", "n_intervals"),
)
def update_caching_status(_n):
    status = get_cache_status()
    sections = []
    for slug, entries in sorted(status.items()):
        items = [
            html.Li(f"{k}: cached at {cached_at}", style={"marginBottom": "6px"})
            for k, cached_at in sorted(entries.items())
        ]
        sections.append(html.Div([
            html.H5(f"{slug.upper()} cache", style={"color": "#fff", "marginBottom": "12px"}),
            html.Ul(items if items else [html.Li("No keys")], style={"listStyle": "none", "paddingLeft": 0}),
        ], style={"marginBottom": "24px"}))
    return html.Div(sections if sections else [html.P("No cache data", style={"color": "#999"})])
