"""
Caching - view cache status, manually refresh caches.
"""
import dash
from dash import html, dcc, callback, Input, Output, ctx, no_update, ALL
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

from components.page_header import page_header
from services.cache_manager import get_cache_status, trigger_manual_refresh

load_dotenv()
dash.register_page(__name__, path="/caching", name="Caching")

layout = html.Div([
    dcc.Interval(id="caching-interval", interval=60_000, n_intervals=0),
    html.Div(id="caching-toast-container", style={"position": "fixed", "top": "80px", "right": "16px", "zIndex": 9999}),
    dbc.Container([
        page_header("Caching", "/"),
        html.P(
            "Cache status for reports. Click Refresh to manually rebuild a cache.",
            style={"color": "#aaa", "fontSize": "0.85rem", "marginBottom": "12px"},
        ),
        dcc.Loading(
            html.Div(id="caching-status", className="mt-4"),
            type="circle",
            color="#64B5F6",
        ),
    ], fluid=True, className="py-3"),
], style={"backgroundColor": "#1a1a1a", "minHeight": "100vh", "color": "#ddd"})


def _build_sections(status):
    """Build cache sections with refresh buttons."""
    sections = []
    for slug, entries in sorted(status.items()):
        items = [
            html.Li(f"{k}: cached at {cached_at}", style={"marginBottom": "6px"})
            for k, cached_at in sorted(entries.items())
        ]
        sections.append(html.Div([
            html.Div([
                html.H5(f"{slug.upper()} cache", style={"color": "#fff", "marginBottom": "12px", "marginRight": "12px"}),
                dbc.Button("Refresh", id={"type": "cache-refresh-btn", "slug": slug}, color="outline-light", size="sm", className="align-self-center"),
            ], className="d-flex align-items-center flex-wrap", style={"marginBottom": "8px"}),
            html.Ul(items if items else [html.Li("No keys")], style={"listStyle": "none", "paddingLeft": 0}),
        ], style={"marginBottom": "24px"}))
    return html.Div(sections if sections else [html.P("No cache data", style={"color": "#999"})])


@callback(
    Output("caching-status", "children"),
    Input("caching-interval", "n_intervals"),
)
def update_caching_status(_n):
    status = get_cache_status()
    return _build_sections(status)


@callback(
    Output("caching-status", "children", allow_duplicate=True),
    Output("caching-toast-container", "children"),
    Input({"type": "cache-refresh-btn", "slug": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def on_refresh_click(_n):
    tid = ctx.triggered_id
    if not tid or tid.get("type") != "cache-refresh-btn":
        return no_update, no_update
    slug = tid.get("slug", "")
    success, msg = trigger_manual_refresh(slug)
    status = get_cache_status()
    toast = dbc.Toast(
        f"{slug.upper()}: {msg}",
        header="Success" if success else "Error",
        is_open=True,
        duration=4000,
        color="success" if success else "danger",
        style={"position": "fixed", "top": 80, "right": 16},
    )
    return _build_sections(status), toast
