"""
Shared page header: back arrow, centered title, right slot (dropdown + last updated).
Used by TV Display and Production Intra Day KPIs.
"""
from dash import html
import dash_bootstrap_components as dbc


def page_header(title, back_href="/", right_slot=None):
    """
    Build standard page header row.
    title: str or Dash component (e.g. html.H5(id="tv-header", ...) for dynamic)
    back_href: href for Back link
    right_slot: Dash component(s) for right column (e.g. dropdown + last updated)
    """
    if isinstance(title, str):
        title_el = html.H5(title, style={
            "color": "white", "margin": "0", "fontSize": "clamp(0.9rem, 2vw, 1.1rem)",
            "textAlign": "center",
        })
    else:
        title_el = title
    return dbc.Row([
        dbc.Col(
            html.A("← Back", href=back_href, style={
                "color": "#aaa", "fontSize": "0.95rem", "textDecoration": "none",
                "display": "inline-flex", "alignItems": "center",
            }),
            width=2, className="d-flex align-items-center"
        ),
        dbc.Col(
            title_el,
            width=8, className="d-flex justify-content-center align-items-center"
        ),
        dbc.Col(
            right_slot if right_slot is not None else html.Div(),
            width=2, className="align-self-center"
        ),
    ], className="align-items-center mb-2 g-2")
