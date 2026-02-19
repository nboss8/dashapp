"""
Reusable KPI card component. Used by TV Display and potentially PIDK.
"""
from dash import html
import dash_bootstrap_components as dbc


def kpi_card(title, value, goal, delta_pct, color, value_color="#fff", dec=1):
    """Build a KPI card: title, value, optional goal line with delta %."""
    if value is not None:
        value_str = f"{value:,.0f}" if dec == 0 else f"{value:,.1f}"
    else:
        value_str = "—"
    if goal and goal > 0 and value is not None:
        sign = "+" if delta_pct >= 0 else ""
        goal_val = f"{goal:,.0f}" if dec == 0 else f"{goal:,.1f}"
        goal_str = f"Goal: {goal_val} ({sign}{delta_pct:.1f}%)"
    else:
        goal_str = "\u00a0"  # non-breaking space keeps height consistent
    return dbc.Col(
        html.Div([
            html.P(title, style={
                "fontSize": "0.9rem", "color": "#fff",
                "marginBottom": "2px", "textTransform": "uppercase",
                "letterSpacing": "0.05em"
            }),
            html.H2(value_str, style={
                "fontSize": "3rem", "fontWeight": "bold",
                "margin": "0", "lineHeight": "1",
                "color": value_color,
            }),
            html.P(goal_str, style={
                "fontSize": "0.9rem", "color": "#fff",
                "marginTop": "4px", "marginBottom": "0"
            }),
        ], style={
            "backgroundColor": color or "#2d2d2d",
            "borderRadius": "10px",
            "padding": "14px 18px",
            "textAlign": "center",
            "color": "white",
            "minHeight": "120px",
            "display": "flex",
            "flexDirection": "column",
            "justifyContent": "center",
        }),
        width=3
    )
