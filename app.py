import dash
from dash import html, dcc, page_container, page_registry, clientside_callback, Input, Output
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

load_dotenv()

app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
    title="Columbia Fruit Analytics",
)

# Inject dropdown dark theme last so it overrides Bootstrap; dcc.Dropdown menu is portaled to body
app.index_string = """<!DOCTYPE html>
<html lang="en">
<head>
    {%metas%}
    <title>{%title%}</title>
    {%favicon%}
    {%css%}
    <style>
    /* Dash 4.0 dropdown dark theme - override CSS variables */
    #pidk-day-label-dropdown,
    #pidk-sizer-event-dropdown,
    #pfr-group-dropdown,
    #tv-date-dropdown,
    .tv-date-dropdown {
        --Dash-Fill-Inverse-Strong: #1a1a1a;
        --Dash-Stroke-Strong: #555;
        --Dash-Text-Strong: #fff;
        --Dash-Text-Primary: #fff;
        --Dash-Text-Weak: #ccc;
        --Dash-Text-Disabled: #999;
        --Dash-Fill-Primary-Hover: rgba(255,255,255,0.08);
        --Dash-Fill-Primary-Active: rgba(255,255,255,0.12);
        --Dash-Fill-Interactive-Strong: #1565C0;
    }
    /* Direct overrides for control + menu */
    #pidk-day-label-dropdown .dash-dropdown,
    #pidk-sizer-event-dropdown .dash-dropdown,
    #pfr-group-dropdown .dash-dropdown,
    #tv-date-dropdown .dash-dropdown,
    .tv-date-dropdown .dash-dropdown {
        background-color: #1a1a1a !important; border-color: #555 !important; color: #fff !important;
    }
    #pidk-day-label-dropdown .dash-dropdown-value,
    #pidk-sizer-event-dropdown .dash-dropdown-value,
    #pfr-group-dropdown .dash-dropdown-value,
    #pidk-day-label-dropdown .dash-dropdown-placeholder,
    #pidk-sizer-event-dropdown .dash-dropdown-placeholder,
    #pfr-group-dropdown .dash-dropdown-placeholder,
    #pidk-day-label-dropdown .dash-dropdown-trigger-icon,
    #pidk-sizer-event-dropdown .dash-dropdown-trigger-icon,
    #pfr-group-dropdown .dash-dropdown-trigger-icon {
        color: #fff !important; fill: #fff !important;
    }
    .dash-dropdown-content { background-color: #1a1a1a !important; border-color: #555 !important; }
    .dash-dropdown-option { color: #fff !important; }
    .dash-dropdown-option:hover { background-color: #1565C0 !important; }
    .dash-dropdown-option[data-state="checked"] { background-color: #333 !important; color: #fff !important; }
    .dash-dropdown-search-container,
    .dash-dropdown-search { background-color: #1a1a1a !important; color: #fff !important; border-color: #555 !important; }
    </style>
</head>
<body>
    {%app_entry%}
    <footer>{%config%}{%scripts%}{%renderer%}</footer>
</body>
</html>"""

navbar = dbc.Navbar(
    dbc.Container([
        html.A("Columbia Fruit Analytics", className="navbar-brand text-white fw-bold fs-3"),
        dbc.Nav([
            dbc.NavLink(page["name"], href=page["relative_path"])
            for page in page_registry.values()
        ], className="ms-auto")
    ], fluid=True),
    color="dark",
    dark=True,
)

app.layout = dbc.Container([
    html.Div(id="navbar-container", children=[navbar]),
    dcc.Download(id="pfr-download-pdf"),
    dcc.Download(id="pfr-download-zip"),
    html.Div(page_container, id="page-content", className="mt-4")
], fluid=True, className="p-0")

from dash import callback

# Register page callbacks (must run before app starts)
from callbacks.pidk import *  # noqa: F401
from callbacks.pfr import *  # noqa: F401

@callback(
    Output('navbar-container', 'style'),
    Input('_pages_location', 'pathname')
)
def toggle_navbar(pathname):
    if pathname == '/tv':
        return {'display': 'none'}
    return {'display': 'block'}


@app.server.route("/health")
def health():
    return "OK", 200


if __name__ == "__main__":
    app.run(debug=True, dev_tools_ui=False, host="0.0.0.0", port=8050)
