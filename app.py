import logging
import os
import multiprocessing
from dotenv import load_dotenv

load_dotenv()
# Early main-process detection (critical for Windows spawn mode)
if multiprocessing.current_process().name == "MainProcess":
    os.environ["IS_MAIN_DASH_PROCESS"] = "true"
else:
    os.environ["IS_MAIN_DASH_PROCESS"] = "false"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    force=True
)
logger = logging.getLogger(__name__)
if os.environ.get("IS_MAIN_DASH_PROCESS") == "true":
    logger.info("🔧 Logging enabled — cache debug ACTIVE")

# 🔥 Silence the extremely noisy Snowflake connector logs (keeps [Cache] debug readable)
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
logging.getLogger("snowflake.connector.connection").setLevel(logging.WARNING)
logging.getLogger("snowflake").setLevel(logging.WARNING)

import dash
from dash import html, dcc, page_container, page_registry, clientside_callback, Input, Output
import dash_ag_grid as dag
import dash_bootstrap_components as dbc

from services.background_config import background_callback_manager

app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.DARKLY, dag.themes.BASE, dag.themes.ALPINE],
    suppress_callback_exceptions=True,
    title="Columbia Fruit Analytics",
    background_callback_manager=background_callback_manager,
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
    #inv-filter-group,
    #inv-filter-variety,
    #inv-filter-pack,
    #inv-filter-grade,
    #inv-filter-size,
    #inv-filter-stage,
    #trends-filter-source,
    #trends-filter-crop-year,
    #trends-filter-variety,
    #trends-filter-report-group,
    #trends-yoy,
    #tv-date-dropdown,
    .tv-date-dropdown,
    .inv-dropdown,
    .trends-dropdown {
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
    .tv-date-dropdown .dash-dropdown,
    .trends-dropdown .dash-dropdown {
        background-color: #1a1a1a !important; border-color: #555 !important; color: #fff !important;
    }
    #pidk-day-label-dropdown .dash-dropdown-value,
    #pidk-sizer-event-dropdown .dash-dropdown-value,
    #pfr-group-dropdown .dash-dropdown-value,
    .trends-dropdown .dash-dropdown-value,
    #pidk-day-label-dropdown .dash-dropdown-placeholder,
    #pidk-sizer-event-dropdown .dash-dropdown-placeholder,
    #pfr-group-dropdown .dash-dropdown-placeholder,
    #pidk-day-label-dropdown .dash-dropdown-trigger-icon,
    #pidk-sizer-event-dropdown .dash-dropdown-trigger-icon,
    #pfr-group-dropdown .dash-dropdown-trigger-icon {
        color: #fff !important; fill: #fff !important;
    }
    .dash-dropdown-content { background-color: #1a1a1a !important; border-color: #555 !important; z-index: 9999 !important; }
    .dash-dropdown-option { color: #fff !important; }
    .dash-dropdown-option:hover { background-color: #1565C0 !important; }
    .dash-dropdown-option[data-state="checked"] { background-color: #333 !important; color: #fff !important; }
    .dash-dropdown-search-container,
    .dash-dropdown-search { background-color: #1a1a1a !important; color: #fff !important; border-color: #555 !important; }
    /* Agent chat markdown dark theme */
    .agent-messages .agent-markdown p, .agent-messages .agent-markdown li, .agent-messages .agent-markdown code { color: #e5e7eb; }
    .agent-messages .agent-markdown pre { background: #111; border-radius: 6px; padding: 0.5rem; overflow-x: auto; }
    .agent-messages .agent-markdown pre code { background: none; padding: 0; }
    /* Agent charts – dark theme polish + ensure hover/tooltips work */
    .agent-messages .agent-vega-container,
    .agent-messages .vega-embed {
        background: #1a1a1a !important;
        border-radius: 8px !important;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3) !important;
        overflow: hidden;
        pointer-events: auto;
    }
    .agent-messages .vega-embed .vega-actions a {
        color: #0ea5e9 !important;
        background: rgba(255,255,255,0.1) !important;
        border-radius: 4px !important;
    }
    .agent-messages .vega-embed .vega-actions a:hover {
        background: rgba(255,255,255,0.2) !important;
    }
    .agent-messages .agent-vega-container { cursor: crosshair; }
    .agent-messages .vega-embed .vega-tooltip,
    .agent-messages .mark-tooltip {
        background: #2a2a2a !important;
        color: #e5e7eb !important;
        border: 1px solid #555 !important;
        border-radius: 6px !important;
        padding: 8px 12px !important;
        font-size: 12px !important;
    }
    /* Snowsight-style agent chat cards */
    .agent-messages .user-message-card {
        background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%);
        border-radius: 12px;
        margin: 8px 0 8px auto;
        padding: 12px 16px;
        max-width: 85%;
        box-shadow: 0 2px 12px rgba(59,130,246,0.4);
    }
    .agent-messages .assistant-message-card {
        background: linear-gradient(135deg, #1e1e2e 0%, #2a2a3a 100%);
        border-radius: 12px;
        border-left: 4px solid #0ea5e9;
        box-shadow: 0 4px 20px rgba(0,0,0,0.3);
        margin: 8px 0;
        padding: 16px 20px;
        max-width: 85%;
    }
    .agent-messages .assistant-message-card .agent-sql-block {
        background: #111 !important;
        border-radius: 8px;
        border-left: 4px solid #0ea5e9;
    }
    /* Typing indicator – animated dots while agent is responding */
    .agent-messages .agent-typing-dots {
        display: inline-flex;
        gap: 2px;
    }
    .agent-messages .agent-typing-dot {
        animation: agent-typing-blink 1.4s ease-in-out infinite;
        color: #0ea5e9;
    }
    .agent-messages .agent-typing-dot:nth-child(2) { animation-delay: 0.2s; }
    .agent-messages .agent-typing-dot:nth-child(3) { animation-delay: 0.4s; }
    @keyframes agent-typing-blink {
        0%, 60%, 100% { opacity: 0.3; }
        30% { opacity: 1; }
    }
    </style>
</head>
<body>
    {%app_entry%}
    <footer>{%config%}{%scripts%}{%renderer%}</footer>
</body>
</html>"""

# Hierarchical nav: AI Assistant, Apples, Production OPs, TV Displays, Settings (Caching). PFR removed from nav.
def _nav_item(label, path):
    return dbc.NavItem(dbc.NavLink(label, href=path, className="text-white"))

def _nav_dropdown(label, items):
    return dbc.NavItem(
        dbc.DropdownMenu(
            [dbc.DropdownMenuItem(name, href=path) for name, path in items],
            label=label,
            nav=True,
            in_navbar=True,
            color="link",
            className="text-white",
        )
    )

nav_items = [
    _nav_item("Home", "/"),
    _nav_item("AI Assistant", "/ai-assistant"),
    _nav_item("Forms", "/forms"),
    _nav_dropdown("Apples", [
        ("Packed Inventory Trends", "/production/packed-inventory-trends"),
        ("Pallet Inventory", "/production/pallet-inventory"),
    ]),
    _nav_dropdown("Production OPs", [
        ("Production Intra Day KPIs", "/production/intra-day-kpis"),
    ]),
    _nav_dropdown("TV Displays", [
        ("Apple Airport", "/tv"),
    ]),
    _nav_dropdown("Settings", [
        ("Caching", "/caching"),
    ]),
]

navbar = dbc.Navbar(
    dbc.Container([
        html.A("Columbia Fruit Analytics", className="navbar-brand text-white fw-bold fs-3"),
        dbc.Nav(nav_items, className="ms-auto", navbar=True),
    ], fluid=True),
    color="dark",
    dark=True,
)

app.layout = dbc.Container([
    html.Div(id="navbar-container", children=[navbar]),
    dcc.Download(id="pfr-download-pdf"),
    dcc.Download(id="pfr-download-zip"),
    dcc.Download(id="ag-grid-csv-download"),
    dcc.Download(id="inv-csv-download"),
    html.Div(page_container, id="page-content", className="mt-4")
], fluid=True, className="p-0")

from dash import callback

# Register page callbacks (must run before app starts)
from callbacks.pidk import *  # noqa: F401
from callbacks.pfr import *  # noqa: F401
from callbacks.inventory import *  # noqa: F401
from callbacks.trends import *  # noqa: F401
from callbacks.tv import *  # noqa: F401
from callbacks.agent_chat import *  # noqa: F401

@callback(
    Output('navbar-container', 'style'),
    Input('_pages_location', 'pathname')
)
def toggle_navbar(pathname):
    # Show navbar on all pages so users can use the hierarchical nav from anywhere
    return {'display': 'block'}


@app.server.route("/health")
def health():
    return "OK", 200


if __name__ == "__main__":
    from services.cache_manager import load_persistent_cache
    load_persistent_cache()
    logger.info("✅ Persistent cache warm-up triggered from app.py (all slugs ready)")
    app.run(debug=True, dev_tools_ui=False, host="0.0.0.0", port=8050)