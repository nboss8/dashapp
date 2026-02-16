import dash
from dash import html, page_container, page_registry
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

load_dotenv()

app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
    title="Columbia Fruit Analytics"
)

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
    navbar,
    dbc.Container(page_container, fluid=True, className="mt-4")
], fluid=True, className="p-0")

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)