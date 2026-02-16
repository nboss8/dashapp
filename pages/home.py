import dash
from dash import html
import dash_bootstrap_components as dbc

dash.register_page(__name__, path="/", name="Home")

layout = dbc.Container([
    html.H1("Columbia Fruit Analytics", 
            className="text-center text-white my-5 display-3"),
    html.H3("Select a report from the menu above", 
            className="text-center text-secondary")
], fluid=True)