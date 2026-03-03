import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

from utils.agent_chips import get_suggested_questions

dash.register_page(__name__, path="/", name="Home")

_CHIP_QUESTIONS = get_suggested_questions(6)


def _build_chips():
    """Build suggested question chips from YAML."""
    return [
        dbc.Button(
            q[:50] + ("..." if len(q) > 50 else ""),
            id={"type": "agent-chip", "index": i},
            size="sm",
            color="secondary",
            outline=True,
            className="me-1 mb-1",
        )
        for i, q in enumerate(_CHIP_QUESTIONS)
    ]


layout = dbc.Container([
    html.H1("Columbia Fruit Analytics", 
            className="text-center text-white my-5 display-3"),
    html.H3("Select a report from the menu above", 
            className="text-center text-secondary mb-4"),
    dbc.Card([
        dbc.CardBody([
            html.P(
                "Ask about pallet inventory, trends, aging risk, on-hand by variety, and more.",
                className="text-muted mb-3",
                style={"fontSize": "0.9rem"},
            ),
            html.Div(
                [
                    html.Span("Suggested: ", className="text-muted me-1", style={"fontSize": "0.8rem"}),
                    *_build_chips(),
                ],
                className="mb-2",
                style={"flexWrap": "wrap", "display": "flex", "alignItems": "center"},
            ),
            dcc.Store(id="agent-chat-history", data=[]),
            dcc.Store(id="agent-chip-questions", data=_CHIP_QUESTIONS),
            dcc.Loading(
                id="agent-loading",
                type="circle",
                fullscreen=False,
                children=[
                    html.Div(
                        id="agent-messages",
                        className="agent-messages mb-3",
                        style={"maxHeight": "650px", "overflowY": "auto", "padding": "12px"},
                    )
                ],
            ),
            dbc.Row([
                dbc.Col(
                    dcc.Textarea(
                        id="agent-input",
                        placeholder="Ask about pallet inventory, trends...",
                        className="form-control rounded me-1",
                        style={
                            "backgroundColor": "#1a1a1a",
                            "color": "#fff",
                            "borderColor": "#555",
                            "width": "100%",
                            "margin": "0",
                            "padding": "0.5rem 0.75rem",
                            "flex": "1",
                            "minHeight": "44px",
                            "resize": "vertical",
                        },
                        rows=2,
                    ),
                    width=11,
                    style={"padding": "0 0.25rem 0 0.5rem"},
                ),
                dbc.Col(
                    html.Div(
                        [
                            dbc.Button("Send", id="agent-send-btn", color="primary", size="sm"),
                            dbc.Button("Regenerate", id="agent-regenerate-btn", color="secondary", outline=True, size="sm"),
                        ],
                        className="d-flex align-items-center gap-1",
                    ),
                    width=1,
                    style={"padding": "0 0.5rem 0 0.25rem"},
                ),
            ], className="g-1"),
        ]),
    ], style={"backgroundColor": "#1a1a1a", "borderColor": "#333"}),
], fluid=True)