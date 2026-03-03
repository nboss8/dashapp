"""
Agent chat callbacks for Home page chatbot.
Sends questions to Snowflake Cortex Agent and renders text/tables.
Uses background callback so long agent runs don't block/timeout the UI.
"""
import copy
import json
import logging

import dash_vega_components as dvc
from dash import callback, Input, Output, State, html, dcc, no_update, ALL
import dash_ag_grid as dag
import dash_bootstrap_components as dbc

from dash import clientside_callback
from services.snowflake_agent import call_agent

logger = logging.getLogger(__name__)

_agent_grid_counter = 0


def _result_set_to_ag_grid(data, cols, grid_id: str):
    """Build AgGrid from result_set data and column names. Sortable, filterable, exportable."""
    if not cols or not data:
        return None
    # Limit rows for performance; ag-grid handles scrolling
    rows = data[:200]
    row_data = [dict(zip(cols, [row[i] if i < len(row) else None for i in range(len(cols))])) for row in rows]
    column_defs = [
        {"field": c, "headerName": c, "sortable": True, "filter": True, "resizable": True}
        for c in cols
    ]
    return dag.AgGrid(
        id=grid_id,
        rowData=row_data,
        columnDefs=column_defs,
        defaultColDef={"sortable": True, "filter": True, "resizable": True},
        columnSize="sizeToFit",
        dashGridOptions={"domLayout": "autoHeight"},
        className="ag-theme-alpine-dark",
        style={"width": "100%", "minHeight": "80px"},
    )


def _content_to_components(content_list, id_prefix: str = "agent"):
    """Convert Cortex Agent response content array to Dash components."""
    global _agent_grid_counter
    if not content_list:
        return []
    out = []
    for item in content_list:
        if not isinstance(item, dict):
            continue
        ctype = item.get("type")
        if ctype == "text":
            text = item.get("text", "")
            if text:
                out.append(dcc.Markdown(text, style={"marginBottom": "0.5rem"}, className="agent-markdown"))
        elif ctype == "table":
            tbl = item.get("table") or {}
            rs = tbl.get("result_set") or {}
            data = rs.get("data") or []
            meta = rs.get("resultSetMetaData") or {}
            row_type = meta.get("rowType") or []
            cols = [c.get("name", f"Col{i}") for i, c in enumerate(row_type)]
            _agent_grid_counter += 1
            tbl_el = _result_set_to_ag_grid(data, cols, f"{id_prefix}-grid-{_agent_grid_counter}")
            if tbl_el:
                out.append(html.Div(tbl_el, style={"overflowX": "auto", "marginTop": "0.5rem"}))
        elif ctype == "tool_result":
            tr = item.get("tool_result") or {}
            inner = tr.get("content") or []
            for c in inner:
                if isinstance(c, dict) and c.get("type") == "json":
                    j = c.get("json") or {}
                    rs = j.get("result_set")
                    if rs:
                        data = rs.get("data") or []
                        meta = rs.get("resultSetMetaData") or {}
                        row_type = meta.get("rowType") or []
                        cols = [c.get("name", f"Col{i}") for i, c in enumerate(row_type)]
                        _agent_grid_counter += 1
                        tbl_el = _result_set_to_ag_grid(data, cols, f"{id_prefix}-grid-{_agent_grid_counter}")
                        if tbl_el:
                            out.append(html.Div(tbl_el, style={"overflowX": "auto", "marginTop": "0.5rem"}))
                    text = j.get("text") if isinstance(j, dict) else None
                    if text:
                        out.append(dcc.Markdown(text, style={"marginBottom": "0.5rem"}, className="agent-markdown"))
        elif ctype == "thinking":
            thinking_text = item.get("text", "") or item.get("content", "") or ""
            if thinking_text:
                _agent_grid_counter += 1
                out.append(
                    dbc.Accordion([
                        dbc.AccordionItem(
                            dcc.Markdown(thinking_text, className="agent-markdown", style={"fontSize": "0.85rem", "color": "#9ca3af"}),
                            title="Thinking...",
                            item_id=f"{id_prefix}-thinking-{_agent_grid_counter}",
                        )
                    ], start_collapsed=True, className="mt-2")
                )
        elif ctype == "chart":
            # Robust spec extraction for all Cortex formats
            chart_obj = item.get("chart") or item
            spec = None

            if isinstance(chart_obj, dict):
                spec = chart_obj.get("chart_spec") or chart_obj.get("spec")
                if spec is None and ("$schema" in chart_obj or "mark" in chart_obj):
                    spec = chart_obj
                if isinstance(spec, str):
                    try:
                        spec = json.loads(spec)
                    except (json.JSONDecodeError, TypeError):
                        spec = None

            if isinstance(spec, dict):
                _agent_grid_counter += 1
                vega_id = f"{id_prefix}-vega-{_agent_grid_counter}"

                spec = copy.deepcopy(spec)

                if spec.get("width") is None or spec.get("width") == "container":
                    spec["width"] = "container"
                if "config" not in spec:
                    spec["config"] = {}
                spec["config"].setdefault("background", "#1a1a1a")

                out.append(
                    html.Div(
                        dvc.Vega(
                            id=vega_id,
                            spec=spec,
                            opt={
                                "renderer": "svg",
                                "actions": {
                                    "export": True,
                                    "source": False,
                                    "editor": False,
                                    "compiled": False,
                                },
                            },
                            style={
                                "width": "100%",
                                "height": "420px",
                                "margin": "15px 0",
                                "backgroundColor": "#1a1a1a",
                                "borderRadius": "8px",
                                "overflow": "hidden",
                            },
                        ),
                        className="mt-2",
                    )
                )
            else:
                out.append(html.P("[Chart data received but could not be rendered]", className="text-muted small"))
    return out


def _content_to_copyable_text(content_list) -> str:
    """Extract plain text from content list for clipboard copy."""
    if not content_list:
        return ""
    parts = []
    for item in content_list:
        if not isinstance(item, dict):
            continue
        ctype = item.get("type")
        if ctype == "text":
            parts.append(item.get("text", ""))
        elif ctype == "tool_result":
            tr = item.get("tool_result") or {}
            for c in tr.get("content") or []:
                if isinstance(c, dict) and c.get("type") == "json":
                    j = c.get("json") or {}
                    if j.get("text"):
                        parts.append(j["text"])
    return "\n\n".join(p for p in parts if p)


def _history_to_children(history):
    """Convert chat history to message list components with bubbles, markdown, copy buttons."""
    if not history:
        return []
    children = []
    avatar_style = {
        "width": "28px",
        "height": "28px",
        "borderRadius": "50%",
        "display": "flex",
        "alignItems": "center",
        "justifyContent": "center",
        "fontSize": "0.75rem",
        "fontWeight": "bold",
        "flexShrink": 0,
    }
    user_bubble_style = {
        "maxWidth": "80%",
        "marginLeft": "auto",
        "padding": "0.5rem 0.75rem",
        "backgroundColor": "#3b82f6",
        "color": "white",
        "borderRadius": "18px 18px 4px 18px",
        "marginBottom": "0.5rem",
    }
    assistant_bubble_style = {
        "maxWidth": "80%",
        "marginRight": "auto",
        "padding": "0.5rem 0.75rem",
        "backgroundColor": "#1f2937",
        "borderRadius": "18px 18px 18px 4px",
        "marginBottom": "0.5rem",
    }
    for idx, msg in enumerate(history):
        role = msg.get("role", "user")
        content = msg.get("content", [])
        if role == "user":
            text = content if isinstance(content, str) else (content[0] if content else "")
            if isinstance(text, dict):
                text = text.get("text", str(text))
            children.append(
                html.Div(
                    [
                        html.Div("U", style={**avatar_style, "backgroundColor": "#2563eb", "color": "white"}, className="me-2"),
                        html.Div(dcc.Markdown(text, style={"margin": 0}, className="agent-markdown"), style={"flex": 1}),
                    ],
                    style={**user_bubble_style, "display": "flex", "alignItems": "flex-start"},
                )
            )
        else:
            comps = _content_to_components(content, id_prefix=f"agent-msg-{idx}") if isinstance(content, list) else [html.P(str(content))]
            copy_text = _content_to_copyable_text(content) if isinstance(content, list) else str(content)
            copy_btn = []
            if copy_text:
                copy_btn = [
                    dcc.Clipboard(
                        content=copy_text,
                        title="Copy to clipboard",
                        children=html.Span(
                            "Copy",
                            style={"fontSize": "0.75rem", "cursor": "pointer", "color": "#9ca3af"},
                            className="ms-1",
                        ),
                    ),
                ]
            inner = comps + [html.Div(copy_btn, style={"marginTop": "0.25rem"})] if copy_btn else comps
            children.append(
                html.Div(
                    [
                        html.Div("A", style={**avatar_style, "backgroundColor": "#4b5563", "color": "#e5e7eb"}, className="me-2"),
                        html.Div(inner, style={"flex": 1}),
                    ],
                    style={**assistant_bubble_style, "display": "flex", "alignItems": "flex-start"},
                )
            )
    return children


@callback(
    Output("agent-messages", "children"),
    Output("agent-chat-history", "data"),
    Output("agent-input", "value"),
    Input("agent-send-btn", "n_clicks"),
    Input("agent-input", "n_submit"),
    Input("agent-regenerate-btn", "n_clicks"),
    State("agent-input", "value"),
    State("agent-chat-history", "data"),
    running=[
        (Output("agent-send-btn", "disabled"), True, False),
        (Output("agent-input", "disabled"), True, False),
    ],
)
def on_send(send_clicks, submit_clicks, regen_clicks, value, history):
    from dash import ctx
    triggered = ctx.triggered_id if ctx.triggered_id else None
    if not triggered or triggered not in ("agent-send-btn", "agent-input", "agent-regenerate-btn"):
        return no_update, no_update, no_update
    history = history or []
    is_regen = triggered == "agent-regenerate-btn"
    if is_regen:
        if len(history) < 2 or history[-1].get("role") != "assistant":
            return no_update, no_update, no_update
        history = history[:-1]
        question = history[-1].get("content", "")
        if isinstance(question, list) and question:
            question = question[0].get("text", "") if isinstance(question[0], dict) else str(question[0])
        question = str(question).strip()
    else:
        question = (value or "").strip()
        if not question:
            return no_update, no_update, ""
        history.append({"role": "user", "content": question})
    try:
        resp = call_agent(question, history)
        content = resp.get("content") or []
        comps = _content_to_components(content) if isinstance(content, list) else []
        if not comps:
            logger.warning("[Agent] No displayable content parsed; resp keys=%s", list(resp.keys()) if isinstance(resp, dict) else "n/a")
            content = [{"type": "text", "text": "Received response but no displayable content. Check logs for raw format."}]
        history.append({"role": "assistant", "content": content})
    except Exception as e:
        logger.exception("[Agent] call_agent failed")
        history.append({"role": "assistant", "content": [{"type": "text", "text": f"Error: {e}"}]})
    return _history_to_children(history), history, ""


# Auto-scroll chat to bottom when new messages arrive
clientside_callback(
    """
    function(n) {
        const el = document.getElementById('agent-messages');
        if (el) { el.scrollTop = el.scrollHeight; }
        return {minHeight: '200px', maxHeight: '400px', overflowY: 'auto', padding: '0.5rem'};
    }
    """,
    Output("agent-messages", "style"),
    Input("agent-messages", "children"),
)

@callback(
    Output("agent-input", "value", allow_duplicate=True),
    Input({"type": "agent-chip", "index": ALL}, "n_clicks"),
    State("agent-chip-questions", "data"),
    prevent_initial_call=True,
)
def chip_to_input(n_clicks_list, questions):
    """When a suggested chip is clicked, populate the input with that question."""
    from dash import ctx
    if not ctx.triggered:
        return no_update
    tid = ctx.triggered_id
    if isinstance(tid, dict) and tid.get("type") == "agent-chip":
        idx = tid.get("index")
        if questions and 0 <= idx < len(questions):
            return questions[idx]
    return no_update
