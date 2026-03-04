"""
Agent chat callbacks for Home page chatbot.
Sends questions to Snowflake Cortex Agent and renders text/tables.
Streaming runs in a background thread; Interval reads from a queue to avoid "generator already executing".
"""
import copy
import json
import logging
import queue
import re
import threading
import uuid

import dash_vega_components as dvc
from dash import callback, Input, Output, State, html, dcc, no_update, ALL
import dash_ag_grid as dag
import dash_bootstrap_components as dbc

from dash import clientside_callback
from services.snowflake_agent import call_agent, can_use_stream, stream_agent_rest_generator

logger = logging.getLogger(__name__)

_agent_grid_counter = 0
# request_id -> queue.Queue; thread pushes content, Interval does get_nowait()
_stream_queues = {}
_STREAM_DONE = object()  # sentinel

# Substrings that suggest SQL for code-block rendering
_SQL_MARKERS = ("SELECT ", "WITH ", "INSERT ", "UPDATE ", "DELETE ", "FROM ", "CREATE ", "ALTER ", "MERGE ")


def _is_sql_like(text: str) -> bool:
    """Heuristic: treat as SQL if it looks like a statement (SELECT, WITH, etc.)."""
    if not text or not isinstance(text, str):
        return False
    t = text.strip().upper()
    return any(t.startswith(m.strip()) or m.strip() in t[:200] for m in _SQL_MARKERS)


def _sanitize_agent_text(text: str) -> str:
    """Replace emoji with plain-text so they don't render as mojibake (e.g. ð¨)."""
    if not text or not isinstance(text, str):
        return text or ""
    replacements = [
        ("\U0001f6a8", "[!]"),
        ("\U0001f4ca", "[Chart]"),
        ("\U000026a1", "->"),
        ("\U00002705", "[OK]"),
        ("\U0000274c", "[X]"),
        ("\U0001f4c8", "[Chart]"),
        ("ð¨", "[!]"),
        ("ð¦", "[Chart]"),
        ("ð¡", "->"),
    ]
    out = text
    for old, new in replacements:
        out = out.replace(old, new)
    out = re.sub(r"[\u200b-\u200f\u2028-\u202f\ufeff]", "", out)
    return out


def _extract_table_data(item: dict):
    """
    Extract (data, cols) from various Cortex/API shapes: table.result_set,
    result_set at top level, json.result_set. Returns (None, None) if not found.
    """
    rs = None
    if isinstance(item.get("table"), dict):
        rs = item["table"].get("result_set") or item["table"].get("resultSet")
    if not rs:
        rs = item.get("result_set") or item.get("resultSet")
    if not rs and isinstance(item.get("json"), dict):
        rs = item["json"].get("result_set") or item["json"].get("resultSet")
    if not rs or not isinstance(rs, dict):
        return None, None
    data = rs.get("data") or rs.get("rows") or []
    meta = rs.get("resultSetMetaData") or rs.get("result_set_metadata") or {}
    row_type = meta.get("rowType") or meta.get("row_type") or meta.get("columns") or []
    if isinstance(row_type, list) and row_type and isinstance(row_type[0], dict):
        cols = [c.get("name") or c.get("label") or f"Col{i}" for i, c in enumerate(row_type)]
    elif isinstance(row_type, list) and row_type:
        cols = [str(c) for c in row_type]
    elif data and isinstance(data[0], dict):
        cols = list(data[0].keys())
    else:
        cols = [f"Col{i}" for i in range(len(data[0]) if data and isinstance(data[0], (list, tuple)) else 0)]
    if not cols and data:
        cols = [f"Col{i}" for i in range(len(data[0]) if isinstance(data[0], (list, tuple)) else 0)]
    logger.info(f"[DEBUG] Table extracted from {item.get('type', 'unknown')}: {len(data) if data else 0} rows, cols: {cols}")
    return (data, cols) if (data and cols) else (None, None)


def _rows_and_cols(data, cols):
    """Normalize data to list of dicts and column list."""
    if not cols or not data:
        return [], []
    rows = data[:500]
    if isinstance(rows[0], dict):
        return rows, list(cols)
    return [dict(zip(cols, [row[i] if i < len(row) else None for i in range(len(cols))])) for row in rows], list(cols)


def _result_set_to_html_table(data, cols):
    """Build a simple html.Table so table data always renders (no AgGrid dependency)."""
    row_data, col_list = _rows_and_cols(data, cols)
    if not row_data or not col_list:
        return None
    header = html.Thead(html.Tr([html.Th(c, style={"padding": "6px 10px", "borderBottom": "1px solid #555"}) for c in col_list]))
    body = html.Tbody([
        html.Tr([html.Td(row.get(c), style={"padding": "6px 10px", "borderBottom": "1px solid #333"}) for c in col_list])
        for row in row_data
    ])
    return html.Table(
        [header, body],
        style={"width": "100%", "borderCollapse": "collapse", "fontSize": "0.85rem", "color": "#e5e7eb"},
        className="mb-2",
    )


def _result_set_to_ag_grid(data, cols, grid_id: str):
    """Build AgGrid from result_set data and column names."""
    row_data, col_list = _rows_and_cols(data, cols)
    if not row_data or not col_list:
        return None
    column_defs = [
        {"field": c, "headerName": str(c), "sortable": True, "filter": True, "resizable": True}
        for c in col_list
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
    """Convert Cortex Agent response content array to Dash components. Skip SQL blocks (answer only)."""
    global _agent_grid_counter
    if not content_list:
        return []
    out = []
    for item in content_list:
        if not isinstance(item, dict):
            continue
        ctype = item.get("type")
        if ctype == "text":
            text = _sanitize_agent_text(item.get("text", ""))
            if text and not _is_sql_like(text):
                out.append(dcc.Markdown(text, style={"marginBottom": "0.5rem"}, className="agent-markdown"))
        elif ctype == "table":
            data, cols = _extract_table_data(item)
            if data is not None and cols:
                tbl_el = _result_set_to_html_table(data, cols)
                if tbl_el:
                    out.append(html.Div(tbl_el, style={"overflowX": "auto", "marginTop": "0.5rem"}))
        elif ctype == "tool_result":
            tr = item.get("tool_result") or {}
            inner = tr.get("content") or []
            seen_table = False
            for c in inner:
                if not isinstance(c, dict):
                    continue
                if c.get("type") == "json":
                    j = c.get("json") or {}
                    data, cols = _extract_table_data({"json": j})
                    if data is not None and cols and not seen_table:
                        seen_table = True
                        tbl_el = _result_set_to_html_table(data, cols)
                        if tbl_el:
                            out.append(html.Div(tbl_el, style={"overflowX": "auto", "marginTop": "0.5rem"}))
                    text = j.get("text") if isinstance(j, dict) else None
                    if text and not _is_sql_like(text):
                        out.append(dcc.Markdown(_sanitize_agent_text(text), style={"marginBottom": "0.5rem"}, className="agent-markdown"))
                else:
                    data, cols = _extract_table_data(c)
                    if data is not None and cols and not seen_table:
                        seen_table = True
                        tbl_el = _result_set_to_html_table(data, cols)
                        if tbl_el:
                            out.append(html.Div(tbl_el, style={"overflowX": "auto", "marginTop": "0.5rem"}))
        elif ctype == "thinking":
            thinking_text = item.get("thinking", {}).get("text", "") or item.get("text", "") or item.get("content", "") or ""
            if thinking_text and not _is_sql_like(thinking_text):
                _agent_grid_counter += 1
                out.append(
                    dbc.Accordion([
                        dbc.AccordionItem(
                            dcc.Markdown(_sanitize_agent_text(thinking_text), className="agent-markdown", style={"fontSize": "0.85rem", "color": "#9ca3af"}),
                            title="Thinking...",
                            item_id=f"{id_prefix}-thinking-{_agent_grid_counter}",
                        )
                    ], start_collapsed=True, className="mt-2")
                )
        elif ctype == "chart":
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
                cfg = spec["config"]
                cfg["background"] = "#1a1a1a"
                cfg["axis"] = {"labelColor": "#e5e7eb", "titleColor": "#e5e7eb", "gridColor": "#444", "domainColor": "#555", "tickColor": "#555"}
                cfg["title"] = {"color": "#e5e7eb", "fontSize": 14}
                cfg["legend"] = {"labelColor": "#e5e7eb", "titleColor": "#e5e7eb"}
                if "range" not in cfg:
                    cfg["range"] = {}
                cfg["range"].setdefault("category", ["#64B5F6", "#0ea5e9", "#34d399", "#fbbf24", "#a78bfa", "#f472b6"])
                cfg["range"].setdefault("ordinal", cfg["range"]["category"])
                mark = spec.get("mark")
                if isinstance(mark, str) and mark in ("line", "point", "area", "bar"):
                    spec["mark"] = {"type": mark, "color": "#64B5F6", "point": True} if mark == "line" else {"type": mark, "color": "#64B5F6"}
                elif isinstance(mark, dict) and "color" not in mark:
                    spec["mark"] = {**mark, "color": "#64B5F6"}
                cfg["tooltip"] = True
                enc = spec.get("encoding") or {}
                if "tooltip" not in enc and enc and "x" in enc and "y" in enc:
                    xf, yf = enc["x"], enc["y"]
                    spec["encoding"] = {**enc, "tooltip": [
                        {"field": xf.get("field"), "type": xf.get("type", "nominal"), "title": xf.get("title") or xf.get("field")},
                        {"field": yf.get("field"), "type": yf.get("type", "quantitative"), "title": yf.get("title") or yf.get("field")},
                    ]}
                out.append(
                    html.Div(
                        dvc.Vega(
                            id=vega_id,
                            spec=spec,
                            opt={"renderer": "svg", "actions": {"export": True, "source": False, "editor": False, "compiled": False}},
                            style={"width": "100%", "height": "420px", "margin": "15px 0", "backgroundColor": "#1a1a1a", "borderRadius": "8px", "overflow": "hidden"},
                        ),
                        className="mt-2 agent-vega-container",
                    )
                )
            else:
                out.append(html.P("[Chart data received but could not be rendered]", className="text-muted small"))
    return out


def _content_to_copyable_text(content_list) -> str:
    if not content_list:
        return ""
    parts = []
    for item in content_list:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "text":
            parts.append(item.get("text", ""))
        elif item.get("type") == "tool_result":
            for c in (item.get("tool_result") or {}).get("content") or []:
                if isinstance(c, dict) and c.get("type") == "json" and (c.get("json") or {}).get("text"):
                    parts.append(c["json"]["text"])
    return "\n\n".join(p for p in parts if p)


def _history_to_children(history, streaming_content=None):
    if not history:
        return []
    children = []
    avatar_style = {"width": "28px", "height": "28px", "borderRadius": "50%", "display": "flex", "alignItems": "center", "justifyContent": "center", "fontSize": "0.75rem", "fontWeight": "bold", "flexShrink": 0}
    user_bubble_style = {"display": "flex", "alignItems": "flex-start", "maxWidth": "85%", "marginLeft": "auto"}
    assistant_bubble_style = {"display": "flex", "alignItems": "flex-start", "maxWidth": "85%", "marginRight": "auto"}
    for idx, msg in enumerate(history):
        role = msg.get("role", "user")
        content = msg.get("content", [])
        if role == "user":
            text = content if isinstance(content, str) else (content[0] if content else "")
            if isinstance(text, dict):
                text = text.get("text", str(text))
            children.append(
                html.Div([
                    html.Div("U", style={**avatar_style, "backgroundColor": "#2563eb", "color": "white"}, className="me-2"),
                    html.Div(dcc.Markdown(text, style={"margin": 0}, className="agent-markdown"), style={"flex": 1}, className="user-message-card"),
                ], style=user_bubble_style)
            )
        else:
            comps = _content_to_components(content, id_prefix=f"agent-msg-{idx}") if isinstance(content, list) else [html.P(str(content))]
            copy_text = _content_to_copyable_text(content) if isinstance(content, list) else str(content)
            copy_btn = [dcc.Clipboard(content=copy_text, title="Copy", children=html.Span("Copy", style={"fontSize": "0.75rem", "cursor": "pointer", "color": "#9ca3af"}, className="ms-1"))] if copy_text else []
            inner = comps + [html.Div(copy_btn, style={"marginTop": "0.25rem"})] if copy_btn else comps
            children.append(
                html.Div([
                    html.Div("A", style={**avatar_style, "backgroundColor": "#4b5563", "color": "#e5e7eb"}, className="me-2"),
                    html.Div(inner, style={"flex": 1}, className="assistant-message-card"),
                ], style=assistant_bubble_style)
            )
    if streaming_content is not None:
        if not streaming_content:
            inner = html.Div([
                html.Span("Thinking", style={"color": "#9ca3af", "marginRight": "4px"}),
                html.Span([html.Span(".", className="agent-typing-dot"), html.Span(".", className="agent-typing-dot"), html.Span(".", className="agent-typing-dot")], className="agent-typing-dots"),
            ], style={"display": "flex", "alignItems": "center", "fontSize": "0.9rem"})
        else:
            comps = _content_to_components(streaming_content, id_prefix="agent-stream")
            copy_text = _content_to_copyable_text(streaming_content)
            copy_btn = [dcc.Clipboard(content=copy_text, title="Copy", children=html.Span("Copy", style={"fontSize": "0.75rem", "cursor": "pointer", "color": "#9ca3af"}, className="ms-1"))] if copy_text else []
            inner = comps + [html.Div(copy_btn, style={"marginTop": "0.25rem"})] if copy_btn else comps
        children.append(
            html.Div([
                html.Div("A", style={**avatar_style, "backgroundColor": "#4b5563", "color": "#e5e7eb"}, className="me-2"),
                html.Div(inner, style={"flex": 1}, className="assistant-message-card"),
            ], style=assistant_bubble_style)
        )
    return children


def _thinking_placeholder():
    """Placeholder shown while the agent is responding (sync path)."""
    avatar_style = {"width": "28px", "height": "28px", "borderRadius": "50%", "display": "flex", "alignItems": "center", "justifyContent": "center", "fontSize": "0.75rem", "fontWeight": "bold", "flexShrink": 0}
    return html.Div([
        html.Div("A", style={**avatar_style, "backgroundColor": "#4b5563", "color": "#e5e7eb"}, className="me-2"),
        html.Div([
            html.Span("Thinking", style={"color": "#9ca3af", "marginRight": "4px"}),
            html.Span([
                html.Span(".", className="agent-typing-dot"),
                html.Span(".", className="agent-typing-dot"),
                html.Span(".", className="agent-typing-dot"),
            ], className="agent-typing-dots"),
        ], style={"display": "flex", "alignItems": "center", "fontSize": "0.9rem"}, className="assistant-message-card"),
    ], style={"display": "flex", "alignItems": "flex-start", "maxWidth": "85%", "marginRight": "auto"})


@callback(
    Output("agent-messages", "children"),
    Output("agent-chat-history", "data"),
    Output("agent-input", "value"),
    Output("agent-streaming-request-id", "data"),
    Output("agent-streaming-content", "data"),
    Output("agent-trigger", "data"),
    Input("agent-send-btn", "n_clicks"),
    Input("agent-input", "n_submit"),
    Input("agent-regenerate-btn", "n_clicks"),
    State("agent-input", "value"),
    State("agent-chat-history", "data"),
)
def on_send(send_clicks, submit_clicks, regen_clicks, value, history):
    """Update chat immediately with user message + Thinking..., then fire trigger for agent."""
    from dash import ctx
    triggered = ctx.triggered_id if ctx.triggered_id else None
    if not triggered or triggered not in ("agent-send-btn", "agent-input", "agent-regenerate-btn"):
        return no_update, no_update, no_update, no_update, no_update, no_update
    history = list(history or [])
    is_regen = triggered == "agent-regenerate-btn"
    if is_regen:
        if len(history) < 2 or history[-1].get("role") != "assistant":
            return no_update, no_update, no_update, no_update, no_update, no_update
        history = history[:-1]
        question = history[-1].get("content", "")
        if isinstance(question, list) and question:
            question = question[0].get("text", "") if isinstance(question[0], dict) else str(question[0])
        question = str(question).strip()
    else:
        question = (value or "").strip()
        if not question:
            return no_update, no_update, "", no_update, no_update, no_update
        history.append({"role": "user", "content": question})

    # Show user message + "Thinking..." immediately; trigger agent in second callback
    return (
        _history_to_children(history, []),
        history,
        "",
        no_update,
        no_update,
        question,
    )


@callback(
    Output("agent-messages", "children", allow_duplicate=True),
    Output("agent-chat-history", "data", allow_duplicate=True),
    Output("agent-trigger", "data", allow_duplicate=True),
    Input("agent-trigger", "data"),
    State("agent-chat-history", "data"),
    running=[
        (Output("agent-send-btn", "disabled"), True, False),
        (Output("agent-input", "disabled"), True, False),
    ],
    prevent_initial_call=True,
)
def on_agent_trigger(question, history):
    """Run agent when trigger is set; replace Thinking... with reply."""
    if not question or not isinstance(question, str) or not question.strip():
        return no_update, no_update, None
    history = list(history or [])
    try:
        resp = call_agent(question.strip(), history)
        logger.info("[DEBUG] Raw agent response: %s", json.dumps(resp, indent=2, default=str)[:2000])
        content = resp.get("content") or []
        comps = _content_to_components(content) if isinstance(content, list) else []
        if not comps:
            logger.warning("[Agent] No displayable content; resp keys=%s", list(resp.keys()) if isinstance(resp, dict) else "n/a")
            content = [{"type": "text", "text": "Received response but no displayable content. Check logs for raw format."}]
        history.append({"role": "assistant", "content": content})
    except Exception as e:
        logger.exception("[Agent] call_agent failed")
        history.append({"role": "assistant", "content": [{"type": "text", "text": f"Error: {e}"}]})
    return _history_to_children(history), history, None


@callback(
    Output("agent-messages", "children", allow_duplicate=True),
    Output("agent-chat-history", "data", allow_duplicate=True),
    Output("agent-streaming-request-id", "data", allow_duplicate=True),
    Output("agent-streaming-content", "data", allow_duplicate=True),
    Input("agent-stream-interval", "n_intervals"),
    State("agent-streaming-request-id", "data"),
    State("agent-streaming-content", "data"),
    State("agent-chat-history", "data"),
    prevent_initial_call=True,
)
def on_stream_tick(_n_intervals, request_id, streaming_content, history):
    if not request_id:
        return no_update, no_update, no_update, no_update
    stream_queue = _stream_queues.get(request_id)
    if not stream_queue:
        return no_update, no_update, None, None
    try:
        content = stream_queue.get_nowait()
    except queue.Empty:
        return no_update, no_update, no_update, no_update
    if content is _STREAM_DONE:
        del _stream_queues[request_id]
        last = streaming_content or []
        new_history = (history or []) + [{"role": "assistant", "content": last}]
        return _history_to_children(new_history), new_history, None, None
    return _history_to_children(history or [], content), no_update, request_id, content


clientside_callback(
    """
    function(n) {
        var el = document.getElementById('agent-messages');
        if (el) el.scrollTop = el.scrollHeight;
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
    from dash import ctx
    if not ctx.triggered:
        return no_update
    tid = ctx.triggered_id
    if isinstance(tid, dict) and tid.get("type") == "agent-chip":
        idx = tid.get("index")
        if questions is not None and 0 <= idx < len(questions):
            return questions[idx]
    return no_update
