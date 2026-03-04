---
name: Agent Chat Chart Rendering v3
overview: Production-ready upgrade for Snowflake Cortex Agent Vega-Lite chart rendering in the Dash chatbot—interactive, exportable, dark-theme charts.
todos:
  - id: chart-block
    content: Replace chart block in callbacks/agent_chat.py
    status: pending
  - id: css
    content: Add Vega-embed CSS to app.py index_string
    status: pending
isProject: false
---

# Agent Chat Chart Rendering – v3 (Production-Ready)

## Objective

Make Snowflake Cortex Agent Vega-Lite charts render as interactive, exportable charts in the Dash chatbot instead of raw JSON or broken components.

## Scope

- `[callbacks/agent_chat.py](callbacks/agent_chat.py)` – only the chart block in `_content_to_components`
- `[app.py](app.py)` – only the inline CSS in `index_string`

Nothing else is touched.

---

## Change 1: Replace Chart Block in `agent_chat.py`

**File:** `[callbacks/agent_chat.py](callbacks/agent_chat.py)`  
**Find:** The `elif ctype == "chart":` block (lines 99–123)

**Replace with:**

```python
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

        # Copy before mutating (content may be re-rendered from history)
        import copy
        spec = copy.deepcopy(spec)

        if spec.get("width") is None or spec.get("width") == "container":
            spec["width"] = "container"
        if "config" not in spec:
            spec["config"] = {}
        spec["config"].setdefault("background", "#1a1a1a")

        try:
            import dash_vega_components as dvc
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
        except ImportError:
            out.append(
                html.Div([
                    html.P("Chart rendering unavailable", className="text-warning small"),
                    html.Pre(json.dumps(spec, indent=2), className="text-muted small bg-dark p-3 rounded"),
                ], className="mt-2")
            )
    else:
        out.append(html.P("[Chart data received but could not be rendered]", className="text-muted small"))
```

**Note:** `global _agent_grid_counter` is already declared at the top of `_content_to_components` (line 45); no additional global declaration is needed.

**Important:** `copy.deepcopy(spec)` is required before mutating `spec`, because the same content is stored in `agent-chat-history` and re-passed when rendering history. Mutating in place would alter stored data and cause subtle bugs on re-render.

---

## Change 2: Add CSS to `app.py`

**File:** `[app.py](app.py)`  
**Find:** The closing `</style>` tag in `index_string` (around line 110), right after the `.agent-messages .agent-markdown` rules.

**Add** immediately before `</style>`:

```css
    /* Agent charts – dark theme polish */
    .agent-messages .vega-embed {
        background: #1a1a1a !important;
        border-radius: 8px !important;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3) !important;
        overflow: hidden;
    }
    .agent-messages .vega-embed .vega-actions a {
        color: #0ea5e9 !important;
        background: rgba(255,255,255,0.1) !important;
        border-radius: 4px !important;
    }
    .agent-messages .vega-embed .vega-actions a:hover {
        background: rgba(255,255,255,0.2) !important;
    }
```

---

## Dependencies

- `dash-vega-components==0.11.0` (already in requirements.txt)
- `json`, `copy` (standard library; `json` already imported; add `copy` at top of agent_chat.py only if not present)

---

## Prerequisites

- Cortex Agent must have the **Data to Chart** tool enabled.

---

## Testing Checklist

1. New chat: “Show me pallet inventory by warehouse as a bar chart” → chart renders
2. Same chat: ask for a second chart → no duplicate ID error
3. Hover tooltips work
4. Export button (top-right) → PNG and SVG download
5. Charts match dark theme (no white flash, shadow visible)
6. Spec as JSON string and as object both work
7. Refresh page → charts still render from history

---

## Rollback

Comment out the new chart block and restore the original 5-line version; remove the added CSS.

---

## Risk

Low. Changes are isolated to chart rendering. No changes to text, table, or tool_result logic.