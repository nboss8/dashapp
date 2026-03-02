"""
Reusable colored table component for Power BI-style KPI tables.
Uses html.Table with conditional green/yellow/red coloring and clean number formatting.
"""
from dash import html
import dash_bootstrap_components as dbc
import pandas as pd

from utils.table_helpers import _cell, color_bar_powerbi
from utils.formatters import _fmt


def _to_float_or_none(value):
    """Coerce value to float when possible; return None for null/non-numeric."""
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def create_colored_table(
    df: pd.DataFrame,
    columns: list,
    id_prefix: str,
    pinned_cols: int = 4,
    row_click_type: str = None,
):
    """
    Build a dense, colorful html.Table with Power BI-style conditional coloring.

    Args:
        df: DataFrame with the data
        columns: List of dicts: {"field", "header", "dec" (0 or 1), "color_target" (optional)}
                 color_target = column name for target; if set, applies green/yellow/red vs target
        id_prefix: For Export CSV button id
        pinned_cols: First N columns get pinned styling (bold/darker background)
        row_click_type: If set (e.g. "pidk-run-row-btn"), first column becomes clickable for selection
    """
    if df is None or df.empty:
        return html.P("No data available", className="text-center text-muted p-5")

    # Header row
    _th_style = {
        "padding": "4px 7px",
        "fontSize": "0.8rem",
        "lineHeight": "1.1",
        "textAlign": "center",
        "color": "#fff",
        "backgroundColor": "#2a2a2a",
        "fontWeight": "600",
        "borderBottom": "1px solid #444",
    }
    _th_pinned = {**_th_style, "backgroundColor": "#252525"}
    header_cells = []
    for i, col_spec in enumerate(columns):
        th_style = _th_pinned if i < pinned_cols else _th_style
        header_cells.append(html.Th(col_spec["header"], style=th_style))
    header_row = html.Tr(header_cells)

    # Body rows
    _td_base = {
        "padding": "4px 7px",
        "fontSize": "0.8rem",
        "lineHeight": "1.1",
        "textAlign": "center",
        "borderColor": "#333",
    }
    _td_pinned = {**_td_base, "fontWeight": "600", "backgroundColor": "#1e1e1e", "color": "#ddd"}
    _td_normal = {**_td_base, "backgroundColor": "#1a1a1a", "color": "#ddd"}
    rows = []
    for row_idx, (_, row) in enumerate(df.iterrows()):
        cells = []
        for j, col_spec in enumerate(columns):
            field = col_spec["field"]
            dec = col_spec.get("dec", 1)
            color_target = col_spec.get("color_target")
            val = row.get(field) if field in row.index else None
            target = row.get(color_target) if color_target and color_target in row.index else None
            val_num = _to_float_or_none(val)
            target_num = _to_float_or_none(target)

            # Special display values
            if pd.isna(val):
                display_val = "—"
            elif field == "BinsOnShift" and val == 0:
                display_val = "Scheduled"
            else:
                display_val = None  # use _cell/_fmt for formatting

            # Color for KPI columns
            hex_color = None
            if color_target and val_num is not None:
                hex_color = color_bar_powerbi(val_num, target_num)

            cell_style = _td_pinned if j < pinned_cols else _td_normal
            if row_click_type is not None and j == 0:
                # First column: clickable for row selection
                if display_val is not None:
                    content = display_val
                elif val_num is not None:
                    content = _fmt(round(val_num, dec), dec)
                else:
                    content = str(val) if val is not None else "—"
                btn = html.Button(
                    content,
                    id={"type": row_click_type, "index": row_idx},
                    n_clicks=0,
                    style={
                        "background": "none", "border": "none", "color": "inherit",
                        "cursor": "pointer", "padding": 0, "font": "inherit", "width": "100%",
                    },
                )
                cells.append(html.Td(btn, style=cell_style))
            elif display_val is not None:
                cells.append(html.Td(display_val, style=cell_style))
            elif val_num is None:
                # Preserve text columns (e.g. Variety/Lot) as text.
                cells.append(html.Td(str(val), style=cell_style))
            else:
                cells.append(_cell(val_num, hex_color=hex_color, dec=dec, cell_style=cell_style))
        rows.append(html.Tr(cells))

    table = html.Table(
        [html.Thead(header_row), html.Tbody(rows)],
        style={"width": "100%", "borderCollapse": "collapse", "fontSize": "0.95rem"},
        className="pidk-colored-table",
    )

    return html.Div(table, style={"overflowX": "auto"})
