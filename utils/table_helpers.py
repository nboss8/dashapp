"""
Table/cell helpers: color_bar, color_bar_powerbi, _normalize_cell_color, _normalize_df_columns, _cell, _hex_to_class.
"""
import pandas as pd
from dash import html

from utils.formatters import _fmt


def color_bar(val, target):
    """KPI card colors: green / yellow / red vs target."""
    if val is None or target is None or target == 0:
        return "#555555"
    pct = (val - target) / target
    if pct >= 0:
        return "#4CAF50"
    elif pct >= -0.10:
        return "#FFC107"
    else:
        return "#F44336"


def color_bar_powerbi(val, target):
    """Power BI–style pastel colors: light green / yellow / red for table cells."""
    if val is None or target is None or target == 0:
        return "#C8E6C9"
    try:
        pct = (float(val) - float(target)) / float(target)
    except (TypeError, ValueError, ZeroDivisionError):
        return "#C8E6C9"
    if pct >= 0:
        return "#C8E6C9"
    elif pct >= -0.10:
        return "#FFF9C4"
    else:
        return "#FFCDD2"


def _normalize_cell_color(val):
    """Use view color if valid hex, else None (caller will use color_bar_powerbi)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().upper()
    if not s or s in ("NAN", "NONE"):
        return None
    if not s.startswith("#"):
        s = "#" + s
    if len(s) >= 7 and all(c in "0123456789ABCDEF#" for c in s):
        return s
    return None


def _normalize_df_columns(df, mapping):
    """Map Snowflake uppercase columns to expected keys for table builders."""
    if df is None or df.empty:
        return df
    rename = {k: v for k, v in mapping.items() if k in df.columns and k != v}
    return df.rename(columns=rename) if rename else df


# Map hex to CSS class for reliable override (Bootstrap table-dark can override inline)
_HEX_TO_CLASS = {"#C8E6C9": "tv-cell-green", "#FFF9C4": "tv-cell-yellow", "#FFCDD2": "tv-cell-red"}


def _text_color_for_bg(hex_bg):
    """Return #fff or #000 for best contrast on given background. Used for dark-theme KPI cells."""
    if not hex_bg or not isinstance(hex_bg, str):
        return "#fff"
    s = str(hex_bg).strip().upper()
    if s.startswith("#") and len(s) >= 7:
        try:
            r, g, b = int(s[1:3], 16), int(s[3:5], 16), int(s[5:7], 16)
            luminance = (0.299 * r + 0.587 * g + 0.136 * b) / 255
            return "#000" if luminance > 0.6 else "#fff"
        except (ValueError, IndexError):
            pass
    if s.startswith("RGB"):
        return "#fff"
    return "#fff"


def _cell(val, hex_color=None, dec=1, cell_style=None):
    """Colored cell: use CSS class when possible, else inline style for custom hex from view."""
    _cs = cell_style or {"padding": "5px 12px", "textAlign": "center"}
    if hex_color:
        cls = _HEX_TO_CLASS.get((hex_color or "").upper())
        if cls:
            return html.Td(_fmt(val, dec), className=cls, style={**_cs, "textAlign": "center"})
        return html.Td(_fmt(val, dec), style={**_cs, "backgroundColor": hex_color, "color": "#000", "fontWeight": "600", "textAlign": "center"})
    return html.Td(_fmt(val, dec), style=_cs)
