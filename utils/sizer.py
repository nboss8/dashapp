"""
Sizer matrix helpers: _size_sort_key, _get_gradient_color, build_sizer_matrix.
"""
import pandas as pd


def _size_sort_key(x):
    """Sort key for size names (e.g. '88', '100') - numeric order, non-numeric last."""
    try:
        return int(str(x).strip())
    except (ValueError, TypeError):
        return 999


def _get_gradient_color(val, min_val=0, max_val=20):
    """Blue gradient for matrix cells (dark theme). Returns (bg_color, text_color)."""
    if val is None or val <= min_val:
        return "#1e1e1e", "#888"
    # Coerce to float to avoid float/Decimal type error from Snowflake numerics
    ratio = min((float(val) - float(min_val)) / float(max_val - min_val), 1.0)
    # Stronger blue gradient: dark #1a3a5c -> #1565C0 -> #42A5F5
    r = int(26 + (66 - 26) * ratio)
    g = int(58 + (165 - 58) * ratio)
    b = int(92 + (200 - 92) * ratio)
    bg = f"rgb({r},{g},{b})"
    luminance = (0.299 * r + 0.587 * g + 0.136 * b) / 255
    text = "#fff" if luminance < 0.5 else "#111"
    return bg, text


def build_sizer_matrix(drops_df):
    """
    Pivot GradeName (or PACKOUT_GROUP/GRADE_NAME) x SizeName, percentages, row/col totals.
    Returns (pct_pivot, row_totals, col_totals) or (None, None, None).
    Handles both PIDK (GradeName, SizeName, WEIGHT/weight_dec) and PFR (GRADE_NAME, SIZE_NAME, WEIGHT) column names.
    """
    if drops_df is None or drops_df.empty:
        return None, None, None
    row_col = "GradeName" if "GradeName" in drops_df.columns else (
        "PACKOUT_GROUP" if "PACKOUT_GROUP" in drops_df.columns else (
            "GRADE_NAME" if "GRADE_NAME" in drops_df.columns else drops_df.columns[0]
        )
    )
    col_col = "SizeName" if "SizeName" in drops_df.columns else (
        "SIZENAME" if "SIZENAME" in drops_df.columns else (
            "SIZE_NAME" if "SIZE_NAME" in drops_df.columns else drops_df.columns[1]
        )
    )
    val_col = "WEIGHT" if "WEIGHT" in drops_df.columns else (
        "weight_dec" if "weight_dec" in drops_df.columns else (
            drops_df.columns[2] if len(drops_df.columns) > 2 else drops_df.columns[1]
        )
    )
    pivot = drops_df.pivot_table(index=row_col, columns=col_col, values=val_col, aggfunc="sum", fill_value=0)
    size_cols = sorted(pivot.columns, key=_size_sort_key)
    pivot = pivot.reindex(columns=size_cols).fillna(0)
    pivot = pivot.sort_index()
    total_weight = pivot.values.sum()
    if total_weight == 0:
        return None, None, None
    # Coerce to float to avoid float/Decimal type error from Snowflake numerics
    total_weight = float(total_weight)
    pivot_float = pivot.astype(float)
    pct_pivot = pivot_float / total_weight * 100
    row_totals = pct_pivot.sum(axis=1)
    col_totals = pct_pivot.sum(axis=0)
    return pct_pivot, row_totals, col_totals
