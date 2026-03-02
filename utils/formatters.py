"""
Formatting helpers for display. Centralizes _fmt, _fmt_num, _fmt_dt, _safe_str, _combine_breakdowns.
"""
import pandas as pd


def _fmt(val, dec=1):
    """Format numeric value for display. Returns em-dash for None/invalid."""
    if val is None:
        return "—"
    try:
        return f"{float(val):,.{dec}f}"
    except (ValueError, TypeError):
        return "—"


def _fmt_num(val, dec=0):
    """Format number with optional decimals. Integer format when dec=0."""
    if pd.isnull(val) or val is None:
        return "—"
    try:
        return f"{float(val):,.{dec}f}" if dec else f"{int(val):,}"
    except (ValueError, TypeError):
        return "—"


def _fmt_dt(val, fmt="%m/%d/%Y %H:%M"):
    """Format datetime for display."""
    if pd.isnull(val) or val is None:
        return "N/A"
    try:
        return pd.Timestamp(val).strftime(fmt)
    except Exception:
        return str(val)


def _safe_str(val):
    """Safe string for filenames/display. Returns N/A for empty/null."""
    if pd.isnull(val) or val is None or str(val).strip() == "":
        return "N/A"
    return str(val)


def _combine_breakdowns(breakdowns):
    """Combine multiple breakdown strings (e.g. 'Key1=10 Bins, Key2=5 Bins') into one aggregated string."""
    if not breakdowns:
        return "N/A"
    combined = {}
    for b in breakdowns:
        if not b or str(b).strip() == "N/A":
            continue
        for part in str(b).split(", "):
            if "=" in part:
                key, rest = part.split("=", 1)
                key = key.strip()
                bins_val = 0
                for t in rest.replace(" Bins", "").replace("@", " ").split():
                    try:
                        bins_val = int(float(t))
                        break
                    except ValueError:
                        pass
                combined[key] = combined.get(key, 0) + bins_val
    if not combined:
        return "N/A"
    return ", ".join(f"{k}={v} Bins" for k, v in combined.items())
