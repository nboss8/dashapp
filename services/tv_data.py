"""
TV Display - data fetching, payload building, chart/tile builders.
All data logic for the TV page lives here. Callbacks in callbacks/tv.py.

dbt models used (section 2):
| Model             | Key columns                                                                 | Purpose              | Queried by        |
|-------------------|-----------------------------------------------------------------------------|----------------------|-------------------|
| tv_shift_totals   | DATE_SHIFT_KEY, SHIFT, BINS_PER_HOUR, BIN_HOUR_TARGET_WEIGHTED, ...         | Shift KPI totals     | get_kpi_totals    |
| tv_chart_data     | BUCKET_START, BINS_PER_HOUR, BIN_HOUR_TARGET, EST_PACKS_PER_MAN_HOUR, ...   | Time-series charts   | get_chart_data    |
| tv_current_runs   | GROWER_NUMBER, VARIETY_ABBR, SHIFT, BINS, BINS_PER_HOUR, STAMPER_PPMH, ...  | Current run details  | get_current_runs  |
"""
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from dash import html

from services.snowflake_service import query
from utils.formatters import _fmt
from utils.table_helpers import color_bar
from components.kpi_card import kpi_card

DBT_SCHEMA = os.getenv("SNOWFLAKE_DATABASE", "FROSTY") + "." + os.getenv("DBT_SCHEMA", "DBT_DEV")


def get_kpi_totals(selected_date=None):
    """Shift KPI totals from tv_shift_totals. Explicit columns, no SELECT *."""
    if selected_date is None:
        return query(
            f"""
            SELECT date_shift_key AS "DATE_SHIFT_KEY", shift AS "SHIFT",
                   bins_per_hour AS "BINS_PER_HOUR", bin_hour_target_weighted AS "BIN_HOUR_TARGET_WEIGHTED",
                   stamper_ppmh AS "STAMPER_PPMH", packs_manhour_target_weighted AS "PACKS_MANHOUR_TARGET_WEIGHTED",
                   total_bins AS "TOTAL_BINS", bins_target_full_shift AS "BINS_TARGET_FULL_SHIFT",
                   packs_per_bin AS "PACKS_PER_BIN"
            FROM {DBT_SCHEMA}.tv_shift_totals
            WHERE is_current_shift = 1
            LIMIT 1
            """
        )
    return query(
        f"""
        SELECT date_shift_key AS "DATE_SHIFT_KEY", shift AS "SHIFT",
               bins_per_hour AS "BINS_PER_HOUR", bin_hour_target_weighted AS "BIN_HOUR_TARGET_WEIGHTED",
               stamper_ppmh AS "STAMPER_PPMH", packs_manhour_target_weighted AS "PACKS_MANHOUR_TARGET_WEIGHTED",
               total_bins AS "TOTAL_BINS", bins_target_full_shift AS "BINS_TARGET_FULL_SHIFT",
               packs_per_bin AS "PACKS_PER_BIN"
        FROM {DBT_SCHEMA}.tv_shift_totals
        WHERE date_shift_key LIKE %s
        ORDER BY date_shift_key DESC
        LIMIT 1
        """,
        params=[f"{selected_date}%"],
    )


def get_chart_data(date_shift_key):
    """Chart data from tv_chart_data mart."""
    if not date_shift_key:
        return pd.DataFrame()
    return query(
        f"""
        SELECT bucket_start AS "BUCKET_START", bins_per_hour AS "BINS_PER_HOUR",
               bin_hour_target AS "BIN_HOUR_TARGET", est_packs_per_man_hour AS "EST_PACKS_PER_MAN_HOUR",
               packs_manhour_target AS "PACKS_MANHOUR_TARGET", minutes_elapsed AS "MINUTES_ELAPSED"
        FROM {DBT_SCHEMA}.tv_chart_data
        WHERE date_shift_key = %s
        ORDER BY bucket_start
        """,
        params=[date_shift_key],
    )


def get_current_runs(date_shift_key=None):
    """Current runs from tv_current_runs mart."""
    if not date_shift_key:
        return pd.DataFrame()
    return query(
        f"""
        SELECT grower_number AS "GROWER_NUMBER", variety_abbr AS "VARIETY_ABBR", shift AS "SHIFT",
               bins AS "BINS", bins_per_hour AS "BINS_PER_HOUR", stamper_ppmh AS "STAMPER_PPMH",
               bin_hour_target AS "BIN_HOUR_TARGET", packs_manhour_target AS "PACKS_MANHOUR_TARGET",
               bins_target_color AS "BINS_TARGET_COLOR", packs_target_color AS "PACKS_TARGET_COLOR"
        FROM {DBT_SCHEMA}.tv_current_runs
        WHERE date_shift_key = %s
        """,
        params=[date_shift_key],
    )


def build_chart(df, y_col, target_col, title, color_col):
    """Build stacked bar + target line figure (dark theme, no-data placeholder). Fills container height (fluid layout)."""
    base_layout = dict(
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        font=dict(color="white", size=14),
        title=dict(text=title, font=dict(size=24, color="white", family="Arial Black, sans-serif"), x=0.5, xanchor="center"),
        legend=dict(orientation="h", y=1.15, x=0),
        margin=dict(l=40, r=60, t=40, b=30),
        autosize=True,
        barmode="stack",
        xaxis=dict(gridcolor="#2a2a2a", showgrid=True),
        yaxis=dict(gridcolor="#2a2a2a", showgrid=True),
        uirevision="tv-dashboard",
    )
    fig = go.Figure(layout=base_layout)
    if df.empty or y_col not in df.columns:
        fig.add_annotation(
            text="No data", xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color="#666", size=18)
        )
        return fig
    val = df[y_col].fillna(0).clip(lower=0)
    tgt = df[target_col].fillna(0)
    met = pd.concat([val, tgt], axis=1).min(axis=1)
    over = (val - tgt).clip(lower=0)
    fig.add_trace(go.Bar(
        x=df["BUCKET_START"], y=met,
        marker_color=df[color_col], name="To target"
    ))
    fig.add_trace(go.Bar(
        x=df["BUCKET_START"], y=over,
        marker_color="#4CAF50", name="Over target"
    ))
    fig.add_trace(go.Scatter(
        x=df["BUCKET_START"], y=df[target_col],
        mode="lines", line=dict(color="#1565C0", width=2.5),
        name="Target"
    ))
    return fig


def _empty_figure(title):
    """Empty figure for initial render (before callback runs)."""
    return build_chart(pd.DataFrame(), None, None, title, None)


def _run_tile(label, value, dec=1, width=4):
    """Simple run metric tile (label + value)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        val_str = "—"
    elif isinstance(value, str):
        val_str = value.strip() or "—"
    else:
        val_str = _fmt(value, dec)
    return dbc.Col(
        html.Div([
            html.P(label, className="tv-run-tile-label"),
            html.Div(val_str, className="tv-run-tile-value"),
        ], className="tv-run-tile"),
        width=width,
    )


def _run_tile_vs_target(label, value, target, dec=1, width=6):
    """Run metric tile with value (target) and full-tile background color vs target."""
    val_str = _fmt(value, dec) if value is not None else "—"
    target_str = _fmt(target, dec) if target is not None and float(target) > 0 else "—"
    display = f"{val_str} ({target_str} target)"
    bg_color = color_bar(value, target)
    return dbc.Col(
        html.Div([
            html.P(label, className="tv-run-tile-label"),
            html.Div(display, className="tv-run-tile-value"),
        ], className="tv-run-tile tv-run-tile-colored", style={"backgroundColor": bg_color}),
        width=width,
    )


def build_tv_payload(selected_date):
    """Build full TV payload (cards, figures, run content, header, last_updated) for a given date."""
    _start = time.perf_counter()
    kpi_df = get_kpi_totals(selected_date)
    display_date = selected_date if selected_date else datetime.now().strftime("%Y-%m-%d")

    if kpi_df.empty:
        cards = [dbc.Col(html.P(
            f"No shift data for {selected_date or 'today'}",
            style={"color": "#FFC107"}), width=12)]
        header = f"{display_date} - — - Airport Apple Packing"
        date_shift_key = None
    else:
        row = kpi_df.iloc[0]
        date_shift_key = row.get("DATE_SHIFT_KEY")
        shift_str = row.get("SHIFT", "—")
        header = f"{display_date} - Shift {shift_str} - Airport Apple Packing"

        bph = row.get("BINS_PER_HOUR")
        bph_target = row.get("BIN_HOUR_TARGET_WEIGHTED")
        bph_delta = ((bph - bph_target) / bph_target * 100) if bph and bph_target else 0
        bph_color = color_bar(bph, bph_target)

        ppmh = row.get("STAMPER_PPMH")
        ppmh_target = row.get("PACKS_MANHOUR_TARGET_WEIGHTED")
        ppmh_delta = ((ppmh - ppmh_target) / ppmh_target * 100) if ppmh and ppmh_target else 0
        ppmh_color = color_bar(ppmh, ppmh_target)

        total_bins = row.get("TOTAL_BINS")
        bins_target = row.get("BINS_TARGET_FULL_SHIFT")
        bins_delta = ((total_bins - bins_target) / bins_target * 100) if total_bins and bins_target else 0
        bins_color = color_bar(total_bins, bins_target)

        ppb = row.get("PACKS_PER_BIN")

        cards = [
            kpi_card("Bins Per Hour", bph, bph_target, bph_delta, bph_color),
            kpi_card("Packs Per Man Hour", ppmh, ppmh_target, ppmh_delta, ppmh_color),
            kpi_card("Total Bins", total_bins, bins_target, bins_delta, bins_color, dec=0),
            kpi_card("Packs Per Bin", ppb, None, 0, "#2d2d2d"),
        ]

    chart_df = get_chart_data(date_shift_key)
    if not chart_df.empty:
        chart_df = chart_df.copy()
        chart_df["PPMH_COLOR"] = chart_df.apply(
            lambda r: color_bar(r["EST_PACKS_PER_MAN_HOUR"], r["PACKS_MANHOUR_TARGET"]), axis=1)
        chart_df["BPH_COLOR"] = chart_df.apply(
            lambda r: color_bar(r["BINS_PER_HOUR"], r["BIN_HOUR_TARGET"]), axis=1)

    ppmh_fig = build_chart(chart_df, "EST_PACKS_PER_MAN_HOUR",
                          "PACKS_MANHOUR_TARGET", "Packs Per Man Hour", "PPMH_COLOR")
    bph_fig = build_chart(chart_df, "BINS_PER_HOUR",
                         "BIN_HOUR_TARGET", "Bins Per Hour", "BPH_COLOR")

    runs_df = get_current_runs(date_shift_key)
    if runs_df.empty:
        run_section_content = html.P("No active runs",
                          style={"color": "#FFC107", "margin": "0", "textAlign": "center", "fontSize": "1.5rem"})
    else:
        r = runs_df.iloc[0]
        tiles_row1 = dbc.Row([
            _run_tile("Grower", r.get("GROWER_NUMBER"), dec=0, width=4),
            _run_tile("Variety", r.get("VARIETY_ABBR"), dec=0, width=4),
            _run_tile("Bins", r.get("BINS"), dec=0, width=4),
        ], className="g-2 mb-2")
        tiles_row2 = dbc.Row([
            _run_tile_vs_target("Bins Per Hour", r.get("BINS_PER_HOUR"), r.get("BIN_HOUR_TARGET"), dec=1),
            _run_tile_vs_target("PPMH", r.get("STAMPER_PPMH"), r.get("PACKS_MANHOUR_TARGET"), dec=1),
        ], className="g-2")
        run_section_content = html.Div([tiles_row1, tiles_row2], className="tv-run-tiles-grid")

    last_updated = f"Last updated: {datetime.now().strftime('%I:%M:%S %p')} · Refreshes every 5 min"
    cached_at = datetime.now().isoformat()
    duration_seconds = round(time.perf_counter() - _start, 2)
    return (cards, ppmh_fig, bph_fig, run_section_content, header, last_updated, cached_at, duration_seconds)


def build_runs_section(run_content):
    """Build runs section wrapper (title + run_content). Used by callbacks."""
    return [
        html.P("Current Run", className="tv-current-run-title", style={
            "color": "#fff", "fontSize": "1.5rem", "fontWeight": "700",
            "letterSpacing": "0.05em", "marginBottom": "16px",
            "paddingTop": "8px", "paddingBottom": "8px",
            "textAlign": "center", "lineHeight": "1.4",
        }),
        html.Div(run_content, style={"width": "100%"}, className="tv-run-tiles-wrapper"),
    ]


def get_date_dropdown_options():
    """Date options for TV dropdown: Today and Yesterday only."""
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    return [
        {"label": "Today", "value": None},
        {"label": "Yesterday", "value": yesterday.strftime("%Y-%m-%d")},
    ]


from services.cache_manager import register_report
register_report(build_tv_payload)

