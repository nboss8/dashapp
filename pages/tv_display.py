import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import threading
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

from services.snowflake_service import query
from utils.formatters import _fmt
from utils.table_helpers import color_bar
from components.kpi_card import kpi_card
from components.page_header import page_header

load_dotenv()
dash.register_page(__name__, path="/tv", name="TV Display")

# ── Data fetchers ─────────────────────────────────────────────────────
def get_kpi_totals(selected_date=None):
    if selected_date is None:
        # Today only - no fallback
        df = query("""
            SELECT *
            FROM FROSTY.STAGING.VW_SHIFT_TOTALS_FAST_03
            WHERE IS_CURRENT_SHIFT = 1
            LIMIT 1
        """)
    else:
        df = query(f"""
            SELECT *
            FROM FROSTY.STAGING.VW_SHIFT_TOTALS_FAST_03
            WHERE DATE_SHIFT_KEY LIKE '{selected_date}%'
            ORDER BY DATE_SHIFT_KEY DESC
            LIMIT 1
        """)
    return df

def get_chart_data(date_shift_key):
    """Chart data. PPMH uses same calc as Power BI: SUM(STAMPER_EQS) / (SUM(MINUTES_WORKED_ALLOC)/60)."""
    if not date_shift_key:
        return pd.DataFrame()
    return query(f"""
        SELECT
            BUCKET_START,
            SUM(BINS_PER_HOUR)                  AS BINS_PER_HOUR,
            AVG(BIN_HOUR_TARGET)                AS BIN_HOUR_TARGET,
            SUM(STAMPER_EQS) / NULLIF(SUM(MINUTES_WORKED_ALLOC) / 60, 0) AS EST_PACKS_PER_MAN_HOUR,
            AVG(PACKS_MANHOUR_TARGET)           AS PACKS_MANHOUR_TARGET,
            SUM(MINUTES_WORKED_ALLOC)           AS MINUTES_ELAPSED
        FROM FROSTY.STAGING.DT_SHIFT_10MIN_KPI_A_PER_RUN03_DT
        WHERE DATE_SHIFT_KEY = '{date_shift_key}'
        AND MINUTES_WORKED_ALLOC > 0
        GROUP BY BUCKET_START
        ORDER BY BUCKET_START
    """)

def get_current_runs(date_shift_key=None):
    """Current runs from VW_RUN_TOTALS_FAST_03 filtered by VW_LOT_DUMPER_TIME_03.IS_CURRENT_LOT = 1."""
    if not date_shift_key:
        return pd.DataFrame()
    return query(f"""
        SELECT
            v.GROWER_NUMBER,
            v.VARIETY_ABBR,
            v.SHIFT,
            COALESCE(v.BINS_ON_SHIFT, 0) + COALESCE(v.BINS_PRE_SHIFT, 0) AS BINS,
            v.BINS_PER_HOUR,
            v.STAMPER_PPMH,
            v.BIN_HOUR_TARGET,
            v.PACKS_MANHOUR_TARGET,
            v.BINS_TARGET_COLOR,
            v.PACKS_TARGET_COLOR
        FROM FROSTY.STAGING.VW_RUN_TOTALS_FAST_03 v
        INNER JOIN FROSTY.STAGING.VW_LOT_DUMPER_TIME_03 l
            ON l.DATE_SHIFT_KEY = v.DATE_SHIFT_KEY
            AND l.RUN_KEY = v.RUN_KEY
        WHERE v.DATE_SHIFT_KEY = '{date_shift_key}'
          AND l.IS_CURRENT_LOT = 1
    """)

# ── Helpers ───────────────────────────────────────────────────────────
def build_chart(df, y_col, target_col, title, color_col):
    base_layout = dict(
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        font=dict(color="white", size=14),
        title=dict(text=title, font=dict(size=24, color="white", family="Arial Black, sans-serif"), x=0.5, xanchor="center"),
        legend=dict(orientation="h", y=1.15, x=0),
        margin=dict(l=40, r=60, t=40, b=30),
        height=435,
        barmode="stack",
        xaxis=dict(gridcolor="#2a2a2a", showgrid=True),
        yaxis=dict(gridcolor="#2a2a2a", showgrid=True),
        uirevision="tv-dashboard",  # constant = smooth incremental updates, no full redraw
    )
    fig = go.Figure(layout=base_layout)
    if df.empty or y_col not in df.columns:
        fig.add_annotation(
            text="No data", xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color="#666", size=18)
        )
        return fig
    # Stacked bars: segment 1 = up to target (colored by performance), segment 2 = over target (green)
    met = df.apply(lambda r: min(r[y_col] or 0, r[target_col] or 0), axis=1)
    over = df.apply(lambda r: max(0, (r[y_col] or 0) - (r[target_col] or 0)), axis=1)
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
    """Empty figure for initial render (before callback runs). Uses uirevision for consistency."""
    return build_chart(pd.DataFrame(), None, None, title, None)


def _run_tile(label, value, dec=1, width=4):
    """Simple run metric tile (label + value)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        val_str = "—"
    elif isinstance(value, str):
        val_str = value.strip() or "—"
    else:
        val_str = _fmt(value, dec)  # handles int, float, Decimal, numpy types
    return dbc.Col(
        html.Div([
            html.P(label, className="tv-run-tile-label"),
            html.Div(val_str, className="tv-run-tile-value"),
        ], className="tv-run-tile"),
        width=width,
    )


def _run_tile_vs_target(label, value, target, dec=1, width=6):
    """Run metric tile showing value (target) with full-tile background color (green/yellow/red vs target)."""
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


# ── Cache for background refresh (Power BI–style) ─────────────────────
_tv_cache = {}
_tv_cache_lock = threading.Lock()


def _build_tv_payload(selected_date):
    """Build full TV payload (cards, figures, table, header, last_updated) for a given date."""
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
    return (cards, ppmh_fig, bph_fig, run_section_content, header, last_updated)


def _refresh_cache_today():
    """Refresh in-memory cache for 'today' (selected_date=None). Runs in background thread."""
    try:
        payload = _build_tv_payload(None)
        with _tv_cache_lock:
            _tv_cache[None] = payload
    except Exception as e:
        print(f"TV cache refresh error: {e}")


def _tv_background_worker():
    """Daemon thread: refresh cache for today every 5 minutes."""
    _refresh_cache_today()
    while True:
        time.sleep(300)
        _refresh_cache_today()


_tv_background_thread = threading.Thread(target=_tv_background_worker, daemon=True)
_tv_background_thread.start()

# ── Date options for dropdown (e.g. Today + last 31 days) ─────────────
def get_date_dropdown_options():
    today = datetime.now().date()
    options = [{"label": "Today", "value": None}]
    for i in range(31):
        d = today - timedelta(days=i)
        options.append({"label": d.strftime("%Y-%m-%d"), "value": d.strftime("%Y-%m-%d")})
    return options


# ── Layout ────────────────────────────────────────────────────────────
layout = html.Div([
    dcc.Interval(id="tv-interval", interval=300_000, n_intervals=0),
    dcc.Store(id="tv-date-store", data=None),

    # Main content (no sidebar)
    html.Div([
        # Header row: back arrow, title (dynamic), dropdown + last updated
        page_header(
            html.H5(id="tv-header", className="tv-main-header", style={
                "color": "white", "margin": "0", "fontSize": "1.5rem", "fontWeight": "700",
                "textAlign": "center",
            }),
            back_href="/",
            right_slot=html.Div([
                html.Button("⛶ Fullscreen", id="tv-fullscreen-btn", className="tv-fullscreen-btn", title="Toggle fullscreen (like F11)"),
                dcc.Loading(
                    [
                        dcc.Dropdown(
                            id="tv-date-dropdown",
                            options=get_date_dropdown_options(),
                            value=None,
                            clearable=False,
                            placeholder="Select date",
                            className="tv-date-dropdown",
                            style={"minWidth": "140px"},
                        ),
                        html.P(id="tv-last-updated", style={
                            "color": "#fff", "margin": "0", "marginTop": "4px",
                            "fontSize": "0.75rem", "textAlign": "right"
                        }),
                    ],
                    type="circle",
                    color="white",
                    fullscreen=False,
                    style={"minHeight": "40px"},
                ),
            ], className="tv-header-right", style={"display": "flex", "alignItems": "center", "gap": "10px", "flexWrap": "wrap"}),
        ),

        # Main content (fixed structure: graphs persist, only their figure props update)
        html.Div([
            dbc.Row(id="tv-cards-row", className="g-2 mb-2 flex-shrink-0", children=[]),
            dbc.Row([
                dbc.Col(dcc.Graph(
                    id="tv-ppmh-chart",
                    figure=_empty_figure("Packs Per Man Hour"),
                    config={"displayModeBar": False, "displaylogo": False, "showLink": False},
                ), width=12),
            ], className="mb-1 tv-chart-row"),
            dbc.Row([
                dbc.Col(dcc.Graph(
                    id="tv-bph-chart",
                    figure=_empty_figure("Bins Per Hour"),
                    config={"displayModeBar": False, "displaylogo": False, "showLink": False},
                ), width=12),
            ], className="mb-3 tv-chart-row"),
            html.Div(id="tv-runs-section", className="tv-runs-section", children=[
                html.P("Loading…", style={"color": "#999", "textAlign": "center"}),
            ]),
        ], className="g-2", style={
            "flex": "1 1 0", "minHeight": 0, "display": "flex", "flexDirection": "column",
            "overflow": "hidden",
        }),

    ], style={
        "padding": "10px 14px",
        "height": "100vh",
        "maxHeight": "100vh",
        "overflow": "hidden",
        "overflowX": "hidden",
        "display": "flex",
        "flexDirection": "column",
        "boxSizing": "border-box",
    }),

], className="tv-root", style={"backgroundColor": "#1a1a1a", "height": "100vh", "overflow": "hidden", "overflowX": "hidden"})


# ── Callbacks ─────────────────────────────────────────────────────────
@callback(
    Output("tv-date-store", "data"),
    Input("tv-date-dropdown", "value"),
)
def update_date_store(selected_value):
    return selected_value


def _build_runs_section(run_content):
    """Build runs section from run_content (tiles grid or 'No active runs' message)."""
    return [
        html.P("Current Run", className="tv-current-run-title", style={
            "color": "#fff", "fontSize": "1.5rem", "fontWeight": "700",
            "letterSpacing": "0.05em", "marginBottom": "16px",
            "paddingTop": "8px", "paddingBottom": "8px",
            "textAlign": "center", "lineHeight": "1.4",
        }),
        html.Div(run_content, style={"width": "100%"}, className="tv-run-tiles-wrapper"),
    ]


@callback(
    Output("tv-header", "children"),
    Output("tv-last-updated", "children"),
    Output("tv-cards-row", "children"),
    Output("tv-ppmh-chart", "figure"),
    Output("tv-bph-chart", "figure"),
    Output("tv-runs-section", "children"),
    Input("tv-interval", "n_intervals"),
    Input("tv-date-store", "data"),
)
def update_tv(_n_interval, selected_date):
    cache_key = selected_date
    with _tv_cache_lock:
        cached = _tv_cache.get(cache_key)
    if cached is not None:
        cards, ppmh_fig, bph_fig, run_content, header, last_updated = cached
        return header, last_updated, cards, ppmh_fig, bph_fig, _build_runs_section(run_content)
    payload = _build_tv_payload(selected_date)
    with _tv_cache_lock:
        _tv_cache[cache_key] = payload
    cards, ppmh_fig, bph_fig, run_content, header, last_updated = payload
    return header, last_updated, cards, ppmh_fig, bph_fig, _build_runs_section(run_content)