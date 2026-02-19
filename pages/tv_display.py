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
from utils.formatters import *
from utils.table_helpers import *
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
        font=dict(color="white", size=11),
        title=dict(text=title, font=dict(size=13, color="white"), x=0.5, xanchor="center"),
        legend=dict(orientation="h", y=1.15, x=0),
        margin=dict(l=40, r=60, t=40, b=30),
        height=320,
        barmode="stack",
        xaxis=dict(gridcolor="#2a2a2a", showgrid=True),
        yaxis=dict(gridcolor="#2a2a2a", showgrid=True),
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
        run_table = html.P("No active runs",
                          style={"color": "#FFC107", "margin": "0", "textAlign": "center"})
    else:
        _cs = {"padding": "5px 12px", "textAlign": "center"}
        _ths = {"padding": "5px 12px", "fontSize": "0.82rem", "textAlign": "center"}
        rows = []
        for _, r in runs_df.iterrows():
            bph_hex = _normalize_cell_color(r.get("BINS_TARGET_COLOR")) or color_bar_powerbi(r.get("BINS_PER_HOUR"), r.get("BIN_HOUR_TARGET"))
            ppmh_hex = _normalize_cell_color(r.get("PACKS_TARGET_COLOR")) or color_bar_powerbi(r.get("STAMPER_PPMH"), r.get("PACKS_MANHOUR_TARGET"))
            rows.append(html.Tr([
                html.Td(r.get("GROWER_NUMBER", "—"), style=_cs),
                html.Td(r.get("VARIETY_ABBR", "—"), style=_cs),
                html.Td(r.get("SHIFT", "—"), style=_cs),
                html.Td(_fmt(r.get("BINS"), 0), style=_cs),
                _cell(r.get("BINS_PER_HOUR"), bph_hex, cell_style=_cs),
                html.Td(_fmt(r.get("BIN_HOUR_TARGET")), style=_cs),
                _cell(r.get("STAMPER_PPMH"), ppmh_hex, cell_style=_cs),
                html.Td(_fmt(r.get("PACKS_MANHOUR_TARGET")), style=_cs),
            ]))

        cols = ["Grower", "Variety", "Shift", "Bins", "Bins Per Hour", "BPH Target", "PPMH", "PPMH Target"]
        run_table = html.Table([
            html.Thead(html.Tr([
                html.Th(c, style={**_ths, "backgroundColor": "#222", "color": "#fff", "borderBottom": "2px solid #444", "textAlign": "center"})
                for c in cols
            ])),
            html.Tbody(rows, style={"backgroundColor": "#1a1a1a"}),
        ], style={
            "width": "100%", "tableLayout": "fixed", "fontSize": "0.88rem", "borderCollapse": "collapse",
            "color": "#fff", "backgroundColor": "#1a1a1a",
        }, className="tv-runs-fixed-cols")

    last_updated = f"Last updated: {datetime.now().strftime('%I:%M:%S %p')} · Refreshes every 5 min"
    return (cards, ppmh_fig, bph_fig, run_table, header, last_updated)


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
            html.H5(id="tv-header", style={
                "color": "white", "margin": "0", "fontSize": "clamp(0.9rem, 2vw, 1.1rem)",
                "textAlign": "center",
            }),
            back_href="/",
            right_slot=dcc.Loading(
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
        ),

        # Main content (no scrollbars - fit TV display)
        html.Div(
            html.Div(id="tv-main-block", className="g-2"),
            style={
                "flex": "1 1 0", "minHeight": 0, "display": "flex", "flexDirection": "column",
                "overflow": "hidden",
            },
        ),

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


def _main_block_children(payload):
    """Build main block content from payload (cards, ppmh_fig, bph_fig, run_table, header, last_updated)."""
    cards, ppmh_fig, bph_fig, run_table, _h, _u = payload
    return [
        dbc.Row(cards, className="g-2 mb-2 flex-shrink-0"),
        dbc.Row([
            dbc.Col(dcc.Graph(id="tv-ppmh-chart", figure=ppmh_fig, config={
                "displayModeBar": False, "displaylogo": False, "showLink": False,
            }), width=12),
        ], className="mb-1 tv-chart-row"),
        dbc.Row([
            dbc.Col(dcc.Graph(id="tv-bph-chart", figure=bph_fig, config={
                "displayModeBar": False, "displaylogo": False, "showLink": False,
            }), width=12),
        ], className="mb-3 tv-chart-row"),
        html.Div([
            html.P("Current Run", style={
                "color": "#fff", "fontSize": "0.9rem",
                "letterSpacing": "0.05em", "marginBottom": "8px",
                "paddingTop": "4px", "paddingBottom": "4px",
                "textAlign": "center", "lineHeight": "1.4",
            }),
            html.Div(run_table, style={
                "fontSize": "0.82rem", "width": "100%",
            }, className="tv-runs-table"),
        ], className="tv-runs-section"),
    ]


@callback(
    Output("tv-header", "children"),
    Output("tv-last-updated", "children"),
    Output("tv-main-block", "children"),
    Input("tv-interval", "n_intervals"),
    Input("tv-date-store", "data"),
)
def update_tv(_n_interval, selected_date):
    cache_key = selected_date
    with _tv_cache_lock:
        cached = _tv_cache.get(cache_key)
    if cached is not None:
        return cached[4], cached[5], _main_block_children(cached)
    # Cache miss: build once (for Today this only happens on first load; worker keeps cache warm after)
    payload = _build_tv_payload(selected_date)
    with _tv_cache_lock:
        _tv_cache[cache_key] = payload
    return payload[4], payload[5], _main_block_children(payload)