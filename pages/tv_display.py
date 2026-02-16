import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import snowflake.connector
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
dash.register_page(__name__, path="/tv", name="TV Display")

# ── Snowflake connection ──────────────────────────────────────────────
_conn = None

def get_conn():
    global _conn
    try:
        if _conn is None or _conn.is_closed():
            _conn = snowflake.connector.connect(
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                user=os.getenv("SNOWFLAKE_USER"),
                authenticator="programmatic_access_token",
                token=os.getenv("SNOWFLAKE_TOKEN"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                network_timeout=30,
                login_timeout=30,
            )
    except Exception:
        _conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            authenticator="programmatic_access_token",
            token=os.getenv("SNOWFLAKE_TOKEN"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            network_timeout=30,
            login_timeout=30,
        )
    return _conn

def query(sql):
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        print(f"Query error: {e}")
        return pd.DataFrame()

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
    if not date_shift_key:
        return pd.DataFrame()
    return query(f"""
        SELECT
            BUCKET_START,
            SUM(BINS_PER_HOUR)                  AS BINS_PER_HOUR,
            AVG(BIN_HOUR_TARGET)                AS BIN_HOUR_TARGET,
            SUM(EST_PACKS_PER_MAN_HOUR)         AS EST_PACKS_PER_MAN_HOUR,
            AVG(PACKS_MANHOUR_TARGET)           AS PACKS_MANHOUR_TARGET,
            SUM(MINUTES_ELAPSED)                AS MINUTES_ELAPSED
        FROM FROSTY.STAGING.DT_SHIFT_10MIN_KPI_A_PER_RUN03_DT
        WHERE DATE_SHIFT_KEY = '{date_shift_key}'
        AND MINUTES_ELAPSED > 0
        GROUP BY BUCKET_START
        ORDER BY BUCKET_START
    """)

def get_current_runs(date_shift_key=None):
    if not date_shift_key:
        # No active shift — return empty
        return pd.DataFrame()
    else:
        date_parts = date_shift_key.split('-')
        date_part = '-'.join(date_parts[:3])
        shift_part = date_parts[3] if len(date_parts) > 3 else ''
        return query(f"""
            SELECT DISTINCT
                p.RUN_KEY,
                p.GROWER_NUMBER,
                p.VARIETY_LIST,
                p.SHIFT,
                p.PACKS_MANHOUR_TARGET,
                p.BIN_HOUR_TARGET
            FROM FROSTY.STAGING.DQ_PTRUN_N_REPORT_03 p
            WHERE p.DATE_D = '{date_part}'
            AND p.SHIFT = '{shift_part}'
        """)

# ── Helpers ───────────────────────────────────────────────────────────
def color_bar(val, target):
    if val is None or target is None or target == 0:
        return "#555555"
    pct = (val - target) / target
    if pct >= 0:
        return "#4CAF50"
    elif pct >= -0.10:
        return "#FFC107"
    else:
        return "#F44336"

def kpi_card(title, value, goal, delta_pct, color):
    value_str = f"{value:,.1f}" if value is not None else "—"
    if goal and goal > 0 and value is not None:
        sign = "+" if delta_pct >= 0 else ""
        goal_str = f"Goal: {goal:,.1f} ({sign}{delta_pct:.1f}%)"
    else:
        goal_str = "\u00a0"  # non-breaking space keeps height consistent
    return dbc.Col(
        html.Div([
            html.P(title, style={
                "fontSize": "0.9rem", "color": "#ccc",
                "marginBottom": "2px", "textTransform": "uppercase",
                "letterSpacing": "0.05em"
            }),
            html.H2(value_str, style={
                "fontSize": "3rem", "fontWeight": "bold",
                "margin": "0", "lineHeight": "1"
            }),
            html.P(goal_str, style={
                "fontSize": "0.9rem", "color": "#555",
                "marginTop": "4px", "marginBottom": "0"
            }),
        ], style={
            "backgroundColor": color or "#2d2d2d",
            "borderRadius": "10px",
            "padding": "14px 18px",
            "textAlign": "center",
            "color": "white",
            "minHeight": "120px",
            "display": "flex",
            "flexDirection": "column",
            "justifyContent": "center",
        }),
        width=3
    )

def build_chart(df, y_col, target_col, title, color_col):
    base_layout = dict(
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        font=dict(color="white", size=11),
        title=dict(text=title, font=dict(size=13, color="white")),
        legend=dict(orientation="h", y=1.15, x=0),
        margin=dict(l=40, r=60, t=40, b=30),
        height=230,
        xaxis=dict(gridcolor="#2a2a2a", showgrid=True),
        yaxis=dict(gridcolor="#2a2a2a", showgrid=True),
        yaxis2=dict(overlaying="y", side="right", showgrid=False)
    )
    fig = go.Figure(layout=base_layout)
    if df.empty or y_col not in df.columns:
        fig.add_annotation(
            text="No data", xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color="#666", size=18)
        )
        return fig
    fig.add_trace(go.Bar(
        x=df["BUCKET_START"], y=df[y_col],
        marker_color=df[color_col], name=title
    ))
    fig.add_trace(go.Scatter(
        x=df["BUCKET_START"], y=df[target_col],
        mode="lines", line=dict(color="#1565C0", width=2.5),
        name="Target", yaxis="y2"
    ))
    return fig

# ── Layout ────────────────────────────────────────────────────────────
layout = html.Div([
    dcc.Interval(id="tv-interval", interval=300_000, n_intervals=0),
    dcc.Store(id="tv-date-store", data=None),

    # Retractable sidebar
    html.Div([
        html.Div([
            html.P("SELECT DATE", style={
                "color": "#aaa", "fontSize": "0.75rem",
                "marginBottom": "8px", "letterSpacing": "0.1em"
            }),
            dcc.DatePickerSingle(
                id="tv-date-picker",
                date=None,
                placeholder="Today",
                display_format="YYYY-MM-DD",
                style={"marginBottom": "12px"}
            ),
            dbc.Button("TODAY", id="tv-reset-btn",
                      color="secondary", size="sm",
                      style={"width": "100%", "marginBottom": "8px"}),
            html.Hr(style={"borderColor": "#444"}),
            html.P("Move mouse to right edge to show controls",
                  style={"color": "#666", "fontSize": "0.7rem",
                         "textAlign": "center"})
        ], style={"padding": "16px"})
    ], id="tv-sidebar", style={
        "position": "fixed",
        "top": 0, "right": "-220px",
        "width": "220px",
        "height": "100vh",
        "backgroundColor": "#111",
        "borderLeft": "1px solid #333",
        "zIndex": 1000,
        "transition": "right 0.3s ease",
        "overflowY": "auto"
    }),

    # Main content
    html.Div([
        # Header
        dbc.Row([
            dbc.Col(html.H5(id="tv-header", style={
                "color": "white", "margin": "0", "fontSize": "1rem"
            }), width=True),
            dbc.Col(html.P(id="tv-last-updated", style={
                "color": "#555", "margin": "0",
                "fontSize": "0.8rem", "textAlign": "right"
            }), width="auto"),
        ], className="align-items-center mb-2"),

        # KPI Cards
        dbc.Row(id="tv-kpi-cards", className="g-2 mb-2"),

        # Charts
        dbc.Row([
            dbc.Col(dcc.Graph(id="tv-ppmh-chart",
                             config={"displayModeBar": False}), width=6),
            dbc.Col(dcc.Graph(id="tv-bph-chart",
                             config={"displayModeBar": False}), width=6),
        ], className="mb-2"),

        # Run table
        html.P("CURRENT RUNS", style={
            "color": "#aaa", "fontSize": "0.75rem",
            "letterSpacing": "0.1em", "marginBottom": "4px"
        }),
        html.Div(id="tv-run-table", style={"fontSize": "0.82rem"}),

    ], style={
        "padding": "12px 16px",
        "height": "100vh",
        "overflow": "hidden",
        "display": "flex",
        "flexDirection": "column",
    }),

], style={"backgroundColor": "#1a1a1a", "height": "100vh", "overflow": "hidden"})


# ── Callbacks ─────────────────────────────────────────────────────────
@callback(
    Output("tv-date-store", "data"),
    Input("tv-reset-btn", "n_clicks"),
    Input("tv-date-picker", "date"),
    prevent_initial_call=True
)
def update_date_store(reset_clicks, picked_date):
    from dash import ctx
    if ctx.triggered_id == "tv-reset-btn":
        return None
    return picked_date


@callback(
    Output("tv-kpi-cards", "children"),
    Output("tv-ppmh-chart", "figure"),
    Output("tv-bph-chart", "figure"),
    Output("tv-run-table", "children"),
    Output("tv-header", "children"),
    Output("tv-last-updated", "children"),
    Input("tv-interval", "n_intervals"),
    Input("tv-date-store", "data"),
)
def update_tv(n_intervals, selected_date):
    # KPI Cards
    kpi_df = get_kpi_totals(selected_date)

    if kpi_df.empty:
        cards = [dbc.Col(html.P(
            f"No shift data for {selected_date or 'today'}",
            style={"color": "#FFC107"}), width=12)]
        header = f"No data — {selected_date or 'Today'}"
        date_shift_key = None
    else:
        row = kpi_df.iloc[0]
        date_shift_key = row.get("DATE_SHIFT_KEY")
        header = f"{row.get('DAY_LABEL', '')} — Shift {row.get('SHIFT', '')}"

        bph = row.get("BINS_PER_HOUR")
        bph_target = row.get("BIN_HOUR_TARGET_WEIGHTED")
        bph_color = row.get("BPH_TARGET_COLOR") or "#2d2d2d"
        bph_delta = ((bph - bph_target) / bph_target * 100) if bph and bph_target else 0

        ppmh = row.get("STAMPER_PPMH")
        ppmh_target = row.get("PACKS_MANHOUR_TARGET_WEIGHTED")
        ppmh_color = row.get("PACKS_TARGET_COLOR") or "#2d2d2d"
        ppmh_delta = ((ppmh - ppmh_target) / ppmh_target * 100) if ppmh and ppmh_target else 0

        total_bins = row.get("TOTAL_BINS")
        bins_target = row.get("BINS_TARGET_FULL_SHIFT")
        bins_color = row.get("BINS_AT_TARGET_ELAPSED_COLOR") or "#2d2d2d"
        bins_delta = ((total_bins - bins_target) / bins_target * 100) if total_bins and bins_target else 0

        ppb = row.get("PACKS_PER_BIN")

        cards = [
            kpi_card("Bins Per Hour", bph, bph_target, bph_delta, bph_color),
            kpi_card("Packs Per Man Hour", ppmh, ppmh_target, ppmh_delta, ppmh_color),
            kpi_card("Total Bins", total_bins, bins_target, bins_delta, bins_color),
            kpi_card("Packs Per Bin", ppb, None, 0, "#2d2d2d"),
        ]

    # Charts
    chart_df = get_chart_data(date_shift_key)
    if not chart_df.empty:
        chart_df["PPMH_COLOR"] = chart_df.apply(
            lambda r: color_bar(r["EST_PACKS_PER_MAN_HOUR"], r["PACKS_MANHOUR_TARGET"]), axis=1)
        chart_df["BPH_COLOR"] = chart_df.apply(
            lambda r: color_bar(r["BINS_PER_HOUR"], r["BIN_HOUR_TARGET"]), axis=1)

    ppmh_fig = build_chart(chart_df, "EST_PACKS_PER_MAN_HOUR",
                           "PACKS_MANHOUR_TARGET", "Packs Per Man Hour", "PPMH_COLOR")
    bph_fig = build_chart(chart_df, "BINS_PER_HOUR",
                          "BIN_HOUR_TARGET", "Bins Per Hour", "BPH_COLOR")

    # Run table
    runs_df = get_current_runs(date_shift_key)
    if runs_df.empty:
        run_table = html.P("No active runs",
                          style={"color": "#FFC107", "margin": "0"})
    else:
        run_table = dbc.Table([
            html.Thead(html.Tr([
                html.Th(c) for c in [
                    "Run Key", "Grower", "Variety",
                    "Shift", "BPH Target", "PPMH Target"
                ]
            ], style={"backgroundColor": "#222", "color": "#aaa"})),
            html.Tbody([
                html.Tr([
                    html.Td(r["RUN_KEY"]),
                    html.Td(r["GROWER_NUMBER"]),
                    html.Td(r["VARIETY_LIST"]),
                    html.Td(r["SHIFT"]),
                    html.Td(r["BIN_HOUR_TARGET"]),
                    html.Td(r["PACKS_MANHOUR_TARGET"]),
                ]) for _, r in runs_df.iterrows()
            ])
        ], color="dark", striped=True, bordered=False,
           size="sm", style={"margin": "0"})

    last_updated = f"Last updated: {datetime.now().strftime('%I:%M:%S %p')}"
    return cards, ppmh_fig, bph_fig, run_table, header, last_updated