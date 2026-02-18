"""
Production Intra Day KPIs - PTRUN-driven report.
Dropdown: DQ_PTRUN_N_REPORT_03.DAY_LABEL
Run Totals: dims from DQ_PTRUN_N_REPORT_03, facts from VW_RUN_TOTALS_FAST_03
Shift Totals: VW_RUN_TOTALS_FAST_03 aggregated by shift

Joins use column lookups for clarity:
- PACKDATE_RUN_KEY (date+shift, e.g. 2026-02-17-1) links PTRUN to VW_RUN_TOTALS_FAST_03
- GROWER_NUMBER disambiguates runs within same date/shift
"""
import dash
from dash import html, dcc, callback, Input, Output, State, no_update, ALL, ctx
import dash.dash_table as dt
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import snowflake.connector
import pandas as pd
import os
import threading
import time
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv()
dash.register_page(__name__, path="/production/intra-day-kpis", name="Production Intra Day KPIs")

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
    global _conn
    last_err = None
    # Retry once after forcing a reconnect; expired Snowflake sessions can remain "open"
    # at socket level but fail on execute with 390111.
    for attempt in range(2):
        try:
            if attempt == 1:
                try:
                    if _conn is not None:
                        _conn.close()
                except Exception:
                    pass
                _conn = None
            conn = get_conn()
            cursor = conn.cursor()
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=cols)
        except Exception as e:
            last_err = e
    print(f"Query error: {last_err}")
    return pd.DataFrame()

# ── Helpers (Power BI–style cell colors) ──────────────────────────────
def color_bar_powerbi(val, target):
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

def _normalize_df_columns(df, mapping):
    """Map Snowflake uppercase columns to expected keys for table builders."""
    if df is None or df.empty:
        return df
    rename = {k: v for k, v in mapping.items() if k in df.columns and k != v}
    return df.rename(columns=rename) if rename else df

_RUN_COL_MAP = {
    "RUN": "Run", "VARIETY": "Variety", "SHIFT": "Shift", "LOT": "Lot",
    "BINSPRESHIFT": "BinsPreShift", "BINSONSHIFT": "BinsOnShift",
    "BINSPERHOUR": "BinsPerHour", "STAMPERPPMH": "StamperPPMH",
    "BINPERHOURTARGET": "BinPerHourTarget", "PACKSPERHOURMANHOUR": "PacksPerHourManHour",
}
_SHIFT_COL_MAP = {
    "SHIFT": "Shift", "TOTALBINS": "TotalBins", "BINPERHOUR": "BinPerHour",
    "PPMH": "PPMH", "PPMHTARGET": "PPMHTarget", "BPHTARGET": "BPHTarget",
}

def _normalize_cell_color(val):
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

# ── Data fetchers ─────────────────────────────────────────────────────
def _resolve_day_to_date(day_label):
    """Resolve DAY_LABEL (TODAY or YYYY-MM-DD) to actual date for DQ_EQ filtering."""
    if not day_label:
        return date.today().isoformat()
    if str(day_label).upper() == "TODAY":
        df = query("""
            SELECT DATE_D FROM FROSTY.STAGING.DQ_PTRUN_N_REPORT_03
            WHERE DAY_LABEL = 'TODAY' LIMIT 1
        """)
        if not df.empty and df.iloc[0]["DATE_D"] is not None:
            d = df.iloc[0]["DATE_D"]
            return d.isoformat() if hasattr(d, "isoformat") else str(d)[:10]
        return date.today().isoformat()
    return str(day_label)[:10]

def get_day_label_options():
    """Dropdown options from DQ_PTRUN_N_REPORT_03.DAY_LABEL. Format matches TV: 'Today' + YYYY-MM-DD."""
    df = query("""
        SELECT DISTINCT DAY_LABEL
        FROM FROSTY.STAGING.DQ_PTRUN_N_REPORT_03
        ORDER BY CASE WHEN DAY_LABEL = 'TODAY' THEN 0 ELSE 1 END, DAY_LABEL
    """)
    if df.empty:
        return [{"label": "Today", "value": "TODAY"}]
    opts = []
    for _, r in df.iterrows():
        label = str(r["DAY_LABEL"])
        val = label
        if label == "TODAY":
            opts.insert(0, {"label": "Today", "value": "TODAY"})
        else:
            opts.append({"label": label, "value": val})
    return opts if opts else [{"label": "Today", "value": "TODAY"}]

def get_run_totals(day_label):
    """Run Totals: dims from DQ_PTRUN_N_REPORT_03, facts from VW_RUN_TOTALS_FAST_03.
    Join on PACKDATE_RUN_KEY (date+shift) and GROWER_NUMBER - column lookups for clear data model connection.
    Includes RUN_KEY and PACKDATE_RUN_KEY for cross-filtering.
    """
    if not day_label:
        return pd.DataFrame()
    return query(f"""
        SELECT
            p.RUN_KEY AS "RUN_KEY",
            p.PACKDATE_RUN_KEY AS "PACKDATE_RUN_KEY",
            p.RUNS AS "Run",
            v.VARIETY_ABBR AS "Variety",
            p.SHIFT AS "Shift",
            p.GROWER_NUMBER AS "Lot",
            COALESCE(v.BINS_PRE_SHIFT, 0) AS "BinsPreShift",
            COALESCE(v.BINS_ON_SHIFT, 0) AS "BinsOnShift",
            v.BINS_PER_HOUR AS "BinsPerHour",
            v.STAMPER_PPMH AS "StamperPPMH",
            COALESCE(p.BIN_HOUR_TARGET, v.BIN_HOUR_TARGET) AS "BinPerHourTarget",
            COALESCE(p.PACKS_MANHOUR_TARGET, v.PACKS_MANHOUR_TARGET) AS "PacksPerHourManHour",
            v.BINS_TARGET_COLOR AS "BINS_TARGET_COLOR",
            v.PACKS_TARGET_COLOR AS "PACKS_TARGET_COLOR"
        FROM FROSTY.STAGING.DQ_PTRUN_N_REPORT_03 p
        INNER JOIN FROSTY.STAGING.VW_RUN_TOTALS_FAST_03 v
            ON v.PACKDATE_RUN_KEY = p.PACKDATE_RUN_KEY
            AND v.GROWER_NUMBER = p.GROWER_NUMBER
        WHERE p.DAY_LABEL = '{day_label.replace("'", "''")}'
        ORDER BY p.RUNS, p.GROWER_NUMBER
    """)

def get_shift_totals(day_label):
    """Shift Totals from VW_RUN_TOTALS_FAST_03, filtered via PTRUN DAY_LABEL.
    Join on PACKDATE_RUN_KEY (date+shift) - column lookup for clear connection.
    Aggregates by SHIFT and PACKDATE_RUN_KEY (one row per shift).
    Includes PACKDATE_RUN_KEY for cross-filtering.
    """
    if not day_label:
        return pd.DataFrame()
    return query(f"""
        SELECT
            v.PACKDATE_RUN_KEY AS "PACKDATE_RUN_KEY",
            v.SHIFT AS "Shift",
            SUM(COALESCE(v.BINS_ON_SHIFT, 0) + COALESCE(v.BINS_PRE_SHIFT, 0)) AS "TotalBins",
            AVG(v.BINS_PER_HOUR) AS "BinPerHour",
            AVG(v.STAMPER_PPMH) AS "PPMH",
            MAX(v.PACKS_MANHOUR_TARGET) AS "PPMHTarget",
            MAX(v.BIN_HOUR_TARGET) AS "BPHTarget"
        FROM FROSTY.STAGING.DQ_PTRUN_N_REPORT_03 p
        INNER JOIN FROSTY.STAGING.VW_RUN_TOTALS_FAST_03 v
            ON v.PACKDATE_RUN_KEY = p.PACKDATE_RUN_KEY
        WHERE p.DAY_LABEL = '{day_label.replace("'", "''")}'
        GROUP BY v.SHIFT, v.PACKDATE_RUN_KEY
        ORDER BY v.SHIFT
    """)


def get_pidk_bph_chart_data(day_label, grower_number, run_key=None, packdate_run_key=None):
    """Bucket-level Bins Per Hour and target for one grower on the selected day.
    When run_key/packdate_run_key provided, uses them directly; else resolves from PTRUN.
    Filters by DATE_SHIFT_KEY and RUN_KEY for temporal separation (Run 1 morning, Run 2 afternoon).
    """
    if not day_label or grower_number is None or grower_number == "":
        return pd.DataFrame()
    if packdate_run_key:
        pk = str(packdate_run_key).replace("'", "''")
        run_key_esc = str(run_key).replace("'", "''") if run_key else None
        run_filter = f"AND RUN_KEY = '{run_key_esc}'" if run_key_esc else ""
    else:
        esc = str(grower_number).replace("'", "''")
        key_df = query(f"""
            SELECT p."PACKDATE_RUN_KEY" AS "PACKDATE_RUN_KEY", p."RUN_KEY" AS "RUN_KEY"
            FROM FROSTY.STAGING.DQ_PTRUN_N_REPORT_03 p
            WHERE p."DAY_LABEL" = '{day_label.replace("'", "''")}'
              AND p."GROWER_NUMBER" = '{esc}'
            ORDER BY p."RUNS"
            LIMIT 1
        """)
        if key_df.empty:
            return pd.DataFrame()
        row0 = key_df.iloc[0]
        packdate_run_key = row0.get("PACKDATE_RUN_KEY") or row0.get("packdate_run_key")
        if not packdate_run_key:
            return pd.DataFrame()
        pk = str(packdate_run_key).replace("'", "''")
        run_key = row0.get("RUN_KEY") or row0.get("run_key")
        run_key_esc = str(run_key).replace("'", "''") if run_key else None
        run_filter = f"AND RUN_KEY = '{run_key_esc}'" if run_key_esc else ""
    return query(f"""
        SELECT
            BUCKET_START,
            SUM(BINS_PER_HOUR)     AS BINS_PER_HOUR,
            SUM(BIN_HOUR_TARGET)   AS BIN_HOUR_TARGET
        FROM FROSTY.STAGING.DT_SHIFT_10MIN_KPI_A_PER_RUN03_DT
        WHERE DATE_SHIFT_KEY = '{pk}'
        {run_filter}
        AND MINUTES_WORKED_ALLOC > 0
        GROUP BY BUCKET_START
        ORDER BY BUCKET_START
    """)


# Power BI–style: first grower = lighter blue, second = dark blue, Target = orange
_BPH_BAR_COLORS = ["#64B5F6", "#1565C0", "#42A5F5", "#1E88E5", "#0D47A1"]


def build_pidk_bph_chart_all_growers(grower_dfs):
    """Bin Per Hour by grower: one bar series per grower (distinct colors), one Target line.
    Matches Power BI: light blue / dark blue bars by grower, orange target line, x-axis "Bin Dumper".
    Title is on the card only (no duplicate chart title).
    """
    layout = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#fff", size=11),
        title=None,
        showlegend=True,
        legend=dict(orientation="h", y=1.12, x=0, traceorder="normal"),
        margin=dict(l=50, r=50, t=24, b=45),
        height=320,
        barmode="stack",
        xaxis=dict(
            title="Bin Dumper",
            gridcolor="#2a2a2a",
            showgrid=True,
        ),
        yaxis=dict(
            title="Bins Per Hour",
            gridcolor="#2a2a2a",
            showgrid=True,
        ),
    )
    fig = go.Figure(layout=layout)
    if not grower_dfs:
        fig.add_annotation(
            text="No data",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color="#666", size=18),
        )
        return fig
    # Collect (BUCKET_START, BIN_HOUR_TARGET) from all growers so Target spans full day
    target_x, target_y = [], []
    for idx, (grower_number, df) in enumerate(grower_dfs):
        if df is None or df.empty:
            continue
        cols = [c.upper() for c in df.columns]
        x_col = df.columns[cols.index("BUCKET_START")] if "BUCKET_START" in cols else df.columns[0]
        y_col = df.columns[cols.index("BINS_PER_HOUR")] if "BINS_PER_HOUR" in cols else df.columns[1]
        t_col = df.columns[cols.index("BIN_HOUR_TARGET")] if "BIN_HOUR_TARGET" in cols else (df.columns[2] if len(df.columns) > 2 else None)
        x = df[x_col]
        y = df[y_col].fillna(0)
        color = _BPH_BAR_COLORS[idx % len(_BPH_BAR_COLORS)]
        fig.add_trace(go.Bar(
            x=x, y=y,
            name=str(grower_number),
            marker_color=color,
            legendgroup=str(grower_number),
        ))
        if t_col and t_col in df.columns:
            target_x.extend(df[x_col].tolist())
            target_y.extend(df[t_col].tolist())
    # One Target trace spanning all growers' buckets, sorted by time
    if target_x and target_y:
        combined = sorted(zip(target_x, target_y), key=lambda r: r[0])
        tx, ty = [r[0] for r in combined], [r[1] for r in combined]
        fig.add_trace(go.Scatter(
            x=tx, y=ty,
            mode="lines",
            line=dict(color="#FF9800", width=2.5),
            name="Target",
            connectgaps=True,
        ))
    return fig


# ── Sizer Profile (header → PTRUN by SHIFT_KEY/RUN_KEY; EventId → drops) ─
# Uses DQ_APPLE_SIZER_HEADER_VIEW_03 (BatchID, EventId, SHIFT_KEY) and DQ_APPLE_SIZER_DROPSUMMARY_03 (EventId, GradeName, SizeName, weight_dec)
# Lookup: Header EventId = Drops EventId (BatchID is display only).

def get_sizer_events_for_day(day_label, run_key=None, packdate_run_key=None):
    """Batches/events for the day: join sizer header to PTRUN by RUN_KEY = SHIFT_KEY, filter by DAY_LABEL.
    Optional run_key or packdate_run_key to filter to a specific run or shift.
    Returns DataFrame with BatchID, EventId, SHIFT_KEY, and label columns for dropdown.
    """
    if not day_label:
        return pd.DataFrame()
    run_filter = f"AND p.\"RUN_KEY\" = '{str(run_key).replace(chr(39), chr(39)+chr(39))}'" if run_key else ""
    packdate_filter = f"AND p.\"PACKDATE_RUN_KEY\" = '{str(packdate_run_key).replace(chr(39), chr(39)+chr(39))}'" if packdate_run_key else ""
    extra = run_filter or packdate_filter
    q = f"""
        SELECT h."BatchID" AS "BatchID", h."EventId" AS "EventId", h."SHIFT_KEY" AS "SHIFT_KEY", h."GrowerCode" AS "GrowerCode",
               h."VarietyName" AS "VarietyName", h."StartTime" AS "StartTime", h."SHIFT_CODE" AS "SHIFT_CODE"
        FROM FROSTY.STAGING.DQ_APPLE_SIZER_HEADER_VIEW_03 h
        INNER JOIN FROSTY.STAGING.DQ_PTRUN_N_REPORT_03 p
            ON p."RUN_KEY" = h."SHIFT_KEY"
        WHERE p."DAY_LABEL" = '{day_label.replace("'", "''")}'
        {extra}
        ORDER BY h."StartTime" DESC, h."BatchID"
    """
    df = query(q)
    if not df.empty:
        return df
    q2 = f"""
        SELECT h."BatchID" AS "BatchID", h."EventId" AS "EventId", h."SHIFT_KEY" AS "SHIFT_KEY", h."GrowerCode" AS "GrowerCode",
               h."VarietyName" AS "VarietyName", h."StartTime" AS "StartTime", h."SHIFT_CODE" AS "SHIFT_CODE"
        FROM FROSTY.STAGING.DQ_APPLE_SIZER_HEADER_VIEW_03 h
        INNER JOIN FROSTY.STAGING.DQ_PTRUN_N_REPORT_03 p
            ON h."SHIFT_KEY" LIKE p."PACKDATE_RUN_KEY" || '%'
        WHERE p."DAY_LABEL" = '{day_label.replace("'", "''")}'
        {extra}
        ORDER BY h."StartTime" DESC, h."BatchID"
    """
    return query(q2)


def get_sizer_events_with_event_ids(day_label, run_key=None, packdate_run_key=None):
    """List of events for dropdown: use EventId from header for drops lookup (EventId = EventId).
    Optional run_key or packdate_run_key to filter to a specific run or shift.
    Drops table (DQ_APPLE_SIZER_DROPSUMMARY_03) has EventId only. BatchID is display only.
    Returns list of dicts: {event_id, batch_id, label} for dropdown.
    """
    header_df = get_sizer_events_for_day(day_label, run_key=run_key, packdate_run_key=packdate_run_key)
    if header_df.empty:
        return []
    bid_col = "BatchID" if "BatchID" in header_df.columns else ("BATCHID" if "BATCHID" in header_df.columns else header_df.columns[0])
    eid_col = "EventId" if "EventId" in header_df.columns else ("EVENTID" if "EVENTID" in header_df.columns else None)
    start_col = "StartTime" if "StartTime" in header_df.columns else ("STARTTIME" if "STARTTIME" in header_df.columns else None)
    out = []
    for _, r in header_df.iterrows():
        bid = r.get(bid_col)
        eid = r.get(eid_col) if eid_col else None
        if eid is None or (isinstance(eid, float) and pd.isna(eid)):
            continue
        start = r.get(start_col) if start_col else None
        label = f"Batch {bid}"
        if start is not None and str(start) != "NaT":
            try:
                label = f"Batch {bid} · {pd.Timestamp(start).strftime('%m/%d %I:%M %p')}"
            except Exception:
                pass
        out.append({"event_id": str(eid), "batch_id": bid, "label": label})
    return out


def get_sizer_drops_for_event(event_id, batch_id=None):
    """Drops for one event: GradeName, SizeName, weight_dec for matrix.
    DQ_APPLE_SIZER_DROPSUMMARY_03 has EventId only; filter by EventId.
    """
    safe_id = str(event_id).replace("'", "''")
    df = query(f"""
        SELECT "GradeName" AS "GradeName", "SizeName" AS "SizeName",
               SUM(COALESCE("weight_dec", "WEIGHT", 0)) AS "WEIGHT"
        FROM FROSTY.STAGING.DQ_APPLE_SIZER_DROPSUMMARY_03
        WHERE "EventId" = '{safe_id}'
        GROUP BY "GradeName", "SizeName"
        ORDER BY "GradeName", "SizeName"
    """)
    return df


def get_sizer_drops_for_all_events(day_label, run_key=None, packdate_run_key=None):
    """Aggregate drops from all events for the day (and optional run/shift filter)."""
    events = get_sizer_events_with_event_ids(day_label, run_key=run_key, packdate_run_key=packdate_run_key)
    if not events:
        return pd.DataFrame()
    dfs = []
    for e in events:
        df = get_sizer_drops_for_event(e["event_id"])
        if df is not None and not df.empty:
            dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    combined = pd.concat(dfs, ignore_index=True)
    agg = combined.groupby(["GradeName", "SizeName"], as_index=False).agg({"WEIGHT": "sum"})
    return agg


def _size_sort_key(x):
    try:
        return int(str(x).strip())
    except (ValueError, TypeError):
        return 999


def _get_gradient_color(value, min_val=0, max_val=20):
    """Blue gradient for matrix cells (Power BI style)."""
    if value is None or value <= min_val:
        return "#ecf0f1"
    ratio = min((float(value) - min_val) / (max_val - min_val), 1.0)
    r = int(255 - (255 - 100) * ratio)
    g = int(255 - (255 - 149) * ratio)
    b = int(255 - (255 - 237) * ratio)
    return f"rgb({r},{g},{b})"


def build_sizer_matrix(drops_df):
    """Pivot GradeName (or PACKOUT_GROUP) x SizeName, percentages, row/col totals. Returns (pct_pivot, row_totals, col_totals) or (None,*)."""
    if drops_df is None or drops_df.empty:
        return None, None, None
    row_col = "GradeName" if "GradeName" in drops_df.columns else ("PACKOUT_GROUP" if "PACKOUT_GROUP" in drops_df.columns else drops_df.columns[0])
    col_col = "SizeName" if "SizeName" in drops_df.columns else ("SIZENAME" if "SIZENAME" in drops_df.columns else drops_df.columns[1])
    val_col = "WEIGHT" if "WEIGHT" in drops_df.columns else ("weight_dec" if "weight_dec" in drops_df.columns else drops_df.columns[2])
    pivot = drops_df.pivot_table(index=row_col, columns=col_col, values=val_col, aggfunc="sum", fill_value=0)
    size_cols = sorted(pivot.columns, key=_size_sort_key)
    pivot = pivot.reindex(columns=size_cols).fillna(0)
    pivot = pivot.sort_index()
    total_weight = pivot.values.sum()
    if total_weight == 0:
        return None, None, None
    pct_pivot = (pivot / total_weight * 100)
    row_totals = pct_pivot.sum(axis=1)
    col_totals = pct_pivot.sum(axis=0)
    return pct_pivot, row_totals, col_totals


def _build_sizer_matrix_table(drops_df):
    """Render Sizer Profile matrix: rows=GradeName, columns=SizeName, gradient cells, Total row/col."""
    if drops_df is None or drops_df.empty:
        return html.P("No data", style={"color": "#999", "textAlign": "center", "padding": "16px"})
    pct_pivot, row_totals, col_totals = build_sizer_matrix(drops_df)
    if pct_pivot is None:
        return html.P("No data", style={"color": "#999", "textAlign": "center", "padding": "16px"})
    size_columns = list(pct_pivot.columns)
    grade_names = list(pct_pivot.index)
    _ths = {"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "center", "color": "#000", "backgroundColor": "#e9ecef"}
    header_row = [html.Th("Packout Group", style={**_ths, "textAlign": "left"})] + [html.Th(str(s), style=_ths) for s in size_columns] + [html.Th("Total", style=_ths)]
    rows = [html.Tr(header_row)]
    for grade in grade_names:
        row_cells = [html.Td(grade, style={"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "left", "fontWeight": "600", "color": "#000", "backgroundColor": "#fff"})]
        for size in size_columns:
            val = pct_pivot.loc[grade, size]
            if pd.isna(val) or val is None:
                val = 0.0
            val = float(val)
            bg = _get_gradient_color(val) if val > 0 else "#ecf0f1"
            text = f"{val:.2f}%"
            row_cells.append(html.Td(text, style={"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "center", "backgroundColor": bg, "color": "#333"}))
        row_total_val = row_totals[grade]
        if pd.isna(row_total_val) or row_total_val is None:
            row_total_val = 0.0
        row_cells.append(html.Td(f"{float(row_total_val):.2f}%", style={"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "center", "backgroundColor": "#ecf0f1", "fontWeight": "600", "color": "#333"}))
        rows.append(html.Tr(row_cells))
    total_row = [html.Td("Total", style={"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "left", "fontWeight": "600", "backgroundColor": "#ecf0f1", "color": "#333"})]
    for size in size_columns:
        val = col_totals[size]
        if pd.isna(val) or val is None:
            val = 0.0
        total_row.append(html.Td(f"{float(val):.2f}%", style={"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "center", "backgroundColor": "#ecf0f1", "fontWeight": "600", "color": "#333"}))
    total_row.append(html.Td("100.00%", style={"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "center", "backgroundColor": "#ecf0f1", "fontWeight": "600", "color": "#333"}))
    rows.append(html.Tr(total_row))
    return html.Table(
        [html.Thead(rows[0]), html.Tbody(rows[1:])],
        style={"width": "100%", "color": "#000", "borderCollapse": "collapse", "fontSize": "0.75rem"},
        className="pidk-sizer-matrix",
    )


# ── EQ Matrix (Computech Carton Palletized) & Package Type ─────────────
def get_eq_data(day_label, run_key=None, packdate_run_key=None):
    """Shared EQ data: DQ_EQ_WITH_KEYS03 INNER JOIN PTRUN, LEFT OUTER JOIN PACK_CLASSIFICATION on PACK_ABBR.
    Returns raw rows for use by both EQ matrix and Package Type table."""
    if not day_label:
        return pd.DataFrame()
    dl = str(day_label).replace("'", "''")
    extra = ""
    if run_key:
        rk = str(run_key).replace("'", "''")
        extra = f" AND p.RUN_KEY = '{rk}'"
    elif packdate_run_key:
        pk = str(packdate_run_key).replace("'", "''")
        extra = f" AND p.PACKDATE_RUN_KEY = '{pk}'"
    return query(f"""
        SELECT
            TRIM(e.PACK_ABBR) AS PACK_ABBR,
            TRIM(e.GRADE_ABBR) AS GRADE_ABBR,
            COALESCE(e.CARTONS, 0) AS CARTONS,
            COALESCE(e.EQ_ON_HAND, e.CARTONS, 0) AS EQ_VAL,
            COALESCE(NULLIF(TRIM(pc.CLASSIFICATION), ''), 'Unclassified') AS CLASSIFICATION
        FROM FROSTY.STAGING.DQ_EQ_WITH_KEYS03 e
        INNER JOIN FROSTY.STAGING.DQ_PTRUN_N_REPORT_03 p
          ON p.RUN_KEY = e.RUN_KEY
          AND p.DAY_LABEL = '{dl}'
        LEFT OUTER JOIN FROSTY.STAGING.PACK_CLASSIFICATION pc
          ON UPPER(TRIM(e.PACK_ABBR)) = UPPER(TRIM(pc.PACK_ABBR))
        WHERE 1=1
        {extra}
    """)


def _filter_eq_by_classification(eq_df, classification):
    """Filter eq_df to rows matching CLASSIFICATION. None/empty = no filter."""
    if eq_df is None or eq_df.empty or not classification:
        return eq_df
    df = eq_df.copy()
    df.columns = [c.strip().upper() if isinstance(c, str) else c for c in df.columns]
    if "CLASSIFICATION" not in df.columns:
        return eq_df
    g = df["CLASSIFICATION"].fillna("Unclassified").astype(str).str.strip()
    return df[g == str(classification)]


def build_eq_matrix(eq_df):
    """Pivot PACK_ABBR x GRADE_ABBR (aggregated over raw rows). Returns (pivot, row_totals, col_totals) or (None,*)."""
    if eq_df is None or eq_df.empty:
        return None, None, None
    df = eq_df.copy()
    df.columns = [c.strip().upper() if isinstance(c, str) else c for c in df.columns]
    if "CARTONS" not in df.columns:
        return None, None, None
    pivot = df.pivot_table(
        index="PACK_ABBR", columns="GRADE_ABBR", values="CARTONS", aggfunc="sum", fill_value=0
    )
    pivot = pivot.sort_index()
    row_totals = pivot.sum(axis=1)
    col_totals = pivot.sum(axis=0)
    return pivot, row_totals, col_totals


def _build_eq_matrix_table(eq_df):
    """Render Computech Carton Palletized: single table like Sizer Profile. Grade on columns, Package as rows."""
    if eq_df is None or eq_df.empty:
        return html.P("No data", style={"color": "#999", "textAlign": "center", "padding": "16px"})
    pivot, row_totals, col_totals = build_eq_matrix(eq_df)
    if pivot is None:
        return html.P("No data", style={"color": "#999", "textAlign": "center", "padding": "16px"})
    grade_cols = list(pivot.columns)
    pack_abbrs = list(pivot.index)
    max_val = pivot.values.max() if pivot.size else 0
    _ths = {"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "center", "color": "#000", "backgroundColor": "#e9ecef"}
    _tds = {"padding": "6px 8px", "fontSize": "0.75rem"}
    header_row = [html.Th("Pack", style={**_ths, "textAlign": "left"})] + [html.Th(str(g), style=_ths) for g in grade_cols] + [html.Th("Total", style=_ths)]
    rows = [html.Tr(header_row)]
    for pack in pack_abbrs:
        rt = row_totals[pack]
        pack_total = int(rt) if not pd.isna(rt) else 0
        row_cells = [html.Td(pack, style={**_tds, "textAlign": "left", "fontWeight": "600", "color": "#000", "backgroundColor": "#fff"})]
        for g in grade_cols:
            val = int(pivot.loc[pack, g]) if g in pivot.columns else 0
            bg = _get_gradient_color(float(val), min_val=0, max_val=max(1, max_val)) if val > 0 else "#ecf0f1"
            row_cells.append(html.Td(str(val), style={**_tds, "textAlign": "center", "backgroundColor": bg, "color": "#333"}))
        row_cells.append(html.Td(str(pack_total), style={**_tds, "textAlign": "center", "backgroundColor": "#ecf0f1", "fontWeight": "600", "color": "#333"}))
        rows.append(html.Tr(row_cells))
    total_row = [html.Td("Total", style={**_tds, "textAlign": "left", "fontWeight": "600", "backgroundColor": "#ecf0f1", "color": "#333"})]
    for grade in grade_cols:
        v = col_totals[grade]
        total_row.append(html.Td(str(int(v)) if not pd.isna(v) else "0", style={**_tds, "textAlign": "center", "backgroundColor": "#ecf0f1", "fontWeight": "600", "color": "#333"}))
    grand = pivot.values.sum()
    total_row.append(html.Td(str(int(grand)), style={**_tds, "textAlign": "center", "backgroundColor": "#ecf0f1", "fontWeight": "600", "color": "#333"}))
    rows.append(html.Tr(total_row))
    return html.Table(
        [html.Thead(rows[0]), html.Tbody(rows[1:])],
        style={"width": "100%", "color": "#000", "borderCollapse": "collapse", "fontSize": "0.75rem"},
        className="pidk-eq-matrix",
    )


# ── Package Type Table (derived from get_eq_data) ──────────────────────
def _eq_data_to_package_type_df(eq_df):
    """Aggregate raw EQ data by CLASSIFICATION (fillna Unclassified). Returns Group, eq_sum."""
    if eq_df is None or eq_df.empty:
        return pd.DataFrame()
    df = eq_df.copy()
    df.columns = [c.strip().upper() if isinstance(c, str) else c for c in df.columns]
    if "EQ_VAL" not in df.columns:
        return pd.DataFrame()
    if "CLASSIFICATION" not in df.columns:
        df["CLASSIFICATION"] = None
    # Treat null/blank/whitespace classifications as Unclassified.
    df["grp"] = (
        df["CLASSIFICATION"]
        .astype(str)
        .str.strip()
        .replace({"": "Unclassified", "NONE": "Unclassified", "NAN": "Unclassified"})
    )
    df.loc[df["CLASSIFICATION"].isna(), "grp"] = "Unclassified"
    agg = df.groupby("grp", as_index=False).agg({"EQ_VAL": "sum"})
    agg.columns = ["Group", "eq_sum"]
    return agg.sort_values("Group")


def _build_package_type_table(pkg_df, selected_package_type=None):
    """Render Package Type: Group | % (weighted share). Group names are clickable; Power BI–style filter."""
    no_data_style = {"color": "#999", "textAlign": "center", "padding": "16px", "fontSize": "0.9rem"}
    if pkg_df is None or pkg_df.empty:
        return html.P("No data — select a day with EQ runs", style=no_data_style)
    pkg_df = pkg_df.copy()
    pkg_df.columns = [c.strip().lower() if isinstance(c, str) else c for c in pkg_df.columns]
    sum_col = "eq_sum"
    grp_col = "group"
    if sum_col not in pkg_df.columns:
        return html.P("No data — select a day with EQ runs", style=no_data_style)
    # Snowflake numeric values may arrive as Decimal; coerce to float-safe numeric
    pkg_df[sum_col] = pd.to_numeric(pkg_df[sum_col], errors="coerce").fillna(0.0)
    total = float(pkg_df[sum_col].sum())
    if total == 0:
        return html.P("No data — select a day with EQ runs", style=no_data_style)
    pkg_df["pct"] = 100.0 * pkg_df[sum_col] / total
    _ths = {"padding": "6px 8px", "fontSize": "0.75rem", "color": "#ccc", "backgroundColor": "#34495e"}
    _btn = {"background": "none", "border": "none", "color": "#ccc", "cursor": "pointer", "fontSize": "0.75rem", "padding": 0, "textAlign": "left", "width": "100%"}
    header_row = [html.Th("Group", style={**_ths, "textAlign": "left"}), html.Th("%", style={**_ths, "textAlign": "right"})]
    rows = [html.Tr(header_row)]
    # "All" row - click to clear filter (Power BI style)
    all_selected = selected_package_type is None
    all_style = {**_btn, "color": "#1565C0" if all_selected else "#ccc", "fontWeight": "600" if all_selected else "normal"}
    rows.append(html.Tr([
        html.Td(html.Button("All", id={"type": "pidk-pkg-filter-btn", "index": "All"}, n_clicks=0, style=all_style), style={"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "left"}),
        html.Td("—", style={"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "right", "color": "#ccc"}),
    ]))
    for _, r in pkg_df.iterrows():
        grp = str(r.get(grp_col, r.get("Group", "")))
        pct = r["pct"]
        sel = selected_package_type is not None and str(selected_package_type) == grp
        btn_style = {**_btn, "color": "#1565C0" if sel else "#ccc", "fontWeight": "600" if sel else "normal"}
        rows.append(html.Tr([
            html.Td(html.Button(grp, id={"type": "pidk-pkg-filter-btn", "index": grp}, n_clicks=0, style=btn_style), style={"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "left"}),
            html.Td(f"{float(pct):.2f}%", style={"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "right", "color": "#ccc"}),
        ]))
    rows.append(html.Tr([
        html.Td("Total", style={"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "left", "fontWeight": "600", "backgroundColor": "#ecf0f1", "color": "#333"}),
        html.Td("100.00%", style={"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "right", "fontWeight": "600", "backgroundColor": "#ecf0f1", "color": "#333"}),
    ]))
    return html.Table(
        [html.Thead(rows[0]), html.Tbody(rows[1:])],
        style={"width": "100%", "color": "#ccc", "borderCollapse": "collapse", "fontSize": "0.75rem"},
        className="pidk-package-type-table",
    )


# ── Employee Count Summary ────────────────────────────────────────────
def get_employee_summary_data(day_label, packdate_run_key=None):
    """DT rows for Employee Summary. Filter by DATE_SHIFT_KEY from PTRUN for the day; optionally by packdate_run_key."""
    if not day_label:
        return pd.DataFrame()
    key_df = query(f"""
        SELECT DISTINCT p.PACKDATE_RUN_KEY
        FROM FROSTY.STAGING.DQ_PTRUN_N_REPORT_03 p
        WHERE p.DAY_LABEL = '{day_label.replace("'", "''")}'
        ORDER BY p.PACKDATE_RUN_KEY
    """)
    if key_df.empty:
        return pd.DataFrame()
    if packdate_run_key:
        keys = [str(packdate_run_key)]
    else:
        keys = key_df["PACKDATE_RUN_KEY"].dropna().astype(str).unique().tolist()
    if not keys:
        return pd.DataFrame()
    in_list = ",".join([f"'{str(k).replace(chr(39), chr(39)+chr(39))}'" for k in keys])
    return query(f"""
        SELECT SHIFT, DATE_SHIFT_KEY, BUCKET_START, EMPLOYEE_COUNT_ALLOC, MINUTES_WORKED_ALLOC,
               STAMPER_EQS, PACKS_MANHOUR_TARGET
        FROM FROSTY.STAGING.DT_SHIFT_10MIN_KPI_A_PER_RUN03_DT
        WHERE DATE_SHIFT_KEY IN ({in_list})
        ORDER BY DATE_SHIFT_KEY, BUCKET_START
    """)


def _compute_employee_summary(df):
    """Per-shift: MaxEmployees, CurrentEmployees, ReduceToHitTarget. Returns list of dicts."""
    if df is None or df.empty:
        return []
    out = []
    for (shift_key, shift_val), grp in df.groupby(["DATE_SHIFT_KEY", "SHIFT"]):
        g = grp.copy()
        g = g.astype({"EMPLOYEE_COUNT_ALLOC": float, "MINUTES_WORKED_ALLOC": float, "STAMPER_EQS": float, "PACKS_MANHOUR_TARGET": float}, errors="ignore")
        with_emp = g[g["EMPLOYEE_COUNT_ALLOC"] > 0]
        if with_emp.empty:
            out.append({"shift": str(shift_val), "max_emp": 0, "current_emp": 0, "reduce": 0})
            continue
        max_emp = float(with_emp["EMPLOYEE_COUNT_ALLOC"].max())
        latest_bucket = with_emp["BUCKET_START"].max()
        current_emp = float(g[g["BUCKET_START"] == latest_bucket]["EMPLOYEE_COUNT_ALLOC"].sum())
        cum_mask = g["BUCKET_START"] <= latest_bucket
        cum_stamper = float(g.loc[cum_mask, "STAMPER_EQS"].sum())
        cum_hrs = float(g.loc[cum_mask, "MINUTES_WORKED_ALLOC"].sum()) / 60
        cum_ppmh = cum_stamper / cum_hrs if cum_hrs > 0 else 0
        target_ppmh = float(g["PACKS_MANHOUR_TARGET"].dropna().mean()) if g["PACKS_MANHOUR_TARGET"].notna().any() else 0
        pct_diff = (cum_ppmh - target_ppmh) / target_ppmh if target_ppmh and target_ppmh != 0 else 0
        raw_reduce = max_emp * pct_diff
        emp_to_reduce = 0 if raw_reduce > 0 else abs(raw_reduce)
        reduce_to_hit = max(0, round(emp_to_reduce) - (max_emp - current_emp))
        out.append({"shift": str(shift_val), "max_emp": int(max_emp), "current_emp": int(current_emp), "reduce": int(reduce_to_hit)})
    return out


def _build_employee_summary_table(summary_list):
    """Render Employee Count: SHIFT | Max Employees, Current Employees, Reduce to Hit Target (multiline)."""
    if not summary_list:
        return html.P("No data — select a day with shift data", style={"color": "#999", "textAlign": "center", "padding": "16px", "fontSize": "0.9rem"})
    _ths = {"padding": "6px 8px", "fontSize": "0.75rem", "color": "#ccc", "backgroundColor": "#34495e"}
    header_row = [html.Th("Shift", style={**_ths, "textAlign": "left"}), html.Th("Employee", style={**_ths, "textAlign": "left"})]
    rows = [html.Tr(header_row)]
    for s in summary_list:
        txt = f"Max Employees: {s['max_emp']}\nCurrent Employees: {s['current_emp']}\nReduce to Hit Target: {s['reduce']}"
        rows.append(html.Tr([
            html.Td(s["shift"], style={"padding": "6px 8px", "fontSize": "0.75rem", "color": "#ccc", "verticalAlign": "top"}),
            html.Td(txt.replace("\n", "\n"), style={"padding": "6px 8px", "fontSize": "0.75rem", "color": "#ccc", "whiteSpace": "pre-line"}),
        ]))
    return html.Table(
        [html.Thead(rows[0]), html.Tbody(rows[1:])],
        style={"width": "100%", "color": "#ccc", "borderCollapse": "collapse", "fontSize": "0.75rem"},
        className="pidk-employee-summary",
    )


# ── Cache for background refresh (TV-style) ────────────────────────────
_pidk_cache = {}
_pidk_cache_lock = threading.Lock()


def _build_pidk_payload(day_label):
    """Build full PIDK payload (run_table, shift_table, last_updated, run_data, shift_data) for a given day_label."""
    if not day_label:
        day_label = "TODAY"
    run_df = get_run_totals(day_label)
    shift_df = get_shift_totals(day_label)
    run_df = _normalize_df_columns(run_df, _RUN_COL_MAP)
    shift_df = _normalize_df_columns(shift_df, _SHIFT_COL_MAP)
    run_table = _build_run_totals_table(run_df)
    shift_table = _build_shift_totals_table(shift_df)
    last_updated = f"Last updated: {datetime.now().strftime('%I:%M:%S %p')} · Refreshes every 5 min"
    run_data = run_df.to_dict("records") if not run_df.empty else []
    shift_data = shift_df.to_dict("records") if not shift_df.empty else []
    return (run_table, shift_table, last_updated, run_data, shift_data)


def _refresh_cache_pidk_today():
    """Refresh in-memory cache for TODAY. Runs in background thread."""
    try:
        payload = _build_pidk_payload("TODAY")
        with _pidk_cache_lock:
            _pidk_cache["TODAY"] = payload
    except Exception as e:
        print(f"PIDK cache refresh error: {e}")


def _pidk_background_worker():
    """Daemon thread: refresh cache for TODAY every 5 minutes."""
    _refresh_cache_pidk_today()
    while True:
        time.sleep(300)
        _refresh_cache_pidk_today()


_pidk_background_thread = threading.Thread(target=_pidk_background_worker, daemon=True)
_pidk_background_thread.start()

# ── Layout ─────────────────────────────────────────────────────────────
def _fmt(val, dec=1):
    if val is None:
        return "—"
    try:
        return f"{float(val):,.{dec}f}"
    except (ValueError, TypeError):
        return "—"

_hex_to_class = {"#C8E6C9": "tv-cell-green", "#FFF9C4": "tv-cell-yellow", "#FFCDD2": "tv-cell-red"}

def _cell(val, hex_color=None, dec=1):
    _cs = {"padding": "6px 10px", "textAlign": "center", "fontSize": "0.85rem"}
    if hex_color:
        cls = _hex_to_class.get((hex_color or "").upper())
        if cls:
            return html.Td(_fmt(val, dec), className=cls, style={**_cs, "textAlign": "center"})
        return html.Td(_fmt(val, dec), style={**_cs, "backgroundColor": hex_color, "color": "#000", "fontWeight": "600", "textAlign": "center"})
    return html.Td(_fmt(val, dec), style=_cs)

def _build_run_totals_table(df):
    if df is None or df.empty:
        return html.P("No data", style={"color": "#999", "textAlign": "center", "padding": "16px"})
    display_df = df.copy()
    display_df["BPH_BG"] = ""
    display_df["PPMH_BG"] = ""
    for idx in display_df.index:
        r = display_df.loc[idx]
        display_df.at[idx, "BPH_BG"] = _normalize_cell_color(r.get("BINS_TARGET_COLOR")) or color_bar_powerbi(r.get("BinsPerHour"), r.get("BinPerHourTarget"))
        display_df.at[idx, "PPMH_BG"] = _normalize_cell_color(r.get("PACKS_TARGET_COLOR")) or color_bar_powerbi(r.get("StamperPPMH"), r.get("PacksPerHourManHour"))
    cond = []
    for i in range(len(display_df)):
        bph_bg = display_df.iloc[i]["BPH_BG"]
        ppmh_bg = display_df.iloc[i]["PPMH_BG"]
        if bph_bg:
            cond.append({"if": {"row_index": i, "column_id": "BinsPerHour"}, "backgroundColor": bph_bg, "color": "#000", "fontWeight": "600"})
        if ppmh_bg:
            cond.append({"if": {"row_index": i, "column_id": "StamperPPMH"}, "backgroundColor": ppmh_bg, "color": "#000", "fontWeight": "600"})
    cols = [
        {"id": "Run", "name": "Run", "type": "text"},
        {"id": "Variety", "name": "Variety", "type": "text"},
        {"id": "Shift", "name": "Shift", "type": "text"},
        {"id": "Lot", "name": "Lot", "type": "text"},
        {"id": "BinsPreShift", "name": "Bins Pre", "type": "numeric"},
        {"id": "BinsOnShift", "name": "Bins On", "type": "text"},
        {"id": "BinsPerHour", "name": "BPH", "type": "numeric", "format": {"specifier": ",.1f"}},
        {"id": "StamperPPMH", "name": "PPMH", "type": "numeric", "format": {"specifier": ",.1f"}},
        {"id": "BinPerHourTarget", "name": "BPH Target", "type": "numeric", "format": {"specifier": ",.1f"}},
        {"id": "PacksPerHourManHour", "name": "PPMH Target", "type": "numeric", "format": {"specifier": ",.1f"}},
        {"id": "RUN_KEY", "name": "RUN_KEY", "type": "text"},
        {"id": "PACKDATE_RUN_KEY", "name": "PACKDATE_RUN_KEY", "type": "text"},
    ]
    data = display_df.to_dict("records")
    for row in data:
        for k, v in row.items():
            if pd.isna(v):
                row[k] = "—"
            elif k == "BinsOnShift" and (v == 0 or (isinstance(v, (int, float)) and v == 0)):
                row[k] = "Scheduled"
            elif isinstance(v, (int, float)) and k == "BinsPreShift":
                row[k] = int(v) if v == v else "—"
    return dt.DataTable(
        data=data,
        columns=cols,
        hidden_columns=["RUN_KEY", "PACKDATE_RUN_KEY"],
        row_selectable="single",
        selected_rows=[],
        id="pidk-run-totals-datatable",
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": "#e9ecef", "color": "#000", "fontSize": "0.8rem"},
        style_cell={"backgroundColor": "#fff", "color": "#000", "fontSize": "0.85rem", "textAlign": "center", "padding": "6px 10px"},
        style_data_conditional=cond,
        style_as_list_view=True,
    )

def _build_shift_totals_table(df):
    if df is None or df.empty:
        return html.P("No data", style={"color": "#999", "textAlign": "center", "padding": "16px"})
    display_df = df.copy()
    display_df["BPH_BG"] = ""
    display_df["PPMH_BG"] = ""
    for idx in display_df.index:
        r = display_df.loc[idx]
        display_df.at[idx, "BPH_BG"] = color_bar_powerbi(r.get("BinPerHour"), r.get("BPHTarget"))
        display_df.at[idx, "PPMH_BG"] = color_bar_powerbi(r.get("PPMH"), r.get("PPMHTarget"))
    cond = []
    for i in range(len(display_df)):
        bph_bg = display_df.iloc[i]["BPH_BG"]
        ppmh_bg = display_df.iloc[i]["PPMH_BG"]
        if bph_bg:
            cond.append({"if": {"row_index": i, "column_id": "BinPerHour"}, "backgroundColor": bph_bg, "color": "#000", "fontWeight": "600"})
        if ppmh_bg:
            cond.append({"if": {"row_index": i, "column_id": "PPMH"}, "backgroundColor": ppmh_bg, "color": "#000", "fontWeight": "600"})
    cols = [
        {"id": "Shift", "name": "Shift", "type": "text"},
        {"id": "TotalBins", "name": "Total Bins", "type": "numeric"},
        {"id": "BinPerHour", "name": "Bin Per Hour", "type": "numeric", "format": {"specifier": ",.1f"}},
        {"id": "PPMH", "name": "PPMH", "type": "numeric", "format": {"specifier": ",.1f"}},
        {"id": "PPMHTarget", "name": "PPMH Target", "type": "numeric", "format": {"specifier": ",.1f"}},
        {"id": "BPHTarget", "name": "BPH Target", "type": "numeric", "format": {"specifier": ",.1f"}},
        {"id": "PACKDATE_RUN_KEY", "name": "PACKDATE_RUN_KEY", "type": "text"},
    ]
    data = display_df.to_dict("records")
    for row in data:
        for k, v in row.items():
            if pd.isna(v):
                row[k] = "—"
            elif isinstance(v, (int, float)) and k == "TotalBins":
                row[k] = int(v) if v == v else "—"
    return dt.DataTable(
        data=data,
        columns=cols,
        hidden_columns=["PACKDATE_RUN_KEY"],
        row_selectable="single",
        selected_rows=[],
        id="pidk-shift-totals-datatable",
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": "#e9ecef", "color": "#000", "fontSize": "0.8rem"},
        style_cell={"backgroundColor": "#fff", "color": "#000", "fontSize": "0.85rem", "textAlign": "center", "padding": "6px 10px"},
        style_data_conditional=cond,
        style_as_list_view=True,
    )

layout = html.Div([
    dcc.Interval(id="pidk-interval", interval=300_000, n_intervals=0),
    dcc.Store(id="pidk-day-store", data="TODAY"),
    dcc.Store(id="pidk-selected-run", data=None),
    dcc.Store(id="pidk-selected-shift", data=None),
    dcc.Store(id="pidk-selected-package-type", data=None),
    dcc.Store(id="pidk-run-data", data=[]),
    dcc.Store(id="pidk-shift-data", data=[]),
    dbc.Container([
        # Header row - same format as tv_display: back, title, dropdown + last updated
        dbc.Row([
            dbc.Col(
                html.A("← Back", href="/", style={
                    "color": "#aaa", "fontSize": "0.95rem", "textDecoration": "none",
                    "display": "inline-flex", "alignItems": "center",
                }),
                width=2, className="d-flex align-items-center"
            ),
            dbc.Col(html.H5("Production Intra Day KPIs", style={
                "color": "white", "margin": "0", "fontSize": "clamp(0.9rem, 2vw, 1.1rem)",
                "textAlign": "center",
            }), width=8, className="d-flex justify-content-center align-items-center"),
            dbc.Col([
                dcc.Loading(
                    [
                        dcc.Dropdown(
                            id="pidk-day-label-dropdown",
                            options=get_day_label_options(),
                            value="TODAY",
                            clearable=False,
                            placeholder="Select date",
                            className="tv-date-dropdown",
                            style={"minWidth": "140px"},
                        ),
                        html.P(id="pidk-last-updated", style={
                            "color": "#fff", "margin": "0", "marginTop": "4px",
                            "fontSize": "0.75rem", "textAlign": "right",
                        }),
                    ],
                    type="circle",
                    color="white",
                    fullscreen=False,
                    style={"minHeight": "40px"},
                ),
            ], width=2, className="align-self-center"),
        ], className="align-items-center mb-2 g-2"),
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Run Totals", className="pidk-card-header"),
                    dbc.CardBody([
                        html.Div(id="pidk-run-totals-table", className="pidk-table-wrapper"),
                    ], className="pidk-card-body p-0"),
                ], className="pidk-table-card"),
            ], width=6),
            dbc.Col([
            dbc.Card([
                dbc.CardHeader("Shift Totals", className="pidk-card-header"),
                dbc.CardBody([
                    html.Div(id="pidk-shift-totals-table", className="pidk-table-wrapper"),
                ], className="pidk-card-body p-0"),
            ], className="pidk-table-card"),
        ], width=6),
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Bin Per Hour By Grower", className="pidk-card-header"),
                    dbc.CardBody([
                        html.Div(id="pidk-filter-badge", style={"marginBottom": "8px", "minHeight": "24px"}),
                        html.Div(
                            dcc.Graph(id="pidk-bph-chart", config={"displayModeBar": False, "displaylogo": False},
                                      style={"width": "100%", "height": "320px"}),
                            className="pidk-bph-chart-wrapper pidk-table-wrapper",
                        ),
                    ], className="pidk-card-body"),
                ], className="pidk-table-card"),
            ], width=8),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Employee Count", className="pidk-card-header"),
                    dbc.CardBody([
                        html.Div(id="pidk-employee-summary", className="pidk-table-wrapper"),
                    ], className="pidk-card-body p-0"),
                ], className="pidk-table-card"),
            ], width=4),
        ], className="mt-3 g-3"),
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.Span("Sizer Profile", style={"marginRight": "1rem"}),
                        html.Span("Event / Batch ", style={"color": "#aaa", "fontSize": "0.8rem", "marginRight": "6px"}),
                        dcc.Dropdown(
                            id="pidk-sizer-event-dropdown",
                            options=[],
                            value=None,
                            clearable=False,
                            placeholder="Select event or batch",
                            className="tv-date-dropdown",
                            style={"minWidth": "140px"},
                        ),
                    ], className="pidk-card-header d-flex flex-wrap align-items-center gap-2"),
                    dbc.CardBody([
                        html.Div(id="pidk-sizer-matrix", className="pidk-table-wrapper pidk-sizer-matrix-wrapper"),
                    ], className="pidk-card-body"),
                ], className="pidk-table-card"),
            ], width=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Computech Carton Palletized", className="pidk-card-header"),
                    dbc.CardBody([
                        html.Div(id="pidk-eq-matrix", className="pidk-table-wrapper"),
                    ], className="pidk-card-body p-0"),
                ], className="pidk-table-card"),
            ], width=4),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Package Type", className="pidk-card-header"),
                    dbc.CardBody([
                        html.Div(id="pidk-package-type-table", className="pidk-table-wrapper"),
                    ], className="pidk-card-body p-0"),
                ], className="pidk-table-card"),
            ], width=2),
        ], className="mt-3 g-3"),
    ], className="align-items-start g-3"),
], fluid=True, className="py-4")],
    className="tv-root pidk-root",
    style={"backgroundColor": "#1a1a1a", "minHeight": "100vh", "paddingTop": "1rem"},
)

# ── Callbacks ─────────────────────────────────────────────────────────
@callback(
    Output("pidk-day-label-dropdown", "value", allow_duplicate=True),
    Input("_pages_location", "pathname"),
    prevent_initial_call=True,
)
def set_default_today_on_load(pathname):
    """When navigating to this page, always default to Today."""
    if pathname == "/production/intra-day-kpis":
        return "TODAY"
    return no_update


@callback(
    Output("pidk-day-store", "data"),
    Output("pidk-selected-run", "data"),
    Output("pidk-selected-shift", "data"),
    Input("pidk-day-label-dropdown", "value"),
)
def update_day_store(day_label):
    return day_label or "TODAY", None, None


@callback(
    Output("pidk-selected-run", "data", allow_duplicate=True),
    Output("pidk-selected-shift", "data", allow_duplicate=True),
    Output("pidk-shift-totals-datatable", "selected_rows", allow_duplicate=True),
    Input("pidk-run-totals-datatable", "selected_rows"),
    State("pidk-run-data", "data"),
    prevent_initial_call=True,
)
def update_run_selection(selected_rows, run_data):
    if not selected_rows or not run_data or selected_rows[0] >= len(run_data):
        return None, None, []
    row = run_data[selected_rows[0]]
    run_filter = {
        "run_key": row.get("RUN_KEY"),
        "packdate_run_key": row.get("PACKDATE_RUN_KEY"),
        "lot": row.get("Lot"),
        "shift": row.get("Shift"),
        "run": row.get("Run"),
    }
    return run_filter, None, []


@callback(
    Output("pidk-selected-shift", "data", allow_duplicate=True),
    Output("pidk-selected-run", "data", allow_duplicate=True),
    Output("pidk-run-totals-datatable", "selected_rows", allow_duplicate=True),
    Input("pidk-shift-totals-datatable", "selected_rows"),
    State("pidk-shift-data", "data"),
    prevent_initial_call=True,
)
def update_shift_selection(selected_rows, shift_data):
    if not selected_rows or not shift_data or selected_rows[0] >= len(shift_data):
        return None, None, []
    row = shift_data[selected_rows[0]]
    shift_filter = {
        "packdate_run_key": row.get("PACKDATE_RUN_KEY"),
        "shift": row.get("Shift"),
    }
    return shift_filter, None, []


@callback(
    Output("pidk-run-totals-table", "children"),
    Output("pidk-shift-totals-table", "children"),
    Output("pidk-last-updated", "children"),
    Output("pidk-run-data", "data"),
    Output("pidk-shift-data", "data"),
    Input("pidk-interval", "n_intervals"),
    Input("pidk-day-store", "data"),
)
def update_pidk(_n_interval, day_label):
    cache_key = day_label or "TODAY"
    with _pidk_cache_lock:
        cached = _pidk_cache.get(cache_key)
    if cached is not None:
        return cached[0], cached[1], cached[2], cached[3], cached[4]
    payload = _build_pidk_payload(cache_key)
    with _pidk_cache_lock:
        _pidk_cache[cache_key] = payload
    return payload[0], payload[1], payload[2], payload[3], payload[4]


def _get_run_keys_for_shift(day_label, packdate_run_key):
    """Get (GROWER_NUMBER, RUN_KEY) for a shift from PTRUN."""
    if not day_label or not packdate_run_key:
        return []
    pk = str(packdate_run_key).replace("'", "''")
    df = query(f"""
        SELECT p.GROWER_NUMBER, p.RUN_KEY
        FROM FROSTY.STAGING.DQ_PTRUN_N_REPORT_03 p
        WHERE p.DAY_LABEL = '{day_label.replace("'", "''")}'
          AND p.PACKDATE_RUN_KEY = '{pk}'
        ORDER BY p.RUNS, p.GROWER_NUMBER
    """)
    if df.empty:
        return []
    return list(zip(
        df["GROWER_NUMBER"].fillna("").astype(str),
        df["RUN_KEY"].fillna("").astype(str),
    ))


@callback(
    Output("pidk-bph-chart", "figure"),
    Input("pidk-day-store", "data"),
    Input("pidk-selected-run", "data"),
    Input("pidk-selected-shift", "data"),
)
def update_pidk_bph_chart(day_label, selected_run, selected_shift):
    """Chart filtered by run or shift selection; no selection = full day."""
    day_label = day_label or "TODAY"
    run_df = get_run_totals(day_label)
    run_df = _normalize_df_columns(run_df, _RUN_COL_MAP)
    lot_col = "Lot" if "Lot" in run_df.columns else (run_df.columns[3] if len(run_df.columns) > 3 else None)

    if selected_run:
        run_key = selected_run.get("run_key")
        packdate_run_key = selected_run.get("packdate_run_key")
        lot = selected_run.get("lot")
        lots = [lot] if lot else []
        grower_dfs = []
        for g in lots:
            chart_df = get_pidk_bph_chart_data(day_label, g, run_key=run_key, packdate_run_key=packdate_run_key)
            if not chart_df.empty:
                grower_dfs.append((g, chart_df))
        return build_pidk_bph_chart_all_growers(grower_dfs)

    if selected_shift:
        packdate_run_key = selected_shift.get("packdate_run_key")
        lot_run_pairs = _get_run_keys_for_shift(day_label, packdate_run_key)
        grower_dfs = []
        for lot, run_key in lot_run_pairs:
            chart_df = get_pidk_bph_chart_data(day_label, lot, run_key=run_key, packdate_run_key=packdate_run_key)
            if not chart_df.empty:
                grower_dfs.append((lot, chart_df))
        return build_pidk_bph_chart_all_growers(grower_dfs)

    if lot_col is None or run_df.empty:
        return build_pidk_bph_chart_all_growers([])
    lots = run_df[lot_col].dropna().astype(str).unique().tolist()
    grower_dfs = []
    for lot in lots:
        chart_df = get_pidk_bph_chart_data(day_label, lot)
        if not chart_df.empty:
            grower_dfs.append((lot, chart_df))
    return build_pidk_bph_chart_all_growers(grower_dfs)


@callback(
    Output("pidk-sizer-event-dropdown", "options"),
    Output("pidk-sizer-event-dropdown", "value"),
    Input("pidk-day-store", "data"),
    Input("pidk-selected-run", "data"),
    Input("pidk-selected-shift", "data"),
)
def update_sizer_event_options(day_label, selected_run, selected_shift):
    day_label = day_label or "TODAY"
    run_key = selected_run.get("run_key") if selected_run else None
    packdate_run_key = None
    if selected_run:
        packdate_run_key = selected_run.get("packdate_run_key")
    elif selected_shift:
        packdate_run_key = selected_shift.get("packdate_run_key")
    events = get_sizer_events_with_event_ids(day_label, run_key=run_key, packdate_run_key=packdate_run_key)
    options = [{"label": "All", "value": "ALL"}] + [{"label": e["label"], "value": e["event_id"]} for e in events]
    value = "ALL"
    return options, value


@callback(
    Output("pidk-filter-badge", "children"),
    Input("pidk-selected-run", "data"),
    Input("pidk-selected-shift", "data"),
)
def update_filter_badge(selected_run, selected_shift):
    if selected_run:
        run = selected_run.get("run", "")
        lot = selected_run.get("lot", "")
        label = f"Filtered by Run {run} · Lot {lot}" if run and lot else "Filtered by run"
        return html.Span([
            html.Span(label, style={"color": "#bbb", "fontSize": "0.8rem"}),
            " ",
            html.A("Clear", href="#", id="pidk-clear-filter", style={"color": "#64B5F6", "fontSize": "0.8rem"}),
        ])
    if selected_shift:
        shift = selected_shift.get("shift", "")
        label = f"Filtered by Shift {shift}" if shift else "Filtered by shift"
        return html.Span([
            html.Span(label, style={"color": "#bbb", "fontSize": "0.8rem"}),
            " ",
            html.A("Clear", href="#", id="pidk-clear-filter", style={"color": "#64B5F6", "fontSize": "0.8rem"}),
        ])
    return None


@callback(
    Output("pidk-selected-run", "data", allow_duplicate=True),
    Output("pidk-selected-shift", "data", allow_duplicate=True),
    Output("pidk-run-totals-datatable", "selected_rows", allow_duplicate=True),
    Output("pidk-shift-totals-datatable", "selected_rows", allow_duplicate=True),
    Input("pidk-clear-filter", "n_clicks"),
    prevent_initial_call=True,
)
def clear_filter(_n_clicks):
    return None, None, [], []


@callback(
    Output("pidk-sizer-matrix", "children"),
    Input("pidk-sizer-event-dropdown", "value"),
    Input("pidk-day-store", "data"),
    Input("pidk-selected-run", "data"),
    Input("pidk-selected-shift", "data"),
)
def update_sizer_matrix(event_id, day_label, selected_run, selected_shift):
    if not event_id:
        return html.P("Select an event or batch", style={"color": "#999", "textAlign": "center", "padding": "16px"})
    if event_id == "ALL":
        run_key = selected_run.get("run_key") if selected_run else None
        packdate_run_key = None
        if selected_run:
            packdate_run_key = selected_run.get("packdate_run_key")
        elif selected_shift:
            packdate_run_key = selected_shift.get("packdate_run_key")
        drops_df = get_sizer_drops_for_all_events(day_label or "TODAY", run_key=run_key, packdate_run_key=packdate_run_key)
    else:
        drops_df = get_sizer_drops_for_event(event_id)
    return _build_sizer_matrix_table(drops_df)


@callback(
    Output("pidk-selected-package-type", "data"),
    Input("pidk-day-store", "data"),
    Input("pidk-selected-run", "data"),
    Input("pidk-selected-shift", "data"),
)
def reset_package_type_filter_on_context_change(day_label, selected_run, selected_shift):
    return None


@callback(
    Output("pidk-selected-package-type", "data", allow_duplicate=True),
    Input({"type": "pidk-pkg-filter-btn", "index": ALL}, "n_clicks"),
    State("pidk-selected-package-type", "data"),
    prevent_initial_call=True,
)
def update_package_type_filter(_n_clicks, current):
    tid = ctx.triggered_id
    if not tid or tid.get("type") != "pidk-pkg-filter-btn":
        return no_update
    idx = tid.get("index")
    if idx == "All":
        return None
    if current == idx:
        return None
    return idx


@callback(
    Output("pidk-eq-matrix", "children"),
    Input("pidk-day-store", "data"),
    Input("pidk-selected-run", "data"),
    Input("pidk-selected-shift", "data"),
    Input("pidk-selected-package-type", "data"),
)
def update_eq_matrix(day_label, selected_run, selected_shift, selected_pkg):
    day_label = day_label or "TODAY"
    run_key = selected_run.get("run_key") if selected_run else None
    packdate_run_key = None
    if selected_run:
        packdate_run_key = selected_run.get("packdate_run_key")
    elif selected_shift:
        packdate_run_key = selected_shift.get("packdate_run_key")
    eq_df = get_eq_data(day_label, run_key=run_key, packdate_run_key=packdate_run_key)
    eq_df = _filter_eq_by_classification(eq_df, selected_pkg)
    return _build_eq_matrix_table(eq_df)


@callback(
    Output("pidk-package-type-table", "children"),
    Input("pidk-day-store", "data"),
    Input("pidk-selected-run", "data"),
    Input("pidk-selected-shift", "data"),
    Input("pidk-selected-package-type", "data"),
)
def update_package_type_table(day_label, selected_run, selected_shift, selected_pkg):
    day_label = day_label or "TODAY"
    run_key = selected_run.get("run_key") if selected_run else None
    packdate_run_key = None
    if selected_run:
        packdate_run_key = selected_run.get("packdate_run_key")
    elif selected_shift:
        packdate_run_key = selected_shift.get("packdate_run_key")
    eq_df = get_eq_data(day_label, run_key=run_key, packdate_run_key=packdate_run_key)
    pkg_df = _eq_data_to_package_type_df(eq_df)
    return _build_package_type_table(pkg_df, selected_package_type=selected_pkg)


@callback(
    Output("pidk-employee-summary", "children"),
    Input("pidk-day-store", "data"),
    Input("pidk-selected-run", "data"),
    Input("pidk-selected-shift", "data"),
)
def update_employee_summary(day_label, selected_run, selected_shift):
    day_label = day_label or "TODAY"
    packdate_run_key = None
    if selected_shift:
        packdate_run_key = selected_shift.get("packdate_run_key")
    elif selected_run:
        packdate_run_key = selected_run.get("packdate_run_key")
    dt_df = get_employee_summary_data(day_label, packdate_run_key=packdate_run_key)
    summary = _compute_employee_summary(dt_df)
    return _build_employee_summary_table(summary)
