"""
Production Intra Day KPIs - data fetching, payload building, table/chart builders.
All data logic for the PIDK page lives here.
"""
from datetime import datetime, date
import threading
import time

import pandas as pd
from dash import html

from components.colored_table import create_colored_table
from services.snowflake_service import query
from utils.table_helpers import _normalize_df_columns
from utils.sizer import build_sizer_matrix, _get_gradient_color

# Column maps for Snowflake uppercase → display names (exported for callbacks)
RUN_COL_MAP = {
    "RUN": "Run", "VARIETY": "Variety", "SHIFT": "Shift", "LOT": "Lot",
    "BINSPRESHIFT": "BinsPreShift", "BINSONSHIFT": "BinsOnShift",
    "BINSPERHOUR": "BinsPerHour", "STAMPERPPMH": "StamperPPMH",
    "BINPERHOURTARGET": "BinPerHourTarget", "PACKSPERHOURMANHOUR": "PacksPerHourManHour",
}
SHIFT_COL_MAP = {
    "SHIFT": "Shift", "TOTALBINS": "TotalBins", "BINPERHOUR": "BinPerHour",
    "PPMH": "PPMH", "PPMHTARGET": "PPMHTarget", "BPHTARGET": "BPHTarget",
}

_BPH_BAR_COLORS = ["#42A5F5", "#1976D2", "#1E88E5", "#1565C0", "#0D47A1"]


def _resolve_day_to_date(day_label):
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


def get_run_keys_for_shift(day_label, packdate_run_key):
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


def get_pidk_bph_chart_data(day_label, grower_number, run_key=None, packdate_run_key=None):
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


def build_pidk_bph_chart_all_growers(grower_dfs):
    import plotly.graph_objects as go
    layout = dict(
        paper_bgcolor="#1a1a1a",
        plot_bgcolor="#1a1a1a",
        font=dict(color="#fff", size=13),
        title=None,
        showlegend=True,
        legend=dict(orientation="h", y=1.12, x=0, traceorder="normal", font=dict(size=12)),
        margin=dict(l=55, r=55, t=20, b=50),
        height=320,
        barmode="stack",
        xaxis=dict(title="Bin Dumper", gridcolor="#333", showgrid=True, title_font=dict(size=13)),
        yaxis=dict(title="Bins Per Hour", gridcolor="#333", showgrid=True, title_font=dict(size=13)),
    )
    fig = go.Figure(layout=layout)
    if not grower_dfs:
        fig.add_annotation(
            text="No data", xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False, font=dict(color="#666", size=18),
        )
        return fig
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
        fig.add_trace(go.Bar(x=x, y=y, name=str(grower_number), marker_color=color, legendgroup=str(grower_number)))
        if t_col and t_col in df.columns:
            target_x.extend(df[x_col].tolist())
            target_y.extend(df[t_col].tolist())
    if target_x and target_y:
        combined = sorted(zip(target_x, target_y), key=lambda r: r[0])
        tx, ty = [r[0] for r in combined], [r[1] for r in combined]
        fig.add_trace(go.Scatter(
            x=tx, y=ty, mode="lines", line=dict(color="#FF9800", width=4),
            name="Target", connectgaps=True,
        ))
    return fig


def get_sizer_events_for_day(day_label, run_key=None, packdate_run_key=None):
    if not day_label:
        return pd.DataFrame()
    run_filter = f"AND p.\"RUN_KEY\" = '{str(run_key).replace(chr(39), chr(39)+chr(39))}'" if run_key else ""
    packdate_filter = f"AND p.\"PACKDATE_RUN_KEY\" = '{str(packdate_run_key).replace(chr(39), chr(39)+chr(39))}'" if packdate_run_key else ""
    extra = run_filter or packdate_filter
    q = f"""
        SELECT h."BatchID" AS "BatchID", h."EventId" AS "EventId", h."SHIFT_KEY" AS "SHIFT_KEY", h."GrowerCode" AS "GrowerCode",
               h."VarietyName" AS "VarietyName", h."StartTime" AS "StartTime", h."SHIFT_CODE" AS "SHIFT_CODE",
               p."PACKDATE_RUN_KEY", p."RUN_KEY"
        FROM FROSTY.STAGING.DQ_APPLE_SIZER_HEADER_VIEW_03 h
        INNER JOIN FROSTY.STAGING.DQ_PTRUN_N_REPORT_03 p ON p."RUN_KEY" = h."SHIFT_KEY"
        WHERE p."DAY_LABEL" = '{day_label.replace("'", "''")}' {extra}
        ORDER BY h."StartTime" DESC, h."BatchID"
    """
    df = query(q)
    if not df.empty:
        return df
    q2 = f"""
        SELECT h."BatchID" AS "BatchID", h."EventId" AS "EventId", h."SHIFT_KEY" AS "SHIFT_KEY", h."GrowerCode" AS "GrowerCode",
               h."VarietyName" AS "VarietyName", h."StartTime" AS "StartTime", h."SHIFT_CODE" AS "SHIFT_CODE",
               p."PACKDATE_RUN_KEY", p."RUN_KEY"
        FROM FROSTY.STAGING.DQ_APPLE_SIZER_HEADER_VIEW_03 h
        INNER JOIN FROSTY.STAGING.DQ_PTRUN_N_REPORT_03 p ON h."SHIFT_KEY" LIKE p."PACKDATE_RUN_KEY" || '%'
        WHERE p."DAY_LABEL" = '{day_label.replace("'", "''")}' {extra}
        ORDER BY h."StartTime" DESC, h."BatchID"
    """
    return query(q2)


def get_sizer_events_with_event_ids(day_label, run_key=None, packdate_run_key=None):
    header_df = get_sizer_events_for_day(day_label, run_key=run_key, packdate_run_key=packdate_run_key)
    if header_df.empty:
        return []
    bid_col = "BatchID" if "BatchID" in header_df.columns else ("BATCHID" if "BATCHID" in header_df.columns else header_df.columns[0])
    eid_col = "EventId" if "EventId" in header_df.columns else ("EVENTID" if "EVENTID" in header_df.columns else None)
    start_col = "StartTime" if "StartTime" in header_df.columns else ("STARTTIME" if "STARTTIME" in header_df.columns else None)
    pk_col = "PACKDATE_RUN_KEY" if "PACKDATE_RUN_KEY" in header_df.columns else None
    rk_col = "RUN_KEY" if "RUN_KEY" in header_df.columns else None
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
        ev = {"event_id": str(eid), "batch_id": bid, "label": label}
        if pk_col:
            ev["packdate_run_key"] = r.get(pk_col)
        if rk_col:
            ev["run_key"] = r.get(rk_col)
        out.append(ev)
    return out


def filter_sizer_events_by_run_packdate(events, run_key=None, packdate_run_key=None):
    """Filter events list by run_key or packdate_run_key. Prefer run_key when both given (run selection)."""
    if not events or (not run_key and not packdate_run_key):
        return events
    out = []
    for e in events:
        if run_key and str(e.get("run_key", "")).strip() == str(run_key).strip():
            out.append(e)
        elif packdate_run_key and str(e.get("packdate_run_key", "")).strip() == str(packdate_run_key).strip():
            out.append(e)
    return out


def get_sizer_drops_for_event(event_id, batch_id=None):
    safe_id = str(event_id).replace("'", "''")
    return query(f"""
        SELECT "GradeName" AS "GradeName", "SizeName" AS "SizeName",
               COALESCE(NULLIF(TRIM("PACKOUT_GROUP"), ''), 'Unclassified') AS "PACKOUT_GROUP",
               SUM(COALESCE("weight_dec", "WEIGHT", 0)) AS "WEIGHT"
        FROM FROSTY.STAGING.DQ_APPLE_SIZER_DROPSUMMARY_03
        WHERE "EventId" = '{safe_id}'
        GROUP BY "GradeName", "SizeName", "PACKOUT_GROUP"
        ORDER BY "GradeName", "SizeName"
    """)


def get_sizer_drops_for_all_events(day_label, run_key=None, packdate_run_key=None):
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
    return combined.groupby(["GradeName", "SizeName", "PACKOUT_GROUP"], as_index=False).agg({"WEIGHT": "sum"})


def aggregate_sizer_drops_from_cache(drops_by_event, event_ids):
    """Aggregate drops from cached {event_id: drops_df} for given event_ids."""
    if not drops_by_event or not event_ids:
        return pd.DataFrame()
    dfs = []
    for eid in event_ids:
        df = drops_by_event.get(str(eid))
        if df is not None and not df.empty:
            dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    combined = pd.concat(dfs, ignore_index=True)
    return combined.groupby(["GradeName", "SizeName", "PACKOUT_GROUP"], as_index=False).agg({"WEIGHT": "sum"})


def build_sizer_matrix_table(drops_df):
    if drops_df is None or drops_df.empty:
        return html.P("No data", style={"color": "#999", "textAlign": "center", "padding": "16px"})
    pct_pivot, row_totals, col_totals = build_sizer_matrix(drops_df)
    if pct_pivot is None:
        return html.P("No data", style={"color": "#999", "textAlign": "center", "padding": "16px"})
    size_columns = list(pct_pivot.columns)
    grade_names = list(pct_pivot.index)
    _ths = {"padding": "4px 7px", "fontSize": "0.8rem", "textAlign": "center", "color": "#fff", "backgroundColor": "#2a2a2a", "fontWeight": "600"}
    _tds = {"padding": "4px 7px", "fontSize": "0.8rem"}
    header_row = [html.Th("Packout Group", style={**_ths, "textAlign": "left"})] + [html.Th(str(s), style=_ths) for s in size_columns] + [html.Th("Total", style=_ths)]
    rows = [html.Tr(header_row)]
    for grade in grade_names:
        row_cells = [html.Td(grade, style={**_tds, "textAlign": "left", "fontWeight": "600", "color": "#ddd", "backgroundColor": "#1e1e1e"})]
        for size in size_columns:
            val = pct_pivot.loc[grade, size]
            val = 0.0 if pd.isna(val) or val is None else float(val)
            bg, txt = _get_gradient_color(val, min_val=0, max_val=100) if val > 0 else ("#1e1e1e", "#888")
            row_cells.append(html.Td(f"{val:.2f}%", style={**_tds, "textAlign": "center", "backgroundColor": bg, "color": txt, "fontWeight": "600"}))
        row_total_val = row_totals[grade]
        row_total_val = 0.0 if pd.isna(row_total_val) or row_total_val is None else float(row_total_val)
        row_cells.append(html.Td(f"{row_total_val:.2f}%", style={**_tds, "textAlign": "center", "backgroundColor": "#2a2a2a", "fontWeight": "700", "color": "#fff"}))
        rows.append(html.Tr(row_cells))
    total_row = [html.Td("Total", style={**_tds, "textAlign": "left", "fontWeight": "700", "backgroundColor": "#2a2a2a", "color": "#fff"})]
    for size in size_columns:
        val = col_totals[size]
        val = 0.0 if pd.isna(val) or val is None else float(val)
        total_row.append(html.Td(f"{val:.2f}%", style={**_tds, "textAlign": "center", "backgroundColor": "#2a2a2a", "fontWeight": "700", "color": "#fff"}))
    total_row.append(html.Td("100.00%", style={**_tds, "textAlign": "center", "backgroundColor": "#2a2a2a", "fontWeight": "700", "color": "#fff"}))
    rows.append(html.Tr(total_row))
    return html.Table(
        [html.Thead(rows[0]), html.Tbody(rows[1:])],
        style={"width": "100%", "color": "#ddd", "borderCollapse": "collapse", "fontSize": "0.8rem"},
        className="pidk-sizer-matrix",
    )


def get_eq_data(day_label, run_key=None, packdate_run_key=None):
    if not day_label:
        return pd.DataFrame()
    dl = str(day_label).replace("'", "''")
    extra = ""
    if run_key:
        extra = f" AND p.RUN_KEY = '{str(run_key).replace(chr(39), chr(39)+chr(39))}'"
    elif packdate_run_key:
        extra = f" AND p.PACKDATE_RUN_KEY = '{str(packdate_run_key).replace(chr(39), chr(39)+chr(39))}'"
    return query(f"""
        SELECT TRIM(e.PACK_ABBR) AS PACK_ABBR, TRIM(e.GRADE_ABBR) AS GRADE_ABBR,
               COALESCE(e.CARTONS, 0) AS CARTONS, COALESCE(e.EQ_ON_HAND, e.CARTONS, 0) AS EQ_VAL,
               COALESCE(NULLIF(TRIM(pc.CLASSIFICATION), ''), 'Unclassified') AS CLASSIFICATION,
               p.PACKDATE_RUN_KEY, p.RUN_KEY
        FROM FROSTY.STAGING.DQ_EQ_WITH_KEYS03 e
        INNER JOIN FROSTY.STAGING.DQ_PTRUN_N_REPORT_03 p ON p.RUN_KEY = e.RUN_KEY AND p.DAY_LABEL = '{dl}'
        LEFT OUTER JOIN FROSTY.STAGING.PACK_CLASSIFICATION pc ON UPPER(TRIM(e.PACK_ABBR)) = UPPER(TRIM(pc.PACK_ABBR))
        WHERE 1=1 {extra}
    """)


def filter_eq_by_run_or_packdate(eq_df, run_key=None, packdate_run_key=None):
    """Filter EQ df by run_key or packdate_run_key. Use when eq_df has PACKDATE_RUN_KEY/RUN_KEY cols."""
    if eq_df is None or eq_df.empty or (not run_key and not packdate_run_key):
        return eq_df
    df = eq_df.copy()
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
    pk_col = "PACKDATE_RUN_KEY" if "PACKDATE_RUN_KEY" in df.columns else "packdate_run_key"
    rk_col = "RUN_KEY" if "RUN_KEY" in df.columns else "run_key"
    if packdate_run_key and pk_col in df.columns:
        df = df[df[pk_col].astype(str).str.strip() == str(packdate_run_key).strip()]
    elif run_key and rk_col in df.columns:
        df = df[df[rk_col].astype(str).str.strip() == str(run_key).strip()]
    return df


def filter_eq_by_classification(eq_df, classification):
    if eq_df is None or eq_df.empty or not classification:
        return eq_df
    df = eq_df.copy()
    df.columns = [c.strip().upper() if isinstance(c, str) else c for c in df.columns]
    if "CLASSIFICATION" not in df.columns:
        return eq_df
    g = df["CLASSIFICATION"].fillna("Unclassified").astype(str).str.strip()
    return df[g == str(classification)]


def build_eq_matrix(eq_df):
    if eq_df is None or eq_df.empty:
        return None, None, None
    df = eq_df.copy()
    df.columns = [c.strip().upper() if isinstance(c, str) else c for c in df.columns]
    if "CARTONS" not in df.columns:
        return None, None, None
    pivot = df.pivot_table(index="PACK_ABBR", columns="GRADE_ABBR", values="CARTONS", aggfunc="sum", fill_value=0)
    pivot = pivot.sort_index()
    return pivot, pivot.sum(axis=1), pivot.sum(axis=0)


def build_eq_matrix_table(eq_df):
    if eq_df is None or eq_df.empty:
        return html.P("No data", style={"color": "#999", "textAlign": "center", "padding": "16px"})
    pivot, row_totals, col_totals = build_eq_matrix(eq_df)
    if pivot is None:
        return html.P("No data", style={"color": "#999", "textAlign": "center", "padding": "16px"})
    grade_cols = list(pivot.columns)
    pack_abbrs = list(pivot.index)
    max_val = pivot.values.max() if pivot.size else 0
    _ths = {"padding": "4px 7px", "fontSize": "0.8rem", "textAlign": "center", "color": "#fff", "backgroundColor": "#2a2a2a", "fontWeight": "600"}
    _tds = {"padding": "4px 7px", "fontSize": "0.8rem"}
    header_row = [html.Th("Pack", style={**_ths, "textAlign": "left"})] + [html.Th(str(g), style=_ths) for g in grade_cols] + [html.Th("Total", style=_ths)]
    rows = [html.Tr(header_row)]
    for pack in pack_abbrs:
        rt = row_totals[pack]
        pack_total = int(rt) if not pd.isna(rt) else 0
        row_cells = [html.Td(pack, style={**_tds, "textAlign": "left", "fontWeight": "600", "color": "#ddd", "backgroundColor": "#1e1e1e"})]
        for g in grade_cols:
            val = int(pivot.loc[pack, g]) if g in pivot.columns else 0
            bg, txt = _get_gradient_color(float(val), min_val=0, max_val=max(1, max_val)) if val > 0 else ("#1e1e1e", "#888")
            row_cells.append(html.Td(str(val), style={**_tds, "textAlign": "center", "backgroundColor": bg, "color": txt, "fontWeight": "600"}))
        row_cells.append(html.Td(str(pack_total), style={**_tds, "textAlign": "center", "backgroundColor": "#2a2a2a", "fontWeight": "700", "color": "#fff"}))
        rows.append(html.Tr(row_cells))
    total_row = [html.Td("Total", style={**_tds, "textAlign": "left", "fontWeight": "700", "backgroundColor": "#2a2a2a", "color": "#fff"})]
    for grade in grade_cols:
        v = col_totals[grade]
        total_row.append(html.Td(str(int(v)) if not pd.isna(v) else "0", style={**_tds, "textAlign": "center", "backgroundColor": "#2a2a2a", "fontWeight": "700", "color": "#fff"}))
    grand = pivot.values.sum()
    total_row.append(html.Td(str(int(grand)), style={**_tds, "textAlign": "center", "backgroundColor": "#2a2a2a", "fontWeight": "700", "color": "#fff"}))
    rows.append(html.Tr(total_row))
    return html.Table(
        [html.Thead(rows[0]), html.Tbody(rows[1:])],
        style={"width": "100%", "color": "#ddd", "borderCollapse": "collapse", "fontSize": "0.8rem"},
        className="pidk-eq-matrix",
    )


def eq_data_to_package_type_df(eq_df):
    if eq_df is None or eq_df.empty:
        return pd.DataFrame()
    df = eq_df.copy()
    df.columns = [c.strip().upper() if isinstance(c, str) else c for c in df.columns]
    if "EQ_VAL" not in df.columns:
        return pd.DataFrame()
    if "CLASSIFICATION" not in df.columns:
        df["CLASSIFICATION"] = None
    df["grp"] = (
        df["CLASSIFICATION"].astype(str).str.strip()
        .replace({"": "Unclassified", "NONE": "Unclassified", "NAN": "Unclassified"})
    )
    df.loc[df["CLASSIFICATION"].isna(), "grp"] = "Unclassified"
    agg = df.groupby("grp", as_index=False).agg({"EQ_VAL": "sum"})
    agg.columns = ["Group", "eq_sum"]
    return agg.sort_values("Group")


def build_package_type_table(pkg_df, selected_package_type=None):
    no_data_style = {"color": "#999", "textAlign": "center", "padding": "16px", "fontSize": "0.9rem"}
    if pkg_df is None or pkg_df.empty:
        return html.P("No data — select a day with EQ runs", style=no_data_style)
    pkg_df = pkg_df.copy()
    pkg_df.columns = [c.strip().lower() if isinstance(c, str) else c for c in pkg_df.columns]
    sum_col, grp_col = "eq_sum", "group"
    if sum_col not in pkg_df.columns:
        return html.P("No data — select a day with EQ runs", style=no_data_style)
    pkg_df[sum_col] = pd.to_numeric(pkg_df[sum_col], errors="coerce").fillna(0.0)
    total = float(pkg_df[sum_col].sum())
    if total == 0:
        return html.P("No data — select a day with EQ runs", style=no_data_style)
    pkg_df["pct"] = 100.0 * pkg_df[sum_col] / total
    _ths = {"padding": "4px 7px", "fontSize": "0.8rem", "color": "#fff", "backgroundColor": "#2a2a2a", "fontWeight": "600"}
    _btn = {"background": "none", "border": "none", "color": "#ddd", "cursor": "pointer", "fontSize": "0.8rem", "padding": 0, "textAlign": "left", "width": "100%"}
    header_row = [html.Th("Group", style={**_ths, "textAlign": "left"}), html.Th("%", style={**_ths, "textAlign": "right"})]
    rows = [html.Tr(header_row)]
    all_selected = selected_package_type is None
    all_style = {**_btn, "color": "#1565C0" if all_selected else "#ccc", "fontWeight": "600" if all_selected else "normal"}
    _tds = {"padding": "4px 7px", "fontSize": "0.8rem"}
    rows.append(html.Tr([
        html.Td(html.Button("All", id={"type": "pidk-pkg-filter-btn", "index": "All"}, n_clicks=0, style=all_style), style={**_tds, "textAlign": "left", "backgroundColor": "#1e1e1e"}),
        html.Td("—", style={**_tds, "textAlign": "right", "color": "#ddd", "backgroundColor": "#1e1e1e"}),
    ]))
    for _, r in pkg_df.iterrows():
        grp = str(r.get(grp_col, r.get("Group", "")))
        pct = r["pct"]
        sel = selected_package_type is not None and str(selected_package_type) == grp
        btn_style = {**_btn, "color": "#1565C0" if sel else "#ccc", "fontWeight": "600" if sel else "normal"}
        rows.append(html.Tr([
            html.Td(html.Button(grp, id={"type": "pidk-pkg-filter-btn", "index": grp}, n_clicks=0, style=btn_style), style={**_tds, "textAlign": "left", "backgroundColor": "#1e1e1e"}),
            html.Td(f"{float(pct):.2f}%", style={**_tds, "textAlign": "right", "color": "#ddd", "backgroundColor": "#1e1e1e"}),
        ]))
    rows.append(html.Tr([
        html.Td("Total", style={**_tds, "textAlign": "left", "fontWeight": "700", "backgroundColor": "#2a2a2a", "color": "#fff"}),
        html.Td("100.00%", style={**_tds, "textAlign": "right", "fontWeight": "700", "backgroundColor": "#2a2a2a", "color": "#fff"}),
    ]))
    return html.Table(
        [html.Thead(rows[0]), html.Tbody(rows[1:])],
        style={"width": "100%", "color": "#ddd", "borderCollapse": "collapse", "fontSize": "0.8rem"},
        className="pidk-package-type-table pidk-package-type",
    )


def get_employee_summary_data(day_label, packdate_run_key=None):
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
    keys = [str(packdate_run_key)] if packdate_run_key else key_df["PACKDATE_RUN_KEY"].dropna().astype(str).unique().tolist()
    if not keys:
        return pd.DataFrame()
    in_list = ",".join([f"'{str(k).replace(chr(39), chr(39)+chr(39))}'" for k in keys])
    return query(f"""
        SELECT SHIFT, DATE_SHIFT_KEY, BUCKET_START, EMPLOYEE_COUNT_ALLOC, MINUTES_WORKED_ALLOC, STAMPER_EQS, PACKS_MANHOUR_TARGET
        FROM FROSTY.STAGING.DT_SHIFT_10MIN_KPI_A_PER_RUN03_DT
        WHERE DATE_SHIFT_KEY IN ({in_list})
        ORDER BY DATE_SHIFT_KEY, BUCKET_START
    """)


def compute_employee_summary(df):
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


def build_employee_summary_table(summary_list):
    if not summary_list:
        return html.P("No data — select a day with shift data", style={"color": "#999", "textAlign": "center", "padding": "16px", "fontSize": "0.8rem"})
    _ths = {"padding": "4px 7px", "fontSize": "0.8rem", "color": "#fff", "backgroundColor": "#2a2a2a", "fontWeight": "700"}
    _tds = {"padding": "4px 7px", "fontSize": "0.8rem"}
    header_row = [html.Th("Shift", style={**_ths, "textAlign": "left"}), html.Th("Employee", style={**_ths, "textAlign": "left"})]
    rows = [html.Tr(header_row)]
    for s in summary_list:
        txt = f"Max Employees: {s['max_emp']}\nCurrent Employees: {s['current_emp']}\nReduce to Hit Target: {s['reduce']}"
        rows.append(html.Tr([
            html.Td(s["shift"], style={**_tds, "color": "#ddd", "verticalAlign": "top", "backgroundColor": "#1e1e1e", "fontWeight": "600"}),
            html.Td(txt.replace("\n", "\n"), style={**_tds, "color": "#ddd", "whiteSpace": "pre-line", "backgroundColor": "#1e1e1e"}),
        ]))
    return html.Table(
        [html.Thead(rows[0]), html.Tbody(rows[1:])],
        style={"width": "100%", "color": "#ddd", "borderCollapse": "collapse", "fontSize": "0.8rem"},
        className="pidk-employee-summary",
    )


RUN_COLUMNS = [
    {"field": "Run", "header": "Run", "dec": 0},
    {"field": "Variety", "header": "Variety", "dec": 0},
    {"field": "Shift", "header": "Shift", "dec": 0},
    {"field": "Lot", "header": "Lot", "dec": 0},
    {"field": "BinsPreShift", "header": "Bins Pre Shift", "dec": 0},
    {"field": "BinsOnShift", "header": "Bins On Shift", "dec": 0},
    {"field": "BinsPerHour", "header": "Bins Per Hour", "dec": 1, "color_target": "BinPerHourTarget"},
    {"field": "StamperPPMH", "header": "Stamper PPMH", "dec": 1, "color_target": "PacksPerHourManHour"},
    {"field": "BinPerHourTarget", "header": "Bin Per Hour Target", "dec": 1},
    {"field": "PacksPerHourManHour", "header": "Packs Per Hour Man Hour", "dec": 1},
]
SHIFT_COLUMNS = [
    {"field": "Shift", "header": "Shift", "dec": 0},
    {"field": "TotalBins", "header": "Total Bins", "dec": 0},
    {"field": "BinPerHour", "header": "Bin Per Hour", "dec": 1, "color_target": "BPHTarget"},
    {"field": "PPMH", "header": "PPMH", "dec": 1, "color_target": "PPMHTarget"},
    {"field": "PPMHTarget", "header": "PPMH Target", "dec": 1},
    {"field": "BPHTarget", "header": "BPH Target", "dec": 0},
]


def build_run_totals_table(df):
    if df is None or df.empty:
        return html.P("No data", style={"color": "#999", "textAlign": "center", "padding": "16px"})
    cols_to_show = [c["field"] for c in RUN_COLUMNS] + ["RUN_KEY", "PACKDATE_RUN_KEY"]
    display_df = df[[c for c in cols_to_show if c in df.columns]].copy()
    return create_colored_table(
        display_df,
        columns=RUN_COLUMNS,
        id_prefix="pidk-run-totals",
        pinned_cols=4,
        row_click_type="pidk-run-row-btn",
    )


def build_shift_totals_table(df):
    if df is None or df.empty:
        return html.P("No data", style={"color": "#999", "textAlign": "center", "padding": "16px"})
    cols_to_show = [c["field"] for c in SHIFT_COLUMNS] + ["PACKDATE_RUN_KEY"]
    display_df = df[[c for c in cols_to_show if c in df.columns]].copy()
    return create_colored_table(
        display_df,
        columns=SHIFT_COLUMNS,
        id_prefix="pidk-shift-totals",
        pinned_cols=2,
        row_click_type="pidk-shift-row-btn",
    )


# Cache (exported for callbacks)
pidk_cache = {}
pidk_cache_lock = threading.Lock()


def build_pidk_payload(day_label):
    if not day_label:
        day_label = "TODAY"
    run_df = get_run_totals(day_label)
    shift_df = get_shift_totals(day_label)
    run_df = _normalize_df_columns(run_df, RUN_COL_MAP)
    shift_df = _normalize_df_columns(shift_df, SHIFT_COL_MAP)
    run_table = build_run_totals_table(run_df)
    shift_table = build_shift_totals_table(shift_df)
    last_updated = f"Last updated: {datetime.now().strftime('%I:%M:%S %p')} · Refreshes every 5 min"
    run_data = run_df.to_dict("records") if not run_df.empty else []
    shift_data = shift_df.to_dict("records") if not shift_df.empty else []
    payload = {
        "run_table": run_table, "shift_table": shift_table, "last_updated": last_updated,
        "run_data": run_data, "shift_data": shift_data,
    }
    if day_label == "TODAY":
        emp_df = get_employee_summary_data(day_label)
        payload["employee_df_full"] = emp_df
        payload["employee"] = build_employee_summary_table(compute_employee_summary(emp_df))
        eq_df = get_eq_data(day_label)
        payload["eq_df_full"] = eq_df
        payload["eq_matrix"] = build_eq_matrix_table(eq_df)
        pkg_df = eq_data_to_package_type_df(eq_df)
        payload["package_type"] = build_package_type_table(pkg_df, selected_package_type=None)
        events = get_sizer_events_with_event_ids(day_label)
        payload["sizer_events_full"] = events
        payload["sizer_options"] = [{"label": "All", "value": "ALL"}] + [{"label": e["label"], "value": e["event_id"]} for e in events]
        payload["sizer_value"] = "ALL"
        drops_by_event = {}
        for e in events:
            df = get_sizer_drops_for_event(e["event_id"])
            if df is not None and not df.empty:
                drops_by_event[str(e["event_id"])] = df
        payload["sizer_drops_by_event"] = drops_by_event
        drops_all = get_sizer_drops_for_all_events(day_label)
        payload["sizer_matrix"] = build_sizer_matrix_table(drops_all)
        lot_col = "Lot" if "Lot" in run_df.columns else (run_df.columns[3] if len(run_df.columns) > 3 else None)
        pk_col = "PACKDATE_RUN_KEY" if "PACKDATE_RUN_KEY" in run_df.columns else (run_df.columns[-1] if "PACKDATE_RUN_KEY" in [c.upper() for c in run_df.columns] else None)
        rk_col = "RUN_KEY" if "RUN_KEY" in run_df.columns else None
        bph_data = {}
        grower_dfs = []
        if lot_col and not run_df.empty:
            for _, r in run_df.iterrows():
                lot = str(r.get(lot_col, "")).strip()
                pk = str(r.get(pk_col, "")).strip() if pk_col else None
                rk = r.get(rk_col) if rk_col else None
                if not lot:
                    continue
                key = (pk or "", lot)
                if key in bph_data:
                    continue
                chart_df = get_pidk_bph_chart_data(day_label, lot, run_key=rk, packdate_run_key=pk)
                if not chart_df.empty:
                    bph_data[key] = chart_df
                    grower_dfs.append((lot, chart_df))
        payload["bph_data"] = bph_data
        payload["run_df"] = run_df
        payload["bph_figure"] = build_pidk_bph_chart_all_growers(grower_dfs)
    return payload


def _refresh_cache_pidk_today():
    try:
        with pidk_cache_lock:
            pidk_cache["TODAY"] = build_pidk_payload("TODAY")
    except Exception as e:
        print(f"PIDK cache refresh error: {e}")


def _pidk_background_worker():
    _refresh_cache_pidk_today()
    while True:
        time.sleep(300)
        _refresh_cache_pidk_today()


_pidk_bg_thread = threading.Thread(target=_pidk_background_worker, daemon=True)
_pidk_bg_thread.start()
