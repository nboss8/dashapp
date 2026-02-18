"""
Production Finalized Report — Interactive version of the Grower Production Summary PDF.
Uses finalized data from POWERBI_PRODUCTION_HEADER (BINS_SUBMITTED > 0).
Groups by Grower, Variety, Pool (same as PDF). Shows: Run Info, Bins & Weight,
Packed Weights, Processor, Packout, Sizer Profile, Quality Control, Cull, Pressure, Notes.
"""
import base64
import dash
from dash import html, dcc, callback, Input, Output, State, no_update
import dash_bootstrap_components as dbc
import snowflake.connector
import pandas as pd
import os
import zipfile
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv

try:
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from reportlab.lib.colors import HexColor, white, black
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, KeepTogether
    from reportlab.lib.enums import TA_CENTER
    _HAS_REPORTLAB = True
except ImportError:
    _HAS_REPORTLAB = False

load_dotenv()
dash.register_page(__name__, path="/production/finalized-report", name="Production Finalized Report")

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
            if attempt == 1:
                print(f"Query error: {e}")
    return pd.DataFrame()


def load_groups_for_date(run_date):
    """Finalized runs grouped by Grower, Variety, Pool. BINS_SUBMITTED > 0."""
    d = str(run_date).replace("'", "''")
    return query(f"""
        SELECT GROWER, VARIETY_USER_CD, POOL,
               COUNT(*) AS RUNS,
               SUM(BINS_SUBMITTED) AS BINS,
               SUM(ACTUAL_NET) AS NET
        FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER_MAT
        WHERE RUN_DATE = '{d}' AND COALESCE(BINS_SUBMITTED, 0) > 0
        GROUP BY GROWER, VARIETY_USER_CD, POOL
        ORDER BY GROWER, VARIETY_USER_CD, POOL
    """)


def load_main_for_group(run_date, grower, variety, pool):
    """All runs for a group (finalized)."""
    d = str(run_date).replace("'", "''")
    g = str(grower).replace("'", "''")
    v = str(variety).replace("'", "''")
    p = str(pool).replace("'", "''")
    return query(f"""
        SELECT * FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER_MAT
        WHERE RUN_DATE = '{d}' AND GROWER = '{g}' AND VARIETY_USER_CD = '{v}' AND POOL = '{p}'
        ORDER BY SHIFT, RUN_NUMBER
    """)


def load_sizer_profile_multi(run_keys):
    if not run_keys:
        return pd.DataFrame()
    in_list = ",".join([f"'{str(k).replace(chr(39), chr(39)+chr(39))}'" for k in run_keys])
    return query(f"""
        SELECT d.GRADE_NAME, d.SIZE_NAME, SUM(d.WEIGHT_DEC) AS WEIGHT
        FROM FROSTY.APP.PTRUN_SIZER_DROP_SNAPSHOT d
        INNER JOIN FROSTY.APP.PTRUN_SIZER_PACKED p
            ON d.UNIQUE_RUN_KEY = p.UNIQUE_RUN_KEY
            AND d.QUALITY_NAME = p.QUALITY_NAME
            AND d.GRADE_NAME = p.GRADE_NAME
            AND d.SIZE_NAME = p.SIZE_NAME
        WHERE d.UNIQUE_RUN_KEY IN ({in_list}) AND p.IS_PACKED = TRUE
        GROUP BY d.GRADE_NAME, d.SIZE_NAME
        ORDER BY d.GRADE_NAME, d.SIZE_NAME
    """)


def load_processor_multi(run_keys):
    if not run_keys:
        return pd.DataFrame()
    in_list = ",".join([f"'{str(k).replace(chr(39), chr(39)+chr(39))}'" for k in run_keys])
    return query(f"""
        SELECT SIZE_ABBR, SUM(TOTAL_NET_WT) AS NET_WEIGHT
        FROM FROSTY.APP.PTRUN_PROCESSOR_VIEW_PBIX
        WHERE UNIQUE_RUN_KEY IN ({in_list})
        GROUP BY SIZE_ABBR
        ORDER BY SIZE_ABBR
    """)


def load_cull_defects_multi(run_keys):
    if not run_keys:
        return pd.DataFrame()
    in_list = ",".join([f"'{str(k).replace(chr(39), chr(39)+chr(39))}'" for k in run_keys])
    return query(f"""
        SELECT DEFECT_TYPE, SUM(COUNT_INT) AS COUNT_INT
        FROM FROSTY.APP.PTRUN_CULL_DEFECT
        WHERE UNIQUE_RUN_KEY IN ({in_list})
        GROUP BY DEFECT_TYPE
        ORDER BY COUNT_INT DESC
    """)


def load_pressure_multi(run_keys):
    if not run_keys:
        return pd.DataFrame()
    in_list = ",".join([f"'{str(k).replace(chr(39), chr(39)+chr(39))}'" for k in run_keys])
    return query(f"""
        SELECT FRUIT_SIZE_INT, AVG(PRESSURE_DEC) AS PRESSURE_DEC
        FROM FROSTY.APP.PTRUN_PRESSURE_DETAIL
        WHERE UNIQUE_RUN_KEY IN ({in_list})
        GROUP BY FRUIT_SIZE_INT
        ORDER BY FRUIT_SIZE_INT
    """)


def load_cull_headers_multi(run_keys):
    if not run_keys:
        return []
    in_list = ",".join([f"'{str(k).replace(chr(39), chr(39)+chr(39))}'" for k in run_keys])
    df = query(f"""
        SELECT * FROM FROSTY.APP.PTRUN_CULL_HEADER
        WHERE UNIQUE_RUN_KEY IN ({in_list})
    """)
    return df.to_dict("records") if not df.empty else []


def _size_sort_key(x):
    try:
        return int(str(x).strip())
    except (ValueError, TypeError):
        return 999


def build_sizer_matrix(sizer_df):
    if sizer_df is None or sizer_df.empty:
        return None, None, None
    row_col = "GRADE_NAME" if "GRADE_NAME" in sizer_df.columns else sizer_df.columns[0]
    col_col = "SIZE_NAME" if "SIZE_NAME" in sizer_df.columns else sizer_df.columns[1]
    val_col = "WEIGHT" if "WEIGHT" in sizer_df.columns else sizer_df.columns[2]
    pivot = sizer_df.pivot_table(index=row_col, columns=col_col, values=val_col, aggfunc="sum", fill_value=0)
    size_cols = sorted(pivot.columns, key=_size_sort_key)
    pivot = pivot.reindex(columns=size_cols).fillna(0)
    pivot = pivot.sort_index()
    total = pivot.values.sum()
    if total == 0:
        return None, None, None
    pct = pivot / total * 100
    return pct, pct.sum(axis=1), pct.sum(axis=0)


def _get_gradient_color(val, min_val=0, max_val=20):
    if val is None or val <= min_val:
        return "#ecf0f1"
    ratio = min((float(val) - min_val) / (max_val - min_val), 1.0)
    r = int(255 - (255 - 100) * ratio)
    g = int(255 - (255 - 149) * ratio)
    b = int(255 - (255 - 237) * ratio)
    return f"rgb({r},{g},{b})"


def _section(title, children):
    return html.Div([
        html.H5(title, style={
            "color": "#fff", "backgroundColor": "#2980b9", "padding": "8px 12px",
            "marginTop": "16px", "marginBottom": "8px", "fontSize": "0.95rem",
        }),
        html.Div(children, style={"padding": "8px 0"}),
    ])


def _fmt_num(val, dec=0):
    if pd.isnull(val) or val is None:
        return "—"
    try:
        return f"{float(val):,.{dec}f}" if dec else f"{int(val):,}"
    except (ValueError, TypeError):
        return "—"


def _fmt_dt(val, fmt="%m/%d/%Y %H:%M"):
    if pd.isnull(val) or val is None:
        return "N/A"
    try:
        return pd.Timestamp(val).strftime(fmt)
    except Exception:
        return str(val)


def _safe_str(val):
    if pd.isnull(val) or val is None or str(val).strip() == "":
        return "N/A"
    return str(val)


def _combine_breakdowns(breakdowns):
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


def _pdf_color(val, min_val=0, max_val=20):
    """ReportLab gradient color (RGB 0-1) for sizer cells."""
    if val is None or val <= min_val:
        return HexColor("#ecf0f1")
    ratio = min((float(val) - min_val) / (max_val - min_val), 1.0)
    r = (255 - (255 - 100) * ratio) / 255
    g = (255 - (255 - 149) * ratio) / 255
    b = (255 - (255 - 237) * ratio) / 255
    return HexColor(f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}")


def generate_pdf_bytes(run_date, group_value):
    """Generate PDF for the selected group. Returns (bytes, filename) or (None, error_msg)."""
    if not _HAS_REPORTLAB:
        return None, "reportlab is required. Install with: pip install reportlab"
    if not run_date or not group_value or "|" not in group_value:
        return None, "Select a date and a group first."
    g, v, p = group_value.split("|", 2)
    main_df = load_main_for_group(run_date, g, v, p)
    if main_df.empty:
        return None, "No data for this group."

    run_keys = main_df["UNIQUE_RUN_KEY"].dropna().astype(str).tolist()
    first = main_df.iloc[0]
    bins = int(main_df["BINS_SUBMITTED"].fillna(0).sum())
    gross = int(main_df["ACTUAL_GROSS"].fillna(0).sum())
    net = int(main_df["ACTUAL_NET"].fillna(0).sum())
    tare = int(main_df["ACTUAL_TARE"].fillna(0).sum())
    sizer_wt = float(main_df["ACTUAL_SIZER_WEIGHT"].fillna(0).sum())
    stamper_wt = float(main_df["ACTUAL_STAMPER_WEIGHT"].fillna(0).sum())
    packs = float(main_df["PACKS"].fillna(0).sum()) if "PACKS" in main_df.columns else 0
    shifts = sorted(main_df["SHIFT"].dropna().unique())
    shift_str = "+".join(str(s) for s in shifts)
    first_dump = main_df["FIRST_DUMP_TIME"].min()
    last_dump = main_df["LAST_DUMP_TIME"].max()
    tare_breakdown = ", ".join(t for t in main_df["TARE_BREAKDOWN"].dropna().tolist() if t) or "N/A"
    block_breakdown = _combine_breakdowns(main_df["BLOCK_BREAKDOWN"].dropna().tolist())
    pick_breakdown = _combine_breakdowns(main_df["PICK_BREAKDOWN"].dropna().tolist())
    is_combined = len(main_df) > 1

    run_date_obj = first.get("RUN_DATE")
    date_str = str(run_date_obj).replace("-", "")[:8] if run_date_obj else "00000000"
    pdf_filename = f"{date_str}_{_safe_str(g)}_{_safe_str(p)}_{_safe_str(v)}_{shift_str}.pdf".replace(" ", "_").replace("/", "-")

    FULL_WIDTH = 7.5 * inch
    PRIMARY_COLOR = HexColor("#2980b9")
    HEADER_BG = HexColor("#34495e")
    LIGHT_BG = HexColor("#ecf0f1")
    ACCENT_COLOR = HexColor("#27ae60")

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=0.5 * inch,
        leftMargin=0.5 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
    )
    styles = getSampleStyleSheet()
    section_style = ParagraphStyle(
        "SectionHeader",
        parent=styles["Heading2"],
        fontSize=12,
        textColor=white,
        backColor=PRIMARY_COLOR,
        spaceAfter=6,
        spaceBefore=12,
        leftIndent=6,
        rightIndent=6,
        leading=16,
    )
    label_style = ParagraphStyle("Label", parent=styles["Normal"], fontSize=9, textColor=HexColor("#505050"), fontName="Helvetica-Bold")
    value_style = ParagraphStyle("Value", parent=styles["Normal"], fontSize=9, textColor=black)

    def _header_footer(canvas, document, filename):
        canvas.saveState()
        canvas.setFillColor(HEADER_BG)
        canvas.rect(0, letter[1] - 50, letter[0], 50, fill=1, stroke=0)
        canvas.setFillColor(white)
        canvas.setFont("Helvetica-Bold", 18)
        canvas.drawString(0.5 * inch, letter[1] - 30, "Grower Production Summary")
        canvas.setFont("Helvetica", 10)
        canvas.drawString(0.5 * inch, letter[1] - 42, "Columbia Fruit Packers - Airport Facility")
        canvas.setFillColor(HexColor("#808080"))
        canvas.setFont("Helvetica-Oblique", 8)
        canvas.drawString(0.5 * inch, 0.5 * inch, filename)
        canvas.drawRightString(letter[0] - 0.5 * inch, 0.5 * inch, f"Page {document.page}")
        canvas.restoreState()

    story = [Spacer(1, 0.3 * inch)]
    if is_combined:
        combined_style = ParagraphStyle("Combined", parent=styles["Normal"], fontSize=10, textColor=HexColor("#8e44ad"), fontName="Helvetica-Bold", alignment=TA_CENTER)
        story.append(Paragraph(f"COMBINED REPORT: Shifts {shift_str}", combined_style))
        story.append(Spacer(1, 6))

    col_w = FULL_WIDTH / 4
    run_info_data = [
        [Paragraph("<b>Packing Date:</b>", label_style), Paragraph(_fmt_dt(first.get("RUN_DATE"), "%m/%d/%Y"), value_style), Paragraph("<b>Pack Line:</b>", label_style), Paragraph(_safe_str(first.get("PACK_LINE")), value_style)],
        [Paragraph("<b>Grower:</b>", label_style), Paragraph(f"{_safe_str(g)} — {_safe_str(first.get('GROWER_FULL_NAME',''))}", value_style), Paragraph("<b>Shift:</b>", label_style), Paragraph(shift_str, value_style)],
        [Paragraph("<b>Pool:</b>", label_style), Paragraph(_safe_str(p), value_style), Paragraph("<b>Start Time:</b>", label_style), Paragraph(_fmt_dt(first_dump), value_style)],
        [Paragraph("<b>Variety:</b>", label_style), Paragraph(_safe_str(v), value_style), Paragraph("<b>End Time:</b>", label_style), Paragraph(_fmt_dt(last_dump), value_style)],
    ]
    run_info_table = Table(run_info_data, colWidths=[col_w] * 4)
    run_info_table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
    story.append(Paragraph("Run Information", section_style))
    story.append(Spacer(1, 6))
    story.append(run_info_table)

    metrics_data = [["BINS", "GROSS (lbs)", "TARE (lbs)", "NET (lbs)"], [_fmt_num(bins), _fmt_num(gross), _fmt_num(tare), _fmt_num(net)]]
    metrics_table = Table(metrics_data, colWidths=[col_w] * 4)
    metrics_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), LIGHT_BG),
        ("TEXTCOLOR", (0, 0), (-1, 0), black),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 1), (-1, 1), 12),
        ("BACKGROUND", (3, 0), (3, 1), ACCENT_COLOR),
        ("TEXTCOLOR", (3, 0), (3, 1), white),
        ("BOX", (0, 0), (-1, -1), 1, black),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, black),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(Paragraph("Bins & Weight Summary", section_style))
    story.append(Spacer(1, 6))
    story.append(metrics_table)
    story.append(Spacer(1, 6))
    story.append(Paragraph(f"<b>Tare Breakdown:</b> {tare_breakdown}", value_style))
    story.append(Spacer(1, 6))
    breakdown_data = [["Block Breakdown", "Pick Breakdown"], [block_breakdown, pick_breakdown]]
    breakdown_table = Table(breakdown_data, colWidths=[FULL_WIDTH / 2, FULL_WIDTH / 2])
    breakdown_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
        ("TEXTCOLOR", (0, 0), (-1, 0), white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("FONTNAME", (0, 1), (-1, 1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, 1), 9),
        ("ALIGN", (0, 1), (-1, 1), "LEFT"),
        ("BOX", (0, 0), (-1, -1), 1, black),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, black),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 1), (-1, 1), 6),
    ]))
    story.append(breakdown_table)

    packed_data = [["Sizer Weight (lbs)", "Stamper Weight (lbs)"], [_fmt_num(sizer_wt, 2), _fmt_num(stamper_wt, 2)]]
    packed_table = Table(packed_data, colWidths=[FULL_WIDTH / 2, FULL_WIDTH / 2])
    packed_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
        ("TEXTCOLOR", (0, 0), (-1, 0), white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 1), (-1, 1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, 1), 9),
        ("BOX", (0, 0), (-1, -1), 1, black),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, black),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(Paragraph("Packed Weights", section_style))
    story.append(Spacer(1, 6))
    story.append(packed_table)

    proc_df = load_processor_multi(run_keys)
    if not proc_df.empty and net > 0:
        story.append(Paragraph("Processor", section_style))
        story.append(Spacer(1, 6))
        proc_data = [["Type", "Net Weight", "Processor %"]]
        for _, pr in proc_df.iterrows():
            w = float(pr["NET_WEIGHT"]) if pd.notna(pr["NET_WEIGHT"]) else 0
            pct = (w / net * 100) if net else 0
            proc_data.append([_safe_str(pr["SIZE_ABBR"]), _fmt_num(w), f"{pct:.1f}%"])
        tot_proc = proc_df["NET_WEIGHT"].sum()
        tot_pct = (tot_proc / net * 100) if net else 0
        proc_data.append(["Total", _fmt_num(tot_proc), f"{tot_pct:.1f}%"])
        proc_table = Table(proc_data, colWidths=[FULL_WIDTH / 3] * 3)
        proc_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
            ("TEXTCOLOR", (0, 0), (-1, 0), white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 9),
            ("ALIGN", (0, 0), (0, -1), "LEFT"),
            ("ALIGN", (1, 0), (-1, -1), "CENTER"),
            ("FONTNAME", (0, 1), (-1, -2), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), 9),
            ("BOX", (0, 0), (-1, -1), 1, black),
            ("INNERGRID", (0, 0), (-1, -1), 0.5, black),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING", (0, 0), (0, -1), 6),
            ("BACKGROUND", (0, -1), (-1, -1), LIGHT_BG),
            ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ]))
        story.append(proc_table)

    if packs and packs > 0:
        packout_val = packs / bins if bins > 0 else 0
        packout_pct = (packs * 40 / net * 100) if net > 0 else 0
        story.append(Paragraph("Packout Summary", section_style))
        story.append(Spacer(1, 6))
        packout_data = [["Bins", "Packs", "Packout", "Packout %"], [_fmt_num(bins), _fmt_num(packs, 0), _fmt_num(packout_val, 1), f"{packout_pct:.1f}%"]]
        packout_table = Table(packout_data, colWidths=[FULL_WIDTH / 4] * 4)
        packout_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
            ("TEXTCOLOR", (0, 0), (-1, 0), white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 9),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("FONTNAME", (0, 1), (-1, 1), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, 1), 9),
            ("BOX", (0, 0), (-1, -1), 1, black),
            ("INNERGRID", (0, 0), (-1, -1), 0.5, black),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(packout_table)

    sizer_df = load_sizer_profile_multi(run_keys)
    pct_pivot, row_totals, col_totals = build_sizer_matrix(sizer_df) if not sizer_df.empty else (None, None, None)
    if pct_pivot is not None:
        size_cols = list(pct_pivot.columns)
        grades = list(pct_pivot.index)
        sizer_header = ["Grade"] + [str(s) for s in size_cols] + ["Total"]
        sizer_data = [sizer_header]
        for grade in grades:
            row_data = [grade]
            for size in size_cols:
                val = pct_pivot.loc[grade, size]
                val = 0.0 if pd.isna(val) else float(val)
                row_data.append(f"{val:.2f}%" if val > 0 else "")
            row_data.append(f"{float(row_totals[grade]):.2f}%")
            sizer_data.append(row_data)
        total_row = ["Total"]
        for size in size_cols:
            val = col_totals[size]
            total_row.append(f"{float(val):.2f}%" if val and float(val) > 0 else "")
        total_row.append("100.00%")
        sizer_data.append(total_row)
        grade_col_w = 0.6 * inch
        total_col_w = 0.6 * inch
        size_col_w = (FULL_WIDTH - grade_col_w - total_col_w) / len(size_cols)
        col_widths = [grade_col_w] + [size_col_w] * len(size_cols) + [total_col_w]
        sizer_table = Table(sizer_data, colWidths=col_widths)
        sizer_style = [
            ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
            ("TEXTCOLOR", (0, 0), (-1, 0), white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 7),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), 7),
            ("BACKGROUND", (0, 1), (0, -1), LIGHT_BG),
            ("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"),
            ("BACKGROUND", (-1, 1), (-1, -1), LIGHT_BG),
            ("FONTNAME", (-1, 1), (-1, -1), "Helvetica-Bold"),
            ("BACKGROUND", (0, -1), (-1, -1), LIGHT_BG),
            ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
            ("BOX", (0, 0), (-1, -1), 1, black),
            ("INNERGRID", (0, 0), (-1, -1), 0.5, HexColor("#cccccc")),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ("LEFTPADDING", (0, 0), (-1, -1), 2),
            ("RIGHTPADDING", (0, 0), (-1, -1), 2),
        ]
        for row_idx, grade in enumerate(grades):
            for col_idx, size in enumerate(size_cols):
                val = pct_pivot.loc[grade, size]
                if val and float(val) > 0:
                    sizer_style.append(("BACKGROUND", (col_idx + 1, row_idx + 1), (col_idx + 1, row_idx + 1), _pdf_color(float(val))))
        sizer_table.setStyle(TableStyle(sizer_style))
        sizer_section = [Paragraph("Sizer Profile", section_style), Spacer(1, 6), sizer_table]
        story.append(KeepTogether(sizer_section))

    cull_headers = load_cull_headers_multi(run_keys)
    bin_temps = [float(h["BIN_TEMP"]) for h in cull_headers if h.get("BIN_TEMP") and pd.notnull(h.get("BIN_TEMP")) and float(h.get("BIN_TEMP", 0)) != 0]
    tub_temps = [float(h["TUB_TEMP"]) for h in cull_headers if h.get("TUB_TEMP") and pd.notnull(h.get("TUB_TEMP")) and float(h.get("TUB_TEMP", 0)) != 0]
    avg_bin = sum(bin_temps) / len(bin_temps) if bin_temps else None
    avg_tub = sum(tub_temps) / len(tub_temps) if tub_temps else None
    inspector = (cull_headers[0].get("CMI_INSPECTOR") if cull_headers else None) or "N/A"
    qc_parts = []
    if avg_bin is not None:
        qc_parts.append(f"<b>Bin Temp:</b> {avg_bin:.1f}F")
    if avg_tub is not None:
        qc_parts.append(f"<b>Tub Temp:</b> {avg_tub:.1f}F")
    qc_parts.append(f"<b>CMI Inspector:</b> {inspector}")
    if qc_parts:
        story.append(Paragraph("Quality Control", section_style))
        story.append(Spacer(1, 6))
        story.append(Paragraph(" ".join(qc_parts), value_style))
        story.append(Spacer(1, 6))

    cull_df = load_cull_defects_multi(run_keys)
    cull_df = cull_df[cull_df["COUNT_INT"].fillna(0) > 0] if not cull_df.empty else cull_df
    if not cull_df.empty:
        story.append(Paragraph("Cull Analysis", section_style))
        story.append(Spacer(1, 6))
        cull_data = [["Defect", "Count"]]
        for _, cr in cull_df.iterrows():
            cull_data.append([_safe_str(cr["DEFECT_TYPE"]), _fmt_num(cr["COUNT_INT"])])
        cull_data.append(["TOTAL", _fmt_num(int(cull_df["COUNT_INT"].sum()))])
        cull_table = Table(cull_data, colWidths=[FULL_WIDTH * 0.7, FULL_WIDTH * 0.3])
        cull_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
            ("TEXTCOLOR", (0, 0), (-1, 0), white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 9),
            ("ALIGN", (0, 0), (0, -1), "LEFT"),
            ("ALIGN", (1, 0), (1, -1), "CENTER"),
            ("FONTNAME", (0, 1), (-1, -2), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), 9),
            ("BOX", (0, 0), (-1, -1), 1, black),
            ("INNERGRID", (0, 0), (-1, -1), 0.5, black),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("LEFTPADDING", (0, 0), (0, -1), 6),
            ("BACKGROUND", (0, -1), (-1, -1), LIGHT_BG),
            ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ]))
        story.append(cull_table)

    press_df = load_pressure_multi(run_keys)
    press_df = press_df[press_df["PRESSURE_DEC"].fillna(0) > 0] if not press_df.empty else press_df
    if not press_df.empty:
        story.append(Paragraph("Pressure Analysis", section_style))
        story.append(Spacer(1, 6))
        press_data = [["Size", "Pressure"]]
        for _, pr in press_df.iterrows():
            press_data.append([str(int(pr["FRUIT_SIZE_INT"])) if pd.notna(pr["FRUIT_SIZE_INT"]) else "—", f"{float(pr['PRESSURE_DEC']):.2f}" if pd.notna(pr["PRESSURE_DEC"]) else "—"])
        press_table = Table(press_data, colWidths=[FULL_WIDTH / 2, FULL_WIDTH / 2])
        press_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
            ("TEXTCOLOR", (0, 0), (-1, 0), white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 9),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), 9),
            ("BOX", (0, 0), (-1, -1), 1, black),
            ("INNERGRID", (0, 0), (-1, -1), 0.5, black),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        story.append(press_table)
        avg_press = float(press_df["PRESSURE_DEC"].mean())
        story.append(Spacer(1, 6))
        story.append(Paragraph(f"<b>Average Pressure: {avg_press:.2f}</b>", value_style))

    comments = "; ".join(c for c in main_df["COMMENTS"].dropna().astype(str).tolist() if c and c.strip())
    contacted = "; ".join(c for c in main_df["GROWER_CONTACTED"].dropna().astype(str).tolist() if c and c.strip())
    submitted = ", ".join(set(s for s in main_df["SUBMITTED_BY"].dropna().astype(str).tolist() if s)) or "N/A"
    if comments or contacted:
        story.append(Paragraph("Notes", section_style))
        story.append(Spacer(1, 6))
        if contacted:
            story.append(Paragraph(f"<b>Grower Contacted:</b> {contacted}", value_style))
        if comments:
            story.append(Paragraph(f"<b>Comments:</b> {comments}", value_style))
    story.append(Spacer(1, 12))
    verification_table = Table([[f"Verified By: {submitted}"]], colWidths=[FULL_WIDTH])
    verification_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_BG),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(verification_table)
    story.append(Spacer(1, 12))
    footer_style = ParagraphStyle("Footer", parent=styles["Normal"], fontSize=8, textColor=HexColor("#808080"), fontName="Helvetica-Oblique", alignment=TA_CENTER)
    story.append(Paragraph("Generated by Columbia Fruit Analytics", footer_style))

    doc.build(story, onFirstPage=lambda c, d: _header_footer(c, d, pdf_filename), onLaterPages=lambda c, d: _header_footer(c, d, pdf_filename))
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes, pdf_filename


def generate_all_pdfs_zip(run_date):
    """Generate PDFs for all groups on the given date, return as ZIP bytes. Returns (bytes, filename) or (None, error_msg)."""
    if not _HAS_REPORTLAB:
        return None, "reportlab is required. Install with: pip install reportlab"
    if not run_date:
        return None, "Select a date first."
    df = load_groups_for_date(run_date)
    if df.empty:
        return None, "No finalized groups for this date."
    date_str = str(run_date).replace("-", "")[:8]
    zip_filename = f"{date_str}_production_reports.zip"
    zip_buffer = BytesIO()
    generated = 0
    errors = []
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for _, r in df.iterrows():
            g, v, p = r["GROWER"], r["VARIETY_USER_CD"], r["POOL"]
            group_value = f"{g}|{v}|{p}"
            pdf_bytes, pdf_name = generate_pdf_bytes(run_date, group_value)
            if pdf_bytes:
                zf.writestr(pdf_name, pdf_bytes)
                generated += 1
            else:
                errors.append(f"{g}/{v}/{p}: {pdf_name}")
    if generated == 0:
        return None, "No PDFs generated. " + ("; ".join(errors[:3]) if errors else "")
    zip_buffer.seek(0)
    return zip_buffer.getvalue(), zip_filename


# Shared table styles
_th = {"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "center", "color": "#000", "backgroundColor": "#e9ecef"}
_td = {"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "center", "color": "#333", "backgroundColor": "#fff"}
_td_left = {"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "left", "fontWeight": "600", "backgroundColor": "#ecf0f1", "color": "#333"}
_td_total = {"padding": "6px 8px", "fontSize": "0.75rem", "textAlign": "center", "fontWeight": "600", "backgroundColor": "#ecf0f1", "color": "#333"}


# ── Layout ─────────────────────────────────────────────────────────────
layout = html.Div([
    dbc.Container([
        html.A("← Back", href="/", style={
            "color": "#aaa", "fontSize": "0.95rem", "textDecoration": "none",
            "display": "inline-flex", "alignItems": "center", "marginBottom": "12px",
        }),
        html.H4("Production Finalized Report", className="text-white mb-3"),
        html.P(
            "View verified production summaries by date and grower/variety/pool. Same data as the PDF report.",
            style={"color": "#aaa", "marginBottom": "16px"},
        ),
        dbc.Row([
            dbc.Col([
                html.Label("Report Date", style={"color": "#ccc", "fontSize": "0.85rem"}),
                dcc.DatePickerSingle(
                    id="pfr-date",
                    date=datetime.now().date(),
                    display_format="YYYY-MM-DD",
                    style={"width": "100%"},
                ),
            ], width=2),
            dbc.Col([
                html.Label("Group (Grower — Variety — Pool)", style={"color": "#ccc", "fontSize": "0.85rem"}),
                dcc.Dropdown(id="pfr-group-dropdown", placeholder="Select a group", className="tv-date-dropdown"),
            ], width=4),
            dbc.Col([
                html.Label("\u00a0", style={"color": "#ccc", "fontSize": "0.85rem"}),
                html.Div([
                    dbc.Button("Generate PDF", id="pfr-generate-pdf-btn", color="primary", size="md", className="me-2 mb-1"),
                    dbc.Button("Generate All PDFs for Date", id="pfr-generate-all-pdf-btn", color="secondary", size="md", outline=True, className="mb-1"),
                ]),
            ], width=3, className="d-flex flex-column justify-content-end"),
        ], className="mb-4 g-3"),
        html.Div([
            html.Div(id="pfr-pdf-error", style={"color": "#e74c3c", "fontSize": "0.85rem", "marginTop": "4px"}),
            html.Div(id="pfr-zip-error", style={"color": "#e74c3c", "fontSize": "0.85rem", "marginTop": "4px"}),
        ]),
        html.Div(id="pfr-report-content"),
    ], fluid=True, className="py-4"),
], className="tv-root", style={"backgroundColor": "#1a1a1a", "minHeight": "100vh"})


# ── Callbacks ─────────────────────────────────────────────────────────
@callback(
    Output("pfr-download-pdf", "data"),
    Output("pfr-pdf-error", "children"),
    Input("pfr-generate-pdf-btn", "n_clicks"),
    State("pfr-date", "date"),
    State("pfr-group-dropdown", "value"),
    prevent_initial_call=True,
)
def trigger_pdf_download(n_clicks, run_date, group_value):
    if not n_clicks:
        return no_update, no_update
    try:
        pdf_bytes, filename = generate_pdf_bytes(run_date, group_value)
        if pdf_bytes is None:
            return no_update, filename if isinstance(filename, str) else "PDF generation failed."

        if hasattr(dcc, "send_bytes"):
            def _write(bio):
                bio.write(pdf_bytes)
            return dcc.send_bytes(_write, filename), ""
        return (
            dict(content=base64.b64encode(pdf_bytes).decode(), filename=filename, base64=True, type="application/pdf"),
            "",
        )
    except Exception as e:
        return no_update, f"Error: {str(e)}"


@callback(
    Output("pfr-download-zip", "data"),
    Output("pfr-zip-error", "children"),
    Input("pfr-generate-all-pdf-btn", "n_clicks"),
    State("pfr-date", "date"),
    prevent_initial_call=True,
)
def trigger_all_pdfs_zip(n_clicks, run_date):
    if not n_clicks:
        return no_update, no_update
    try:
        zip_bytes, filename = generate_all_pdfs_zip(run_date)
        if zip_bytes is None:
            return no_update, filename if isinstance(filename, str) else "ZIP generation failed."

        if hasattr(dcc, "send_bytes"):
            def _write(bio):
                bio.write(zip_bytes)
            return dcc.send_bytes(_write, filename), ""
        return (
            dict(content=base64.b64encode(zip_bytes).decode(), filename=filename, base64=True, type="application/zip"),
            "",
        )
    except Exception as e:
        return no_update, f"Error: {str(e)}"


@callback(
    Output("pfr-group-dropdown", "options"),
    Output("pfr-group-dropdown", "value"),
    Input("pfr-date", "date"),
)
def update_group_options(run_date):
    if not run_date:
        return [], None
    df = load_groups_for_date(run_date)
    if df.empty:
        return [], None
    opts = []
    for _, r in df.iterrows():
        g, v, p = r["GROWER"], r["VARIETY_USER_CD"], r["POOL"]
        val = f"{g}|{v}|{p}"
        runs = int(r["RUNS"]) if pd.notna(r["RUNS"]) else 0
        bins = int(r["BINS"]) if pd.notna(r["BINS"]) else 0
        net = int(r["NET"]) if pd.notna(r["NET"]) else 0
        label = f"{g} — {v} — {p} ({runs} run(s), {bins:,} bins, {net:,} net lbs)"
        opts.append({"label": label, "value": val})
    return opts, opts[0]["value"] if opts else None


@callback(
    Output("pfr-report-content", "children"),
    Input("pfr-date", "date"),
    Input("pfr-group-dropdown", "value"),
)
def render_report(run_date, group_value):
    if not run_date or not group_value or "|" not in group_value:
        return html.P("Select a date and a group.", style={"color": "#999", "padding": "20px"})
    g, v, p = group_value.split("|", 2)
    main_df = load_main_for_group(run_date, g, v, p)
    if main_df.empty:
        return html.P("No data for this group.", style={"color": "#FFC107"})

    run_keys = main_df["UNIQUE_RUN_KEY"].dropna().astype(str).tolist()
    first = main_df.iloc[0]
    bins = int(main_df["BINS_SUBMITTED"].fillna(0).sum())
    gross = int(main_df["ACTUAL_GROSS"].fillna(0).sum())
    net = int(main_df["ACTUAL_NET"].fillna(0).sum())
    tare = int(main_df["ACTUAL_TARE"].fillna(0).sum())
    sizer_wt = float(main_df["ACTUAL_SIZER_WEIGHT"].fillna(0).sum())
    stamper_wt = float(main_df["ACTUAL_STAMPER_WEIGHT"].fillna(0).sum())
    packs = float(main_df["PACKS"].fillna(0).sum()) if "PACKS" in main_df.columns else 0
    shifts = sorted(main_df["SHIFT"].dropna().unique())
    shift_str = "+".join(str(s) for s in shifts)
    first_dump = main_df["FIRST_DUMP_TIME"].min()
    last_dump = main_df["LAST_DUMP_TIME"].max()
    tare_breakdown = ", ".join(t for t in main_df["TARE_BREAKDOWN"].dropna().tolist() if t) or "N/A"
    block_breakdown = _combine_breakdowns(main_df["BLOCK_BREAKDOWN"].dropna().tolist())
    pick_breakdown = _combine_breakdowns(main_df["PICK_BREAKDOWN"].dropna().tolist())
    is_combined = len(main_df) > 1

    # Sizer profile
    sizer_df = load_sizer_profile_multi(run_keys)
    pct_pivot, row_totals, col_totals = build_sizer_matrix(sizer_df) if not sizer_df.empty else (None, None, None)
    sizer_section = []
    if pct_pivot is not None:
        size_cols = list(pct_pivot.columns)
        grades = list(pct_pivot.index)
        header = [html.Th("Grade", style=_th)] + [html.Th(str(s), style=_th) for s in size_cols] + [html.Th("Total", style=_th)]
        sizer_rows = [html.Tr(header)]
        for grade in grades:
            cells = [html.Td(grade, style=_td_left)]
            for size in size_cols:
                val = pct_pivot.loc[grade, size]
                val = 0.0 if pd.isna(val) else float(val)
                bg = _get_gradient_color(val) if val > 0 else "#ecf0f1"
                cells.append(html.Td(f"{val:.2f}%" if val > 0 else "", style={**_td, "backgroundColor": bg}))
            cells.append(html.Td(f"{float(row_totals[grade]):.2f}%", style=_td_total))
            sizer_rows.append(html.Tr(cells))
        total_row = [html.Td("Total", style=_td_left)]
        for size in size_cols:
            val = col_totals[size]
            total_row.append(html.Td(f"{float(val):.2f}%" if val and float(val) > 0 else "", style=_td_total))
        total_row.append(html.Td("100.00%", style=_td_total))
        sizer_rows.append(html.Tr(total_row))
        sizer_section = [html.Table(
            [html.Thead(sizer_rows[0]), html.Tbody(sizer_rows[1:])],
            style={"width": "100%", "borderCollapse": "collapse", "fontSize": "0.8rem"},
        )]
    else:
        sizer_section = [html.P("No sizer data.", style={"color": "#999"})]

    # Processor
    proc_df = load_processor_multi(run_keys)
    proc_section = []
    if not proc_df.empty and net > 0:
        proc_rows = []
        for _, pr in proc_df.iterrows():
            w = float(pr["NET_WEIGHT"]) if pd.notna(pr["NET_WEIGHT"]) else 0
            pct = (w / net * 100) if net else 0
            proc_rows.append(html.Tr([
                html.Td(_safe_str(pr["SIZE_ABBR"]), style=_td_left),
                html.Td(_fmt_num(w), style=_td),
                html.Td(f"{pct:.1f}%", style=_td),
            ]))
        total_proc = proc_df["NET_WEIGHT"].sum()
        total_pct = (total_proc / net * 100) if net else 0
        proc_rows.append(html.Tr([
            html.Td("Total", style={**_td_left, "fontWeight": "700"}),
            html.Td(_fmt_num(total_proc), style={**_td, "fontWeight": "600"}),
            html.Td(f"{total_pct:.1f}%", style={**_td, "fontWeight": "600"}),
        ]))
        proc_section = [html.Table(
            [html.Thead(html.Tr([html.Th("Type", style=_th), html.Th("Net Weight", style=_th), html.Th("Processor %", style=_th)])),
             html.Tbody(proc_rows)],
            style={"width": "100%", "borderCollapse": "collapse", "fontSize": "0.85rem"},
        )]
    else:
        proc_section = [html.P("No processor data.", style={"color": "#999"})]

    # Cull
    cull_df = load_cull_defects_multi(run_keys)
    cull_df = cull_df[cull_df["COUNT_INT"].fillna(0) > 0] if not cull_df.empty else cull_df
    cull_section = []
    if not cull_df.empty:
        cull_rows = [html.Tr([html.Th("Defect", style=_th), html.Th("Count", style=_th)])]
        for _, cr in cull_df.iterrows():
            cull_rows.append(html.Tr([
                html.Td(_safe_str(cr["DEFECT_TYPE"]), style=_td_left),
                html.Td(_fmt_num(cr["COUNT_INT"]), style=_td),
            ]))
        tot = int(cull_df["COUNT_INT"].sum())
        cull_rows.append(html.Tr([
            html.Td("TOTAL", style={**_td_left, "fontWeight": "700"}),
            html.Td(_fmt_num(tot), style={**_td, "fontWeight": "600"}),
        ]))
        cull_section = [html.Table(
            [html.Thead(cull_rows[0]), html.Tbody(cull_rows[1:])],
            style={"width": "100%", "borderCollapse": "collapse", "fontSize": "0.85rem"},
        )]
    else:
        cull_section = [html.P("No cull data.", style={"color": "#999"})]

    # Pressure
    press_df = load_pressure_multi(run_keys)
    press_df = press_df[press_df["PRESSURE_DEC"].fillna(0) > 0] if not press_df.empty else press_df
    press_section = []
    if not press_df.empty:
        press_rows = [html.Tr([html.Th("Size", style=_th), html.Th("Pressure", style=_th)])]
        for _, pr in press_df.iterrows():
            press_rows.append(html.Tr([
                html.Td(str(int(pr["FRUIT_SIZE_INT"])) if pd.notna(pr["FRUIT_SIZE_INT"]) else "—", style=_td),
                html.Td(f"{float(pr['PRESSURE_DEC']):.2f}" if pd.notna(pr["PRESSURE_DEC"]) else "—", style=_td),
            ]))
        avg = float(press_df["PRESSURE_DEC"].mean())
        press_section = [
            html.Table([html.Thead(press_rows[0]), html.Tbody(press_rows[1:])], style={"width": "100%", "borderCollapse": "collapse", "fontSize": "0.85rem"}),
            html.P(f"Average Pressure: {avg:.2f}", style={"color": "#ccc", "fontSize": "0.9rem", "marginTop": "8px"}),
        ]
    else:
        press_section = [html.P("No pressure data.", style={"color": "#999"})]

    # QC
    cull_headers = load_cull_headers_multi(run_keys)
    bin_temps = [float(h["BIN_TEMP"]) for h in cull_headers if h.get("BIN_TEMP") and pd.notnull(h.get("BIN_TEMP")) and float(h.get("BIN_TEMP", 0)) != 0]
    tub_temps = [float(h["TUB_TEMP"]) for h in cull_headers if h.get("TUB_TEMP") and pd.notnull(h.get("TUB_TEMP")) and float(h.get("TUB_TEMP", 0)) != 0]
    avg_bin = sum(bin_temps) / len(bin_temps) if bin_temps else None
    avg_tub = sum(tub_temps) / len(tub_temps) if tub_temps else None
    inspector = (cull_headers[0].get("CMI_INSPECTOR") if cull_headers else None) or "N/A"
    qc_parts = []
    if avg_bin is not None:
        qc_parts.append(f"Bin Temp: {avg_bin:.1f}F")
    if avg_tub is not None:
        qc_parts.append(f"Tub Temp: {avg_tub:.1f}F")
    qc_parts.append(f"CMI Inspector: {inspector}")
    qc_section = html.P(" • ".join(qc_parts), style={"color": "#ccc", "fontSize": "0.9rem"}) if qc_parts else html.P("No QC data.", style={"color": "#999"})

    # Notes
    comments = "; ".join(c for c in main_df["COMMENTS"].dropna().astype(str).tolist() if c and c.strip())
    contacted = "; ".join(c for c in main_df["GROWER_CONTACTED"].dropna().astype(str).tolist() if c and c.strip())
    submitted = ", ".join(set(s for s in main_df["SUBMITTED_BY"].dropna().astype(str).tolist() if s))

    packout_val = packs / bins if bins > 0 else 0
    packout_pct = (packs * 40 / net * 100) if net > 0 and packs else 0

    report_parts = []

    if is_combined:
        report_parts.append(html.Div("COMBINED REPORT: Shifts " + shift_str, style={"color": "#9b59b6", "fontWeight": "600", "textAlign": "center", "marginBottom": "16px"}))

    report_parts.append(_section("Run Information", [
        html.Table(
            html.Tbody([
                html.Tr([
                    html.Td("Packing Date:", style={"color": "#888", "padding": "4px 12px 4px 0"}), html.Td(_fmt_dt(first.get("RUN_DATE"), "%m/%d/%Y"), style={"color": "#fff"}),
                    html.Td("Pack Line:", style={"color": "#888", "padding": "4px 12px 4px 0"}), html.Td(_safe_str(first.get("PACK_LINE")), style={"color": "#fff"}),
                ]),
                html.Tr([
                    html.Td("Grower:", style={"color": "#888", "padding": "4px 12px 4px 0"}), html.Td(f"{_safe_str(g)} — {_safe_str(first.get('GROWER_FULL_NAME',''))}", style={"color": "#fff"}),
                    html.Td("Shift:", style={"color": "#888", "padding": "4px 12px 4px 0"}), html.Td(shift_str, style={"color": "#fff"}),
                ]),
                html.Tr([
                    html.Td("Pool:", style={"color": "#888", "padding": "4px 12px 4px 0"}), html.Td(_safe_str(p), style={"color": "#fff"}),
                    html.Td("Start Time:", style={"color": "#888", "padding": "4px 12px 4px 0"}), html.Td(_fmt_dt(first_dump), style={"color": "#fff"}),
                ]),
                html.Tr([
                    html.Td("Variety:", style={"color": "#888", "padding": "4px 12px 4px 0"}), html.Td(_safe_str(v), style={"color": "#fff"}),
                    html.Td("End Time:", style={"color": "#888", "padding": "4px 12px 4px 0"}), html.Td(_fmt_dt(last_dump), style={"color": "#fff"}),
                ]),
            ]),
            style={"color": "#fff", "fontSize": "0.9rem"},
        ),
    ]))

    report_parts.append(_section("Bins & Weight Summary", [
        html.Table([
            html.Thead(html.Tr([
                html.Th("BINS", style={**_th, "width": "25%"}), html.Th("GROSS (lbs)", style=_th), html.Th("TARE (lbs)", style=_th),
                html.Th("NET (lbs)", style={**_th, "backgroundColor": "#27ae60", "color": "#fff"}),
            ])),
            html.Tbody(html.Tr([
                html.Td(_fmt_num(bins), style={**_td, "fontSize": "1rem", "fontWeight": "600"}),
                html.Td(_fmt_num(gross), style={**_td, "fontSize": "1rem"}),
                html.Td(_fmt_num(tare), style={**_td, "fontSize": "1rem"}),
                html.Td(_fmt_num(net), style={**_td, "fontSize": "1rem", "fontWeight": "600", "backgroundColor": "#27ae60", "color": "#fff"}),
            ])),
        ], style={"width": "100%", "borderCollapse": "collapse"}),
        html.P(f"Tare Breakdown: {tare_breakdown}", style={"color": "#ccc", "marginTop": "8px"}),
        html.Table([
            html.Thead(html.Tr([html.Th("Block Breakdown", style=_th), html.Th("Pick Breakdown", style=_th)])),
            html.Tbody(html.Tr([html.Td(block_breakdown, style=_td_left), html.Td(pick_breakdown, style=_td_left)])),
        ], style={"width": "100%", "borderCollapse": "collapse", "marginTop": "8px"}),
    ]))

    report_parts.append(_section("Packed Weights", [
        html.Table([
            html.Thead(html.Tr([html.Th("Sizer Weight (lbs)", style=_th), html.Th("Stamper Weight (lbs)", style=_th)])),
            html.Tbody(html.Tr([html.Td(_fmt_num(sizer_wt, 2), style=_td), html.Td(_fmt_num(stamper_wt, 2), style=_td)])),
        ], style={"width": "100%", "borderCollapse": "collapse"}),
    ]))

    report_parts.append(_section("Processor", proc_section))

    if packs and packs > 0:
        report_parts.append(_section("Packout Summary", [
            html.Table([
                html.Thead(html.Tr([html.Th("Bins", style=_th), html.Th("Packs", style=_th), html.Th("Packout", style=_th), html.Th("Packout %", style=_th)])),
                html.Tbody(html.Tr([
                    html.Td(_fmt_num(bins), style=_td),
                    html.Td(_fmt_num(packs, 0), style=_td),
                    html.Td(_fmt_num(packout_val, 1), style=_td),
                    html.Td(f"{packout_pct:.1f}%", style=_td),
                ])),
            ], style={"width": "100%", "borderCollapse": "collapse"}),
        ]))

    report_parts.append(_section("Sizer Profile", sizer_section))
    report_parts.append(_section("Quality Control", [qc_section]))
    report_parts.append(_section("Cull Analysis", cull_section))
    report_parts.append(_section("Pressure Analysis", press_section))

    notes_children = []
    if contacted:
        notes_children.append(html.P(f"Grower Contacted: {contacted}", style={"color": "#ccc"}))
    if comments:
        notes_children.append(html.P(f"Comments: {comments}", style={"color": "#ccc"}))
    notes_children.append(html.P(f"Verified By: {submitted or 'N/A'}", style={"color": "#27ae60", "fontWeight": "600", "marginTop": "12px"}))
    report_parts.append(_section("Notes", notes_children))

    return html.Div(report_parts)
