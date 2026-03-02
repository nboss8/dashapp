# pages/6_PDF.py
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from io import BytesIO
import zipfile
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor, white, black, Color
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
import tempfile
import os
from utils import *
st.set_page_config(page_title="PDF Report", layout="wide")
if 'selected_run_key' not in st.session_state:
    st.error("No run selected")
    if st.button("Back to Main"):
        st.switch_page("streamlit_app.py")
    st.stop()
row = pd.Series(st.session_state.selected_run_data)
run_key_suffix = f"*{st.session_state.selected_run_key}"
st.title("PDF Report")
st.subheader(f"Run {row['RUN_NUMBER']} - {row['VARIETY_USER_CD']} - Grower {row['GROWER']} - {row['PACK_LINE']} - Pool {row['POOL']} - Date {row['RUN_DATE']}")
# Color scheme
PRIMARY_COLOR = HexColor('#2980b9')
HEADER_BG = HexColor('#34495e')
LIGHT_BG = HexColor('#ecf0f1')
ACCENT_COLOR = HexColor('#27ae60')
WARNING_COLOR = HexColor('#e74c3c')
FULL_WIDTH = 7.5 * inch
def get_gradient_color(value, min_val=0, max_val=20):
    if value <= min_val:
        return white
    ratio = min((value - min_val) / (max_val - min_val), 1.0)
    r = int(255 - (255 - 100) * ratio)
    g = int(255 - (255 - 149) * ratio)
    b = int(255 - (255 - 237) * ratio)
    return Color(r/255, g/255, b/255)
def load_related_runs(grower, run_date, variety, pool):
    """Load all runs for the same grower, date, variety, and pool."""
    return session.sql("""
        SELECT UNIQUE_RUN_KEY, RUN_NUMBER, SHIFT, VARIETY_USER_CD, PACK_LINE, POOL,
               COALESCE(BINS_SUBMITTED, 0) as BINS_SUBMITTED,
               COALESCE(ACTUAL_NET, 0) as ACTUAL_NET
        FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER
        WHERE GROWER = ? AND RUN_DATE = ? AND VARIETY_USER_CD = ? AND POOL = ?
        ORDER BY SHIFT, RUN_NUMBER
    """, params=[grower, run_date, variety, pool]).to_pandas()
def load_all_runs_for_date(run_date):
    """Load all runs for a given date."""
    return session.sql("""
        SELECT UNIQUE_RUN_KEY, RUN_NUMBER, SHIFT, VARIETY_USER_CD, PACK_LINE, POOL, GROWER,
               COALESCE(BINS_SUBMITTED, 0) as BINS_SUBMITTED,
               COALESCE(ACTUAL_NET, 0) as ACTUAL_NET
        FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER
        WHERE RUN_DATE = ?
        ORDER BY GROWER, VARIETY_USER_CD, POOL, SHIFT, RUN_NUMBER
    """, params=[run_date]).to_pandas()
def load_sizer_profile(unique_run_key):
    return session.sql("""
        SELECT d.GRADE_NAME, d.SIZE_NAME, SUM(d.WEIGHT_DEC) as WEIGHT
        FROM FROSTY.APP.PTRUN_SIZER_DROP_SNAPSHOT d
        INNER JOIN FROSTY.APP.PTRUN_SIZER_PACKED p
            ON d.UNIQUE_RUN_KEY = p.UNIQUE_RUN_KEY
            AND d.QUALITY_NAME = p.QUALITY_NAME
            AND d.GRADE_NAME = p.GRADE_NAME
            AND d.SIZE_NAME = p.SIZE_NAME
        WHERE d.UNIQUE_RUN_KEY = ? AND p.IS_PACKED = TRUE
        GROUP BY d.GRADE_NAME, d.SIZE_NAME
        ORDER BY d.GRADE_NAME, d.SIZE_NAME
    """, params=[unique_run_key]).to_pandas()
def load_sizer_profile_multi(unique_run_keys):
    """Load sizer profile for multiple runs combined."""
    if not unique_run_keys:
        return pd.DataFrame()
    placeholders = ','.join(['?' for _ in unique_run_keys])
    return session.sql(f"""
        SELECT d.GRADE_NAME, d.SIZE_NAME, SUM(d.WEIGHT_DEC) as WEIGHT
        FROM FROSTY.APP.PTRUN_SIZER_DROP_SNAPSHOT d
        INNER JOIN FROSTY.APP.PTRUN_SIZER_PACKED p
            ON d.UNIQUE_RUN_KEY = p.UNIQUE_RUN_KEY
            AND d.QUALITY_NAME = p.QUALITY_NAME
            AND d.GRADE_NAME = p.GRADE_NAME
            AND d.SIZE_NAME = p.SIZE_NAME
        WHERE d.UNIQUE_RUN_KEY IN ({placeholders}) AND p.IS_PACKED = TRUE
        GROUP BY d.GRADE_NAME, d.SIZE_NAME
        ORDER BY d.GRADE_NAME, d.SIZE_NAME
    """, params=unique_run_keys).to_pandas()
def build_sizer_matrix(sizer_df):
    if sizer_df.empty:
        return None, None, None
    pivot = sizer_df.pivot_table(index='GRADE_NAME', columns='SIZE_NAME', values='WEIGHT', aggfunc='sum', fill_value=0)
    def size_sort_key(x):
        try:
            return int(str(x).strip())
        except:
            return 999
    size_cols = sorted(pivot.columns, key=size_sort_key)
    pivot = pivot[size_cols]
    pivot = pivot.sort_index()
    total_weight = pivot.values.sum()
    if total_weight == 0:
        return None, None, None
    pct_pivot = (pivot / total_weight * 100)
    row_totals = pct_pivot.sum(axis=1)
    col_totals = pct_pivot.sum(axis=0)
    return pct_pivot, row_totals, col_totals
def load_processor_data(unique_run_key):
    return session.sql("""
        SELECT SIZE_ABBR, SUM(TOTAL_NET_WT) as NET_WEIGHT
        FROM FROSTY.APP.PTRUN_PROCESSOR_VIEW_PBIX
        WHERE UNIQUE_RUN_KEY = ?
        GROUP BY SIZE_ABBR
        ORDER BY SIZE_ABBR
    """, params=[unique_run_key]).to_pandas()
def load_processor_data_multi(unique_run_keys):
    """Load processor data for multiple runs combined."""
    if not unique_run_keys:
        return pd.DataFrame()
    placeholders = ','.join(['?' for _ in unique_run_keys])
    return session.sql(f"""
        SELECT SIZE_ABBR, SUM(TOTAL_NET_WT) as NET_WEIGHT
        FROM FROSTY.APP.PTRUN_PROCESSOR_VIEW_PBIX
        WHERE UNIQUE_RUN_KEY IN ({placeholders})
        GROUP BY SIZE_ABBR
        ORDER BY SIZE_ABBR
    """, params=unique_run_keys).to_pandas()
def load_cull_defects_multi(unique_run_keys):
    """Load cull defects for multiple runs combined."""
    if not unique_run_keys:
        return pd.DataFrame()
    placeholders = ','.join(['?' for _ in unique_run_keys])
    return session.sql(f"""
        SELECT DEFECT_TYPE, SUM(COUNT_INT) as COUNT_INT
        FROM FROSTY.APP.PTRUN_CULL_DEFECT
        WHERE UNIQUE_RUN_KEY IN ({placeholders})
        GROUP BY DEFECT_TYPE
        ORDER BY COUNT_INT DESC
    """, params=unique_run_keys).to_pandas()
def load_pressure_details_multi(unique_run_keys):
    """Load pressure details for multiple runs - average by size."""
    if not unique_run_keys:
        return pd.DataFrame()
    placeholders = ','.join(['?' for _ in unique_run_keys])
    return session.sql(f"""
        SELECT FRUIT_SIZE_INT, AVG(PRESSURE_DEC) as PRESSURE_DEC
        FROM FROSTY.APP.PTRUN_PRESSURE_DETAIL
        WHERE UNIQUE_RUN_KEY IN ({placeholders})
        GROUP BY FRUIT_SIZE_INT
        ORDER BY FRUIT_SIZE_INT
    """, params=unique_run_keys).to_pandas()
def safe_value(val, default=0, as_type=float):
    if pd.isnull(val) or val == '' or val is None:
        return default
    try:
        return as_type(val)
    except:
        return default
def safe_str(val, default='N/A'):
    if pd.isnull(val) or val == '' or val is None:
        return default
    return str(val)
def format_number(val, decimals=0):
    if decimals == 0:
        return f"{int(val):,}"
    return f"{val:,.{decimals}f}"
def format_datetime(val, fmt='%m/%d/%Y %H:%M'):
    if pd.isnull(val):
        return 'N/A'
    if isinstance(val, str):
        return val
    try:
        return val.strftime(fmt)
    except:
        return str(val)
def create_header_footer(canvas, doc, filename, logo_path):
    canvas.saveState()
    canvas.setFillColor(HEADER_BG)
    canvas.rect(0, letter[1] - 50, letter[0], 50, fill=1, stroke=0)
    canvas.setFillColor(white)
    canvas.setFont('Helvetica-Bold', 18)
    canvas.drawString(0.5 * inch, letter[1] - 30, 'Grower Production Summary')
    canvas.setFont('Helvetica', 10)
    canvas.drawString(0.5 * inch, letter[1] - 42, 'Columbia Fruit Packers - Airport Facility')
    # Add logo to top right
    logo_width = 1 * inch  # Adjust width as needed
    logo_height = 0.5 * inch  # Adjust height to fit
    x_pos = letter[0] - logo_width - 0.5 * inch  # 0.5 inch margin from right
    y_pos = letter[1] - 50 + (50 - logo_height) / 2  # Center vertically in header
    canvas.drawImage(logo_path, x_pos, y_pos, width=logo_width, height=logo_height, mask='auto')
    canvas.setFillColor(HexColor('#808080'))
    canvas.setFont('Helvetica-Oblique', 8)
    canvas.drawString(0.5 * inch, 0.5 * inch, filename)
    canvas.drawRightString(letter[0] - 0.5 * inch, 0.5 * inch, f'Page {doc.page}')
    canvas.restoreState()
def combine_breakdowns(breakdowns):
    """Combine multiple breakdown strings intelligently."""
    if not breakdowns:
        return 'N/A'
    combined = {}
    for breakdown in breakdowns:
        if not breakdown or breakdown == 'N/A':
            continue
        parts = breakdown.split(', ')
        for part in parts:
            if '=' in part:
                key, val = part.rsplit('=', 1)
                bins = int(val.replace(' Bins', '').replace('@', '').split('@')[0]) if 'Bins' in val or '@' in val else 0
                if key in combined:
                    combined[key] += bins
                else:
                    combined[key] = bins
    if not combined:
        return 'N/A'
    return ', '.join([f"{k}={v} Bins" for k, v in combined.items()])
# GENERATE_PDF_PLACEHOLDER
def generate_pdf(selected_run_keys, sections):
    """Generate PDF for one or more runs combined."""
   
    # Fetch data for all selected runs
    placeholders = ','.join(['?' for _ in selected_run_keys])
    main_df = session.sql(f"""
        SELECT * FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER_MAT
        WHERE UNIQUE_RUN_KEY IN ({placeholders})
        ORDER BY SHIFT, RUN_NUMBER
    """, params=selected_run_keys).to_pandas()
   
    if main_df.empty:
        main_df = session.sql(f"""
            SELECT * FROM FROSTY.APP.POWERBI_PRODUCTION_HEADER
            WHERE UNIQUE_RUN_KEY IN ({placeholders})
            ORDER BY SHIFT, RUN_NUMBER
        """, params=selected_run_keys).to_pandas()
   
    if main_df.empty:
        return None, "No data found for selected runs."
   
    # Determine if this is a combined report
    is_combined = len(selected_run_keys) > 1
   
    # Aggregate main data
    first_row = main_df.iloc[0]
    bins = int(main_df['BINS_SUBMITTED'].fillna(0).sum())
    gross = int(main_df['ACTUAL_GROSS'].fillna(0).sum())
    net = int(main_df['ACTUAL_NET'].fillna(0).sum())
    tare = int(main_df['ACTUAL_TARE'].fillna(0).sum())
    sizer_weight = float(main_df['ACTUAL_SIZER_WEIGHT'].fillna(0).sum())
    stamper_weight = float(main_df['ACTUAL_STAMPER_WEIGHT'].fillna(0).sum())
    packs = float(main_df['PACKS'].fillna(0).sum())
   
    # Combine breakdowns
    tare_breakdowns = main_df['TARE_BREAKDOWN'].dropna().tolist()
    block_breakdowns = main_df['BLOCK_BREAKDOWN'].dropna().tolist()
    pick_breakdowns = main_df['PICK_BREAKDOWN'].dropna().tolist()
   
    tare_breakdown = ', '.join([t for t in tare_breakdowns if t]) if tare_breakdowns else 'N/A'
    block_breakdown = combine_breakdowns(block_breakdowns)
    pick_breakdown = combine_breakdowns(pick_breakdowns)
   
    # Get shifts involved
    shifts = sorted(main_df['SHIFT'].unique())
    shift_str = '+'.join([str(s) for s in shifts])
   
    # Get time range
    first_dump = main_df['FIRST_DUMP_TIME'].min()
    last_dump = main_df['LAST_DUMP_TIME'].max()
   
    # Aggregate QC data
    cull_headers = []
    for key in selected_run_keys:
        ch = load_existing_cull_header(key)
        if not ch.empty:
            cull_headers.append(ch.iloc[0].to_dict())
   
    cull_header = cull_headers[0] if cull_headers else None
    bin_temps = []
    tub_temps = []
    for ch in cull_headers:
        bt = ch.get('BIN_TEMP')
        tt = ch.get('TUB_TEMP')
        if bt is not None and pd.notnull(bt) and float(bt) != 0:
            bin_temps.append(float(bt))
        if tt is not None and pd.notnull(tt) and float(tt) != 0:
            tub_temps.append(float(tt))
    avg_bin_temp = sum(bin_temps) / len(bin_temps) if bin_temps else None
    avg_tub_temp = sum(tub_temps) / len(tub_temps) if tub_temps else None
   
    cull_defects = load_cull_defects_multi(selected_run_keys)
    cull_defects = cull_defects[cull_defects['COUNT_INT'] > 0].sort_values('COUNT_INT', ascending=False) if not cull_defects.empty else pd.DataFrame()
   
    pressure_details = load_pressure_details_multi(selected_run_keys)
    pressure_details = pressure_details[pressure_details['PRESSURE_DEC'] > 0].sort_values('FRUIT_SIZE_INT') if not pressure_details.empty else pd.DataFrame()
   
    # Generate filename
    run_date = first_row['RUN_DATE']
    date_str = run_date.replace('-', '') if isinstance(run_date, str) else run_date.strftime('%Y%m%d')
    pdf_filename = f"{date_str}_{first_row['GROWER']}_{first_row['POOL']}_{first_row['VARIETY_USER_CD']}_{shift_str}.pdf"
   
    # Download logo from stage to temp directory
    stage_path = '@"FROSTY"."APP"."PRODUCTION_STAGE"/CFPLogo2019Final (1).jpg'
    with tempfile.TemporaryDirectory() as temp_dir:
        session.file.get(stage_path, temp_dir)
        logo_filename = 'CFPLogo2019Final (1).jpg'
        logo_path = os.path.join(temp_dir, logo_filename)
   
        # Create PDF
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=0.5*inch, leftMargin=0.5*inch, topMargin=0.75*inch, bottomMargin=0.75*inch)
        styles = getSampleStyleSheet()
        section_style = ParagraphStyle('SectionHeader', parent=styles['Heading2'], fontSize=12, textColor=white, backColor=PRIMARY_COLOR, spaceAfter=6, spaceBefore=12, leftIndent=6, rightIndent=6, leading=16)
        label_style = ParagraphStyle('Label', parent=styles['Normal'], fontSize=9, textColor=HexColor('#505050'), fontName='Helvetica-Bold')
        value_style = ParagraphStyle('Value', parent=styles['Normal'], fontSize=9, textColor=black)
   
        story = []
        story.append(Spacer(1, 0.3 * inch))
   
        # Combined notice
        if is_combined:
            combined_style = ParagraphStyle('Combined', parent=styles['Normal'], fontSize=10, textColor=HexColor('#8e44ad'), fontName='Helvetica-Bold', alignment=TA_CENTER)
            story.append(Paragraph(f'COMBINED REPORT: Shifts {shift_str}', combined_style))
            story.append(Spacer(1, 6))
   
        # RUN INFORMATION
        if sections.get('run_info', True):
            story.append(Paragraph('Run Information', section_style))
            story.append(Spacer(1, 6))
            col_width = FULL_WIDTH / 4
            run_info_data = [
                [Paragraph('<b>Packing Date:</b>', label_style), Paragraph(format_datetime(first_row['RUN_DATE'], '%m/%d/%Y'), value_style), Paragraph('<b>Pack Line:</b>', label_style), Paragraph(safe_str(first_row.get('PACK_LINE', 'N/A')), value_style)],
                [Paragraph('<b>Grower:</b>', label_style), Paragraph(f"{first_row['GROWER']} - {safe_str(first_row.get('GROWER_FULL_NAME', ''))}", value_style), Paragraph('<b>Shift:</b>', label_style), Paragraph(shift_str, value_style)],
                [Paragraph('<b>Pool:</b>', label_style), Paragraph(safe_str(first_row['POOL']), value_style), Paragraph('<b>Start Time:</b>', label_style), Paragraph(format_datetime(first_dump), value_style)],
                [Paragraph('<b>Variety:</b>', label_style), Paragraph(safe_str(first_row['VARIETY_USER_CD']), value_style), Paragraph('<b>End Time:</b>', label_style), Paragraph(format_datetime(last_dump), value_style)],
            ]
            run_info_table = Table(run_info_data, colWidths=[col_width] * 4)
            run_info_table.setStyle(TableStyle([('VALIGN', (0, 0), (-1, -1), 'MIDDLE'), ('TOPPADDING', (0, 0), (-1, -1), 3), ('BOTTOMPADDING', (0, 0), (-1, -1), 3)]))
            story.append(run_info_table)
   
        # BINS & WEIGHT
        if sections.get('bins_weight', True):
            story.append(Paragraph('Bins & Weight Summary', section_style))
            story.append(Spacer(1, 6))
            col_width = FULL_WIDTH / 4
            metrics_data = [['BINS', 'GROSS (lbs)', 'TARE (lbs)', 'NET (lbs)'], [format_number(bins), format_number(gross), format_number(tare), format_number(net)]]
            metrics_table = Table(metrics_data, colWidths=[col_width] * 4)
            metrics_table.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, 0), LIGHT_BG), ('TEXTCOLOR', (0, 0), (-1, 0), black), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica'), ('FONTSIZE', (0, 0), (-1, 0), 8), ('ALIGN', (0, 0), (-1, -1), 'CENTER'), ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'), ('FONTNAME', (0, 1), (-1, 1), 'Helvetica-Bold'), ('FONTSIZE', (0, 1), (-1, 1), 12), ('BACKGROUND', (3, 0), (3, 1), ACCENT_COLOR), ('TEXTCOLOR', (3, 0), (3, 1), white), ('BOX', (0, 0), (-1, -1), 1, black), ('INNERGRID', (0, 0), (-1, -1), 0.5, black), ('TOPPADDING', (0, 0), (-1, -1), 4), ('BOTTOMPADDING', (0, 0), (-1, -1), 4)]))
            story.append(metrics_table)
            story.append(Spacer(1, 6))
            story.append(Paragraph(f"<b>Tare Breakdown:</b> {tare_breakdown}", value_style))
            story.append(Spacer(1, 6))
            breakdown_data = [['Block Breakdown', 'Pick Breakdown'], [block_breakdown, pick_breakdown]]
            breakdown_table = Table(breakdown_data, colWidths=[FULL_WIDTH / 2, FULL_WIDTH / 2])
            breakdown_table.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, 0), HEADER_BG), ('TEXTCOLOR', (0, 0), (-1, 0), white), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, 0), 9), ('ALIGN', (0, 0), (-1, 0), 'CENTER'), ('FONTNAME', (0, 1), (-1, 1), 'Helvetica'), ('FONTSIZE', (0, 1), (-1, 1), 9), ('ALIGN', (0, 1), (-1, 1), 'LEFT'), ('BOX', (0, 0), (-1, -1), 1, black), ('INNERGRID', (0, 0), (-1, -1), 0.5, black), ('TOPPADDING', (0, 0), (-1, -1), 4), ('BOTTOMPADDING', (0, 0), (-1, -1), 4), ('LEFTPADDING', (0, 1), (-1, 1), 6)]))
            story.append(breakdown_table)
   
        # PACKED WEIGHTS
        if sections.get('packed_weights', True):
            story.append(Paragraph('Packed Weights', section_style))
            story.append(Spacer(1, 6))
            packed_data = [['Sizer Weight (lbs)', 'Stamper Weight (lbs)'], [format_number(sizer_weight, 2), format_number(stamper_weight, 2)]]
            packed_table = Table(packed_data, colWidths=[FULL_WIDTH / 2, FULL_WIDTH / 2])
            packed_table.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, 0), HEADER_BG), ('TEXTCOLOR', (0, 0), (-1, 0), white), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, 0), 9), ('ALIGN', (0, 0), (-1, -1), 'CENTER'), ('FONTNAME', (0, 1), (-1, 1), 'Helvetica'), ('FONTSIZE', (0, 1), (-1, 1), 9), ('BOX', (0, 0), (-1, -1), 1, black), ('INNERGRID', (0, 0), (-1, -1), 0.5, black), ('TOPPADDING', (0, 0), (-1, -1), 4), ('BOTTOMPADDING', (0, 0), (-1, -1), 4)]))
            story.append(packed_table)
            # Skip variance for combined reports - too complex to aggregate accurately
   
        # PROCESSOR
        if sections.get('processor', True):
            processor_df = load_processor_data_multi(selected_run_keys)
            if not processor_df.empty:
                story.append(Paragraph('Processor', section_style))
                story.append(Spacer(1, 6))
                total_processor_weight = processor_df['NET_WEIGHT'].sum()
                processor_data = [['Type', 'Net Weight', 'Processor %']]
                for _, proc_row in processor_df.iterrows():
                    weight = safe_value(proc_row['NET_WEIGHT'], 0, float)
                    pct = (weight / net * 100) if net > 0 else 0
                    processor_data.append([proc_row['SIZE_ABBR'], format_number(weight, 0), f"{pct:.1f}%"])
                total_pct = (total_processor_weight / net * 100) if net > 0 else 0
                processor_data.append(['Total', format_number(total_processor_weight, 0), f"{total_pct:.1f}%"])
                processor_table = Table(processor_data, colWidths=[FULL_WIDTH / 3] * 3)
                processor_table.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, 0), HEADER_BG), ('TEXTCOLOR', (0, 0), (-1, 0), white), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, 0), 9), ('ALIGN', (0, 0), (0, -1), 'LEFT'), ('ALIGN', (1, 0), (-1, -1), 'CENTER'), ('FONTNAME', (0, 1), (-1, -2), 'Helvetica'), ('FONTSIZE', (0, 1), (-1, -1), 9), ('BOX', (0, 0), (-1, -1), 1, black), ('INNERGRID', (0, 0), (-1, -1), 0.5, black), ('TOPPADDING', (0, 0), (-1, -1), 4), ('BOTTOMPADDING', (0, 0), (-1, -1), 4), ('LEFTPADDING', (0, 0), (0, -1), 6), ('BACKGROUND', (0, -1), (-1, -1), LIGHT_BG), ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold')]))
                story.append(processor_table)
   
        # PACKOUT
        if sections.get('packout', True) and packs > 0:
            story.append(Paragraph('Packout Summary', section_style))
            story.append(Spacer(1, 6))
            packout = packs / bins if bins > 0 else 0
            packout_pct = (packs * 40 / net * 100) if net > 0 else 0
            packout_data = [['Bins', 'Packs', 'Packout', 'Packout %'], [format_number(bins), format_number(packs, 0), format_number(packout, 1), f"{packout_pct:.1f}%"]]
            packout_table = Table(packout_data, colWidths=[FULL_WIDTH / 4] * 4)
            packout_table.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, 0), HEADER_BG), ('TEXTCOLOR', (0, 0), (-1, 0), white), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, 0), 9), ('ALIGN', (0, 0), (-1, -1), 'CENTER'), ('FONTNAME', (0, 1), (-1, 1), 'Helvetica'), ('FONTSIZE', (0, 1), (-1, 1), 9), ('BOX', (0, 0), (-1, -1), 1, black), ('INNERGRID', (0, 0), (-1, -1), 0.5, black), ('TOPPADDING', (0, 0), (-1, -1), 4), ('BOTTOMPADDING', (0, 0), (-1, -1), 4)]))
            story.append(packout_table)
   
        # SIZER_AND_MORE_PLACEHOLDER
   
        # SIZER PROFILE
        if sections.get('sizer_profile', True):
            sizer_profile_df = load_sizer_profile_multi(selected_run_keys)
            if not sizer_profile_df.empty:
                pct_pivot, row_totals, col_totals = build_sizer_matrix(sizer_profile_df)
                if pct_pivot is not None:
                    size_columns = list(pct_pivot.columns)
                    header_row = ['Grade'] + [str(s) for s in size_columns] + ['Total']
                    sizer_data = [header_row]
                    grade_names = list(pct_pivot.index)
                    for grade in grade_names:
                        row_data = [grade]
                        for size in size_columns:
                            val = pct_pivot.loc[grade, size]
                            row_data.append(f"{val:.2f}%" if val > 0 else "")
                        row_data.append(f"{row_totals[grade]:.2f}%")
                        sizer_data.append(row_data)
                    total_row = ['Total']
                    for size in size_columns:
                        val = col_totals[size]
                        total_row.append(f"{val:.2f}%" if val > 0 else "")
                    total_row.append("100.00%")
                    sizer_data.append(total_row)
                    grade_col_width = 0.6 * inch
                    total_col_width = 0.6 * inch
                    remaining_width = FULL_WIDTH - grade_col_width - total_col_width
                    size_col_width = remaining_width / len(size_columns)
                    col_widths = [grade_col_width] + [size_col_width] * len(size_columns) + [total_col_width]
                    sizer_table = Table(sizer_data, colWidths=col_widths)
                    sizer_style = [('BACKGROUND', (0, 0), (-1, 0), HEADER_BG), ('TEXTCOLOR', (0, 0), (-1, 0), white), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, 0), 7), ('ALIGN', (0, 0), (-1, -1), 'CENTER'), ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'), ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'), ('FONTSIZE', (0, 1), (-1, -1), 7), ('BACKGROUND', (0, 1), (0, -1), LIGHT_BG), ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'), ('BACKGROUND', (-1, 1), (-1, -1), LIGHT_BG), ('FONTNAME', (-1, 1), (-1, -1), 'Helvetica-Bold'), ('BACKGROUND', (0, -1), (-1, -1), LIGHT_BG), ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'), ('BOX', (0, 0), (-1, -1), 1, black), ('INNERGRID', (0, 0), (-1, -1), 0.5, HexColor('#cccccc')), ('TOPPADDING', (0, 0), (-1, -1), 2), ('BOTTOMPADDING', (0, 0), (-1, -1), 2), ('LEFTPADDING', (0, 0), (-1, -1), 2), ('RIGHTPADDING', (0, 0), (-1, -1), 2)]
                    for row_idx, grade in enumerate(grade_names):
                        for col_idx, size in enumerate(size_columns):
                            val = pct_pivot.loc[grade, size]
                            if val > 0:
                                sizer_style.append(('BACKGROUND', (col_idx + 1, row_idx + 1), (col_idx + 1, row_idx + 1), get_gradient_color(val)))
                    sizer_table.setStyle(TableStyle(sizer_style))
                    # Wrap the entire sizer section in KeepTogether to prevent page breaks
                    sizer_section = [
                        Paragraph('Sizer Profile', section_style),
                        Spacer(1, 6),
                        sizer_table
                    ]
                    story.append(KeepTogether(sizer_section))
   
        # QUALITY CONTROL
        if sections.get('quality_control', True):
            if avg_bin_temp is not None or avg_tub_temp is not None or cull_header is not None:
                story.append(Paragraph('Quality Control', section_style))
                story.append(Spacer(1, 6))
                qc_parts = []
                if avg_bin_temp is not None:
                    qc_parts.append(f"<b>Bin Temp:</b> {avg_bin_temp:.1f}F")
                if avg_tub_temp is not None:
                    qc_parts.append(f"<b>Tub Temp:</b> {avg_tub_temp:.1f}F")
                if cull_header and cull_header.get('CMI_INSPECTOR'):
                    qc_parts.append(f"<b>CMI Inspector:</b> {cull_header.get('CMI_INSPECTOR')}")
                if qc_parts:
                    story.append(Paragraph(' '.join(qc_parts), value_style))
                    story.append(Spacer(1, 6))
   
        # CULL ANALYSIS
        if sections.get('cull_analysis', True) and not cull_defects.empty:
            story.append(Paragraph('Cull Analysis', section_style))
            story.append(Spacer(1, 6))
            defects_list = cull_defects.to_dict('records')
            total_defects = sum(d['COUNT_INT'] for d in defects_list)
            cull_data = [['Defect', 'Count']]
            for d in defects_list:
                cull_data.append([d['DEFECT_TYPE'], format_number(int(d['COUNT_INT']))])
            cull_data.append(['TOTAL', format_number(total_defects)])
            cull_table = Table(cull_data, colWidths=[FULL_WIDTH * 0.7, FULL_WIDTH * 0.3])
            cull_style = [('BACKGROUND', (0, 0), (-1, 0), HEADER_BG), ('TEXTCOLOR', (0, 0), (-1, 0), white), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, 0), 9), ('ALIGN', (0, 0), (0, -1), 'LEFT'), ('ALIGN', (1, 0), (1, -1), 'CENTER'), ('FONTNAME', (0, 1), (-1, -2), 'Helvetica'), ('FONTSIZE', (0, 1), (-1, -1), 9), ('BOX', (0, 0), (-1, -1), 1, black), ('INNERGRID', (0, 0), (-1, -1), 0.5, black), ('TOPPADDING', (0, 0), (-1, -1), 3), ('BOTTOMPADDING', (0, 0), (-1, -1), 3), ('LEFTPADDING', (0, 0), (0, -1), 6), ('BACKGROUND', (0, -1), (-1, -1), LIGHT_BG), ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold')]
            for i in range(1, len(cull_data) - 1):
                if i % 2 == 0:
                    cull_style.append(('BACKGROUND', (0, i), (-1, i), LIGHT_BG))
            cull_table.setStyle(TableStyle(cull_style))
            story.append(cull_table)
   
        # PRESSURE ANALYSIS
        if sections.get('pressure_analysis', True) and not pressure_details.empty:
            story.append(Paragraph('Pressure Analysis', section_style))
            story.append(Spacer(1, 6))
            pressure_list = pressure_details.to_dict('records')
            if len(pressure_list) > 5:
                mid = (len(pressure_list) + 1) // 2
                pressure_data = [['Size', 'Pressure', '', 'Size', 'Pressure']]
                for i in range(mid):
                    left = pressure_list[i] if i < len(pressure_list) else None
                    right = pressure_list[mid + i] if (mid + i) < len(pressure_list) else None
                    left_size = str(int(left['FRUIT_SIZE_INT'])) if left else ''
                    left_press = f"{left['PRESSURE_DEC']:.2f}" if left else ''
                    right_size = str(int(right['FRUIT_SIZE_INT'])) if right else ''
                    right_press = f"{right['PRESSURE_DEC']:.2f}" if right else ''
                    pressure_data.append([left_size, left_press, '', right_size, right_press])
                side_width = (FULL_WIDTH - 0.4 * inch) / 4
                p_col_widths = [side_width, side_width, 0.4 * inch, side_width, side_width]
            else:
                pressure_data = [['Size', 'Pressure']]
                for p in pressure_list:
                    pressure_data.append([str(int(p['FRUIT_SIZE_INT'])), f"{p['PRESSURE_DEC']:.2f}"])
                p_col_widths = [FULL_WIDTH / 2, FULL_WIDTH / 2]
            pressure_table = Table(pressure_data, colWidths=p_col_widths)
            pressure_style = [('BACKGROUND', (0, 0), (-1, 0), HEADER_BG), ('TEXTCOLOR', (0, 0), (-1, 0), white), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, 0), 9), ('ALIGN', (0, 0), (-1, -1), 'CENTER'), ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'), ('FONTSIZE', (0, 1), (-1, -1), 9), ('BOX', (0, 0), (-1, -1), 1, black), ('TOPPADDING', (0, 0), (-1, -1), 3), ('BOTTOMPADDING', (0, 0), (-1, -1), 3)]
            if len(pressure_list) > 5:
                pressure_style.extend([('INNERGRID', (0, 0), (1, -1), 0.5, black), ('INNERGRID', (3, 0), (4, -1), 0.5, black), ('BACKGROUND', (2, 0), (2, -1), white), ('BOX', (0, 0), (1, -1), 1, black), ('BOX', (3, 0), (4, -1), 1, black)])
            else:
                pressure_style.append(('INNERGRID', (0, 0), (-1, -1), 0.5, black))
            pressure_table.setStyle(TableStyle(pressure_style))
            story.append(pressure_table)
            avg_pressure = sum(p['PRESSURE_DEC'] for p in pressure_list) / len(pressure_list)
            story.append(Spacer(1, 6))
            story.append(Paragraph(f'<b>Average Pressure: {avg_pressure:.2f}</b>', value_style))
   
        # NOTES
        if sections.get('notes', True):
            all_comments = [safe_str(r.get('COMMENTS'), '') for _, r in main_df.iterrows()]
            all_contacted = [safe_str(r.get('GROWER_CONTACTED'), '') for _, r in main_df.iterrows()]
            comments = '; '.join([c for c in all_comments if c and c != 'N/A'])
            grower_contacted = '; '.join([c for c in all_contacted if c and c != 'N/A'])
            if grower_contacted or comments:
                story.append(Paragraph('Notes', section_style))
                story.append(Spacer(1, 6))
                if grower_contacted:
                    story.append(Paragraph(f'<b>Grower Contacted:</b> {grower_contacted}', value_style))
                if comments:
                    story.append(Paragraph(f'<b>Comments:</b> {comments}', value_style))
   
        # VERIFICATION
        story.append(Spacer(1, 12))
        submitted_bys = [safe_str(r.get('SUBMITTED_BY'), '') for _, r in main_df.iterrows()]
        submitted_by = ', '.join(set([s for s in submitted_bys if s and s != 'N/A'])) or 'N/A'
        verification_table = Table([[f'Verified By: {submitted_by}']], colWidths=[FULL_WIDTH])
        verification_table.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, -1), LIGHT_BG), ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, -1), 9), ('ALIGN', (0, 0), (-1, -1), 'LEFT'), ('LEFTPADDING', (0, 0), (-1, -1), 6), ('TOPPADDING', (0, 0), (-1, -1), 4), ('BOTTOMPADDING', (0, 0), (-1, -1), 4)]))
        story.append(verification_table)
        story.append(Spacer(1, 12))
        footer_style = ParagraphStyle('Footer', parent=styles['Normal'], fontSize=8, textColor=HexColor('#808080'), fontName='Helvetica-Oblique', alignment=TA_CENTER)
        story.append(Paragraph('Generated by Apple Line Production System', footer_style))
   
        def on_page(canvas, doc):
            create_header_footer(canvas, doc, pdf_filename, logo_path)
        doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
        pdf_bytes = buffer.getvalue()
        buffer.close()
   
    return pdf_bytes, pdf_filename
def generate_selected_pdfs_zip(run_date, include_sizer, selected_groups, progress_callback=None):
    """Generate PDFs for selected grower/variety/pool groups, returned as a zip."""
    all_runs = st.session_state.all_runs
    if all_runs.empty:
        return None, "No runs found for this date.", 0, 0, None
    total_groups = len(selected_groups)
    sections = {
        'run_info': True, 'bins_weight': True, 'packed_weights': True,
        'processor': True, 'packout': True, 'sizer_profile': include_sizer,
        'quality_control': True, 'cull_analysis': True, 'pressure_analysis': True,
        'notes': True
    }
    zip_buffer = BytesIO()
    generated = 0
    errors = []
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for idx, (grower, variety, pool) in enumerate(selected_groups):
            group_df = all_runs[(all_runs['GROWER'] == grower) & (all_runs['VARIETY_USER_CD'] == variety) & (all_runs['POOL'] == pool)]
            if group_df.empty:
                errors.append(f"No runs found for Grower {grower}/{variety}/{pool}")
                continue
            run_keys = group_df['UNIQUE_RUN_KEY'].tolist()
            if progress_callback:
                progress_callback(idx + 1, total_groups, f"Grower {grower} - {variety} - Pool {pool}")
            try:
                pdf_bytes, pdf_filename = generate_pdf(run_keys, sections)
                if pdf_bytes:
                    zf.writestr(pdf_filename, pdf_bytes)
                    generated += 1
                else:
                    errors.append(f"Grower {grower}/{variety}/{pool}: {pdf_filename}")
            except Exception as e:
                errors.append(f"Grower {grower}/{variety}/{pool}: {str(e)}")
    zip_buffer.seek(0)
    date_str = run_date.replace('-', '') if isinstance(run_date, str) else run_date.strftime('%Y%m%d')
    zip_filename = f"SelectedReports_{date_str}.zip"
    error_msg = None
    if errors:
        error_msg = f"{len(errors)} error(s):\n" + "\n".join(errors)
    return zip_buffer.getvalue(), zip_filename, generated, total_groups, error_msg
# ============================================================
# STREAMLIT UI
# ============================================================
unique_run_key = row.get("UNIQUE_RUN_KEY", row.get("Unique Run Key"))
grower = row.get("GROWER")
run_date = row.get("RUN_DATE")
variety = row.get("VARIETY_USER_CD")
pool = row.get("POOL")
# ============================================================
# BULK DAILY PDF GENERATION — AT THE TOP
# ============================================================
st.markdown("### 📋 Bulk Generate — Selected PDFs for Day")
st.caption(
    f"Generate PDFs for selected grower/variety/pool groups on **{run_date}**. "
    "Runs with matching grower, variety, and pool across shifts are automatically combined. "
    "Downloads as a single .zip file."
)
# Load groups once and store in session state
if 'all_runs' not in st.session_state:
    with st.spinner("Loading groups for the day..."):
        st.session_state.all_runs = load_all_runs_for_date(run_date)
preview_runs = st.session_state.all_runs
if preview_runs.empty:
    st.warning("No runs found for this date.")
    grouped = pd.DataFrame()
else:
    grouped = preview_runs.groupby(['GROWER', 'VARIETY_USER_CD', 'POOL']).agg(
        Runs=('RUN_NUMBER', 'count'),
        Shifts=('SHIFT', lambda x: '+'.join(sorted(x.unique().astype(str)))),
        Bins=('BINS_SUBMITTED', 'sum'),
        Net_Lbs=('ACTUAL_NET', 'sum')
    ).reset_index()
    grouped.columns = ['Grower', 'Variety', 'Pool', 'Runs', 'Shifts', 'Bins', 'Net (lbs)']
    grouped['Bins'] = grouped['Bins'].astype(int)
    grouped['Net (lbs)'] = grouped['Net (lbs)'].astype(int).apply(lambda x: f"{x:,}")

if not grouped.empty:
    st.markdown("### Select Groups to Generate PDFs For")
    selected_groups = []
    for idx, g_row in grouped.iterrows():
        label = f"{g_row['Grower']} - {g_row['Variety']} - {g_row['Pool']} (Runs: {g_row['Runs']}, Shifts: {g_row['Shifts']}, Bins: {g_row['Bins']}, Net: {g_row['Net (lbs)']})"
        if st.checkbox(label, key=f"group_{idx}"):
            selected_groups.append((g_row['Grower'], g_row['Variety'], g_row['Pool']))
    st.caption(f"**{len(selected_groups)} groups selected** out of {len(grouped)}.")

bulk_include_sizer = st.checkbox("Include Sizer Profile in bulk PDFs", value=True, key="bulk_inc_sizer")
if st.button("🚀 Generate Selected PDFs", type="primary", key="bulk_gen"):
    if not selected_groups:
        st.warning("Please select at least one group.")
    else:
        progress_bar = st.progress(0)
        status_text = st.empty()
        def update_progress(current, total, label):
            progress_bar.progress(current / total)
            status_text.text(f"Generating {current}/{total}: {label}")
        with st.spinner("Generating selected PDFs..."):
            try:
                result = generate_selected_pdfs_zip(run_date, bulk_include_sizer, selected_groups, progress_callback=update_progress)
                zip_bytes, zip_filename, generated, total, error_msg = result
                progress_bar.progress(1.0)
                status_text.empty()
                if zip_bytes is None:
                    st.error(zip_filename)
                else:
                    st.success(f"Generated **{generated}/{total}** PDFs successfully.")
                    if error_msg:
                        st.warning(error_msg)
                    st.download_button(
                        label=f"📥 Download {zip_filename}",
                        data=zip_bytes,
                        file_name=zip_filename,
                        mime="application/zip",
                        type="primary"
                    )
            except Exception as e:
                st.error(f"Error generating bulk PDFs: {str(e)}")
                import traceback
                st.code(traceback.format_exc())
# ============================================================
# SINGLE RUN / COMBINE RUNS — BELOW
# ============================================================
st.markdown("---")
st.markdown("### Single Run PDF")
# Find related runs for same grower/date/variety/pool
related_runs = load_related_runs(grower, run_date, variety, pool)
st.markdown("##### Combine Runs (Same Grower/Date/Variety/Pool)")
if len(related_runs) > 1:
    st.caption("Select runs to combine into a single PDF report:")
    selected_keys = []
    for _, r in related_runs.iterrows():
        is_current = r['UNIQUE_RUN_KEY'] == unique_run_key
        default_checked = is_current
        label = f"Run {r['RUN_NUMBER']} - Shift {r['SHIFT']} - {r['VARIETY_USER_CD']} - {int(r['BINS_SUBMITTED'])} Bins - {int(r['ACTUAL_NET']):,} lbs"
        if is_current:
            label += " (current)"
        checked = st.checkbox(label, value=default_checked, key=f"run_{r['UNIQUE_RUN_KEY']}")
        if checked:
            selected_keys.append(r['UNIQUE_RUN_KEY'])
    if not selected_keys:
        st.warning("Please select at least one run.")
        selected_keys = [unique_run_key]
else:
    st.caption("No other runs found for this grower/date/variety/pool to combine.")
    selected_keys = [unique_run_key]
st.markdown("---")
st.markdown("### PDF Options")
include_sizer_profile = st.checkbox("Include Sizer Profile", value=True, key="inc_sizer_profile")
sections = {'run_info': True, 'bins_weight': True, 'packed_weights': True, 'processor': True, 'packout': True, 'sizer_profile': include_sizer_profile, 'quality_control': True, 'cull_analysis': True, 'pressure_analysis': True, 'notes': True}
st.markdown("---")
st.markdown("### Data Preview")
# Show combined totals if multiple runs selected
if len(selected_keys) > 1:
    st.info(f"**Combined Preview: {len(selected_keys)} runs selected**")
    combined_bins = related_runs[related_runs['UNIQUE_RUN_KEY'].isin(selected_keys)]['BINS_SUBMITTED'].sum()
    combined_net = related_runs[related_runs['UNIQUE_RUN_KEY'].isin(selected_keys)]['ACTUAL_NET'].sum()
    st.write(f"- Combined Bins: {int(combined_bins)}")
    st.write(f"- Combined Net Weight: {int(combined_net):,} lbs")
else:
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Basic Info**")
        st.write(f"- Bins Submitted: {row.get('BINS_SUBMITTED', 0)}")
        st.write(f"- Actual Gross: {row.get('ACTUAL_GROSS', 0):,} lbs")
        st.write(f"- Actual Net: {row.get('ACTUAL_NET', 0):,} lbs")
    with col2:
        st.markdown("**Breakdowns**")
        st.write(f"- Tare: {row.get('TARE_BREAKDOWN', 'N/A')}")
        st.write(f"- Block: {row.get('BLOCK_BREAKDOWN', 'N/A')}")
st.markdown("---")
if st.button("Generate PDF Report", type="primary"):
    with st.spinner("Generating PDF..."):
        try:
            pdf_bytes, pdf_filename = generate_pdf(selected_keys, sections)
            if pdf_bytes is None:
                st.error(pdf_filename)
            else:
                st.success(f"PDF generated successfully: {pdf_filename}")
                st.download_button(label="Download PDF", data=pdf_bytes, file_name=pdf_filename, mime="application/pdf", type="primary")
        except Exception as e:
            st.error(f"Error generating PDF: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
st.markdown("---")
col1, col2 = st.columns([1, 1])
with col1:
    if st.button("Pressure"):
        st.switch_page("pages/5_Pressure.py")
with col2:
    if st.button("Back to Main"):
        st.switch_page("streamlit_app.py")