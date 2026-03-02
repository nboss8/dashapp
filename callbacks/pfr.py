"""
Production Finalized Report - all callbacks.
Imported by app.py for registration.
"""
import base64
import time
from datetime import datetime

import pandas as pd
from dash import callback, Input, Output, State, dcc, no_update

from services.pfr_data import (
    load_groups_for_date,
    generate_pdf_bytes,
    generate_all_pdfs_zip,
    build_report_content,
    pfr_groups_cache,
    pfr_report_cache,
    pfr_cache_lock,
    _groups_df_to_opts_and_first,
    _evict_pfr_report_cache_if_needed,
)


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
    Input("pfr-interval", "n_intervals"),
    Input("pfr-date", "date"),
)
def update_group_options(_n_intervals, run_date):
    if not run_date:
        return [], None
    with pfr_cache_lock:
        cached = pfr_groups_cache.get(run_date)
    if cached is not None:
        return cached["opts"], cached["first_val"]
    _t0 = time.perf_counter()
    df = load_groups_for_date(run_date)
    if df.empty:
        return [], None
    opts, first_val = _groups_df_to_opts_and_first(df)
    duration = round(time.perf_counter() - _t0, 2)
    with pfr_cache_lock:
        pfr_groups_cache[run_date] = {
            "opts": opts, "first_val": first_val,
            "_cached_at": datetime.now().isoformat(),
            "_cached_duration_seconds": duration,
        }
    return opts, first_val


@callback(
    Output("pfr-report-content", "children"),
    Input("pfr-interval", "n_intervals"),
    Input("pfr-date", "date"),
    Input("pfr-group-dropdown", "value"),
)
def render_report(_n_intervals, run_date, group_value):
    if not run_date or not group_value:
        return build_report_content(run_date, group_value)
    cache_key = (run_date, group_value)
    with pfr_cache_lock:
        cached = pfr_report_cache.get(cache_key)
    if cached is not None:
        return cached["content"]
    _t0 = time.perf_counter()
    content = build_report_content(run_date, group_value)
    duration = round(time.perf_counter() - _t0, 2)
    with pfr_cache_lock:
        _evict_pfr_report_cache_if_needed()
        pfr_report_cache[cache_key] = {
            "content": content,
            "_cached_at": datetime.now().isoformat(),
            "_cached_duration_seconds": duration,
        }
    return content
