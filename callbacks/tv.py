"""
TV Display - all callbacks.
Imported by app.py for registration. Data logic in services/tv_data.py.
"""
from dash import callback, Input, Output, ctx, no_update

from services.cache_manager import get_cached_data
from services.tv_data import build_runs_section


@callback(
    Output("tv-date-store", "data"),
    Input("tv-date-dropdown", "value"),
)
def update_date_store(selected_value):
    return selected_value


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
    period = "today" if selected_date is None else "yesterday"
    if ctx.triggered_id == "tv-interval" and period != "today":
        return (no_update,) * 6
    cached = get_cached_data("tv", period)
    cards, ppmh_fig, bph_fig, run_content, header, last_updated = cached[0], cached[1], cached[2], cached[3], cached[4], cached[5]
    return header, last_updated, cards, ppmh_fig, bph_fig, build_runs_section(run_content)
