# 🚀 Golden Template: New Report Pages (PIDK Pattern)

Exemplifies cache-first Dash page with filtering, visuals, persistence. **Copy-paste for every new page.**

## Why This Pattern?
- **Performance**: &lt;100ms loads (cache hits), persistent across restarts.
- **Scalable**: 30+ pages, &lt;10s cold starts.
- **Polish**: Loaders, expand/export, slicers, dark theme.
- **Auto-cache**: `register_report()` handles threads/persistence.

## 1. App-Level (app.py - Already Set)
```python
from callbacks.new_page import *  # Add this line
```
- `use_pages=True`, `load_persistent_cache()` post-imports.

## 2. Layout (pages/new_page.py)
```python
import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from components.page_header import page_header
from services.new_data import get_day_label_options

dash.register_page(__name__, path=&quot;/section/new-page&quot;, name=&quot;New Page&quot;)

# Header dropdowns + last_updated
_header_right = dcc.Loading([dcc.Dropdown(id=&quot;new-day-dropdown&quot;, options=get_day_label_options(), value=&quot;TODAY&quot;), ...])

layout = html.Div([
    dcc.Interval(id=&quot;new-interval&quot;, interval=300_000),
    dcc.Store(id=&quot;new-day-store&quot;),
    dcc.Store(id=&quot;new-filter-store&quot;),
    dcc.Store(id=&quot;new-raw-data&quot;),
    
    dbc.Container([
        page_header(&quot;New Page&quot;, &quot;/&quot;, right_slot=_header_right),
        dbc.Row([... dbc.Col(dbc.Card(tile_content)) ...])  # Tiles: table/chart/matrix
    ])
])
```
**Patterns**: Loaders (`delay_show=180`), slicers (`value=&quot;ALL&quot;`), expand btns (`id={&quot;type&quot;: &quot;new-expand&quot;, &quot;index&quot;: &quot;tile1&quot;}`).

## 3. Callbacks (callbacks/new_page.py)
**Main**:
```python
@callback(Output(&quot;new-table&quot;, &quot;children&quot;), Output(&quot;new-chart&quot;, &quot;figure&quot;), ..., Input(&quot;new-interval&quot;, &quot;n_intervals&quot;), Input(&quot;new-day-store&quot;, &quot;data&quot;), ...)
def update_all(n_int, day, filter):
    cached = get_cached_data(&quot;new&quot;, day)
    filtered = _filter_data(cached[&quot;raw_data&quot;], filter)
    return build_table(filtered), cached[&quot;fig&quot;], slicer_opts, ...
```
**Helpers**: Slicer→filter store, expand toggle, CSV export (`dcc.send_data_frame`).

**Fallback**: Tuple of no-data/error placeholders.

## 4. Data (services/new_data.py)
```python
# Queries: query(&quot;SELECT ... FROM {DBT_SCHEMA}.new_mart WHERE DAY_LABEL=%s&quot;, [day])

def build_payload(day: str) -&gt; dict:
    df = query_run_totals(day)
    return {
        &quot;table&quot;: build_table(df),
        &quot;fig&quot;: make_fig(df),
        &quot;raw_data&quot;: df.to_dict(&quot;records&quot;),
        &quot;_cached_at&quot;: datetime.now(),
    }

def get_day_label_options(): ...

register_report(build_payload, get_day_label_options)  # Auto-cache!
```
**dbt**: Add marts + `report_cache_config.yml` (slug: `new`, periods: `today|historical`, `refresh_seconds: 300`).

## 5. Cache (services/cache_manager.py - GLOBAL)
- `get_cached_data(&quot;new&quot;, day)`: Hit → instant; Miss → build → persist (diskcache).
- Threads: Auto-start (today every 300s, historical per-key).
- Startup: `load_persistent_cache()` restores fresh data.
- Prod: `REPORT_CACHE_DIR=/app/cache` + volume.

## Replication (5min)
1. dbt marts + config.
2. Copy PIDK files → new_page (rename ids).
3. `app.py`: Import callbacks.
4. Restart → Visit `/new-page`.

## Gotchas
- Outputs must match returns (use fallbacks).
- Filter stores clear on day change.
- Console: Watch `[Cache]` / `[refresh]` logs.

**Fork this for your team.** 🚀
