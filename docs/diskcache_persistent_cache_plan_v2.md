---
name: diskcache persistent report cache
overview: Add persistent disk-based caching with diskcache so the app warms up in under 10 seconds on restart instead of rebuilding all reports from Snowflake. Scope is limited to reports using cache_manager (TV and PIDK); PFR and Inventory use separate caches.
version: 2
date: 2026-02-26
---

# Persistent Report Cache with diskcache — Plan v2

**v2 changes**: Added missing imports (`timezone`, `logging`), exact `get_interval_for_key` implementation (adapted for DataFrame config), improved `cached_at` format, and `REPORT_CACHE_DIR` env-var override for production.

**Goal**: Zero-downtime cold starts on deploy/restart even with 30+ reports. App warms up in &lt;10 seconds by loading fresh cache entries from disk.

---

## 1. Codebase Summary and Adaptations

### Current Architecture

- `services/cache_manager.py`: Central cache for reports using `register_report`. Stores payloads in `_caches[slug][cache_key]`. No disk persistence.
- **Reports using cache_manager**: TV (`tv_data.py`), PIDK (`pidk_data.py`) only. PFR and Inventory use separate caches and are out of scope.
- **Payload shapes**:
  - TV: 8-tuple `(cards, ppmh_fig, bph_fig, run_content, header, last_updated, cached_at, duration)` — contains Plotly figures and Dash components.
  - PIDK: dict with `run_table`, `shift_table`, `bph_figure`, `_cached_at`, etc. — contains Plotly figures, Dash components, DataFrames.

### Plan vs Code Adaptations

| Plan assumption | Actual code | Adaptation |
|-----------------|-------------|------------|
| `get_interval_for_key(slug, cache_key)` | Does not exist | Add helper that maps cache_key → period → refresh_seconds from config |
| `load_config()` then `load_persistent_cache()` before threads | `register_report` has no single pre-thread hook | Call `load_persistent_cache()` once per `register_report`, guarded by `_persistent_loaded` flag |
| JSON serialization for payloads | Dash components (dbc.Col, html.Div) not JSON-serializable | Use diskcache's pickle backend; wrap `{payload, cached_at, version}` and store as-is |
| `CURRENT_PAYLOAD_VERSION` invalidation | N/A | Add `CURRENT_PAYLOAD_VERSION = "1.0"`; skip disk entries with mismatched version on load |
| `/tmp/myapp_report_cache` | Windows may not have `/tmp` | Use `Path(tempfile.gettempdir()) / "myapp_report_cache"` |

### Serialization

No custom JSON serialization. diskcache uses pickle by default; store the entry dict `{payload, cached_at, version}` directly. Pickle handles Plotly figures, DataFrames, Dash components, and tuples.

---

## 2. Implementation Order

### Step 1: Dependencies

Add to `requirements.txt`:

```
diskcache>=5.6.3
```

### Step 2: cache_manager.py Changes

**2a. Imports** — add these; `os` already present. Update existing datetime import to include `timezone`:

```python
import logging
import tempfile
from pathlib import Path
from diskcache import Cache
```

Change the existing:

```python
from datetime import datetime, timedelta, date
```

to:

```python
from datetime import datetime, timedelta, date, timezone
```

**2b. Constants and persistent cache setup** (after `_config = None`):

```python
# Env override for production: set REPORT_CACHE_DIR=/app/cache and mount a volume
CACHE_DIR = os.getenv("REPORT_CACHE_DIR", str(Path(tempfile.gettempdir()) / "myapp_report_cache"))
PERSISTENT_CACHE_DIR = Path(CACHE_DIR)
PERSISTENT_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_persistent = Cache(
    directory=str(PERSISTENT_CACHE_DIR),
    size_limit=2_000_000_000,  # 2 GB
    cull_limit=0,
    statistics=True,
)
CURRENT_PAYLOAD_VERSION = "1.0"
_persistent_loaded = False
```

**2c. Add `get_interval_for_key(slug, cache_key)`** (config is a DataFrame):

```python
def get_interval_for_key(slug: str, cache_key: str) -> int:
    """Map cache_key → period → refresh_seconds from loaded config."""
    cfg = load_config()
    if cfg is None or (hasattr(cfg, "empty") and cfg.empty):
        return 300  # safe fallback

    key_lower = str(cache_key).lower().strip()
    if key_lower in ("today", "todays"):
        period = "today"
    elif key_lower in ("yesterday", "yest"):
        period = "yesterday"
    else:
        period = "historical"  # any date string

    try:
        row = cfg[(cfg["report_slug"] == slug) & (cfg["period"] == period)]
        if row.empty:
            return 300
        return int(row.iloc[0]["refresh_seconds"])
    except (KeyError, TypeError, IndexError):
        return 300
```

**2d. Add `_is_fresh(cached_at_str, refresh_seconds, tolerance_factor=1.5)`**:

```python
def _is_fresh(cached_at_str: str | None, refresh_seconds: int, tolerance_factor: float = 1.5) -> bool:
    """Return True if data is still acceptable (up to 1.5× normal interval)."""
    if not cached_at_str:
        return False
    try:
        s = cached_at_str.replace("Z", "+00:00")
        cached_at = datetime.fromisoformat(s)
        age_seconds = (datetime.now(timezone.utc) - cached_at).total_seconds()
        return age_seconds < (refresh_seconds * tolerance_factor)
    except Exception:
        return False
```

**2e. Add `load_persistent_cache()`**

- Set `_persistent_loaded = True` at start (guard against multiple runs).
- For each slug in `_builders`, iterate `_persistent.iterkeys()` with prefix `{slug}:`.
- Load config and get interval via `get_interval_for_key`.
- If entry version matches `CURRENT_PAYLOAD_VERSION` and `_is_fresh(...)`, deserialize (pickle returns original payload) and insert into `_caches[slug][cache_key]`.

**2f. Modify `_refresh`**

After successful build and `_caches.setdefault(slug, {})[ckey] = payload`, add:

```python
try:
    entry = {
        "payload": payload,
        "cached_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "version": CURRENT_PAYLOAD_VERSION,
    }
    _persistent.set(f"{slug}:{ckey}", entry)
except Exception as e:
    logging.getLogger(__name__).warning("Failed to persist cache to disk: %s", e)
```

**2g. Modify `register_report`**

- After `load_config()` and before starting threads/prewarm:
  - If not `_persistent_loaded`, call `load_persistent_cache()`.

**2h. Add `clear_all_caches()`**

- Clear `_caches` and call `_persistent.clear()`.

**2i. Optional: Extend `get_cache_status`**

- Include `_persistent.stats()` in the returned dict if useful for the /caching page.

### Step 3: Builder Files (tv_data.py, pidk_data.py)

- No changes required. Payloads remain as-is; pickle handles them.

### Step 4: /caching Page

- Optionally display disk stats from `get_cache_status()` if `disk_stats` is added.

---

## 3. Data Flow Diagram

```
Startup:
  register_report → load_config → _persistent_loaded?
    → No:  load_persistent_cache (scan disk, load fresh entries into _caches)
    → Yes: skip
  → start refresh threads / prewarm

Runtime:
  get_cached_data → in _caches?
    → Yes: return payload
    → No:  _refresh → builder builds payload → store in _caches → persist to disk → return
```

---

## 4. Testing Checklist

1. Fresh start: no disk entries → `load_persistent_cache` loads 0 items.
2. Visit TV Today → miss → refresh → verify directory has new file.
3. Restart app → TV Today loads from disk with no Snowflake query.
4. Wait past interval → background refresh updates both memory and disk.
5. Change `CURRENT_PAYLOAD_VERSION` → restart → old entries skipped.
6. Add new historical date → first visit builds it; restart keeps it.
7. Scale: 30 dummy entries → startup still under ~10s.

---

## 5. Rollback

- Comment out `load_persistent_cache()` call and the persistence block in `_refresh`.
- Delete `{tempdir}/myapp_report_cache` (or `REPORT_CACHE_DIR` if set).

---

## 6. Future Notes

- Production: set `REPORT_CACHE_DIR=/app/cache` (or similar) and mount a persistent volume.
- Redis migration: swap `_persistent` for a Redis-backed store; payload serialization can be revisited for JSON/msgpack if needed.

---

## 7. v2 Feedback Summary (Apply Before Coding)

| # | Change | Where |
|---|--------|-------|
| 1 | Add `logging`, `timezone` imports | Step 2a |
| 2 | Use exact `get_interval_for_key` (DataFrame-aware) | Step 2c |
| 3 | Use `cached_at` line with `.replace("+00:00", "Z")` | Step 2f |
| 4 | Add `REPORT_CACHE_DIR` env-var override for cache dir | Step 2b |
