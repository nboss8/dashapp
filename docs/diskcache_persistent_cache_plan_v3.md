---
name: diskcache persistent report cache
overview: Add persistent disk-based caching with diskcache so the app warms up in under 10 seconds on restart instead of rebuilding all reports from Snowflake. Scope is limited to reports using cache_manager (TV and PIDK); PFR and Inventory use separate caches.
version: 3
date: 2026-02-26
---

# Persistent Report Cache with diskcache — Plan v3

**v3 changes (critical fix + polish)**:

1. **Fix repeated `load_config()` calls** — `get_interval_for_key` no longer calls `load_config()` internally. Config is loaded **once** in `load_persistent_cache()` and passed in. Avoids 10–100+ Snowflake queries on every startup.
2. **Full `load_persistent_cache()` implementation** — Complete function with exact logic (no high-level outline).
3. **Consistent naming + logging** — Use `cache_key` (not `ckey`), module-level logger, startup info logs.

**Goal**: Zero-downtime cold starts on deploy/restart even with 30+ reports. App warms up in &lt;10 seconds by loading fresh cache entries from disk.

---

## 1. Codebase Summary and Adaptations

### Current Architecture

- `services/cache_manager.py`: Central cache for reports using `register_report`. Stores payloads in `_caches[slug][cache_key]`. No disk persistence.
- **Reports using cache_manager**: TV (`tv_data.py`), PIDK (`pidk_data.py`) only. PFR and Inventory use separate caches and are out of scope.
- **Payload shapes**:
  - TV: 8-tuple `(cards, ppmh_fig, bph_fig, run_content, header, last_updated, cached_at, duration)` — contains Plotly figures and Dash components.
  - PIDK: dict with `run_table`, `shift_table`, `bph_figure`, `_cached_at`, etc. — contains Plotly figures, Dash components, DataFrames.

### Serialization

No custom JSON serialization. diskcache uses pickle by default; store the entry dict `{payload, cached_at, version}` directly. Pickle handles Plotly figures, DataFrames, Dash components, and tuples.

### Critical v3 Fix

**v2 bug**: `get_interval_for_key` called `load_config()` inside the loop. During `load_persistent_cache()`, this caused one Snowflake query per disk cache key (10–100+ on startup). **v3**: Load config once in `load_persistent_cache()` and pass the DataFrame into `get_interval_for_key(config_df, slug, cache_key)`.

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

Add module-level logger (after imports):

```python
logger = logging.getLogger(__name__)
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

**2c. Add `get_interval_for_key(config_df, slug, cache_key)`** — accepts pre-loaded config (no Snowflake calls):

```python
def get_interval_for_key(config_df, slug: str, cache_key: str) -> int:
    """Uses already-loaded config DataFrame (passed in to avoid Snowflake spam)."""
    if config_df is None or (hasattr(config_df, "empty") and config_df.empty):
        return 300

    key_lower = str(cache_key).lower().strip()
    if key_lower in ("today", "todays"):
        period = "today"
    elif key_lower in ("yesterday", "yest"):
        period = "yesterday"
    else:
        period = "historical"

    try:
        row = config_df[(config_df["report_slug"] == slug) & (config_df["period"] == period)]
        if row.empty:
            return 300
        return int(row.iloc[0]["refresh_seconds"])
    except (KeyError, TypeError, IndexError, ValueError):
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

**2e. Add full `load_persistent_cache()`** — config loaded once; store raw payload in `_caches`:

```python
def load_persistent_cache():
    """Warm in-memory cache from disk on startup — only fresh entries."""
    global _persistent_loaded
    if _persistent_loaded:
        return
    _persistent_loaded = True

    logger.info("[Cache] Warming in-memory cache from persistent disk...")

    config_df = load_config()  # ONCE only — no repeated Snowflake calls
    loaded_count = 0

    for slug in list(_builders.keys()):
        _caches.setdefault(slug, {})
        prefix = f"{slug}:"

        for full_key in list(_persistent.iterkeys()):
            if not str(full_key).startswith(prefix):
                continue
            _, cache_key = str(full_key).split(":", 1)

            entry = _persistent.get(full_key)
            if not entry or entry.get("version") != CURRENT_PAYLOAD_VERSION:
                continue

            interval = get_interval_for_key(config_df, slug, cache_key)
            if _is_fresh(entry.get("cached_at"), interval):
                # Store raw payload only — callbacks expect builder return value (tuple/dict)
                _caches[slug][cache_key] = entry["payload"]
                loaded_count += 1

    logger.info(f"[Cache] Persistent warm-up complete — {loaded_count} entries loaded")
```

**2f. Modify `_refresh`** — use `cache_key` (not `ckey`), add persistence block:

- Rename `ckey` → `cache_key` throughout the function for consistency.
- After `_caches.setdefault(slug, {})[cache_key] = payload`:

```python
try:
    entry = {
        "payload": payload,
        "cached_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "version": CURRENT_PAYLOAD_VERSION,
    }
    _persistent.set(f"{slug}:{cache_key}", entry)
except Exception as e:
    logger.warning("Failed to persist cache to disk: %s", e)
```

**2g. Modify `register_report`**

- After `load_config()` and before starting threads/prewarm:
  - If not `_persistent_loaded`, call `load_persistent_cache()`.

**2h. Add `clear_all_caches()`**

```python
def clear_all_caches():
    """For /caching page or admin use."""
    _caches.clear()
    _persistent.clear()
    logger.info("[Cache] All caches cleared")
```

**2i. Optional: Extend `get_cache_status`**

- Include `_persistent.stats()` in the returned dict if useful for the /caching page.

### Step 3: Builder Files (tv_data.py, pidk_data.py)

- No changes required.

### Step 4: /caching Page

- Optionally display disk stats from `get_cache_status()` if `disk_stats` is added.

---

## 3. Data Flow Diagram

```
Startup:
  register_report → load_config → _persistent_loaded?
    → No:  load_persistent_cache
            → load_config() ONCE
            → iterate disk keys
            → get_interval_for_key(config_df, ...)  ← no Snowflake
            → load fresh entries into _caches
    → Yes: skip
  → start refresh threads / prewarm

Runtime:
  get_cached_data → in _caches?
    → Yes: return payload
    → No:  _refresh → builder builds → store in _caches → persist to disk → return
```

---

## 4. Testing Checklist

1. Fresh start: no disk entries → `load_persistent_cache` loads 0 items.
2. Visit TV Today → miss → refresh → verify directory has new file.
3. Restart app → TV Today loads from disk with no Snowflake query.
4. Confirm startup: only ONE Snowflake query (for config) — not N queries for N disk keys.
5. Wait past interval → background refresh updates both memory and disk.
6. Change `CURRENT_PAYLOAD_VERSION` → restart → old entries skipped.
7. Add new historical date → first visit builds it; restart keeps it.
8. Scale: 30 dummy entries → startup still under ~10s.

---

## 5. Rollback

- Comment out `load_persistent_cache()` call and the persistence block in `_refresh`.
- Delete `{tempdir}/myapp_report_cache` (or `REPORT_CACHE_DIR` if set).

---

## 6. Future Notes

- Production: set `REPORT_CACHE_DIR=/app/cache` (or similar) and mount a persistent volume.
- Redis migration: swap `_persistent` for a Redis-backed store; payload serialization can be revisited for JSON/msgpack if needed.

---

## 7. v3 Changes Summary (vs v2)

| # | Change | Why |
|---|--------|-----|
| 1 | `get_interval_for_key(config_df, slug, cache_key)` — config passed in | Avoid 10–100+ Snowflake queries on startup |
| 2 | Full `load_persistent_cache()` implementation | Complete, copy-paste ready logic |
| 3 | Store `entry["payload"]` directly in `_caches` (not wrapper dict) | Callbacks expect raw builder return value |
| 4 | Use `cache_key` not `ckey` in `_refresh` | Consistent naming |
| 5 | Module-level `logger`, startup info logs | Easier debugging and observability |
