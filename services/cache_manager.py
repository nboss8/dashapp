"""
Centralized caching foundation for all reports.
- dbt-configurable refresh intervals per report_slug / period
- Auto-detects slug from *_data.py filename (tv_data.py → 'tv')
- One daemon thread per (slug, period)
- Zero breaking changes to TV
"""

import logging
import os
import tempfile
import threading
import time
from datetime import datetime, timedelta, date, timezone
from pathlib import Path
import inspect
import pandas as pd
from diskcache import Cache
from services.snowflake_service import query

logger = logging.getLogger(__name__)

# Globals - styled exactly like tv_data.py
_builders = {}      # slug → build_func
_caches = {}        # slug → {period: payload}
_locks = {}         # slug → Lock
_threads = {}       # (slug, period) → Thread
_config = None

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

DBT_SCHEMA = f"{os.getenv('SNOWFLAKE_DATABASE', 'FROSTY')}.{os.getenv('DBT_SCHEMA', 'DBT_DEV')}"

def load_config():
    """Load once from Snowflake"""
    global _config
    if _config is None:
        _config = query(f"""
            SELECT "report_slug", "period", "refresh_seconds"
            FROM {DBT_SCHEMA}.report_cache_config
        """)
    return _config


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


def load_persistent_cache():
    """Warm in-memory cache from disk on startup — only fresh entries."""
    global _persistent_loaded
    if _persistent_loaded:
        return
    _persistent_loaded = True

    logger.info("[Cache] Warming in-memory cache from persistent disk...")

    # Reuse global _config to avoid second Snowflake query on startup
    config_df = _config if _config is not None else load_config()
    loaded_count = 0

    try:
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
                    _caches[slug][cache_key] = entry["payload"]
                    loaded_count += 1
    except Exception as e:
        logger.warning("[Cache] Error loading from persistent cache (continuing with empty cache): %s", e)

    logger.info("[Cache] Persistent warm-up complete — %s entries loaded", loaded_count)


def register_report(build_func, get_options_func=None):
    """Call this at bottom of every *_data.py. Auto-detects slug."""
    # Auto-detect slug: tv_data.py → 'tv'
    frame = inspect.stack()[1]
    caller_file = os.path.basename(frame.filename)
    slug = os.path.splitext(caller_file)[0].replace('_data', '').lower()

    _builders[slug] = build_func
    print(f"[OK] Registered report: {slug}")

    # Load config and start threads for this report
    cfg = load_config()
    slug_cfg = cfg[cfg['report_slug'] == slug]
    if slug_cfg.empty:
        print(f"[WARN] No cache config found for '{slug}' - using defaults")
        return

    if not _persistent_loaded:
        load_persistent_cache()

    lock = _locks.setdefault(slug, threading.RLock())
    if get_options_func:
        print(f"Prewarming historical for {slug} (background)")
        options = get_options_func()
        cutoff = (date.today() - timedelta(days=7)).isoformat()
        vals = [opt.get('value') for opt in options if opt.get('value') and str(opt.get('value')).upper() != 'TODAY' and opt.get('value') >= cutoff]
        if vals:
            def _prewarm_worker(slug=slug, lock=lock, vals=vals):
                for val in vals:
                    try:
                        _refresh(slug, 'historical', lock, val)
                        print(f"Prewarmed {slug}/{val}")
                    except Exception as e:
                        print(f"Prewarm skip {slug}/{val}: {e}")
            t = threading.Thread(target=_prewarm_worker, daemon=True)
            t.start()
            print(f"Background prewarm started ({len(vals)} dates)")

    for _, row in slug_cfg.iterrows():
        period = row['period']
        interval = int(row['refresh_seconds'])

        if period == 'historical':
            def h_worker(slug=slug, interval=interval, lock=lock):
                while True:
                    time.sleep(interval)
                    with lock:
                        for k in list(_caches.get(slug, {})):
                            if k != 'today':
                                _refresh(slug, 'historical', lock, k)

            t = threading.Thread(target=h_worker, daemon=True)
            _threads[(slug, period)] = t
            t.start()
            print(f"   [{period}] (cache keys) thread started ({interval}s interval)")
        else:
            def worker(slug=slug, period=period, interval=interval, lock=lock):
                while True:
                    _refresh(slug, period, lock)
                    time.sleep(interval)

            t = threading.Thread(target=worker, daemon=True)
            _threads[(slug, period)] = t
            t.start()
            print(f"   [{period}] thread started ({interval}s interval)")

def _refresh(slug, period, lock, cache_key=None):
    """Build and cache payload"""
    try:
        builder = _builders[slug]
        if period == 'today':
            effective_date = None
        elif period == 'yesterday':
            effective_date = (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')
        else:  # historical
            effective_date = cache_key
        payload = builder(effective_date)

        cache_key = 'today' if period == 'today' else period if period == 'yesterday' else cache_key
        with lock:
            _caches.setdefault(slug, {})[cache_key] = payload

        try:
            entry = {
                "payload": payload,
                "cached_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "version": CURRENT_PAYLOAD_VERSION,
            }
            _persistent.set(f"{slug}:{cache_key}", entry)
        except Exception as e:
            logger.warning("Failed to persist cache to disk: %s", e)

        print(f"[refresh] {slug}/{cache_key} @ {datetime.now():%H:%M:%S}")
    except Exception as e:
        print(f"[ERROR] Cache refresh error {slug}/{period}: {e}")

def get_cache_status():
    """Read-only status for Caching page. Returns {slug: {cache_key: cached_at_str}}."""
    result = {}
    for slug in _builders:
        lock = _locks.get(slug)
        if lock is None:
            continue
        with lock:
            entries = _caches.get(slug, {})
            sub = {}
            for k, payload in entries.items():
                if isinstance(payload, dict) and "_cached_at" in payload:
                    sub[k] = str(payload.get("_cached_at", "-"))
                elif hasattr(payload, "__getitem__") and len(payload) >= 7:
                    sub[k] = str(payload[6])
                else:
                    sub[k] = "-"
            if sub:
                result[slug] = dict(sub)
    return result


def clear_all_caches():
    """For /caching page or admin use."""
    _caches.clear()
    _persistent.clear()
    logger.info("[Cache] All caches cleared")


def get_cached_data(slug: str, identifier: str = 'today'):
    """Public API — used by all callbacks"""
    load_config()  # ensure loaded
    if slug not in _builders:
        raise ValueError(f"Report '{slug}' not registered. Did you call register_report()?")
    
    period = identifier
    if period.upper() == 'TODAY':
        period = 'today'
    
    cfg = load_config()
    slug_cfg = cfg[cfg['report_slug'] == slug]
    row = slug_cfg[slug_cfg['period'] == period]
    if row.empty:
        period = 'historical'
    
    lock = _locks.setdefault(slug, threading.RLock())
    with lock:
        if period == 'today':
            ckey = 'today'
        elif period == 'yesterday':
            ckey = 'yesterday'
        else:
            ckey = identifier
        if ckey not in _caches.get(slug, {}):
            _refresh(slug, period, lock, ckey if period == 'historical' else None)
        return _caches[slug][ckey]
