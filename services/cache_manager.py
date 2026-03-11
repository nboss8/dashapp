"""
Centralized caching foundation for all reports.
- dbt-configurable refresh intervals per report_slug / period
- Auto-detects slug from *_data.py filename (tv_data.py → 'tv')
- Single batch coordinator thread runs due refreshes together to reduce Snowflake wake-ups
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
_builders = {}       # slug → build_func
_caches = {}         # slug → {period: payload}
_locks = {}          # slug → Lock
_refresh_specs = {}  # slug → [(period, cache_key), ...] for manual refresh
_config = None
_refreshing_slugs = set()
_refreshing_keys = {}  # slug -> set of cache_key (which key is currently refreshing)
_refreshing_lock = threading.Lock()
_slug_hits = {}       # slug -> int (cache hits from get_cached_data)
_slug_refreshes = {}  # slug -> int (refresh invocations)
_slug_stats_lock = threading.Lock()
_coordinator_started = False

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

# Default cache refresh: 15 minutes when report_cache_config has no row or is unavailable
DEFAULT_CACHE_REFRESH_SECONDS = 900

# How often the coordinator wakes to check for due refreshes; batches Snowflake calls
BATCH_INTERVAL = int(os.getenv("REPORT_CACHE_BATCH_INTERVAL", "300"))

# Cached stats for Caching page (avoids disk I/O every 60s)
_stats_cache = None
_stats_cache_ts = 0.0
STATS_CACHE_TTL = 300  # 5 min

DBT_SCHEMA = f"{os.getenv('SNOWFLAKE_DATABASE', 'FROSTY')}.{os.getenv('DBT_SCHEMA', 'DBT_DEV')}"

def load_config():
    """Load once from Snowflake. Includes prewarm column if present in table."""
    global _config
    if _config is None:
        try:
            _config = query(f"""
                SELECT "report_slug", "period", "refresh_seconds", "prewarm"
                FROM {DBT_SCHEMA}.report_cache_config
            """)
        except Exception:
            _config = query(f"""
                SELECT "report_slug", "period", "refresh_seconds"
                FROM {DBT_SCHEMA}.report_cache_config
            """)
    return _config


def get_interval_for_key(config_df, slug: str, cache_key: str) -> int:
    """Uses already-loaded config DataFrame (passed in to avoid Snowflake spam).
    When no config row exists, returns DEFAULT_CACHE_REFRESH_SECONDS (15 min)."""
    if config_df is None or (hasattr(config_df, "empty") and config_df.empty):
        return DEFAULT_CACHE_REFRESH_SECONDS

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
            return DEFAULT_CACHE_REFRESH_SECONDS
        return int(row.iloc[0]["refresh_seconds"])
    except (KeyError, TypeError, IndexError, ValueError):
        return DEFAULT_CACHE_REFRESH_SECONDS


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


def register_report(build_func, get_options_func=None, prewarm_all_options=False, historical_refresh_keys=None):
    """Call this at bottom of every *_data.py. Auto-detects slug."""
    # Auto-detect slug: tv_data.py → 'tv'
    frame = inspect.stack()[1]
    caller_file = os.path.basename(frame.filename)
    slug = os.path.splitext(caller_file)[0].replace('_data', '').lower()

    _builders[slug] = build_func
    if os.environ.get("IS_MAIN_DASH_PROCESS") != "true":
        return

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
    # Prewarm from config: table prewarm column, with fallback to get_options_func when column missing
    cfg_prewarm = False
    if "prewarm" in slug_cfg.columns:
        cfg_prewarm = (slug_cfg["prewarm"] == 1).any() or (slug_cfg["prewarm"] is True).any()
    else:
        cfg_prewarm = get_options_func is not None  # fallback: prewarm if options func present
    if get_options_func and cfg_prewarm:
        print(f"Prewarming historical for {slug} (background)")
        options = get_options_func()
        cutoff = (date.today() - timedelta(days=7)).isoformat()
        if prewarm_all_options:
            opts_filtered = [opt for opt in options if opt.get('period') in ('today', 'yesterday') or (opt.get('value') and str(opt.get('value')).upper() != 'TODAY')]
        else:
            opts_filtered = [opt for opt in options if opt.get('period') in ('today', 'yesterday') or (opt.get('value') and str(opt.get('value')).upper() != 'TODAY' and opt.get('value') >= cutoff)]
        # Support optional "period" on option (e.g. tv: today/yesterday) so we store under correct key
        prewarm_tasks = []
        for opt in opts_filtered:
            val = opt.get('value')
            period_opt = opt.get('period')
            if period_opt in ('today', 'yesterday'):
                prewarm_tasks.append((period_opt, None))
            else:
                prewarm_tasks.append(('historical', val))
        if prewarm_tasks:
            def _prewarm_worker(slug=slug, lock=lock, tasks=prewarm_tasks):
                for period, ckey in tasks:
                    try:
                        _refresh(slug, period, lock, ckey)
                        print(f"Prewarmed {slug}/{period or ckey}")
                    except Exception as e:
                        print(f"Prewarm skip {slug}/{period or ckey}: {e}")
            t = threading.Thread(target=_prewarm_worker, daemon=True)
            t.start()
            print(f"Background prewarm started ({len(prewarm_tasks)} keys)")

    # Build refresh specs for manual refresh (only explicit historical keys, not inferred)
    specs = []
    for _, row in slug_cfg.iterrows():
        period = row['period']
        if period == 'today':
            specs.append(('today', None))
        elif period == 'yesterday':
            specs.append(('yesterday', None))
        elif historical_refresh_keys:
            for k in historical_refresh_keys:
                specs.append(('historical', k))
    if specs:
        _refresh_specs[slug] = specs

    # Coordinator thread handles periodic refresh (batched); started once by start_cache_coordinator()
    _start_coordinator_if_needed()

def _start_coordinator_if_needed():
    """Start the batch coordinator thread once, after first report registers."""
    global _coordinator_started
    if _coordinator_started:
        return
    _coordinator_started = True
    def _coordinator_worker():
        time.sleep(60)  # Let prewarm complete first
        logger.info("[Cache] Batch coordinator started (interval %ds)", BATCH_INTERVAL)
        # Align to wall-clock boundary (e.g. :00, :05, :10 for 5-min)
        now_secs = int(time.time())
        remainder = now_secs % BATCH_INTERVAL
        sleep_until_next = BATCH_INTERVAL - remainder if remainder else BATCH_INTERVAL
        if sleep_until_next > 0:
            time.sleep(sleep_until_next)
        while True:
            try:
                _run_due_refreshes()
            except Exception as e:
                logger.exception("[Cache] Coordinator error: %s", e)
            time.sleep(BATCH_INTERVAL)

    t = threading.Thread(target=_coordinator_worker, daemon=True)
    t.start()
    print(f"   [coordinator] batch refresh every {BATCH_INTERVAL}s")

def _run_due_refreshes():
    """Check all refresh specs and run those that are due."""
    config_df = load_config()
    if config_df is None or (hasattr(config_df, "empty") and config_df.empty):
        return
    due = []
    now = datetime.now(timezone.utc)
    for slug, specs in list(_refresh_specs.items()):
        lock = _locks.get(slug)
        if not lock:
            continue
        for period, cache_key in specs:
            if period == "historical":
                ckey = cache_key
            elif period == "today":
                ckey = "today"
            else:
                ckey = "yesterday"
            interval = get_interval_for_key(config_df, slug, ckey)
            entry = _persistent.get(f"{slug}:{ckey}")
            if entry and entry.get("cached_at"):
                try:
                    s = entry["cached_at"].replace("Z", "+00:00")
                    cached_at = datetime.fromisoformat(s)
                    if hasattr(cached_at, "tzinfo") and cached_at.tzinfo is None:
                        cached_at = cached_at.replace(tzinfo=timezone.utc)
                    age = (now - cached_at).total_seconds()
                    if age < interval:
                        continue
                except Exception:
                    pass
            due.append((slug, period, lock, cache_key))
    for slug, period, lock, ckey in due:
        try:
            _refresh(slug, period, lock, ckey)
        except Exception as e:
            logger.exception("[Cache] Refresh %s/%s failed: %s", slug, ckey, e)

def _refresh(slug, period, lock, cache_key=None):
    """Build and cache payload"""
    ckey = 'today' if period == 'today' else ('yesterday' if period == 'yesterday' else cache_key)
    with _refreshing_lock:
        _refreshing_slugs.add(slug)
        _refreshing_keys.setdefault(slug, set()).add(ckey)
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

        with _slug_stats_lock:
            _slug_refreshes[slug] = _slug_refreshes.get(slug, 0) + 1
        print(f"[refresh] {slug}/{cache_key} @ {datetime.now():%H:%M:%S}")
    except Exception as e:
        logger.exception("Cache refresh error %s/%s: %s", slug, period, e)
        print(f"[ERROR] Cache refresh error {slug}/{period}: {e}")
        raise
    finally:
        with _refreshing_lock:
            _refreshing_slugs.discard(slug)
            keys_set = _refreshing_keys.get(slug)
            if keys_set is not None:
                keys_set.discard(ckey)
                if not keys_set:
                    _refreshing_keys.pop(slug, None)

def get_refreshing_slugs() -> set:
    """Return set of slugs currently being refreshed. For Caching page."""
    with _refreshing_lock:
        return set(_refreshing_slugs)


def get_refreshing_keys() -> dict:
    """Return {slug: set(cache_key)} for keys currently refreshing. For Caching page."""
    with _refreshing_lock:
        return {slug: set(keys) for slug, keys in _refreshing_keys.items()}


def _do_manual_refresh_sync(slug: str, lock, specs) -> tuple[bool, str]:
    """Internal: run manual refresh synchronously."""
    try:
        for period, cache_key in specs:
            if period == 'historical':
                _refresh(slug, 'historical', lock, cache_key)
            else:
                _refresh(slug, period, lock, None)
        return True, "Refreshed"
    except Exception as e:
        logger.exception("Manual refresh failed for %s: %s", slug, e)
        return False, str(e)

def trigger_manual_refresh(slug: str, block: bool = False) -> tuple[bool, str]:
    """
    Manually refresh all cache keys for a slug. By default runs in background; use block=True for prewarm.
    Returns (success, message). Caching page shows "Refreshing" via get_refreshing_slugs().
    """
    if slug not in _builders:
        return False, f"Unknown slug: {slug}"
    lock = _locks.get(slug)
    if not lock:
        return False, f"No lock for {slug}"
    specs = _refresh_specs.get(slug)
    if not specs:
        return False, f"No refresh spec for {slug}"
    if slug in get_refreshing_slugs() and not block:
        return True, "Already refreshing"

    if block:
        return _do_manual_refresh_sync(slug, lock, specs)

    def _do_refresh():
        _do_manual_refresh_sync(slug, lock, specs)

    threading.Thread(target=_do_refresh, daemon=True).start()
    return True, "Refresh started"


def get_registered_slugs_with_refresh():
    """Slugs that support manual refresh (have refresh specs)."""
    return list(_refresh_specs.keys())


def get_prewarm_by_slug():
    """Return {slug: bool} from config for Caching page. Prewarm=True when config has prewarm=1 for that slug."""
    try:
        cfg = load_config()
        if cfg is None or (hasattr(cfg, "empty") and cfg.empty):
            return {}
        if "prewarm" not in cfg.columns:
            return {slug: False for slug in cfg["report_slug"].unique()}
        out = {}
        for slug in cfg["report_slug"].unique():
            rows = cfg[cfg["report_slug"] == slug]
            out[slug] = (rows["prewarm"] == 1).any() or (rows["prewarm"] == True).any()
        return out
    except Exception as e:
        logger.warning("get_prewarm_by_slug failed (Snowflake/config): %s", e)
        return {}


def _bytes_to_human(n):
    """Convert bytes to human-readable string (B, KB, MB, GB)."""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} TB"


def get_cache_stats():
    """Return disk cache stats for the Caching page. Cached 5 min to avoid disk I/O every tick."""
    global _stats_cache, _stats_cache_ts
    now = time.time()
    if _stats_cache is not None and (now - _stats_cache_ts) < STATS_CACHE_TTL:
        return _stats_cache
    try:
        disk_bytes = _persistent.volume()
        entry_count = len(_persistent)
        hits, misses = _persistent.stats()
        directory = str(_persistent.directory)
        size_limit = getattr(_persistent, "size_limit", 2_000_000_000)
        _stats_cache = {
            "disk_size_bytes": disk_bytes,
            "disk_size_human": _bytes_to_human(disk_bytes),
            "entry_count": entry_count,
            "hits": hits,
            "misses": misses,
            "cache_directory": directory,
            "size_limit_human": _bytes_to_human(size_limit),
        }
        _stats_cache_ts = now
        return _stats_cache
    except Exception as e:
        logger.warning("get_cache_stats failed: %s", e)
        fallback = {
            "disk_size_bytes": 0,
            "disk_size_human": "?",
            "entry_count": 0,
            "hits": 0,
            "misses": 0,
            "cache_directory": str(PERSISTENT_CACHE_DIR),
            "size_limit_human": "2 GB",
        }
        _stats_cache = fallback
        _stats_cache_ts = now
        return fallback


def get_cache_status():
    """Read-only status for Caching page. Returns {slug: {cache_key: cached_at_str}}.
    Includes slugs with refresh_specs even if cache is empty (shows 'No keys')."""
    result = {}
    for slug in sorted(set(_builders.keys()) | set(_refresh_specs.keys())):
        lock = _locks.get(slug)
        if lock is None:
            continue
        with lock:
            entries = _caches.get(slug, {})
            sub = {}
            for k, payload in entries.items():
                if isinstance(payload, dict) and "_cached_at" in payload:
                    sub[k] = str(payload.get("_cached_at", "-"))
                elif isinstance(payload, dict) and "payload" in payload:
                    sub[k] = str(payload.get("cached_at", "-"))
                elif hasattr(payload, "__getitem__") and len(payload) >= 7:
                    sub[k] = str(payload[6])
                else:
                    sub[k] = "-"
            if sub or slug in _refresh_specs:
                result[slug] = dict(sub) if sub else {}
    return result


def _estimate_payload_size(payload) -> int:
    """Rough size in bytes for display. Includes DataFrame memory when payload is a dict with trends_df etc.
    Uses memory_usage(deep=False) for DataFrames to avoid blocking (deep=True was too slow on 2M+ rows)."""
    import sys
    try:
        total = sys.getsizeof(payload)
        if isinstance(payload, dict):
            for v in payload.values():
                if isinstance(v, pd.DataFrame):
                    try:
                        total += int(v.memory_usage(deep=False).sum())
                    except Exception:
                        total += sys.getsizeof(v)
                else:
                    total += sys.getsizeof(v)
        return total
    except Exception:
        return 0


def get_cache_status_extended():
    """Extended status for Caching page: per-key cached_at + duration, per-slug size, hits, refreshes."""
    status = {}  # slug -> key -> {cached_at, duration_seconds}
    slug_sizes = {}  # slug -> size_bytes
    with _slug_stats_lock:
        slug_hits = dict(_slug_hits)
        slug_refreshes = dict(_slug_refreshes)
    for slug in sorted(set(_builders.keys()) | set(_refresh_specs.keys())):
        lock = _locks.get(slug)
        if lock is None:
            continue
        with lock:
            entries = _caches.get(slug, {})
            sub = {}
            size_bytes = 0
            for k, payload in entries.items():
                if isinstance(payload, dict) and "_cached_at" in payload:
                    cached_at = str(payload.get("_cached_at", "-"))
                    duration = payload.get("_cached_duration_seconds")
                elif isinstance(payload, dict) and "payload" in payload:
                    cached_at = str(payload.get("cached_at", "-"))
                    duration = payload.get("payload", {}).get("_cached_duration_seconds") if isinstance(payload.get("payload"), dict) else None
                elif hasattr(payload, "__getitem__") and len(payload) >= 7:
                    cached_at = str(payload[6])
                    duration = payload[7] if len(payload) > 7 else None
                else:
                    cached_at = "-"
                    duration = None
                sub[k] = {"cached_at": cached_at, "duration_seconds": duration}
                size_bytes += _estimate_payload_size(payload)
            if sub or slug in _refresh_specs:
                status[slug] = sub
                slug_sizes[slug] = size_bytes
    slug_sizes_human = {s: _bytes_to_human(sz) for s, sz in slug_sizes.items()}
    return {
        "status": status,
        "slug_sizes": slug_sizes,
        "slug_sizes_human": slug_sizes_human,
        "slug_hits": slug_hits,
        "slug_refreshes": slug_refreshes,
    }


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
        _caches.setdefault(slug, {})
        if period == 'today':
            ckey = 'today'
        elif period == 'yesterday':
            ckey = 'yesterday'
        else:
            ckey = identifier
        was_hit = ckey in _caches[slug]
        if not was_hit:
            _refresh(slug, period, lock, ckey if period == 'historical' else None)
        if ckey not in _caches[slug]:
            raise RuntimeError(
                f"Cache build failed for report '{slug}' key '{ckey}'. Check server logs for errors."
            )
        if was_hit:
            with _slug_stats_lock:
                _slug_hits[slug] = _slug_hits.get(slug, 0) + 1
        return _caches[slug][ckey]


def get_cached_data_if_ready(slug: str, identifier: str = 'today'):
    """Non-blocking: return cached payload if available, else None. Never triggers a build.
    Use for filter options callback so dropdowns populate quickly when cache is cold."""
    load_config()
    if slug not in _builders:
        return None
    period = identifier
    if period.upper() == 'TODAY':
        period = 'today'
    cfg = load_config()
    slug_cfg = cfg[cfg['report_slug'] == slug]
    row = slug_cfg[slug_cfg['period'] == period]
    if row.empty:
        period = 'historical'
    lock = _locks.get(slug)
    if lock is None:
        return None
    with lock:
        entries = _caches.get(slug, {})
        if period == 'today':
            ckey = 'today'
        elif period == 'yesterday':
            ckey = 'yesterday'
        else:
            ckey = identifier
        if ckey in entries:
            return entries[ckey]
    return None
