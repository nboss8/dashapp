"""Clear the Packed Inventory Trends cache so the next load uses fresh Snowflake data.
Run this if the Cartons chart shows wrong/ramp data while EQs look correct.
Then restart the app or go to Caching page and click Refresh for 'trends'.

When the app runs in Docker, run this script inside the container so it uses the same
REPORT_CACHE_DIR as the app (e.g. docker compose exec app python scripts/clear_trends_cache.py).
Running on the host uses a different cache directory and will report "Key not found on disk".

After fixing payload or chart logic, run this (in the container) and refresh TRENDS on Caching page so the next load uses fresh data — required for fixes to take effect."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from services.cache_manager import _persistent, _caches, _locks, PERSISTENT_CACHE_DIR

def main():
    print(f"Cache directory: {PERSISTENT_CACHE_DIR}")
    for key in ("trends:default",):
        if key in _persistent:
            _persistent.delete(key)
            print(f"Deleted disk cache key: {key}")
        else:
            print(f"Key not found on disk: {key}")
    for slug in ("trends",):
        if slug in _caches and "default" in _caches[slug]:
            del _caches[slug]["default"]
            print(f"Cleared in-memory {slug}/default (only matters if app is running in this process)")
    print("Done. Restart the Dash app, or go to Caching page and click Refresh for TRENDS.")

if __name__ == "__main__":
    main()
