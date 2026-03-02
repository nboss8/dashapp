"""
Lightweight Snowflake service. Uses snowflake.connector + context manager.
No SQLAlchemy. Token auth. Safe for gunicorn: per-request connections, no global shared conn.

Why context manager (not global conn):
- Gunicorn workers are separate processes; a global conn in one worker is not shared.
- Long-lived conns can expire (Snowflake token/session). Per-request avoids stale connections.
- Context manager guarantees conn.close() even on errors.
"""
from contextlib import contextmanager
import os

import pandas as pd
import snowflake.connector


@contextmanager
def get_connection():
    conn = snowflake.connector.connect(
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
    try:
        yield conn
    finally:
        conn.close()


def query(sql: str, params=None) -> pd.DataFrame:
    '''Add USE_CACHED_RESULT hint for query reuse.'''
    """Execute Snowflake query and return DataFrame. Safe for gunicorn multi-worker.
    Use params for parameterized queries (e.g. params=[day_label]).
    Returns empty DataFrame on error (preserves graceful UI fallback)."""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"/*+ USE_CACHED_RESULT */ {sql}", params or [])
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        print(f"Query error: {e}")
        return pd.DataFrame()
