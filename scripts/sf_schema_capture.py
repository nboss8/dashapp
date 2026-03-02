"""
Capture Snowflake schema (objects, columns, sample rows) to JSON.

Targets FROSTY schema objects the DASH_APP role can access:
  - FROSTY.APP:    tables and views (GRANT SELECT ON ALL TABLES/VIEWS)
  - FROSTY.STAGING: tables, views, and dynamic tables (shared data)

Run periodically to refresh. Use for context when developing.
"""
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

load_dotenv()

import pandas as pd

from services.snowflake_service import get_connection

SAMPLE_ROW_LIMIT = 2
SCHEMA_OUTPUT = "schema/snowflake_catalog.json"


def to_json_serializable(val):
    """Convert pandas/numpy values to JSON-serializable types."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (datetime, pd.Timestamp)):
        return val.isoformat()
    if isinstance(val, (pd.Timedelta,)):
        return str(val)
    if isinstance(val, (bytes, bytearray)):
        return val.decode("utf-8", errors="replace")
    if hasattr(val, "item"):  # numpy scalar
        return val.item()
    return val


# Schema config: (database, schema, include_tables, include_views, include_dynamic_tables)
SCHEMA_TARGETS = [
    ("FROSTY", "APP", True, True, False),      # tables + views
    ("FROSTY", "STAGING", True, True, True),   # tables + views + dynamic tables (shared)
]


def get_accessible_objects(conn):
    """
    Discover objects per schema targets:
    - FROSTY.APP: tables and views
    - FROSTY.STAGING: tables, views, and dynamic tables
    """
    cur = conn.cursor()
    objects = []
    # SHOW output: created_on, name, database_name, schema_name, kind, ...
    for db, schema, incl_tables, incl_views, incl_dynamic in SCHEMA_TARGETS:
        try:
            if incl_tables:
                cur.execute(f'SHOW TABLES IN SCHEMA "{db}"."{schema}"')
                for row in cur.fetchall():
                    objects.append(f'{db}.{schema}.{row[1]}')
            if incl_views:
                cur.execute(f'SHOW VIEWS IN SCHEMA "{db}"."{schema}"')
                for row in cur.fetchall():
                    objects.append(f'{db}.{schema}.{row[1]}')
            if incl_dynamic:
                cur.execute(f'SHOW DYNAMIC TABLES IN SCHEMA "{db}"."{schema}"')
                for row in cur.fetchall():
                    objects.append(f'{db}.{schema}.{row[1]}')
        except Exception as e:
            print(f"  Skip {db}.{schema}: {e}", file=sys.stderr)
    return sorted(set(objects))


def fetch_columns(conn, full_name: str) -> list[dict]:
    """Get column metadata from INFORMATION_SCHEMA."""
    parts = full_name.split(".")
    db, schema, obj = parts[0], parts[1], parts[2]
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
        FROM {db}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (schema.upper(), obj.upper()))
    rows = cursor.fetchall()
    return [
        {"name": r[0], "type": r[1], "ordinal": r[2]}
        for r in rows
    ]


def fetch_sample(conn, full_name: str, limit: int = 2) -> list[dict]:
    """Get sample rows. Returns list of dicts with JSON-serializable values."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {full_name} LIMIT {limit}")
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    out = []
    for row in rows:
        out.append({
            cols[i]: to_json_serializable(row[i])
            for i in range(len(cols))
        })
    return out


def capture():
    with get_connection() as conn:
        objects = get_accessible_objects(conn)
        print(f"Discovered {len(objects)} objects (FROSTY.APP: tables+views, FROSTY.STAGING: tables+views+dynamic)", file=sys.stderr)
        catalog = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "schema_targets": [f"{d}.{s}" for d, s, _, _, _ in SCHEMA_TARGETS],
            "objects": {},
        }
        errors = []
        for obj in objects:
            try:
                columns = fetch_columns(conn, obj)
                sample = fetch_sample(conn, obj, SAMPLE_ROW_LIMIT)
                catalog["objects"][obj] = {
                    "columns": columns,
                    "sample": sample,
                }
                print(f"  OK   {obj}", file=sys.stderr)
            except Exception as e:
                catalog["objects"][obj] = {"error": str(e), "columns": [], "sample": []}
                errors.append((obj, str(e)))
                print(f"  FAIL {obj}: {e}", file=sys.stderr)

    os.makedirs(os.path.dirname(SCHEMA_OUTPUT) or ".", exist_ok=True)
    out_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        SCHEMA_OUTPUT,
    )
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, default=str)

    print(f"\nWrote {out_path}", file=sys.stderr)
    if errors:
        print(f"Errors: {len(errors)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(capture())
