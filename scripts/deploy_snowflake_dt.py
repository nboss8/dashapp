"""
Deploy Snowflake Dynamic Table scripts. Run from project root with .env loaded.

Usage:
  python scripts/deploy_snowflake_dt.py
  # Or: python scripts/deploy_snowflake_dt.py scripts/snowflake/create_dt_inv_on_hand_sku_grain.sql

Loads .env, executes CREATE OR REPLACE DYNAMIC TABLE scripts.
"""
import os
import sys

from dotenv import load_dotenv

load_dotenv()

import snowflake.connector


def run_sql_file(path: str) -> bool:
    """Execute SQL file. Returns True on success."""
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    sql = sql.strip()
    if not sql:
        print(f"  (empty file)")
        return True
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            authenticator="programmatic_access_token",
            token=os.getenv("SNOWFLAKE_TOKEN"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "CT_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE", "FROSTY"),
            schema=os.getenv("DBT_SCHEMA", "DBT_DEV_DBT_DEV"),
            network_timeout=120,
            login_timeout=30,
        )
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            print(f"  OK: {path}")
            return True
        finally:
            conn.close()
    except Exception as e:
        print(f"  FAIL: {path}\n  {e}")
        return False


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    defaults = [
        "scripts/snowflake/create_dt_inv_on_hand_sku_grain.sql",
        "scripts/snowflake/create_dt_carton_daily_snapshot_eq.sql",
    ]
    paths = sys.argv[1:] if len(sys.argv) > 1 else defaults
    ok = True
    for p in paths:
        full = os.path.join(root, p) if not os.path.isabs(p) else p
        if not os.path.isfile(full):
            print(f"  SKIP (not found): {p}")
            continue
        ok = run_sql_file(full) and ok
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
