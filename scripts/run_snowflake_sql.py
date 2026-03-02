"""
Run a Snowflake SQL file from the command line. Loads .env for credentials.
Usage: python scripts/run_snowflake_sql.py scripts/snowflake/create_dt_inv_on_hand_sku_grain.sql
"""
import os
import sys
from pathlib import Path

# Load .env from project root
root = Path(__file__).resolve().parent.parent
env_path = root / ".env"
if env_path.exists():
    from dotenv import load_dotenv
    load_dotenv(env_path)

import snowflake.connector


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/run_snowflake_sql.py <path-to-sql-file>")
        sys.exit(1)

    sql_path = Path(sys.argv[1])
    if not sql_path.is_absolute():
        sql_path = root / sql_path

    if not sql_path.exists():
        print(f"File not found: {sql_path}")
        sys.exit(1)

    sql = sql_path.read_text(encoding="utf-8")

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        authenticator="programmatic_access_token",
        token=os.getenv("SNOWFLAKE_TOKEN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    try:
        conn.cursor().execute(sql)
        print("SQL executed successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
