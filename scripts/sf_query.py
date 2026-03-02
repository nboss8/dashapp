"""Run Snowflake queries from the command line using app credentials."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

load_dotenv()

from services.snowflake_service import query

if __name__ == "__main__":
    sql = sys.argv[1] if len(sys.argv) > 1 else sys.stdin.read()
    try:
        df = query(sql)
        print(df.to_string() if not df.empty else "No rows")
    except Exception as e:
        print(f"Query error: {e}", file=sys.stderr)
        sys.exit(1)
