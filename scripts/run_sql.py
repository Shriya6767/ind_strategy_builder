"""Apply a .sql file (e.g. a migration under scripts/sql/) through the
project's own PostgreSQL connection, so the same command works on the
laptop and the host without needing psql on the PATH:

    venv\\Scripts\\python.exe scripts\\run_sql.py scripts\\sql\\portfolio_risk_migration.sql   (Windows)
    venv/bin/python scripts/run_sql.py scripts/sql/portfolio_risk_migration.sql               (Linux)

Connection details come from .env (DATABASE_*). The whole file runs in one
transaction: any failure rolls everything back. Our migrations are written
with IF NOT EXISTS, so re-running one is safe.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import Database


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    path = sys.argv[1]
    with open(path, encoding="utf-8") as fh:
        sql = fh.read()

    conn = Database.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        print(f"applied {path}")
        return 0
    except Exception as exc:
        conn.rollback()
        print(f"FAILED (rolled back): {exc}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
