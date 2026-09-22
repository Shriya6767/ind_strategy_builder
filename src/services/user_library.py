"""What a logged-in user owns: the 'My Strategies' and 'Portfolios'
sidebars. Only rows with the caller's user_id are ever returned."""
from src.core.modules import RealDictCursor
from src.core.config import Database
from src.core.logger import get_logger

logger = get_logger(__name__)


class UserLibraryService:

    @staticmethod
    def list_strategies(user_id: int) -> list[dict]:
        """One entry per strategy_id with every saved version, newest first."""
        conn = None
        try:
            conn = Database.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(
                """
                SELECT strategy_id, strategy_name, version, strategy_type, symbol,
                       start_date, end_date, leg_count, created_at, updated_at
                FROM strategy
                WHERE user_id = %s
                ORDER BY strategy_id DESC, version DESC;
                """,
                (user_id,),
            )
            grouped = {}
            for row in cursor.fetchall():
                entry = grouped.setdefault(row["strategy_id"], {
                    "strategy_id": row["strategy_id"],
                    "strategy_name": row["strategy_name"],
                    "latest_version": row["version"],
                    "versions": [],
                })
                entry["versions"].append({
                    "version": row["version"],
                    "strategy_name": row["strategy_name"],
                    "strategy_type": row["strategy_type"],
                    "symbol": row["symbol"],
                    "start_date": row["start_date"],
                    "end_date": row["end_date"],
                    "leg_count": row["leg_count"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                })
            return list(grouped.values())
        finally:
            if conn:
                conn.close()


    @staticmethod
    def list_portfolios(user_id: int) -> list[dict]:
        conn = None
        try:
            conn = Database.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(
                """
                SELECT portfolio_id, portfolio_name, strategies, created_at, updated_at
                FROM portfolio
                WHERE user_id = %s
                ORDER BY portfolio_id DESC;
                """,
                (user_id,),
            )
            return cursor.fetchall()
        finally:
            if conn:
                conn.close()