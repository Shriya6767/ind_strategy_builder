from psycopg2.extras import Json
from src.core.config import Database
from src.core.logger import get_logger

logger = get_logger(__name__)


class DeletePortfolioService:
    @staticmethod
    def delete_portfolio(request: dict) -> dict:
        conn = None
        try:
            portfolio_id = request.get("portfolio_id")

            if not portfolio_id:
                return {"status": False, "message": "Portfolio ID is required."}

            conn = Database.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                DELETE FROM portfolio
                WHERE portfolio_id = %s
                RETURNING id;
                """,
                (portfolio_id,)
            )

            row = cursor.fetchone()
            if row is None:
                return {"status": False, "message": f"Portfolio {portfolio_id} not found."}

            conn.commit()
            logger.info(f"Portfolio deleted successfully. portfolio_id={portfolio_id}")
            return {
                "status": True,
                "message": f"Portfolio {portfolio_id} deleted successfully."
            }
        except Exception as e:
            if conn:
                conn.rollback()
            logger.exception(f"Failed to delete portfolio: {e}")
            return {"status": False, "message": str(e)}
        finally:
            if conn:
                cursor.close()
                conn.close()