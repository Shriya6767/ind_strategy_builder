from src.core.config import Database
from src.core.logger import get_logger
logger = get_logger(__name__)


class DeleteStrategyService:

    @staticmethod
    def delete_strategy(strategy_id, strategy_name, user_id):
        conn = None
        cursor = None

        try:
            conn = Database.get_connection()
            cursor = conn.cursor()

            # Ownership first: legs have no user_id of their own, so confirm
            # the strategy belongs to the caller before touching them.
            cursor.execute(
                "SELECT 1 FROM strategy WHERE strategy_id = %s AND strategy_name = %s AND user_id = %s LIMIT 1;",
                (strategy_id, strategy_name, user_id)
            )
            if cursor.fetchone() is None:
                return {
                    "success": False,
                    "error": f"Strategy not found for strategy_id={strategy_id}, strategy_name='{strategy_name}'"
                }

            delete_legs_query = """
            DELETE FROM leg_details WHERE strategy_id = %s;
            """

            cursor.execute(delete_legs_query, (strategy_id,))
            deleted_leg_count = cursor.rowcount

            delete_strategy_query = """
            DELETE FROM strategy
            WHERE strategy_id = %s AND strategy_name = %s AND user_id = %s
            RETURNING strategy_id;
            """
            cursor.execute(delete_strategy_query, (strategy_id, strategy_name, user_id))
            deleted = cursor.fetchone()

            if not deleted:
                conn.rollback()
                return {
                    "success": False,
                    "error": f"Strategy not found for strategy_id={strategy_id}, strategy_name='{strategy_name}'"
                }

            conn.commit()
            logger.info(
                f"Strategy deleted successfully. strategy_id={strategy_id}, "
                f"strategy_name='{strategy_name}', legs_deleted={deleted_leg_count}"
            )

            return {
                "success": True,
                "message": "Strategy deleted successfully.",
                "strategy_id": strategy_id,
                "legs_deleted": deleted_leg_count
            }

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"[DELETE-STRATEGY] Failed to delete strategy: {e}")
            return {
                "success": False,
                "error": str(e)
            }
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()