from src.core.modules import APIRouter, HTTPException, ORJSONResponse, Depends
from src.core.schemas import LoadDataRequest
from src.core.security import get_current_user
from src.core.data_store import DataStore
from src.core.executor import reset_pool
from src.services.backtesting.load_data import DataLoader
from src.services.backtesting.backtest_service import BacktestService, DataRangeError, NotOwnerError
from src.services.backtesting.compare_backtest import CompareBacktestService
from src.services.backtesting.portfolio_backtest import PortfolioBacktestService
from src.services.auth_service import AuthService
from src.services.user_library import UserLibraryService
from src.services.save_strategy import SaveStrategyService
from src.services.update_strategy import UpdateStrategyService
from src.services.save_portfolio import SavePortfolioService
from src.services.delete_portfolio import DeletePortfolioService
from src.core.logger import get_logger
from src.services.get_strategy import GetStrategyService
from src.services.delete_strategy import DeleteStrategyService


logger = get_logger(__name__)

router = APIRouter()

def _auth_result(result: dict):
    if not result.get("status"):
        raise HTTPException(status_code=result.get("http_status", 400),
                            detail={"status": False, "message": result.get("message")})
    result.pop("http_status", None)
    return result


@router.get("/health")
def health():
    """Public liveness/readiness probe for nginx, systemd and uptime checks:
    200 with the resident data range once the startup preload is done."""
    ready = DataStore.is_loaded()
    rng = DataStore.loaded_range
    return {
        "status": "ok" if ready else "loading",
        "data_loaded": ready,
        "loaded_range": f"{rng[0]} to {rng[1]}" if rng else None,
        "rows": len(DataStore.get_df()) if ready else 0,
    }


@router.post("/auth/signup")
def auth_signup(request: dict):
    """{email, name, password} -> verification code emailed."""
    return _auth_result(AuthService.signup(request))


@router.post("/auth/verify-otp")
def auth_verify_otp(request: dict):
    """{email, otp} -> account verified + access_token."""
    return _auth_result(AuthService.verify_signup_otp(request))


@router.post("/auth/resend-otp")
def auth_resend_otp(request: dict):
    """{email, purpose?: 'signup' | 'reset_password'}"""
    purpose = request.get("purpose") or "signup"
    if purpose not in ("signup", "reset_password"):
        raise HTTPException(status_code=400, detail={"status": False, "message": "Invalid purpose."})
    return _auth_result(AuthService.resend_otp(request, purpose))


@router.post("/auth/login")
def auth_login(request: dict):
    """{email, password} -> access_token."""
    return _auth_result(AuthService.login(request))


@router.post("/auth/forgot-password")
def auth_forgot_password(request: dict):
    """{email} -> reset code emailed."""
    return _auth_result(AuthService.forgot_password(request))


@router.post("/auth/reset-password")
def auth_reset_password(request: dict):
    """{email, otp, new_password}"""
    return _auth_result(AuthService.reset_password(request))


@router.post("/auth/google")
def auth_google(request: dict):
    """{id_token} from Google Identity Services -> access_token."""
    return _auth_result(AuthService.google_login(request))


@router.get("/auth/me")
def auth_me(user=Depends(get_current_user)):
    return _auth_result(AuthService.me(user["user_id"]))


@router.get("/strategies")
def list_strategies(user=Depends(get_current_user)):
    return ORJSONResponse({"status": True, "data": UserLibraryService.list_strategies(user["user_id"])})


@router.get("/portfolios")
def list_portfolios(user=Depends(get_current_user)):
    return ORJSONResponse({"status": True, "data": UserLibraryService.list_portfolios(user["user_id"])})


@router.post("/run-backtest")
def run_backtest(request: dict, user=Depends(get_current_user)):
    try:
        service = BacktestService()
        result = service.run_engine(request, user["user_id"])
        return ORJSONResponse({
            "success": True,
            "message": "Backtest completed successfully.",
            "data": result
        })
    except DataRangeError as e:
        raise HTTPException(status_code=400, detail={"status_code": 400, "message": str(e)})
    except NotOwnerError as e:
        raise HTTPException(status_code=404, detail={"status_code": 404, "message": str(e)})
    except Exception as e:
        logger.exception(f"Unexpected error in run_backtest: {e}")
        raise HTTPException(status_code=500, detail={"status_code": 500, "message": "Failed to start backtest."})


@router.post("/apply-slippage")
def apply_slippage(request: dict, user=Depends(get_current_user)):
    try:
        strategy_id = request["strategy_id"]
        slippage_percent = float(request.get("slippage_percent", 0))
    except (KeyError, TypeError, ValueError):
        raise HTTPException(status_code=422, detail={"status_code": 422, "message": "strategy_id and a numeric slippage_percent are required."})

    try:
        service = BacktestService()
        result = service.apply_slippage(user["user_id"], strategy_id, slippage_percent)
        return ORJSONResponse({
            "success": True,
            "message": "Slippage applied successfully.",
            "data": result
        })
    except KeyError:
        return {
            "success": False,
            "status_code": 404,
            "message": "Backtest result expired, please re-run the backtest.",
            "data": None
        }
    except Exception as e:
        logger.error(f"Unexpected error in apply_slippage: {e}")
        raise HTTPException(status_code=500, detail={"status_code": 500, "message": "Failed to apply slippage."})


@router.post("/compare-backtest")
def compare_backtest(request: dict, user=Depends(get_current_user)):
    try:
        service = CompareBacktestService()
        result = service.compare_backtests(request, user["user_id"])
        return ORJSONResponse({
            "success": True,
            "message": "Backtests compared successfully.",
            "data": result
        })
    except Exception as e:
        logger.error(f"Unexpected error in compare_backtest: {e}")
        raise HTTPException(status_code=500, detail={"status_code": 500, "message": "Failed to compare backtests."})


@router.post("/load-data")
def load_data(request: LoadDataRequest, user=Depends(get_current_user)):
    try:
        if request.reload or not DataStore.is_loaded():
            DataLoader.preload_from_env()
            reset_pool()
        lo, hi = DataStore.bounds(request.start_date, request.end_date)
        avail = DataStore.loaded_range
        if lo >= hi:
            raise HTTPException(
                status_code=400,
                detail={"success": False, "message": f"No market data between {request.start_date} and "
                        f"{request.end_date}" + (f" (available: {avail[0]} to {avail[1]})." if avail else ".")},
            )
        return {
            "success": True,
            "message": "Data available.",
            "data": {
                "symbol": request.symbol,
                "rows": hi - lo,
                "date_range": f"{request.start_date} to {request.end_date}",
                "loaded_range": f"{avail[0]} to {avail[1]}" if avail else None,
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to load data: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@router.post("/save-strategy")
def save_strategy(request: dict, user=Depends(get_current_user)):
    try:
        result = SaveStrategyService.save_strategy(request, user["user_id"])
        if not result["status"] and result.get("not_found"):
            raise HTTPException(status_code=404, detail={"status": False, "message": result["message"]})
        return {
            "status": result["status"],
            "message": "Strategy saved successfully." if result["status"] else result["message"],
            "strategy_id": result.get("strategy_id"),
            "version": result.get("version")
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in save_strategy: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }
        )


@router.put("/update-strategy")
def update_strategy(request: dict, user=Depends(get_current_user)):
    try:
        result = UpdateStrategyService.update_strategy(request, user["user_id"])
        if not result["status"]:
            raise HTTPException(
                status_code=404 if result.get("not_found") else 400,
                detail={"status": False, "message": result["message"]}
            )
        return {
            "status": True,
            "message": "Strategy updated successfully.",
            "strategy_id": result["strategy_id"],
            "version": result["version"],
            "legs_saved": result["legs_saved"],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in update_strategy: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }
        )


@router.get("/get-strategy")
def get_strategy(strategy_id: int, strategy_name: str, version: int, user=Depends(get_current_user)):
    if not strategy_id or not strategy_name or not version:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "error": "strategy_id, strategy_name, and version are required"
            }
        )
    try:
        result = GetStrategyService.get_strategy(strategy_id, strategy_name, version, user["user_id"])

        if not result.get("success"):
            error_msg = result.get("error", "Failed to load strategy")
            status_code = 404 if "not found" in error_msg.lower() else 500
            raise HTTPException(
                status_code=status_code,
                detail={
                    "success": False,
                    "error": error_msg
                }
            )

        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[GET-STRATEGY] Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": f"Internal server error: {str(e)}"
            }
        )


@router.delete("/delete-strategy")
def delete_strategy(strategy_id: int, strategy_name: str, user=Depends(get_current_user)):
    if not strategy_id or not strategy_name:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "error": "strategy_id and strategy_name are required"
            }
        )
    try:
        result = DeleteStrategyService.delete_strategy(strategy_id, strategy_name, user["user_id"])
        if not result.get("success"):
            error_msg = result.get("error", "Failed to delete strategy")
            status_code = 404 if "not found" in error_msg.lower() else 500
            raise HTTPException(
                status_code=status_code,
                detail={
                    "success": False,
                    "error": error_msg
                }
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[DELETE-STRATEGY] Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": f"Internal server error: {str(e)}"
            }
        )


@router.post("/save-portfolio")
def save_portfolio(request: dict, user=Depends(get_current_user)):
    try:
        save_portfolio_service = SavePortfolioService()
        result = save_portfolio_service.save_portfolio(request, user["user_id"])
        return {
            "status": result["status"],
            "message": result["message"],
            "portfolio_id": result.get("portfolio_id"),
            "portfolio_name": result.get("portfolio_name")
        }
    except Exception as e:
        logger.error(f"Unexpected error in save_portfolio: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }
        )


@router.post("/delete-portfolio")
def delete_portfolio(request: dict, user=Depends(get_current_user)):
    try:
        delete_portfolio_service = DeletePortfolioService()
        result = delete_portfolio_service.delete_portfolio(request, user["user_id"])
        return {
            "status": result["status"],
            "message": result["message"]
        }
    except Exception as e:
        logger.error(f"Unexpected error in delete_portfolio: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }
        )


@router.post("/run-portfolio-backtest")
def run_portfolio_backtest(request: dict, user=Depends(get_current_user)):
    try:
        portfolio_service = PortfolioBacktestService()
        return ORJSONResponse(portfolio_service.run_portfolio(request, user["user_id"]))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run-trading")
async def run_trading(user=Depends(get_current_user)):
    try:
        logger.info("Trading started successfully.")
        return {
            "success": True,
            "message": "Trading started successfully.",
            "data": {}
        }
    except Exception as e:
        logger.error(f"Unexpected error in run_trading: {e}")
        raise HTTPException(status_code=500, detail="Failed to start trading.")