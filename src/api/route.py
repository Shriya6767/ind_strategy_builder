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
from src.live.broker_store import BrokerAccountService
from src.live.service import LiveTradeService, LiveValidationError
from src.live.xts_client import XTSError


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


# Live trading (Symphony Open XTS). State lives in PostgreSQL; execution in
# the live worker process (src/live/worker.py), which these routes call.

def _live_error(e: Exception):
    if isinstance(e, LiveValidationError) or isinstance(e, ValueError):
        raise HTTPException(status_code=400, detail={"status": False, "message": str(e)})
    if isinstance(e, LookupError):
        raise HTTPException(status_code=404, detail={"status": False, "message": str(e)})
    if isinstance(e, PermissionError):
        raise HTTPException(status_code=403, detail={"status": False, "message": str(e)})
    if isinstance(e, XTSError):
        raise HTTPException(status_code=502, detail={"status": False, "message": f"Broker: {e}"})
    logger.exception(f"[LIVE] unexpected error: {e}")
    raise HTTPException(status_code=500, detail={"status": False, "message": f"Internal server error: {e}"})


#Broker Setup
@router.get("/live/brokers")
def live_list_brokers(user=Depends(get_current_user)):
    return {"status": True, "data": BrokerAccountService.list(user["user_id"])}


@router.post("/live/brokers")
def live_add_broker(request: dict, user=Depends(get_current_user)):
    """{connection_name, interactive_key, interactive_secret, connection_url,
        marketdata_key?, marketdata_secret?, marketdata_url?, host_lookup_url?,
        host_lookup_password?, dealer_client_id?}"""
    try:
        return {"status": True, "message": "Broker added.", "data": BrokerAccountService.add(user["user_id"], request)}
    except Exception as e:
        _live_error(e)


@router.delete("/live/brokers/{broker_account_id}")
def live_delete_broker(broker_account_id: int, user=Depends(get_current_user)):
    if not BrokerAccountService.delete(broker_account_id, user["user_id"]):
        raise HTTPException(status_code=404, detail={"status": False, "message": "Broker account not found"})
    return {"status": True, "message": "Broker removed."}


@router.post("/live/brokers/{broker_account_id}/login")
async def live_broker_login(broker_account_id: int, user=Depends(get_current_user)):
    """Daily broker login: HostLookUp (if configured) + Interactive API session."""
    try:
        data = await BrokerAccountService.login(broker_account_id, user["user_id"])
        return {"status": True, "message": "Broker login successful.", "data": data}
    except Exception as e:
        _live_error(e)


@router.post("/live/brokers/{broker_account_id}/logout")
async def live_broker_logout(broker_account_id: int, user=Depends(get_current_user)):
    try:
        return {"status": True, "message": "Logged out.", "data": await BrokerAccountService.logout(broker_account_id, user["user_id"])}
    except Exception as e:
        _live_error(e)


@router.get("/live/brokers/{broker_account_id}/funds")
async def live_broker_funds(broker_account_id: int, user=Depends(get_current_user)):
    try:
        return {"status": True, "data": await BrokerAccountService.funds(broker_account_id, user["user_id"])}
    except Exception as e:
        _live_error(e)


@router.put("/live/execution-settings")
def live_save_execution_settings(request: dict, user=Depends(get_current_user)):
    """{strategy_id, version?, broker_account_id?, auto_activate?,
        settings: {mode, qty_multiplier, trade_monitoring, monitoring_frequency_sec, strategy_execution_time,
                   order_timeout_sec, exit_fallback_market, execution_days_mode, execution_days, execution_dte,
                   squareoff_on_entry_error, max_daily_loss, paper_slippage_pct,
                   legs: {"<leg number>": {product, tgt_sl_ref_price, delay_entry_sec, entry_order_type,
                          exit_order_type, entry_buffer_type, exit_buffer_type, entry_trigger_buffer,
                          entry_limit_buffer, exit_trigger_buffer, exit_limit_buffer, sl_order_at_broker,
                          trail_monitoring, trail_frequency_sec, entry_convert_to_market_sec,
                          exit_convert_to_market_sec}}}}"""
    try:
        return {"status": True, "message": "Execution settings saved.", "data": LiveTradeService.save_execution_settings(user["user_id"], request)}
    except Exception as e:
        _live_error(e)


@router.get("/live/overview")
def live_overview(user=Depends(get_current_user)):
    """Everything the Algo Trade page needs on load: saved execution settings + today's deployments."""
    return ORJSONResponse({"status": True, "data": LiveTradeService.overview(user["user_id"])})


@router.post("/live/deployments")
def live_activate(request: dict, user=Depends(get_current_user)):
    """Activate: {strategy_id, version?, broker_account_id?, mode?, settings?} -> deployment (starts in the worker)."""
    try:
        return {"status": True, "message": "Strategy activated.", "data": LiveTradeService.activate(user["user_id"], request)}
    except Exception as e:
        _live_error(e)


@router.get("/live/deployments")
def live_list_deployments(trade_date: str | None = None, include_archived: bool = False, user=Depends(get_current_user)):
    try:
        return ORJSONResponse({"status": True, "data": LiveTradeService.list(user["user_id"], trade_date, include_archived)})
    except Exception as e:
        _live_error(e)


@router.get("/live/deployments/{deployment_id}")
def live_deployment_detail(deployment_id: int, user=Depends(get_current_user)):
    try:
        return ORJSONResponse({"status": True, "data": LiveTradeService.detail(user["user_id"], deployment_id)})
    except Exception as e:
        _live_error(e)


@router.post("/live/deployments/{deployment_id}/{cmd}")
def live_deployment_command(deployment_id: int, cmd: str, user=Depends(get_current_user)):
    """cmd = pause | resume | squareoff | activate (re-send to the worker)"""
    try:
        return {"status": True, "data": LiveTradeService.command(user["user_id"], deployment_id, cmd)}
    except Exception as e:
        _live_error(e)


@router.delete("/live/deployments/{deployment_id}")
def live_archive_deployment(deployment_id: int, user=Depends(get_current_user)):
    if not LiveTradeService.archive(user["user_id"], deployment_id):
        raise HTTPException(status_code=400, detail={"status": False, "message": "Only finished deployments can be archived"})
    return {"status": True, "message": "Archived."}


@router.post("/live/squareoff-all")
def live_squareoff_all(user=Depends(get_current_user)):
    """Kill switch: exits every open leg of every running deployment of the caller."""
    return {"status": True, "data": LiveTradeService.squareoff_all(user["user_id"])}


@router.get("/live/snapshots")
def live_snapshots(user=Depends(get_current_user)):
    """REST fallback for the /ws/live stream (same payload, polled)."""
    return ORJSONResponse({"status": True, "data": LiveTradeService.live_snapshots(user["user_id"])})