from urllib import request
from src.core.modules import APIRouter, HTTPException, ORJSONResponse
from src.core.schemas import LoadDataRequest
from src.services.backtesting.load_data import DataLoader
from src.services.backtesting.backtest_service import BacktestService
from src.services.backtesting.compare_backtest import CompareBacktestService
from src.services.backtesting.portfolio_backtest import PortfolioBacktestService
from src.services.save_strategy import SaveStrategyService
from src.services.save_portfolio import SavePortfolioService
from src.services.delete_portfolio import DeletePortfolioService
from src.core.logger import get_logger
from src.services.get_strategy import GetStrategyService
from src.services.delete_strategy import DeleteStrategyService


logger = get_logger(__name__)

router = APIRouter()

@router.post("/run-backtest")
async def run_backtest(request: dict):
    try:
        service = BacktestService()
        result = service.run_engine(request)
        return ORJSONResponse({
            "success": True,
            "message": "Backtest completed successfully.",
            "data": result
        })
    except Exception as e:
        logger.error(f"Unexpected error in run_backtest: {e}")
        raise HTTPException(status_code=500, detail={"status_code": 500, "message": "Failed to start backtest."})


@router.post("/apply-slippage")
async def apply_slippage(request: dict):
    try:
        strategy_id = request["strategy_id"]
        slippage_percent = float(request.get("slippage_percent", 0))
    except (KeyError, TypeError, ValueError):
        raise HTTPException(status_code=422, detail={"status_code": 422, "message": "strategy_id and a numeric slippage_percent are required."})

    try:
        service = BacktestService()
        result = service.apply_slippage(strategy_id, slippage_percent)
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
async def compare_backtest(request: dict):
    try:
        service = CompareBacktestService()
        result = service.compare_backtests(request)
        return ORJSONResponse({
            "success": True,
            "message": "Backtests compared successfully.",
            "data": result
        })
    except Exception as e:
        logger.error(f"Unexpected error in compare_backtest: {e}")
        raise HTTPException(status_code=500, detail={"status_code": 500, "message": "Failed to compare backtests."})


@router.post("/load-data")
def load_data(request: LoadDataRequest):
    try:
        loader = DataLoader(symbol=request.symbol)
        rows = loader.load(request.start_date, request.end_date)
        return {
            "success": True,
            "message": "Data loaded successfully.",
            "data": {
                "symbol": request.symbol,
                "rows": rows,
                "date_range": f"{request.start_date} to {request.end_date}",
            }
        }
    except Exception as e:
        logger.exception(f"Failed to load data: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
        
        
@router.post("/save-strategy")
def save_strategy(request: dict):
    try:
        result = SaveStrategyService.save_strategy(request)
        return {
            "status": result["status"],
            "message": "Strategy saved successfully." if result["status"] else result["message"],
            "strategy_id": result.get("strategy_id"),
            "version": result.get("version")
        }
    except Exception as e:
        logger.error(f"Unexpected error in save_strategy: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }
        )


@router.get("/get-strategy")
def get_strategy(strategy_id: int, strategy_name: str, version: int):  
    if not strategy_id or not strategy_name or not version:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "error": "strategy_id, strategy_name, and version are required"
            }
        )
    try:
        result = GetStrategyService.get_strategy(strategy_id, strategy_name, version)
        
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
def delete_strategy(strategy_id: int, strategy_name: str):
    if not strategy_id or not strategy_name:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "error": "strategy_id and strategy_name are required"
            }
        )
    try:
        result = DeleteStrategyService.delete_strategy(strategy_id, strategy_name)
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
def save_portfolio(request: dict):
    try:
        save_portfolio_service = SavePortfolioService()
        result = save_portfolio_service.save_portfolio(request)
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
def delete_portfolio(request: dict):
    try:
        delete_portfolio_service = DeletePortfolioService()
        result = delete_portfolio_service.delete_portfolio(request)
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
def run_portfolio_backtest(request: dict):
    try:
        portfolio_service = PortfolioBacktestService()
        return ORJSONResponse(portfolio_service.run_portfolio(request))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run-trading")
async def run_trading():
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