from src.core.logger import get_logger
from src.services.backtesting.report_builder import BacktestReportBuilder

logger = get_logger(__name__)


class SlippageService:

    @staticmethod
    def apply(trade_results: list, slippage_percent: float) -> dict:
        adjusted_trade_results = SlippageService._adjust_trade_results(trade_results, slippage_percent)
        report = BacktestReportBuilder(adjusted_trade_results).build()
        return {
            "slippage_percent": slippage_percent,
            "trade_results": adjusted_trade_results,
            **report,
        }


    @staticmethod
    def _adjust_trade_results(trade_results: list, slippage_percent: float) -> list:
        adjusted_days = []
        for day_result in trade_results:
            adjusted_legs = [
                SlippageService._apply_slippage_to_leg(leg, slippage_percent)
                for leg in day_result["legs"]
            ]
            adjusted_days.append({**day_result, "legs": adjusted_legs})
        return adjusted_days


    @staticmethod
    def _apply_slippage_to_leg(leg: dict, slippage_percent: float) -> dict:
        if leg.get("status") != "EXIT_DONE" or leg.get("entry_price") is None or leg.get("exit_price") is None:
            return dict(leg)

        adjusted = dict(leg)
        direction = 1 if leg["position"] == "BUY" else -1
        slip = slippage_percent / 100

        entry_price = round(leg["entry_price"] * (1 + direction * slip), 2)
        exit_price = round(leg["exit_price"] * (1 - direction * slip), 2)
        adjusted["entry_price"] = entry_price
        adjusted["exit_price"] = exit_price

        multiplier = leg.get("quantity_multiplier")
        if multiplier is None:
            logger.warning(
                f"Leg {leg.get('leg')}: no quantity_multiplier on this trade result "
                f"(older backtest run?) -- pnl left unadjusted for slippage"
            )
            return adjusted

        adjusted["pnl"] = round((exit_price - entry_price) * direction * multiplier, 2)
        return adjusted