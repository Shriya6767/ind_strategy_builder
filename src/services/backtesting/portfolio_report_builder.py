class PortfolioReportBuilder:
    """Reshapes per-strategy + aggregate summaryReport blocks into the
    'Strategy-wise Report' table (Statistic rows x [Aggregate, strategy1,
    strategy2, ...] columns) shown on the Portfolio Backtest screen.

    Deliberately does NOT recompute any stats itself -- it reuses
    BacktestReportBuilder's output verbatim (both for each individual
    strategy and for the merged/aggregate trade_results), so there is a
    single source of truth for how a stat is calculated.
    """

    # (internal key in summaryReport dict, display label for the UI table)
    _ROW_LABELS_BY_UNIT = {
        "Days": [
            ("OverallProfit", "Overall Profit"),
            ("NumberOfDays", "No. of Trades(Periods)"),
            ("AvgProfitPerDays", "Average Profit per Period"),
            ("WinPer", "Win %(Periods)"),
            ("LossPer", "Loss %(Periods)"),
            ("AvgProfitOnWinningDays", "Average Profit on Winning Periods"),
            ("AvgLossOnLossingDays", "Average Loss on Losing Periods"),
            ("MaxProfitInSingleDays", "Max Profit in Single Period"),
            ("MaxLossInSingleDays", "Max Loss in Single Period"),
            ("Max_Drawdown", "Max Drawdown"),
            ("Days_of_Max_Drawdown", "Days of Max Drawdown"),
            ("ReturnPerMaxDD", "Return / MaxDD"),
            ("RewardToRiskRatio", "Reward to Risk Ratio"),
            ("ExpectancyRatio", "Expectancy Ratio"),
            ("MaxWiningStreak", "Max Winning Streak"),
            ("MaxLossingStreak", "Max Losing Streak"),
            ("MaxTradesInDrawdown", "Max Trades In Drawdown"),
        ],
        "Trades": [
            ("OverallProfit", "Overall Profit"),
            ("NumberOfTrades", "No. of Trades(Periods)"),
            ("AvgProfitPerTrade", "Average Profit per Period"),
            ("WinPer", "Win %(Periods)"),
            ("LossPer", "Loss %(Periods)"),
            ("AvgProfitOnWinningTrade", "Average Profit on Winning Periods"),
            ("AvgLossOnLossingTrade", "Average Loss on Losing Periods"),
            ("MaxProfitInSingleTrade", "Max Profit in Single Period"),
            ("MaxLossInSingleTrade", "Max Loss in Single Period"),
            ("Max_Drawdown", "Max Drawdown"),
            ("Days_of_Max_Drawdown", "Days of Max Drawdown"),
            ("ReturnPerMaxDD", "Return / MaxDD"),
            ("RewardToRiskRatio", "Reward to Risk Ratio"),
            ("ExpectancyRatio", "Expectancy Ratio"),
            ("MaxWiningStreak", "Max Winning Streak"),
            ("MaxLossingStreak", "Max Losing Streak"),
            ("MaxTradesInDrawdown", "Max Trades In Drawdown"),
        ],
    }

    def __init__(self, aggregate_report: dict, strategy_payloads: list[dict]):
        """aggregate_report: output of BacktestReportBuilder(merged_trade_results).build()
        strategy_payloads: list of {"strategy_id", "strategy_name", "summary_report_result", ...}
        one per strategy, each already produced by its own BacktestEngine.run()."""
        self.aggregate_report = aggregate_report
        self.strategy_payloads = strategy_payloads

    @staticmethod
    def merge_trade_results(trade_results_by_strategy: dict) -> list[dict]:
        by_date = {}
        for strategy_id, trade_results in trade_results_by_strategy.items():
            for day_result in trade_results:
                trade_date = day_result["trade_date"]
                bucket = by_date.setdefault(trade_date, {"trade_date": trade_date, "legs": []})
                bucket["legs"].extend(
                    {**leg, "strategy_id": strategy_id} for leg in day_result["legs"]
                )
        return [by_date[d] for d in sorted(by_date)]

    def build(self, aggregate_at_eod: bool = True) -> dict:
        """aggregate_at_eod=True -> Daywise (the 'Aggregate At End of Trading
        Day' checkbox checked); False -> Tradewise (every leg/re-entry as
        its own period)."""
        report_type = "Daywise" if aggregate_at_eod else "Tradewise"
        unit = "Days" if aggregate_at_eod else "Trades"

        agg_block = self._find_block(self.aggregate_report["summary_report_result"], report_type)
        strategy_blocks = {
            payload["strategy_id"]: self._find_block(payload["summary_report_result"], report_type)
            for payload in self.strategy_payloads
        }

        rows = []
        for key, label in self._ROW_LABELS_BY_UNIT[unit]:
            row = {"statistic": label, "aggregate": agg_block.get(key)}
            for payload in self.strategy_payloads:
                sid = payload["strategy_id"]
                row[sid] = strategy_blocks[sid].get(key)
            rows.append(row)

        return {
            "reportType": report_type,
            "columns": ["aggregate"] + [p["strategy_id"] for p in self.strategy_payloads],
            "column_names": {
                "aggregate": "Aggregate",
                **{p["strategy_id"]: p.get("strategy_name", p["strategy_id"]) for p in self.strategy_payloads},
            },
            "statistics": rows,
        }

    @staticmethod
    def _find_block(summary_report_result: list[dict], report_type: str) -> dict:
        for block in summary_report_result:
            if block["reportType"] == report_type:
                return block["summaryReport"]
        return {}