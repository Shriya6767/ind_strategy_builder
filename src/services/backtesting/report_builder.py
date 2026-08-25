from src.core.modules import np

class BacktestReportBuilder:
    """Builds summary_report_result + monthly_state_result from a
    trade_results list. Extracted out of BacktestEngine so the exact same
    reporting logic can be reused for slippage-adjusted recomputation
    without re-running the (expensive) day-by-day simulation -- just call
    BacktestReportBuilder(adjusted_trade_results).build() again."""

    _SUMMARY_FIELD_NAMES = {
        "Days": {
            "count": "NumberOfDays",
            "avg": "AvgProfitPerDays",
            "avg_win": "AvgProfitOnWinningDays",
            "avg_loss": "AvgLossOnLossingDays",
            "max_win": "MaxProfitInSingleDays",
            "max_loss": "MaxLossInSingleDays",
        },
        "Trades": {
            "count": "NumberOfTrades",
            "avg": "AvgProfitPerTrade",
            "avg_win": "AvgProfitOnWinningTrade",
            "avg_loss": "AvgLossOnLossingTrade",
            "max_win": "MaxProfitInSingleTrade",
            "max_loss": "MaxLossInSingleTrade",
        },
    }

    _MONTH_NAMES = ["january", "february", "march", "april", "may", "june",
                    "july", "august", "september", "october", "november", "december"]

    def __init__(self, trade_results: list):
        self.trade_results = trade_results

    def build(self) -> dict:
        return {
            "summary_report_result": self._build_summaryreport_result(),
            "monthly_state_result": self._build_monthly_state_result(),
        }

    def _build_summaryreport_result(self) -> list[dict]:
        """Builds the Daywise + Tradewise summaryreportResult blocks.
        Daywise treats each trading day's combined leg pnl as one unit;
        Tradewise treats every individual executed leg/re-entry as one unit."""
        daily_pnls, daily_dates = self._collect_daily_pnls()
        trade_pnls, trade_dates = self._collect_trade_pnls()
        return [
            {"reportType": "Daywise", "summaryReport": self._build_summary_block(daily_pnls, daily_dates, "Days")},
            {"reportType": "Tradewise", "summaryReport": self._build_summary_block(trade_pnls, trade_dates, "Trades")},
        ]

    def _collect_daily_pnls(self):
        """One combined pnl number per trading day (sum of that day's leg
        pnls), skipping days where nothing actually executed."""
        pnls, dates = [], []
        for day_result in self.trade_results:
            day_pnls = [leg["pnl"] for leg in day_result["legs"] if leg.get("pnl") is not None]
            if not day_pnls:
                continue
            pnls.append(sum(day_pnls))
            dates.append(day_result["trade_date"])
        return pnls, dates

    def _build_monthly_state_result(self) -> list[dict]:
        """Year-by-year monthly pnl breakdown, plus each year's own peak-to-trough
        drawdown scoped to just that year's days."""
        daily_pnls, daily_dates = self._collect_daily_pnls()

        by_year = {}
        for pnl, date in zip(daily_pnls, daily_dates):
            by_year.setdefault(date.year, []).append((date, pnl))

        result = []
        for year in sorted(by_year):
            year_dates = [d for d, _ in by_year[year]]
            year_pnls = [p for _, p in by_year[year]]

            monthly_sums = {m: None for m in range(1, 13)}
            for d, p in zip(year_dates, year_pnls):
                monthly_sums[d.month] = (monthly_sums[d.month] or 0.0) + p

            total = sum(year_pnls)
            max_dd, dd_label, _ = self._max_drawdown_stats(year_pnls, year_dates)
            yearly_return_per_maxdd = total / abs(max_dd) if max_dd else 0.0

            entry = {
                name: (f"{monthly_sums[i + 1]:.2f}" if monthly_sums[i + 1] is not None else None)
                for i, name in enumerate(self._MONTH_NAMES)
            }
            entry.update({
                "total": f"{total:.2f}",
                "max_Drawdown": f"{max_dd:.2f}",
                "days_of_Max_Drawdown": dd_label,
                "yearly_Return_per_MaxDD": f"{yearly_return_per_maxdd:.2f}",
                "year": str(year),
            })
            result.append(entry)
        return result

    def _collect_trade_pnls(self):
        """Every individual executed leg (including re-entries) as its
        own unit, in chronological order."""
        pnls, dates = [], []
        for day_result in self.trade_results:
            for leg in day_result["legs"]:
                if leg.get("pnl") is None:
                    continue
                entry_dt = leg.get("entry_datetime")
                pnls.append(leg["pnl"])
                dates.append(entry_dt.date() if entry_dt is not None else day_result["trade_date"])
        return pnls, dates

    def _build_summary_block(self, pnls: list, dates: list, unit: str) -> dict:
        names = self._SUMMARY_FIELD_NAMES[unit]

        if not pnls:
            return {"OverallProfit": "0.00", names["count"]: "0", names["avg"]: "0.00",
                    "WinPer": "0.00", "LossPer": "0.00", names["avg_win"]: "0.00", names["avg_loss"]: "0.00",
                    names["max_win"]: "0.00", names["max_loss"]: "0.00", "Max_Drawdown": "0.00",
                    "Days_of_Max_Drawdown": "0 [NA to NA]", "ReturnPerMaxDD": "0.00", "RewardToRiskRatio": "0.00",
                    "ExpectancyRatio": "0.00", "MaxWiningStreak": "0", "MaxLossingStreak": "0",
                    "MaxTradesInDrawdown": "0"}

        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]

        overall_profit = sum(pnls)
        count = len(pnls)
        avg = overall_profit / count
        win_per = len(wins) / count * 100
        loss_per = len(losses) / count * 100
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0

        max_dd, dd_label, trades_in_dd = self._max_drawdown_stats(pnls, dates)
        return_per_max_dd = overall_profit / abs(max_dd) if max_dd else 0.0
        reward_to_risk = abs(avg_win) / abs(avg_loss) if avg_loss else 0.0
        expectancy_ratio = avg / abs(avg_loss) if avg_loss else 0.0
        max_win_streak, max_loss_streak = self._max_streaks(pnls)

        return {
            "OverallProfit": f"{overall_profit:.2f}",
            names["count"]: str(count),
            names["avg"]: f"{avg:.2f}",
            "WinPer": f"{win_per:.2f}",
            "LossPer": f"{loss_per:.2f}",
            names["avg_win"]: f"{avg_win:.2f}",
            names["avg_loss"]: f"{avg_loss:.2f}",
            names["max_win"]: f"{max(pnls):.2f}",
            names["max_loss"]: f"{min(pnls):.2f}",
            "Max_Drawdown": f"{max_dd:.2f}",
            "Days_of_Max_Drawdown": dd_label,
            "ReturnPerMaxDD": f"{return_per_max_dd:.2f}",
            "RewardToRiskRatio": f"{reward_to_risk:.2f}",
            "ExpectancyRatio": f"{expectancy_ratio:.2f}",
            "MaxWiningStreak": str(max_win_streak),
            "MaxLossingStreak": str(max_loss_streak),
            "MaxTradesInDrawdown": str(trades_in_dd),
        }

    def _max_drawdown_stats(self, pnls: list, dates: list):
        """Peak-to-trough on the cumulative pnl curve. Returns
        (max_drawdown, 'N [start to end]' label, trade_count_in_drawdown)."""
        cum = np.cumsum(pnls)
        running_peak = np.maximum.accumulate(cum)
        drawdown = cum - running_peak

        trough_idx = int(np.argmin(drawdown))
        max_dd = float(drawdown[trough_idx])
        if max_dd == 0:
            return 0.0, f"0 [{dates[0]} to {dates[0]}]", 0

        peak_idx = int(np.where(cum[:trough_idx + 1] == running_peak[trough_idx])[0][-1])
        calendar_days = (dates[trough_idx] - dates[peak_idx]).days
        trades_in_dd = trough_idx - peak_idx
        return max_dd, f"{calendar_days} [{dates[peak_idx]} to {dates[trough_idx]}]", trades_in_dd

    def _max_streaks(self, pnls: list):
        max_win_streak = max_loss_streak = cur_win = cur_loss = 0
        for p in pnls:
            if p > 0:
                cur_win += 1
                cur_loss = 0
            elif p < 0:
                cur_loss += 1
                cur_win = 0
            else:
                cur_win = cur_loss = 0
            max_win_streak = max(max_win_streak, cur_win)
            max_loss_streak = max(max_loss_streak, cur_loss)
        return max_win_streak, max_loss_streak