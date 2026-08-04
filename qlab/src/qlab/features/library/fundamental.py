"""基本面派生因子：PE / PB / ROE 等.

设计：派生比率不在 data/，由 features/ 用行情 × 财务 × 股本组合计算。
"""

from __future__ import annotations

import pandas as pd

from qlab.features.base import DailyFeature, FeatureMeta
from qlab.features.context import FeatureContext
from qlab.features.registry import registry


class PE_TTM(DailyFeature):
    """PE (TTM) = 总市值 / TTM 净利润 (归母)."""

    meta = FeatureMeta(
        name="pe_ttm", version="1.0",
        lookback_days=400,  # TTM 需要至少 1 年财报历史
        available_at="today_close",
        description="Price-to-Earnings (TTM, 归母)",
    )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        df = ctx.daily(["close_raw", "total_shares"], lookback_days=1)
        # 用原始价 + 总股本算市值
        market_cap = df["close_raw"] * df["total_shares"]
        # TTM 净利润（每个日期单独算）
        ttm_profit = ctx.fundamental("net_profit_to_shareholders", ttm=True)
        # 对齐 index
        merged = pd.concat([market_cap.rename("mcap"),
                            ttm_profit.rename("profit_ttm")], axis=1)
        pe = merged["mcap"] / merged["profit_ttm"].replace(0, pd.NA)
        return pe.rename(self.meta.name)


registry.register(PE_TTM)


class PB(DailyFeature):
    """PB = 总市值 / 归母股东权益."""

    meta = FeatureMeta(
        name="pb", version="1.0",
        lookback_days=200,
        available_at="today_close",
        description="Price-to-Book (归母)",
    )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        df = ctx.daily(["close_raw", "total_shares"], lookback_days=1)
        market_cap = df["close_raw"] * df["total_shares"]
        equity = ctx.fundamental("equity_to_shareholders", ttm=False)
        merged = pd.concat([market_cap.rename("mcap"),
                            equity.rename("equity")], axis=1)
        pb = merged["mcap"] / merged["equity"].replace(0, pd.NA)
        return pb.rename(self.meta.name)


registry.register(PB)
