"""开盘集合竞价派生特征 — available_at=today_open."""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlab.features.base import DailyFeature, FeatureMeta
from qlab.features.context import FeatureContext
from qlab.features.registry import registry


class AuctionPremium(DailyFeature):
    """竞价溢价: auction_price / 昨收 - 1.

    09:25 竞价结束即确定，T 日开盘决策可用（``available_at='today_open'``）。
    """

    def __init__(self) -> None:
        self.meta = FeatureMeta(
            name="auction_premium",
            version="1.0",
            lookback_days=2,
            available_at="today_open",
            description="开盘竞价价相对昨收的溢价 (auction/prev_close - 1)",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        ca = ctx.call_auction()
        if ca.empty or "auction_price" not in ca.columns:
            idx = pd.MultiIndex.from_product(
                [ctx.target_dates, ctx.universe.all_symbols()],
                names=["date", "symbol"],
            )
            return pd.Series(np.nan, index=idx, name=self.meta.name)

        close = ctx.daily(["close"], lookback_days=2)["close"]
        prev_close = close.groupby(level="symbol").shift(1)
        px = ca["auction_price"].astype("float64")
        prev = prev_close.reindex(px.index)
        out = (px / prev - 1.0).replace([np.inf, -np.inf], np.nan)
        return out.rename(self.meta.name)


class AuctionVolumeRatio(DailyFeature):
    """竞价量比: auction_volume / 昨成交量.

    衡量开盘竞价相对前一日的参与热度；同样 ``today_open``。
    """

    def __init__(self) -> None:
        self.meta = FeatureMeta(
            name="auction_vol_ratio",
            version="1.0",
            lookback_days=2,
            available_at="today_open",
            description="开盘竞价量 / 昨日成交量",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        ca = ctx.call_auction()
        if ca.empty or "auction_volume" not in ca.columns:
            idx = pd.MultiIndex.from_product(
                [ctx.target_dates, ctx.universe.all_symbols()],
                names=["date", "symbol"],
            )
            return pd.Series(np.nan, index=idx, name=self.meta.name)

        vol = ctx.daily(["volume"], lookback_days=2)["volume"].astype("float64")
        prev_vol = vol.groupby(level="symbol").shift(1)
        av = ca["auction_volume"].astype("float64")
        prev = prev_vol.reindex(av.index).replace(0, np.nan)
        out = (av / prev).replace([np.inf, -np.inf], np.nan)
        return out.rename(self.meta.name)


registry.register(instance=AuctionPremium())
registry.register(instance=AuctionVolumeRatio())
