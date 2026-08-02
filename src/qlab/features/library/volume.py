"""量价类特征."""

from __future__ import annotations

import pandas as pd

from qlab.features.base import DailyFeature, FeatureMeta
from qlab.features.context import FeatureContext
from qlab.features.registry import registry


class TurnoverRatio(DailyFeature):
    """换手率 = volume / float_shares."""

    def __init__(self, window: int = 5):
        self.window = window
        self.meta = FeatureMeta(
            name=f"turnover_{window}d",
            version="1.0",
            lookback_days=window,
            available_at="today_close",
            description=f"过去 {window} 日平均换手率",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        df = ctx.daily(["volume", "float_shares"], lookback_days=self.window)
        # 换手率 = volume / float_shares（按日）
        turnover = df["volume"] / df["float_shares"].replace(0, pd.NA)
        # 滚动平均
        turnover_wide = turnover.unstack("symbol")
        result = turnover_wide.rolling(self.window).mean()
        return result.stack(future_stack=True).rename(self.meta.name)


registry.register(instance=TurnoverRatio(window=5))
registry.register(instance=TurnoverRatio(window=20))


class VolumeRatio(DailyFeature):
    """量比 = 当日成交量 / 过去 N 日平均成交量."""

    def __init__(self, window: int = 5):
        self.window = window
        self.meta = FeatureMeta(
            name=f"vol_ratio_{window}d",
            version="1.0",
            lookback_days=window + 1,
            available_at="today_close",
            description=f"当日量 / 过去 {window} 日平均量",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        df = ctx.daily(["volume"], lookback_days=self.window + 1)
        vol = df["volume"].unstack("symbol").astype(float)
        avg_vol = vol.shift(1).rolling(self.window).mean()
        ratio = vol / avg_vol.replace(0, pd.NA)
        return ratio.stack(future_stack=True).rename(self.meta.name)


registry.register(instance=VolumeRatio(window=5))
