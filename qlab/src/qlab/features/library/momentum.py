"""动量类特征."""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlab.features.base import DailyFeature, FeatureMeta
from qlab.features.context import FeatureContext
from qlab.features.registry import registry


class Momentum(DailyFeature):
    """N 日动量（log return）."""

    def __init__(self, window: int = 5):
        self.window = window
        self.meta = FeatureMeta(
            name=f"mom_{window}d",
            version="1.0",
            lookback_days=window + 1,
            available_at="today_close",
            description=f"过去 {window} 日的对数收益",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        df = ctx.daily(["close"], lookback_days=self.window + 1)
        close = df["close"].unstack("symbol")
        log_close = np.log(close)
        mom = log_close - log_close.shift(self.window)
        return mom.stack(future_stack=True).rename(self.meta.name)


registry.register(instance=Momentum(5))
registry.register(instance=Momentum(10))
registry.register(instance=Momentum(20))


class MomentumResidual(DailyFeature):
    """剔除均值后的动量（横截面 demean）."""

    def __init__(self, window: int = 20):
        self.window = window
        self.meta = FeatureMeta(
            name=f"mom_resid_{window}d",
            version="1.0",
            lookback_days=window + 1,
            available_at="today_close",
            dependencies=(f"mom_{window}d",),
            description=f"{window} 日动量减去当日横截面均值",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        mom = ctx.upstream(f"mom_{self.window}d")
        # 横截面 demean
        mom_mean = mom.groupby(level="date").transform("mean")
        return (mom - mom_mean).rename(self.meta.name)


registry.register(instance=MomentumResidual(20))
