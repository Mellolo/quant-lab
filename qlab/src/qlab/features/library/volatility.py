"""波动率类特征."""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlab.features.base import DailyFeature, FeatureMeta
from qlab.features.context import FeatureContext
from qlab.features.registry import registry


class EwmVol(DailyFeature):
    """EWM 日波动率（书中 Ch3 推荐用于动态阈值）."""

    def __init__(self, span: int = 100):
        self.span = span
        self.meta = FeatureMeta(
            name=f"ewm_vol_{span}d",
            version="1.0",
            lookback_days=span,
            available_at="today_close",
            description=f"span={span} 的 EWM std of log returns",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        df = ctx.daily(["close"], lookback_days=self.span)
        close = df["close"].unstack("symbol")
        log_ret = np.log(close).diff()
        vol = log_ret.ewm(span=self.span, min_periods=10).std()
        return vol.stack(future_stack=True).rename(self.meta.name)


registry.register(instance=EwmVol(span=20))
registry.register(instance=EwmVol(span=100))


class RealizedVol(DailyFeature):
    """rolling 已实现波动率."""

    def __init__(self, window: int = 20):
        self.window = window
        self.meta = FeatureMeta(
            name=f"rv_{window}d",
            version="1.0",
            lookback_days=window + 1,
            available_at="today_close",
            description=f"过去 {window} 日 log-return 的样本标准差",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        df = ctx.daily(["close"], lookback_days=self.window + 1)
        close = df["close"].unstack("symbol")
        log_ret = np.log(close).diff()
        vol = log_ret.rolling(self.window).std()
        return vol.stack(future_stack=True).rename(self.meta.name)


registry.register(instance=RealizedVol(window=20))
