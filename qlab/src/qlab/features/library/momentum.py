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


class SmoothMomentum(DailyFeature):
    """平滑动量（Clenow）: 年化 log 价格斜率 × R².

    对过去 ``window`` 日的 ln(close) 做线性回归，得分 =
    ``(exp(slope × 252) - 1) × R²``。惩罚靠尖峰堆出的假动量。
    宽表算法见 :func:`~qlab.core.price_panels.smooth_momentum_panel`。
    """

    def __init__(self, window: int = 90):
        if window < 5:
            raise ValueError("window 必须 >= 5")
        self.window = window
        self.meta = FeatureMeta(
            name=f"smooth_mom_{window}d",
            version="1.0",
            lookback_days=window,
            available_at="today_close",
            description=f"过去 {window} 日 ln(price) 回归年化斜率 × R²（Clenow）",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        from qlab.core.price_panels import smooth_momentum_panel

        close = ctx.daily(["close"], lookback_days=self.window)["close"].unstack(
            "symbol"
        )
        out = smooth_momentum_panel(close, window=self.window)
        return out.stack(future_stack=True).rename(self.meta.name)


registry.register(instance=SmoothMomentum(90))
registry.register(instance=SmoothMomentum(60))
