"""价格类特征：含分数阶差分（书 Ch5 核心创新）."""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlab.features.base import DailyFeature, FeatureMeta
from qlab.features.context import FeatureContext
from qlab.features.registry import registry


class LogPrice(DailyFeature):
    """log(close)."""

    meta = FeatureMeta(
        name="log_close", version="1.0", lookback_days=1,
        available_at="today_close", description="log close price",
    )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        df = ctx.daily(["close"], lookback_days=1)
        return np.log(df["close"]).rename(self.meta.name)


registry.register(LogPrice)


class PriceMA(DailyFeature):
    """N 日收盘价均线."""

    def __init__(self, window: int = 20):
        self.window = window
        self.meta = FeatureMeta(
            name=f"ma_{window}d", version="1.0",
            lookback_days=window, available_at="today_close",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        df = ctx.daily(["close"], lookback_days=self.window)
        close = df["close"].unstack("symbol")
        ma = close.rolling(self.window).mean()
        return ma.stack(future_stack=True).rename(self.meta.name)


registry.register(instance=PriceMA(window=20))


# ============================================================
# 分数阶差分（Ch5）
# ============================================================


def _frac_diff_weights(d: float, threshold: float = 1e-5) -> np.ndarray:
    """FFD 权重序列（截断到 |w| < threshold）."""
    weights = [1.0]
    k = 1
    while True:
        w = -weights[-1] / k * (d - k + 1)
        if abs(w) < threshold:
            break
        weights.append(w)
        k += 1
        if k > 10000:  # 安全上限
            break
    return np.array(weights[::-1])


class FractionalDiff(DailyFeature):
    """Fixed-Width Window Fracdiff（书 Ch5）.

    在 log close 上应用 FFD，保留长记忆同时达成平稳性。
    """

    def __init__(self, d: float = 0.4, threshold: float = 1e-5):
        self.d = d
        self.threshold = threshold
        self._weights = _frac_diff_weights(d, threshold)
        self.meta = FeatureMeta(
            name=f"ffd_d{d}",
            version="1.0",
            lookback_days=len(self._weights),
            available_at="today_close",
            description=f"Fractional differentiation d={d} on log close",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        df = ctx.daily(["close"], lookback_days=len(self._weights))
        log_close = np.log(df["close"]).unstack("symbol")
        weights = self._weights
        width = len(weights)

        out = pd.DataFrame(index=log_close.index, columns=log_close.columns, dtype=float)
        for symbol in log_close.columns:
            series = log_close[symbol].dropna()
            if len(series) < width:
                continue
            values = series.values
            for i in range(width - 1, len(values)):
                out.loc[series.index[i], symbol] = float(
                    np.dot(weights, values[i - width + 1: i + 1])
                )
        return out.stack(future_stack=True).rename(self.meta.name)


registry.register(instance=FractionalDiff(d=0.4))
