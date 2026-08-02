"""日内派生特征 — 用过去 N 日的分钟数据算出一个日级值."""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlab.core.enums import Freq
from qlab.features.base import FeatureMeta, IntradayDerivedFeature
from qlab.features.context import FeatureContext
from qlab.features.registry import registry


def _compute_vol_slope(intraday_window: pd.DataFrame) -> float:
    """日内波动率的"上升/下降"斜率.

    把窗口内的分钟收益率方差按时间分桶，回归斜率。
    """
    if len(intraday_window) < 10:
        return np.nan
    log_ret = np.log(intraday_window["close"]).diff().dropna()
    if len(log_ret) < 10:
        return np.nan
    # 滚动 10 分钟方差
    rolling_var = log_ret.rolling(10).var().dropna()
    if len(rolling_var) < 2:
        return np.nan
    x = np.arange(len(rolling_var))
    y = rolling_var.values
    # 线性回归斜率
    slope = np.polyfit(x, y, 1)[0]
    return float(slope)


class IntradayVolSlope(IntradayDerivedFeature):
    """日内波动率斜率（过去 N 日）."""

    def __init__(self, lookback_days: int = 5, freq: Freq = Freq.MIN_30):
        self.lookback_days = lookback_days
        self.freq = freq
        self.meta = FeatureMeta(
            name=f"intraday_vol_slope_{lookback_days}d",
            version="1.0",
            lookback_days=lookback_days,
            available_at="next_open",  # 需要全日收盘后才能算
            requires_intraday=True,
            intraday_freq=freq,
            description="日内分钟方差对时间的回归斜率（衡量日内波动是否递增）",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        return ctx.intraday_rolling(
            compute_fn=_compute_vol_slope,
            lookback_days=self.lookback_days,
            freq=self.freq,
        ).rename(self.meta.name)


registry.register(instance=IntradayVolSlope(lookback_days=5))


def _morning_return(intraday_window: pd.DataFrame) -> float:
    """上午（9:30-11:30）累计 log-return."""
    if "session" in intraday_window.columns:
        morning = intraday_window[intraday_window["session"] == "morning"]
    else:
        # 用时间筛选
        ts = intraday_window.index.get_level_values("timestamp") if isinstance(intraday_window.index, pd.MultiIndex) else intraday_window.index
        mask = (ts.hour == 9) | (ts.hour == 10) | ((ts.hour == 11) & (ts.minute <= 30))
        morning = intraday_window[mask]
    if len(morning) < 2:
        return np.nan
    closes = morning["close"].dropna()
    if len(closes) < 2:
        return np.nan
    return float(np.log(closes.iloc[-1]) - np.log(closes.iloc[0]))


class MorningReturn(IntradayDerivedFeature):
    """当日上午累计收益."""

    def __init__(self, freq: Freq = Freq.MIN_30):
        self.freq = freq
        self.meta = FeatureMeta(
            name="morning_return",
            version="1.0",
            lookback_days=1,
            available_at="today_close",  # 11:30 后就可算，今日收盘前可用
            requires_intraday=True,
            intraday_freq=freq,
            description="9:30-11:30 累计 log-return",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        return ctx.intraday_rolling(
            compute_fn=_morning_return,
            lookback_days=1,
            freq=self.freq,
        ).rename(self.meta.name)


registry.register(instance=MorningReturn())
