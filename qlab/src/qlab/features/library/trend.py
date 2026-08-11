"""趋势阶段 / 结构类特征 — Weinstein Stage、Trend Template 量化版."""

from __future__ import annotations

import pandas as pd

from qlab.core.price_panels import dist_to_high_panel, stage_panel
from qlab.features.base import DailyFeature, FeatureMeta
from qlab.features.context import FeatureContext
from qlab.features.registry import registry


class StageLabel(DailyFeature):
    """Weinstein 四阶段标签 ∈ {1,2,3,4}（量化近似）.

    长期均线默认 200 日（书中 30 周均线的日频近似）。
    """

    def __init__(self, ma_window: int = 200, slope_lookback: int = 20):
        self.ma_window = ma_window
        self.slope_lookback = slope_lookback
        self.meta = FeatureMeta(
            name=f"stage_{ma_window}d",
            version="1.0",
            lookback_days=ma_window + slope_lookback,
            available_at="today_close",
            description=f"Weinstein Stage 1–4（MA{ma_window}+斜率{slope_lookback}d）",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        close = ctx.daily(
            ["close"], lookback_days=self.ma_window + self.slope_lookback
        )["close"].unstack("symbol")
        stage = stage_panel(
            close, ma_window=self.ma_window, slope_lookback=self.slope_lookback
        )
        return stage.stack(future_stack=True).rename(self.meta.name)


class IsStage2(DailyFeature):
    """是否 Stage 2（0/1）— 可作采样硬过滤或模型特征."""

    def __init__(self, ma_window: int = 200, slope_lookback: int = 20):
        self.ma_window = ma_window
        self.slope_lookback = slope_lookback
        dep = f"stage_{ma_window}d"
        self.meta = FeatureMeta(
            name=f"is_stage2_{ma_window}d",
            version="1.0",
            lookback_days=ma_window + slope_lookback,
            available_at="today_close",
            dependencies=(dep,),
            description="1 if Weinstein Stage 2 else 0",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        stage = ctx.upstream(f"stage_{self.ma_window}d")
        return (stage == 2.0).astype("float64").rename(self.meta.name)


class DistToHigh(DailyFeature):
    """距 N 日高点的距离: close / rolling_max - 1（0=新高）."""

    def __init__(self, window: int = 252):
        if window < 2:
            raise ValueError("window 必须 >= 2")
        self.window = window
        self.meta = FeatureMeta(
            name=f"dist_high_{window}d",
            version="1.0",
            lookback_days=window,
            available_at="today_close",
            description=f"close / {window}d high - 1（越接近 0 越靠近高点）",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        close = ctx.daily(["close"], lookback_days=self.window)["close"].unstack(
            "symbol"
        )
        dist = dist_to_high_panel(close, window=self.window)
        return dist.stack(future_stack=True).rename(self.meta.name)


registry.register(instance=StageLabel())
registry.register(instance=IsStage2())
registry.register(instance=DistToHigh(252))
registry.register(instance=DistToHigh(60))
