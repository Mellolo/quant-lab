"""趋势阶段 / 结构类特征 — Weinstein Stage、Trend Template、诊断五轴."""

from __future__ import annotations

import pandas as pd

from qlab.core.price_panels import dist_to_high_panel, stage_panel
from qlab.diagnostics.trend import trend_panels
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


def _trend_panel_field(
    ctx: FeatureContext, field: str, *, lookback_days: int = 260
) -> pd.Series:
    """从日线宽表算 trend_panels 并取单字段 Series."""
    df = ctx.daily(["close", "high", "low"], lookback_days=lookback_days)
    close = df["close"].unstack("symbol")
    high = df["high"].unstack("symbol")
    low = df["low"].unstack("symbol")
    panels = trend_panels(
        close, high=high, low=low, volume=None, include_xs_rank=False
    )
    return panels[field].stack(future_stack=True)


class TrendDirection(DailyFeature):
    """趋势方向 ∈ {-1, 0, +1}（诊断模块汇总方向）."""

    def __init__(self):
        self.meta = FeatureMeta(
            name="trend_direction",
            version="1.0",
            lookback_days=260,
            available_at="today_close",
            description="市场结构+Stage 汇总方向（diagnostics.trend）",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        return _trend_panel_field(ctx, "direction").rename(self.meta.name)


class TrendStrength(DailyFeature):
    """趋势强度 ∈ [0,1]（滚动分位合成）."""

    def __init__(self):
        self.meta = FeatureMeta(
            name="trend_strength",
            version="1.0",
            lookback_days=260,
            available_at="today_close",
            description="smooth mom + 结构扩展比的滚动分位强度",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        return _trend_panel_field(ctx, "strength").rename(self.meta.name)


class TrendPhase(DailyFeature):
    """趋势相位码：range=0, early=1, mid=2, late=3."""

    def __init__(self):
        self.meta = FeatureMeta(
            name="trend_phase",
            version="1.0",
            lookback_days=260,
            available_at="today_close",
            description="趋势相位码（PHASE_CODE）",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        return _trend_panel_field(ctx, "phase_code").rename(self.meta.name)


class TrendRisk(DailyFeature):
    """趋势风险 ∈ [0,1]."""

    def __init__(self):
        self.meta = FeatureMeta(
            name="trend_risk",
            version="1.0",
            lookback_days=260,
            available_at="today_close",
            description="CHoCH/Stage3/贴高滞涨等风险分",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        return _trend_panel_field(ctx, "risk").rename(self.meta.name)


registry.register(instance=TrendDirection())
registry.register(instance=TrendStrength())
registry.register(instance=TrendPhase())
registry.register(instance=TrendRisk())
