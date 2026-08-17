"""资金博弈特征 — 仓位强弱，独立于趋势方向/效率。"""

from __future__ import annotations

import pandas as pd

from qlab.diagnostics.flow import flow_panels
from qlab.features.base import DailyFeature, FeatureMeta
from qlab.features.context import FeatureContext
from qlab.features.registry import registry


class FlowHold(DailyFeature):
    """仓位 ∈ [0,1]：资金博弈（区间强弱 / 油尽）。"""

    def __init__(self):
        self.meta = FeatureMeta(
            name="flow_hold",
            version="1.0",
            lookback_days=260,
            available_at="today_close",
            description="资金博弈仓位分（diagnostics.flow）",
        )

    def compute(self, ctx: FeatureContext) -> pd.Series:
        df = ctx.daily(lookback_days=260)
        volume = df["volume"].unstack("symbol") if "volume" in df.columns else None
        float_shares = (
            df["float_shares"].unstack("symbol") if "float_shares" in df.columns else None
        )
        panels = flow_panels(
            df["close"].unstack("symbol"),
            high=df["high"].unstack("symbol"),
            low=df["low"].unstack("symbol"),
            volume=volume,
            float_shares=float_shares,
        )
        return panels["hold"].stack(future_stack=True).rename(self.meta.name)


registry.register(instance=FlowHold())
