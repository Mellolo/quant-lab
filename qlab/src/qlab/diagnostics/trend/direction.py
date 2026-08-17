"""方向：双 SuperTrend 生方向，这条路径自己吐回太多则置 0。"""

from __future__ import annotations

import pandas as pd

from qlab.core.price_panels import dual_supertrend_panels
from qlab.diagnostics.trend.common import hysteresis_direction, wide_ohlc

RETAIN_GATE = 0.50
RETAIN_MIN_AGE = 5


def st_direction_panels(
    close: pd.DataFrame,
    high: pd.DataFrame | None = None,
    low: pd.DataFrame | None = None,
    *,
    dir_hold: int = 2,
    st_atr: int = 10,
    st_mult_slow: float = 3.0,
    st_mult_fast: float = 2.0,
) -> pd.DataFrame:
    """慢轨 ATR×3、快轨 ATR×2；同向才非 0。切换需连续 ``dir_hold`` 根。"""
    close, high, low = wide_ohlc(close, high, low)
    dual = dual_supertrend_panels(
        high, low, close, atr_window=st_atr, slow=st_mult_slow, fast=st_mult_fast
    )
    slow = dual["slow"]["direction"]
    fast = dual["fast"]["direction"]
    agree = slow.notna() & fast.notna() & slow.eq(fast)
    raw = slow.where(agree, 0.0).fillna(0.0)
    return hysteresis_direction(raw, hold=dir_hold)


def gate_direction(
    raw: pd.DataFrame,
    retention: pd.DataFrame,
    origin_age: pd.DataFrame,
    *,
    thresh: float = RETAIN_GATE,
    min_age: int = RETAIN_MIN_AGE,
    hold: int = 1,
) -> pd.DataFrame:
    """起点龄够了且这条路径的留存 < ``thresh`` → 0。"""
    raw = raw.astype(float)
    keep = retention.reindex_like(raw)
    age = origin_age.reindex_like(raw)
    weak = (age >= float(min_age)) & keep.notna() & (keep < float(thresh))
    return hysteresis_direction(raw.where(~weak, 0.0), hold=hold)
