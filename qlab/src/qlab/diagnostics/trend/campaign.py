"""有方向时：折线找起点，再给这段打分。"""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlab.core.price_panels import atr_panel, cross_sectional_rank_panel
from qlab.diagnostics.trend.common import empty_like, run_age, wide_ohlc
from qlab.diagnostics.trend.direction import st_direction_panels
from qlab.diagnostics.trend.origin import SWING_LEFT_RIGHT, _iter_campaigns
from qlab.diagnostics.trend.score import (
    _efficiency,
    _open_or_ungapped,
    _overnight_efficiency,
    _retention,
    _session_efficiency,
)

CAMPAIGN_KEYS = (
    "origin",
    "origin_age",
    "efficiency",
    "overnight_efficiency",
    "session_efficiency",
    "retention",
)


def _metrics_one(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    direction: np.ndarray,
    age: np.ndarray,
    atr: np.ndarray,
    *,
    open_: np.ndarray | None = None,
    left_right: int,
) -> dict[str, np.ndarray]:
    n = len(close)
    open_ = _open_or_ungapped(open_, close)
    out = {k: np.full(n, np.nan) for k in CAMPAIGN_KEYS}
    for i, oi, op, _zig in _iter_campaigns(
        high, low, direction, age, atr, left_right=left_right
    ):
        d = direction[i]
        out["origin"][i] = op
        out["origin_age"][i] = float(i - oi)
        out["efficiency"][i] = _efficiency(close, oi, i, d)
        out["overnight_efficiency"][i] = _overnight_efficiency(open_, close, oi, i, d)
        out["session_efficiency"][i] = _session_efficiency(open_, close, oi, i, d)
        out["retention"][i] = _retention(close, oi, i, d, op)
    return out


def campaign_panels(
    close: pd.DataFrame,
    high: pd.DataFrame | None = None,
    low: pd.DataFrame | None = None,
    *,
    open_: pd.DataFrame | None = None,
    direction: pd.DataFrame | None = None,
    dir_hold: int = 2,
    left_right: int = SWING_LEFT_RIGHT,
    include_xs_rank: bool = False,
) -> dict[str, pd.DataFrame]:
    """起点用折线；效率拆收盘 / 隔夜 / 盘中；留存看这段自己还留多少。方向为 0 时为空。"""
    close, high, low = wide_ohlc(close, high, low)
    if open_ is not None:
        open_ = open_.reindex_like(close).astype(float)
    if direction is None:
        direction = st_direction_panels(close, high, low, dir_hold=dir_hold)
    else:
        direction = direction.reindex_like(close).astype(float)
    atr = atr_panel(high, low, close, window=14)
    out = {k: empty_like(close) for k in CAMPAIGN_KEYS}
    for col in close.columns:
        dcol = direction[col].to_numpy(dtype=float)
        one = _metrics_one(
            high[col].to_numpy(dtype=float),
            low[col].to_numpy(dtype=float),
            close[col].to_numpy(dtype=float),
            dcol,
            run_age(dcol),
            atr[col].to_numpy(dtype=float),
            open_=None if open_ is None else open_[col].to_numpy(dtype=float),
            left_right=left_right,
        )
        for key, arr in one.items():
            out[key][col] = arr
    if include_xs_rank:
        out["efficiency_xs"] = cross_sectional_rank_panel(out["efficiency"].fillna(0.0))
    return out
