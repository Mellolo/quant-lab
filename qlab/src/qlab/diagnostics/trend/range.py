"""震荡：只在方向为 0 时评。看是不是在磨、收盘还在这段高低点里。"""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlab.core.price_panels import atr_panel
from qlab.diagnostics.trend.common import empty_like, run_age, wide_ohlc
from qlab.diagnostics.trend.direction import st_direction_panels

RANGE_KEYS = ("range_ok", "range_edge", "range_width_atr")


def _er(close: np.ndarray, start: int, i: int) -> float:
    if i <= start:
        return np.nan
    net = abs(float(close[i]) - float(close[start]))
    path = float(np.nansum(np.abs(np.diff(close[start : i + 1]))))
    if path <= 1e-12:
        return 0.0
    return float(net / path)


def range_one(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    direction: np.ndarray,
    atr: np.ndarray,
    *,
    min_age: int = 10,
) -> dict[str, np.ndarray]:
    """方向为 0 的箱子：ER≤0.5 且收盘仍在这段高低点内。"""
    n = len(close)
    age = run_age(direction, stay_zero=True)
    out = {k: np.full(n, np.nan) for k in RANGE_KEYS}

    for i in range(n):
        a = age[i]
        if not np.isfinite(a):
            continue
        start = i - min(int(a) + 1, 40) + 1
        hh = float(np.nanmax(high[start : i + 1]))
        ll = float(np.nanmin(low[start : i + 1]))
        span = hh - ll
        atr_i = float(atr[i]) if np.isfinite(atr[i]) and atr[i] > 0 else np.nan
        er = _er(close, start, i)
        if np.isfinite(atr_i) and span > 0:
            out["range_width_atr"][i] = span / atr_i
        if span > 1e-12 and np.isfinite(close[i]):
            loc = (float(close[i]) - ll) / span
            out["range_edge"][i] = 1.0 if loc >= 2.0 / 3.0 else (-1.0 if loc <= 1.0 / 3.0 else 0.0)
        inside = np.isfinite(close[i]) and ll <= float(close[i]) <= hh
        out["range_ok"][i] = float(a >= min_age and np.isfinite(er) and er <= 0.50 and inside)
    return out


def range_panels(
    close: pd.DataFrame,
    high: pd.DataFrame | None = None,
    low: pd.DataFrame | None = None,
    *,
    direction: pd.DataFrame | None = None,
    dir_hold: int = 2,
    min_age: int = 10,
) -> dict[str, pd.DataFrame]:
    """震荡箱子。方向非 0 的日子为空。"""
    close, high, low = wide_ohlc(close, high, low)
    if direction is None:
        direction = st_direction_panels(close, high, low, dir_hold=dir_hold)
    else:
        direction = direction.reindex_like(close).astype(float)
    atr = atr_panel(high, low, close, window=14)
    out = {k: empty_like(close) for k in RANGE_KEYS}
    for col in close.columns:
        one = range_one(
            high[col].to_numpy(dtype=float),
            low[col].to_numpy(dtype=float),
            close[col].to_numpy(dtype=float),
            direction[col].to_numpy(dtype=float),
            atr[col].to_numpy(dtype=float),
            min_age=min_age,
        )
        for k, arr in one.items():
            out[k][col] = arr
    return out
