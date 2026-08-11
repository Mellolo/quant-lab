"""宽表价量面板算法 — 特征与采样门共用，避免双份实现漂移.

``core/`` 不依赖 features/labeling；下游各自封装 API。
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def stage_panel(
    close: pd.DataFrame,
    *,
    ma_window: int = 200,
    slope_lookback: int = 20,
) -> pd.DataFrame:
    """机械四阶段标签面板（列=symbol）.

    Stage 2: 价在 MA 上且 MA 上升 —— 唯一理想做多区
    Stage 4: 价在 MA 下且 MA 下降
    Stage 3: 价在 MA 上但 MA 未升（见顶/派发近似）
    Stage 1: 其余（筑底 / 吸筹近似）
    """
    close = close.sort_index().astype(float)
    ma = close.rolling(ma_window, min_periods=ma_window).mean()
    prev = ma.shift(slope_lookback)
    slope = (ma - prev) / prev.replace(0, np.nan)
    above = close > ma
    rising = slope > 0

    stage = pd.DataFrame(1, index=close.index, columns=close.columns, dtype="float64")
    stage = stage.mask(above & rising, 2.0)
    stage = stage.mask(above & ~rising, 3.0)
    stage = stage.mask(~above & ~rising, 4.0)
    return stage.where(ma.notna() & slope.notna())


def is_stage2_panel(
    close: pd.DataFrame,
    *,
    ma_window: int = 200,
    slope_lookback: int = 20,
) -> pd.DataFrame:
    """Stage2 bool 宽表."""
    stage = stage_panel(
        close, ma_window=ma_window, slope_lookback=slope_lookback
    )
    return stage.eq(2.0)


def dist_to_high_panel(close: pd.DataFrame, *, window: int = 60) -> pd.DataFrame:
    """``close / rolling_max - 1``（0=新高）."""
    close = close.sort_index().astype(float)
    hi = close.rolling(window, min_periods=window).max()
    return close / hi.replace(0, np.nan) - 1.0


def smooth_momentum_panel(close: pd.DataFrame, *, window: int = 60) -> pd.DataFrame:
    """Clenow 平滑动量: 年化 log 斜率 × R²."""
    if window < 5:
        raise ValueError("window 必须 >= 5")
    log_c = np.log(close.sort_index().astype(float))
    x = np.arange(window, dtype=float)
    x = x - x.mean()
    ss_x = float((x ** 2).sum())

    def _score(y: np.ndarray) -> float:
        if y.shape[0] != window or np.isnan(y).any():
            return np.nan
        y = y.astype(float)
        y0 = y - y.mean()
        slope = float((x * y0).sum() / ss_x)
        yhat = slope * x + y.mean()
        ss_res = float(((y - yhat) ** 2).sum())
        ss_tot = float((y0 ** 2).sum())
        if ss_tot <= 0:
            return 0.0
        r2 = 1.0 - ss_res / ss_tot
        annualized = float(np.exp(slope * 252.0) - 1.0)
        return annualized * max(r2, 0.0)

    return log_c.rolling(window).apply(_score, raw=True)
