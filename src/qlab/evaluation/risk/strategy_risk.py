"""策略风险 — 书 Ch15.

策略风险 ≠ 组合风险.
组合风险：持仓波动；策略风险：策略长期失败的概率 P[p < p_θ*].
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats


def implied_precision(
    sl: float,
    pt: float,
    freq: int,
    target_sr: float,
) -> float:
    """给定 {sl, pt, freq, target_sr}，反推所需最小精度 p — 书 Ch15 Snippet 15.3.

    sl < 0, pt > 0
    """
    a = (freq + target_sr ** 2) * (pt - sl) ** 2
    b = (2 * freq * sl - target_sr ** 2 * (pt - sl)) * (pt - sl)
    c = freq * sl ** 2
    disc = b ** 2 - 4 * a * c
    if disc < 0:
        return float("nan")
    return float((-b + disc ** 0.5) / (2.0 * a))


def binary_implied_freq(
    sl: float,
    pt: float,
    p: float,
    target_sr: float,
) -> float:
    """给定精度，反推所需下注频率 — 书 Ch15 Snippet 15.4."""
    if p <= 0 or p >= 1:
        return float("nan")
    return (target_sr * (pt - sl)) ** 2 * p * (1 - p) / ((pt - sl) * p + sl) ** 2


def prob_strategy_failure(
    returns: pd.Series,
    freq: int,
    target_sr: float,
) -> float:
    """策略失败概率 P[p < p_{θ*}] — 书 Ch15 Snippet 15.5.

    用正态近似 bootstrap.
    """
    pos = returns[returns > 0]
    neg = returns[returns <= 0]
    if len(pos) == 0 or len(neg) == 0:
        return float("nan")

    pt = float(pos.mean())
    sl = float(neg.mean())
    p = len(pos) / len(returns)

    thres_p = implied_precision(sl, pt, freq, target_sr)
    if np.isnan(thres_p):
        return float("nan")

    var = p * (1 - p)
    if var <= 0:
        return float("nan")
    risk = stats.norm.cdf(thres_p, loc=p, scale=var)
    return float(risk)
