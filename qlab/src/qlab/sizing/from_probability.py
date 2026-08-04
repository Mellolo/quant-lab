"""从预测概率到下注规模 — 书 Ch10 §10.3-10.5."""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import norm


def bet_size_from_probability(
    prob: pd.Series | np.ndarray,
    pred: pd.Series | np.ndarray | int,
    num_classes: int = 2,
    side: pd.Series | np.ndarray | None = None,
) -> pd.Series | np.ndarray:
    """根据预测概率计算下注规模 ∈ [-1, 1] — 书 Ch10 Snippet 10.1.

    二分类: m = 2 * Z[(2p - 1) / (2*sqrt(p*(1-p)))] - 1
    多分类: one-vs-rest, t-stat 对最大类概率

    参数
    ----
    prob : 最大类的概率
    pred : 预测类别（决定 size 的符号）
    num_classes : 类别数
    side : meta-labeling 情形下，主模型给的方向（+1/-1）
    """
    p = prob.values if isinstance(prob, pd.Series) else np.asarray(prob)

    # 防 0/1 边界
    p = np.clip(p, 1e-6, 1 - 1e-6)

    z = (p - 1.0 / num_classes) / np.sqrt(p * (1 - p))
    size = 2 * norm.cdf(z) - 1  # 0..1
    # 乘以方向
    signed_size = size * pred if isinstance(pred, (int, float)) else size * np.asarray(pred)

    if side is not None:
        side_arr = side.values if isinstance(side, pd.Series) else np.asarray(side)
        signed_size = signed_size * side_arr

    if isinstance(prob, pd.Series):
        return pd.Series(signed_size, index=prob.index, name="bet_size")
    return signed_size


def avg_active_signals(
    signals: pd.DataFrame,
    num_threads: int = 1,
) -> pd.Series:
    """对所有活跃信号取平均 — 书 Ch10 Snippet 10.2.

    参数
    ----
    signals : DataFrame, index=signal_start, 必备列 ['signal', 't1']
    """
    if signals.empty:
        return pd.Series(dtype=float)

    # 所有时间点（信号开始 + 结束）
    t_pnts = set(signals["t1"].dropna().tolist())
    t_pnts.update(signals.index.tolist())
    t_pnts = sorted(t_pnts)

    out = pd.Series(0.0, index=pd.DatetimeIndex(t_pnts))
    for loc in t_pnts:
        # 活跃 = 已开始 AND 未结束
        active_mask = (signals.index <= loc) & ((loc < signals["t1"]) | signals["t1"].isna())
        active = signals[active_mask]
        if len(active) > 0:
            out.loc[loc] = active["signal"].mean()
    return out


def discretize_signal(
    signal: pd.Series | float,
    step_size: float = 0.05,
) -> pd.Series | float:
    """信号离散化，避免微调过度交易 — 书 Ch10 §10.5.

    m* = round(m / d) * d, clipped to [-1, 1]
    """
    if isinstance(signal, (int, float)):
        m = round(signal / step_size) * step_size
        return float(np.clip(m, -1, 1))

    out = (signal / step_size).round() * step_size
    return out.clip(-1, 1)
