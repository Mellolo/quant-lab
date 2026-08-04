"""时间衰减 — 书 Ch4 §4.7."""

from __future__ import annotations

import pandas as pd


def time_decay_factors(
    tw: pd.Series,
    clf_last_w: float = 1.0,
) -> pd.Series:
    """计算时间衰减因子 — 书 Ch4 Snippet 4.11.

    按"累计唯一性"而非"按时间"衰减（避免冗余样本密集时权重塌缩）。

    参数
    ----
    tw : 各样本的唯一性 Series（按 event_start 排序）
    clf_last_w : 最老样本的权重。
                 = 1: 无衰减
                 ∈ (0, 1): 线性衰减但全员有正权重
                 = 0: 最老线性收敛到 0
                 ∈ (-1, 0): 最老 cT 比例样本权重为 0

    返回
    ----
    Series with same index as tw
    """
    clf_w = tw.sort_index().cumsum()
    if len(clf_w) == 0:
        return pd.Series(dtype=float)

    if clf_last_w >= 0:
        slope = (1.0 - clf_last_w) / clf_w.iloc[-1]
    else:
        slope = 1.0 / ((clf_last_w + 1) * clf_w.iloc[-1])
    const = 1.0 - slope * clf_w.iloc[-1]

    out = const + slope * clf_w
    out[out < 0] = 0
    return out.reindex(tw.index)
