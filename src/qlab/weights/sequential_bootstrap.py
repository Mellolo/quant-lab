"""Sequential Bootstrap — 书 Ch4 §4.5.

每抽一个样本就更新概率，让"与已抽样本重叠多的"再被抽中的概率下降。
比标准 bootstrap 更接近 IID 抽样。
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def build_indicator_matrix(
    bar_idx: pd.DatetimeIndex,
    t1: pd.Series,
) -> pd.DataFrame:
    """构造指示矩阵 — 书 Ch4 Snippet 4.3.

    返回 DataFrame，行索引 = bar_idx，列 = 事件序号 0..N-1，
    值为 1 表示该 bar 落在事件 i 的 [t0, t1] 区间内。
    """
    ind = pd.DataFrame(0, index=bar_idx, columns=range(len(t1)), dtype=np.int8)
    for i, (t0, t1_) in enumerate(t1.items()):
        # 找时间范围（处理 NaT）
        end_ = t1_ if not pd.isna(t1_) else bar_idx[-1]
        mask = (bar_idx >= t0) & (bar_idx <= end_)
        ind.loc[mask, i] = 1
    return ind


def _average_uniqueness_of_indicator(ind_m: pd.DataFrame) -> pd.Series:
    """计算指示矩阵下各事件的平均唯一性 — 书 Ch4 Snippet 4.4."""
    c = ind_m.sum(axis=1)  # concurrency
    u = ind_m.div(c.replace(0, np.nan), axis=0)
    return u.where(u > 0).mean(axis=0)


def seq_bootstrap_sample(
    ind_m: pd.DataFrame,
    sample_length: int | None = None,
    seed: int | None = None,
) -> list[int]:
    """Sequential bootstrap 抽样 — 书 Ch4 Snippet 4.5.

    参数
    ----
    ind_m : build_indicator_matrix 的输出
    sample_length : 抽样数量（None = ind_m.shape[1]）
    seed : 随机种子

    返回
    ----
    被抽中的事件序号列表（可能含重复）
    """
    rng = np.random.default_rng(seed)
    sample_length = sample_length or ind_m.shape[1]

    selected: list[int] = []
    all_cols = list(ind_m.columns)

    while len(selected) < sample_length:
        # 对每个候选 j，计算"加入它之后"的最后一列平均唯一性
        avg_u = pd.Series(index=all_cols, dtype=float)
        for j in all_cols:
            sub = ind_m[selected + [j]]
            avg_u_j = _average_uniqueness_of_indicator(sub).iloc[-1]
            avg_u.loc[j] = avg_u_j if not pd.isna(avg_u_j) else 0.0

        if avg_u.sum() <= 0:
            # fallback: uniform
            prob = np.ones(len(all_cols)) / len(all_cols)
        else:
            prob = (avg_u / avg_u.sum()).values

        choice = rng.choice(all_cols, p=prob)
        selected.append(int(choice))

    return selected
