"""PBO (Probability of Backtest Overfitting) — 书 Ch11 §11.6.

CSCV (Combinatorially Symmetric Cross-Validation) 方法.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PBOResult:
    """PBO 计算结果.

    统一用 dataclass 而非裸 dict —— 与 :class:`~qlab.features.matrix.FeatureMatrix`
    等结构化结果保持一致, 支持 ``res.pbo`` 属性访问与 IDE 补全。
    为兼容既有 ``res["pbo"]`` 写法, 保留 ``__getitem__``。
    """

    pbo: float
    """过拟合概率: OOS 表现劣于中位数的组合占比. 越低越好."""

    logits: np.ndarray = field(repr=False)
    """各组合的 logit 分布(负值表示该组合 OOS 退化)."""

    n_combinations: int
    """CSCV 组合总数."""

    def __getitem__(self, key: str):
        """兼容旧的 dict 式访问(``res['pbo']``)."""
        try:
            return getattr(self, key)
        except AttributeError as exc:
            raise KeyError(key) from exc

    def keys(self) -> tuple[str, ...]:
        """兼容 dict 式解包."""
        return ("pbo", "logits", "n_combinations")


def compute_pbo(
    performance_matrix: pd.DataFrame,
    n_splits: int = 16,
) -> PBOResult:
    """计算 PBO.

    参数
    ----
    performance_matrix : T × N 的性能矩阵
        T 行 = 时间观测
        N 列 = 不同的策略配置（试验）
        每个值 = 该时刻该策略的回报
    n_splits : 把行均分多少份（必须为偶数）

    返回
    ----
    :class:`PBOResult` —— 含 ``pbo`` / ``logits`` / ``n_combinations``
    (亦支持 ``res["pbo"]`` 的 dict 式访问)。
    """
    if n_splits % 2 != 0:
        raise ValueError("n_splits 必须为偶数")
    T, N = performance_matrix.shape
    if n_splits > T:
        raise ValueError(f"行数 {T} 不足以分 {n_splits} 份")

    # 切分
    sub_size = T // n_splits
    submatrices = []
    for s in range(n_splits):
        start = s * sub_size
        end = (s + 1) * sub_size if s < n_splits - 1 else T
        submatrices.append(performance_matrix.iloc[start:end])

    logits: list[float] = []
    for combo in combinations(range(n_splits), n_splits // 2):
        train_idx = list(combo)
        test_idx = [i for i in range(n_splits) if i not in combo]

        J = pd.concat([submatrices[i] for i in train_idx])
        J_bar = pd.concat([submatrices[i] for i in test_idx])

        # 在训练集上找最优列
        R = J.mean()  # 用 mean return 代理 SR；可换为 SR
        n_star = R.idxmax()

        # 测试集上看 n_star 的相对排名
        R_bar = J_bar.mean()
        rank = (R_bar < R_bar.loc[n_star]).sum() / max(len(R_bar) - 1, 1)
        omega = rank
        # 避免 0/1
        omega = max(min(omega, 1 - 1e-10), 1e-10)
        logit = np.log(omega / (1 - omega))
        logits.append(logit)

    logits_arr = np.array(logits)
    pbo = float((logits_arr < 0).mean())

    return PBOResult(
        pbo=pbo,
        logits=logits_arr,
        n_combinations=len(logits),
    )
