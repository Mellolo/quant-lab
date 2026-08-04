"""Inverse-Variance Portfolio — 书 Ch16 §16.A.2.

对角协方差矩阵下的最优解。
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def inverse_variance_portfolio(
    cov: np.ndarray | pd.DataFrame,
) -> pd.Series | np.ndarray:
    """ω_n = (1/V_nn) / Σ(1/V_ii).

    Args:
        cov: 协方差矩阵。传 DataFrame 时保留标的名。

    Returns:
        传入 DataFrame → 返回 ``pd.Series``(index=标的名), 与
        :meth:`~qlab.allocation.hrp.HierarchicalRiskParity.allocate` 一致;
        传入裸 ndarray → 返回 ``np.ndarray``(无标的名可用)。

    Note:
        对 DataFrame 返回 Series 而非裸数组 —— 组合权重必须能对回标的,
        丢掉列名会让调用方靠位置对齐, 一旦顺序变动就静默错配。

    Raises:
        ValueError: 对角线含非正方差(零/负/NaN)。不拦的话 ``1/0`` 会静默
            产出 **NaN 权重**且不报错 —— NaN 会一路传到下单环节。
            零方差通常意味着该标的在样本期全程停牌或数据缺失。
    """
    is_frame = isinstance(cov, pd.DataFrame)
    diag = np.diag(cov.to_numpy() if is_frame else cov)
    bad = ~np.isfinite(diag) | (diag <= 0)
    if bad.any():
        names = (
            [str(c) for c, b in zip(cov.columns, bad, strict=False) if b]
            if is_frame
            else [f"idx={i}" for i in np.flatnonzero(bad)]
        )
        raise ValueError(
            f"IVP 需要所有方差为正, 但以下标的的方差非正或非有限: "
            f"{names[:10]}{'...' if len(names) > 10 else ''}\n"
            f"  对应方差值: {[round(float(v), 10) for v in diag[bad][:10]]}\n"
            "  成因: 该标的在样本期全程停牌/无价格变动, 或协方差矩阵含 NaN。\n"
            "  出路: 先剔除这些标的再算权重。"
        )
    ivp = 1.0 / diag
    ivp = ivp / ivp.sum()
    if is_frame:
        return pd.Series(ivp, index=cov.columns, name="weight")
    return ivp
