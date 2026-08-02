"""PCA 正交化 — 书 Ch8 §8.4.2.

破替代效应：先把特征矩阵 PCA 正交化，再做特征重要性。
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def orthogonalize_features(
    X: pd.DataFrame,
    var_thres: float = 0.95,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    """对 X 做 PCA 正交化，保留累计方差 ≥ var_thres 的主成分.

    返回
    ----
    (P, eigen_vectors, eigen_values)
        P : 正交化后的特征矩阵，列 = PC_1, PC_2, ...
        eigen_vectors : NxM 矩阵
        eigen_values : 各主成分的方差贡献
    """
    # 标准化
    Z = (X - X.mean()) / X.std(ddof=0).replace(0, np.nan)
    Z = Z.dropna(how="all", axis=1).fillna(0)

    dot = pd.DataFrame(
        np.dot(Z.T.values, Z.values),
        index=Z.columns, columns=Z.columns,
    )

    eVal, eVec = np.linalg.eigh(dot.values)
    # 降序
    idx = eVal.argsort()[::-1]
    eVal, eVec = eVal[idx], eVec[:, idx]

    eVal = pd.Series(eVal, index=[f"PC_{i+1}" for i in range(len(eVal))])
    eVec = pd.DataFrame(eVec, index=dot.index, columns=eVal.index)

    # 截断
    cum_var = eVal.cumsum() / eVal.sum()
    dim = int(cum_var.values.searchsorted(var_thres)) + 1
    eVal_kept = eVal.iloc[:dim]
    eVec_kept = eVec.iloc[:, :dim]

    P = pd.DataFrame(
        np.dot(Z.values, eVec_kept.values),
        index=X.index, columns=eVal_kept.index,
    )
    return P, eVec_kept, eVal_kept
