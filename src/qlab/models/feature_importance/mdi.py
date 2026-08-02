"""MDI (Mean Decrease Impurity) — 书 Ch8 §8.3.1 Snippet 8.2.

仅适用于树模型，样本内、快。受替代效应影响。
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def feat_imp_mdi(fit, feature_names: list[str]) -> pd.DataFrame:
    """
    参数
    ----
    fit : 已拟合的 BaggingClassifier / RandomForestClassifier
    feature_names : 特征名列表

    返回
    ----
    DataFrame, index=feature_names, columns=['mean', 'std']
    """
    if hasattr(fit, "estimators_"):
        # bagging / random forest
        df0 = {i: tree.feature_importances_ for i, tree in enumerate(fit.estimators_)}
        df0 = pd.DataFrame.from_dict(df0, orient="index")
        df0.columns = feature_names
    else:
        # 单一 estimator with feature_importances_
        arr = fit.feature_importances_.reshape(1, -1)
        df0 = pd.DataFrame(arr, columns=feature_names)

    # 0 值替换为 NaN（避免被错误纳入平均）
    df0 = df0.replace(0, np.nan)

    n = df0.shape[0]
    imp = pd.concat(
        {"mean": df0.mean(), "std": df0.std() * (n ** -0.5)},
        axis=1,
    )
    # 归一化（mean 之和 = 1）
    total = imp["mean"].sum()
    if total > 0:
        imp = imp / total
    return imp
