"""MDA (Mean Decrease Accuracy) — 书 Ch8 §8.3.2 Snippet 8.3.

通用、样本外、慢。受替代效应影响。
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, log_loss

from qlab.models.cv.purged_kfold import PurgedKFold


def feat_imp_mda(
    clf,
    X: pd.DataFrame,
    y: pd.Series,
    sample_weight: pd.Series,
    t1: pd.Series,
    cv: int = 5,
    pct_embargo: float = 0.0,
    scoring: Literal["neg_log_loss", "accuracy"] = "neg_log_loss",
    random_state: int = 0,
) -> tuple[pd.DataFrame, float]:
    """
    返回
    ----
    (imp_df, oos_baseline)
        imp_df : DataFrame, index=feature_names, columns=['mean', 'std']
        oos_baseline : 基线 OOS 得分（permutation 前）
    """
    if scoring not in ("neg_log_loss", "accuracy"):
        raise ValueError(f"不支持的 scoring: {scoring}")

    cv_gen = PurgedKFold(n_splits=cv, t1=t1, pct_embargo=pct_embargo)
    scr0 = pd.Series(dtype=float)  # baseline per fold
    # 必须显式声明 float dtype —— 空 DataFrame 默认 object,
    # 后续 .loc 逐格赋值不会把它推回 float, 导致返回的重要度是 object dtype
    # (np.isfinite/np.log 等 numpy 运算会直接 TypeError, 与 MDI 的 float64 不一致)。
    scr1 = pd.DataFrame(columns=X.columns, dtype=float)

    rng = np.random.default_rng(random_state)

    for fold_i, (train_idx, test_idx) in enumerate(cv_gen.split(X=X)):
        X_train, y_train, w_train = (
            X.iloc[train_idx], y.iloc[train_idx], sample_weight.iloc[train_idx],
        )
        X_test, y_test, w_test = (
            X.iloc[test_idx], y.iloc[test_idx], sample_weight.iloc[test_idx],
        )

        fit = clf.fit(X_train, y_train, sample_weight=w_train.values)

        # baseline
        if scoring == "neg_log_loss":
            prob = fit.predict_proba(X_test)
            scr0.loc[fold_i] = -log_loss(y_test, prob,
                                          sample_weight=w_test.values,
                                          labels=clf.classes_)
        else:
            pred = fit.predict(X_test)
            scr0.loc[fold_i] = accuracy_score(y_test, pred, sample_weight=w_test.values)

        # 逐列 permutation
        for col in X.columns:
            X_perm = X_test.copy(deep=True)
            X_perm[col] = rng.permutation(X_perm[col].values)
            if scoring == "neg_log_loss":
                prob = fit.predict_proba(X_perm)
                scr1.loc[fold_i, col] = -log_loss(
                    y_test, prob, sample_weight=w_test.values, labels=clf.classes_,
                )
            else:
                pred = fit.predict(X_perm)
                scr1.loc[fold_i, col] = accuracy_score(
                    y_test, pred, sample_weight=w_test.values,
                )

    # 重要性：(baseline - permuted) / 最大可能改进
    imp = (-scr1).add(scr0, axis=0)
    imp = imp / -scr1 if scoring == "neg_log_loss" else imp / (1.0 - scr1)

    n = imp.shape[0]
    out = pd.concat(
        {"mean": imp.mean(), "std": imp.std() * (n ** -0.5)},
        axis=1,
    )
    # 出口强制 float64: 与 :func:`feat_imp_mdi` 保持一致, 保证下游
    # np.isfinite / 阀值筛选等 numpy 运算可用。
    out = out.astype("float64")
    return out, float(scr0.mean())
