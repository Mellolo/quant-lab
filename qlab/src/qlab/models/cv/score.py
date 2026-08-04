"""cvScore — 书 Ch7 Snippet 7.4.

替代 sklearn 的 cross_val_score（其有传递 sample_weight 的 bug）。
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, log_loss

from qlab.models.cv.purged_kfold import PurgedKFold


def cv_score(
    clf,
    X: pd.DataFrame,
    y: pd.Series,
    sample_weight: pd.Series | None = None,
    scoring: Literal["neg_log_loss", "accuracy", "f1"] = "neg_log_loss",
    t1: pd.Series | None = None,
    cv: int = 5,
    cv_gen: PurgedKFold | None = None,
    pct_embargo: float = 0.0,
) -> np.ndarray:
    """带 PurgedKFold + sample_weight 的 CV 评分.

    sample_weight 正确传给 fit 和 metric（sklearn 的 cross_val_score 有 bug）。
    """
    if scoring not in {"neg_log_loss", "accuracy", "f1"}:
        raise ValueError(f"不支持的 scoring: {scoring}")

    if cv_gen is None:
        if t1 is None:
            raise ValueError("cv_gen 与 t1 至少提供一个")
        cv_gen = PurgedKFold(n_splits=cv, t1=t1, pct_embargo=pct_embargo)

    if sample_weight is None:
        sample_weight = pd.Series(1.0, index=X.index)

    scores: list[float] = []
    for train_idx, test_idx in cv_gen.split(X=X):
        X_train, y_train, w_train = X.iloc[train_idx], y.iloc[train_idx], sample_weight.iloc[train_idx]
        X_test, y_test, w_test = X.iloc[test_idx], y.iloc[test_idx], sample_weight.iloc[test_idx]

        # 训练
        fit = clf.fit(X_train, y_train, sample_weight=w_train.values)

        if scoring == "neg_log_loss":
            prob = fit.predict_proba(X_test)
            score = -log_loss(y_test, prob, sample_weight=w_test.values, labels=clf.classes_)
        elif scoring == "accuracy":
            pred = fit.predict(X_test)
            score = accuracy_score(y_test, pred, sample_weight=w_test.values)
        elif scoring == "f1":
            from sklearn.metrics import f1_score
            pred = fit.predict(X_test)
            score = f1_score(y_test, pred, sample_weight=w_test.values, average="binary")
        scores.append(score)

    return np.array(scores)
