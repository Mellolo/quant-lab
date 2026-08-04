"""SFI (Single Feature Importance) — 书 Ch8 §8.4.1.

每个特征单独训练分类器，OOS 得分作为重要性。
不受替代效应影响，但丢失联合效应。
"""

from __future__ import annotations

from typing import Literal

import pandas as pd

from qlab.models.cv.purged_kfold import PurgedKFold
from qlab.models.cv.score import cv_score


def feat_imp_sfi(
    clf,
    X: pd.DataFrame,
    y: pd.Series,
    sample_weight: pd.Series,
    t1: pd.Series,
    cv: int = 5,
    pct_embargo: float = 0.0,
    scoring: Literal["neg_log_loss", "accuracy"] = "neg_log_loss",
) -> pd.DataFrame:
    """每个特征单独 CV 评估."""
    cv_gen = PurgedKFold(n_splits=cv, t1=t1, pct_embargo=pct_embargo)

    imp = pd.DataFrame(columns=["mean", "std"], dtype=float)
    for col in X.columns:
        sub_X = X[[col]]
        scores = cv_score(
            clf=clf, X=sub_X, y=y, sample_weight=sample_weight,
            scoring=scoring, cv_gen=cv_gen,
        )
        n = len(scores)
        imp.loc[col, "mean"] = scores.mean()
        imp.loc[col, "std"] = scores.std() * (n ** -0.5) if n > 1 else 0.0

    return imp
