"""分类指标 — 书 Ch14 §14.8.

封装 sklearn 的 metrics，确保对退化情况的处理一致。
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
)


def classification_scores(
    y_true: pd.Series | np.ndarray,
    y_pred: pd.Series | np.ndarray,
    y_prob: np.ndarray | None = None,
    sample_weight: pd.Series | np.ndarray | None = None,
) -> dict[str, float]:
    """返回 accuracy / precision / recall / f1 / neg_log_loss."""
    out = {
        "accuracy": float(accuracy_score(y_true, y_pred, sample_weight=sample_weight)),
    }

    # precision / recall / f1 仅对二分类有意义
    try:
        out["precision"] = float(precision_score(y_true, y_pred, sample_weight=sample_weight,
                                                 zero_division=0))
        out["recall"] = float(recall_score(y_true, y_pred, sample_weight=sample_weight,
                                            zero_division=0))
        out["f1"] = float(f1_score(y_true, y_pred, sample_weight=sample_weight,
                                    zero_division=0))
    except Exception:
        out["precision"] = float("nan")
        out["recall"] = float("nan")
        out["f1"] = float("nan")

    if y_prob is not None:
        try:
            out["neg_log_loss"] = float(-log_loss(y_true, y_prob, sample_weight=sample_weight))
        except Exception:
            out["neg_log_loss"] = float("nan")

    return out
