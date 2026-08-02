"""Walk-Forward 回测 — 书 Ch12 §12.2."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd


def walk_forward_backtest(
    X: pd.DataFrame,
    y: pd.Series,
    sample_weight: pd.Series | None = None,
    *,
    n_splits: int = 5,
    embargo_size: int = 0,
    fit_fn: Callable[..., Any] | None = None,
    predict_fn: Callable[..., np.ndarray] | None = None,
    score_fn: Callable[[np.ndarray, np.ndarray], float] | None = None,
) -> pd.DataFrame:
    """简单 Walk-Forward.

    把数据按时间均分 n_splits 段。第 i 段作为测试集，前 i-1 段作为训练集。

    返回
    ----
    DataFrame, index=split_id, columns=['train_end', 'test_start', 'test_end', 'score']
    """
    if fit_fn is None or predict_fn is None or score_fn is None:
        raise ValueError("必须提供 fit_fn, predict_fn, score_fn")
    if sample_weight is None:
        sample_weight = pd.Series(1.0, index=X.index)

    n = len(X)
    edges = np.linspace(0, n, n_splits + 1).astype(int)
    rows = []
    for i in range(1, n_splits):
        train_end = edges[i] - embargo_size
        test_start = edges[i]
        test_end = edges[i + 1]
        if train_end <= 0 or test_end <= test_start:
            continue
        X_train, y_train, w_train = X.iloc[:train_end], y.iloc[:train_end], sample_weight.iloc[:train_end]
        X_test, y_test = X.iloc[test_start:test_end], y.iloc[test_start:test_end]
        model = fit_fn(X_train, y_train, w_train)
        pred = predict_fn(model, X_test)
        score = score_fn(y_test.values, pred)
        rows.append({
            "split_id": i,
            "train_end": X.index[train_end - 1] if train_end > 0 else None,
            "test_start": X.index[test_start],
            "test_end": X.index[test_end - 1],
            "score": float(score),
        })
    return pd.DataFrame(rows).set_index("split_id")
