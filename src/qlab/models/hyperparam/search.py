"""超参搜索 — 书 Ch9 Snippet 9.1 / 9.3.

GridSearchCV / RandomizedSearchCV + PurgedKFold + sample_weight 修补。
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV

from qlab.models.cv.purged_kfold import PurgedKFold
from qlab.models.pipeline import MyPipeline


def clf_hyper_fit(
    feat: pd.DataFrame,
    lbl: pd.Series,
    t1: pd.Series,
    pipe_clf: Any,  # sklearn Pipeline / MyPipeline / Estimator
    param_grid: dict,
    *,
    cv: int = 3,
    rnd_search_iter: int = 0,
    n_jobs: int = -1,
    pct_embargo: float = 0.0,
    sample_weight: pd.Series | None = None,
    **fit_params,
) -> Any:
    """带 PurgedKFold 的超参搜索.

    参数
    ----
    rnd_search_iter : 0 → GridSearch；>0 → RandomizedSearch
    """
    # meta-labeling 用 f1，否则 neg_log_loss
    scoring = "f1" if set(lbl.unique()) == {0, 1} else "neg_log_loss"

    inner_cv = PurgedKFold(n_splits=cv, t1=t1, pct_embargo=pct_embargo)

    if rnd_search_iter == 0:
        gs = GridSearchCV(
            estimator=pipe_clf, param_grid=param_grid,
            scoring=scoring, cv=inner_cv, n_jobs=n_jobs,
        )
    else:
        gs = RandomizedSearchCV(
            estimator=pipe_clf, param_distributions=param_grid,
            scoring=scoring, cv=inner_cv, n_jobs=n_jobs,
            n_iter=rnd_search_iter,
        )

    # MyPipeline 支持 sample_weight；普通 estimator 用 fit_params 传
    if sample_weight is not None and isinstance(pipe_clf, MyPipeline):
        # MyPipeline.fit 会把 sample_weight 转发给最后一步
        gs = gs.fit(feat, lbl, sample_weight=sample_weight, **fit_params)
    elif sample_weight is not None:
        gs = gs.fit(feat, lbl, sample_weight=sample_weight.values, **fit_params)
    else:
        gs = gs.fit(feat, lbl, **fit_params)

    return gs.best_estimator_
