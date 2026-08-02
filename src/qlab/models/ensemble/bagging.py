"""Bagging 工具 — 书 Ch6.

金融场景推荐 bagging > boosting：过拟合风险高于欠拟合。
默认配置已为非 IID 标签优化（max_samples = avgU）。
"""

from __future__ import annotations

from typing import Any

from sklearn.ensemble import BaggingClassifier
from sklearn.tree import DecisionTreeClassifier


def build_bagging_classifier(
    base_estimator: Any | None = None,
    n_estimators: int = 1000,
    max_samples: float = 1.0,
    max_features: float = 1.0,
    class_weight: str | dict | None = "balanced",
    n_jobs: int = -1,
    random_state: int | None = None,
) -> BaggingClassifier:
    """构造金融场景推荐的 Bagging 分类器.

    参数
    ----
    base_estimator : 基学习器（默认 DecisionTreeClassifier，按书推荐）
    n_estimators : 基学习器数量
    max_samples : in-bag 样本比例。**推荐设为 average uniqueness（书 Ch6 §6.3.3）**
    max_features : 特征采样比例
    class_weight : 类别权重，默认 balanced
    n_jobs : 并行
    random_state : 随机种子
    """
    if base_estimator is None:
        base_estimator = DecisionTreeClassifier(
            criterion="entropy",
            max_features="sqrt",
            class_weight=class_weight,
        )

    return BaggingClassifier(
        estimator=base_estimator,
        n_estimators=n_estimators,
        max_samples=max_samples,
        max_features=max_features,
        n_jobs=n_jobs,
        random_state=random_state,
    )
