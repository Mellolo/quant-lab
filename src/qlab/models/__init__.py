"""模型模块 — 书 Ch6-9.

- cv/             PurgedKFold + cvScore (替代 sklearn 的 cross_val_score)
- ensemble/       Bagging / RF 包装
- feature_importance/  MDI / MDA / SFI + 正交化
- hyperparam/     GridSearch / RandomSearch + LogUniform + MyPipeline
"""

from qlab.models.cv.purged_kfold import PurgedKFold
from qlab.models.cv.score import cv_score
from qlab.models.ensemble.bagging import build_bagging_classifier
from qlab.models.feature_importance.mda import feat_imp_mda
from qlab.models.feature_importance.mdi import feat_imp_mdi
from qlab.models.feature_importance.orthogonal import orthogonalize_features
from qlab.models.feature_importance.sfi import feat_imp_sfi
from qlab.models.hyperparam.log_uniform import log_uniform
from qlab.models.hyperparam.search import clf_hyper_fit
from qlab.models.pipeline import MyPipeline

__all__ = [
    "PurgedKFold",
    "cv_score",
    "build_bagging_classifier",
    "feat_imp_mdi",
    "feat_imp_mda",
    "feat_imp_sfi",
    "orthogonalize_features",
    "log_uniform",
    "clf_hyper_fit",
    "MyPipeline",
]
