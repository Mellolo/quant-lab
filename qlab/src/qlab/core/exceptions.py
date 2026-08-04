"""Exception hierarchy for qlab.

按错误来源分层。下游模块用 isinstance 判断时只需关心顶层。
"""

from __future__ import annotations


class QlabError(Exception):
    """所有 qlab 异常的基类."""


# ---- 数据层 ----------------------------------------------------------------


class DataSourceError(QlabError):
    """数据源接入相关错误（拉取失败、连接断开、字段缺失）."""


class DataUnavailableError(DataSourceError):
    """schema 必填字段无法从数据源取得.

    数据源实现应抛出本异常而非用默认值降级 —— ``is_st=False`` / ``total_shares=0``
    这类降级值在 schema 看来完全合法，会静默污染研究结论而不给任何信号。
    """


class SchemaViolationError(QlabError):
    """数据 schema 不符合契约."""

    def __init__(self, message: str, missing_columns: list[str] | None = None,
                 invariant: str | None = None) -> None:
        super().__init__(message)
        self.missing_columns = missing_columns or []
        self.invariant = invariant


class PITViolationError(QlabError):
    """Point-in-Time 不变量违反（如用了未来数据）."""


class UniverseError(QlabError):
    """Universe 相关错误（如查询了不存在的成分股、跨市场冲突）."""


# ---- 特征层 ----------------------------------------------------------------


class FeatureComputationError(QlabError):
    """特征计算失败."""

    def __init__(self, feature_name: str, message: str,
                 cause: Exception | None = None) -> None:
        super().__init__(f"[{feature_name}] {message}")
        self.feature_name = feature_name
        self.cause = cause


class FeatureRegistrationError(QlabError):
    """特征注册冲突（重名、版本冲突等）."""


# ---- 模型层 ----------------------------------------------------------------


class CVError(QlabError):
    """交叉验证相关错误（如 Purge 后训练集为空）."""


# ---- 评估层 ----------------------------------------------------------------


class BacktestError(QlabError):
    """回测错误."""


class TrialRegistryError(QlabError):
    """实验追踪错误."""
