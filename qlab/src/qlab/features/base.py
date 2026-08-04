"""Feature 基类与元数据.

§3.7 FeatureMeta 的代码化定义。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Literal, Protocol, runtime_checkable

import pandas as pd

from qlab.core.enums import Freq


@dataclass(frozen=True)
class FeatureMeta:
    """特征元数据."""

    name: str
    version: str
    lookback_days: int
    available_at: Literal["today_close", "next_open"] = "today_close"
    requires_intraday: bool = False
    intraday_freq: Freq | None = None
    dependencies: tuple[str, ...] = ()
    universe_filter: str = "all"
    output_dtype: str = "float64"
    output_range: tuple[float, float] | None = None
    description: str = ""


@dataclass(frozen=True)
class FeatureValueMeta:
    """单次特征计算结果的伴随元数据 — §3.8.

    随 FeatureValue 一同持久化（JSON sidecar），用于审计与重现。
    """

    feature_name: str
    feature_version: str
    computed_at: str               # ISO 8601 时间戳
    dataset_id: str
    universe_id: str
    date_range: tuple[str, str]    # (start, end) date strings
    pipeline_hash: str             # 全流水线哈希（含依赖、数据源版本）
    data_version: str = ""         # 上游数据版本（DataSource.source_version）

    def to_json_dict(self) -> dict:
        d = asdict(self)
        d["date_range"] = list(d["date_range"])  # tuple → list for JSON
        return d

    @classmethod
    def from_json_dict(cls, d: dict) -> FeatureValueMeta:
        d = dict(d)
        if "date_range" in d and isinstance(d["date_range"], list):
            d["date_range"] = tuple(d["date_range"])
        return cls(**d)

    @staticmethod
    def now_iso() -> str:
        return datetime.utcnow().isoformat(timespec="seconds") + "Z"


@runtime_checkable
class Feature(Protocol):
    """特征协议."""

    meta: FeatureMeta

    def compute(self, ctx: FeatureContext) -> pd.Series:  # noqa: F821 - forward ref
        ...


class FeatureBase(ABC):
    """Feature 的便捷基类（可选继承）."""

    meta: FeatureMeta

    @abstractmethod
    def compute(self, ctx) -> pd.Series:
        ...

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.meta.name}@v{self.meta.version}>"


class DailyFeature(FeatureBase):
    """基于日线数据的特征.

    子类只需实现 compute(ctx) → pd.Series。
    元数据通过 __init__ 或类属性提供。
    """

    requires_intraday = False


class IntradayDerivedFeature(FeatureBase):
    """基于日内数据派生、对齐到日的特征.

    子类实现 compute(ctx)；通常会用 ctx.intraday_rolling() 完成对齐。
    """

    requires_intraday = True


class CompositeFeature(FeatureBase):
    """依赖其他特征的复合特征.

    子类必须在 meta.dependencies 里列出所有依赖。
    compute 时通过 ctx.upstream(name) 取依赖值。
    """

    pass
