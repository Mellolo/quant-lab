"""特征模块.

依赖 data/，被 labeling/ 消费。
"""

from qlab.features.base import (
    CompositeFeature,
    DailyFeature,
    Feature,
    FeatureMeta,
    FeatureValueMeta,
    IntradayDerivedFeature,
)
from qlab.features.context import FeatureContext
from qlab.features.matrix import FeatureMatrix, build_feature_matrix
from qlab.features.registry import FeatureRegistry, registry
from qlab.features.store import FeatureStore, InMemoryFeatureStore, ParquetFeatureStore

__all__ = [
    "Feature",
    "FeatureMeta",
    "FeatureValueMeta",
    "DailyFeature",
    "IntradayDerivedFeature",
    "CompositeFeature",
    "FeatureContext",
    "FeatureMatrix",
    "build_feature_matrix",
    "FeatureRegistry",
    "registry",
    "FeatureStore",
    "InMemoryFeatureStore",
    "ParquetFeatureStore",
]
