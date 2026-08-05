"""特征模块.

依赖 data/，被 labeling/ 消费。
"""

from qlab.features.alignment import (
    align_features_for_entry,
    assert_features_admissible,
    attach_features_to_events,
    events_entry_timing,
    feature_shift_days,
)
from qlab.features.base import (
    FEATURE_AVAILABLE_AT,
    CompositeFeature,
    DailyFeature,
    Feature,
    FeatureAvailableAt,
    FeatureMeta,
    FeatureValueMeta,
    IntradayDerivedFeature,
)
from qlab.features.context import FeatureContext
from qlab.features.matrix import FeatureMatrix, build_feature_matrix
from qlab.features.registry import FeatureRegistry, registry
from qlab.features.store import FeatureStore, InMemoryFeatureStore, ParquetFeatureStore

__all__ = [
    "FEATURE_AVAILABLE_AT",
    "Feature",
    "FeatureAvailableAt",
    "FeatureMeta",
    "FeatureValueMeta",
    "DailyFeature",
    "IntradayDerivedFeature",
    "CompositeFeature",
    "FeatureContext",
    "FeatureMatrix",
    "build_feature_matrix",
    "align_features_for_entry",
    "assert_features_admissible",
    "attach_features_to_events",
    "events_entry_timing",
    "feature_shift_days",
    "FeatureRegistry",
    "registry",
    "FeatureStore",
    "InMemoryFeatureStore",
    "ParquetFeatureStore",
]
