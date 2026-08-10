"""Foundational types, protocols, calendar, parallel primitives.

`core/` 不依赖任何其他 qlab 模块。所有跨模块共享的概念都在此定义。
"""

from qlab.core.enums import AdjustMode, EntryAt, EntryTiming, Freq, ReportType, Session
from qlab.core.exceptions import (
    DataSourceError,
    FeatureComputationError,
    PITViolationError,
    QlabError,
    SchemaViolationError,
    UniverseError,
)
from qlab.core.parallel import linear_partitions, mp_pandas_obj, nested_partitions
from qlab.core.schema import (
    SCHEMA_CONCEPT,
    SCHEMA_CORPORATE_ACTION,
    SCHEMA_DAILY_BAR,
    SCHEMA_EVENT,
    SCHEMA_FUNDAMENTAL,
    SCHEMA_INDUSTRY,
    SCHEMA_INTRADAY_BAR,
    SCHEMA_LABEL,
    SCHEMA_SAMPLE_WEIGHT,
    SCHEMA_UNIVERSE,
    validate_schema,
)

__all__ = [
    # enums
    "AdjustMode",
    "EntryAt",
    "EntryTiming",
    "Freq",
    "ReportType",
    "Session",
    # exceptions
    "QlabError",
    "DataSourceError",
    "SchemaViolationError",
    "PITViolationError",
    "FeatureComputationError",
    "UniverseError",
    # parallel
    "mp_pandas_obj",
    "linear_partitions",
    "nested_partitions",
    # schema
    "SCHEMA_DAILY_BAR",
    "SCHEMA_INTRADAY_BAR",
    "SCHEMA_UNIVERSE",
    "SCHEMA_CORPORATE_ACTION",
    "SCHEMA_FUNDAMENTAL",
    "SCHEMA_INDUSTRY",
    "SCHEMA_CONCEPT",
    "SCHEMA_EVENT",
    "SCHEMA_LABEL",
    "SCHEMA_SAMPLE_WEIGHT",
    "validate_schema",
]
