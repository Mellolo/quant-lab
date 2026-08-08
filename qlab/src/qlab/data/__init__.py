"""数据层模块.

只关心：干净、对齐、PIT 正确的 OHLCV + 状态 + 财务 + 行业。
不依赖 features/labeling/... 等任何下游模块。

公开 API：
    DataLayer            统一数据访问入口
    DataSource           外部数据源 Protocol
    BarStore             缓存层 Protocol
    Universe             投资域查询
"""

from qlab.data.interfaces import BarStore, DataSource, ShardedBarStore
from qlab.data.layer import DataLayer
from qlab.data.store import (
    InMemoryBarStore,
    InMemoryShardedBarStore,
    ParquetBarStore,
    ParquetShardedBarStore,
)
from qlab.data.universe import Universe, UniverseSpec, filter_by_dollar_volume

__all__ = [
    "DataLayer",
    "DataSource",
    "BarStore",
    "ShardedBarStore",
    "InMemoryBarStore",
    "InMemoryShardedBarStore",
    "ParquetBarStore",
    "ParquetShardedBarStore",
    "Universe",
    "UniverseSpec",
    "filter_by_dollar_volume",
]
