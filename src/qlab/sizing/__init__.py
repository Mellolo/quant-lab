"""仓位规模模块 — 书 Ch10.

依赖 core/，独立于 data/features/models。
"""

from qlab.sizing.dynamic import dynamic_position, limit_price
from qlab.sizing.from_probability import (
    avg_active_signals,
    bet_size_from_probability,
    discretize_signal,
)

__all__ = [
    "bet_size_from_probability",
    "avg_active_signals",
    "discretize_signal",
    "dynamic_position",
    "limit_price",
]
