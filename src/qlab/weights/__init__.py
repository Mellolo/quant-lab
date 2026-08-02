"""样本权重模块 — 书 Ch4.

非 IID 标签的修正：uniqueness + sequential bootstrap + return attribution + time decay。
"""

from qlab.weights.sequential_bootstrap import (
    build_indicator_matrix,
    seq_bootstrap_sample,
)
from qlab.weights.time_decay import time_decay_factors
from qlab.weights.uniqueness import (
    average_uniqueness,
    num_concurrent_events,
    return_attribution_weights,
    sample_weights,
)

__all__ = [
    "num_concurrent_events",
    "average_uniqueness",
    "return_attribution_weights",
    "sample_weights",
    "build_indicator_matrix",
    "seq_bootstrap_sample",
    "time_decay_factors",
]
