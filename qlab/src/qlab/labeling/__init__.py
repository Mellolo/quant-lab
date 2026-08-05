"""标注模块 — Ch3.

依赖 data/ + features/，被 weights/ 与 models/ 消费。
"""

from qlab.core.enums import EntryTiming
from qlab.labeling.events import (
    CUSUMFilter,
    EntropySampler,
    EventSampler,
    HMMTrendSampler,
    RunSampler,
    TrendBreakoutSampler,
    VolumeCUSUMFilter,
    daily_event_pairs,
    to_event_dataframe,
)
from qlab.labeling.meta_labeling import meta_label_bins, to_meta_labels
from qlab.labeling.thresholds import daily_ewm_vol
from qlab.labeling.triple_barrier import TripleBarrier, label_events

__all__ = [
    "CUSUMFilter",
    "EntropySampler",
    "EntryTiming",
    "EventSampler",
    "HMMTrendSampler",
    "RunSampler",
    "TrendBreakoutSampler",
    "VolumeCUSUMFilter",
    "daily_event_pairs",
    "to_event_dataframe",
    "daily_ewm_vol",
    "TripleBarrier",
    "label_events",
    "to_meta_labels",
    "meta_label_bins",
]
