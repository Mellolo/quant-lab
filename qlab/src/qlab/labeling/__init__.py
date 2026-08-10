"""标注模块 — Ch3.

依赖 data/ + features/，被 weights/ 与 models/ 消费。

采样分工
--------
- **确认日**: :class:`EventSampler` / 网格 — 条件成立日（网格则每日皆确认）
- **入场**: :class:`SampleSpec` 默认**次日开盘**；可选 ``entry_at=confirm_close``
- **出场**: :class:`ExitSettings` — 经典三重屏障（pt/sl + 定日）；其他出场见该模块扩展点

Join 注意: 多标的下 ``event_start`` 会重复，请用 ``event_id`` 或
:func:`ensure_event_key`，勿只按 ``event_start`` merge。
"""

from qlab.core.enums import EntryAt, EntryTiming
from qlab.labeling.events import (
    CUSUMFilter,
    EntropySampler,
    EventSampler,
    HMMTrendSampler,
    NewHighBreakoutSampler,
    RunSampler,
    VolumeCUSUMFilter,
    daily_event_pairs,
    ensure_event_key,
    filter_pairs,
    to_event_dataframe,
)
from qlab.labeling.exit import (
    EXIT_RESEARCH_DEFAULT,
    EXIT_TB_1_5_1_V20,
    ExitSettings,
)
from qlab.labeling.meta_labeling import meta_label_bins, to_meta_labels
from qlab.labeling.sample_spec import (
    SampleSpec,
    confirmation_to_entry,
    wide_ohlc_to_long,
)
from qlab.labeling.thresholds import daily_ewm_vol, daily_ewm_vol_panel, targets_from_panel
from qlab.labeling.triple_barrier import TripleBarrier, label_events

__all__ = [
    "CUSUMFilter",
    "EntropySampler",
    "EntryAt",
    "EntryTiming",
    "EventSampler",
    "ExitSettings",
    "EXIT_RESEARCH_DEFAULT",
    "EXIT_TB_1_5_1_V20",
    "HMMTrendSampler",
    "NewHighBreakoutSampler",
    "RunSampler",
    "SampleSpec",
    "VolumeCUSUMFilter",
    "confirmation_to_entry",
    "daily_event_pairs",
    "ensure_event_key",
    "filter_pairs",
    "to_event_dataframe",
    "wide_ohlc_to_long",
    "daily_ewm_vol",
    "daily_ewm_vol_panel",
    "targets_from_panel",
    "TripleBarrier",
    "label_events",
    "to_meta_labels",
    "meta_label_bins",
]
