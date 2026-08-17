"""诊断工具 — 独立于采样/因子的行情评价模块."""

from qlab.diagnostics.flow import (
    FlowReport,
    diagnose_flow,
    flow_panels,
    format_flow_summary,
)
from qlab.diagnostics.trend import (
    TrendReport,
    campaign_panels,
    diagnose_trend,
    direction_panels,
    format_trend_summary,
    range_panels,
    trend_panels,
)
from qlab.diagnostics.trend_eval import score_trend_panels

__all__ = [
    "FlowReport",
    "TrendReport",
    "campaign_panels",
    "diagnose_flow",
    "diagnose_trend",
    "direction_panels",
    "flow_panels",
    "format_flow_summary",
    "format_trend_summary",
    "range_panels",
    "score_trend_panels",
    "trend_panels",
]
