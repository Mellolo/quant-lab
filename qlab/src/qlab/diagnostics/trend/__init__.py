"""趋势诊断。

任意一天只答三件事：方向、起点、效率（收盘 / 隔夜 / 盘中）。
方向为 0 时效率为空，改评震荡箱子。仓位见 ``diagnostics.flow``。
"""

from qlab.diagnostics.trend.campaign import campaign_panels
from qlab.diagnostics.trend.common import hysteresis_direction
from qlab.diagnostics.trend.range import range_panels
from qlab.diagnostics.trend.report import (
    TrendReport,
    diagnose_trend,
    direction_panels,
    format_trend_summary,
    trend_panels,
)

__all__ = [
    "TrendReport",
    "campaign_panels",
    "diagnose_trend",
    "direction_panels",
    "format_trend_summary",
    "hysteresis_direction",
    "range_panels",
    "trend_panels",
]
