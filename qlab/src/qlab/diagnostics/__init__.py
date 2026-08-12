"""诊断工具 — 独立于采样/因子的行情评价模块."""

from qlab.diagnostics.trend import (
    PHASE_CODE,
    TrendReport,
    diagnose_trend,
    format_trend_summary,
    trend_panels,
)

__all__ = [
    "PHASE_CODE",
    "TrendReport",
    "diagnose_trend",
    "format_trend_summary",
    "trend_panels",
]
