"""单标的报告与全市场宽表入口。"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from qlab.diagnostics.trend.campaign import CAMPAIGN_KEYS, campaign_panels
from qlab.diagnostics.trend.common import as_ohlcv_frame, resolve_asof, wide_ohlc
from qlab.diagnostics.trend.direction import gate_direction, st_direction_panels
from qlab.diagnostics.trend.range import range_panels


@dataclass
class TrendReport:
    asof: pd.Timestamp
    direction: int
    efficiency: float
    overnight_efficiency: float
    session_efficiency: float
    summary: str
    components: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["asof"] = str(pd.Timestamp(self.asof).date())
        return d


def format_trend_summary(report: TrendReport) -> str:
    day = pd.Timestamp(report.asof).date()
    if report.direction == 0:
        return f"{day} 区间；趋势侧不评价"
    side = {1: "多头", -1: "空头"}.get(report.direction, "?")

    def _fmt(v: float) -> str:
        return "—" if not np.isfinite(v) else f"{v:.2f}"

    return (
        f"{day} {side}，效率 {_fmt(report.efficiency)}，"
        f"隔夜 {_fmt(report.overnight_efficiency)}，盘中 {_fmt(report.session_efficiency)}"
    )


def _trend_bundle(
    close: pd.DataFrame,
    high: pd.DataFrame | None,
    low: pd.DataFrame | None,
    *,
    open_: pd.DataFrame | None = None,
    dir_hold: int,
    st_atr: int,
    st_mult_slow: float,
    st_mult_fast: float,
    include_xs_rank: bool,
) -> dict[str, pd.DataFrame]:
    close, high, low = wide_ohlc(close, high, low)
    raw = st_direction_panels(
        close,
        high,
        low,
        dir_hold=dir_hold,
        st_atr=st_atr,
        st_mult_slow=st_mult_slow,
        st_mult_fast=st_mult_fast,
    )
    campaign = campaign_panels(
        close,
        high,
        low,
        open_=open_,
        direction=raw,
        include_xs_rank=include_xs_rank,
    )
    direction = gate_direction(raw, campaign["retention"], campaign["origin_age"])
    live = direction != 0
    for key in CAMPAIGN_KEYS:
        campaign[key] = campaign[key].where(live)
    if "efficiency_xs" in campaign:
        campaign["efficiency_xs"] = campaign["efficiency_xs"].where(live)
    range_out = range_panels(close, high, low, direction=direction)
    return {"direction": direction, **campaign, **range_out}


def direction_panels(
    close: pd.DataFrame,
    high: pd.DataFrame | None = None,
    low: pd.DataFrame | None = None,
    *,
    open_: pd.DataFrame | None = None,
    dir_hold: int = 2,
    st_atr: int = 10,
    st_mult_slow: float = 3.0,
    st_mult_fast: float = 2.0,
) -> dict[str, pd.DataFrame]:
    """SuperTrend 生方向，这条路径留存过低则 0。"""
    bundle = _trend_bundle(
        close,
        high,
        low,
        open_=open_,
        dir_hold=dir_hold,
        st_atr=st_atr,
        st_mult_slow=st_mult_slow,
        st_mult_fast=st_mult_fast,
        include_xs_rank=False,
    )
    return {"direction": bundle["direction"]}


def trend_panels(
    close: pd.DataFrame,
    high: pd.DataFrame | None = None,
    low: pd.DataFrame | None = None,
    *,
    open_: pd.DataFrame | None = None,
    dir_hold: int = 2,
    st_atr: int = 10,
    st_mult_slow: float = 3.0,
    st_mult_fast: float = 2.0,
    include_xs_rank: bool = True,
) -> dict[str, pd.DataFrame]:
    return _trend_bundle(
        close,
        high,
        low,
        open_=open_,
        dir_hold=dir_hold,
        st_atr=st_atr,
        st_mult_slow=st_mult_slow,
        st_mult_fast=st_mult_fast,
        include_xs_rank=include_xs_rank,
    )


def diagnose_trend(
    ohlcv: pd.DataFrame,
    *,
    asof: object | None = None,
) -> TrendReport:
    df = as_ohlcv_frame(ohlcv)
    asof_ts = resolve_asof(df.index, asof)
    df = df.loc[:asof_ts]
    close = df[["close"]].rename(columns={"close": "S"})
    high = df[["high"]].rename(columns={"high": "S"})
    low = df[["low"]].rename(columns={"low": "S"})
    open_ = df[["open"]].rename(columns={"open": "S"}) if "open" in df.columns else None
    panels = trend_panels(close, high=high, low=low, open_=open_, include_xs_rank=False)

    def _cell(name: str) -> float:
        if name not in panels:
            return float("nan")
        v = panels[name].loc[asof_ts, "S"]
        return float(v) if not np.isnan(v) else float("nan")

    d_raw = _cell("direction")
    direction = int(d_raw) if np.isfinite(d_raw) else 0
    live = direction != 0
    report = TrendReport(
        asof=asof_ts,
        direction=direction,
        efficiency=_cell("efficiency") if live else float("nan"),
        overnight_efficiency=_cell("overnight_efficiency") if live else float("nan"),
        session_efficiency=_cell("session_efficiency") if live else float("nan"),
        summary="",
        components={
            "origin": _cell("origin"),
            "origin_age": _cell("origin_age"),
            "retention": _cell("retention"),
            "range_ok": _cell("range_ok"),
            "range_edge": _cell("range_edge"),
            "range_width_atr": _cell("range_width_atr"),
        },
    )
    report.summary = format_trend_summary(report)
    return report
