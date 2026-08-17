"""资金博弈宽表与单标的报告。窗口可借用趋势起点，但评分不在趋势模块里。"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from qlab.core.price_panels import atr_panel
from qlab.diagnostics.flow.book import FLOW_KEYS, bar_weight, score_book
from qlab.diagnostics.trend.common import as_ohlcv_frame, empty_like, resolve_asof, run_age, wide_ohlc
from qlab.diagnostics.trend.report import direction_panels
from qlab.diagnostics.trend.origin import SWING_LEFT_RIGHT, _campaign_pts, _iter_campaigns


@dataclass
class FlowReport:
    asof: pd.Timestamp
    hold: float
    summary: str
    components: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["asof"] = str(pd.Timestamp(self.asof).date())
        return d


def format_flow_summary(report: FlowReport) -> str:
    def _fmt(v: object) -> str:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return "—"
        return f"{float(v):.2f}"

    return f"{pd.Timestamp(report.asof).date()} 资金博弈 仓位 {_fmt(report.hold)}"


def _flow_one(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    direction: np.ndarray,
    age: np.ndarray,
    atr: np.ndarray,
    weight: np.ndarray | None,
    *,
    left_right: int,
) -> dict[str, np.ndarray]:
    n = len(close)
    out = {k: np.full(n, np.nan) for k in FLOW_KEYS}
    for i, oi, op, zig in _iter_campaigns(
        high, low, direction, age, atr, left_right=left_right
    ):
        dd = direction[i]
        sl = slice(oi, i + 1)
        camp_high = (
            float(np.nanmax(high[sl])) if np.isfinite(high[sl]).any() else float("nan")
        )
        camp_low = (
            float(np.nanmin(low[sl])) if np.isfinite(low[sl]).any() else float("nan")
        )
        scored = score_book(
            direction=dd,
            close_i=float(close[i]),
            atr_i=float(atr[i]) if np.isfinite(atr[i]) else float("nan"),
            camp_high=camp_high,
            camp_low=camp_low,
            prices=close[sl],
            highs=high[sl],
            lows=low[sl],
            weight=None if weight is None else weight[sl],
            pts=_campaign_pts(zig, origin=(oi, op), i=i, direction=dd, close=close),
            origin_px=op,
        )
        for key, val in scored.items():
            out[key][i] = val
    return out


def flow_panels(
    close: pd.DataFrame,
    high: pd.DataFrame | None = None,
    low: pd.DataFrame | None = None,
    volume: pd.DataFrame | None = None,
    *,
    float_shares: pd.DataFrame | None = None,
    direction: pd.DataFrame | None = None,
    dir_hold: int = 2,
    left_right: int = SWING_LEFT_RIGHT,
) -> dict[str, pd.DataFrame]:
    """资金博弈宽表。方向为 0 的日子为空。方向可传入，否则自己算 SuperTrend。"""
    close, high, low = wide_ohlc(close, high, low)
    if direction is None:
        direction = direction_panels(close, high, low, dir_hold=dir_hold)["direction"]
    else:
        direction = direction.reindex_like(close).astype(float)
    atr = atr_panel(high, low, close, window=14)
    vol = None if volume is None else volume.reindex_like(close).astype(float)
    shares = (
        None if float_shares is None else float_shares.reindex_like(close).astype(float)
    )
    out = {k: empty_like(close) for k in FLOW_KEYS}
    for col in close.columns:
        dcol = direction[col].to_numpy(dtype=float)
        ccol = close[col].to_numpy(dtype=float)
        vcol = None if vol is None else vol[col].to_numpy(dtype=float)
        scol = None if shares is None else shares[col].to_numpy(dtype=float)
        one = _flow_one(
            high[col].to_numpy(dtype=float),
            low[col].to_numpy(dtype=float),
            ccol,
            dcol,
            run_age(dcol),
            atr[col].to_numpy(dtype=float),
            bar_weight(vcol, ccol, scol),
            left_right=left_right,
        )
        for key, arr in one.items():
            out[key][col] = arr
    return out


def diagnose_flow(
    ohlcv: pd.DataFrame,
    *,
    asof: object | None = None,
) -> FlowReport:
    df = as_ohlcv_frame(ohlcv)
    asof_ts = resolve_asof(df.index, asof)
    df = df.loc[:asof_ts]
    close = df[["close"]].rename(columns={"close": "S"})
    high = df[["high"]].rename(columns={"high": "S"})
    low = df[["low"]].rename(columns={"low": "S"})
    volume = df[["volume"]].rename(columns={"volume": "S"}) if "volume" in df.columns else None
    float_shares = (
        df[["float_shares"]].rename(columns={"float_shares": "S"})
        if "float_shares" in df.columns
        else None
    )
    panels = flow_panels(
        close, high=high, low=low, volume=volume, float_shares=float_shares
    )

    def _cell(name: str) -> float:
        v = panels[name].loc[asof_ts, "S"]
        return float(v) if not np.isnan(v) else float("nan")

    report = FlowReport(
        asof=asof_ts,
        hold=_cell("hold"),
        summary="",
        components={k: _cell(k) for k in FLOW_KEYS},
    )
    report.summary = format_flow_summary(report)
    return report
