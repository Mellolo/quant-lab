"""趋势诊断 — 五轴评价一段日线 K 线「现在怎么走」.

独立于采样门与因子库；算法在 :mod:`qlab.core.price_panels`，本模块提供
单标的报告与全市场宽表门面。

五轴
----
- ``direction`` ∈ {-1, 0, +1}
- ``strength`` ∈ [0, 1]
- ``phase`` ∈ {early, mid, late, range}
- ``quality`` ∈ [0, 1]
- ``risk`` ∈ [0, 1]
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from qlab.core.price_panels import (
    cross_sectional_rank_panel,
    dist_to_high_panel,
    rolling_rank_panel,
    smooth_momentum_panel,
    smooth_momentum_r2_panel,
    stage_panel,
    structure_state_panels,
)

PHASE_CODE = {"range": 0, "early": 1, "mid": 2, "late": 3}
_CODE_PHASE = {v: k for k, v in PHASE_CODE.items()}


@dataclass
class TrendReport:
    """单标的趋势诊断结果（某一确认日收盘后）。"""

    asof: pd.Timestamp
    direction: int
    strength: float
    phase: str
    quality: float
    risk: float
    regime: str
    conflict: bool
    summary: str
    components: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["asof"] = str(pd.Timestamp(self.asof).date())
        return d


def _as_ohlcv_frame(ohlcv: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(ohlcv, pd.DataFrame):
        raise TypeError("ohlcv 必须是 DataFrame")
    df = ohlcv.sort_index().copy()
    if "close" not in df.columns:
        raise ValueError("ohlcv 至少需要 close 列")
    if "high" not in df.columns:
        df["high"] = df["close"]
    if "low" not in df.columns:
        df["low"] = df["close"]
    return df


def _phase_from_rules(
    *,
    direction: float,
    bos_count: float,
    choch: float,
    stage: float,
    dist60: float,
    dist120: float,
    strength: float,
    strength_delta: float,
    stage_prev: float,
) -> str:
    if choch >= 1.0:
        return "late"
    if (
        not np.isnan(stage)
        and not np.isnan(stage_prev)
        and stage_prev == 2.0
        and stage == 3.0
    ):
        return "late"
    if (
        direction != 0
        and not np.isnan(dist60)
        and dist60 > -0.02
        and not np.isnan(strength_delta)
        and strength_delta < 0
    ):
        return "late"

    if direction == 0:
        return "range"
    if not np.isnan(stage) and stage in (1.0, 3.0) and (
        np.isnan(strength) or strength < 0.4
    ):
        return "range"

    far_from_high = (
        (direction > 0 and not np.isnan(dist120) and dist120 < -0.08)
        or (direction < 0 and not np.isnan(dist120) and dist120 > -0.92)
    )
    # 空头「远离低点」用对称近似：多头用 dist_to_high；空头 phase early 用 bos<=1 为主
    if direction != 0 and (np.isnan(bos_count) or bos_count <= 1) and (
        far_from_high or direction < 0
    ):
        if direction > 0 and far_from_high:
            return "early"
        if direction < 0 and (np.isnan(bos_count) or bos_count <= 1):
            return "early"

    if (
        direction != 0
        and not np.isnan(bos_count)
        and bos_count >= 2
        and not np.isnan(strength)
        and strength >= 0.6
    ):
        return "mid"

    if direction != 0 and (np.isnan(bos_count) or bos_count <= 1):
        return "early"
    if direction != 0:
        return "mid"
    return "range"


def _regime_label(direction: int, phase: str, strength: float, stage: float) -> str:
    if phase == "range" or direction == 0:
        return "range"
    side = "bull" if direction > 0 else "bear"
    st = ""
    if not np.isnan(stage):
        st = f"/S{int(stage)}"
    str_tag = "strong" if (not np.isnan(strength) and strength >= 0.7) else "mild"
    return f"{side}_{phase}_{str_tag}{st}"


def format_trend_summary(report: TrendReport) -> str:
    """人类可读摘要（中文）."""
    dir_map = {1: "多头", -1: "空头", 0: "震荡/无序"}
    phase_map = {
        "early": "初期",
        "mid": "中期",
        "late": "末期",
        "range": "区间",
    }
    conf = "；结构与 Stage 冲突" if report.conflict else ""
    return (
        f"{pd.Timestamp(report.asof).date()} "
        f"{dir_map.get(report.direction, '?')}·{phase_map.get(report.phase, report.phase)}，"
        f"强度 {report.strength:.2f}，质量 {report.quality:.2f}，风险 {report.risk:.2f}"
        f"{conf}"
    )


def diagnose_trend(
    ohlcv: pd.DataFrame,
    *,
    asof: object | None = None,
    left_right_fast: int = 3,
    left_right_slow: int = 5,
    stage_ma: int = 200,
    stage_slope: int = 20,
    mom_window: int = 60,
    rank_window: int = 252,
) -> TrendReport:
    """诊断单标的日线趋势（确认日收盘语义）.

    Args:
        ohlcv: index=date，列至少 ``close``；建议含 ``high``/``low``/``volume``.
        asof: 评价日；默认最后一根 bar。
    """
    df = _as_ohlcv_frame(ohlcv)
    if asof is None:
        asof_ts = pd.Timestamp(df.index[-1])
    else:
        asof_ts = pd.Timestamp(asof)
        if asof_ts not in df.index:
            # 取不超过 asof 的最后交易日
            ok = df.index[df.index <= asof_ts]
            if len(ok) == 0:
                raise ValueError(f"asof={asof} 早于序列起点")
            asof_ts = pd.Timestamp(ok[-1])
    df = df.loc[:asof_ts]

    close = df[["close"]].rename(columns={"close": "S"})
    high = df[["high"]].rename(columns={"high": "S"})
    low = df[["low"]].rename(columns={"low": "S"})
    vol = None
    if "volume" in df.columns:
        vol = df[["volume"]].rename(columns={"volume": "S"})

    panels = trend_panels(
        close,
        high=high,
        low=low,
        volume=vol,
        left_right_fast=left_right_fast,
        left_right_slow=left_right_slow,
        stage_ma=stage_ma,
        stage_slope=stage_slope,
        mom_window=mom_window,
        rank_window=rank_window,
        include_xs_rank=False,
    )
    row = {k: float(panels[k].loc[asof_ts, "S"]) for k in panels}
    direction = int(row.get("direction", 0) if not np.isnan(row.get("direction", np.nan)) else 0)
    phase_code = row.get("phase_code", 0)
    phase = _CODE_PHASE.get(int(phase_code) if not np.isnan(phase_code) else 0, "range")
    strength = float(row["strength"]) if not np.isnan(row.get("strength", np.nan)) else float("nan")
    quality = float(row["quality"]) if not np.isnan(row.get("quality", np.nan)) else float("nan")
    risk = float(row["risk"]) if not np.isnan(row.get("risk", np.nan)) else float("nan")
    conflict = bool(row.get("conflict", 0) >= 1)
    stage = row.get("stage", float("nan"))
    regime = _regime_label(direction, phase, strength, stage)
    components = {
        "stage": stage,
        "direction_fast": row.get("direction_fast"),
        "direction_slow": row.get("direction_slow"),
        "bos_count_fast": row.get("bos_count_fast"),
        "choch_fast": row.get("choch_fast"),
        "dist_high_60": row.get("dist_high_60"),
        "dist_high_120": row.get("dist_high_120"),
        "smooth_mom": row.get("smooth_mom"),
        "smooth_mom_r2": row.get("smooth_mom_r2"),
        "ext_ratio_fast": row.get("ext_ratio_fast"),
        "strength_mom": row.get("strength_mom"),
        "strength_ext": row.get("strength_ext"),
    }
    report = TrendReport(
        asof=asof_ts,
        direction=direction,
        strength=0.0 if np.isnan(strength) else strength,
        phase=phase,
        quality=0.0 if np.isnan(quality) else quality,
        risk=0.0 if np.isnan(risk) else risk,
        regime=regime,
        conflict=conflict,
        summary="",
        components=components,
    )
    report.summary = format_trend_summary(report)
    return report


def trend_panels(
    close: pd.DataFrame,
    high: pd.DataFrame | None = None,
    low: pd.DataFrame | None = None,
    volume: pd.DataFrame | None = None,
    *,
    left_right_fast: int = 3,
    left_right_slow: int = 5,
    stage_ma: int = 200,
    stage_slope: int = 20,
    mom_window: int = 60,
    rank_window: int = 252,
    include_xs_rank: bool = True,
) -> dict[str, pd.DataFrame]:
    """全市场趋势诊断宽表.

    Returns:
        含 ``direction, strength, phase_code, quality, risk, conflict, stage, ...``
        等 DataFrame 的字典（列=symbol）。
    """
    close = close.sort_index().astype(float)
    if high is None:
        high = close
    else:
        high = high.reindex_like(close).astype(float)
    if low is None:
        low = close
    else:
        low = low.reindex_like(close).astype(float)

    fast = structure_state_panels(
        close, high=high, low=low, left_right=left_right_fast
    )
    slow = structure_state_panels(
        close, high=high, low=low, left_right=left_right_slow
    )
    stage = stage_panel(close, ma_window=stage_ma, slope_lookback=stage_slope)
    dist60 = dist_to_high_panel(close, window=60)
    dist120 = dist_to_high_panel(close, window=120)
    mom = smooth_momentum_panel(close, window=mom_window)
    r2 = smooth_momentum_r2_panel(close, window=mom_window)
    strength_mom = rolling_rank_panel(mom, window=rank_window)
    # ext_ratio → 分位；缺失填 0.5 中性
    strength_ext = rolling_rank_panel(fast["ext_ratio"], window=rank_window)
    strength = (strength_mom.fillna(0.5) + strength_ext.fillna(0.5)) / 2.0

    dir_fast = fast["direction"]
    dir_slow = slow["direction"]
    # 主方向：优先 slow；fast/slow 相反 → 0 并 conflict
    direction = dir_slow.copy()
    conflict = (dir_fast * dir_slow) < 0
    direction = direction.where(~conflict, 0.0)
    # slow 为 0 时回退 fast
    direction = direction.where(dir_slow != 0, dir_fast)

    strength_delta = strength - strength.shift(20)
    stage_prev = stage.shift(1)

    # phase 逐元素规则（矩阵化）
    choch = fast["choch"].fillna(0.0)
    bos = fast["bos_count"]
    late = (
        (choch >= 1.0)
        | ((stage_prev == 2.0) & (stage == 3.0))
        | ((direction != 0) & (dist60 > -0.02) & (strength_delta < 0))
    )
    range_mask = (direction == 0) | (
        stage.isin([1.0, 3.0]) & (strength.fillna(0.0) < 0.4)
    )
    early = (
        (direction != 0)
        & (bos.fillna(0) <= 1)
        & (
            ((direction > 0) & (dist120 < -0.08))
            | (direction < 0)
        )
    )
    mid = (
        (direction != 0)
        & (bos.fillna(0) >= 2)
        & (strength.fillna(0) >= 0.6)
    )
    phase_code = pd.DataFrame(
        PHASE_CODE["range"], index=close.index, columns=close.columns, dtype="float64"
    )
    phase_code = phase_code.mask(early & ~late & ~range_mask, float(PHASE_CODE["early"]))
    phase_code = phase_code.mask(mid & ~late & ~range_mask, float(PHASE_CODE["mid"]))
    # 未命中 mid/early 但有方向 → mid 弱默认 / early
    weak_trend = (direction != 0) & ~late & ~range_mask & ~early & ~mid
    phase_code = phase_code.mask(
        weak_trend & (bos.fillna(0) <= 1), float(PHASE_CODE["early"])
    )
    phase_code = phase_code.mask(
        weak_trend & (bos.fillna(0) > 1), float(PHASE_CODE["mid"])
    )
    phase_code = phase_code.mask(range_mask & ~late, float(PHASE_CODE["range"]))
    phase_code = phase_code.mask(late, float(PHASE_CODE["late"]))
    phase_code = phase_code.mask(conflict, float(PHASE_CODE["range"]))

    # quality: R² + 回调不过深（pullback 越小越好，映射到 0~1）
    pull = fast["pullback_pct"]
    pull_health = (1.0 - pull.clip(0.0, 0.5) / 0.5).clip(0.0, 1.0)
    quality = (r2.fillna(0.5) + pull_health.fillna(0.5)) / 2.0
    if volume is not None:
        volume = volume.reindex_like(close).astype(float)
        up = close > close.shift(1)
        # 20 日上涨日量占比
        up_vol = volume.where(up, 0.0)
        share = up_vol.rolling(20, min_periods=10).sum() / volume.rolling(
            20, min_periods=10
        ).sum().replace(0, np.nan)
        quality = (quality + share.clip(0.0, 1.0).fillna(quality)) / 2.0

    # risk
    risk = pd.DataFrame(0.0, index=close.index, columns=close.columns, dtype="float64")
    risk = risk + 0.35 * (choch >= 1.0).astype(float)
    risk = risk + 0.25 * ((stage == 3.0) | ((stage_prev == 2.0) & (stage == 3.0))).astype(
        float
    )
    risk = risk + 0.20 * ((dist60 > -0.02) & (strength_delta < 0)).fillna(False).astype(
        float
    )
    risk = risk + 0.20 * (strength_delta < -0.1).fillna(False).astype(float)
    risk = risk + 0.15 * conflict.astype(float)
    risk = risk.clip(0.0, 1.0)

    out: dict[str, pd.DataFrame] = {
        "direction": direction.astype("float64"),
        "direction_fast": dir_fast,
        "direction_slow": dir_slow,
        "strength": strength,
        "phase_code": phase_code,
        "quality": quality,
        "risk": risk,
        "conflict": conflict.astype("float64"),
        "stage": stage,
        "bos_count_fast": bos,
        "choch_fast": choch,
        "dist_high_60": dist60,
        "dist_high_120": dist120,
        "smooth_mom": mom,
        "smooth_mom_r2": r2,
        "ext_ratio_fast": fast["ext_ratio"],
        "strength_mom": strength_mom,
        "strength_ext": strength_ext,
    }
    if include_xs_rank:
        out["strength_xs"] = cross_sectional_rank_panel(mom)
    return out
