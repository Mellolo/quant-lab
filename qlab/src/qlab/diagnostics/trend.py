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


def _sticky_events(events: pd.DataFrame, hold: int) -> pd.DataFrame:
    """事件发生后保持 ``hold`` 根（含当日），因果滚动 max."""
    if hold <= 1:
        return (events.fillna(0.0) >= 1.0).astype("float64")
    return (
        events.fillna(0.0)
        .ge(1.0)
        .astype("float64")
        .rolling(hold, min_periods=1)
        .max()
    )


def _hysteresis_direction(raw: pd.DataFrame, hold: int = 3) -> pd.DataFrame:
    """方向切换需连续 ``hold`` 根确认，抑制单日抖动."""
    if hold <= 1:
        return raw.fillna(0.0).astype("float64")
    out = pd.DataFrame(0.0, index=raw.index, columns=raw.columns, dtype="float64")
    for col in raw.columns:
        r = raw[col].fillna(0.0).to_numpy(dtype=float)
        o = np.empty_like(r)
        cur = float(r[0]) if len(r) else 0.0
        pending = cur
        run = 0
        for i, v in enumerate(r):
            if v == cur:
                pending = v
                run = 0
            else:
                if v == pending:
                    run += 1
                else:
                    pending = v
                    run = 1
                if run >= hold:
                    cur = pending
                    run = 0
            o[i] = cur
        out[col] = o
    return out


def _efficiency_ratio(close: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """Kaufman ER：|净位移| / 路径长度，趋势越干净越接近 1."""
    close = close.sort_index().astype(float)
    net = close.diff(window).abs()
    path = close.diff().abs().rolling(window, min_periods=max(5, window // 2)).sum()
    return (net / path.replace(0.0, np.nan)).clip(0.0, 1.0)


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
    dir_hold: int = 3,
    late_hold: int = 5,
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
    # 扩展比用 slow 更稳；缺失填 0.5 中性
    strength_ext = rolling_rank_panel(slow["ext_ratio"], window=rank_window)
    # 绝对动量：把年化 smooth mom 压到 (0,1)，避免 strength 永远贴 0.5
    mom_abs = (np.tanh(mom.fillna(0.0) / 0.35).abs() + 1.0) / 2.0
    strength = (
        0.55 * strength_mom.fillna(0.5)
        + 0.25 * strength_ext.fillna(0.5)
        + 0.20 * mom_abs
    )

    dir_fast = fast["direction"]
    dir_slow = slow["direction"]
    # 主方向：slow；不再因 fast/slow 冲突清零（冲突只进 risk / conflict 旗）
    direction_raw = dir_slow.copy()
    mom_sign = np.sign(mom.fillna(0.0))
    # slow=0 时：仅当 fast 与动量同向才回退，避免横盘噪声
    fallback = (
        (dir_slow == 0)
        & (dir_fast != 0)
        & (np.sign(dir_fast) == mom_sign)
        & (strength_mom.fillna(0.5) > 0.55)
    )
    direction_raw = direction_raw.where(~fallback, dir_fast)
    # 弱市磨底：仅当结构不稳（slow=0 或 fast/slow 冲突）才压成 0；稳定 slow 方向保留
    struct_unstable = (dir_slow == 0) | ((dir_fast * dir_slow) < 0)
    weak_grind = (
        stage.isin([1.0, 4.0])
        & (strength.fillna(0.5) < 0.50)
        & struct_unstable
    )
    direction_raw = direction_raw.where(~weak_grind, 0.0)
    direction = _hysteresis_direction(direction_raw, hold=dir_hold)

    conflict_fs = (dir_fast * dir_slow) < 0
    conflict_stage = ((direction > 0) & stage.eq(4.0)) | (
        (direction < 0) & stage.eq(2.0)
    )
    conflict = conflict_fs | conflict_stage.fillna(False)

    strength_delta = strength - strength.shift(20)
    stage_prev = stage.shift(1)
    eff = _efficiency_ratio(close, window=20)

    # phase：用 slow 的 BOS/CHoCH；仅 slow CHoCH 短粘滞（fast 太噪，只进 risk）
    choch_fast = fast["choch"].fillna(0.0)
    choch_slow = slow["choch"].fillna(0.0)
    choch_sticky = _sticky_events(choch_slow >= 1.0, hold=late_hold)
    bos = slow["bos_count"]
    # 动量仍在走强且当日无新 CHoCH → 清掉粘滞 late（保住主升 early/mid）
    clear_late = (
        (choch_slow < 1.0)
        & (direction != 0)
        & (strength.fillna(0) >= 0.58)
        & (strength_delta.fillna(0) >= 0.0)
    )
    late = (
        ((choch_sticky >= 1.0) & ~clear_late)
        | ((stage_prev == 2.0) & (stage == 3.0))
        | (
            (direction > 0)
            & (dist60 > -0.01)
            & (strength_delta < -0.08)
            & stage.isin([2.0, 3.0])
        )
    )
    range_mask = direction == 0
    # 急升段 bos 常被重置：用强度+路径效率识别 mid
    mid = (direction != 0) & ~late & (
        ((bos.fillna(0) >= 2) & (strength.fillna(0) >= 0.45))
        | ((strength.fillna(0) >= 0.65) & (eff.fillna(0) >= 0.28))
        | ((strength.fillna(0) >= 0.75) & (bos.fillna(0) >= 1))
    )
    early = (direction != 0) & ~late & ~mid
    phase_code = pd.DataFrame(
        PHASE_CODE["range"], index=close.index, columns=close.columns, dtype="float64"
    )
    phase_code = phase_code.mask(early & ~range_mask, float(PHASE_CODE["early"]))
    phase_code = phase_code.mask(mid & ~range_mask, float(PHASE_CODE["mid"]))
    phase_code = phase_code.mask(range_mask & ~late, float(PHASE_CODE["range"]))
    phase_code = phase_code.mask(late, float(PHASE_CODE["late"]))

    # quality: R² + 路径效率 + 回调健康（趋势干净时明显抬升）
    pull = slow["pullback_pct"]
    pull_health = (1.0 - pull.clip(0.0, 0.5) / 0.5).clip(0.0, 1.0)
    quality = (
        0.40 * r2.fillna(0.35)
        + 0.40 * eff.fillna(0.35)
        + 0.20 * pull_health.fillna(0.5)
    )
    if volume is not None:
        volume = volume.reindex_like(close).astype(float)
        up = close > close.shift(1)
        up_vol = volume.where(up, 0.0)
        share = up_vol.rolling(20, min_periods=10).sum() / volume.rolling(
            20, min_periods=10
        ).sum().replace(0, np.nan)
        # 量能只做轻修正，避免再把 quality 拉回中性
        quality = 0.85 * quality + 0.15 * share.clip(0.0, 1.0).fillna(quality)

    # risk：fast CHoCH 当日加一点；slow 粘滞窗口加主风险
    risk = pd.DataFrame(0.0, index=close.index, columns=close.columns, dtype="float64")
    risk = risk + 0.30 * (choch_sticky >= 1.0).astype(float)
    risk = risk + 0.15 * (choch_fast >= 1.0).astype(float)
    risk = risk + 0.25 * ((stage == 3.0) | ((stage_prev == 2.0) & (stage == 3.0))).astype(
        float
    )
    risk = risk + 0.20 * (
        (direction > 0) & (dist60 > -0.01) & (strength_delta < -0.08)
    ).fillna(False).astype(float)
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
        "bos_count_fast": fast["bos_count"],
        "bos_count_slow": bos,
        "choch_fast": choch_fast,
        "choch_slow": choch_slow,
        "choch_sticky": choch_sticky,
        "dist_high_60": dist60,
        "dist_high_120": dist120,
        "smooth_mom": mom,
        "smooth_mom_r2": r2,
        "efficiency": eff,
        "ext_ratio_fast": fast["ext_ratio"],
        "ext_ratio_slow": slow["ext_ratio"],
        "strength_mom": strength_mom,
        "strength_ext": strength_ext,
    }
    if include_xs_rank:
        out["strength_xs"] = cross_sectional_rank_panel(mom)
    return out
