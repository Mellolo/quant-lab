"""② 风格：场内关注点。四轴 + 时钟 / 栖息 / 拥挤。"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from qlab.review.panels import (
    active_name_mask,
    cap_weight_market,
    classify_crowding,
    crowding_series,
    eligible_mask,
    equal_weight_market,
    float_mcap,
    industry_code_norm,
    lagged,
    limit_continuation_panel,
    long_short,
    rolling_beta,
    rolling_strength,
    score_spread,
    score_z,
    simple_return,
    tail_masks,
    total_mcap,
    trailing_return,
    turnover,
    wide,
)
from qlab.review.spec import ReviewSpec


def _axis_row(
    ret: pd.Series,
    weights: pd.Series,
    feature: pd.Series,
    spec: ReviewSpec,
    *,
    low_is_high_pref: bool,
) -> float:
    hi, lo = tail_masks(
        feature,
        low_is_high_pref=low_is_high_pref,
        quintile=spec.quintile,
        min_names=spec.min_names,
    )
    return long_short(ret, weights, hi, lo, spec)


def _pe_series(
    mcap: pd.Series,
    profit: pd.Series,
) -> pd.Series:
    m = mcap.astype(float)
    p = profit.reindex(m.index).astype(float)
    pe = m / p
    return pe.where((p > 0) & m.notna() & (m > 0))


def _industry_spread(
    ret: pd.Series,
    weights: pd.Series,
    industry: pd.Series,
    spec: ReviewSpec,
) -> float:
    codes = industry.map(industry_code_norm)
    growth = codes.isin(spec.growth_codes)
    value = codes.isin(spec.value_codes)
    if int(growth.sum()) < spec.min_names or int(value.sum()) < spec.min_names:
        return float("nan")
    return long_short(ret, weights, growth, value, spec)


def _clock_bin(
    turnover_ratio: float,
    short_s: float,
    long_s: float,
    spec: ReviewSpec,
    *,
    active_rising: bool = False,
    crowding_bin: str = "正常",
    short_paying: bool = False,
) -> str:
    """换手是主尺；结构只确认短博弈，不靠涨停家数开门。"""
    tr = float(turnover_ratio) if turnover_ratio == turnover_ratio else float("nan")
    ss = float(short_s) if short_s == short_s else 0.0
    ls = float(long_s) if long_s == long_s else 0.0
    gathered = crowding_bin in ("聚集", "拥挤待切")
    hot = tr == tr and tr >= spec.clock_hot_turnover
    confirmed = (
        tr == tr
        and tr >= spec.clock_confirm_turnover
        and (
            (active_rising and gathered)
            or short_paying
        )
    )
    if (hot or confirmed) and ss > ls:
        return "短博弈"
    if (
        tr == tr and tr < spec.clock_calm_turnover
        and ls >= ss and ls > 0
        and not gathered
    ):
        return "中线"
    return "混合"


def _habitat(
    size_m5: float,
    liq_m5: float,
    clock: str,
    spec: ReviewSpec,
) -> str:
    """五日规模与流动性/时钟同向才定栖息。单日噪声不够。"""
    sm = float(size_m5) if size_m5 == size_m5 else 0.0
    lm = float(liq_m5) if liq_m5 == liq_m5 else 0.0
    small = sm >= spec.score_soft
    large = sm <= -spec.score_soft
    alloc = lm >= spec.score_soft
    game = lm <= -spec.score_soft
    if large and (alloc or clock == "中线"):
        return "权重"
    if small and (game or clock == "短博弈"):
        return "弹性"
    if clock == "中线" and alloc and not small:
        return "权重"
    if clock == "短博弈" and game and not large:
        return "弹性"
    return "混合"


def _narrative(
    habitat: str,
    size: int,
    risk: int,
    growth: int,
    trend: int,
    *,
    liq: int = 0,
    clock: str = "混合",
    crowding_bin: str = "正常",
    lu_yest: int = 0,
    promote: int = 0,
    board2: int = 0,
) -> str:
    bits: list[str] = []
    if habitat == "权重":
        bits.append("栖息在权重")
    elif habitat == "弹性":
        bits.append("栖息在弹性")
    elif size > 0:
        bits.append("偏小盘")
    elif size < 0:
        bits.append("偏大盘")
    if growth > 0:
        bits.append("成长")
    elif growth < 0:
        bits.append("价值")
    if risk > 0:
        bits.append("要弹性")
    elif risk < 0:
        bits.append("要低波")
    if trend > 0:
        bits.append("趋势仍在")
    elif trend < 0:
        bits.append("奖反转")
    if habitat == "混合":
        if liq > 0:
            bits.append("偏配置")
        elif liq < 0:
            bits.append("偏博弈换手")
    if clock == "短博弈":
        bits.append("短博弈")
    elif clock == "中线":
        bits.append("中线")
    if lu_yest > 0:
        bits.append("昨涨停在赚")
    elif lu_yest < 0:
        bits.append("昨涨停在亏")
    if promote > 0:
        bits.append("晋级偏强")
    elif promote < 0:
        bits.append("晋级偏弱")
    if board2 > 0:
        bits.append("二板在铺")
    elif board2 < 0:
        bits.append("二板在收")
    if crowding_bin == "聚集":
        bits.append("拥挤在筑")
    elif crowding_bin == "拥挤待切":
        bits.append("拥挤待切")
    elif crowding_bin == "分散":
        bits.append("分散无共识")
    if not bits:
        return "风格不清晰"
    if len(bits) == 1:
        return bits[0]
    head = bits[0]
    rest = bits[1:]
    if head.startswith("偏") and rest and rest[0] in ("成长", "价值"):
        return "、".join([head + rest[0], *rest[1:]])
    return "、".join(bits)


def style_from_daily(
    daily: pd.DataFrame,
    dates: pd.DatetimeIndex,
    spec: ReviewSpec | None = None,
    *,
    pe_profit: pd.DataFrame | None = None,
    industry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """从 DailyBar 算 ②。

    pe_profit : date × symbol 的 T 日可见净利润（调用方保证 as-of ≤ 该日，
                本函数再 shift 成 T-1）。
    industry  : date × symbol 的申万一级代码。
    """
    spec = spec or ReviewSpec()
    elig = eligible_mask(daily)
    close = wide(daily, "close")
    ret = simple_return(close)
    amount = wide(daily, "amount")
    mcap = float_mcap(daily)
    mcap_lag = lagged(mcap)
    tmcap_lag = lagged(total_mcap(daily))
    to = turnover(daily)
    to_mkt = to.where(elig).mean(axis=1, skipna=True)
    to_ma = to_mkt.shift(1).rolling(spec.volume_window, min_periods=spec.volume_window).mean()
    to_ratio = to_mkt / to_ma.replace(0, np.nan)
    hot_name = active_name_mask(to, elig, spec)
    n_active = hot_name.sum(axis=1)
    n_active_ma = n_active.shift(1).rolling(
        spec.volume_window, min_periods=spec.volume_window,
    ).mean()
    active_rising = (n_active > n_active.shift(1)) & (n_active > n_active_ma)
    ew_mkt = equal_weight_market(ret, elig)

    profit_lag = None
    if pe_profit is not None and not pe_profit.empty:
        profit_lag = lagged(pe_profit.reindex(index=ret.index, columns=ret.columns))

    ind = None
    if industry is not None and not industry.empty:
        ind = industry.reindex(index=ret.index, columns=ret.columns)

    raw: dict[str, list[float]] = {
        "size_s": [], "risk_s": [], "growth_s": [], "trend_s": [],
        "short_s": [], "long_s": [], "liq_s": [],
    }
    top10 = crowding_series(amount, elig, spec.crowding_top)
    top10_ma = top10.shift(1).rolling(spec.volume_window, min_periods=spec.volume_window).mean()
    top10_d = top10 - top10_ma
    mkt_ret = cap_weight_market(ret, mcap_lag, elig)
    amt_sum = amount.where(elig).sum(axis=1, min_count=1)
    amt_ma = amt_sum.shift(1).rolling(spec.volume_window, min_periods=spec.volume_window).mean()
    amt_ratio = amt_sum / amt_ma.replace(0, np.nan)
    lu = wide(daily, "is_limit_up").reindex_like(elig).fillna(False).astype(bool)
    lu_panel = limit_continuation_panel(ret, lu, elig, ew_mkt, spec)
    meta: list[dict[str, Any]] = []

    for date in dates:
        date = pd.Timestamp(date).normalize()
        if date not in elig.index:
            continue
        e = elig.loc[date]
        r = ret.loc[date]
        w = mcap_lag.loc[date]
        feat_mcap = mcap_lag.loc[date].where(e)
        size_s = _axis_row(r, w, feat_mcap, spec, low_is_high_pref=True)

        beta = rolling_beta(ret, ew_mkt, date, spec).where(e)
        risk_s = _axis_row(r, w, beta, spec, low_is_high_pref=False)

        growth_s = float("nan")
        growth_source = ""
        if profit_lag is not None and date in profit_lag.index:
            pe = _pe_series(tmcap_lag.loc[date], profit_lag.loc[date]).where(e)
            growth_s = _axis_row(r, w, pe, spec, low_is_high_pref=False)
            if growth_s == growth_s:
                growth_source = "pe"
        if growth_s != growth_s and ind is not None and date in ind.index:
            growth_s = _industry_spread(r.where(e), w, ind.loc[date], spec)
            if growth_s == growth_s:
                growth_source = "industry"

        mom20 = trailing_return(ret, date, spec.mom_window).where(e)
        trend_s = _axis_row(r, w, mom20, spec, low_is_high_pref=False)
        mom5 = trailing_return(ret, date, spec.short_mom_window).where(e)
        mom60 = trailing_return(ret, date, spec.long_mom_window).where(e)
        short_s = _axis_row(r, w, mom5, spec, low_is_high_pref=False)
        long_s = _axis_row(r, w, mom60, spec, low_is_high_pref=False)

        to_lag = lagged(to).loc[date].where(e)
        liq_s = _axis_row(r, w, to_lag, spec, low_is_high_pref=True)
        crowd = float(top10.get(date, np.nan))
        cdelta = float(top10_d.get(date, np.nan))
        cbin = classify_crowding(
            crowd, float(top10_ma.get(date, np.nan)), cdelta,
            float(amt_ratio.get(date, np.nan)),
            float(mkt_ret.get(date, np.nan)),
            spec,
        )
        rising = bool(active_rising.get(date, False))
        lu_s = float(lu_panel["lu_yest_s"].get(date, np.nan))
        paying = lu_s == lu_s and lu_s >= spec.score_soft
        clock = _clock_bin(
            float(to_ratio.get(date, np.nan)), short_s, long_s, spec,
            active_rising=rising, crowding_bin=cbin, short_paying=paying,
        )

        raw["size_s"].append(size_s)
        raw["risk_s"].append(risk_s)
        raw["growth_s"].append(growth_s)
        raw["trend_s"].append(trend_s)
        raw["short_s"].append(short_s)
        raw["long_s"].append(long_s)
        raw["liq_s"].append(liq_s)
        meta.append({
            "date": date,
            "growth_source": growth_source,
            "clock_bin": clock,
            "crowding": crowd,
            "crowding_delta": cdelta,
            "crowding_bin": cbin,
        })

    if not meta:
        return _empty_style()

    idx = pd.DatetimeIndex([m["date"] for m in meta], name="date")
    s_size = pd.Series(raw["size_s"], index=idx, dtype="float64")
    s_risk = pd.Series(raw["risk_s"], index=idx, dtype="float64")
    s_growth = pd.Series(raw["growth_s"], index=idx, dtype="float64")
    s_trend = pd.Series(raw["trend_s"], index=idx, dtype="float64")
    s_liq = pd.Series(raw["liq_s"], index=idx, dtype="float64")

    out = pd.DataFrame({
        "size_s": s_size,
        "size_m5": rolling_strength(s_size, spec.strength_window),
        "size_score": s_size.map(lambda x: score_spread(float(x), spec)).astype("int64"),
        "risk_s": s_risk,
        "risk_m5": rolling_strength(s_risk, spec.strength_window),
        "risk_score": s_risk.map(lambda x: score_spread(float(x), spec)).astype("int64"),
        "growth_s": s_growth,
        "growth_m5": rolling_strength(s_growth, spec.strength_window),
        "growth_score": s_growth.map(lambda x: score_spread(float(x), spec)).astype("int64"),
        "growth_source": [m["growth_source"] for m in meta],
        "trend_s": s_trend,
        "trend_m5": rolling_strength(s_trend, spec.strength_window),
        "trend_score": s_trend.map(lambda x: score_spread(float(x), spec)).astype("int64"),
        "clock_bin": [m["clock_bin"] for m in meta],
        "liq_s": s_liq,
        "liq_m5": rolling_strength(s_liq, spec.strength_window),
        "liq_score": s_liq.map(lambda x: score_spread(float(x), spec)).astype("int64"),
        "crowding": [m["crowding"] for m in meta],
        "crowding_delta": [m["crowding_delta"] for m in meta],
        "crowding_bin": [m["crowding_bin"] for m in meta],
    }, index=idx)
    lu_al = lu_panel.reindex(idx)
    out["lu_yest_ret"] = lu_al["lu_yest_ret"].astype("float64")
    out["lu_yest_s"] = lu_al["lu_yest_s"].astype("float64")
    out["lu_yest_m5"] = rolling_strength(
        lu_panel["lu_yest_s"], spec.strength_window,
    ).reindex(idx)
    out["lu_yest_score"] = out["lu_yest_s"].map(
        lambda x: score_spread(float(x), spec)
    ).astype("int64")
    out["n_lu_yest"] = lu_al["n_lu_yest"].fillna(0).astype("int64")
    out["lu_promote"] = lu_al["lu_promote"].astype("float64")
    out["lu_promote_excess"] = lu_al["lu_promote_excess"].astype("float64")
    out["lu_promote_z"] = lu_al["lu_promote_z"].astype("float64")
    out["lu_promote_score"] = out["lu_promote_z"].map(
        lambda x: score_z(float(x), spec)
    ).astype("int64")
    out["n_board2"] = lu_al["n_board2"].fillna(0).astype("int64")
    out["board2_share"] = lu_al["board2_share"].astype("float64")
    out["board2_z"] = lu_al["board2_z"].astype("float64")
    out["board2_score"] = out["board2_z"].map(
        lambda x: score_z(float(x), spec)
    ).astype("int64")
    out["max_board"] = lu_al["max_board"].fillna(0).astype("int64")
    out["habitat"] = [
        _habitat(float(a), float(b), str(c), spec)
        for a, b, c in zip(out["size_m5"], out["liq_m5"], out["clock_bin"], strict=True)
    ]
    out["narrative"] = [
        _narrative(
            str(h),
            score_spread(float(sm), spec),
            score_spread(float(rm5), spec),
            score_spread(float(gm), spec),
            score_spread(float(tm), spec),
            liq=score_spread(float(lm), spec),
            clock=str(ck),
            crowding_bin=str(cb),
            lu_yest=int(ly),
            promote=int(pr),
            board2=int(b2),
        )
        for h, sm, rm5, gm, tm, lm, ck, cb, ly, pr, b2 in zip(
            out["habitat"], out["size_m5"], out["risk_m5"],
            out["growth_m5"], out["trend_m5"], out["liq_m5"],
            out["clock_bin"], out["crowding_bin"],
            out["lu_yest_score"], out["lu_promote_score"], out["board2_score"],
            strict=True,
        )
    ]
    out["crowding"] = out["crowding"].astype("float64")
    out["crowding_delta"] = out["crowding_delta"].astype("float64")
    out["growth_source"] = out["growth_source"].astype(object)
    out["clock_bin"] = out["clock_bin"].astype(object)
    out["habitat"] = out["habitat"].astype(object)
    out["crowding_bin"] = out["crowding_bin"].astype(object)
    out["narrative"] = out["narrative"].astype(object)
    return out


def _empty_style() -> pd.DataFrame:
    cols = [
        "size_s", "size_m5", "size_score",
        "risk_s", "risk_m5", "risk_score",
        "growth_s", "growth_m5", "growth_score", "growth_source",
        "trend_s", "trend_m5", "trend_score",
        "clock_bin", "habitat", "liq_s", "liq_m5", "liq_score",
        "crowding", "crowding_delta", "crowding_bin",
        "lu_yest_ret", "lu_yest_s", "lu_yest_m5", "lu_yest_score", "n_lu_yest",
        "lu_promote", "lu_promote_excess", "lu_promote_z", "lu_promote_score",
        "n_board2", "board2_share", "board2_z", "board2_score", "max_board",
        "narrative",
    ]
    return pd.DataFrame(columns=cols).rename_axis("date")
