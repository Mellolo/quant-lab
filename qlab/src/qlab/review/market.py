"""① 市场：参与度、盘面流向、广度、情绪。"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from qlab.review.panels import (
    active_name_mask,
    cap_weight_market,
    chip_active_frac,
    classify_crowding,
    crowding_series,
    eligible_mask,
    float_mcap,
    lagged,
    long_short,
    realized_vol,
    simple_return,
    tail_masks,
    trailing_return,
    wide,
)
from qlab.review.spec import ReviewSpec


def classify_volume(amount: pd.Series, spec: ReviewSpec) -> pd.Series:
    """全市场成交额序列 → 量档。均量不含当日。"""
    amt = amount.astype(float)
    ma = amt.shift(1).rolling(spec.volume_window, min_periods=spec.volume_window).mean()
    ratio = amt / ma.replace(0, np.nan)
    shrinking = amt < amt.shift(1)
    prior_climax = (
        (ratio >= spec.volume_climax)
        .shift(1)
        .rolling(spec.climax_lookback, min_periods=1)
        .max()
        .fillna(0)
        .astype(bool)
    )
    prior_peak = (
        ratio.shift(1)
        .rolling(spec.climax_lookback, min_periods=1)
        .max()
    )

    out = pd.Series("正常", index=amt.index, dtype=object)
    dry = (ratio < spec.volume_dry) & shrinking
    hot = (ratio >= spec.volume_hot) & (ratio < spec.volume_climax)
    climax = ratio >= spec.volume_climax
    fade = (
        prior_climax
        & shrinking
        & (ratio < spec.climax_fade * prior_peak)
        & ~climax
    )
    out = out.mask(dry, "地量")
    out = out.mask(hot, "放量")
    out = out.mask(fade, "天量后缩")
    out = out.mask(climax, "天量")
    out = out.where(ma.notna(), other=pd.NA)
    return out


def classify_emotion(
    *,
    amount_ratio: float,
    market_ret: float,
    advance_ratio: float,
    n_limit_up: int,
    n_limit_down: int,
    attack_defense: float,
    volume_bin: str,
    above_ma20: bool,
    trend_proxy: float,
    spec: ReviewSpec,
    median_ret: float = float("nan"),
    n_eligible: int = 0,
    amount_up: bool = False,
) -> str:
    """① 情绪：结算初判，量只确认。打不清 → 中性/撕裂。

    初判看上涨面、中位数、涨跌停（极端结算）。量只做两件事：
    量大走不动 → 分配；结算已经很宽且放量 → 升狂热。
    地量/缩量不否掉「结算已宽」。指数红、中位数不红，不写乐观。
    ``volume_bin`` / ``amount_up`` 留着兼容调用，不再挡乐观/修复。
    """
    r = float(amount_ratio) if amount_ratio == amount_ratio else float("nan")
    rm = float(market_ret) if market_ret == market_ret else float("nan")
    adv = float(advance_ratio) if advance_ratio == advance_ratio else float("nan")
    md = float(median_ret) if median_ret == median_ret else float("nan")
    ad = float(attack_defense) if attack_defense == attack_defense else 0.0
    tr = float(trend_proxy) if trend_proxy == trend_proxy else 0.0
    _ = volume_bin, amount_up
    lu, ld = int(n_limit_up), int(n_limit_down)
    n = max(int(n_eligible), 0)
    ld_min = max(spec.panic_ld_min, int(spec.panic_ld_frac * n))
    lu_min = max(spec.mania_lu_min, int(spec.mania_lu_frac * n))
    hollow = (
        rm == rm and md == md and rm > 0 and md <= 0
    ) or (
        rm == rm and md == md and (rm - md) >= spec.hollow_gap and adv == adv and adv < 0.50
    )
    volume_confirms = r == r and r >= spec.volume_hot
    extreme_up = (
        lu >= 2 * max(ld, 1)
        and lu >= lu_min
        and adv == adv and adv >= spec.mania_advance
    )

    if r == r and rm == rm and r >= spec.stall_ratio and abs(rm) < spec.stall_abs_ret:
        return "分配"
    ice_limits = ld >= 2 * max(lu, 1) and ld >= ld_min and rm == rm and rm < 0
    ice_median = (
        ld >= ld_min and md == md and md <= spec.ice_ret
    )
    ice_breadth = (
        adv == adv and adv <= spec.ice_advance
        and rm == rm and rm <= spec.ice_ret
        and ad < 0
    )
    if ice_limits or ice_median or ice_breadth:
        return "冰点/恐慌"
    if extreme_up and not hollow and volume_confirms:
        return "狂热"
    strong_bounce = (
        not hollow
        and rm == rm and rm >= 0.015
        and adv == adv and adv >= spec.confirm_advance
        and lu > ld
    )
    optimistic = (
        not hollow
        and adv == adv and adv >= spec.optimistic_advance
        and rm == rm and rm > 0
        and lu > ld
    )
    if extreme_up and not hollow:
        return "乐观"
    if strong_bounce or optimistic:
        return "乐观"
    if (
        above_ma20
        and adv == adv and adv >= spec.confirm_advance
        and tr > 0
    ):
        return "认知"
    if (
        adv == adv and adv >= spec.repair_advance
        and rm == rm and 0 < rm < 0.012
    ):
        return "修复"
    return "中性/撕裂"


def _bool_wide(daily: pd.DataFrame, col: str) -> pd.DataFrame:
    return wide(daily, col).fillna(False).astype(bool)


def market_from_daily(
    daily: pd.DataFrame,
    dates: pd.DatetimeIndex,
    spec: ReviewSpec | None = None,
    *,
    margin: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """从 DailyBar 算 ①。``dates`` 是要输出的复盘日（需已有足够暖机）。

    margin : 可选，index=date，列 ``fin_balance`` / ``fin_buy``（已按 PIT 对齐到当日可见）。
    """
    spec = spec or ReviewSpec()
    elig = eligible_mask(daily)
    close = wide(daily, "close")
    ret = simple_return(close)
    amount = wide(daily, "amount")
    mcap = float_mcap(daily)
    mcap_lag = lagged(mcap)
    sus = _bool_wide(daily, "is_suspended")
    st = _bool_wide(daily, "is_st")
    listed = (~sus) & (~st) & mcap.notna() & (mcap > 0) & mcap_lag.notna() & (mcap_lag > 0)
    lu = _bool_wide(daily, "is_limit_up")
    ld = _bool_wide(daily, "is_limit_down")

    amt_sum = amount.where(elig).sum(axis=1, min_count=1)
    volume_bin = classify_volume(amt_sum, spec)
    ma20 = amt_sum.shift(1).rolling(spec.volume_window, min_periods=spec.volume_window).mean()
    ratio = amt_sum / ma20.replace(0, np.nan)
    mkt_ret = cap_weight_market(ret, mcap_lag, elig)
    flow_s = amt_sum * np.sign(mkt_ret)
    flow_w_s = amt_sum * mkt_ret
    impact_s = mkt_ret / ratio

    tradable = listed & ret.notna()
    n_up = ((ret > 0) & tradable).sum(axis=1)
    n_down = ((ret < 0) & tradable).sum(axis=1)
    n_tradable = tradable.sum(axis=1)
    adv = n_up.astype(float) / n_tradable.replace(0, np.nan)
    n_lu = (lu & elig).sum(axis=1)
    n_ld = (ld & elig).sum(axis=1)
    median_ret = ret.where(elig).median(axis=1, skipna=True)
    disp = ret.where(elig).std(axis=1, skipna=True)
    to_name = amount / mcap_lag.replace(0, np.nan)
    frac = chip_active_frac(to_name, spec)
    # 指南针 0AMV / 0号：今日流通市值 × 活筹比例；涨跌是水平的日变化
    active_mcap = (mcap * frac).where(listed).sum(axis=1, min_count=1)
    listed_mcap = mcap.where(listed).sum(axis=1, min_count=1)
    active_share = active_mcap / listed_mcap.replace(0, np.nan)
    live_ret = active_mcap / active_mcap.shift(1) - 1.0
    zero_ret = listed_mcap / listed_mcap.shift(1) - 1.0
    live_gap = live_ret - zero_ret
    hot_name = active_name_mask(to_name, listed, spec)
    n_active = hot_name.sum(axis=1)
    turnover = amt_sum / listed_mcap.replace(0, np.nan)
    to_ma = turnover.shift(1).rolling(spec.volume_window, min_periods=spec.volume_window).mean()
    to_ratio = turnover / to_ma.replace(0, np.nan)
    top1 = crowding_series(amount, elig, spec.crowding_top1)
    top10 = crowding_series(amount, elig, spec.crowding_top)
    top10_ma = top10.shift(1).rolling(spec.volume_window, min_periods=spec.volume_window).mean()
    top10_d = top10 - top10_ma

    close_mkt = cap_weight_close(close, mcap_lag, elig)
    ma_px = close_mkt.shift(1).rolling(spec.volume_window, min_periods=spec.volume_window).mean()
    fin_bal = fin_buy = None
    if margin is not None and not margin.empty:
        fin_bal = margin.get("fin_balance")
        fin_buy = margin.get("fin_buy")

    rows: list[dict[str, Any]] = []
    for date in dates:
        date = pd.Timestamp(date).normalize()
        if date not in elig.index:
            continue
        e = elig.loc[date]
        r = ret.loc[date]
        w = mcap_lag.loc[date]
        vol = realized_vol(ret, date, spec.vol_window).where(e)
        hi, lo = tail_masks(
            vol, low_is_high_pref=False, quintile=spec.quintile, min_names=spec.min_names,
        )
        attack = long_short(r, w, hi, lo, spec)
        mom = trailing_return(ret, date, spec.mom_window).where(e)
        strong, weak = tail_masks(
            mom, low_is_high_pref=False, quintile=spec.quintile, min_names=spec.min_names,
        )
        trend_proxy = long_short(r, w, strong, weak, spec)
        px = close_mkt.get(date, np.nan)
        px_ma = ma_px.get(date, np.nan)
        above = bool(px == px and px_ma == px_ma and px > px_ma)
        vbin = volume_bin.get(date, pd.NA)
        if vbin is pd.NA or (isinstance(vbin, float) and np.isnan(vbin)):
            continue
        emotion = classify_emotion(
            amount_ratio=float(ratio.get(date, np.nan)),
            market_ret=float(mkt_ret.get(date, np.nan)),
            advance_ratio=float(adv.get(date, np.nan)),
            n_limit_up=int(n_lu.get(date, 0)),
            n_limit_down=int(n_ld.get(date, 0)),
            attack_defense=float(attack) if attack == attack else 0.0,
            volume_bin=str(vbin),
            above_ma20=above,
            trend_proxy=float(trend_proxy) if trend_proxy == trend_proxy else 0.0,
            spec=spec,
            median_ret=float(median_ret.get(date, np.nan)),
            n_eligible=int(n_tradable.get(date, 0)),
            amount_up=bool(amt_sum.get(date, np.nan) > amt_sum.shift(1).get(date, np.nan)),
        )
        cbin = classify_crowding(
            float(top10.get(date, np.nan)),
            float(top10_ma.get(date, np.nan)),
            float(top10_d.get(date, np.nan)),
            float(ratio.get(date, np.nan)),
            float(mkt_ret.get(date, np.nan)),
            spec,
        )
        fb = fd = fs = np.nan
        if fin_bal is not None and date in fin_bal.index:
            fb = float(fin_bal.loc[date])
            prev = fin_bal.shift(1)
            if date in prev.index:
                fd = float(prev.loc[date])
                fd = fb - fd if fb == fb and fd == fd else np.nan
        if fin_buy is not None and date in fin_buy.index and amt_sum.get(date, 0):
            fs = float(fin_buy.loc[date]) / float(amt_sum.get(date))
        rows.append({
            "date": date,
            "amount": float(amt_sum.get(date, np.nan)),
            "amount_ma20": float(ma20.get(date, np.nan)),
            "amount_ratio": float(ratio.get(date, np.nan)),
            "volume_bin": str(vbin),
            "market_ret": float(mkt_ret.get(date, np.nan)),
            "flow": float(flow_s.get(date, np.nan)),
            "flow_weighted": float(flow_w_s.get(date, np.nan)),
            "impact": float(impact_s.get(date, np.nan)),
            "n_up": int(n_up.get(date, 0)),
            "n_down": int(n_down.get(date, 0)),
            "n_tradable": int(n_tradable.get(date, 0)),
            "advance_ratio": float(adv.get(date, np.nan)),
            "n_limit_up": int(n_lu.get(date, 0)),
            "n_limit_down": int(n_ld.get(date, 0)),
            "n_active": int(n_active.get(date, 0)),
            "live_ret": float(live_ret.get(date, np.nan)),
            "live_gap": float(live_gap.get(date, np.nan)),
            "attack_defense": float(attack) if attack == attack else np.nan,
            "emotion_bin": emotion,
            "turnover": float(turnover.get(date, np.nan)),
            "turnover_ratio": float(to_ratio.get(date, np.nan)),
            "active_mcap": float(active_mcap.get(date, np.nan)),
            "listed_mcap": float(listed_mcap.get(date, np.nan)),
            "active_share": float(active_share.get(date, np.nan)),
            "median_ret": float(median_ret.get(date, np.nan)),
            "breadth_gap": float(mkt_ret.get(date, np.nan) - median_ret.get(date, np.nan)),
            "ret_dispersion": float(disp.get(date, np.nan)),
            "crowding_top1": float(top1.get(date, np.nan)),
            "crowding_top10": float(top10.get(date, np.nan)),
            "crowding_top10_ma20": float(top10_ma.get(date, np.nan)),
            "crowding_delta": float(top10_d.get(date, np.nan)),
            "crowding_bin": cbin,
            "fin_balance": fb,
            "fin_delta": fd,
            "fin_amount_share": fs,
        })

    if not rows:
        return _empty_market()
    out = pd.DataFrame(rows).set_index("date").sort_index()
    return _cast_market(out)


def cap_weight_close(
    close: pd.DataFrame,
    mcap_lag: pd.DataFrame,
    elig: pd.DataFrame,
) -> pd.Series:
    w = mcap_lag.where(elig.reindex_like(mcap_lag).fillna(False) & (mcap_lag > 0))
    px = close.where(w.notna())
    w = w.where(px.notna())
    denom = w.sum(axis=1)
    return (px * w).sum(axis=1) / denom.replace(0, np.nan)


def _empty_market() -> pd.DataFrame:
    cols = [
        "amount", "amount_ma20", "amount_ratio", "volume_bin", "market_ret",
        "flow", "flow_weighted", "impact", "n_up", "n_down", "n_tradable",
        "advance_ratio", "n_limit_up", "n_limit_down", "n_active",
        "live_ret", "live_gap", "attack_defense", "emotion_bin",
        "turnover", "turnover_ratio", "active_mcap", "listed_mcap", "active_share",
        "median_ret", "breadth_gap", "ret_dispersion",
        "crowding_top1", "crowding_top10", "crowding_top10_ma20",
        "crowding_delta", "crowding_bin",
        "fin_balance", "fin_delta", "fin_amount_share",
    ]
    return pd.DataFrame(columns=cols).rename_axis("date")


def _cast_market(df: pd.DataFrame) -> pd.DataFrame:
    floats = [
        "amount", "amount_ma20", "amount_ratio", "market_ret",
        "flow", "flow_weighted", "impact", "advance_ratio",
        "live_ret", "live_gap", "attack_defense",
        "turnover", "turnover_ratio", "active_mcap", "listed_mcap", "active_share",
        "median_ret", "breadth_gap", "ret_dispersion",
        "crowding_top1", "crowding_top10", "crowding_top10_ma20", "crowding_delta",
        "fin_balance", "fin_delta", "fin_amount_share",
    ]
    ints = ["n_up", "n_down", "n_tradable", "n_limit_up", "n_limit_down", "n_active"]
    for c in floats:
        df[c] = df[c].astype("float64")
    for c in ints:
        df[c] = df[c].astype("int64")
    df["volume_bin"] = df["volume_bin"].astype(object)
    df["emotion_bin"] = df["emotion_bin"].astype(object)
    df["crowding_bin"] = df["crowding_bin"].astype(object)
    return df
