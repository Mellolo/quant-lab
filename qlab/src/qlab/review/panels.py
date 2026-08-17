"""复盘共用的价量面板：收益、市值、分组、多空差。

分组特征由调用方保证是 T-1 可知的；本文件不解释日历。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlab.review.spec import ReviewSpec


def wide(daily: pd.DataFrame, col: str) -> pd.DataFrame:
    """DailyBar 长表 → date × symbol。"""
    return daily[col].unstack("symbol").sort_index()


def eligible_mask(daily: pd.DataFrame) -> pd.DataFrame:
    """宇宙已在 daily 里截过。再去掉停牌 / ST / 无成交 / 无收盘价。"""
    close = wide(daily, "close")
    amount = wide(daily, "amount")
    sus = wide(daily, "is_suspended").fillna(True).astype(bool)
    st = wide(daily, "is_st").fillna(False).astype(bool)
    return close.notna() & (amount.fillna(0) > 0) & ~sus & ~st


def simple_return(close: pd.DataFrame) -> pd.DataFrame:
    return close / close.shift(1) - 1.0


def float_mcap(daily: pd.DataFrame) -> pd.DataFrame:
    """不复权收盘 × 流通股本。停牌日为 NaN。"""
    px = wide(daily, "close_raw")
    sh = wide(daily, "float_shares")
    return px * sh


def total_mcap(daily: pd.DataFrame) -> pd.DataFrame:
    px = wide(daily, "close_raw")
    sh = wide(daily, "total_shares")
    return px * sh


def lagged(frame: pd.DataFrame) -> pd.DataFrame:
    """整表下移一日：T 行变成 T-1 的值。"""
    return frame.shift(1)


def where_eligible(frame: pd.DataFrame, elig: pd.DataFrame) -> pd.DataFrame:
    aligned = elig.reindex(index=frame.index, columns=frame.columns).fillna(False)
    return frame.where(aligned)


def cap_weighted_return(
    ret: pd.Series,
    weights: pd.Series,
    mask: pd.Series,
    min_names: int,
) -> float:
    """mask 内、权重>0、收益非空的流通市值加权收益。不够只数则 NaN。"""
    w = weights.reindex(mask.index)
    r = ret.reindex(mask.index)
    keep = mask.fillna(False) & r.notna() & w.notna() & (w > 0)
    if int(keep.sum()) < min_names:
        return float("nan")
    ww = w[keep].astype(float)
    ww = ww / ww.sum()
    return float((r[keep].astype(float) * ww).sum())


def tail_masks(
    feature: pd.Series,
    *,
    low_is_high_pref: bool,
    quintile: float,
    min_names: int,
) -> tuple[pd.Series, pd.Series]:
    """返回 (高偏好组, 低偏好组) 的 bool Series，index=symbol。

    ``low_is_high_pref=True`` 用于规模（小市值是正分一侧）。
    """
    s = feature.astype(float).replace([np.inf, -np.inf], np.nan).dropna()
    empty = pd.Series(False, index=feature.index)
    if len(s) < min_names * 2:
        return empty, empty
    lo = float(s.quantile(quintile))
    hi = float(s.quantile(1.0 - quintile))
    low_tail = feature.index.isin(s.index[s <= lo])
    high_tail = feature.index.isin(s.index[s >= hi])
    low_s = pd.Series(low_tail, index=feature.index)
    high_s = pd.Series(high_tail, index=feature.index)
    if int(low_s.sum()) < min_names or int(high_s.sum()) < min_names:
        return empty, empty
    if low_is_high_pref:
        return low_s, high_s
    return high_s, low_s


def long_short(
    ret: pd.Series,
    weights: pd.Series,
    high_pref: pd.Series,
    low_pref: pd.Series,
    spec: ReviewSpec,
) -> float:
    up = cap_weighted_return(ret, weights, high_pref, spec.min_names)
    dn = cap_weighted_return(ret, weights, low_pref, spec.min_names)
    if np.isnan(up) or np.isnan(dn):
        return float("nan")
    return float(up - dn)


def score_spread(s: float, spec: ReviewSpec) -> int:
    """看不清打 0。"""
    if s != s:  # NaN
        return 0
    if s >= spec.score_hard:
        return 2
    if s >= spec.score_soft:
        return 1
    if s <= -spec.score_hard:
        return -2
    if s <= -spec.score_soft:
        return -1
    return 0


def score_z(z: float, spec: ReviewSpec) -> int:
    """时间序列 z：对照自己近窗，不是样本里写死的 29%、32 家。"""
    if z != z:
        return 0
    if z >= spec.z_hard:
        return 2
    if z >= spec.z_soft:
        return 1
    if z <= -spec.z_hard:
        return -2
    if z <= -spec.z_soft:
        return -1
    return 0


def trailing_z(s: pd.Series, window: int) -> pd.Series:
    """(今日 − 昨日及以前的均) / 近窗标准差。基准不含 T。"""
    x = s.astype(float)
    prior = x.shift(1)
    min_p = max(8, window // 2)
    mu = prior.rolling(window, min_periods=min_p).mean()
    sd = prior.rolling(window, min_periods=min_p).std()
    return (x - mu) / sd.replace(0, np.nan)


def limit_up_streak(lu: pd.DataFrame) -> pd.DataFrame:
    """每列连续涨停天数（今日未涨停则 0）。"""
    arr = lu.fillna(False).to_numpy(dtype=np.int8, copy=False)
    out = np.zeros(arr.shape, dtype=np.int16)
    if len(arr):
        out[0] = arr[0]
        for i in range(1, len(arr)):
            out[i] = (out[i - 1] + 1) * arr[i]
    return pd.DataFrame(out, index=lu.index, columns=lu.columns)


def limit_continuation_panel(
    ret: pd.DataFrame,
    lu: pd.DataFrame,
    elig: pd.DataFrame,
    bench: pd.Series,
    spec: ReviewSpec,
) -> pd.DataFrame:
    """短线风格细读：昨涨停今日、晋级、二板。只出相对量，不写死家数档。"""
    mask = elig.reindex_like(ret).fillna(False)
    hit = (lu.reindex_like(ret).fillna(False).astype(bool) & mask).astype(np.int8)
    yest = hit.shift(1).fillna(0).astype(bool)
    hit = hit.astype(bool)
    n_yest = yest.sum(axis=1)
    yest_ret = ret.where(yest).mean(axis=1, skipna=True)
    yest_ret = yest_ret.where(n_yest >= spec.min_names)
    n_lu = hit.sum(axis=1)
    n_tr = mask.sum(axis=1).replace(0, np.nan)
    promote = (hit & yest).sum(axis=1) / n_yest.replace(0, np.nan)
    promote = promote.where(n_yest >= spec.min_names)
    excess = promote - (n_lu / n_tr)
    streak = limit_up_streak(hit)
    n_board = (streak >= spec.board_min).sum(axis=1)
    share = n_board / n_lu.replace(0, np.nan)
    spread = yest_ret - bench.reindex(yest_ret.index)
    return pd.DataFrame({
        "lu_yest_ret": yest_ret.astype("float64"),
        "lu_yest_s": spread.astype("float64"),
        "n_lu_yest": n_yest.astype("int64"),
        "lu_promote": promote.astype("float64"),
        "lu_promote_excess": excess.astype("float64"),
        "lu_promote_z": trailing_z(excess, spec.short_lookback),
        "n_board2": n_board.astype("int64"),
        "board2_share": share.astype("float64"),
        "board2_z": trailing_z(share, spec.short_lookback),
        "max_board": streak.max(axis=1).fillna(0).astype("int64"),
    })


def rolling_strength(s: pd.Series, window: int) -> pd.Series:
    """M^{(n)} = Π(1+S) − 1，按日滚动。"""
    x = s.astype(float)
    return (1.0 + x).rolling(window, min_periods=window).apply(
        lambda v: float(np.prod(v) - 1.0), raw=True
    )


def rolling_beta(
    ret: pd.DataFrame,
    market: pd.Series,
    date: pd.Timestamp,
    spec: ReviewSpec,
) -> pd.Series:
    """对等权市场、窗口 [T-W, T-1] 的个股 β。有效点不够则该票 NaN。"""
    hist = ret.loc[:date]
    if len(hist) < 2:
        return pd.Series(np.nan, index=ret.columns)
    hist = hist.iloc[:-1].tail(spec.beta_window)
    rm = market.reindex(hist.index).astype(float)
    ok = rm.notna()
    hist = hist.loc[ok]
    rm = rm.loc[ok]
    if len(hist) < spec.min_beta_obs:
        return pd.Series(np.nan, index=ret.columns)
    rm_c = rm - rm.mean()
    var = float((rm_c ** 2).sum())
    if var < 1e-16:
        return pd.Series(np.nan, index=ret.columns)
    demeaned = hist.sub(hist.mean(axis=0), axis=1)
    cov = demeaned.mul(rm_c, axis=0).sum(axis=0)
    n_obs = hist.notna().sum(axis=0)
    beta = cov / var
    return beta.where(n_obs >= spec.min_beta_obs)


def realized_vol(ret: pd.DataFrame, date: pd.Timestamp, window: int) -> pd.Series:
    """T-1 可知的近 window 日收益标准差。"""
    hist = ret.loc[:date]
    if len(hist) < 2:
        return pd.Series(np.nan, index=ret.columns)
    hist = hist.iloc[:-1].tail(window)
    return hist.std(axis=0, ddof=1)


def trailing_return(ret: pd.DataFrame, date: pd.Timestamp, window: int) -> pd.Series:
    """截至 T-1 的 window 日累计收益。有效点不足则 NaN。"""
    hist = ret.loc[:date]
    if len(hist) < 2:
        return pd.Series(np.nan, index=ret.columns)
    hist = hist.iloc[:-1].tail(window)
    n_ok = hist.notna().sum(axis=0)
    prod = (1.0 + hist).prod(axis=0, skipna=True) - 1.0
    return prod.where(n_ok >= max(2, window // 2))


def equal_weight_market(ret: pd.DataFrame, elig: pd.DataFrame) -> pd.Series:
    r = ret.where(elig.reindex_like(ret).fillna(False))
    return r.mean(axis=1, skipna=True)


def cap_weight_market(
    ret: pd.DataFrame,
    mcap_lag: pd.DataFrame,
    elig: pd.DataFrame,
) -> pd.Series:
    w = mcap_lag.where(elig.reindex_like(mcap_lag).fillna(False) & (mcap_lag > 0))
    r = ret.where(w.notna())
    w = w.where(r.notna())
    denom = w.sum(axis=1)
    return (r * w).sum(axis=1) / denom.replace(0, np.nan)


def amount_weight_market(
    ret: pd.DataFrame,
    amount: pd.DataFrame,
    mask: pd.DataFrame,
) -> pd.Series:
    """mask 内成交额加权收益。"""
    w = amount.where(mask.reindex_like(amount).fillna(False) & (amount > 0))
    r = ret.where(w.notna())
    w = w.where(r.notna())
    denom = w.sum(axis=1)
    return (r * w).sum(axis=1) / denom.replace(0, np.nan)


def chip_active_frac(turnover: pd.DataFrame, spec: ReviewSpec) -> pd.DataFrame:
    """个股活筹比例。换手越高，流通盘里算进 0AMV 的越多。"""
    to = turnover.astype(float).clip(lower=0)
    tau = float(spec.active_tau)
    if tau <= 0:
        raise ValueError("active_tau 必须 > 0")
    return 1.0 - np.exp(-to / tau)


def active_name_mask(
    turnover: pd.DataFrame,
    listed: pd.DataFrame,
    spec: ReviewSpec,
) -> pd.DataFrame:
    """活筹过半的可交易票，只用于家数，不定义 0AMV 成员。"""
    listed_ok = listed.reindex_like(turnover).fillna(False)
    frac = chip_active_frac(turnover, spec)
    return listed_ok & frac.notna() & (frac >= spec.active_frac_min)


def turnover(daily: pd.DataFrame) -> pd.DataFrame:
    """成交额 / 流通市值。"""
    amount = wide(daily, "amount")
    mcap = float_mcap(daily)
    return amount / mcap.replace(0, np.nan)


def industry_code_norm(code: object) -> str:
    """申万代码只留前 6 位数字，兼容 801780 / 801780.SI。"""
    if code is None or (isinstance(code, float) and np.isnan(code)):
        return ""
    digits = "".join(ch for ch in str(code) if ch.isdigit())
    return digits[:6]


def crowding_share(amount: pd.Series, top: float) -> float:
    a = amount.astype(float).replace([np.inf, -np.inf], np.nan).dropna()
    a = a[a > 0]
    if a.empty:
        return float("nan")
    k = max(1, int(np.ceil(len(a) * top)))
    return float(a.nlargest(k).sum() / a.sum())


def crowding_series(amount: pd.DataFrame, elig: pd.DataFrame, top: float) -> pd.Series:
    """每日成交额集中度。"""
    a = amount.where(elig.reindex_like(amount).fillna(False))
    out = {}
    for date, row in a.iterrows():
        out[date] = crowding_share(row, top)
    return pd.Series(out, dtype="float64")


def classify_crowding(
    top10: float,
    ma20: float,
    delta: float,
    amount_ratio: float,
    market_ret: float,
    spec: ReviewSpec,
) -> str:
    """水平 + 变化。高且滞涨才叫拥挤待切，不是高就切。"""
    t = float(top10) if top10 == top10 else float("nan")
    m = float(ma20) if ma20 == ma20 else float("nan")
    d = float(delta) if delta == delta else float("nan")
    r = float(amount_ratio) if amount_ratio == amount_ratio else float("nan")
    rm = float(market_ret) if market_ret == market_ret else float("nan")
    stall = r == r and rm == rm and r >= spec.stall_ratio and abs(rm) < spec.stall_abs_ret
    if t == t and m == m and stall and t >= m:
        return "拥挤待切"
    if d == d and d >= spec.crowding_delta_gather:
        return "聚集"
    if d == d and d <= spec.crowding_delta_fade:
        return "分散"
    return "正常"
