"""可叠加采样门 — 作用在确认日 pairs 上，不负责入场.

角色拆分
--------
- **采样器** ``EventSampler`` / ``daily_event_pairs``: 产出确认日 ``(timestamp, symbol)``
- **采样门** (本模块): 产出 MultiIndex ``(date, symbol)`` bool Series，
  经 :func:`~qlab.labeling.events.filter_pairs` AND 叠加到 pairs 上
- **入场**: 仍由 :class:`~qlab.labeling.sample_spec.SampleSpec` 映射（默认次日开盘）

门只用确认日收盘可知信息（``today_close`` 语义）。开盘入场时，确认日收盘
信息服务于次日开盘，不构成未来函数。

主栈推荐（trading-books 流水线）::

    pairs = NewHighBreakoutSampler(window=20).sample_per_symbol(close)
    # 或 daily_event_pairs / CUSUMFilter
    elig = mask_to_wide(tradable_hygiene_mask(daily, min_close=2.0, min_avg_amount=5e7))
    gates = combine_masks(
        tradable_hygiene_mask(daily, min_close=2.0, min_avg_amount=5e7),
        stage2_mask(close),
        relative_strength_mask(close, method="smooth", window=60, top_pct=0.2,
                               eligible=elig),
        near_high_mask(close, window=60, max_dist=0.25),
        volume_confirm_mask(amount, min_ratio=1.5),
        market_breadth_mask(close, index_close=idx,
                            require_index_above_ma=True),
        how="and",
    )
    pairs = filter_pairs(pairs, gates)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlab.core.price_panels import (
    dist_to_high_panel,
    is_stage2_panel,
    smooth_momentum_panel,
)
from qlab.diagnostics.trend import trend_panels


def _stack_bool(mask: pd.DataFrame, name: str) -> pd.Series:
    """宽表 bool → MultiIndex (date, symbol) Series；缺失视为 False."""
    try:
        out = mask.stack(future_stack=True)
    except TypeError:
        out = mask.stack()
    out = out.fillna(False).astype(bool)
    out.index = out.index.set_names(["date", "symbol"])
    return out.rename(name)


def _stack_series(wide: pd.DataFrame) -> pd.Series:
    try:
        out = wide.stack(future_stack=True)
    except TypeError:
        out = wide.stack()
    out.index = out.index.set_names(["date", "symbol"])
    return out


def _as_wide(score: pd.DataFrame | pd.Series) -> pd.DataFrame:
    """接受宽表或 (date, symbol) 长 Series，统一成 date × symbol."""
    if isinstance(score, pd.DataFrame):
        return score.sort_index()
    if not isinstance(score.index, pd.MultiIndex) or score.index.nlevels < 2:
        raise ValueError("长格式 score 需要 MultiIndex (date, symbol)")
    names = list(score.index.names)
    wide = score.copy()
    wide.index = wide.index.set_names(["date", "symbol"] + list(names[2:]))
    return wide.unstack("symbol").sort_index()


def _apply_eligible(score: pd.DataFrame, eligible: pd.DataFrame | None) -> pd.DataFrame:
    if eligible is None:
        return score
    return score.where(eligible.reindex_like(score).fillna(False))


def mask_to_wide(mask: pd.Series) -> pd.DataFrame:
    """(date, symbol) bool Series → 宽表，供其他门的 ``eligible=`` 使用."""
    if not isinstance(mask.index, pd.MultiIndex) or mask.index.nlevels < 2:
        raise ValueError("mask 需要 MultiIndex (date, symbol)")
    s = mask.copy()
    s.index = s.index.set_names(["date", "symbol"] + list(s.index.names[2:]))
    if s.dtype != bool:
        s = s.fillna(0).astype(float) != 0.0
    else:
        s = s.fillna(False)
    return s.unstack("symbol").fillna(False).astype(bool)


def combine_masks(
    *masks: pd.Series,
    how: str = "and",
    name: str | None = None,
) -> pd.Series:
    """合并多个 (date, symbol) bool mask.

    Parameters
    ----------
    how :
        ``"and"`` / ``"or"``。
    """
    if not masks:
        raise ValueError("至少传入一个 mask")
    how_l = how.lower()
    if how_l not in {"and", "or"}:
        raise ValueError("how 必须是 'and' 或 'or'")

    idx = masks[0].index
    for m in masks[1:]:
        idx = idx.union(m.index)
    aligned = [m.reindex(idx).fillna(False).astype(bool) for m in masks]
    if how_l == "and":
        out = aligned[0]
        for m in aligned[1:]:
            out = out & m
    else:
        out = aligned[0]
        for m in aligned[1:]:
            out = out | m
    out.index = out.index.set_names(["date", "symbol"])
    return out.rename(name or f"combine_{how_l}")


def expand_date_mask(
    day_ok: pd.Series,
    symbols: list[str] | pd.Index,
    *,
    name: str = "date_gate",
) -> pd.Series:
    """把日频 bool（index=date）广播成 (date, symbol) mask."""
    day_ok = day_ok.copy()
    day_ok.index = pd.DatetimeIndex(day_ok.index).normalize()
    day_ok = day_ok.fillna(False).astype(bool)
    syms = list(dict.fromkeys(symbols))
    if day_ok.empty or not syms:
        return pd.Series(
            dtype=bool,
            index=pd.MultiIndex.from_arrays([[], []], names=["date", "symbol"]),
            name=name,
        )
    wide = pd.DataFrame(
        np.repeat(day_ok.to_numpy()[:, None], len(syms), axis=1),
        index=day_ok.index,
        columns=syms,
    )
    return _stack_bool(wide, name)


# ---------------------------------------------------------------------------
# 通用截面门 / 分数
# ---------------------------------------------------------------------------


def cross_sectional_rank_mask(
    score: pd.DataFrame | pd.Series,
    *,
    top_n: int | None = None,
    top_pct: float | None = None,
    ascending: bool = False,
    eligible: pd.DataFrame | None = None,
    name: str = "cs_rank",
) -> pd.Series:
    """截面排名门：每日保留 score 最高（或最低）的 Top-N / Top-pct.

    ``top_n`` 与 ``top_pct`` 必须且只能给一个。
    ``ascending=False``（默认）保留高分；``True`` 保留低分。
    """
    if (top_n is None) == (top_pct is None):
        raise ValueError("必须且只能指定 top_n 或 top_pct 之一")
    if top_n is not None and top_n < 1:
        raise ValueError("top_n 必须 >= 1")
    if top_pct is not None and not (0.0 < top_pct <= 1.0):
        raise ValueError("top_pct 必须在 (0, 1]")

    wide = _apply_eligible(_as_wide(score), eligible)
    rank = wide.rank(axis=1, ascending=ascending, method="first")
    if top_n is not None:
        keep = rank <= float(top_n)
    else:
        n_valid = wide.notna().sum(axis=1).astype(float)
        thresh = (n_valid * float(top_pct)).clip(lower=1.0)
        keep = rank.le(thresh, axis=0)
    return _stack_bool(keep, name)


def feature_threshold_mask(
    score: pd.DataFrame | pd.Series,
    *,
    lower: float | None = None,
    upper: float | None = None,
    name: str = "feat_thresh",
) -> pd.Series:
    """阈值门：``lower ≤ score ≤ upper``（未给的一侧不约束）."""
    if lower is None and upper is None:
        raise ValueError("至少指定 lower 或 upper")
    wide = _as_wide(score)
    keep = wide.notna()
    if lower is not None:
        keep = keep & (wide >= float(lower))
    if upper is not None:
        keep = keep & (wide <= float(upper))
    return _stack_bool(keep, name)


def smooth_momentum_score(close: pd.DataFrame, *, window: int = 60) -> pd.DataFrame:
    """Clenow 平滑动量宽表（委托 :func:`~qlab.core.price_panels.smooth_momentum_panel`）."""
    return smooth_momentum_panel(close, window=window)


# ---------------------------------------------------------------------------
# 趋势结构 / 相对强弱 / 行业
# ---------------------------------------------------------------------------


def stage2_mask(
    close: pd.DataFrame,
    *,
    ma_window: int = 200,
    slope_lookback: int = 20,
) -> pd.Series:
    """Weinstein Stage2 门（与 ``is_stage2_*`` 特征同算法）."""
    if ma_window < 2:
        raise ValueError("ma_window 必须 >= 2")
    if slope_lookback < 1:
        raise ValueError("slope_lookback 必须 >= 1")
    keep = is_stage2_panel(
        close, ma_window=ma_window, slope_lookback=slope_lookback
    )
    return _stack_bool(keep, f"stage2_{ma_window}d")


def near_high_mask(
    close: pd.DataFrame,
    *,
    window: int = 60,
    max_dist: float = 0.25,
) -> pd.Series:
    """距 N 日高点带状门（Minervini 强势区）.

    ``dist = close / rolling_max - 1``（0=新高，通常 ≤ 0）。
    保留 ``dist ≥ -max_dist``，即收盘距窗口高点回撤不超过 ``max_dist``
    （默认 25%）。
    """
    if window < 2:
        raise ValueError("window 必须 >= 2")
    if max_dist < 0:
        raise ValueError("max_dist 必须 >= 0")
    dist = dist_to_high_panel(close, window=window)
    keep = dist.notna() & (dist >= -float(max_dist))
    return _stack_bool(keep, f"near_high_{window}d")


def liquidity_top_n_mask(
    amount: pd.DataFrame,
    *,
    n: int = 500,
    window: int = 20,
    min_periods: int | None = None,
    eligible: pd.DataFrame | None = None,
) -> pd.Series:
    """日频流动性 Top-N 采样门（确认日可用）.

    对每个交易日，用过去 ``window`` 日成交额中位数做截面排名，
    排名 ≤ ``n`` 的标的为 True。
    """
    if n < 1:
        raise ValueError("n 必须 >= 1")
    if window < 1:
        raise ValueError("window 必须 >= 1")
    mp = max(5, window // 2) if min_periods is None else int(min_periods)
    score = amount.sort_index().rolling(window, min_periods=mp).median()
    score = _apply_eligible(score, eligible)
    rank = score.rank(axis=1, ascending=False, method="first")
    keep = rank <= float(n)
    return _stack_bool(keep, "in_liq_top_n")

def relative_strength_mask(
    close: pd.DataFrame | None = None,
    *,
    score: pd.DataFrame | pd.Series | None = None,
    window: int = 20,
    method: str = "return",
    top_n: int | None = None,
    top_pct: float | None = None,
    benchmark: pd.Series | None = None,
    eligible: pd.DataFrame | None = None,
) -> pd.Series:
    """截面相对强度门（领涨股）.

    强度来源（三选一优先级）
    ------------------------
    1. 显式 ``score``（宽表或长 Series，如已算好的 ``smooth_mom_60d``）
    2. ``method="smooth"``: 对 ``close`` 算 Clenow 平滑动量再截面截断
    3. ``method="return"``（默认）: ``window`` 日对数收益；可减 ``benchmark``

    ``top_n`` / ``top_pct`` 只给一个；都省略时默认 ``top_pct=0.2``。
    """
    method_l = method.lower()
    if method_l not in {"return", "smooth"}:
        raise ValueError("method 必须是 'return' 或 'smooth'")
    if top_n is None and top_pct is None:
        top_pct = 0.2

    if score is not None:
        strength = _as_wide(score)
        name = "rs_score"
    else:
        if close is None:
            raise ValueError("未提供 score 时必须给 close")
        if window < 1:
            raise ValueError("window 必须 >= 1")
        close = close.sort_index()
        if method_l == "smooth":
            if window < 5:
                raise ValueError("method='smooth' 时 window 必须 >= 5")
            strength = smooth_momentum_score(close, window=window)
            name = f"rs_smooth_{window}d"
        else:
            log_c = np.log(close.astype(float))
            strength = log_c - log_c.shift(window)
            if benchmark is not None:
                b = benchmark.reindex(close.index).astype(float)
                log_b = np.log(b)
                b_ret = log_b - log_b.shift(window)
                strength = strength.sub(b_ret, axis=0)
            name = f"rs_top_{window}d"

    # score 路径下 top_n/top_pct 仍必填其一；默认 top_pct 已给
    return cross_sectional_rank_mask(
        strength,
        top_n=top_n,
        top_pct=top_pct,
        ascending=False,
        eligible=eligible,
        name=name,
    )


def _prep_industry_ret(
    close: pd.DataFrame,
    industry: pd.DataFrame,
    *,
    window: int,
    eligible: pd.DataFrame | None,
) -> tuple[pd.Series, pd.Series]:
    """返回对齐的长 Series: ret, industry_code（已 dropna）."""
    close = close.sort_index()
    ind = industry.reindex(index=close.index, columns=close.columns)
    log_c = np.log(close.astype(float))
    ret = log_c - log_c.shift(window)
    if eligible is not None:
        ret = _apply_eligible(ret, eligible)
        ind = ind.where(eligible.reindex_like(ind).fillna(False))
    ret_s = _stack_series(ret)
    ind_s = _stack_series(ind)
    df = pd.DataFrame({"ret": ret_s, "ind": ind_s}).dropna()
    return df["ret"], df["ind"]


def industry_rs_mask(
    close: pd.DataFrame,
    industry: pd.DataFrame,
    *,
    window: int = 20,
    top_n_industries: int = 5,
    eligible: pd.DataFrame | None = None,
) -> pd.Series:
    """行业相对强度门：个股所属行业近 ``window`` 日等权收益排名 Top-N.

    Parameters
    ----------
    close :
        宽表收盘价。
    industry :
        宽表行业代码，index=date，columns=symbol（与 close 对齐；
        可用 :func:`~qlab.data.industry.industry_matrix_as_of` 构造）。
    top_n_industries :
        每日保留的最强行业数。
    """
    if window < 1:
        raise ValueError("window 必须 >= 1")
    if top_n_industries < 1:
        raise ValueError("top_n_industries 必须 >= 1")

    ret_s, ind_s = _prep_industry_ret(
        close, industry, window=window, eligible=eligible
    )
    name = f"ind_rs_top{top_n_industries}_{window}d"
    empty = pd.DataFrame(
        False, index=close.sort_index().index, columns=close.columns
    )
    if ret_s.empty:
        return _stack_bool(empty, name)

    dates = ret_s.index.get_level_values(0)
    ind_ret = ret_s.groupby([dates, ind_s], sort=False).mean()
    ranks = ind_ret.groupby(level=0).rank(ascending=False, method="first")
    top_flag = ranks <= float(top_n_industries)
    keys = pd.MultiIndex.from_arrays([dates, ind_s.to_numpy()])
    in_top = top_flag.reindex(keys).fillna(False)
    in_top.index = ret_s.index
    keep = in_top.astype(bool).unstack(fill_value=False)
    keep = keep.reindex(index=empty.index, columns=empty.columns)
    keep = keep.where(keep.notna(), False).astype(bool)
    return _stack_bool(keep, name)


def industry_leader_mask(
    close: pd.DataFrame,
    industry: pd.DataFrame,
    *,
    window: int = 20,
    top_pct: float = 0.3,
    eligible: pd.DataFrame | None = None,
) -> pd.Series:
    """行业内领涨门：在所属行业内，近 ``window`` 日收益排名 Top-pct."""
    if window < 1:
        raise ValueError("window 必须 >= 1")
    if not (0.0 < top_pct <= 1.0):
        raise ValueError("top_pct 必须在 (0, 1]")

    ret_s, ind_s = _prep_industry_ret(
        close, industry, window=window, eligible=eligible
    )
    name = f"ind_leader_{window}d"
    empty = pd.DataFrame(
        False, index=close.sort_index().index, columns=close.columns
    )
    if ret_s.empty:
        return _stack_bool(empty, name)

    dates = ret_s.index.get_level_values(0)
    tmp = pd.DataFrame({"ret": ret_s.to_numpy(), "ind": ind_s.to_numpy()}, index=ret_s.index)
    g = tmp.groupby([dates, "ind"], sort=False)["ret"]
    ranks = g.rank(ascending=False, method="first")
    n = g.transform("count")
    thresh = (n * float(top_pct)).clip(lower=1.0)
    ok = ranks <= thresh
    keep = ok.astype(bool).unstack(fill_value=False)
    keep = keep.reindex(index=empty.index, columns=empty.columns)
    keep = keep.where(keep.notna(), False).astype(bool)
    return _stack_bool(keep, name)


# ---------------------------------------------------------------------------
# 量能 / 反高潮 / 可交易卫生 / 大盘
# ---------------------------------------------------------------------------


def volume_confirm_mask(
    volume: pd.DataFrame,
    *,
    window: int = 5,
    min_ratio: float = 1.5,
    min_periods: int | None = None,
) -> pd.Series:
    """触发日量能确认：当日量 / 过去 ``window`` 日均量 ≥ ``min_ratio``.

    ``volume`` 可以是成交量或成交额宽表。均量用 ``shift(1)`` 的滚动均值，
    与 ``vol_ratio_*`` 特征语义一致（不含当日）。
    """
    if window < 1:
        raise ValueError("window 必须 >= 1")
    if min_ratio <= 0:
        raise ValueError("min_ratio 必须 > 0")
    mp = max(1, window // 2) if min_periods is None else int(min_periods)
    vol = volume.sort_index().astype(float)
    avg = vol.shift(1).rolling(window, min_periods=mp).mean()
    ratio = vol / avg.replace(0.0, np.nan)
    keep = ratio >= float(min_ratio)
    return _stack_bool(keep, f"vol_confirm_{window}d")


def anti_climax_mask(
    close: pd.DataFrame,
    *,
    short_window: int = 5,
    max_return: float | None = None,
    max_pct: float | None = 0.95,
    eligible: pd.DataFrame | None = None,
) -> pd.Series:
    """反高潮门：排除短窗涨幅已极端的标的.

    - ``max_return``: 绝对上限（对数收益），超过则剔除
    - ``max_pct``: 截面分位上限（默认 0.95 → 剔除当日最热 5%）

    至少指定一个；两者都给则同时生效（更严）。
    """
    if short_window < 1:
        raise ValueError("short_window 必须 >= 1")
    if max_return is None and max_pct is None:
        raise ValueError("至少指定 max_return 或 max_pct")
    if max_pct is not None and not (0.0 < max_pct <= 1.0):
        raise ValueError("max_pct 必须在 (0, 1]")

    close = close.sort_index()
    log_c = np.log(close.astype(float))
    ret = log_c - log_c.shift(short_window)
    ret = _apply_eligible(ret, eligible)
    keep = ret.notna()
    if max_return is not None:
        keep = keep & (ret <= float(max_return))
    if max_pct is not None:
        pct = ret.rank(axis=1, ascending=True, pct=True, method="average")
        keep = keep & (pct <= float(max_pct))
    return _stack_bool(keep, f"anti_climax_{short_window}d")


def tradable_hygiene_mask(
    daily: pd.DataFrame,
    *,
    exclude_st: bool = True,
    exclude_suspended: bool = True,
    exclude_limit_up: bool = True,
    min_listing_days: int = 30,
    min_close: float | None = None,
    min_avg_amount: float | None = None,
    avg_amount_window: int = 20,
    price_col: str = "close_raw",
) -> pd.Series:
    """可交易卫生门（确认日）.

    默认: 非 ST / 非停牌 / 非涨停 / 上市天数够。
    可选: ``min_close``（默认用 ``close_raw`` 实价）、``min_avg_amount``
    （滚动日均成交额门槛；成交额约束放在采样门，不进宇宙层）。

    Parameters
    ----------
    daily :
        DailyBar 长表，index=(date, symbol)。缺列且对应排除项开启时 fail-loud。
    price_col :
        价格门槛用的列；``close_raw`` 不存在时回退 ``close``。
    """
    if not isinstance(daily.index, pd.MultiIndex):
        raise ValueError("daily 需要 MultiIndex (date, symbol)")
    if avg_amount_window < 1:
        raise ValueError("avg_amount_window 必须 >= 1")

    def _col_bool(col: str) -> pd.Series:
        if col not in daily.columns:
            raise ValueError(f"tradable_hygiene_mask 需要列 {col}")
        s = daily[col]
        if s.dtype == bool:
            return s.fillna(False)
        return s.fillna(0).astype(float) != 0.0

    keep = pd.Series(True, index=daily.index, dtype=bool)
    if exclude_st:
        keep &= ~_col_bool("is_st")
    if exclude_suspended:
        keep &= ~_col_bool("is_suspended")
    if exclude_limit_up:
        keep &= ~_col_bool("is_limit_up")
    if min_listing_days > 0:
        if "days_since_listing" not in daily.columns:
            raise ValueError("tradable_hygiene_mask 需要列 days_since_listing")
        days = daily["days_since_listing"].astype(float)
        keep &= days.fillna(-1) >= float(min_listing_days)

    if min_close is not None:
        col = price_col if price_col in daily.columns else "close"
        if col not in daily.columns:
            raise ValueError(
                f"tradable_hygiene_mask 需要价格列 {price_col!r} 或 'close'"
            )
        keep &= daily[col].astype(float).fillna(0.0) >= float(min_close)

    if min_avg_amount is not None:
        if "amount" not in daily.columns:
            raise ValueError("tradable_hygiene_mask 需要列 amount")
        amt = daily["amount"].astype(float).unstack("symbol")
        mp = max(1, avg_amount_window // 2)
        avg = amt.rolling(avg_amount_window, min_periods=mp).mean()
        ok_amt = _stack_bool(avg >= float(min_avg_amount), "amt_ok")
        keep &= ok_amt.reindex(keep.index).fillna(False)

    keep.index = keep.index.set_names(["date", "symbol"])
    return keep.rename("tradable_ok")


def market_breadth_ok(
    close: pd.DataFrame,
    *,
    min_advance_pct: float = 0.45,
    index_close: pd.Series | None = None,
    require_index_above_ma: bool = False,
    require_index_stage2: bool = False,
    ma_window: int = 200,
    slope_lookback: int = 20,
    is_limit_up: pd.DataFrame | None = None,
    is_limit_down: pd.DataFrame | None = None,
    min_net_limit_ratio: float | None = None,
) -> pd.Series:
    """日频大盘/广度开关（index=date 的 bool）.

    - 上涨家数比（相对昨收）≥ ``min_advance_pct``
    - 可选：指数收盘 > MA（``require_index_above_ma``）
    - 可选：指数 Stage2 — 价上 MA 且 MA 上升（``require_index_stage2``）
    - 可选：涨停净占比 ``(n_up - n_down) / n_valid ≥ min_net_limit_ratio``
    """
    if not (0.0 <= min_advance_pct <= 1.0):
        raise ValueError("min_advance_pct 必须在 [0, 1]")
    if require_index_stage2 and require_index_above_ma:
        # stage2 已蕴含 above_ma；允许同时开，但不重复约束
        pass
    close = close.sort_index().astype(float)
    up = close > close.shift(1)
    valid = close.notna() & close.shift(1).notna()
    n_up = up.where(valid, False).sum(axis=1)
    n_valid = valid.sum(axis=1).astype(float)
    advance = n_up / n_valid.replace(0.0, np.nan)
    ok = advance >= float(min_advance_pct)

    need_index = require_index_above_ma or require_index_stage2
    if need_index:
        if index_close is None:
            raise ValueError("指数条件开启时需要 index_close")
        if ma_window < 2:
            raise ValueError("ma_window 必须 >= 2")
        idx = index_close.reindex(close.index).astype(float)
        ma = idx.rolling(ma_window, min_periods=ma_window).mean()
        if require_index_stage2:
            if slope_lookback < 1:
                raise ValueError("slope_lookback 必须 >= 1")
            prev = ma.shift(slope_lookback)
            rising = (ma - prev) / prev.replace(0.0, np.nan) > 0
            ok = ok & (idx > ma) & rising
        elif require_index_above_ma:
            ok = ok & (idx > ma)

    if min_net_limit_ratio is not None:
        if is_limit_up is None or is_limit_down is None:
            raise ValueError("min_net_limit_ratio 需要 is_limit_up 与 is_limit_down")
        lu = is_limit_up.reindex_like(close).fillna(False).astype(bool)
        ld = is_limit_down.reindex_like(close).fillna(False).astype(bool)
        cell_ok = close.notna()
        n_lu = lu.where(cell_ok, False).sum(axis=1).astype(float)
        n_ld = ld.where(cell_ok, False).sum(axis=1).astype(float)
        n_cell = cell_ok.sum(axis=1).astype(float).replace(0.0, np.nan)
        net = (n_lu - n_ld) / n_cell
        ok = ok & (net >= float(min_net_limit_ratio))

    return ok.fillna(False).astype(bool).rename("market_breadth_ok")


def market_breadth_mask(
    close: pd.DataFrame,
    *,
    symbols: list[str] | pd.Index | None = None,
    min_advance_pct: float = 0.45,
    index_close: pd.Series | None = None,
    require_index_above_ma: bool = False,
    require_index_stage2: bool = False,
    ma_window: int = 200,
    slope_lookback: int = 20,
    is_limit_up: pd.DataFrame | None = None,
    is_limit_down: pd.DataFrame | None = None,
    min_net_limit_ratio: float | None = None,
) -> pd.Series:
    """大盘/广度门的 (date, symbol) 形式，可直接 ``filter_pairs``.

    某日开关为 False 时，该日所有 symbol 均被剔除。
    """
    day_ok = market_breadth_ok(
        close,
        min_advance_pct=min_advance_pct,
        index_close=index_close,
        require_index_above_ma=require_index_above_ma,
        require_index_stage2=require_index_stage2,
        ma_window=ma_window,
        slope_lookback=slope_lookback,
        is_limit_up=is_limit_up,
        is_limit_down=is_limit_down,
        min_net_limit_ratio=min_net_limit_ratio,
    )
    syms = list(symbols) if symbols is not None else list(close.columns)
    return expand_date_mask(day_ok, syms, name="market_breadth")


def bull_trend_mask(
    close: pd.DataFrame,
    high: pd.DataFrame | None = None,
    low: pd.DataFrame | None = None,
    *,
    min_efficiency: float | None = None,
) -> pd.Series:
    """多头趋势门：``direction=+1``，可选效率下限。仓位门请叠加 ``flow_panels``."""
    panels = trend_panels(close, high=high, low=low, include_xs_rank=False)
    keep = panels["direction"] > 0
    if min_efficiency is not None:
        keep = keep & (panels["efficiency"] >= float(min_efficiency))
    return _stack_bool(keep, "bull_trend")
