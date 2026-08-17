"""复盘入口：取数 + ①市场 + ②风格。③ 题材/共识不算。"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from qlab.core.schema import SCHEMA_REVIEW_MARKET, SCHEMA_REVIEW_STYLE, validate_schema
from qlab.data.fundamentals import latest_fundamental_as_of
from qlab.data.layer import DataLayer
from qlab.review.market import market_from_daily
from qlab.review.spec import ReviewSpec
from qlab.review.style import style_from_daily


@dataclass(frozen=True)
class ReviewResult:
    market: pd.DataFrame
    style: pd.DataFrame

    def summary(self, date: str | pd.Timestamp | None = None) -> str:
        if self.market.empty or self.style.empty:
            return "复盘：无输出（暖机不足或宇宙为空）"
        d = pd.Timestamp(date).normalize() if date is not None else self.market.index.max()
        if d not in self.market.index or d not in self.style.index:
            return f"复盘：{d.date()} 不在结果里"
        m = self.market.loc[d]
        s = self.style.loc[d]
        to_r = m.get("turnover_ratio", float("nan"))
        to_txt = f"{to_r:.2f}x" if to_r == to_r else "—"
        act = m.get("active_share", float("nan"))
        act_txt = f"{act:.0%}" if act == act else "—"
        n_act = m.get("n_active", float("nan"))
        n_act_txt = f"{int(n_act)}" if n_act == n_act else "—"
        gap = m.get("breadth_gap", float("nan"))
        gap_txt = f"{gap:+.2%}" if gap == gap else "—"
        live = m.get("live_gap", float("nan"))
        live_txt = f"{live:+.2%}" if live == live else "—"
        c10 = m.get("crowding_top10", float("nan"))
        cd = m.get("crowding_delta", float("nan"))
        c_txt = f"{c10:.0%}" if c10 == c10 else "—"
        d_txt = f"{cd:+.1%}" if cd == cd else "—"
        fin = _fmt_fin(m.get("fin_delta", float("nan")))
        habitat = s.get("habitat", "混合")
        clock = s.get("clock_bin", "混合")
        return (
            f"量：{m['volume_bin']}（换手{to_txt}）  活筹/流通{act_txt}  活筹过半{n_act_txt}家\n"
            f"情绪：{m['emotion_bin']}  虚涨{gap_txt}  活跃市值相对0号{live_txt}\n"
            f"集中：Top10 {c_txt}（Δ{d_txt}）{m.get('crowding_bin', '')}\n"
            f"风格：栖息在{habitat}（{clock}）\n"
            f"  五日：规模{_fmt_pct(s['size_m5'])}  "
            f"风险{_fmt_pct(s['risk_m5'])}  "
            f"成长/价值{_fmt_pct(s['growth_m5'])}  "
            f"趋势/反转{_fmt_pct(s['trend_m5'])}  "
            f"流动性{_fmt_pct(s.get('liq_m5', float('nan')))}\n"
            f"  当日：规模{_fmt_score(s['size_score'])}  "
            f"风险{_fmt_score(s['risk_score'])}  "
            f"成长/价值{_fmt_score(s['growth_score'])}  "
            f"趋势/反转{_fmt_score(s['trend_score'])}  "
            f"流动性{_fmt_score(s.get('liq_score', 0))}\n"
            f"  短线：昨涨停相对等权{_fmt_pct(s.get('lu_yest_s', float('nan')))}"
            f"（{_fmt_score(s.get('lu_yest_score', 0))}）  "
            f"晋级{_fmt_pct(s.get('lu_promote', float('nan')))}"
            f"（z{_fmt_z(s.get('lu_promote_z', float('nan')))}）  "
            f"二板占涨停{_fmt_pct(s.get('board2_share', float('nan')))}"
            f"（z{_fmt_z(s.get('board2_z', float('nan')))}）  "
            f"最高板{_fmt_int(s.get('max_board', float('nan')))}\n"
            f"水管：融资余额变化 {fin}\n"
            f"主洋流：{s['narrative']}"
        )


def _fmt_score(v: object) -> str:
    n = int(v)
    return f"+{n}" if n > 0 else str(n)


def _fmt_z(v: object) -> str:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "—"
    if x != x:
        return "—"
    return f"{x:+.2f}"


def _fmt_int(v: object) -> str:
    try:
        if v != v:
            return "—"
        return str(int(v))
    except (TypeError, ValueError):
        return "—"


def _fmt_pct(v: object) -> str:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "—"
    if x != x:
        return "—"
    return f"{x:+.2%}"


def _fmt_fin(v: object) -> str:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "无（未取到已发布两融）"
    if x != x:
        return "无（未取到已发布两融）"
    yi = x / 1e8
    return f"{yi:+.1f} 亿（已发布，非当日盘中）"


def run(
    data: DataLayer,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    universe: str | None = None,
    spec: ReviewSpec | None = None,
    *,
    validate: bool = True,
) -> ReviewResult:
    """算 [start, end] 每个交易日的 ①市场 + ②风格。

    往前多取 ``spec.warmup`` 个交易日做均量 / β / 动量。输出不含暖机日。
    """
    spec = spec or ReviewSpec()
    uni_name = universe or spec.universe
    start = pd.Timestamp(start).normalize()
    end = pd.Timestamp(end).normalize()
    cal = data.calendar
    lookback_start = cal.prev_trading_day(start, n=spec.warmup)
    review_dates = cal.trading_days(start, end)
    if len(review_dates) == 0:
        raise ValueError(f"[{start.date()}, {end.date()}] 没有交易日")

    uni = data.universe(uni_name, lookback_start, end)
    symbols = uni.all_symbols()
    if not symbols:
        raise ValueError(f"宇宙 {uni_name!r} 在 {lookback_start.date()}~{end.date()} 为空")

    daily = data.daily(symbols, lookback_start, end, validate=False)
    if daily.empty:
        raise ValueError("日线为空")
    daily = _restrict_universe(daily, uni)
    if daily.empty:
        raise ValueError("与宇宙求交后日线为空")

    profit = pd.DataFrame()
    if spec.use_fundamentals:
        prev = cal.prev_trading_day(review_dates[0], n=1)
        profit_dates = pd.DatetimeIndex([prev]).append(review_dates).unique()
        profit = _profit_panel(
            data, symbols, profit_dates,
            lookback_days=spec.fund_lookback_days,
        )
    industry = pd.DataFrame()
    if spec.use_industry:
        industry = _industry_panel(data, symbols, review_dates, cal)
    margin = _margin_panel(data, symbols, lookback_start, end, review_dates, spec)

    market = market_from_daily(daily, review_dates, spec, margin=margin)
    style = style_from_daily(
        daily, review_dates, spec, pe_profit=profit, industry=industry,
    )
    # 两表对齐到都算出来的日子
    common = market.index.intersection(style.index)
    market = market.loc[common]
    style = style.loc[common]
    if validate and not market.empty:
        validate_schema(market, SCHEMA_REVIEW_MARKET, strict_index=True)
        validate_schema(style, SCHEMA_REVIEW_STYLE, strict_index=True)
    return ReviewResult(market=market, style=style)


def _restrict_universe(daily: pd.DataFrame, uni) -> pd.DataFrame:
    members = uni.as_dataframe()["in_universe"].astype(bool)
    aligned = members.reindex(daily.index).fillna(False)
    return daily.loc[aligned]


def _profit_panel(
    data: DataLayer,
    symbols: list[str],
    dates: pd.DatetimeIndex,
    lookback_days: int = 400,
) -> pd.DataFrame:
    if len(dates) == 0:
        return pd.DataFrame()
    start = pd.Timestamp(dates.min()) - pd.Timedelta(days=lookback_days)
    try:
        fund = data.fundamentals(symbols, start, dates.max(), validate=False)
    except (NotImplementedError, AttributeError, ValueError):
        return pd.DataFrame()
    if fund is None or fund.empty:
        return pd.DataFrame()
    cols = {}
    for date in dates:
        date = pd.Timestamp(date).normalize()
        cols[date] = latest_fundamental_as_of(
            fund, "net_profit_to_shareholders", date, symbols,
        )
    out = pd.DataFrame(cols).T
    out.index.name = "date"
    out = out.reindex(columns=symbols)
    return out


def _industry_panel(
    data: DataLayer,
    symbols: list[str],
    dates: pd.DatetimeIndex,
    cal,
) -> pd.DataFrame:
    rows = {}
    for date in dates:
        date = pd.Timestamp(date).normalize()
        asof = cal.prev_trading_day(date, n=1)
        try:
            rows[date] = data.industry(symbols, asof, system="sw", level=1)
        except (NotImplementedError, AttributeError, ValueError):
            continue
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).T
    out.index.name = "date"
    return out


def _margin_panel(
    data: DataLayer,
    symbols: list[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
    review_dates: pd.DatetimeIndex,
    spec: ReviewSpec,
) -> pd.DataFrame:
    """PIT：只用 available_at ≤ 当日 15:00 的两融，通常是 T-1 已发布数。"""
    if not spec.use_margin:
        return pd.DataFrame()
    try:
        raw = data.margin_trading(symbols, start, end, validate=False)
    except (NotImplementedError, AttributeError, ValueError):
        return pd.DataFrame()
    if raw is None or raw.empty:
        return pd.DataFrame()
    df = raw.reset_index() if isinstance(raw.index, pd.MultiIndex) else raw.copy()
    if "date" not in df.columns:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    rows = []
    for date in review_dates:
        date = pd.Timestamp(date).normalize()
        cutoff = date + pd.Timedelta(hours=15)
        if "available_at" in df.columns:
            vis = df[pd.to_datetime(df["available_at"]) <= cutoff]
        else:
            vis = df[df["date"] < date]
        if vis.empty:
            rows.append({"date": date, "fin_balance": float("nan"), "fin_buy": float("nan")})
            continue
        last = vis["date"].max()
        day = vis[vis["date"] == last]
        bal = float(day["fin_balance"].sum()) if "fin_balance" in day.columns else float("nan")
        buy = float(day["fin_buy"].sum()) if "fin_buy" in day.columns else float("nan")
        rows.append({"date": date, "fin_balance": bal, "fin_buy": buy})
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).set_index("date")
