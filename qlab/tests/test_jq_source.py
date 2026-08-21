"""JQDataSource 单测 — 用假 DataCache, 不触网."""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from qlab.core.enums import AdjustMode, Freq
from qlab.core.exceptions import DataSourceError, DataUnavailableError
from qlab.core.schema import SCHEMA_DAILY_BAR, SCHEMA_UNIVERSE, validate_schema
from qlab.data.adjust import apply_adjust
from qlab.data.layer import DataLayer
from qlab.data.sources.jq_source import (
    JQCalendar,
    JQDataSource,
    to_jq_code,
    to_qlab_symbol,
)
from qlab.data.store import InMemoryShardedBarStore

# ======================================================================
# 假数据构造
# ======================================================================

_DATES = pd.DatetimeIndex(["2024-06-03", "2024-06-04", "2024-06-05", "2024-06-06"])

#: 不复权收盘价; 第 3 日除权(factor 跳变), 第 4 日停牌
_CLOSE_RAW = np.array([100.0, 110.0, 105.0, 105.0])
_FACTOR = np.array([2.0, 2.0, 2.5, 2.5])
_PAUSED = np.array([0.0, 0.0, 0.0, 1.0])
_VOLUME_RAW = np.array([1_000_000.0, 2_000_000.0, 1_500_000.0, 0.0])
_MONEY = np.array([1.0e8, 2.2e8, 1.575e8, 0.0])


def _fake_bars(fq: str | None) -> pd.DataFrame:
    """造出与 jq.cache 一致口径的行情: fq='post' 为后复权, fq=None 为不复权."""
    scale = _FACTOR if fq == "post" else np.ones_like(_FACTOR)
    close = _CLOSE_RAW * scale
    # 第 2 日收盘 == 涨停价(用于验证 is_limit_up 判定)
    high_limit = _CLOSE_RAW * 1.1 * scale
    high_limit[1] = close[1]
    return pd.DataFrame(
        {
            "open": close * 0.99,
            "close": close,
            "high": close * 1.01,
            "low": close * 0.98,
            "volume": _VOLUME_RAW / (scale if fq == "post" else 1.0),
            "money": _MONEY,
            "factor": _FACTOR if fq == "post" else np.ones_like(_FACTOR),
            "high_limit": high_limit,
            "low_limit": _CLOSE_RAW * 0.9 * scale,
            "avg": close * 0.995,
            "pre_close": close,
            "paused": _PAUSED,
        },
        index=_DATES,
    )


class FakeCache:
    """只实现 JQDataSource 用到的那几个方法."""

    def __init__(self, st: bool = False):
        self.st = st
        self.calls: list[str] = []

    def get_price_batch(self, codes, start, end, *, frequency="daily", fq="pre", **kw):
        self.calls.append(f"get_price_batch:{fq}")
        return {c: _fake_bars(fq) for c in codes}

    def get_extras(self, info, codes, start, end):
        self.calls.append("get_extras")
        return pd.DataFrame(
            {c: [self.st] * len(_DATES) for c in codes}, index=_DATES
        )

    def get_all_securities(self, date, types=None):
        self.calls.append("get_all_securities")
        return pd.DataFrame(
            {
                "display_name": ["甲", "乙"],
                "start_date": [pd.Timestamp("2000-01-01"), pd.Timestamp("2010-01-01")],
                "type": ["stock", "stock"],
            },
            index=["600519.XSHG", "000001.XSHE"],
        )

    def get_valuation(self, code, start, end, *, fields=None):
        self.calls.append("get_valuation")
        return pd.DataFrame(
            {"capitalization": [10_000.0] * len(_DATES),
             "circulating_cap": [8_000.0] * len(_DATES)},
            index=_DATES,
        )

    def get_valuation_batch(self, codes, start, end, *, fields=None):
        """批量版: 委派给(可能被子类 override 的)单只 get_valuation 再组长表.

        这样所有重写了 get_valuation 的子类(退市/送转股/缺口等场景)
        自动通过批量路径生效, 无需逐个改。真实 jq 侧是一次 RPC 拿全部。
        """
        self.calls.append("get_valuation_batch")
        req = list(dict.fromkeys(["code", "day", *(fields or [])]))
        frames = []
        for code in dict.fromkeys(codes):
            df = self.get_valuation(code, start, end, fields=fields)
            if not isinstance(df, pd.DataFrame) or len(df) == 0:
                continue
            part = pd.DataFrame({"code": code, "day": pd.DatetimeIndex(df.index)})
            for col in (fields or []):
                if col in df.columns:
                    part[col] = df[col].to_numpy()
            frames.append(part)
        if not frames:
            return pd.DataFrame(columns=req)
        return pd.concat(frames, ignore_index=True)

    def get_all_trade_days(self):
        """模拟聚宽: 日历向后延伸到**未来两年**(实测行为)."""
        self.calls.append("get_all_trade_days")
        end = pd.Timestamp.today().normalize() + pd.DateOffset(years=2)
        return list(pd.bdate_range("2024-01-01", end).date)

    def get_index_stocks(self, code, date):
        self.calls.append("get_index_stocks")
        return ["600519.XSHG", "000001.XSHE"]

    def get_index_weights(self, code, date):
        """聚宽返回的 weight 是**百分数**且含舍入误差(实测合计 100.003)."""
        self.calls.append("get_index_weights")
        return pd.DataFrame(
            {
                "date": [pd.Timestamp(date)] * 2,
                "weight": [60.002, 40.001],
                "display_name": ["甲", "乙"],
            },
            index=["600519.XSHG", "000001.XSHE"],
        )

    def get_industry(self, codes, date):
        """聚宽一次返回全部层级(用于填 parent_code)."""
        self.calls.append("get_industry")
        return {
            c: {
                "sw_l1": {"industry_code": "801120", "industry_name": "食品饮料"},
                "sw_l2": {"industry_code": "801123", "industry_name": "白酒II"},
                "sw_l3": {"industry_code": "850831", "industry_name": "白酒III"},
                "zjw": {"industry_code": "C15", "industry_name": "酒饮料精制茶"},
            }
            for c in codes
        }

    def get_concept(self, codes, date):
        self.calls.append("get_concept")
        return {c: {"jq_concept": [{"concept_code": "GN001", "concept_name": "白酒"}]}
                for c in codes}

    def get_index_valuation(self, code, start, end, *, fields=None):
        self.calls.append("get_index_valuation")
        return pd.DataFrame(
            {
                "code": code,
                "turnover_ratio": 1.7916,
                "circulating_market_cap": 944382.3351,
                "market_cap": 1.1e6,
            },
            index=_DATES,
        )

    def get_mtss(self, code, start, end):
        self.calls.append("get_mtss")
        # jq 层会注入 available_at(下一交易日 09:30), 此处模拟
        return pd.DataFrame(
            {
                "sec_code": code, "fin_value": 2.1e10, "fin_buy_value": 4.3e8,
                "fin_refund_value": 3.7e8, "sec_value": 1.2e5,
                "sec_sell_value": 2e3, "sec_refund_value": 7e2, "fin_sec_value": 2.1e10,
                "available_at": [
                    d + pd.Timedelta(days=1, hours=9, minutes=30) for d in _DATES
                ],
            },
            index=_DATES,
        )

    def get_money_flow(self, code, start, end):
        self.calls.append("get_money_flow")
        # 聚宽原始为**万元**; 主力 = 超大单 + 大单; available_at 由 jq 层注入
        return pd.DataFrame(
            {
                "sec_code": code, "change_pct": 0.5,
                "net_amount_main": 4.0, "net_pct_main": 4.0,
                "net_amount_xl": 3.0, "net_pct_xl": 3.0,
                "net_amount_l": 1.0, "net_pct_l": 1.0,
                "net_amount_m": -2.0, "net_pct_m": -2.0,
                "net_amount_s": -2.0, "net_pct_s": -2.0,
                "available_at": [
                    d + pd.Timedelta(days=1, hours=9, minutes=30) for d in _DATES
                ],
            },
            index=_DATES,
        )

    def get_call_auction(self, code, start, end):
        """聚宽 get_call_auction 是单标的接口(security: str).

        jq 层注入 available_at = **当日** 09:30(竞价当日即可用)。
        """
        self.calls.append("get_call_auction")
        d = {"code": code, "current": 1650.0, "volume": 22070.0, "money": 3.6e7}
        for k in range(1, 6):
            d[f"a{k}_p"] = 1650.0 + k * 0.01
            d[f"a{k}_v"] = 100.0 * k
            d[f"b{k}_p"] = 1650.0 - k * 0.01
            d[f"b{k}_v"] = 100.0 * k
        out = pd.DataFrame([d] * len(_DATES), index=_DATES)
        out["available_at"] = [
            x + pd.Timedelta(hours=9, minutes=30) for x in _DATES
        ]
        return out

    def get_billboard_list(self, end_date, stock_list=None, count=5):
        self.calls.append("get_billboard_list")
        codes = stock_list or ["600519.XSHG"]
        rows = []
        for c in codes:
            for direction in ("BUY", "SELL"):
                bv = 1e7 if direction == "BUY" else 0.0
                sv = 0.0 if direction == "BUY" else 8e6
                rows.append({
                    "code": c, "day": str(pd.Timestamp(end_date).date()),
                    "direction": direction, "rank": 1, "abnormal_code": 106,
                    "abnormal_name": "异动", "sales_depart_name": "某营业部",
                    "buy_value": bv, "buy_rate": 5.0, "sell_value": sv, "sell_rate": 3.0,
                    "total_value": bv + sv, "net_value": bv - sv, "amount": 5e7,
                })
        return pd.DataFrame(rows)

    def get_factor_values(self, codes, factors, end_date, *, start_date=None):
        self.calls.append("get_factor_values")
        return {
            f: pd.DataFrame({c: [0.9] * len(_DATES) for c in codes}, index=_DATES)
            for f in factors
        }

    def get_fundamentals(self, table, codes, fields, end_date,
                         start_date=None, *, report_type=None):
        self.calls.append(f"get_fundamentals:{table}")
        # 模拟每个 code 2 期(披露日在报告期之后 = 正式表语义)
        rows = []
        for c in codes:
            for period, pub in [("2024-03-31", "2024-04-27"),
                                ("2024-06-30", "2024-08-09")]:
                row = {"code": c, "end_date": pd.Timestamp(period),
                       "pub_date": pd.Timestamp(pub)}
                for fld in fields:
                    # type 字段给字符串, 其余给数值
                    row[fld] = "业绩预增" if fld == "type" else 1.0e9
                rows.append(row)
        return pd.DataFrame(rows)


@pytest.fixture
def src() -> JQDataSource:
    return JQDataSource(cache=FakeCache())


SYMS = ["600519.SH", "000001.SZ"]


# ======================================================================
# 符号映射
# ======================================================================


@pytest.mark.parametrize(
    ("qlab", "jq"),
    [("600519.SH", "600519.XSHG"), ("000001.SZ", "000001.XSHE"),
     ("000985.CSI", "000985.CSI"), ("801780.SI", "801780.SI")],
)
def test_symbol_roundtrip(qlab: str, jq: str):
    assert to_jq_code(qlab) == jq
    assert to_qlab_symbol(jq) == qlab
    # 幂等: 传入已是目标格式时原样返回
    assert to_jq_code(jq) == jq
    assert to_qlab_symbol(qlab) == qlab


@pytest.mark.parametrize("bad", ["600519.XX", "600519", "600519.NYSE"])
def test_symbol_unknown_suffix_raises(bad: str):
    with pytest.raises(ValueError, match="无法识别"):
        to_jq_code(bad)


# ======================================================================
# fetch_bars → DailyBar schema
# ======================================================================


def test_fetch_bars_satisfies_daily_schema(src: JQDataSource):
    bars = src.fetch_bars(SYMS, _DATES[0], _DATES[-1])
    validate_schema(bars, SCHEMA_DAILY_BAR, strict_index=True)
    assert list(bars.index.names) == ["date", "symbol"]
    assert set(bars.index.get_level_values("symbol")) == set(SYMS)


def test_backward_close_equals_raw_times_factor(src: JQDataSource):
    """不变量 1 —— 必须精确成立(而非近似)."""
    bars = src.fetch_bars(SYMS, _DATES[0], _DATES[-1]).dropna(subset=["close_raw"])
    diff = (bars["close"] - bars["close_raw"] * bars["adj_factor"]).abs().max()
    assert diff < 1e-9


def test_only_one_remote_shape_for_two_fq(src: JQDataSource):
    """两个复权口径都来自同一份缓存 —— 各调一次 get_price_batch."""
    src.fetch_bars(SYMS, _DATES[0], _DATES[-1])
    assert src.cache.calls.count("get_price_batch:post") == 1
    assert src.cache.calls.count("get_price_batch:None") == 1


def test_suspended_day_normalized(src: JQDataSource):
    """停牌日: 价格 NaN、量额归零、涨跌停标记清除(schema 不变量强制)."""
    bars = src.fetch_bars(SYMS, _DATES[0], _DATES[-1])
    sus = bars[bars["is_suspended"]]
    assert len(sus) == len(SYMS)  # 每只标的最后一天停牌
    for col in ("open", "high", "low", "close", "vwap", "close_raw", "limit_up_price"):
        assert sus[col].isna().all(), col
    assert (sus["volume"] == 0).all()
    assert (sus["amount"] == 0).all()
    assert not sus["is_limit_up"].any()


def test_limit_up_detected_and_consistent(src: JQDataSource):
    """第 2 日构造为涨停; 不变量要求 close_raw ≈ limit_up_price."""
    bars = src.fetch_bars(SYMS, _DATES[0], _DATES[-1])
    one = bars.xs("600519.SH", level="symbol")
    assert bool(one["is_limit_up"].iloc[1])
    assert not one["is_limit_up"].iloc[0]
    assert abs(one["close_raw"].iloc[1] - one["limit_up_price"].iloc[1]) < 1e-2


def test_adj_factor_jump_preserved(src: JQDataSource):
    bars = src.fetch_bars(SYMS, _DATES[0], _DATES[-1])
    one = bars.xs("600519.SH", level="symbol")
    assert sorted(set(one["adj_factor"])) == [2.0, 2.5]


def test_volume_is_raw_scale_int64(src: JQDataSource):
    """volume 用不复权口径(真实成交股数)且为 int64."""
    bars = src.fetch_bars(SYMS, _DATES[0], _DATES[-1])
    one = bars.xs("600519.SH", level="symbol")
    assert str(one["volume"].dtype) == "int64"
    assert one["volume"].iloc[0] == 1_000_000


def test_shares_are_int64_from_wan_unit(src: JQDataSource):
    """聚宽 capitalization 单位是万股 → 换算为股."""
    bars = src.fetch_bars(SYMS, _DATES[0], _DATES[-1])
    one = bars.xs("600519.SH", level="symbol")
    assert str(one["total_shares"].dtype) == "int64"
    assert one["total_shares"].iloc[0] == 100_000_000  # 10000 万股
    assert one["float_shares"].iloc[0] == 80_000_000
    assert (bars["float_shares"] <= bars["total_shares"]).all()


def test_free_float_column_absent(src: JQDataSource):
    """聚宽无自由流通股本; int64 列不能塞 NaN, 故整列不提供."""
    bars = src.fetch_bars(SYMS, _DATES[0], _DATES[-1])
    assert "free_float_shares" not in bars.columns


def test_days_since_listing(src: JQDataSource):
    bars = src.fetch_bars(SYMS, _DATES[0], _DATES[-1])
    one = bars.xs("600519.SH", level="symbol")
    expected = (_DATES[0] - pd.Timestamp("2000-01-01")).days
    assert one["days_since_listing"].iloc[0] == expected
    assert str(one["days_since_listing"].dtype) == "int32"


def test_is_st_flag(src: JQDataSource):
    st_src = JQDataSource(cache=FakeCache(st=True))
    assert st_src.fetch_bars(SYMS, _DATES[0], _DATES[-1])["is_st"].all()
    assert not src.fetch_bars(SYMS, _DATES[0], _DATES[-1])["is_st"].any()


def test_adjust_param_does_not_change_stored_close(src: JQDataSource):
    """adjust 不影响存储态 —— close 恒为后复权(否则违反不变量 1)."""
    a = src.fetch_bars(SYMS, _DATES[0], _DATES[-1], Freq.DAILY, AdjustMode.BACKWARD)
    b = src.fetch_bars(SYMS, _DATES[0], _DATES[-1], Freq.DAILY, AdjustMode.NONE)
    pd.testing.assert_series_equal(a["close"], b["close"])


def test_intraday_has_session_column(src: JQDataSource):
    bars = src.fetch_bars(SYMS, _DATES[0], _DATES[-1], Freq.MIN_30)
    assert list(bars.index.names) == ["timestamp", "symbol"]
    assert "session" in bars.columns


@pytest.mark.parametrize(
    ("clock", "expected"),
    [
        ("09:25", "open_auction"),
        ("09:30", "open_auction"),
        ("09:31", "morning"),
        ("11:30", "morning"),
        ("13:00", "afternoon"),
        ("14:57", "afternoon"),
        ("14:58", "close_auction"),  # ⑥ 2018 年起 14:57-15:00 为尾盘集竞
        ("15:00", "close_auction"),
    ],
)
def test_session_boundaries(clock: str, expected: str):
    from qlab.data.sources.jq_source import _session_of

    assert _session_of(pd.Timestamp(f"2024-06-03 {clock}")) == expected


def test_empty_result(src: JQDataSource):
    class Empty(FakeCache):
        def get_price_batch(self, codes, start, end, **kw):
            return {c: pd.DataFrame() for c in codes}

    bars = JQDataSource(cache=Empty()).fetch_bars(SYMS, _DATES[0], _DATES[-1])
    assert bars.empty
    assert list(bars.index.names) == ["date", "symbol"]


# ======================================================================
# 复权视图切换
# ======================================================================


def test_apply_adjust_switches_view(src: JQDataSource):
    bars = src.fetch_bars(SYMS, _DATES[0], _DATES[-1])
    none_view = apply_adjust(bars.copy(), AdjustMode.NONE)
    one = none_view.xs("600519.SH", level="symbol")
    assert (one["close"] - one["close_raw"]).abs().max() < 1e-9


# ======================================================================
# 日历
# ======================================================================


def test_jq_calendar_semantics(src: JQDataSource):
    cal = src.fetch_calendar()
    assert isinstance(cal, JQCalendar)
    d = pd.Timestamp("2024-06-03")  # 周一
    assert cal.is_trading_day(d)
    assert not cal.is_trading_day(pd.Timestamp("2024-06-08"))  # 周六
    # next/prev 与 AShareCalendar 的 next_session 循环语义一致
    assert cal.next_trading_day(d, 1) == pd.Timestamp("2024-06-04")
    assert cal.next_trading_day(d, 5) == pd.Timestamp("2024-06-10")
    assert cal.prev_trading_day(pd.Timestamp("2024-06-04"), 1) == d
    assert cal.count_trading_days(d, pd.Timestamp("2024-06-07")) == 5


def test_jq_calendar_out_of_range(src: JQDataSource):
    cal = src.fetch_calendar()
    with pytest.raises(IndexError):
        cal.prev_trading_day(cal.sessions[0], 1)
    with pytest.raises(IndexError):
        cal.next_trading_day(cal.sessions[-1], 1)


# ======================================================================
# 其他 Protocol 方法
# ======================================================================


def test_fetch_universe(src: JQDataSource):
    uni = src.fetch_universe("000300.SH", (_DATES[0], _DATES[-1]))
    assert list(uni.index.names) == ["date", "symbol"]
    assert uni["in_universe"].all()
    assert set(uni.index.get_level_values("symbol")) == set(SYMS)
    # 前填到每个交易日
    assert len(uni.index.get_level_values("date").unique()) == len(_DATES)


def test_universe_weight_normalized_to_one(src: JQDataSource):
    """聚宽 weight 是百分数(合计~100.003), 需归一到 sum=1.0 以满足 Universe 不变量."""
    uni = src.fetch_universe("000300.SH", (_DATES[0], _DATES[-1]))
    validate_schema(uni, SCHEMA_UNIVERSE, strict_index=True)
    daily_sum = uni["weight"].groupby(level="date").sum()
    assert ((daily_sum - 1.0).abs() < 1e-9).all()


def test_universe_samples_monthly_not_daily(src: JQDataSource):
    """③ 按月采样: 跨 3 个月的区间不应逐交易日请求."""
    src.cache.calls.clear()
    src.fetch_universe("000300.SH", (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-03-31")))
    n = src.cache.calls.count("get_index_weights")
    # 3 个月 -> 每月首交易日 + 首尾, 远少于交易日数(~60)
    assert n <= 5, f"请求数 {n} 过多, 应按月采样"


def test_universe_all_spec_has_nan_weight(src: JQDataSource):
    uni = src.fetch_universe("all", (_DATES[0], _DATES[1]))
    assert uni["weight"].isna().all()
    validate_schema(uni, SCHEMA_UNIVERSE, strict_index=True)


def test_universe_empty_range(src: JQDataSource):
    uni = src.fetch_universe(
        "000300.SH", (pd.Timestamp("2024-06-08"), pd.Timestamp("2024-06-09"))
    )  # 周六周日
    assert uni.empty
    assert list(uni.index.names) == ["date", "symbol"]


def test_fetch_industry(src: JQDataSource):
    ind = src.fetch_industry_classification(SYMS, (_DATES[0], _DATES[0]))
    assert ind["industry_name"].iloc[0] == "食品饮料"
    assert list(ind.index.names) == ["date", "symbol", "system"]


def test_industry_samples_across_months(src: JQDataSource):
    """⑤ 返回区间内的分类时序, 而非只取起点一天."""
    ind = src.fetch_industry_classification(
        SYMS, (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-03-31"))
    )
    dates = ind.index.get_level_values("date").unique()
    assert len(dates) >= 3, f"应至少覆盖 3 个月, 实际 {len(dates)} 个时点"


def test_concepts_samples_across_months(src: JQDataSource):
    con = src.fetch_concepts(
        SYMS, (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-03-31"))
    )
    # 全区间持续存在 -> 合成一段, expired_date 为 NaT
    assert len(con) == len(SYMS)
    assert con["expired_date"].isna().all()


def test_status_overrides_empty(src: JQDataSource):
    """fetch_bars 已给全状态列, 无需覆盖."""
    assert src.fetch_status_overrides(SYMS, _DATES[0], _DATES[-1]).empty


def test_fetch_industry_unknown_system(src: JQDataSource):
    with pytest.raises(ValueError, match="不支持的行业体系"):
        src.fetch_industry_classification(SYMS, (_DATES[0], _DATES[0]), system="gics")


def test_fetch_concepts(src: JQDataSource):
    con = src.fetch_concepts(SYMS, (_DATES[0], _DATES[0]))
    assert con["concept_name"].iloc[0] == "白酒"
    assert list(con.index.names) == ["effective_date", "symbol", "source"]


@pytest.mark.parametrize("method", ["fetch_corporate_actions"])
def test_unimplemented_methods(src: JQDataSource, method: str):
    with pytest.raises(NotImplementedError):
        getattr(src, method)(SYMS, _DATES[0], _DATES[-1])


# ======================================================================
# 与 DataLayer 集成
# ======================================================================


def test_data_layer_end_to_end():
    layer = DataLayer(source=JQDataSource(cache=FakeCache()),
                      bar_store=InMemoryShardedBarStore())
    bars = layer.daily(SYMS, _DATES[0], _DATES[-1], validate=True)
    assert len(bars) == len(_DATES) * len(SYMS)
    assert list(bars.index.names) == ["date", "symbol"]


def test_data_layer_incremental_hits_cache():
    """二次请求同区间不再调用数据源."""
    cache = FakeCache()
    layer = DataLayer(source=JQDataSource(cache=cache),
                      bar_store=InMemoryShardedBarStore())
    layer.daily(SYMS, _DATES[0], _DATES[-1], validate=True)
    n = len(cache.calls)
    layer.daily(SYMS, _DATES[0], _DATES[-1], validate=True)
    assert len(cache.calls) == n


# ======================================================================
# ① 退市/未上市区间(全 NaN 行)
# ======================================================================


def _delisted_bars(fq: str | None) -> pd.DataFrame:
    """退市股的形态: 全 NaN 行且 paused 也是 NaN(实测聚宽行为)."""
    cols = ["open", "close", "high", "low", "volume", "money", "high_limit",
            "low_limit", "avg", "pre_close", "paused"]
    df = pd.DataFrame({c: [np.nan] * len(_DATES) for c in cols}, index=_DATES)
    df["factor"] = 1.0  # 聚宽对无数据日仍给 factor
    return df


class PartlyDelistedCache(FakeCache):
    """600519 正常, 000001 整段无数据."""

    def get_price_batch(self, codes, start, end, *, frequency="daily", fq="pre", **kw):
        return {
            c: (_delisted_bars(fq) if c.startswith("000001") else _fake_bars(fq))
            for c in codes
        }


def test_delisted_symbol_does_not_crash():
    """全 NaN 行曾导致 IntCastingNaNError 崩溃(int64 存不了 NaN)."""
    src = JQDataSource(cache=PartlyDelistedCache())
    bars = src.fetch_bars(SYMS, _DATES[0], _DATES[-1])
    validate_schema(bars, SCHEMA_DAILY_BAR, strict_index=True)
    # 无数据的标的被整段剔除, 而非凭空造出停牌日
    assert set(bars.index.get_level_values("symbol")) == {"600519.SH"}


def test_all_symbols_delisted_returns_empty():
    class AllGone(FakeCache):
        def get_price_batch(self, codes, start, end, **kw):
            return {c: _delisted_bars(kw.get("fq")) for c in codes}

    bars = JQDataSource(cache=AllGone()).fetch_bars(SYMS, _DATES[0], _DATES[-1])
    assert bars.empty
    assert list(bars.index.names) == ["date", "symbol"]


def test_suspended_row_kept_but_nonexistent_dropped():
    """区分停牌(paused=1, 保留)与不在市(全 NaN, 剔除)."""
    src = JQDataSource(cache=FakeCache())
    bars = src.fetch_bars(["600519.SH"], _DATES[0], _DATES[-1])
    # _fake_bars 的最后一天 paused=1 且价格非 NaN → 应作为停牌保留
    assert len(bars) == len(_DATES)
    assert bars["is_suspended"].sum() == 1


# ======================================================================
# ② 必填字段缺失 → fail loud(不静默降级)
# ======================================================================


class NoStCache(FakeCache):
    def get_extras(self, *a, **k):
        raise RuntimeError("聚宽 is_st 接口挂了")


class PartialStCache(FakeCache):
    """is_st 只返回一只标的 → 覆盖不全."""

    def get_extras(self, info, codes, start, end):
        return pd.DataFrame({codes[0]: [False] * len(_DATES)}, index=_DATES)


class NoListingCache(FakeCache):
    def get_all_securities(self, date, types=None):
        return pd.DataFrame(columns=["display_name", "start_date", "type"])


class NoSharesCache(FakeCache):
    def get_valuation(self, code, start, end, *, fields=None):
        return pd.DataFrame()


@pytest.mark.parametrize(
    ("cache_cls", "match"),
    [
        (PartialStCache, "is_st 覆盖不全"),
        (NoListingCache, "无法取到上市日"),
        (NoSharesCache, "无法取到股本数据"),
    ],
)
def test_missing_required_field_raises(cache_cls, match: str):
    """必填字段缺失必须报错 —— 降级值(False/0)在 schema 看来合法, 会静默污染结论."""
    src = JQDataSource(cache=cache_cls())
    with pytest.raises(DataUnavailableError, match=match):
        src.fetch_bars(SYMS, _DATES[0], _DATES[-1])


def test_upstream_exception_propagates():
    """上游异常不得被吞掉."""
    src = JQDataSource(cache=NoStCache())
    with pytest.raises(RuntimeError, match="is_st 接口挂了"):
        src.fetch_bars(SYMS, _DATES[0], _DATES[-1])


def test_listing_date_falls_back_to_start_asof():
    """区间内退市的标的在 end 名录里没了, 需用 start 时点兜住."""

    class DelistedInWindow(FakeCache):
        def get_all_securities(self, date, types=None):
            base = super().get_all_securities(date, types)
            # end 时点的名录已不含 000001
            if str(date) >= str(_DATES[-1].date()):
                return base.drop(index=["000001.XSHE"])
            return base

    src = JQDataSource(cache=DelistedInWindow())
    bars = src.fetch_bars(SYMS, _DATES[0], _DATES[-1])
    validate_schema(bars, SCHEMA_DAILY_BAR, strict_index=True)
    assert (bars["days_since_listing"] > 0).all()


def test_calendar_failure_warns_and_falls_back():
    """日历是唯一允许降级项(默认日历也是真实日历), 但必须告警."""

    class NoCal(FakeCache):
        def get_all_trade_days(self):
            raise RuntimeError("日历接口挂了")

    src = JQDataSource(cache=NoCal())
    with pytest.warns(RuntimeWarning, match="退回"):
        cal = src.fetch_calendar()
    assert not isinstance(cal, JQCalendar)


# ======================================================================
# ⑧ 日历缓存  ⑨ 异常归位
# ======================================================================


def test_calendar_cached_on_instance(src: JQDataSource):
    """⑧ 建一次要灌入上千交易日, 不应每次重建."""
    first = src.fetch_calendar()
    assert src.fetch_calendar() is first
    n = src.cache.calls.count("get_all_trade_days")
    for _ in range(5):
        src.fetch_calendar()
    assert src.cache.calls.count("get_all_trade_days") == n


def test_calendar_fallback_not_cached():
    """降级的默认日历不缓存, 以便下次重试聚宽口径."""

    class FlakyCal(FakeCache):
        def __init__(self):
            super().__init__()
            self.fail = True

        def get_all_trade_days(self):
            if self.fail:
                raise RuntimeError("暂时不可用")
            return super().get_all_trade_days()

    cache = FlakyCal()
    src = JQDataSource(cache=cache)
    with pytest.warns(RuntimeWarning):
        assert not isinstance(src.fetch_calendar(), JQCalendar)
    cache.fail = False  # 恢复后应能拿到聚宽日历
    assert isinstance(src.fetch_calendar(), JQCalendar)


def test_universe_reuses_cached_calendar(src: JQDataSource):
    """fetch_universe 内部两处用到日历, 应共用同一个实例."""
    src.fetch_calendar()
    src.cache.calls.clear()
    src.fetch_universe("000300.SH", (_DATES[0], _DATES[-1]))
    assert src.cache.calls.count("get_all_trade_days") == 0


def test_error_is_in_core_exceptions():
    """⑨ DataUnavailableError 属于 qlab 异常体系, 可按 DataSourceError 捕获."""
    assert issubclass(DataUnavailableError, DataSourceError)
    src = JQDataSource(cache=NoSharesCache())
    with pytest.raises(DataSourceError):  # 不需要知道具体子类
        src.fetch_bars(SYMS, _DATES[0], _DATES[-1])


# ======================================================================
# ⑩ symbols 去重  ⑪ parent_code  ⑫ 概念区间
# ======================================================================


@pytest.mark.parametrize(
    "dup",
    [
        ["600519.SH", "600519.SH"],
        ["600519.SH", "600519.XSHG"],  # 字面不同但同一只
        ["600519.SH", "600519.SH", "600519.XSHG"],
    ],
)
def test_duplicate_symbols_deduplicated(src: JQDataSource, dup: list[str]):
    """⑩ 重复标的曾让 index 重复, 下游 unstack 直接抛 ValueError."""
    bars = src.fetch_bars(dup, _DATES[0], _DATES[-1])
    assert not bars.index.duplicated().any()
    assert set(bars.index.get_level_values("symbol")) == {"600519.SH"}
    assert len(bars) == len(_DATES)
    # 关键: 下游宽表变换不能崩
    bars["close"].unstack("symbol")


def test_duplicate_symbols_in_data_layer():
    layer = DataLayer(source=JQDataSource(cache=FakeCache()),
                      bar_store=InMemoryShardedBarStore())
    bars = layer.daily(["600519.SH", "600519.SH"], _DATES[0], _DATES[-1], validate=True)
    assert not bars.index.duplicated().any()
    bars["close"].unstack("symbol")  # 不报 duplicate entries


def test_share_capital_dedup(src: JQDataSource):
    df = src.fetch_share_capital(["600519.SH", "600519.SH"], _DATES[0], _DATES[-1])
    assert not df.duplicated(["date", "symbol"]).any()


@pytest.mark.parametrize(
    ("level", "expected_parent"),
    [(1, None), (2, "801120"), (3, "801123")],
)
def test_industry_parent_code(src: JQDataSource, level: int, expected_parent):
    """⑪ SCHEMA_INDUSTRY 不变量: level>1 时 parent_code 必须存在."""
    ind = src.fetch_industry_classification(
        SYMS, (_DATES[0], _DATES[0]), system="sw", level=level
    )
    assert ind["parent_code"].iloc[0] == expected_parent


class ConceptDropCache(FakeCache):
    """概念 GN002 在第二个采样点之后消失(被移出)."""

    def get_concept(self, codes, date):
        base = [{"concept_code": "GN001", "concept_name": "白酒"}]
        if str(date) < "2024-03-01":
            base.append({"concept_code": "GN002", "concept_name": "消费升级"})
        return {c: {"jq_concept": base} for c in codes}


def test_concept_expired_date_inferred():
    """⑫ 按月采样已有序列 → 可推出 expired_date, 不应恒为 NaT."""
    src = JQDataSource(cache=ConceptDropCache())
    con = src.fetch_concepts(
        ["600519.SH"], (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-06-30"))
    )
    by_code = con.reset_index().set_index("concept_code")
    # 一直存在的概念 -> NaT
    assert pd.isna(by_code.loc["GN001", "expired_date"])
    # 中途移出的概念 -> 有具体日期
    exp = by_code.loc["GN002", "expired_date"]
    assert pd.notna(exp)
    assert exp >= pd.Timestamp("2024-03-01")


def test_concept_returns_intervals_not_snapshots(src: JQDataSource):
    """每个 (标的, 概念) 一行区间, 而非每个采样点一行."""
    con = src.fetch_concepts(
        ["600519.SH"], (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-06-30"))
    )
    assert len(con) == 1  # FakeCache 只给 1 个概念, 全区间存在
    assert pd.isna(con["expired_date"].iloc[0])


@pytest.mark.parametrize(
    ("idx", "expected"),
    [
        ([], []),
        ([0], [(0, 0)]),
        ([0, 1, 2], [(0, 2)]),
        ([0, 1, 2, 4, 5], [(0, 2), (4, 5)]),
        ([3, 0, 1], [(0, 1), (3, 3)]),  # 乱序输入
        ([0, 0, 1], [(0, 1)]),  # 重复输入
    ],
)
def test_consecutive_runs(idx: list[int], expected):
    from qlab.data.sources.jq_source import _consecutive_runs

    assert _consecutive_runs(idx) == expected


# ======================================================================
# ⑬ universe 剔除语义  ⑭ 不取未来时点  ⑮ 两口径对齐
# ======================================================================


class ShrinkingIndexCache(FakeCache):
    """000001 从 2024-03 起被移出指数成分."""

    def get_index_weights(self, code, date):
        self.calls.append("get_index_weights")
        members = (
            ["600519.XSHG"]
            if str(date) >= "2024-03-01"
            else ["600519.XSHG", "000001.XSHE"]
        )
        w = [100.0] if len(members) == 1 else [60.0, 40.0]
        return pd.DataFrame(
            {
                "date": [pd.Timestamp(date)] * len(members),
                "weight": w,
                "display_name": ["x"] * len(members),
            },
            index=members,
        )


def test_removed_member_leaves_universe_on_every_session():
    """⑬ 被剔除的成分股必须在**所有**交易日上消失(包括非采样点).

    这里锁住一个脉弱的实现细节: 必须用 ``reindex(method="ffill")``
    (找最近的更早整行, NaN 原样带过来), 而不能用 ``reindex(...).ffill()``
    (逐列填充会跳过 NaN, 把剔除前的 True 填过来) —— 后者会让 universe
    "只进不出", 股票池静默变大且业绩虚高。
    """
    src = JQDataSource(cache=ShrinkingIndexCache())
    uni = src.fetch_universe(
        "000300.SH", (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-06-28"))
    )
    after = uni[uni.index.get_level_values("date") >= pd.Timestamp("2024-03-01")]
    assert "000001.SZ" not in set(after.index.get_level_values("symbol"))
    # 剔除前仍应包含
    before = uni[uni.index.get_level_values("date") < pd.Timestamp("2024-03-01")]
    assert "000001.SZ" in set(before.index.get_level_values("symbol"))
    # 每一个交易日都要正确(采样点与非采样点同等)
    per_day = after.groupby(level="date").size()
    assert (per_day == 1).all()
    # 权重归一在成分变动后仍成立
    sums = uni["weight"].groupby(level="date").sum()
    assert ((sums - 1.0).abs() < 1e-9).all()
    validate_schema(uni, SCHEMA_UNIVERSE, strict_index=True)


def test_ffill_semantics_documented():
    """直接锁住两种写法的行为差异, 以防将来被"优化"成错的那个."""
    w = pd.DataFrame(
        {"A": [True, True, np.nan]},
        index=pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
    )
    full = pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01", "2024-03-15"])
    correct = w.reindex(full, method="ffill")["A"]
    wrong = w.reindex(full)["A"].ffill()
    assert pd.isna(correct.iloc[-1])   # 剔除后保持缺失
    assert bool(wrong.iloc[-1])        # 错的写法会填回 True


def test_sample_dates_never_exceed_today(src: JQDataSource):
    """⑭ 聚宽日历含未来两年, 但采样点不得越过今天."""
    today = pd.Timestamp.today().normalize()
    far = today + pd.Timedelta(days=400)
    samples = src._sample_dates((today - pd.Timedelta(days=30), far))
    assert samples, "历史部分应仍有采样点"
    assert max(samples) <= today


def test_pure_future_range_returns_empty(src: JQDataSource):
    today = pd.Timestamp.today().normalize()
    lo = today + pd.Timedelta(days=30)
    hi = today + pd.Timedelta(days=90)
    assert src._sample_dates((lo, hi)) == []
    assert src.fetch_universe("000300.SH", (lo, hi)).empty
    assert src.fetch_concepts(SYMS, (lo, hi)).empty


def test_mismatched_fq_indexes_raise():
    """⑮ 两个复权口径本应同源, 索引不一致说明上游异常 —— 应报错而非错位."""

    class Mismatch(FakeCache):
        def get_price_batch(self, codes, start, end, *, frequency="daily", fq="pre", **kw):
            df = _fake_bars(fq)
            if fq == "post":
                df = df.iloc[:-1]  # post 少一行
            return {c: df for c in codes}

    src = JQDataSource(cache=Mismatch())
    with pytest.raises(DataUnavailableError, match="索引不一致"):
        src.fetch_bars(SYMS, _DATES[0], _DATES[-1])


def test_missing_post_key_skipped():
    """post 字典缺 key 时跳过该标的, 而非 KeyError."""

    class PartialPost(FakeCache):
        def get_price_batch(self, codes, start, end, *, frequency="daily", fq="pre", **kw):
            out = {c: _fake_bars(fq) for c in codes}
            if fq == "post":
                out.pop(codes[-1], None)
            return out

    src = JQDataSource(cache=PartialPost())
    bars = src.fetch_bars(SYMS, _DATES[0], _DATES[-1])
    assert len(set(bars.index.get_level_values("symbol"))) == len(SYMS) - 1


# ======================================================================
# ⑯ 股本回填不得使用未来值(PIT)
# ======================================================================


class SplitSharesCache(FakeCache):
    """送股场景: 06-05 起股本由 1 亿翻倍为 2 亿.

    ``lookback`` 区间(区间起点之前)有送股前的 1 亿股数据。
    """

    def get_valuation(self, code, start, end, *, fields=None):
        self.calls.append("get_valuation")
        idx = pd.bdate_range(start, end)
        cap = [10_000.0 if d < pd.Timestamp("2024-06-05") else 20_000.0 for d in idx]
        return pd.DataFrame(
            {"capitalization": cap, "circulating_cap": cap}, index=idx
        )


def test_shares_never_backfilled_from_future():
    """⑯ 送转股前的日期不得拿到送转后的股本.

    早期实现用 bfill "补齐"缺失股本, 实测会把送股后的 2 亿股填到送股前,
    下游市值 = 过去价格 × 未来股本 → 虚高 100%。这是静默未来函数。
    """
    src = JQDataSource(cache=SplitSharesCache())
    bars = src.fetch_bars(["600519.SH"], _DATES[0], _DATES[-1])
    one = bars.xs("600519.SH", level="symbol")["total_shares"]
    pre = one[one.index < pd.Timestamp("2024-06-05")]
    post = one[one.index >= pd.Timestamp("2024-06-05")]
    assert (pre == 100_000_000).all(), f"送股前应为 1 亿股, 实际 {pre.tolist()}"
    assert (post == 200_000_000).all(), f"送股后应为 2 亿股, 实际 {post.tolist()}"


def test_shares_lookback_covers_gap_at_range_start():
    """区间起点当日无股本数据 → 用回看期的历史值(而非未来值)补上."""

    class GapAtStart(FakeCache):
        def get_valuation(self, code, start, end, *, fields=None):
            # 只在 lookback 期与区间末尾有数据, 区间前段缺失
            idx = pd.DatetimeIndex([pd.Timestamp("2024-05-20"), _DATES[-1]])
            return pd.DataFrame(
                {"capitalization": [10_000.0, 10_000.0],
                 "circulating_cap": [8_000.0, 8_000.0]},
                index=idx,
            )

    src = JQDataSource(cache=GapAtStart())
    bars = src.fetch_bars(["600519.SH"], _DATES[0], _DATES[-1])
    assert (bars["total_shares"] == 100_000_000).all()


def test_shares_missing_even_after_lookback_raises():
    """回看期内也无数据 → 报错, 不拿未来值蒙混."""

    class OnlyLateShares(FakeCache):
        def get_valuation(self, code, start, end, *, fields=None):
            # 仅最后一天有数据 → 前面的日期无往前参照
            return pd.DataFrame(
                {"capitalization": [20_000.0], "circulating_cap": [20_000.0]},
                index=pd.DatetimeIndex([_DATES[-1]]),
            )

    src = JQDataSource(cache=OnlyLateShares())
    with pytest.raises(DataUnavailableError, match="回看"):
        src.fetch_bars(["600519.SH"], _DATES[0], _DATES[-1])


# ======================================================================
# ⑰ source 跳层一致  ⑱ 采样频率与单日快捷路径
# ======================================================================


def test_concept_source_attribute_matches_output(src: JQDataSource):
    """⑰ 产出的 source 必须等于类属性, 调用方靠它而不是硬编码."""
    assert JQDataSource.concept_source == "jq"
    con = src.fetch_concepts(SYMS, (_DATES[0], _DATES[0]))
    assert set(con.index.get_level_values("source")) == {JQDataSource.concept_source}


def test_concept_source_flows_to_downstream_filter(src: JQDataSource):
    """⑰ 用类属性传给下游能拿到数据; 用默认 eastmoney 则静默为空."""
    from qlab.data.concept import concepts_as_of

    flat = src.fetch_concepts(SYMS, (_DATES[0], _DATES[0])).reset_index()
    hit = concepts_as_of(flat, SYMS, _DATES[0], source=JQDataSource.concept_source)
    miss = concepts_as_of(flat, SYMS, _DATES[0])  # 默认 eastmoney
    assert len(hit) > 0
    assert len(miss) == 0, "默认值不匹配时会静默返回空 —— 文档已记录此行为"


def test_explicit_source_overrides_default(src: JQDataSource):
    con = src.fetch_concepts(SYMS, (_DATES[0], _DATES[0]), source="eastmoney")
    assert set(con.index.get_level_values("source")) == {"eastmoney"}


@pytest.mark.parametrize(
    ("freq", "max_samples"),
    [("M", 14), ("Q", 6), ("Y", 3)],
)
def test_sample_freq_controls_request_count(freq: str, max_samples: int):
    """⑱ 采样频率直接决定远程请求数."""
    cache = FakeCache()
    src = JQDataSource(cache=cache)
    rng = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31"))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        src.fetch_concepts(SYMS, rng, sample_freq=freq)
    n = cache.calls.count("get_concept")
    # 下界也要断言: 否则计数器坏掉时 "n <= max" 会假通过
    assert n > 0, "计数器未生效"
    assert n <= max_samples, f"freq={freq} 产生 {n} 个采样点"


def test_coarser_freq_means_fewer_requests():
    """M > Q > Y 的请求数单调递减."""
    rng = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31"))
    counts = {}
    for freq in ("M", "Q", "Y"):
        cache = FakeCache()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            JQDataSource(cache=cache).fetch_concepts(SYMS, rng, sample_freq=freq)
        counts[freq] = cache.calls.count("get_concept")
    assert counts["M"] > counts["Q"] > counts["Y"], counts


def test_bad_sample_freq_raises(src: JQDataSource):
    with pytest.raises(ValueError, match="sample_freq"):
        src.fetch_concepts(SYMS, (_DATES[0], _DATES[-1]), sample_freq="W")


def test_concepts_asof_single_request(src: JQDataSource):
    """⑱ 只需某一天时只发 1 次请求(对比 5 年区间的 60+ 次)."""
    src.cache.calls.clear()
    con = src.concepts_asof(SYMS, _DATES[0])
    assert src.cache.calls.count("get_concept") == 1
    assert len(con) > 0
    assert con["expired_date"].isna().all()
    # 仍符合 schema, 可直接喂下游
    assert list(con.index.names) == ["effective_date", "symbol", "source"]


def test_long_range_warns_with_actionable_hint():
    """长区间告警应指出出路(concepts_asof / sample_freq)."""
    src = JQDataSource(cache=FakeCache())
    long_rng = (pd.Timestamp("2024-01-01"), pd.Timestamp.today().normalize())
    with pytest.warns(RuntimeWarning, match="concepts_asof"):
        src.fetch_concepts(SYMS, long_rng)


def test_industry_sample_freq(src: JQDataSource):
    cache = FakeCache()
    s = JQDataSource(cache=cache)
    rng = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31"))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        ind = s.fetch_industry_classification(SYMS, rng, sample_freq="Y")
    n = cache.calls.count("get_industry")
    assert 0 < n <= 3, f"年度采样应很少请求, 实际 {n}"
    assert len(ind.index.get_level_values("date").unique()) <= 3


# ======================================================================
# ㉑ DataLayer.industry() 编排  ㉒ concepts source 默认值向数据源取
# ======================================================================


def test_layer_industry_end_to_end():
    """㉑ industry_as_of 曾按 column 访问 system, 但 schema 把它放在 index → KeyError."""
    from qlab.data.industry import industry_as_of

    src = JQDataSource(cache=FakeCache())
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = src.fetch_industry_classification(
            SYMS, (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-06-20"))
        )
    # system 在 index 里
    assert "system" in df.index.names
    # 直接喂 industry_as_of 不应崩
    r = industry_as_of(df, SYMS, pd.Timestamp("2024-06-20"), "sw", 1)
    assert r["600519.SH"] == "801120"


def test_layer_industry_via_datalayer():
    """走完整 DataLayer.industry() 路径."""
    from qlab.data.store import InMemoryBarStore

    layer = DataLayer(
        source=JQDataSource(cache=FakeCache()),
        bar_store=InMemoryShardedBarStore(),
        store=InMemoryBarStore(),
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        r = layer.industry(SYMS, pd.Timestamp("2024-06-20"), system="sw", level=1)
    assert r["600519.SH"] == "801120"


def test_industry_as_of_flat_form_still_works():
    """平铺列形式(system 在 columns)也要兼容."""
    from qlab.data.industry import industry_as_of

    flat = pd.DataFrame(
        {
            "date": [pd.Timestamp("2024-01-01")],
            "symbol": ["600519.SH"],
            "system": ["sw"],
            "level": [1],
            "industry_code": ["801120"],
            "industry_name": ["食品饮料"],
        }
    )
    r = industry_as_of(flat, ["600519.SH"], pd.Timestamp("2024-06-20"), "sw", 1)
    assert r["600519.SH"] == "801120"


def test_layer_concepts_source_defaults_to_datasource():
    """㉒ layer.concepts() 不传 source 时应向数据源取, 而非硬编码 eastmoney."""
    from qlab.data.store import InMemoryBarStore

    layer = DataLayer(
        source=JQDataSource(cache=FakeCache()),
        bar_store=InMemoryShardedBarStore(),
        store=InMemoryBarStore(),
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        con = layer.concepts(SYMS, pd.Timestamp("2024-06-20"))  # 不传 source
    # 数据源是 jq → 应命中而非空
    assert len(con) > 0
    assert con["concept_name"].tolist()[0] == "白酒"


# ======================================================================
# ㉓ FeatureContext.concepts source 默认值(features 层消费)
# ======================================================================


def _make_feature_context(dates):
    from qlab.core.calendar import get_default_calendar
    from qlab.data.store import InMemoryBarStore
    from qlab.data.universe import Universe, UniverseSpec
    from qlab.features.context import FeatureContext

    layer = DataLayer(
        source=JQDataSource(cache=FakeCache()),
        bar_store=InMemoryShardedBarStore(),
        store=InMemoryBarStore(),
    )
    idx = pd.MultiIndex.from_product(
        [dates, SYMS], names=["date", "symbol"]
    )
    udf = pd.DataFrame(
        {"in_universe": True, "weight": [0.6, 0.4] * len(dates)}, index=idx
    )
    uni = Universe(udf, UniverseSpec("test"))
    return FeatureContext(
        data=layer,
        target_dates=pd.DatetimeIndex(dates),
        universe=uni,
        calendar=get_default_calendar(),
    )


def test_feature_context_concepts_source_defaults():
    """㉓ FeatureContext.concepts() 硬编码 eastmoney 会与聚宽 source=jq 失配 → 空."""
    ctx = _make_feature_context([pd.Timestamp("2024-06-20")])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        c = ctx.concepts(date=pd.Timestamp("2024-06-20"))  # 不传 source
    assert len(c) > 0
    assert c["concept_name"].tolist()[0] == "白酒"


def test_feature_context_industry_via_jq():
    """FeatureContext.industry() 走 data.industry() → industry_as_of, 不应崩 KeyError."""
    ctx = _make_feature_context([pd.Timestamp("2024-06-20")])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        ind = ctx.industry(date=pd.Timestamp("2024-06-20"))
    assert ind["600519.SH"] == "801120"


# ======================================================================
# B 类新数据源: 两融/资金流/集合竞价/龙虎榜/因子
# ======================================================================

from qlab.core.schema import (  # noqa: E402
    SCHEMA_BILLBOARD,
    SCHEMA_CALL_AUCTION,
    SCHEMA_FACTOR_EXPOSURE,
    SCHEMA_INDEX_VALUATION,
    SCHEMA_MARGIN_TRADING,
    SCHEMA_MONEY_FLOW,
)


def test_fetch_index_valuation_converts_units(src: JQDataSource):
    """jq 换手是百分数、市值是亿元 → qlab 小数与元。"""
    df = src.fetch_index_valuation("000985.CSI", _DATES[0], _DATES[-1])
    validate_schema(df, SCHEMA_INDEX_VALUATION, strict_index=True)
    assert df["turnover"].iloc[0] == pytest.approx(0.017916)
    assert df["circulating_mcap"].iloc[0] == pytest.approx(944382.3351 * 1e8)
    assert df["amount"].iloc[0] == pytest.approx(0.017916 * 944382.3351 * 1e8)
    assert (df["symbol"] == "000985.CSI").all()
    assert "get_index_valuation" in src.cache.calls


def test_fetch_margin_trading(src: JQDataSource):
    m = src.fetch_margin_trading(SYMS, _DATES[0], _DATES[-1])
    validate_schema(m, SCHEMA_MARGIN_TRADING, strict_index=True)
    assert list(m.index.names) == ["date", "symbol"]
    assert m.xs("600519.SH", level="symbol")["fin_balance"].iloc[0] == 2.1e10


def test_fetch_money_flow_unit_conversion(src: JQDataSource):
    """聚宽原始为万元, 接入需 ×1e4 换为元."""
    f = src.fetch_money_flow(SYMS, _DATES[0], _DATES[-1])
    validate_schema(f, SCHEMA_MONEY_FLOW, strict_index=True)
    # 假 cache 主力=4.0(万元) → 应为 4e4 元
    assert f.xs("600519.SH", level="symbol")["net_amount_main"].iloc[0] == 4e4


def test_fetch_call_auction(src: JQDataSource):
    c = src.fetch_call_auction(SYMS, _DATES[0], _DATES[-1])
    validate_schema(c, SCHEMA_CALL_AUCTION, strict_index=True)
    assert "ask1_price" in c.columns and "bid5_volume" in c.columns
    assert c["auction_price"].iloc[0] == 1650.0
    # get_call_auction 是单标的接口: N 个 symbol 应调 N 次(而非传列表)
    assert src.cache.calls.count("get_call_auction") == len(SYMS)
    # 索引必须单调(否则下游按日期切片的分片落盘会崩)
    assert c.index.is_monotonic_increasing
    assert c.index.get_level_values("date").is_monotonic_increasing


def test_fetch_billboard(src: JQDataSource):
    b = src.fetch_billboard(_DATES[0], symbols=SYMS, count=1)
    validate_schema(b, SCHEMA_BILLBOARD, strict_index=False)
    assert set(b["direction"]) <= {"BUY", "SELL", "ALL"}
    # net_value = buy - sell
    assert (b["net_value"] == b["buy_value"] - b["sell_value"]).all()


def test_fetch_factor_exposure(src: JQDataSource):
    fe = src.fetch_factor_exposure(SYMS, ["size", "beta"], _DATES[0], _DATES[-1])
    validate_schema(fe, SCHEMA_FACTOR_EXPOSURE, strict_index=True)
    assert list(fe.columns) == ["size", "beta"]
    assert list(fe.index.names) == ["date", "symbol"]


def _b_layer():
    from qlab.data.store import InMemoryBarStore

    return DataLayer(
        source=JQDataSource(cache=FakeCache()),
        bar_store=InMemoryShardedBarStore(),
        store=InMemoryBarStore(),
    )


def test_layer_margin_money_auction_orchestration():
    """三个时序类走 DataLayer._timeseries 分片缓存编排."""
    layer = _b_layer()
    m = layer.margin_trading(SYMS, "2024-06-03", "2024-06-06", validate=True)
    f = layer.money_flow(SYMS, "2024-06-03", "2024-06-06", validate=True)
    c = layer.call_auction(SYMS, "2024-06-03", "2024-06-06", validate=True)
    assert len(m) > 0 and len(f) > 0 and len(c) > 0
    # 二次命中缓存不再调数据源
    n = len(layer.source.cache.calls)
    layer.margin_trading(SYMS, "2024-06-03", "2024-06-06", validate=True)
    assert len(layer.source.cache.calls) == n


def test_layer_billboard_and_factor():
    layer = _b_layer()
    b = layer.billboard(pd.Timestamp("2024-06-03"), symbols=SYMS, count=1)
    assert len(b) > 0
    fe = layer.factor_exposure(SYMS, ["size", "beta"], "2024-06-03", "2024-06-06")
    assert list(fe.columns) == ["size", "beta"]


def test_fake_source_implements_b_methods():
    """FakeDataSource 也实现 B 类, 保证 Protocol 完整."""
    from qlab.data.sources import FakeDataSource

    f = FakeDataSource(n_symbols=3)
    d0, d1 = pd.Timestamp("2024-06-03"), pd.Timestamp("2024-06-07")
    syms = ["000001.SZ", "000002.SZ"]
    validate_schema(f.fetch_margin_trading(syms, d0, d1), SCHEMA_MARGIN_TRADING, strict_index=True)
    validate_schema(f.fetch_money_flow(syms, d0, d1), SCHEMA_MONEY_FLOW, strict_index=True)
    validate_schema(f.fetch_call_auction(syms, d0, d1), SCHEMA_CALL_AUCTION, strict_index=True)
    validate_schema(f.fetch_billboard(d1, syms), SCHEMA_BILLBOARD, strict_index=False)
    validate_schema(f.fetch_factor_exposure(syms, ["size"], d0, d1), SCHEMA_FACTOR_EXPOSURE, strict_index=True)
    validate_schema(f.fetch_index_valuation("000985.CSI", d0, d1), SCHEMA_INDEX_VALUATION, strict_index=True)


# ======================================================================
# fetch_fundamentals: 走 jq get_fundamentals, 只做字段映射
# ======================================================================

from qlab.core.enums import ReportType  # noqa: E402
from qlab.core.schema import SCHEMA_FUNDAMENTAL  # noqa: E402


def test_fetch_fundamentals_official(src: JQDataSource):
    of = src.fetch_fundamentals(
        SYMS, pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31"),
        [ReportType.OFFICIAL],
    )
    validate_schema(of, SCHEMA_FUNDAMENTAL, strict_index=False)
    assert "net_profit" in of.columns
    assert set(of["report_type"]) == {"official"}
    # 三表各调一次 get_fundamentals
    assert src.cache.calls.count("get_fundamentals:STK_INCOME_STATEMENT") == 1
    assert src.cache.calls.count("get_fundamentals:STK_BALANCE_SHEET") == 1
    assert src.cache.calls.count("get_fundamentals:STK_CASHFLOW_STATEMENT") == 1


def test_fetch_fundamentals_forecast(src: JQDataSource):
    fc = src.fetch_fundamentals(
        SYMS, pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31"),
        [ReportType.FORECAST],
    )
    validate_schema(fc, SCHEMA_FUNDAMENTAL, strict_index=False)
    assert set(fc["report_type"]) == {"forecast"}
    assert "forecast_net_profit_min" in fc.columns
    assert fc["forecast_type"].iloc[0] == "业绩预增"


def test_fetch_fundamentals_flash_skipped(src: JQDataSource):
    """聚宽无快报专表, FLASH 静默跳过(返回空而非报错)."""
    r = src.fetch_fundamentals(
        SYMS, pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31"),
        [ReportType.FLASH],
    )
    assert r.empty


def test_fetch_fundamentals_pit_columns(src: JQDataSource):
    """announce_date/available_at/report_period 齐全且语义正确."""
    of = src.fetch_fundamentals(
        SYMS, pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31"),
        [ReportType.OFFICIAL],
    )
    # available_at = 披露日 09:30
    row = of.iloc[0]
    assert row["available_at"] == row["announce_date"].normalize() + pd.Timedelta(hours=9, minutes=30)
    # 正式表: 披露日 ≥ 报告期
    assert (of["announce_date"] >= of["report_period"]).all()


def test_layer_concepts_uses_asof_single_request():
    """㉔ layer.concepts(date) 应走数据源 concepts_asof(1 次远程),
    而非为"某一天"拉 5 年 lookback(约 60 次)."""
    layer = _b_layer()
    layer.source.cache.calls.clear()
    c = layer.concepts(SYMS, pd.Timestamp("2024-06-14"))
    assert len(c) > 0
    # 单点查询: get_concept 只应调 1 次(而非按月采样几十次)
    n = layer.source.cache.calls.count("get_concept")
    assert n == 1, f"concepts(date) 应只发 1 次 get_concept, 实际 {n}"


def test_layer_concepts_asof_cache_hit():
    """二次查同一天应命中缓存, 不再远程."""
    layer = _b_layer()
    layer.concepts(SYMS, pd.Timestamp("2024-06-14"))
    n = len(layer.source.cache.calls)
    layer.concepts(SYMS, pd.Timestamp("2024-06-14"))
    assert len(layer.source.cache.calls) == n


def _make_ctx_for_ts():
    from qlab.core.calendar import get_default_calendar
    from qlab.data.store import InMemoryBarStore
    from qlab.data.universe import Universe, UniverseSpec
    from qlab.features.context import FeatureContext

    layer = DataLayer(
        source=JQDataSource(cache=FakeCache()),
        bar_store=InMemoryShardedBarStore(),
        store=InMemoryBarStore(),
    )
    dates = pd.DatetimeIndex(_DATES)
    idx = pd.MultiIndex.from_product([dates, SYMS], names=["date", "symbol"])
    udf = pd.DataFrame({"in_universe": True, "weight": 0.5}, index=idx)
    uni = Universe(udf, UniverseSpec("test"))
    return FeatureContext(data=layer, target_dates=dates, universe=uni,
                          calendar=get_default_calendar())


def test_feature_context_exposes_timeseries_methods():
    """B 类时序数据必须在 FeatureContext 有消费入口(否则接了个孤岛)."""
    ctx = _make_ctx_for_ts()
    m = ctx.margin_trading()
    f = ctx.money_flow()
    c = ctx.call_auction()
    assert len(m) > 0 and list(m.index.names) == ["date", "symbol"]
    assert len(f) > 0 and "net_amount_main" in f.columns
    assert len(c) > 0 and "auction_price" in c.columns


def test_fundamental_asof_no_daily_cache_avalanche():
    """㉗ 相邻交易日查 fundamental 不应雪崩重取(缓存键须对齐到月边界)."""
    layer = _b_layer()
    layer.fundamental_as_of(SYMS, "net_profit", pd.Timestamp("2024-06-14"))
    n = len([c for c in layer.source.cache.calls if "get_fundamentals" in c])
    # 相邻日: 600 天窗口对齐到月后完全相同, 应 0 次新远程
    layer.fundamental_as_of(SYMS, "net_profit", pd.Timestamp("2024-06-13"))
    n2 = len([c for c in layer.source.cache.calls if "get_fundamentals" in c])
    assert n2 == n, f"相邻日雪崩: 6/14 后 {n} 次, 6/13 后 {n2} 次"


def test_fundamental_asof_pit_still_exact_after_month_align():
    """月边界对齐是缓存优化, PIT 精度必须不变: 未来披露的报表不可见."""
    src = JQDataSource(cache=FakeCache())
    of = src.fetch_fundamentals(
        SYMS, pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31")
    )
    # FakeCache 造两期: 报告期 3/31(披露 4/27) 与 6/30(披露 8/09)
    visible_0614 = of[of["available_at"] <= pd.Timestamp("2024-06-14")]
    visible_0810 = of[of["available_at"] <= pd.Timestamp("2024-08-10")]
    # 6/14 只能看到 4/27 那期; 8/09 披露的 Q2 对 6/14 是未来, 不可见
    assert len(visible_0614) < len(visible_0810), "6/14 不应看到 8/09 披露的报表(未来函数)"
    assert (visible_0614["announce_date"] <= pd.Timestamp("2024-06-14")).all()


def test_layer_industry_uses_asof_single_request():
    """㉘ layer.industry(date) 应走数据源 industry_asof(1 次远程),
    而非为"某一天"拉 5 年 lookback(约 60 次), 与 concepts 同源问题."""
    layer = _b_layer()
    layer.source.cache.calls.clear()
    r = layer.industry(SYMS, pd.Timestamp("2024-06-14"))
    assert r["600519.SH"] == "801120"
    n = layer.source.cache.calls.count("get_industry")
    assert n == 1, f"industry(date) 应只发 1 次 get_industry, 实际 {n}"


def test_layer_industry_asof_cache_hit():
    """相邻日应命中缓存, 不雪崩."""
    layer = _b_layer()
    layer.industry(SYMS, pd.Timestamp("2024-06-14"))
    n = len(layer.source.cache.calls)
    layer.industry(SYMS, pd.Timestamp("2024-06-13"))
    # 不同日期是不同 asof 快照, 但至少不应是 62 次的雪崩
    added = len(layer.source.cache.calls) - n
    assert added <= 1, f"相邻日 industry 新增远程 {added} 次(疑似雪崩)"


# ======================================================================
# 通用不变量: 所有"按 date 单点查询"的 layer 方法都不得雪崩
# ----------------------------------------------------------------------
# 背景: concepts(㉔)/fundamentals(㉗)/industry(㉘) 三次都栽在同一个坑——
#   date 语义的"某天"查询却把浮动 date 塞进缓存键, 相邻交易日全 miss。
# 本测试遍历所有 date 单点入口, 断言"相邻日查询"不会退化成整段历史重取,
# 将来任何新增的按日方法若踩同样的坑会被自动拦住(防第四个孪生兄弟)。
# ======================================================================

# (方法名, 调用 lambda) —— 每个都是 date 单点语义
_DATE_POINT_QUERIES = [
    ("industry", lambda lyr, d: lyr.industry(SYMS, d)),
    ("concepts", lambda lyr, d: lyr.concepts(SYMS, d)),
    ("fundamental_as_of", lambda lyr, d: lyr.fundamental_as_of(SYMS, "net_profit", d)),
    ("fundamental_ttm", lambda lyr, d: lyr.fundamental_ttm(SYMS, "net_profit", d)),
]


@pytest.mark.parametrize("name,call", _DATE_POINT_QUERIES, ids=[q[0] for q in _DATE_POINT_QUERIES])
def test_date_point_query_no_cache_avalanche(name, call):
    """相邻交易日重复查询, 新增远程请求必须远低于"整段历史重取"的量级。

    雪崩阈值设 5: 单点查询(concepts/industry=1, fundamentals 三表≤4)都远低于此;
    而按月采样 5 年历史(约 60 次)会远超。相邻日至多补 1 个新月份的缓存。
    """
    layer = _b_layer()
    call(layer, pd.Timestamp("2024-06-14"))
    n = len(layer.source.cache.calls)
    call(layer, pd.Timestamp("2024-06-13"))  # 相邻交易日
    added = len(layer.source.cache.calls) - n
    assert added <= 5, f"{name}: 相邻日新增远程 {added} 次, 疑似浮动 date 污染缓存键(雪崩)"


# ======================================================================
# ㉙ 边界品种: daily() 仅支持个股, ETF/指数 fail-loud
# ----------------------------------------------------------------------
# 教训: 之前单测标的只有 4 个大盘白马, ETF/指数/退市股从未真实覆盖。
# ======================================================================


@pytest.mark.parametrize(
    "symbol,is_stock",
    [
        ("600519.SH", True),   # 沪主板
        ("688981.SH", True),   # 科创板
        ("000001.SZ", True),   # 深主板
        ("300750.SZ", True),   # 创业板
        ("510300.SH", False),  # ETF
        ("000001.SH", False),  # 上证指数(与深市个股 000001.SZ 同号, 靠后缀区分)
        ("159915.SZ", False),  # 深市 ETF
        ("399006.SZ", False),  # 深市指数
    ],
)
def test_is_stock_symbol(symbol, is_stock):
    from qlab.data.sources.jq_source import _is_stock_symbol

    assert _is_stock_symbol(symbol) is is_stock


def test_daily_rejects_etf_index_fail_loud():
    """daily() 遇 ETF/指数应 fail-loud(DataUnavailableError), 而非伪造股票字段."""
    from qlab.core.exceptions import DataUnavailableError

    src = JQDataSource(cache=FakeCache())
    # FakeCache 会正常返回价格, 但 _attach_daily_only 应在入口拦住非个股
    with pytest.raises(DataUnavailableError, match="仅支持个股"):
        src.fetch_bars(["510300.SH"], pd.Timestamp("2024-06-03"), pd.Timestamp("2024-06-05"))


def test_daily_mixed_basket_rejects_if_any_non_stock():
    """混合篮子里只要有一个非个股, 整批 fail-loud(不静默丢弃)."""
    from qlab.core.exceptions import DataUnavailableError

    src = JQDataSource(cache=FakeCache())
    with pytest.raises(DataUnavailableError, match="仅支持个股"):
        src.fetch_bars(
            ["600519.SH", "510300.SH"],
            pd.Timestamp("2024-06-03"), pd.Timestamp("2024-06-05"),
        )


# ======================================================================
# 数值对账: B 类数据的字段映射/单位换算必须精确(不只是"能跑通")
# ======================================================================


def test_money_flow_unit_conversion_exact():
    """money_flow 主力净额: 聚宽万元 ×1e4 精确换算为元(FakeCache 造 4.0万)."""
    src = JQDataSource(cache=FakeCache())
    mf = src.fetch_money_flow(SYMS, _DATES[0], _DATES[-1])
    # FakeCache net_amount_main=4.0(万元) → 应为 4e4 元
    assert (mf["net_amount_main"] == 4e4).all(), "万元→元 换算应精确 ×1e4"


def test_call_auction_price_maps_current():
    """call_auction 竞价价直接映射 jq current 字段(不做任何变换)."""
    src = JQDataSource(cache=FakeCache())
    ca = src.fetch_call_auction(SYMS, _DATES[0], _DATES[-1])
    # FakeCache current=1650.0
    assert (ca["auction_price"] == 1650.0).all()


def test_margin_balance_maps_fin_value():
    """两融融资余额直接映射 jq fin_value(元, 不换算)."""
    src = JQDataSource(cache=FakeCache())
    m = src.fetch_margin_trading(SYMS, _DATES[0], _DATES[-1])
    # FakeCache fin_value=2.1e10
    assert (m["fin_balance"] == 2.1e10).all()


def test_margin_negative_flow_allowed_negative_balance_rejected():
    """两融不变量: 负发生额合法(交易所冲销, 实测 000999 2024-06-14),
    负余额非法。大样本扩测(50只)抓到的真实数据边界."""
    from qlab.core.exceptions import SchemaViolationError
    from qlab.core.schema import SCHEMA_MARGIN_TRADING

    idx = pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2024-06-14"), "000999.SZ")], names=["date", "symbol"]
    )
    avail = [pd.Timestamp("2024-06-17 09:30")]  # 下一交易日(周一)
    # 负发生额(sec_repay<0): 应通过
    ok_df = pd.DataFrame(
        {"available_at": avail, "fin_balance": [4.39e8], "sec_repay": [-66462.0]},
        index=idx,
    )
    validate_schema(ok_df, SCHEMA_MARGIN_TRADING, strict_index=True)
    # 负余额: 应被拦
    bad_df = pd.DataFrame(
        {"available_at": avail, "fin_balance": [-1.0]}, index=idx
    )
    with pytest.raises(SchemaViolationError, match="fin_balance"):
        validate_schema(bad_df, SCHEMA_MARGIN_TRADING, strict_index=True)


# ======================================================================
# available_at: 发布语义由 jq 层给出, qlab 只映射(职责解耦)
# ----------------------------------------------------------------------
# 两融/资金流: T 日收盘后发布 → available_at = 下一交易日 09:30
# 集合竞价:    09:25 竞价结束即确定 → available_at = 当日 09:30
# ======================================================================


def test_available_at_margin_is_next_session(src: JQDataSource):
    """两融 available_at 必严格晚于数据日(T 日盘中不可用)."""
    m = src.fetch_margin_trading(SYMS, _DATES[0], _DATES[-1])
    assert "available_at" in m.columns
    d = m.index.get_level_values("date")
    assert (m["available_at"] > d).all(), "两融 T 日盘中不可用, available_at 须晚于 date"


def test_available_at_money_flow_is_next_session(src: JQDataSource):
    """资金流同理: 当日全天汇总, 收盘后才产生."""
    f = src.fetch_money_flow(SYMS, _DATES[0], _DATES[-1])
    d = f.index.get_level_values("date")
    assert (f["available_at"] > d).all()


def test_available_at_call_auction_is_same_session(src: JQDataSource):
    """集合竞价 available_at 在**当日**(盘中策略能用它的前提)."""
    c = src.fetch_call_auction(SYMS, _DATES[0], _DATES[-1])
    d = c.index.get_level_values("date")
    av = c["available_at"]
    assert (av >= d).all() and (av < d + pd.Timedelta(days=1)).all(), (
        "竞价数据当日即可用, available_at 应落在当日内"
    )


def test_available_at_enables_pit_filter(src: JQDataSource):
    """available_at 的用途: 下游能按"某时刻可见"过滤, 挡掉尚未发布的数据."""
    m = src.fetch_margin_trading(SYMS, _DATES[0], _DATES[-1])
    # 站在首日收盘时刻: 当日两融尚未发布, 应全部不可见
    t0 = _DATES[0] + pd.Timedelta(hours=15)
    visible = m[m["available_at"] <= t0]
    assert len(visible) == 0, "T 日 15:00 时当日两融还没发布, 不应可见"
    # 站在次日开盘后: 首日数据应可见
    t1 = _DATES[0] + pd.Timedelta(days=1, hours=10)
    assert len(m[m["available_at"] <= t1]) > 0


# ======================================================================
# 55/56 非个股标的的行为一致性
# ======================================================================


def test_bj_symbols_rejected_with_reason():
    """55 北交所标的必须明确拒绝 —— 聚宽股票池不含北交所.

    早前 `_is_stock_symbol` 声称支持 .BJ 而 `to_jq_code` 不认, 内部矛盾:
    北交所标的过不了任何接口, 报的却是"无法识别的交易所后缀"这种误导信息。
    实测 get_all_securities(types=['stock']) 的 5115 只全是 XSHG/XSHE。
    """
    from qlab.data.sources.jq_source import _is_stock_symbol, to_jq_code

    # 不再声称支持北交所
    assert not _is_stock_symbol("832000.BJ")
    assert not _is_stock_symbol("430047.BJ")
    # 仍支持的四个板块
    assert _is_stock_symbol("600519.SH")   # 沪主板
    assert _is_stock_symbol("688981.SH")   # 科创板
    assert _is_stock_symbol("000858.SZ")   # 深主板
    assert _is_stock_symbol("300750.SZ")   # 创业板
    # ETF/指数仍不是个股
    assert not _is_stock_symbol("510300.SH")
    assert not _is_stock_symbol("000300.SH")

    # to_jq_code 对 .BJ 给出专门说明(而非泛泛的"无法识别后缀")
    with pytest.raises(ValueError, match="不支持北交所"):
        to_jq_code("832000.BJ")
    # 其他非法后缀仍是通用报错
    with pytest.raises(ValueError, match="无法识别的交易所后缀"):
        to_jq_code("600519.XX")


def test_money_flow_rejects_non_stock():
    """56 money_flow 对非个股应 fail-loud, 与 daily() 行为一致.

    早前直接透出聚宽的 "get_money_flow只能用来查询股票的资金流向数据",
    调用方看不出该怎么办。
    """
    from qlab.core.exceptions import DataUnavailableError

    src = JQDataSource(cache=FakeCache())
    for bad in (["510300.SH"], ["000300.SH"], ["600519.SH", "159915.SZ"]):
        with pytest.raises(DataUnavailableError, match="仅支持个股"):
            src.fetch_money_flow(bad, _DATES[0], _DATES[-1])
    # 纯个股正常
    got = src.fetch_money_flow(["600519.SH"], _DATES[0], _DATES[-1])
    assert len(got) > 0
