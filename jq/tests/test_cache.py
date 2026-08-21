"""``jq.cache.DataCache`` 单元测试 — 用假 runner, 不触网.

重点覆盖:
- ① 复权换算(缓存恒为 post, 本地推导 raw/pre)与 PIT 稳定性
- ⑥ 只补缺失月, 不跨越空档重取
- ⑦ 批量与单只共用同一份缓存
- ⑫ 当月不落盘
- ⑬ fields 校验
"""

from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

import pandas as pd
import pytest

from jq.cache import DataCache
from jq.cache_store import current_month, month_bounds

# 构造一段"后复权"行情: 2024-01 与 2024-02 各 2 天, 2024-02 起 factor 跳变
_FACTORS = {"2024-01": 2.0, "2024-02": 2.5}


def _make_post_bars(start: str, end: str) -> pd.DataFrame:
    """生成后复权口径的假行情(raw close 恒为 10.0, 便于验算)."""
    days = pd.bdate_range(start, end)
    if len(days) == 0:
        return pd.DataFrame()
    rows = []
    for d in days:
        f = _FACTORS.get(d.strftime("%Y-%m"), 3.0)
        rows.append(
            {
                "open": 10.0 * f, "close": 10.0 * f,
                "low": 9.0 * f, "high": 11.0 * f,
                "volume": 1000.0 / f, "money": 10000.0,
                "factor": f,
                "high_limit": 11.0 * f, "low_limit": 9.0 * f,
                "avg": 10.0 * f, "pre_close": 10.0 * f,
                "paused": 0.0,
            }
        )
    return pd.DataFrame(rows, index=days)


class FakeRunner:
    """记录远程调用并按请求区间返回假数据."""

    def __init__(self, empty: bool = False):
        self.calls: list[str] = []
        self.timeouts: list[float] = []
        self.empty = empty

    def run_code(self, code: str, *, timeout: float = 60.0) -> dict:
        self.calls.append(code)
        self.timeouts.append(timeout)
        m = re.search(r"start_date='([\d-]+)', end_date='([\d-]+)'", code)
        start, end = (m.group(1), m.group(2)) if m else ("2024-01-01", "2024-01-02")

        if "get_all_trade_days" in code:
            days = pd.bdate_range("2023-12-01", "2024-12-31")
            return {"error": None, "stdout": _encode([d.date() for d in days])}
        if self.empty:
            result: object = pd.DataFrame()
        elif "get_index_valuation" in code:
            days = pd.bdate_range(start, end)
            result = pd.DataFrame({
                "code": "000001.XSHG",
                "day": days,
                "turnover_ratio": 1.2,
                "circulating_market_cap": 500000.0,
                "market_cap": 600000.0,
                "pe_ratio": 16.0,
                "pb_ratio": 1.4,
            }) if len(days) else pd.DataFrame()
        elif "get_price([" in code:  # 批量: 长表 (time, code, ...)
            codes = re.findall(r"'(\d{6}\.XSH[EG])'", code.split("get_price([")[1])
            frames = []
            for c in dict.fromkeys(codes):
                df = _make_post_bars(start, end)
                if len(df) == 0:
                    continue
                df = df.reset_index().rename(columns={"index": "time"})
                df["code"] = c
                frames.append(df)
            result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        else:
            result = _make_post_bars(start, end)

        return {"error": None, "stdout": _encode(result)}

    @property
    def fetch_count(self) -> int:
        return len(self.calls)


def _encode(obj: object) -> str:
    """模拟远程侧的 base64-pickle 输出格式."""
    from jq.serialize import encode_for_test

    return encode_for_test(obj)


@pytest.fixture
def dc(tmp_path: Path) -> DataCache:
    return DataCache(cache_dir=tmp_path, runner=FakeRunner())


# ======================================================================
# ① 复权换算 / PIT
# ======================================================================


def test_post_is_returned_as_cached(dc: DataCache):
    df = dc.get_price("600519.XSHG", "2024-01-01", "2024-01-31", fq="post")
    assert df["close"].iloc[0] == pytest.approx(20.0)   # 10 * 2.0
    assert df["factor"].iloc[0] == pytest.approx(2.0)


def test_raw_is_derived_from_post(dc: DataCache):
    """fq=None: 价格 /factor, 成交量 *factor, factor 列归 1."""
    df = dc.get_price("600519.XSHG", "2024-01-01", "2024-01-31", fq=None)
    assert df["close"].iloc[0] == pytest.approx(10.0)
    assert df["high_limit"].iloc[0] == pytest.approx(11.0)
    assert df["volume"].iloc[0] == pytest.approx(1000.0)
    assert (df["factor"] == 1.0).all()
    assert df["money"].iloc[0] == pytest.approx(10000.0)  # 成交额不随复权变


def test_pre_is_derived_with_ref_factor(dc: DataCache):
    """fq='pre': 以区间末日为基准, 末日 factor 归 1."""
    df = dc.get_price("600519.XSHG", "2024-01-01", "2024-02-29", fq="pre")
    assert df["factor"].iloc[-1] == pytest.approx(1.0)
    # 1月的 pre 价 = post/f_ref = 20.0/2.5 = 8.0
    assert df["close"].iloc[0] == pytest.approx(8.0)
    # 末日 pre 价 = raw 价
    assert df["close"].iloc[-1] == pytest.approx(10.0)


def test_pre_ref_date_does_not_pollute_cache(dc: DataCache):
    """① 核心: 不同基准日只改变返回值, 不改变缓存内容."""
    a = dc.get_price("X.XSHG", "2024-01-01", "2024-02-29", fq="pre",
                     pre_factor_ref_date="2024-01-31")
    b = dc.get_price("X.XSHG", "2024-01-01", "2024-02-29", fq="pre",
                     pre_factor_ref_date="2024-02-29")
    assert a["close"].iloc[0] != pytest.approx(b["close"].iloc[0])
    # 底层 post 缓存唯一
    post1 = dc.get_price("X.XSHG", "2024-01-01", "2024-02-29", fq="post")
    post2 = dc.get_price("X.XSHG", "2024-01-05", "2024-02-20", fq="post")
    common = post1.index.intersection(post2.index)
    pd.testing.assert_series_equal(
        post1.loc[common, "close"], post2.loc[common, "close"]
    )


def test_pit_stability_across_queries(dc: DataCache):
    """同一 (标的, 日期) 的 post 价格不随查询区间变化(旧实现会跳变)."""
    single = dc.get_price("X.XSHG", "2024-01-02", "2024-01-02", fq="post")
    wide = dc.get_price("X.XSHG", "2024-01-01", "2024-02-29", fq="post")
    assert single["close"].iloc[0] == pytest.approx(
        wide["close"].loc["2024-01-02"]
    )


def test_invalid_fq_raises(dc: DataCache):
    with pytest.raises(ValueError, match="fq 只能是"):
        dc.get_price("X.XSHG", "2024-01-01", "2024-01-31", fq="backward")


def test_skip_paused_filters_rows(dc: DataCache):
    df = dc.get_price("X.XSHG", "2024-01-01", "2024-01-31", skip_paused=True)
    assert len(df) > 0  # 假数据 paused 恒为 0


# ======================================================================
# ⑥ 增量: 只补缺失月
# ======================================================================


def test_second_query_hits_cache(dc: DataCache):
    dc.get_price("X.XSHG", "2024-01-01", "2024-01-31")
    n = dc.runner.fetch_count
    dc.get_price("X.XSHG", "2024-01-01", "2024-01-31")
    assert dc.runner.fetch_count == n  # 零新增远程调用


def test_only_missing_months_are_fetched(dc: DataCache):
    dc.get_price("X.XSHG", "2024-01-01", "2024-01-31")
    dc.runner.calls.clear()
    dc.get_price("X.XSHG", "2024-01-01", "2024-02-29")
    assert dc.runner.fetch_count == 1
    # 只请求了 2 月, 未重取 1 月
    assert "start_date='2024-02-01'" in dc.runner.calls[0]


def test_gap_query_does_not_refetch_middle(dc: DataCache):
    """⑥ 缓存 [2024-01], 请求 [2024-05] 时不得连带重取 2~4 月."""
    dc.get_price("X.XSHG", "2024-01-01", "2024-01-31")
    dc.runner.calls.clear()
    dc.get_price("X.XSHG", "2024-05-01", "2024-05-31")
    assert dc.runner.fetch_count == 1
    assert "start_date='2024-05-01'" in dc.runner.calls[0]
    assert dc.bars.missing_months(
        "get_price", "X.XSHG", "2024-02-01", "2024-04-30", "daily__post"
    ) == ["2024-02", "2024-03", "2024-04"]


def test_empty_range_is_remembered(dc: DataCache):
    """⑧ 上市前区间查过一次后不再远程."""
    dc = DataCache(cache_dir=dc.cache_dir, runner=FakeRunner(empty=True))
    dc.get_price("X.XSHG", "1999-01-01", "1999-03-31")
    n = dc.runner.fetch_count
    assert n > 0
    out = dc.get_price("X.XSHG", "1999-01-01", "1999-03-31")
    assert dc.runner.fetch_count == n
    assert len(out) == 0


def test_reversed_range_returns_empty(dc: DataCache):
    assert len(dc.get_price("X.XSHG", "2024-03-01", "2024-01-01")) == 0
    assert dc.runner.fetch_count == 0


def test_accepts_non_string_dates(dc: DataCache):
    """④ Timestamp / date 入参不再算错缺口."""
    dc.get_price("X.XSHG", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31"))
    n = dc.runner.fetch_count
    dc.get_price("X.XSHG", "2024-1-1", "2024-01-31")   # 非零填充
    assert dc.runner.fetch_count == n


# ======================================================================
# ⑦ 批量
# ======================================================================


def test_batch_returns_all_symbols(dc: DataCache):
    out = dc.get_price_batch(
        ["600519.XSHG", "000001.XSHE"], "2024-01-01", "2024-01-31"
    )
    assert set(out) == {"600519.XSHG", "000001.XSHE"}
    assert all(len(v) > 0 for v in out.values())


def test_batch_uses_single_remote_call(dc: DataCache):
    dc.get_price_batch(
        ["600519.XSHG", "000001.XSHE", "300750.XSHE"], "2024-01-01", "2024-01-31"
    )
    assert dc.runner.fetch_count == 1  # 3 只共用一次调用


def test_batch_and_single_share_cache(dc: DataCache):
    """⑦ 批量取过的标的, 单只查询直接命中(反之亦然)."""
    dc.get_price_batch(["600519.XSHG", "000001.XSHE"], "2024-01-01", "2024-01-31")
    n = dc.runner.fetch_count
    df = dc.get_price("600519.XSHG", "2024-01-01", "2024-01-31")
    assert dc.runner.fetch_count == n
    assert len(df) > 0


def test_single_then_batch_only_fetches_new_symbol(dc: DataCache):
    dc.get_price("600519.XSHG", "2024-01-01", "2024-01-31")
    dc.runner.calls.clear()
    dc.get_price_batch(["600519.XSHG", "000001.XSHE"], "2024-01-01", "2024-01-31")
    assert dc.runner.fetch_count == 1
    assert "600519" not in dc.runner.calls[0].split("get_price([")[1][:60]


def test_batch_applies_fq(dc: DataCache):
    out = dc.get_price_batch(["600519.XSHG"], "2024-01-01", "2024-01-31", fq=None)
    assert out["600519.XSHG"]["close"].iloc[0] == pytest.approx(10.0)


def test_batch_dedupes_input(dc: DataCache):
    out = dc.get_price_batch(
        ["600519.XSHG", "600519.XSHG"], "2024-01-01", "2024-01-31"
    )
    assert len(out) == 1


# ======================================================================
# ⑫⑬ 其他
# ======================================================================


def test_current_month_not_persisted(dc: DataCache):
    lo, hi = month_bounds(current_month())
    dc.get_price("X.XSHG", lo, hi)
    assert not dc.bars.shard_path(
        "get_price", "X.XSHG", current_month(), "daily__post"
    ).exists()


def test_current_month_data_is_still_returned(dc: DataCache):
    """当月数据不落盘, 但**必须仍然返回** —— 否则所有取最新行情的查询都会丢数据."""
    lo, hi = month_bounds(current_month())
    df = dc.get_price("X.XSHG", lo, hi)
    assert len(df) > 0
    # 重复查询仍能拿到(每次重取, 不落盘)
    again = dc.get_price("X.XSHG", lo, hi)
    assert len(again) == len(df)


def test_range_spanning_into_current_month(dc: DataCache):
    """跳越历史月与当月的区间: 历史部分读缓存, 当月部分从内存带出."""
    cur = pd.Period(current_month(), freq="M")
    prev_lo, _ = month_bounds(str(cur - 1))
    _, cur_hi = month_bounds(str(cur))
    df = dc.get_price("X.XSHG", prev_lo, cur_hi)
    months = {d.strftime("%Y-%m") for d in df.index}
    assert str(cur - 1) in months
    assert str(cur) in months


def test_batch_returns_current_month_data(dc: DataCache):
    """批量路径同样不能丢当月数据."""
    lo, hi = month_bounds(current_month())
    out = dc.get_price_batch(["600519.XSHG", "000001.XSHE"], lo, hi)
    assert all(len(v) > 0 for v in out.values())


def test_unknown_field_raises(dc: DataCache):
    with pytest.raises(ValueError, match="请求的字段不存在"):
        dc.get_price(
            "X.XSHG", "2024-01-01", "2024-01-31", fields=["close", "nope"]
        )


def test_field_subset_is_returned(dc: DataCache):
    df = dc.get_price(
        "X.XSHG", "2024-01-01", "2024-01-31", fields=["close", "volume"]
    )
    assert list(df.columns) == ["close", "volume"]


def test_runner_is_lazy(tmp_path: Path):
    """纯缓存命中不应建立远程连接(runner 懒创建)."""
    dc = DataCache(cache_dir=tmp_path, runner=FakeRunner())
    dc.get_price("X.XSHG", "2024-01-01", "2024-01-31")
    fresh = DataCache(cache_dir=tmp_path)  # 不传 runner
    assert fresh._runner is None
    df = fresh.get_price("X.XSHG", "2024-01-01", "2024-01-31")
    assert len(df) > 0
    assert fresh._runner is None  # 全命中 → 从未创建 runner


def test_status_and_clear(dc: DataCache):
    dc.get_price("X.XSHG", "2024-01-01", "2024-02-29")
    st = dc.status()
    assert st["total_files"] > 0
    assert "get_price" in st["bars"]
    removed = dc.clear()
    assert removed > 0
    assert dc.status()["total_files"] == 0


def test_snapshot_cache_reused(dc: DataCache):
    dc.get_concepts()
    n = dc.runner.fetch_count
    dc.get_concepts()
    assert dc.runner.fetch_count == n


def test_snapshot_today_not_persisted(dc: DataCache):
    today = str(pd.Timestamp.today().date())
    dc.get_all_securities(today, types=["stock"])
    n = dc.runner.fetch_count
    dc.get_all_securities(today, types=["stock"])
    assert dc.runner.fetch_count > n  # 当天数据不缓存, 每次重取


def test_snapshot_historical_date_persisted(dc: DataCache):
    """历史时点的名录是确定事实, 永久缓存."""
    dc.get_all_securities("2024-01-05", types=["stock"])
    n = dc.runner.fetch_count
    dc.get_all_securities("2024-01-05", types=["stock"])
    assert dc.runner.fetch_count == n


# ======================================================================
# 时点必填(禁用 date=None) 与 asof 分片
# ======================================================================


@pytest.mark.parametrize(
    "call",
    [
        lambda d: d.get_all_securities(),
        lambda d: d.get_security_info("600519.XSHG"),
        lambda d: d.get_industries(),
        lambda d: d.get_index_stocks("000300.XSHG"),
        lambda d: d.get_index_weights("000300.XSHG"),
        lambda d: d.get_billboard_list(),
        lambda d: d.get_industry_stocks("HY001"),
        lambda d: d.get_industry("600519.XSHG"),
        lambda d: d.get_concept("600519.XSHG"),
        lambda d: d.get_trade_days(),
        lambda d: d.get_factor_values(["600519.XSHG"], ["pe_ratio"]),
        lambda d: d.run_cached("f", "k", "__result__ = 1"),
    ],
)
def test_time_point_is_mandatory(dc: DataCache, call):
    """所有快照类接口都必须显式传时点 —— 缺省就是未来函数的温床."""
    with pytest.raises(TypeError):
        call(dc)


def test_run_cached_requires_today_key(dc: DataCache):
    """run_cached 是任意查询的逃生舱, today_key 不得缺省."""
    import inspect

    sig = inspect.signature(DataCache.run_cached)
    assert sig.parameters["today_key"].default is inspect.Parameter.empty
    # 显式给历史时点 → 正常缓存
    dc.run_cached("f", "k", "__result__ = 1", "2024-01-05")
    n = dc.runner.fetch_count
    dc.run_cached("f", "k", "__result__ = 1", "2024-01-05")
    assert dc.runner.fetch_count == n


def test_asof_snapshot_partitioned_by_fetch_day(dc: DataCache):
    """无 date 参数的接口按取数日分片: 当天内命中, 且 key 里带日期."""
    dc.get_concepts()
    today = str(pd.Timestamp.today().date())
    paths = list((dc.cache_dir / "v2" / "snapshot" / "get_concepts").glob("*.pkl"))
    assert len(paths) == 1
    assert today in paths[0].name


def test_asof_snapshot_refreshes_next_day(dc: DataCache, monkeypatch):
    """跨天后 key 变化 → 自动重取, 且昨天的快照保留(形成时点序列)."""
    import jq.cache as cache_mod

    dc.get_concepts()
    n = dc.runner.fetch_count

    real_date = cache_mod.date

    class FakeDate(real_date):
        @classmethod
        def today(cls):
            return real_date.today() + dt.timedelta(days=1)

    monkeypatch.setattr(cache_mod, "date", FakeDate)
    dc.get_concepts()
    assert dc.runner.fetch_count > n  # 跨天重取

    paths = list((dc.cache_dir / "v2" / "snapshot" / "get_concepts").glob("*.pkl"))
    assert len(paths) == 2  # 两天的快照都在


# ======================================================================
# ① count 参数 / ② 超时传递
# ======================================================================


def test_count_resolves_via_trade_calendar(dc: DataCache):
    """count 用交易日历在本地换算出起始日."""
    df = dc.get_price("X.XSHG", count=10, end_date="2024-02-29")
    assert len(df) > 0
    # 请求的区间不应超过 end_date
    assert df.index.max() <= pd.Timestamp("2024-02-29")


def test_count_conflicts_with_start_date(dc: DataCache):
    with pytest.raises(ValueError, match="不能同时传入"):
        dc.get_price("X.XSHG", "2024-01-01", "2024-02-29", count=10)


def test_count_must_be_positive(dc: DataCache):
    with pytest.raises(ValueError, match="正整数"):
        dc.get_price("X.XSHG", count=0, end_date="2024-02-29")


def test_missing_range_raises(dc: DataCache):
    with pytest.raises(ValueError, match="end_date 必填"):
        dc.get_price("X.XSHG")


def test_end_date_mandatory_even_with_count(dc: DataCache):
    """“取最近 N 天”也必须显式给终点 —— 浮动终点是未来函数的入口."""
    with pytest.raises(ValueError, match="end_date 必填"):
        dc.get_price("X.XSHG", count=5)


def test_batch_end_date_mandatory(dc: DataCache):
    with pytest.raises(ValueError, match="end_date 必填"):
        dc.get_price_batch(["600519.XSHG"], count=5)


def test_only_start_date_raises(dc: DataCache):
    with pytest.raises(ValueError, match="end_date 必填"):
        dc.get_price("X.XSHG", "2024-01-01")


def test_batch_supports_count(dc: DataCache):
    out = dc.get_price_batch(["600519.XSHG"], count=10, end_date="2024-02-29")
    assert len(out["600519.XSHG"]) > 0


def test_exec_timeout_is_passed_through(tmp_path: Path):
    """① 不再硬编码 60s —— 取数类查询需要更长超时."""
    dc = DataCache(cache_dir=tmp_path, runner=FakeRunner(), exec_timeout=123.0)
    dc.get_price("X.XSHG", "2024-01-01", "2024-01-31")
    assert dc.runner.timeouts[-1] == 123.0


def test_batch_timeout_scales_with_workload(tmp_path: Path):
    """批量超时以 exec_timeout 为下限, 按 (标的数 × 月数) 递增."""
    dc = DataCache(cache_dir=tmp_path, runner=FakeRunner(), exec_timeout=60.0)
    # 小工作量: 取下限
    assert dc._batch_timeout(5, "2024-01-01", "2024-01-31") == 60.0
    # 大工作量: 超过下限且随规模递增
    big = dc._batch_timeout(50, "2024-01-01", "2024-12-31")
    assert big > 60.0
    assert big > dc._batch_timeout(50, "2024-01-01", "2024-06-30")
    assert big > dc._batch_timeout(10, "2024-01-01", "2024-12-31")


def test_batch_passes_timeout_to_runner(tmp_path: Path):
    """批量取数实际把自适应超时传给了 runner."""
    dc = DataCache(cache_dir=tmp_path, runner=FakeRunner(), exec_timeout=30.0)
    dc.get_price_batch(
        [f"{i:06d}.XSHE" for i in range(40)], "2024-01-01", "2024-03-31"
    )
    assert dc.runner.timeouts[-1] > 30.0


# ---- get_fundamentals ----------------------------------------------------


class _FundRunner:
    """返回财务长表的假 runner, 并模拟聚宽列名为 sqlalchemy quoted_name."""

    def __init__(self):
        self.calls: list[str] = []

    def run_code(self, code: str, *, timeout: float = 60.0) -> dict:
        self.calls.append(code)
        # 从远程代码里解析出请求字段(_cols = [...])
        m = re.search(r"_cols = (\[[^\]]*\])", code)
        cols = eval(m.group(1)) if m else ["code", "pub_date", "end_date"]  # noqa: S307
        df = pd.DataFrame(
            {c: (["600519.XSHG"] if c == "code"
                 else pd.to_datetime(["2024-04-27"]) if c in ("pub_date", "end_date")
                 else [1.0]) for c in cols}
        )
        return {"error": None, "stdout": _encode(df)}


def test_get_fundamentals_basic(tmp_path: Path):
    dc = DataCache(cache_dir=tmp_path, runner=_FundRunner())
    df = dc.get_fundamentals(
        "STK_INCOME_STATEMENT", ["600519.XSHG"],
        ["total_operating_revenue", "net_profit"],
        "2024-06-30", start_date="2024-01-01", report_type=0,
    )
    assert len(df) == 1
    # 必含对齐键
    assert {"code", "pub_date", "end_date"}.issubset(df.columns)
    # 列名是纯 str(净化后), 不是 sqlalchemy 对象
    assert all(type(c) is str for c in df.columns)
    # 远程代码按 pub_date 过滤 + report_type=0
    code = dc.runner.calls[0]
    assert "_t.pub_date <= '2024-06-30'" in code
    assert "_t.pub_date >= '2024-01-01'" in code
    assert "_t.report_type == 0" in code


def test_get_fundamentals_cache_hit(tmp_path: Path):
    dc = DataCache(cache_dir=tmp_path, runner=_FundRunner())
    args = ("STK_FIN_FORCAST", ["600519.XSHG"], ["type", "profit_min"], "2024-06-30")
    dc.get_fundamentals(*args)
    n = len(dc.runner.calls)
    dc.get_fundamentals(*args)  # 二次应命中缓存
    assert len(dc.runner.calls) == n


def test_get_fundamentals_no_report_type_filter(tmp_path: Path):
    """不传 report_type 时远程代码不应有该 filter(预告表无此列)."""
    dc = DataCache(cache_dir=tmp_path, runner=_FundRunner())
    dc.get_fundamentals("STK_FIN_FORCAST", ["600519.XSHG"], ["type"], "2024-06-30")
    assert "report_type" not in dc.runner.calls[0]


# ---- get_index_valuation ------------------------------------------------


def test_get_index_valuation_returns_day_index(dc: DataCache):
    df = dc.get_index_valuation("000001.XSHG", "2024-01-01", "2024-01-31")
    assert isinstance(df.index, pd.DatetimeIndex)
    assert "turnover_ratio" in df.columns
    assert df["turnover_ratio"].iloc[0] == pytest.approx(1.2)


def test_get_index_valuation_cache_hit(dc: DataCache):
    args = ("000001.XSHG", "2024-01-01", "2024-01-31")
    dc.get_index_valuation(*args)
    n = dc.runner.fetch_count
    dc.get_index_valuation(*args)
    assert dc.runner.fetch_count == n


def test_get_index_valuation_fields_slice(dc: DataCache):
    df = dc.get_index_valuation(
        "000001.XSHG", "2024-01-01", "2024-01-31",
        fields=["turnover_ratio", "pe_ratio"],
    )
    assert list(df.columns) == ["turnover_ratio", "pe_ratio"]


def test_get_index_valuation_rejects_list(dc: DataCache):
    with pytest.raises(TypeError, match="只接单只"):
        dc.get_index_valuation(
            ["000001.XSHG", "399001.XSHE"], "2024-01-01", "2024-01-31"
        )


def test_get_index_valuation_missing_field_raises(dc: DataCache):
    with pytest.raises(ValueError, match="请求的字段不存在"):
        dc.get_index_valuation(
            "000001.XSHG", "2024-01-01", "2024-01-31",
            fields=["pcf_ratio2"],
        )


# ---- get_valuation_batch ------------------------------------------------


class _ValBatchRunner:
    """模拟 get_fundamentals_continuously 批量返回(多 code 多日, 含重复列)."""

    def __init__(self):
        self.calls: list[str] = []

    def run_code(self, code: str, *, timeout: float = 60.0) -> dict:
        self.calls.append(code)
        if "get_all_trade_days" in code or "get_trade_days" in code:
            days = pd.bdate_range("2024-06-03", "2024-06-14")
            return {"error": None, "stdout": _encode([d.date() for d in days])}
        codes = re.findall(r"\d{6}\.XSH[EG]", code)
        codes = list(dict.fromkeys(codes))
        days = pd.bdate_range("2024-06-03", "2024-06-14")
        rows = []
        for c in codes:
            for d in days:
                rows.append({"day": d, "code": c,
                             "capitalization": 1e5, "circulating_cap": 8e4})
        return {"error": None, "stdout": _encode(pd.DataFrame(rows))}


def test_get_valuation_batch_one_rpc_many_codes(tmp_path: Path):
    """批量: 多 code 一次 RPC(而非逐只), 返回长表含 code/day."""
    dc = DataCache(cache_dir=tmp_path, runner=_ValBatchRunner())
    df = dc.get_valuation_batch(
        ["600519.XSHG", "000001.XSHE", "601318.XSHG"],
        "2024-06-03", "2024-06-14",
        fields=["capitalization", "circulating_cap"],
    )
    assert {"code", "day", "capitalization", "circulating_cap"}.issubset(df.columns)
    assert set(df["code"]) == {"600519.XSHG", "000001.XSHE", "601318.XSHG"}
    # 取数 RPC(排除 trade_days) 应只有 1 次批量, 而非 3 次逐只
    fetch_calls = [c for c in dc.runner.calls if "get_fundamentals_continuously" in c]
    assert len(fetch_calls) == 1, f"应 1 次批量 RPC, 实际 {len(fetch_calls)}"


def test_get_valuation_batch_cache_hit(tmp_path: Path):
    dc = DataCache(cache_dir=tmp_path, runner=_ValBatchRunner())
    args = (["600519.XSHG"], "2024-06-03", "2024-06-14")
    dc.get_valuation_batch(*args, fields=["capitalization"])
    n = len(dc.runner.calls)
    dc.get_valuation_batch(*args, fields=["capitalization"])
    assert len(dc.runner.calls) == n  # 二次命中缓存


# ---- 分块按工作量(标的数 × 月数) ----------------------------------------


def test_batch_chunks_by_workload_not_just_symbol_count(tmp_path: Path):
    """㉝ 长区间必须按工作量细分, 不能只看标的数.

    背景: chunk_size=50 只限标的数, 40只×24月(≈万行)会作为单次请求发出,
    聚宽 kernel 静默返回空 stdout(无错误)。修复后按 max_symbol_months 再切。
    """
    dc = DataCache(cache_dir=tmp_path, runner=FakeRunner())
    codes = [f"{i:06d}.XSHE" for i in range(40)]
    dc.get_price_batch(
        codes, "2023-01-01", "2024-12-31", max_symbol_months=400
    )
    price_calls = [c for c in dc.runner.calls if "get_price([" in c]
    # 40只×24月=960 工作量 > 400 → 必须切多块
    assert len(price_calls) >= 2, (
        f"40只×24月 应按工作量切块, 实际只发了 {len(price_calls)} 次"
    )
    # 每块的标的数应受工作量约束(400//24 ≈ 16 只)
    for c in price_calls:
        n = len(re.findall(r"\d{6}\.XSHE", c.split("get_price([")[1].split("]")[0]))
        assert n <= 17, f"单块标的数 {n} 超出工作量上限"


def test_batch_short_range_still_uses_chunk_size(tmp_path: Path):
    """短区间不应被工作量上限过度切分(1个月时仍可 50 只一批)."""
    dc = DataCache(cache_dir=tmp_path, runner=FakeRunner())
    codes = [f"{i:06d}.XSHE" for i in range(40)]
    dc.get_price_batch(codes, "2024-01-01", "2024-01-31", max_symbol_months=400)
    price_calls = [c for c in dc.runner.calls if "get_price([" in c]
    assert len(price_calls) == 1, "40只×1月(工作量40) 应一次搞定"


def test_empty_stdout_error_message_points_to_data_volume():
    """㉝ 空 stdout 的报错须指向"数据量过大", 而非误导为"忘赋值 __result__"."""
    from jq.exceptions import JQExecutionError
    from jq.serialize import decode_result

    with pytest.raises(JQExecutionError, match="数据量过大|空输出") as ei:
        decode_result({"error": None, "stdout": ""})
    msg = str(ei.value)
    assert "__result__" not in msg.split("常见原因")[0], "空输出不应归因为忘赋值"
    assert "max_symbol_months" in msg, "应给出可操作的出路"


def test_valuation_batch_splits_by_row_limit(tmp_path: Path):
    """㉞ 聚宽 continuously 有 10000 行硬上限, 超出静默截断 —— 必须分块.

    100只×242日 理论 24200 行, 实测只返 10000 行(不报错)。
    修复后按 max_rows 分块, 每块标的数 = max_rows // 交易日数。
    """
    dc = DataCache(cache_dir=tmp_path, runner=_ValBatchRunner())
    codes = [f"{i:06d}.XSHE" for i in range(40)]
    # FakeRunner 的交易日是 2024-06-03~06-14(10 个工作日)
    # max_rows=100 → 每块 10 只 → 40 只应切 4 块
    dc.get_valuation_batch(
        codes, "2024-06-03", "2024-06-14",
        fields=["capitalization"], max_rows=100,
    )
    calls = [c for c in dc.runner.calls if "get_fundamentals_continuously" in c]
    assert len(calls) >= 4, f"应按行数上限切 ≥4 块, 实际 {len(calls)}"
    for c in calls:
        n = len(re.findall(r"\d{6}\.XSHE", c.split("_codes = ")[1].split("]")[0]))
        assert n <= 10, f"单块 {n} 只超出 max_rows//count 上限"


def test_valuation_batch_raises_on_row_limit_hit(tmp_path: Path):
    """真截断(理论行数 > 上限但只返上限)时报错, 不静默接受."""
    from jq.exceptions import JQExecutionError

    class _Truncating:
        """模拟聚宽截断: 请求 30 只×10日(理论 300 行), 只返 上限 100 行."""

        def __init__(self):
            self.calls: list[str] = []

        def run_code(self, code: str, *, timeout: float = 60.0) -> dict:
            self.calls.append(code)
            if "get_trade_days" in code or "get_all_trade_days" in code:
                days = pd.bdate_range("2024-06-03", "2024-06-14")
                return {"error": None, "stdout": _encode([d.date() for d in days])}
            # 正好返回 100 行(= max_rows), 但含 30 个 code → 理论 30×10=300 > 100
            rows = [
                {"day": pd.Timestamp("2024-06-03"),
                 "code": f"{i % 30:06d}.XSHE", "capitalization": 1e5}
                for i in range(100)
            ]
            return {"error": None, "stdout": _encode(pd.DataFrame(rows))}

    dc = DataCache(cache_dir=tmp_path, runner=_Truncating())
    # 只传 10 只(不触发分块: 10×10=100 ≤ max_rows), 但 fake 返回的数据里有 30 个 code
    # 且行数恰为 100 → 理论 300 > 100 → 应识别为截断
    with pytest.raises(JQExecutionError, match="静默截断|行数上限"):
        dc.get_valuation_batch(
            [f"{i:06d}.XSHE" for i in range(10)],
            "2024-06-03", "2024-06-14",
            fields=["capitalization"], max_rows=100,
        )


def test_fundamentals_pages_past_run_query_limit(tmp_path: Path):
    """㊱ finance.run_query 有 5000 行硬上限(静默截断), 必须 offset 分页.

    实测: 即使 limit(20000) 也只返 5000 行且不报错。
    """
    class _Paged:
        """模拟 5000 行上限: 第1页满5000, 第2页1000, 第3页空."""

        def __init__(self):
            self.calls: list[str] = []

        def run_code(self, code: str, *, timeout: float = 60.0) -> dict:
            self.calls.append(code)
            # 远程代码里应含 offset 分页循环
            assert "offset(_off)" in code, "财务查询必须用 offset 分页"
            assert "_LIM = 5000" in code, "应按 5000 行上限分页"
            rows = [
                {"code": "600519.XSHG", "pub_date": pd.Timestamp("2024-04-27"),
                 "end_date": pd.Timestamp("2024-03-31"), "net_profit": 1e9}
            ] * 6000   # 模拟分页后合并的总量
            return {"error": None, "stdout": _encode(pd.DataFrame(rows))}

    dc = DataCache(cache_dir=tmp_path, runner=_Paged())
    df = dc.get_fundamentals(
        "STK_INCOME_STATEMENT", ["600519.XSHG"], ["net_profit"],
        "2024-06-30", report_type=0,
    )
    assert len(df) == 6000, "分页合并后应超过单页上限"


def test_call_auction_rejects_list_to_avoid_silent_truncation(tmp_path: Path):
    """㉟ get_call_auction 传 list 必须 fail-loud, 不能静默截断.

    聚宽允许传 list, 但多标的时有 10000 行硬上限且静默截断
    (实测 50只×242日 理论 12100 行 → 只返 10000 行)。
    security 是直接拼进远程代码的, 类型标注 str 并不强制, 故需运行时拦截。
    """
    dc = DataCache(cache_dir=tmp_path, runner=FakeRunner())
    with pytest.raises(TypeError, match="只接单标的|静默截断"):
        dc.get_call_auction(
            ["600519.XSHG", "000001.XSHE"], "2024-06-03", "2024-06-14"
        )
    # 单标的正常工作
    df = dc.get_call_auction("600519.XSHG", "2024-06-03", "2024-06-14")
    assert isinstance(df, pd.DataFrame)


def test_row_limits_registered_as_constants():
    """两个聚宽隐式上限必须是显式常量(而非散落的魔法数字)."""
    from jq.cache import _CONTINUOUSLY_ROW_LIMIT, _RUN_QUERY_ROW_LIMIT

    assert _CONTINUOUSLY_ROW_LIMIT == 10000
    assert _RUN_QUERY_ROW_LIMIT == 5000
