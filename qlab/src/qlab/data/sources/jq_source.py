"""JQDataSource — 聚宽 (JoinQuant) 数据源适配器.

职责边界（纯翻译器）：

- 符号映射：``600519.SH`` ⇄ ``600519.XSHG``
- 字段映射：聚宽列名 → qlab schema 列名
- schema 组装：拼出符合 §3 schema 的 DataFrame

**不做**：网络、鉴权、增量、落盘 —— 全部由 :class:`jq.cache.DataCache` 负责；
qlab 侧的分片缓存由 :class:`~qlab.data.layer.DataLayer` 的 ShardedBarStore 负责。
两层缓存各管各的：jq 缓存原始聚宽响应（省远程请求），qlab 缓存 schema 化结果。

复权口径（设计原则 P2：复权是视图）：本源同时提供 ``*_raw`` 不复权价、
``adj_factor`` 与**后复权**价列（DailyBar 不变量要求存储态的 ``close`` 为后复权）。
其他口径用 :func:`~qlab.data.adjust.apply_adjust` 在读取后切换，**不要**靠
``DataLayer.daily(adjust=...)``（那会把相同数据按 adjust 多存几份）。得益于 jq
缓存恒存后复权并在本地推导其他口径，取两个口径**只需一次远程请求**；后复权列
直接用聚宽原值，比 ``raw × factor`` 少一次除法噪声。
"""

from __future__ import annotations

import warnings
from datetime import date
from datetime import time as _time
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from qlab.core.calendar import Calendar, get_default_calendar
from qlab.core.enums import AdjustMode, Freq, ReportType
from qlab.core.exceptions import DataUnavailableError

if TYPE_CHECKING:  # pragma: no cover
    from jq.cache import DataCache

#: 从 :mod:`qlab.core.exceptions` 重导出 —— 早期版本定义在本模块里,
#: 保留别名以免破坏已有 import。
__all__ = ["DataUnavailableError", "JQCalendar", "JQDataSource", "to_jq_code", "to_qlab_symbol"]


#: 查上市日时覆盖的证券类型(不只股票 —— 用户可能取 ETF/基金行情)
_SECURITY_TYPES = ["stock", "etf", "fund", "index"]

#: 股本取数向前回看的天数。区间起点恰好缺数据时只能从**过去**补(ffill),
#: 故额外向前多取一段作参照; 绝不用 bfill 从未来借(送转股会让股本翻倍)。
_SHARES_LOOKBACK_DAYS = 45

#: 能拿到 ``factor`` 列的频率。
#:
#: 聚宽对**聚合分钟频率**(5m/15m/30m/60m/120m)硬约束 fields 只能取
#: ``['open','close','high','low','volume','money','open_interest']``, 不含 factor
#: —— 那些频率的 ``adj_factor`` 需从日线补(见 :meth:`JQDataSource._daily_factors`)。
_FACTOR_CAPABLE_FREQS = frozenset({Freq.DAILY, Freq.MIN_1})

# ---------------------------------------------------------------------------
# 符号映射
# ---------------------------------------------------------------------------

_EX_JQ_TO_QLAB = {"XSHG": "SH", "XSHE": "SZ"}
_EX_QLAB_TO_JQ = {"SH": "XSHG", "SZ": "XSHE"}


def to_jq_code(symbol: str) -> str:
    """``600519.SH`` → ``600519.XSHG``. 已是聚宽格式则原样返回.

    Raises:
        ValueError: 无法识别的后缀。``.BJ``(北交所)会给出专门说明 ——
            聚宽的股票池里没有北交所标的(实测 5115 只全是 XSHG/XSHE),
            不是后缀映射缺失而是数据源本身不覆盖。
    """
    code, _, ex = symbol.partition(".")
    if ex in _EX_JQ_TO_QLAB:  # 已是聚宽后缀
        return symbol
    if ex.upper() in {"CSI", "SI"}:  # 中证 / 申万行业指数
        return f"{code}.{ex.upper()}"
    if ex.upper() == "BJ":
        raise ValueError(
            f"不支持北交所标的: {symbol!r}。\n"
            "  原因: 聚宽 get_all_securities(types=['stock']) 返回的全部标的"
            "只有 .XSHG/.XSHE 两种后缀, 不包含北交所 —— 本数据源无此数据。\n"
            "  出路: 从 universe 中剔除 .BJ 标的, 或换用覆盖北交所的数据源。"
        )
    try:
        return f"{code}.{_EX_QLAB_TO_JQ[ex.upper()]}"
    except KeyError:
        raise ValueError(
            f"无法识别的交易所后缀: {symbol!r}"
            "（支持 .SH/.SZ/.XSHG/.XSHE，以及指数 .CSI/.SI）"
        ) from None


def to_qlab_symbol(code: str) -> str:
    """``600519.XSHG`` → ``600519.SH``. 已是 qlab 格式则原样返回."""
    num, _, ex = code.partition(".")
    if ex in _EX_QLAB_TO_JQ:  # 已是 qlab 后缀
        return code
    if ex.upper() in {"CSI", "SI"}:
        return f"{num}.{ex.upper()}"
    try:
        return f"{num}.{_EX_JQ_TO_QLAB[ex.upper()]}"
    except KeyError:
        raise ValueError(
            f"无法识别的聚宽后缀: {code!r}（支持 .XSHG/.XSHE/.CSI/.SI）"
        ) from None


def _is_stock_symbol(symbol: str) -> bool:
    """判断是否**本数据源支持的 A 股个股**(非 ETF/基金/指数).

    支持的个股代码段:
    - 沪市: ``60``(主板) / ``68``(科创板), 后缀 .SH
    - 深市: ``00``(主板) / ``30``(创业板), 后缀 .SZ

    不包含北交所(``43``/``83``/``87``/``92``, ``.BJ``): 实测聚宽
    ``get_all_securities(types=['stock'])`` 返回的 5115 只全部是 ``.XSHG``/
    ``.XSHE``, 无任何北交所标的 —— 本源根本拿不到北交所数据,
    声称支持只会让错误推迟到更深的代码路径。

    ETF/基金(51/15/56/58/50/16/18…)、指数(000xxx.SH / 399xxx.SZ)均不是个股。
    注: 深市指数 399xxx 与个股 00xxxx 段不重; 沪市指数 000xxx.SH 与深市个股
    000xxx.SZ 同号但交易所不同, 故需带后缀判断。
    """
    num, _, ex = symbol.partition(".")
    if len(num) != 6:
        return False
    if ex == "SH":
        return num[:2] in ("60", "68")
    if ex == "SZ":
        return num[:2] in ("00", "30")
    # .BJ 不列为个股: 本源无北交所数据(见 docstring)
    return False


#: :class:`~qlab.data.universe.UniverseSpec` 工厂方法生成的别名 → 聚宽指数代码。
#:
#: ``UniverseSpec.csi300()`` 得到的 name 是 ``'csi300'`` 而非 ``'000300.SH'``,
#: 不做映射会被当成指数代码去解析, 报“无法识别的交易所后缀”。
_UNIVERSE_ALIASES = {
    "csi300": "000300.SH",
    "csi500": "000905.SH",
    "csi800": "000906.SH",
    "csi1000": "000852.SH",
}


def _resolve_universe_spec(spec: str) -> str:
    """把 universe spec 归一到 ``'all'`` / ``'main_a'`` / ``'hs_a'`` 或指数代码.

    - 指数代码: ``'000300.SH'`` → 原样
    - 工厂别名: ``'csi300'`` → ``'000300.SH'``
    - 宽基池: ``'main_a'`` / ``'hs_a'`` → 原样（取全市场后再按规则过滤）
    - 调试用裸全市场: ``'all'`` / ``'*'`` → ``'all'``（不做 ST/板块过滤）
    """
    s = str(spec).strip()
    head = s.split(":", 1)[0].lower()
    if head in ("all", "*"):
        return "all"
    if head in ("main_a", "hs_a"):
        return head
    if head in ("all_a", "all_a_raw"):
        raise ValueError(
            f"宇宙规格 {spec!r} 已移除。请改用 UniverseSpec.main_a() "
            f"（剔北交/科创/ST）或 UniverseSpec.hs_a()（剔北交/ST，保留科创）。"
        )
    return _UNIVERSE_ALIASES.get(head, s)


# ---------------------------------------------------------------------------
# 频率映射
# ---------------------------------------------------------------------------

_FREQ_TO_JQ = {
    Freq.DAILY: "daily",
    Freq.MIN_1: "1m",
    Freq.MIN_5: "5m",
    Freq.MIN_15: "15m",
    Freq.MIN_30: "30m",
    Freq.MIN_60: "60m",
}

#: 聚宽列 → qlab 不复权价格列
_RAW_PRICE_MAP = {
    "open": "open_raw",
    "high": "high_raw",
    "low": "low_raw",
    "close": "close_raw",
}

_LIMIT_EPS = 1e-6

# 交易时段分界(分钟线时间戳为 bar 结束时刻)
_T_OPEN_AUCTION_END = pd.Timestamp("09:30").time()
_T_MORNING_CLOSE = pd.Timestamp("11:30").time()
_T_CLOSE_AUCTION_START = pd.Timestamp("14:57").time()


class JQDataSource:
    """聚宽数据源.

    Args:
        cache: :class:`jq.cache.DataCache` 实例. 缺省时自行创建(需已 ``pip install -e ./jq``).

    Note:
        股本列是 DailyBar 的组成部分(schema 声明为 int64, 无法表达缺失),
        因此本源**总是**回填股本. 聚宽估值接口按单只标的查询(实测不支持批量),
        首次取数会每只一次请求; 股本变动罕见且 jq 缓存会兜住重复请求.
    """

    source_version = "jq-v1"

    #: 本源产出的概念 ``source`` 值(聚宽 ``jq_concept`` 体系)。
    #:
    #: 消费端(``concepts_as_of`` / ``DataLayer.concepts`` / ``FeatureContext.concepts``)
    #: 的默认值是 ``eastmoney``, 不传则过滤条件不匹配、**静默返回空**。
    #: 请用本属性而非硬编码字符串::
    #:
    #:     concepts_as_of(df, syms, date, source=JQDataSource.concept_source)
    concept_source = "jq"

    def __init__(self, cache: DataCache | None = None) -> None:
        if cache is None:
            try:
                from jq.cache import DataCache as _DataCache
            except ImportError as e:  # pragma: no cover
                raise ImportError(
                    "JQDataSource 需要 jq 连接器: pip install -e ./jq"
                ) from e
            cache = _DataCache()
        self.cache = cache
        self._calendar: JQCalendar | None = None

    # ==================================================================
    # K 线
    # ==================================================================

    def fetch_bars(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
        freq: Freq = Freq.DAILY,
        adjust: AdjustMode = AdjustMode.BACKWARD,
    ) -> pd.DataFrame:
        """拉取 K 线, 返回 DailyBar / IntradayBar schema.

        同时给出 ``*_raw`` 不复权价、``adj_factor`` 与**后复权**价列
        (open/high/low/close/vwap)。

        Note:
            ``adjust`` 参数不影响返回内容 —— DailyBar 不变量要求
            ``close ≈ close_raw × adj_factor``, 即存储态的 ``close`` 必须是后复权价;
            其他口径是**视图**, 用 :func:`~qlab.data.adjust.apply_adjust` 在读取后
            切换(切换后的结果不再满足该不变量, 不应再用 DailyBar 校验)。
            本行为与 :class:`~qlab.data.sources.fake.FakeDataSource` 一致。

        Warning:
            不要用 ``DataLayer.daily(adjust=...)`` 切换口径。``DataLayer`` 把 ``adjust``
            放进了分片缓存键, 而本源对它不敏感 —— 换一个 ``adjust`` 就会把
            **完全相同的数据**重新拉取并多存一份(实测多花 0.3s 与一倍缓存)。
            正确做法: 用单一 ``adjust`` 取数, 再对结果调 ``apply_adjust``。
        """
        freq = Freq(freq)
        jq_freq = _FREQ_TO_JQ.get(freq)
        if jq_freq is None:  # pragma: no cover
            raise ValueError(f"聚宽不支持的频率: {freq}")

        codes = _unique_jq_codes(symbols)
        s, e = str(pd.Timestamp(start).date()), str(pd.Timestamp(end).date())

        try:
            if freq in _FACTOR_CAPABLE_FREQS:
                # 两次调用共享同一份后复权缓存 —— 远程只取一次, 第二次纯本地换算
                post = self.cache.get_price_batch(
                    codes, s, e, frequency=jq_freq, fq="post"
                )
                raw = self.cache.get_price_batch(
                    codes, s, e, frequency=jq_freq, fq=None
                )
                day_factors: dict[str, pd.Series] = {}
            else:
                # 聚合分钟频率(5m/15m/30m/...)拿不到 factor —— 聚宽只允许 6 个基础字段。
                # 从**日线**补 factor: 同一交易日内所有 bar 共享当日复权因子,
                # 这正好是 IntradayBar 不变量“同一 (date, symbol) 共享 adj_factor”的要求。
                post = self.cache.get_price_batch(
                    codes, s, e, frequency=jq_freq, fq="post"
                )
                raw = {}
                day_factors = self._daily_factors(codes, s, e)
        except Exception as exc:  # noqa: BLE001 - 靠消息识别标的不存在
            msg = str(exc)
            if "找不到标的" in msg or "ParamsError" in msg:
                # 把聚宽原始错误翻译回 qlab 语境: 报出用户传的 symbol 而非聚宽代码
                raise DataUnavailableError(
                    f"取行情失败: 标的不存在或代码有误。\n"
                    f"  请求的 symbols: {list(symbols)[:10]}"
                    f"{'...' if len(symbols) > 10 else ''}\n"
                    f"  换算后的聚宽代码: {codes[:10]}"
                    f"{'...' if len(codes) > 10 else ''}\n"
                    f"  原始错误: {msg.splitlines()[0][:120]}"
                ) from exc
            raise

        frames = []
        for code in codes:
            post_df = post.get(code)
            if post_df is None or len(post_df) == 0:
                continue
            if freq in _FACTOR_CAPABLE_FREQS:
                raw_df = raw.get(code)
                if raw_df is None or len(raw_df) == 0:
                    continue
            else:
                factors = day_factors.get(code)
                if factors is None or factors.empty:
                    raise DataUnavailableError(
                        f"无法从日线取到 {code} 的复权因子, 无法拼出日内 adj_factor。"
                    )
                post_df, raw_df = _split_by_daily_factor(post_df, factors)
                if len(post_df) == 0:
                    continue
            frame = self._build_one(code, raw_df, post_df, freq)
            if frame is not None:
                frames.append(frame)
        if not frames:
            return self._empty_bars(freq)

        bars = pd.concat(frames).sort_index()
        if freq == Freq.DAILY:
            bars = self._attach_daily_only(bars, s, e)
        return bars

    def _daily_factors(
        self, codes: list[str], start: str, end: str
    ) -> dict[str, pd.Series]:
        """取日线的复权因子 ``{code: Series(index=日期, value=factor)}``.

        供聚合分钟频率补 ``adj_factor`` 用 —— 聚宽对那些频率只返回 6 个基础字段。
        日线通常已在缓存里(大多数流程会先取日线), 此时零额外远程开销。
        """
        daily = self.cache.get_price_batch(codes, start, end, frequency="daily", fq="post")
        out = {}
        for code, df in daily.items():
            if isinstance(df, pd.DataFrame) and "factor" in df.columns and len(df):
                s = df["factor"].astype(float)
                s.index = pd.DatetimeIndex(df.index).normalize()
                out[code] = s[~s.index.duplicated(keep="last")]
        return out

    def _build_one(
        self,
        code: str,
        raw: pd.DataFrame,
        post: pd.DataFrame,
        freq: Freq,
    ) -> pd.DataFrame | None:
        """把单只标的的 (raw, post) 两口径拼成 schema 列.

        Returns:
            拼好的 DataFrame; 若该标的在本区间内**整段无数据**则返回 ``None``.
        """
        raw, post = self._drop_nonexistent_rows(raw, post)
        if len(raw) == 0:
            return None

        idx = raw.index
        out = pd.DataFrame(index=idx)

        for src, dst in _RAW_PRICE_MAP.items():
            out[dst] = raw[src].astype("float64")

        # adj_factor 取自后复权口径; raw = post / factor 使
        # close_raw × adj_factor == close_post 在浮点意义上精确成立
        out["adj_factor"] = post["factor"].astype("float64")

        # 后复权价列直接用聚宽 post 原值(而非 raw × factor), 避开一次除法噪声;
        # vwap 用聚宽自己的成交均价 avg(比 amount/volume 估算更准)。
        # 聚合分钟频率无 avg 列, 退回 amount/volume 估算。
        for col in ("open", "high", "low", "close"):
            out[col] = post[col].astype("float64")
        if "avg" in post.columns:
            out["vwap"] = post["avg"].astype("float64")
        else:
            vol = post["volume"].astype("float64").replace(0, np.nan)
            out["vwap"] = post["money"].astype("float64") / vol * post["factor"].astype(
                "float64"
            ) if "factor" in post.columns else post["money"].astype("float64") / vol

        # volume 用不复权口径(真实成交股数).
        # 注: 由后复权整数值反推, 存在 ±factor/2 股的舍入误差(相对误差 ~1e-7).
        # fillna 是防御性的: 停牌日可能无成交量, 而 int64 存不了 NaN.
        out["volume"] = raw["volume"].fillna(0).round().astype("int64")
        out["amount"] = raw["money"].fillna(0.0).astype("float64")

        # 涨跌停判定在后复权口径上做(避开 post/factor 的除法噪声),
        # 参考价则用不复权口径(schema 要求 limit_*_price 为不复权).
        # 聚合分钟频率拿不到涨跌停价/paused, 这些列也不在 IntradayBar schema 里。
        if "high_limit" in raw.columns:
            out["limit_up_price"] = raw["high_limit"].astype("float64")
            out["limit_down_price"] = raw["low_limit"].astype("float64")
            out["is_limit_up"] = (post["close"] - post["high_limit"]).abs() < _LIMIT_EPS
            out["is_limit_down"] = (post["close"] - post["low_limit"]).abs() < _LIMIT_EPS

        if "paused" in raw.columns:
            out["is_suspended"] = raw["paused"].fillna(0).astype(float) > 0

        if freq != Freq.DAILY:
            out["session"] = [_session_of(t) for t in idx]

        out = self._normalize_suspended(out)

        index_name = "date" if freq == Freq.DAILY else "timestamp"
        out.index = pd.MultiIndex.from_arrays(
            [pd.DatetimeIndex(idx), [to_qlab_symbol(code)] * len(idx)],
            names=[index_name, "symbol"],
        )
        return out

    @staticmethod
    def _drop_nonexistent_rows(
        raw: pd.DataFrame, post: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """丢弃"该日此股不存在"的行(退市后 / 上市前).

        聚宽对这类日期返回**全 NaN 行且 paused 也是 NaN**, 语义上与停牌不同:
        停牌是"股票存在但今日不交易"(paused=1), 而这里是"股票当时不在市".
        把后者当停牌处理会凭空造出一堆停牌日; 且 int64 列无法容纳 NaN
        (历史上这里直接抛 IntCastingNaNError), 故直接剔除。
        """
        if not raw.index.equals(post.index):
            raise DataUnavailableError(
                "两个复权口径的日期索引不一致: "
                f"raw {len(raw)} 行 vs post {len(post)} 行。"
                "两者本应来自同一份缓存, 不一致意味着上游数据异常。"
            )
        paused = (
            raw["paused"].fillna(0) if "paused" in raw.columns else 0
        )
        exists = raw["close"].notna() | (paused > 0)
        if exists.all():
            return raw, post
        return raw[exists], post[exists]

    @staticmethod
    def _normalize_suspended(out: pd.DataFrame) -> pd.DataFrame:
        """停牌日归一: 价格置 NaN、量额归零.

        聚宽在停牌日可能返回前收盘价而非 NaN, 而 DailyBar 不变量要求
        "is_suspended → 价格列为 NaN, volume=0, amount=0"。这里**强制**归一,
        不依赖上游行为。
        """
        sus = out["is_suspended"].fillna(False).to_numpy() if "is_suspended" in out.columns else None
        if sus is None or not sus.any():
            return out
        price_cols = [
            c
            for c in out.columns
            if c in ("open", "high", "low", "close", "vwap")
            or c.endswith(("_raw", "_price"))
        ]
        out.loc[sus, price_cols] = np.nan
        out.loc[sus, "volume"] = 0
        out.loc[sus, "amount"] = 0.0
        # 停牌日无成交, 涨跌停标记无意义
        if "is_limit_up" in out.columns:
            out.loc[sus, ["is_limit_up", "is_limit_down"]] = False
        return out

    def _attach_daily_only(
        self, bars: pd.DataFrame, start: str, end: str
    ) -> pd.DataFrame:
        """补齐日频专属列: is_st、days_since_listing、股本.

        这三组列均为 schema 必填项, 且缺失时无法从行情推导 —— 取数失败一律
        **直接报错**, 不做默认值降级。因为降级值会静默污染结论: is_st=False 会把
        ST 股当成普通股(涨跌幅限制 5% vs 10%), 股本=0 会让下游市值/换手率全错,
        而 schema 校验对这些值都是"合法"的, 不会给任何信号。
        """
        symbols = sorted(set(bars.index.get_level_values("symbol")))

        # 方案 A: daily() 仅支持个股。is_st/days_since_listing/股本 都是股票
        # 专属必填字段, ETF/基金/指数天生没有 —— 与其等聚宽报"必须是一个
        # 股票"或伪造 is_st=False/shares=0 污染语义, 不如在入口 fail-loud。
        non_stock = [s for s in symbols if not _is_stock_symbol(s)]
        if non_stock:
            raise DataUnavailableError(
                f"daily() 仅支持个股, 不支持 ETF/基金/指数: "
                f"{non_stock[:10]}{'...' if len(non_stock) > 10 else ''}\n"
                "  原因: DailyBar 的 is_st / days_since_listing / 股本 是个股专属必填字段,"
                "对非股票无意义。\n"
                "  需 ETF/指数行情时请另用轻量价格接口(仅 OHLCV), 而非往个股 schema 里塞。"
            )

        codes = [to_jq_code(s) for s in symbols]
        dates = bars.index.get_level_values("date")

        bars["is_st"] = self._st_series(codes, start, end, bars.index)

        listing = self._listing_dates(codes, start, end)
        missing = [s for s in symbols if s not in listing]
        if missing:
            raise DataUnavailableError(
                f"无法取到上市日(days_since_listing 为 schema 必填项): {missing[:10]}"
                f"{'...' if len(missing) > 10 else ''}\n"
                f"  常见原因: 标的不在聚宽证券名录里(如指数/期货代码), 或 {end} 时已退市。"
            )
        listed_on = pd.to_datetime(
            pd.Series(bars.index.get_level_values("symbol"), index=bars.index).map(listing)
        )
        delta = (pd.Series(dates, index=bars.index) - listed_on).dt.days
        bars["days_since_listing"] = delta.clip(lower=0).astype("int32")

        return self._attach_shares(bars, symbols, start, end)

    def _attach_shares(
        self, bars: pd.DataFrame, symbols: list[str], start: str, end: str
    ) -> pd.DataFrame:
        """回填股本列(int64).

        DailyBar 将股本声明为 int64 —— int64 无法表达 NaN, 故必须给出具体值。
        补缺失值**只能向过去要数据**(ffill), 且为了让区间起点也有往前参照,
        取数时额外向前回看 :data:`_SHARES_LOOKBACK_DAYS` 天。

        Warning:
            **结论是不能用 bfill**。早期实现以"股本几乎恒定"为理由加了 bfill,
            但股本会变的**唯一原因**就是送转股 —— 而 bfill 恰好会把送转后的
            股本填到送转前, 让下游算出"过去价格 × 未来股本"的市值(实测虚高 100%)。
            这是静默的未来函数: schema 全部合法, 不给任何信号。

        Note:
            此处的逐列 ``ffill()`` 是安全的(只用过去值填未来), 与
            :meth:`fetch_universe` 里"不能用 .ffill()"的结论不矛盾:
            那里的 NaN 语义是"不在成分内"(不得填充), 这里的 NaN 是"当日无数据"。
        """
        lookback = pd.Timestamp(start) - pd.Timedelta(days=_SHARES_LOOKBACK_DAYS)
        shares = self.fetch_share_capital(symbols, lookback, pd.Timestamp(end))
        if shares.empty:
            raise DataUnavailableError(
                f"无法取到股本数据(total_shares/float_shares 为 schema 必填项): "
                f"{symbols[:10]}{'...' if len(symbols) > 10 else ''}"
            )
        dates = bars.index.get_level_values("date").unique().sort_values()
        for col in ("total_shares", "float_shares"):
            wide = shares.set_index(["date", "symbol"])[col].unstack("symbol")
            wide = wide.sort_index().ffill()  # 逐标的向后填充(只用过去)
            # reindex(method="ffill") 为每个行情日找最近的**更早**股本快照
            aligned = wide.reindex(dates, method="ffill")
            long = (
                aligned.stack(future_stack=True)
                if _SUPPORTS_FUTURE_STACK
                else aligned.stack()
            )
            long.index.names = ["date", "symbol"]
            src = long.reindex(bars.index)
            if src.isna().any():
                bad = sorted(
                    {
                        s
                        for s, v in zip(
                            src.index.get_level_values("symbol"), src.isna(), strict=True
                        )
                        if v
                    }
                )
                raise DataUnavailableError(
                    f"{col} 在以下标的上缺失(已向前回看 {_SHARES_LOOKBACK_DAYS} 天): "
                    f"{bad[:10]}{'...' if len(bad) > 10 else ''}\n"
                    "  不会用未来值(bfill)补齐 —— 那会把送转股后的股本填到送转前。"
                )
            bars[col] = src.round().astype("int64")
        return bars

    def _st_series(
        self, codes: list[str], start: str, end: str, index: pd.MultiIndex
    ) -> pd.Series:
        """把 get_extras('is_st') 的宽表摊平到 (date, symbol) 索引上.

        取数失败或覆盖不全均**报错**: is_st 默认 False 会把 ST 股当成普通股,
        而两者的涨跌幅限制不同(5% vs 10%), 会直接弄错涨停判定与屏障宽度。
        """
        wide = self.cache.get_extras("is_st", codes, start, end)
        if not isinstance(wide, pd.DataFrame) or wide.empty:
            raise DataUnavailableError(
                f"get_extras('is_st') 未返回数据(is_st 为 schema 必填项): {codes[:10]}"
            )
        long = wide.stack(future_stack=True) if _SUPPORTS_FUTURE_STACK else wide.stack()
        long.index = pd.MultiIndex.from_arrays(
            [
                pd.DatetimeIndex(long.index.get_level_values(0)),
                [to_qlab_symbol(str(c)) for c in long.index.get_level_values(1)],
            ],
            names=["date", "symbol"],
        )
        aligned = long.reindex(index)
        if aligned.isna().any():
            n = int(aligned.isna().sum())
            raise DataUnavailableError(
                f"is_st 覆盖不全: {n}/{len(index)} 个 (日期, 标的) 组合缺值。"
                "不做 False 降级 —— 那会把 ST 股静默当成普通股。"
            )
        return aligned.astype(bool)

    def _listing_dates(
        self, codes: list[str], start: str, end: str
    ) -> dict[str, pd.Timestamp]:
        """{qlab_symbol: 上市日} —— 用于 days_since_listing.

        同时查 ``end`` 与 ``start`` 两个时点的名录: 区间内**退市**的标的在 ``end``
        已不在名录里, 需用 ``start`` 兜住。
        """
        wanted = set(codes)
        out: dict[str, pd.Timestamp] = {}
        for asof in (end, start):
            info = self.cache.get_all_securities(asof, types=_SECURITY_TYPES)
            if not isinstance(info, pd.DataFrame) or "start_date" not in info.columns:
                continue
            for code, row in info.iterrows():
                if str(code) not in wanted:
                    continue
                sym = to_qlab_symbol(str(code))
                out.setdefault(sym, pd.Timestamp(row["start_date"]))
            if len(out) == len(wanted):
                break
        return out

    @staticmethod
    def _empty_bars(freq: Freq) -> pd.DataFrame:
        index_name = "date" if freq == Freq.DAILY else "timestamp"
        return pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [pd.DatetimeIndex([]), []], names=[index_name, "symbol"]
            )
        )

    # ==================================================================
    # 日历
    # ==================================================================

    def fetch_calendar(self, name: str = "SSE") -> Calendar:
        """用聚宽的交易日历. 取数失败时退回默认日历并告警.

        成功构造的日历会**缓存在实例上**: 建一次要把五千多个交易日灌进 set,
        而 ``fetch_universe`` / ``fetch_industry_classification`` 等会反复用到它。
        降级返回的默认日历**不缓存**, 以便下次重试聚宽口径。

        日历是本源唯一允许降级的项: 默认日历(exchange_calendars 的 XSHG)同样是
        真实日历而非猜测值, 且口径差异只会影响极少数调休日; 但仍会发 warning,
        因为日历与行情不同源可能引入"日历说是交易日但无数据"的错位。
        """
        if self._calendar is not None and self._calendar.name == name:
            return self._calendar
        try:
            days = self.cache.get_all_trade_days()
            sessions = pd.DatetimeIndex(
                [pd.Timestamp(d).normalize() for d in days]
            ).sort_values()
        except Exception as e:  # pragma: no cover - 依赖网络
            warnings.warn(
                f"聚宽交易日历取数失败({e!r}), 退回 exchange_calendars 默认日历。"
                "它与行情不同源, 极少数调休日可能错位。",
                RuntimeWarning,
                stacklevel=2,
            )
            return get_default_calendar()
        if len(sessions) == 0:  # pragma: no cover
            warnings.warn(
                "聚宽交易日历为空, 退回 exchange_calendars 默认日历。",
                RuntimeWarning,
                stacklevel=2,
            )
            return get_default_calendar()
        self._calendar = JQCalendar(name=name, sessions=sessions)
        return self._calendar

    # ==================================================================
    # 股本 / 状态覆盖
    # ==================================================================

    def fetch_share_capital(
        self, symbols: list[str], start: pd.Timestamp, end: pd.Timestamp
    ) -> pd.DataFrame:
        """股本变动. 列: date/symbol/total_shares/float_shares.

        聚宽 ``capitalization`` / ``circulating_cap`` 单位为**万股**, 此处换算为股。
        聚宽无自由流通股本, 故不返回 ``free_float_shares`` 列。

        走 jq 的 :meth:`get_valuation_batch` 一次 RPC 拿回全部标的(批量),
        而非逐只请求 —— 300 只股本从 300 次 RPC 降为 1 次。
        本方法只负责字段语义映射(capitalization→total_shares、万股→股、
        code→qlab symbol), 批量/缓存/序列化细节均在 jq 层。
        """
        s, e = str(pd.Timestamp(start).date()), str(pd.Timestamp(end).date())
        codes = _unique_jq_codes(symbols)
        long = self.cache.get_valuation_batch(
            codes, s, e, fields=["capitalization", "circulating_cap"]
        )
        empty = pd.DataFrame(
            columns=["date", "symbol", "total_shares", "float_shares"]
        )
        if not isinstance(long, pd.DataFrame) or len(long) == 0:
            return empty
        out = pd.DataFrame(
            {
                "date": pd.to_datetime(long["day"]),
                "symbol": [to_qlab_symbol(str(c)) for c in long["code"]],
                "total_shares": long["capitalization"].astype(float) * 1e4,
                "float_shares": long["circulating_cap"].astype(float) * 1e4,
            }
        )
        return out.reset_index(drop=True)

    def fetch_index_valuation(
        self, symbol: str, start: pd.Timestamp, end: pd.Timestamp
    ) -> pd.DataFrame:
        """指数/行业市值表. 返回 IndexValuation schema, index=date.

        聚宽 ``turnover_ratio`` 是百分数、市值是亿元；此处换成小数与元。
        成交额 = 换手 × 流通市值，不把个股加总。
        """
        s, e = str(pd.Timestamp(start).date()), str(pd.Timestamp(end).date())
        jq_code = to_jq_code(symbol)
        raw = self.cache.get_index_valuation(jq_code, s, e)
        empty = pd.DataFrame(
            columns=["symbol", "turnover", "circulating_mcap", "amount", "market_cap"]
        )
        empty.index.name = "date"
        if not isinstance(raw, pd.DataFrame) or len(raw) == 0:
            return empty
        idx = pd.DatetimeIndex(raw.index).normalize()
        to = raw["turnover_ratio"].astype("float64") / 100.0
        mcap = raw["circulating_market_cap"].astype("float64") * 1e8
        codes = raw["code"] if "code" in raw.columns else pd.Series(jq_code, index=raw.index)
        out = pd.DataFrame(
            {
                "symbol": [to_qlab_symbol(str(c)) for c in codes],
                "turnover": to.to_numpy(),
                "circulating_mcap": mcap.to_numpy(),
                "amount": (to * mcap).to_numpy(),
            },
            index=idx,
        )
        out.index.name = "date"
        if "market_cap" in raw.columns:
            out["market_cap"] = raw["market_cap"].astype("float64").to_numpy() * 1e8
        return out

    def fetch_status_overrides(
        self, symbols: list[str], start: pd.Timestamp, end: pd.Timestamp
    ) -> pd.DataFrame:
        """状态覆盖 —— 本源的 fetch_bars 已给全状态列, 无需覆盖."""
        return pd.DataFrame()

    # ==================================================================
    # 两融 / 资金流 / 集合竞价 / 龙虎榜 / 因子
    # ==================================================================

    def fetch_margin_trading(
        self, symbols: list[str], start: pd.Timestamp, end: pd.Timestamp
    ) -> pd.DataFrame:
        """两融(融资融券). 返回 MarginTrading schema, index=(date, symbol).

        聚宽 ``get_mtss`` 的金额字段已是元(实测量级 e10), 无需换算。
        数据发布有 T+1~T+2 滞后, 使用方需自行对齐 available_at。
        """
        s, e = str(pd.Timestamp(start).date()), str(pd.Timestamp(end).date())
        frames = []
        for symbol in dict.fromkeys(symbols):
            df = self.cache.get_mtss(to_jq_code(symbol), s, e)
            if not isinstance(df, pd.DataFrame) or len(df) == 0:
                continue
            out = pd.DataFrame(index=pd.DatetimeIndex(df.index))
            out["fin_balance"] = df["fin_value"].astype("float64")
            out["fin_buy"] = df["fin_buy_value"].astype("float64")
            out["fin_repay"] = df["fin_refund_value"].astype("float64")
            out["sec_balance"] = df["sec_value"].astype("float64")
            out["sec_sell"] = df["sec_sell_value"].astype("float64")
            out["sec_repay"] = df["sec_refund_value"].astype("float64")
            out["total_balance"] = df["fin_sec_value"].astype("float64")
            # available_at 由 jq 层给出(发布语义属数据源元知识), 本层只映射
            out["available_at"] = pd.to_datetime(df["available_at"]).to_numpy()
            out.index = pd.MultiIndex.from_arrays(
                [out.index, [symbol] * len(out)], names=["date", "symbol"]
            )
            frames.append(out)
        return pd.concat(frames).sort_index() if frames else _empty_indexed(
            ["date", "symbol"]
        )

    def fetch_money_flow(
        self, symbols: list[str], start: pd.Timestamp, end: pd.Timestamp
    ) -> pd.DataFrame:
        """资金流向. 返回 MoneyFlow schema, index=(date, symbol).

        聚宽 ``get_money_flow`` 的 net_amount_* 单位为**万元**, 此处×1e4 换为元
        (schema 要求金额统一为元)。

        Raises:
            DataUnavailableError: 传入非个股(ETF/基金/指数)。聚宽会报
                ``get_money_flow只能用来查询股票的资金流向数据`` ——
                在入口拦住并给出 qlab 语境的说明, 与 :meth:`fetch_bars` 一致。
        """
        non_stock = [x for x in dict.fromkeys(symbols) if not _is_stock_symbol(x)]
        if non_stock:
            raise DataUnavailableError(
                f"money_flow 仅支持个股, 不支持 ETF/基金/指数: "
                f"{non_stock[:10]}{'...' if len(non_stock) > 10 else ''}\n"
                "  原因: 聚宽 get_money_flow 只接股票 —— 资金流向按单笔委托量级"
                "拆分(超大/大/中/小单), 对基金份额与指数无此口径。\n"
                "  出路: 从标的列表中剔除非个股。"
            )
        s, e = str(pd.Timestamp(start).date()), str(pd.Timestamp(end).date())
        amount_cols = [
            "net_amount_main", "net_amount_xl", "net_amount_l",
            "net_amount_m", "net_amount_s",
        ]
        pct_cols = [
            "change_pct", "net_pct_main", "net_pct_xl",
            "net_pct_l", "net_pct_m", "net_pct_s",
        ]
        frames = []
        for symbol in dict.fromkeys(symbols):
            df = self.cache.get_money_flow(to_jq_code(symbol), s, e)
            if not isinstance(df, pd.DataFrame) or len(df) == 0:
                continue
            out = pd.DataFrame(index=pd.DatetimeIndex(df.index))
            for col in amount_cols:
                out[col] = df[col].astype("float64") * 1e4  # 万元 → 元
            for col in pct_cols:
                out[col] = df[col].astype("float64")
            # available_at 由 jq 层给出(发布语义属数据源元知识), 本层只映射
            out["available_at"] = pd.to_datetime(df["available_at"]).to_numpy()
            out.index = pd.MultiIndex.from_arrays(
                [out.index, [symbol] * len(out)], names=["date", "symbol"]
            )
            frames.append(out)
        return pd.concat(frames).sort_index() if frames else _empty_indexed(
            ["date", "symbol"]
        )

    def fetch_call_auction(
        self, symbols: list[str], start: pd.Timestamp, end: pd.Timestamp
    ) -> pd.DataFrame:
        """集合竞价. 返回 CallAuction schema, index=(date, symbol).

        聚宽 ``get_call_auction`` 是**单标的**接口(security: str), 故逐标的循环;
        返回不复权报价 + 五档盘口(a1_p..b5_v)。
        """
        s, e = str(pd.Timestamp(start).date()), str(pd.Timestamp(end).date())
        frames = []
        for symbol in dict.fromkeys(symbols):
            raw = self.cache.get_call_auction(to_jq_code(symbol), s, e)
            if not isinstance(raw, pd.DataFrame) or len(raw) == 0:
                continue
            out = pd.DataFrame(index=pd.DatetimeIndex(raw.index).normalize())
            out["auction_price"] = raw["current"].astype("float64").to_numpy()
            out["auction_volume"] = raw["volume"].fillna(0).astype("float64").to_numpy()
            out["auction_amount"] = raw["money"].fillna(0).astype("float64").to_numpy()
            for i in range(1, 6):
                out[f"ask{i}_price"] = raw[f"a{i}_p"].astype("float64").to_numpy()
                out[f"ask{i}_volume"] = raw[f"a{i}_v"].astype("float64").to_numpy()
                out[f"bid{i}_price"] = raw[f"b{i}_p"].astype("float64").to_numpy()
                out[f"bid{i}_volume"] = raw[f"b{i}_v"].astype("float64").to_numpy()
            # available_at 由 jq 层给出(竞价当日即可用), 本层只映射
            out["available_at"] = pd.to_datetime(raw["available_at"]).to_numpy()
            out.index = pd.MultiIndex.from_arrays(
                [out.index, [symbol] * len(out)], names=["date", "symbol"]
            )
            frames.append(out)
        if not frames:
            return _empty_indexed(["date", "symbol"])
        return pd.concat(frames).sort_index()

    def fetch_billboard(
        self,
        end_date: pd.Timestamp,
        symbols: list[str] | None = None,
        count: int = 5,
    ) -> pd.DataFrame:
        """龙虎榜. 返回 Billboard schema(RangeIndex, 同 (date,symbol) 多行).

        Args:
            end_date: 截止日(必填).
            symbols: 限定标的; ``None`` = 全市场。
            count: 回看天数(非行数)。
        """
        e = str(pd.Timestamp(end_date).date())
        stock_list = (
            [to_jq_code(s) for s in symbols] if symbols else None
        )
        raw = self.cache.get_billboard_list(e, stock_list=stock_list, count=count)
        if not isinstance(raw, pd.DataFrame) or len(raw) == 0:
            return pd.DataFrame()
        out = pd.DataFrame()
        out["date"] = pd.to_datetime(raw["day"])
        out["symbol"] = [to_qlab_symbol(str(c)) for c in raw["code"]]
        out["direction"] = raw["direction"].astype(str).str.upper()
        out["rank"] = raw["rank"].astype("int64")
        out["abnormal_code"] = raw["abnormal_code"].astype(str)
        out["abnormal_name"] = raw["abnormal_name"].astype(str)
        out["sales_depart"] = raw["sales_depart_name"].astype(str)
        for col, src in [
            ("buy_value", "buy_value"), ("buy_rate", "buy_rate"),
            ("sell_value", "sell_value"), ("sell_rate", "sell_rate"),
            ("total_value", "total_value"), ("net_value", "net_value"),
            ("amount", "amount"),
        ]:
            out[col] = raw[src].astype("float64")
        return out.reset_index(drop=True)

    def fetch_factor_exposure(
        self,
        symbols: list[str],
        factors: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        """因子暴露. 返回 FactorExposure schema, index=(date, symbol), 列名 = 因子名.

        聚宽 ``jqfactor.get_factor_values`` 返回 ``{factor: DataFrame(index=日期, columns=code)}``,
        此处重组为 (date, symbol) × factor 的宽表。
        """
        s, e = str(pd.Timestamp(start).date()), str(pd.Timestamp(end).date())
        codes = _unique_jq_codes(symbols)
        vals = self.cache.get_factor_values(codes, factors, e, start_date=s)
        if not isinstance(vals, dict) or not vals:
            return _empty_indexed(["date", "symbol"])
        long_cols = {}
        for factor, wide in vals.items():
            if not isinstance(wide, pd.DataFrame) or wide.empty:
                continue
            stacked = (
                wide.stack(future_stack=True) if _SUPPORTS_FUTURE_STACK else wide.stack()
            )
            stacked.index = pd.MultiIndex.from_arrays(
                [
                    pd.DatetimeIndex(stacked.index.get_level_values(0)).normalize(),
                    [to_qlab_symbol(str(c)) for c in stacked.index.get_level_values(1)],
                ],
                names=["date", "symbol"],
            )
            long_cols[factor] = stacked.astype("float64")
        if not long_cols:
            return _empty_indexed(["date", "symbol"])
        out = pd.DataFrame(long_cols)
        out.index.names = ["date", "symbol"]
        return out.sort_index()

    # ==================================================================
    # Universe / 行业 / 概念
    # ==================================================================

    def fetch_universe(
        self, spec: str, date_range: tuple[pd.Timestamp, pd.Timestamp]
    ) -> pd.DataFrame:
        """PIT universe.

        **指数成分**: 按月采样权重后前填到每个交易日（聚宽权重本身月频,
        调样集中在少数日子；逐日请求无额外信息且极慢）。前填用
        ``reindex(method='ffill')``，避免成分剔除后被列向 ffill「只进不出」。

        **宽基池** ``main_a`` / ``hs_a``: 先取全市场并按代码剔板块，再按**每个
        交易日**的 ``is_st`` 剔除 ST（ST 状态会变，不能只在月末快照上滤一次）。
        """
        from qlab.data.universe import apply_board_filters, parse_broad_universe

        start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
        sessions = self._sessions_upto_today(start, end)
        if len(sessions) == 0:
            return _empty_universe()

        resolved = _resolve_universe_spec(spec)
        broad = parse_broad_universe(resolved)

        frames = []
        for asof in _period_sample_dates(sessions):
            frames.append(self._universe_at(resolved, asof, broad=broad))
        snapshots = [f for f in frames if f is not None and len(f) > 0]
        if not snapshots:
            return _empty_universe()

        panel = pd.concat(snapshots).sort_index()
        wide_in = panel["in_universe"].unstack("symbol").reindex(sessions, method="ffill")
        wide_w = panel["weight"].unstack("symbol").reindex(sessions, method="ffill")

        out = pd.DataFrame(
            {
                "in_universe": wide_in.stack(future_stack=True)
                if _SUPPORTS_FUTURE_STACK
                else wide_in.stack(),
                "weight": wide_w.stack(future_stack=True)
                if _SUPPORTS_FUTURE_STACK
                else wide_w.stack(),
            }
        )
        out.index.names = ["date", "symbol"]
        in_u = out["in_universe"]
        if in_u.dtype != bool:
            in_u = in_u.fillna(False).astype(bool)
        out = out.loc[in_u].copy()
        out["in_universe"] = True

        # 宽基池：按日剔 ST（PIT）
        if broad is not None:
            _, exclude_st = broad
            if exclude_st and not out.empty:
                out = self._drop_st_from_universe(out)

        return out

    def _drop_st_from_universe(self, uni: pd.DataFrame) -> pd.DataFrame:
        """用日频 is_st 从宇宙中剔除当日 ST（缺值 fail-loud）."""
        symbols = uni.index.get_level_values("symbol").unique().tolist()
        dates = uni.index.get_level_values("date")
        start, end = dates.min(), dates.max()
        codes = _unique_jq_codes(symbols)
        wide = self.cache.get_extras("is_st", codes, start, end)
        if wide is None or (isinstance(wide, pd.DataFrame) and wide.empty):
            raise DataUnavailableError(
                f"构建剔 ST 宇宙需要 is_st，但 get_extras 无数据: {symbols[:10]}"
            )
        # 宽表可能是聚宽代码列 — 对齐到 qlab symbol
        wide = wide.copy()
        wide.columns = [to_qlab_symbol(str(c)) for c in wide.columns]
        wide.index = pd.DatetimeIndex(wide.index).normalize()
        st = wide.reindex(index=pd.DatetimeIndex(uni.index.get_level_values("date").unique()))
        try:
            st_long = st.stack(future_stack=True)
        except TypeError:
            st_long = st.stack()
        st_long.index = st_long.index.set_names(["date", "symbol"])
        flag = st_long.reindex(uni.index)
        if flag.isna().any():
            n_miss = int(flag.isna().sum())
            raise DataUnavailableError(
                f"剔 ST 宇宙时 is_st 覆盖不全: {n_miss} 个 (date, symbol) 缺值，拒绝静默放行。"
            )
        keep = ~flag.astype(bool)
        out = uni.loc[keep].copy()
        return out

    def _universe_at(
        self,
        resolved: str,
        asof: pd.Timestamp,
        *,
        broad: tuple[bool, bool] | None = None,
    ) -> pd.DataFrame | None:
        """单个时点的 universe 快照（``resolved`` 已经过 :func:`_resolve_universe_spec`）."""
        from qlab.data.universe import apply_board_filters

        date_str = str(asof.date())
        if resolved in ("all", "main_a", "hs_a"):
            info = self.cache.get_all_securities(date_str, types=["stock"])
            members = [to_qlab_symbol(str(c)) for c in info.index]
            if broad is not None:
                exclude_star, _exclude_st = broad
                members = apply_board_filters(
                    members, exclude_bj=True, exclude_star=exclude_star
                )
            weights = [np.nan] * len(members)
        else:
            w = self.cache.get_index_weights(to_jq_code(resolved), date_str)
            if not isinstance(w, pd.DataFrame) or w.empty:
                return None
            members = [to_qlab_symbol(str(c)) for c in w.index]
            total = float(w["weight"].sum())
            # 聚宽 weight 是百分数且含舍入误差 —— 归一到 sum=1.0
            weights = (
                (w["weight"].astype(float) / total).tolist()
                if total > 0
                else [np.nan] * len(members)
            )
        if not members:
            return None
        idx = pd.MultiIndex.from_arrays(
            [[asof] * len(members), members], names=["date", "symbol"]
        )
        return pd.DataFrame({"in_universe": True, "weight": weights}, index=idx)

    def fetch_industry_classification(
        self,
        symbols: list[str],
        date_range: tuple[pd.Timestamp, pd.Timestamp],
        system: str = "sw",
        level: int = 1,
        sample_freq: str = "M",
    ) -> pd.DataFrame:
        """行业分类. ``system`` ∈ {sw, jq, csrc}.

        **按 ``sample_freq`` 采样**返回区间内的分类时序(而非只取起点一天) —— 行业归属
        会调整, 只取起点会把后续变更全部丢失; 而逐日取则浪费(分类变更频率远低于日频)。

        Args:
            sample_freq: ``M``(月, 默认) / ``Q``(季) / ``Y``(年)。区间很长时调稀可
                大幅减少请求数, 代价是分类变更的时间粒度变粗。

        聚宽 ``get_industry`` 一次返回 6 套分类, 这里按 (system, level) 选取。
        """
        key = _industry_key(system, level)
        parent_key = _INDUSTRY_PARENT.get(key)
        codes = _unique_jq_codes(symbols)
        rows = []
        for asof in self._sample_dates(date_range, sample_freq, "fetch_industry_classification"):
            raw = self.cache.get_industry(codes, str(asof.date()))
            for code, systems in (raw or {}).items():
                entry = (systems or {}).get(key)
                if not entry:
                    continue
                # 聚宽一次返回全部层级, 故上一层代码本就在手里 ——
                # SCHEMA_INDUSTRY 不变量要求 level>1 时 parent_code 存在。
                parent = None
                if parent_key:
                    parent = ((systems or {}).get(parent_key) or {}).get("industry_code")
                    parent = str(parent) if parent else None
                rows.append(
                    {
                        "date": asof,
                        "symbol": to_qlab_symbol(str(code)),
                        "system": system,
                        "level": np.int8(level),
                        "industry_code": str(entry.get("industry_code", "")),
                        "industry_name": str(entry.get("industry_name", "")),
                        "parent_code": parent,
                    }
                )
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).set_index(["date", "symbol", "system"])

    def fetch_concepts(
        self,
        symbols: list[str],
        date_range: tuple[pd.Timestamp, pd.Timestamp],
        source: str | None = None,
        sample_freq: str = "M",
    ) -> pd.DataFrame:
        """概念板块. 返回每个 (标的, 概念) 的**归属区间**.

        按 ``sample_freq`` 采样后把连续出现的采样点合成一段:

        - ``effective_date`` = 该段首次观测到的采样日
        - ``expired_date`` = 该段之后第一个**未**观测到的采样日; 直到区间末尾仍
          存在则为 ``NaT``

        中途移出又重新纳入会产生多段。``expired_date`` 不是可选装饰 ——
        下游 :func:`~qlab.data.concept.concepts_as_of` 靠它做 PIT 过滤,
        恒为 NaT 会让已移出的概念永久滞留。

        Args:
            source: 缺省用 :attr:`concept_source` (``"jq"``)。消费端需传相同值,
                否则静默返回空。
            sample_freq: ``M``(月, 默认) / ``Q``(季) / ``Y``(年)。

        Note:
            只需某一天的归属时用 :meth:`concepts_asof` —— 它只发 1 次请求。
        """
        codes = _unique_jq_codes(symbols)
        samples = self._sample_dates(date_range, sample_freq, "fetch_concepts")
        if not samples:
            return pd.DataFrame()
        src_tag = self.concept_source if source is None else source

        # (symbol, concept_code) -> {"name": 概念名, "idx": [采样点序号...]}
        seen: dict[tuple[str, str], dict] = {}
        for i, asof in enumerate(samples):
            raw = self.cache.get_concept(codes, str(asof.date()))
            for code, payload in (raw or {}).items():
                sym = to_qlab_symbol(str(code))
                for item in (payload or {}).get("jq_concept", []) or []:
                    ccode = str(item.get("concept_code", ""))
                    rec = seen.setdefault(
                        (sym, ccode),
                        {"name": str(item.get("concept_name", "")), "idx": []},
                    )
                    rec["idx"].append(i)

        rows = []
        for (sym, ccode), rec in seen.items():
            for lo, hi in _consecutive_runs(rec["idx"]):
                rows.append(
                    {
                        "effective_date": samples[lo],
                        "symbol": sym,
                        "source": src_tag,
                        "concept_code": ccode,
                        "concept_name": rec["name"],
                        "expired_date": (
                            samples[hi + 1] if hi + 1 < len(samples) else pd.NaT
                        ),
                    }
                )
        if not rows:
            return pd.DataFrame()
        out = pd.DataFrame(rows).set_index(["effective_date", "symbol", "source"])
        return out.sort_index()

    def concepts_asof(
        self, symbols: list[str], date: object, source: str | None = None
    ) -> pd.DataFrame:
        """**某一天**的概念归属 —— 只发 1 次请求.

        :meth:`fetch_concepts` 面向"需要进出历史"的场景, 会按采样频率逐点取数
        (5 年区间约 62 次); 而很多时候调用方只想知道"某天属于哪些概念",
        此时用本方法。

        返回仍符合 ConceptClassification schema(``expired_date`` 为 NaT),
        可直接喂给 :func:`~qlab.data.concept.concepts_as_of`。
        """
        d = pd.Timestamp(date)
        return self.fetch_concepts(symbols, (d, d), source=source)

    def industry_asof(
        self,
        symbols: list[str],
        date: object,
        system: str = "sw",
        level: int = 1,
    ) -> pd.DataFrame:
        """**某一天**的行业归属 —— 只发 1 次请求.

        :meth:`fetch_industry_classification` 按采样频率逐点取数(5 年区间约 62 次),
        面向"需要行业变更历史"的场景; 而很多时候调用方只想知道"某天属于哪个行业",
        此时用本方法。行业归属基本稳定, 单点快照对归属查询已足。

        返回仍符合 IndustryClassification schema, 可直接喂给
        :func:`~qlab.data.industry.industry_as_of`。
        """
        d = pd.Timestamp(date)
        return self.fetch_industry_classification(symbols, (d, d), system, level)

    def _sample_dates(
        self,
        date_range: tuple[pd.Timestamp, pd.Timestamp],
        sample_freq: str = "M",
        _caller: str = "",
    ) -> list[pd.Timestamp]:
        """把区间压成采样日(含首尾交易日)."""
        sessions = self._sessions_upto_today(date_range[0], date_range[1])
        if len(sessions) == 0:
            return []
        samples = _period_sample_dates(sessions, sample_freq)
        if len(samples) > _SAMPLE_WARN_THRESHOLD and _caller:
            warnings.warn(
                f"{_caller}: 区间跨度导致 {len(samples)} 个采样点(每点一次远程请求)。"
                "\n  只需某一天的归属请用 JQDataSource.concepts_asof(symbols, date)(1 次请求);"
                f"\n  需要历史但可接受更粗粒度则传 sample_freq='Q' 或 'Y'。",
                RuntimeWarning,
                stacklevel=3,
            )
        return samples

    def _sessions_upto_today(
        self, start: object, end: object
    ) -> pd.DatetimeIndex:
        """区间内的交易日, **截到今天**.

        聚宽交易日历含未来两年的日期(实测到 2028 年), 直接拿它去请求
        成分/行业/概念, 聚宽会拿**当前快照**冒充那些未来时点的数据 ——
        这正是本项目极力避免的未来函数。故采样点一律不越过今天。
        """
        lo = pd.Timestamp(start).normalize()
        hi = min(pd.Timestamp(end).normalize(), pd.Timestamp(date.today()))
        if hi < lo:
            return pd.DatetimeIndex([])
        return self.fetch_calendar().trading_days(lo, hi)

    # ==================================================================
    # 暂不实现(非不能, 而是当前不需)
    # ==================================================================

    def fetch_corporate_actions(
        self, symbols: list[str], start: pd.Timestamp, end: pd.Timestamp
    ) -> pd.DataFrame:
        """公司行为事件 —— 暂不实现(有数据源但当前不需).

        聚宽**有**干净的除权除息表 ``finance.STK_XR_XD``(47 列, 含
        ``board_plan_pub_date`` / ``a_registration_date`` / ``a_xr_date`` /
        ``bonus_ratio_rmb`` / ``transfer_ratio`` / 股本变动前后等 PIT 字段), 技术上
        可照 fundamentals 的方式接入。

        不实现的原因是**不需**而非不能:
        - 本源已直接提供 ``adj_factor``(后复权因子), 复权不需由公司行为反推;
        - 公司行为明细对当前中短线 beta 捕捉策略不是有效信息。

        将来需要时, 照 :meth:`fetch_fundamentals` 同模式走 ``get_fundamentals``
        (表名 ``STK_XR_XD``, 按 ``board_plan_pub_date`` 做 PIT 过滤)即可。
        """
        raise NotImplementedError(
            "JQDataSource 暂不提供 corporate_actions（adj_factor 已足用, "
            "且公司行为非当前策略的有效信息; 数据源 finance.STK_XR_XD 可用但未接）"
        )

    def fetch_fundamentals(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
        report_types: list[ReportType] | None = None,
        fields: list[str] | None = None,
    ) -> pd.DataFrame:
        """财务数据. 返回 Fundamental schema, index=(announce_date, symbol, report_type).

        按 ``report_types`` 取不同披露阶段:

        - ``OFFICIAL``: 拼利润/资负/现金流三表(finance.STK_*), 只取 report_type=0(本期,
          避免 =1 的追溯调整重复)
        - ``FORECAST``: finance.STK_FIN_FORCAST(业绩预告)
        - ``FLASH``: 聚宽无业绩快报专表, 静默跳过

        PIT: ``announce_date`` = 聚宽 ``pub_date``(披露日), ``report_period`` = ``end_date``。
        按披露日落入 ``[start, end]`` 区间(而非报告期), 这才是 PIT 正确语义。
        """
        req = report_types or [ReportType.OFFICIAL, ReportType.FORECAST]
        s, e = str(pd.Timestamp(start).date()), str(pd.Timestamp(end).date())
        codes = _unique_jq_codes(symbols)
        parts = []
        for rt in req:
            if rt == ReportType.OFFICIAL:
                df = self._fetch_official(codes, s, e)
            elif rt == ReportType.FORECAST:
                df = self._fetch_forecast(codes, s, e)
            else:  # FLASH —— 聚宽无专表
                continue
            if df is not None and len(df) > 0:
                parts.append(df)
        if not parts:
            return pd.DataFrame()
        out = pd.concat(parts, ignore_index=True)
        return out.sort_values(["announce_date", "symbol"]).reset_index(drop=True)

    def _fetch_official(self, codes: list[str], s: str, e: str) -> pd.DataFrame:
        """拼利润/资负/现金流三表, 按 (code, end_date, pub_date) 对齐.

        三表各自走 jq 的 ``get_fundamentals`` 一等接口(只取 report_type=0 本期),
        query 构造与跨环境净化都在 jq 层; 本层只负责本地 merge + 字段映射。
        """
        keys = ["code", "end_date", "pub_date"]
        inc = self.cache.get_fundamentals(
            "STK_INCOME_STATEMENT", codes,
            ["total_operating_revenue", "operating_revenue", "operating_cost",
             "operating_profit", "net_profit", "np_parent_company_owners",
             "basic_eps", "diluted_eps"],
            e, start_date=s, report_type=0,
        )
        if not isinstance(inc, pd.DataFrame) or len(inc) == 0:
            return pd.DataFrame()
        bal = self.cache.get_fundamentals(
            "STK_BALANCE_SHEET", codes,
            ["total_assets", "total_liability", "total_owner_equities",
             "equities_parent_company_owners", "cash_equivalents",
             "total_current_assets", "total_current_liability"],
            e, start_date=s, report_type=0,
        )
        cfs = self.cache.get_fundamentals(
            "STK_CASHFLOW_STATEMENT", codes,
            ["net_operate_cash_flow", "net_invest_cash_flow", "net_finance_cash_flow"],
            e, start_date=s, report_type=0,
        )
        raw = inc
        if isinstance(bal, pd.DataFrame) and len(bal):
            raw = raw.merge(bal, on=keys, how="outer")
        if isinstance(cfs, pd.DataFrame) and len(cfs):
            raw = raw.merge(cfs, on=keys, how="outer")
        return self._assemble_fundamental(raw, ReportType.OFFICIAL, _OFFICIAL_MAP)

    def _fetch_forecast(self, codes: list[str], s: str, e: str) -> pd.DataFrame:
        """业绩预告(finance.STK_FIN_FORCAST) —— 走 jq get_fundamentals."""
        raw = self.cache.get_fundamentals(
            "STK_FIN_FORCAST", codes,
            ["type", "profit_min", "profit_max",
             "profit_ratio_min", "profit_ratio_max"],
            e, start_date=s,
        )
        if not isinstance(raw, pd.DataFrame) or len(raw) == 0:
            return pd.DataFrame()
        return self._assemble_fundamental(raw, ReportType.FORECAST, _FORECAST_MAP)

    @staticmethod
    def _assemble_fundamental(
        raw: pd.DataFrame, report_type: ReportType, colmap: dict[str, str]
    ) -> pd.DataFrame:
        """把聚宽原始行组装为 Fundamental schema.

        返回**普通列形式**(不 set_index), 与 FakeDataSource 一致:
        DataLayer.fundamentals() 用 strict_index=False 校验, index 字段
        (announce_date/symbol/report_type)同时作为列存在。
        """
        out = pd.DataFrame(index=raw.index)
        for dst, src in colmap.items():
            if src in raw.columns:
                out[dst] = raw[src]
        pub = pd.to_datetime(raw["pub_date"])
        period = pd.to_datetime(raw["end_date"])
        out["report_period"] = period
        out["announce_date"] = pub
        out["report_type"] = report_type.value
        # available_at: 披露日当日 09:30 后可用(schema 不变量)
        out["available_at"] = pub.dt.normalize() + pd.Timedelta(hours=9, minutes=30)
        out["fiscal_year"] = period.dt.year.astype("int16")
        out["fiscal_quarter"] = period.dt.quarter.astype("int8")
        out["source"] = "jq"
        out["symbol"] = [to_qlab_symbol(str(c)) for c in raw["code"]]
        return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

# pandas ≥ 2.1 的 stack 需显式 future_stack 以避免 FutureWarning
_SUPPORTS_FUTURE_STACK = pd.__version__ >= "2.1"


class JQCalendar:
    """用聚宽交易日列表支撑的 :class:`~qlab.core.calendar.Calendar` 实现.

    与默认的 :class:`~qlab.core.calendar.AShareCalendar`(基于 exchange_calendars)
    的区别: 日历口径与行情数据同源, 不会出现"日历说是交易日但数据源无数据"的错位.

    会话时刻类属性与 ``AShareCalendar`` 保持一致 —— 它们不在 ``Calendar``
    Protocol 里, 但是既有实现的事实契约, 下游可能直接读 ``cal.MORNING_OPEN``。
    """

    # 标准会话时间(与 AShareCalendar 一致)
    OPEN_AUCTION_START = _time(9, 15)
    OPEN_AUCTION_END = _time(9, 25)
    MORNING_OPEN = _time(9, 30)
    MORNING_CLOSE = _time(11, 30)
    AFTERNOON_OPEN = _time(13, 0)
    AFTERNOON_CLOSE = _time(15, 0)
    CLOSE_AUCTION_START = _time(14, 57)

    def __init__(self, name: str, sessions: pd.DatetimeIndex) -> None:
        self.name = name
        self.sessions = sessions
        self._set = set(sessions)

    def is_trading_day(self, date: pd.Timestamp) -> bool:
        return pd.Timestamp(date).normalize() in self._set

    def trading_days(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
        lo = pd.Timestamp(start).normalize()
        hi = pd.Timestamp(end).normalize()
        return self.sessions[(self.sessions >= lo) & (self.sessions <= hi)]

    def prev_trading_day(self, date: pd.Timestamp, n: int = 1) -> pd.Timestamp:
        d = pd.Timestamp(date).normalize()
        pos = self.sessions.searchsorted(d, side="left")
        target = int(pos) - n
        if target < 0:
            raise IndexError(f"{d.date()} 前第 {n} 个交易日超出日历起点")
        return self.sessions[target]

    def next_trading_day(self, date: pd.Timestamp, n: int = 1) -> pd.Timestamp:
        d = pd.Timestamp(date).normalize()
        pos = self.sessions.searchsorted(d, side="right")
        target = int(pos) + n - 1
        if target >= len(self.sessions):
            raise IndexError(f"{d.date()} 后第 {n} 个交易日超出日历终点")
        return self.sessions[target]

    def count_trading_days(self, start: pd.Timestamp, end: pd.Timestamp) -> int:
        return len(self.trading_days(start, end))

    def session_times(self, date: pd.Timestamp) -> dict[str, pd.Timestamp]:
        # A 股会话时刻与默认日历一致
        d = pd.Timestamp(date).normalize()
        return {
            "open_auction_start": d + pd.Timedelta(hours=9, minutes=15),
            "open_auction_end": d + pd.Timedelta(hours=9, minutes=25),
            "morning_open": d + pd.Timedelta(hours=9, minutes=30),
            "morning_close": d + pd.Timedelta(hours=11, minutes=30),
            "afternoon_open": d + pd.Timedelta(hours=13, minutes=0),
            "close_auction_start": d + pd.Timedelta(hours=14, minutes=57),
            "afternoon_close": d + pd.Timedelta(hours=15, minutes=0),
        }


def _split_by_daily_factor(
    post: pd.DataFrame, day_factors: pd.Series
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """给聚合分钟数据补上 ``factor``, 并推出不复权口径.

    聚合分钟频率拿不到 ``factor``(聚宽只允许 6 个基础字段), 但同一交易日内
    所有 bar 共享当日复权因子, 所以可以从日线映射过来 —— 这同时满足
    IntradayBar 不变量"同一 (date, symbol) 共享 adj_factor"。

    日线缺该日 factor 的 bar 会被剔除(而非猜一个因子)。

    Returns:
        ``(post_with_factor, raw)`` —— raw 的价格 = post / factor,
        成交量 = post × factor(与聚宽复权语义一致)。
    """
    days = pd.DatetimeIndex(post.index).normalize()
    f = pd.Series(days, index=post.index).map(day_factors)
    keep = f.notna()
    if not keep.all():
        post = post[keep]
        f = f[keep]
    if len(post) == 0:
        return post, post

    p = post.copy()
    p["factor"] = f.astype(float).to_numpy()

    r = p.copy()
    scale = r["factor"].astype(float)
    for col in _PRICE_COLS:
        if col in r.columns:
            r[col] = r[col] / scale
    if "volume" in r.columns:
        r["volume"] = r["volume"] * scale
    r["factor"] = 1.0
    return p, r


def _unique_jq_codes(symbols: list[str]) -> list[str]:
    """映射为聚宽代码并**去重**(保序).

    必须在映射**之后**去重: ``600519.SH`` 与 ``600519.XSHG`` 指同一只但字面不同,
    按原字串去重拦不住。不去重会让同一标的被拼多次 → index 重复 →
    下游 ``unstack`` 抛
    ``ValueError: Index contains duplicate entries``。
    """
    return list(dict.fromkeys(to_jq_code(s) for s in symbols))


def _consecutive_runs(indices: list[int]) -> list[tuple[int, int]]:
    """把升序序号列表压成连续区间: ``[0,1,2,4,5] -> [(0,2), (4,5)]``."""
    if not indices:
        return []
    ordered = sorted(set(indices))
    runs: list[tuple[int, int]] = []
    lo = prev = ordered[0]
    for i in ordered[1:]:
        if i == prev + 1:
            prev = i
            continue
        runs.append((lo, prev))
        lo = prev = i
    runs.append((lo, prev))
    return runs


#: 随复权缩放的价格列(与 jq.cache 的口径一致)
_PRICE_COLS = (
    "open", "close", "high", "low", "avg", "pre_close", "high_limit", "low_limit",
)

#: 采样频率 → pandas period 别名
_SAMPLE_PERIODS = {"M": "M", "Q": "Q", "Y": "Y"}

#: 超过这么多采样点就告警(每点一次远程请求)
_SAMPLE_WARN_THRESHOLD = 24


def _period_sample_dates(
    sessions: pd.DatetimeIndex, sample_freq: str = "M"
) -> list[pd.Timestamp]:
    """从交易日序列里取每期第一个交易日(并保留首尾).

    用于把"逐日查询"降为"按期查询 + 前填": 名录/成分/行业/概念这类数据的
    真实变动频率远低于日频, 聚宽的指数权重更是直接以月为粒度。
    """
    if len(sessions) == 0:
        return []
    period = _SAMPLE_PERIODS.get(str(sample_freq).upper().rstrip("E"))
    if period is None:
        raise ValueError(
            f"不支持的 sample_freq: {sample_freq!r}(支持 M / Q / Y)"
        )
    s = pd.Series(sessions, index=sessions)
    firsts = s.groupby(sessions.to_period(period)).first().tolist()
    out = sorted({sessions[0], *firsts, sessions[-1]})
    return [pd.Timestamp(d) for d in out]


def _empty_universe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "in_universe": pd.Series(dtype=bool),
            "weight": pd.Series(dtype="float64"),
        },
        index=pd.MultiIndex.from_arrays(
            [pd.DatetimeIndex([]), []], names=["date", "symbol"]
        ),
    )


def _empty_indexed(index_names: list[str]) -> pd.DataFrame:
    """空 DataFrame, 带指定名字的空 MultiIndex(供无数据时返回)."""
    return pd.DataFrame(
        index=pd.MultiIndex.from_arrays(
            [pd.DatetimeIndex([])] + [[] for _ in index_names[1:]],
            names=index_names,
        )
    )


#: (system, level) 对应的上一层键 —— 用于填 SCHEMA_INDUSTRY 的 parent_code
_INDUSTRY_PARENT = {
    "sw_l2": "sw_l1",
    "sw_l3": "sw_l2",
    "jq_l2": "jq_l1",
}

#: 正式报表三表 → SCHEMA_FUNDAMENTAL 字段映射(qlab 列 : 聚宽列)
_OFFICIAL_MAP = {
    "total_revenue": "total_operating_revenue",
    "operating_revenue": "operating_revenue",
    "operating_cost": "operating_cost",
    "operating_profit": "operating_profit",
    "net_profit": "net_profit",
    "net_profit_to_shareholders": "np_parent_company_owners",
    "eps_basic": "basic_eps",
    "eps_diluted": "diluted_eps",
    "total_assets": "total_assets",
    "total_liabilities": "total_liability",
    "total_equity": "total_owner_equities",
    "equity_to_shareholders": "equities_parent_company_owners",
    "cash_and_equivalents": "cash_equivalents",
    "current_assets": "total_current_assets",
    "current_liabilities": "total_current_liability",
    "operating_cash_flow": "net_operate_cash_flow",
    "investing_cash_flow": "net_invest_cash_flow",
    "financing_cash_flow": "net_finance_cash_flow",
}

#: 业绩预告 → SCHEMA_FUNDAMENTAL 预告专属字段
_FORECAST_MAP = {
    "forecast_net_profit_min": "profit_min",
    "forecast_net_profit_max": "profit_max",
    "forecast_change_pct_min": "profit_ratio_min",
    "forecast_change_pct_max": "profit_ratio_max",
    "forecast_type": "type",
}


def _industry_key(system: str, level: int) -> str:
    """(system, level) → 聚宽 get_industry 的键名."""
    system = system.lower()
    if system == "csrc":
        return "zjw"
    if system in ("sw", "jq"):
        return f"{system}_l{level}"
    raise ValueError(f"不支持的行业体系: {system}（支持 sw / jq / csrc）")


def _session_of(ts: Any) -> str:
    """按时刻判定交易时段.

    分钟线的时间戳是 bar 的**结束时刻**, 故:

    - ≤ 09:30  → 开盘集合竞价(09:15-09:25 委托, 09:25 成交)
    - ≤ 11:30  → 上午连续竞价
    - ≤ 14:57  → 下午连续竞价
    - > 14:57  → 收盘集合竞价(2018 年起 A 股 14:57-15:00 为尾盘集竞)
    """
    t = pd.Timestamp(ts).time()
    if t <= _T_OPEN_AUCTION_END:
        return "open_auction"
    if t <= _T_MORNING_CLOSE:
        return "morning"
    if t <= _T_CLOSE_AUCTION_START:
        return "afternoon"
    return "close_auction"
