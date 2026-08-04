"""DataLayer — 数据层统一入口.

包装 DataSource + BarStore + Universe，向下游提供干净的访问 API。
下游通过 DataLayer 访问所有数据，不直接接触 DataSource。
"""

from __future__ import annotations

import pandas as pd

from qlab.core.calendar import Calendar, get_default_calendar
from qlab.core.enums import AdjustMode, Freq, ReportType
from qlab.core.schema import (
    SCHEMA_BILLBOARD,
    SCHEMA_CALL_AUCTION,
    SCHEMA_DAILY_BAR,
    SCHEMA_FACTOR_EXPOSURE,
    SCHEMA_FUNDAMENTAL,
    SCHEMA_INTRADAY_BAR,
    SCHEMA_MARGIN_TRADING,
    SCHEMA_MONEY_FLOW,
    validate_schema,
)
from qlab.data.alignment import IntradayAligner
from qlab.data.concept import concepts_as_of
from qlab.data.fundamentals import latest_fundamental_as_of, ttm_value
from qlab.data.industry import industry_as_of
from qlab.data.interfaces import BarStore, DataSource, ShardedBarStore
from qlab.data.store import InMemoryBarStore, InMemoryShardedBarStore, make_cache_key
from qlab.data.universe import Universe, UniverseSpec


def _check_store_arg(obj, name: str, want, other, other_name: str) -> None:
    """校验 store 参数满足其协议; 若满足的是**另一个**协议则指出传反了."""
    if obj is None or isinstance(obj, want):
        return
    hint = (
        f"\n  看起来传反了: 它满足 {other.__name__}, 应该传给 {other_name}=。"
        if isinstance(obj, other)
        else f"\n  需实现 {want.__name__} 的全部方法。"
    )
    raise TypeError(
        f"DataLayer(...{name}=) 需要 {want.__name__}, "
        f"但收到 {type(obj).__name__}。{hint}\n"
        "  职责区分: store= 通用 key-based 缓存(universe/fundamentals/industry), "
        "bar_store= K 线月度分片缓存(daily/intraday)。"
    )


class DataLayer:
    """数据层统一访问入口."""

    def __init__(
        self,
        source: DataSource,
        store: BarStore | None = None,
        calendar: Calendar | None = None,
        adjust: AdjustMode = AdjustMode.BACKWARD,
        merge_status_overrides: bool = True,
        bar_store: ShardedBarStore | None = None,
    ):
        """
        source : 数据源 Protocol 实现
        store  : 通用 key-based 缓存（universe / corp_actions / fundamentals / industry）
        bar_store : K 线分片缓存（daily / intraday）. 默认 InMemoryShardedBarStore.
        calendar : 交易日历（默认顺序：调用方 > source.fetch_calendar > get_default_calendar()）
        adjust : 默认复权模式
        merge_status_overrides : daily 末尾是否自动合并 source.fetch_status_overrides 的覆盖列

        Raises:
            TypeError: ``store`` / ``bar_store`` 不满足对应协议。两者职责不同
                且**极易传反** —— 传反了不报错的话, 分片缓存会静默退回
                ``InMemoryShardedBarStore``: 每次运行全量重拉远程(慢且烧配额),
                而错位的 KV store 要等到调 ``universe()`` 才炸 AttributeError。
        """
        self.source = source
        _check_store_arg(store, "store", BarStore, ShardedBarStore, "bar_store")
        _check_store_arg(bar_store, "bar_store", ShardedBarStore, BarStore, "store")
        self.store = store if store is not None else InMemoryBarStore()
        self.bar_store: ShardedBarStore = bar_store if bar_store is not None else InMemoryShardedBarStore()
        if calendar is not None:
            self.calendar = calendar
        else:
            self.calendar = self._resolve_source_calendar(source)
        self.adjust = adjust
        self.merge_status_overrides = merge_status_overrides

    @staticmethod
    def _resolve_source_calendar(source: DataSource) -> Calendar:
        """优先用 source.fetch_calendar；不实现时退回默认日历."""
        fn = getattr(source, "fetch_calendar", None)
        if fn is None:
            return get_default_calendar()
        try:
            return fn()
        except (NotImplementedError, AttributeError):
            return get_default_calendar()

    # ---- Universe -----------------------------------------------------------

    def universe(self, spec: UniverseSpec | str,
                 start: str | pd.Timestamp, end: str | pd.Timestamp) -> Universe:
        if isinstance(spec, str):
            spec = UniverseSpec(spec)
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)

        key = make_cache_key(
            kind="universe", spec=spec.name,
            start=str(start.date()), end=str(end.date()),
            source_version=getattr(self.source, "source_version", "unknown"),
        )
        if self.store.has(key):
            df = self.store.get(key)
        else:
            df = self.source.fetch_universe(spec.name, (start, end))
            self.store.put(key, df)
        return Universe(df, spec)

    # ---- 日线 ---------------------------------------------------------------

    def daily(
        self,
        symbols: list[str],
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        adjust: AdjustMode | None = None,
        validate: bool = True,
    ) -> pd.DataFrame:
        """拉取日线 + 复权处理 + 股本回填.

        走分片缓存 (kind='daily', symbol, year-month): 缺哪些月份从 source 拉哪些.

        Warning:
            返回的 ``DailyBar`` **同时包含** ``close``(后复权) /
            ``close_raw``(不复权) / ``adj_factor``, 其他口径是这三列的**视图**。
            因此 ``adjust`` 参数**不改变返回内容**, 也不应用于切换口径 ——
            切换口径请在读取后用 :func:`~qlab.data.adjust.apply_adjust`。

            传入非默认 ``adjust`` 会直接报错: 它参与分片缓存键但不影响内容,
            默默接受会让同一份数据重复落盘, 且让调用方误以为拿到了别的口径。

        Raises:
            ValueError: ``adjust`` 与 ``DataLayer`` 的默认值不同。
        """
        if adjust is not None and AdjustMode(adjust) != AdjustMode(self.adjust):
            raise ValueError(
                f"daily() 的 adjust 参数无法切换口径(传入 {AdjustMode(adjust).value!r}, "
                f"本层默认 {AdjustMode(self.adjust).value!r})。\n"
                "  原因: DailyBar 同时包含 close/close_raw/adj_factor, 任何口径都能"
                "从这三列推出; adjust 只参与缓存键而不影响内容。\n"
                "  正确做法: 用默认口径取数, 再对结果调"
                "qlab.data.adjust.apply_adjust(df, mode)。\n"
                "  若确实需要整层切换, 请在构造 DataLayer 时传 adjust=。"
            )
        adjust_mode = AdjustMode(adjust if adjust is not None else self.adjust)
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)
        source_version = getattr(self.source, "source_version", "unknown")

        extra_keys = {"freq": Freq.DAILY.value, "adjust": adjust_mode.value}

        # 1) 找缺哪些分片
        missing = self.bar_store.missing_ranges(
            kind="daily", symbols=symbols, start=start, end=end,
            source_version=source_version, **extra_keys,
        )

        # 2) 缺的从 source 拉取（按缺失月份覆盖区间一次性 fetch，避免按月单独 RPC）
        if missing:
            sym_missing = sorted({s for s, _ in missing})
            ym_missing = sorted({ym for _, ym in missing})
            fetch_start = pd.Timestamp(f"{ym_missing[0]}-01")
            fetch_end = (pd.Timestamp(f"{ym_missing[-1]}-01") + pd.offsets.MonthEnd(0)).normalize()
            new_df = self.source.fetch_bars(
                sym_missing, fetch_start, fetch_end,
                Freq.DAILY, adjust_mode,
            )
            if "total_shares" not in new_df.columns:
                try:
                    share_df = self.source.fetch_share_capital(sym_missing, fetch_start, fetch_end)
                    new_df = self._merge_share_capital(new_df, share_df)
                except (NotImplementedError, AttributeError):
                    for col in ("total_shares", "float_shares", "free_float_shares"):
                        new_df[col] = pd.NA
            if self.merge_status_overrides:
                new_df = self._apply_status_overrides(new_df, sym_missing, fetch_start, fetch_end)
            self.bar_store.put_range(
                new_df, kind="daily", source_version=source_version, **extra_keys,
            )

        # 3) 读完整范围
        df = self.bar_store.get_range(
            kind="daily", symbols=symbols, start=start, end=end,
            source_version=source_version, **extra_keys,
        )

        # 空结果(退市股/全区间无数据)跳过 validate: 空表 index 名字缺失是
        # 正常状态, 且无数据无可校验。下游对空结果自行处理。
        if validate and not df.empty:
            validate_schema(df, SCHEMA_DAILY_BAR, strict_index=True)
        return df

    def status_overrides(
        self,
        symbols: list[str],
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
    ) -> pd.DataFrame:
        """显式暴露状态覆盖入口（一般不需要，daily 已自动合并）."""
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)
        try:
            return self.source.fetch_status_overrides(symbols, start, end)
        except (NotImplementedError, AttributeError):
            return pd.DataFrame(columns=["date", "symbol"]).set_index(["date", "symbol"])

    def _apply_status_overrides(
        self,
        bars: pd.DataFrame,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame:
        try:
            overrides = self.source.fetch_status_overrides(symbols, start, end)
        except (NotImplementedError, AttributeError):
            return bars
        if overrides is None or overrides.empty:
            return bars
        # 规范化索引
        if not isinstance(overrides.index, pd.MultiIndex):
            if {"date", "symbol"}.issubset(overrides.columns):
                overrides = overrides.set_index(["date", "symbol"])
            else:
                return bars
        # 覆盖语义：overrides 列存在即用 overrides 的值
        for col in overrides.columns:
            aligned = overrides[col].reindex(bars.index)
            if col in bars.columns:
                bars[col] = aligned.where(aligned.notna(), bars[col])
            else:
                bars[col] = aligned
        return bars

    def _merge_share_capital(self, bars: pd.DataFrame, shares: pd.DataFrame) -> pd.DataFrame:
        if shares.empty:
            for col in ("total_shares", "float_shares", "free_float_shares"):
                bars[col] = pd.NA
            return bars
        # shares 至少含 date/symbol + 股本字段
        if isinstance(shares.index, pd.MultiIndex):
            return bars.join(shares, how="left")
        if {"date", "symbol"}.issubset(shares.columns):
            shares = shares.set_index(["date", "symbol"])
            return bars.join(shares, how="left")
        return bars

    # ---- 日内 ---------------------------------------------------------------

    def intraday(
        self,
        symbols: list[str],
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        freq: Freq = Freq.MIN_30,
        adjust: AdjustMode | None = None,
        validate: bool = True,
    ) -> pd.DataFrame:
        """日内 K 线（分片缓存：kind='intraday'）.

        Warning:
            同 :meth:`daily` —— ``IntradayBar`` 也同时包含复权/不复权/因子三组列,
            ``adjust`` 不影响返回内容。切换口径请用 ``apply_adjust``。

        Raises:
            ValueError: ``adjust`` 与 ``DataLayer`` 默认值不同。
        """
        if adjust is not None and AdjustMode(adjust) != AdjustMode(self.adjust):
            raise ValueError(
                f"intraday() 的 adjust 参数无法切换口径(传入 "
                f"{AdjustMode(adjust).value!r}, 本层默认 "
                f"{AdjustMode(self.adjust).value!r})。\n"
                "  同 daily(): IntradayBar 已含全口径列, adjust 只参与缓存键。\n"
                "  正确做法: 用默认口径取数, 再调 apply_adjust。"
            )
        adjust_mode = AdjustMode(adjust if adjust is not None else self.adjust)
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)
        source_version = getattr(self.source, "source_version", "unknown")
        freq_obj = Freq(freq)
        extra_keys = {"freq": freq_obj.value, "adjust": adjust_mode.value}

        missing = self.bar_store.missing_ranges(
            kind="intraday", symbols=symbols, start=start, end=end,
            source_version=source_version, **extra_keys,
        )

        if missing:
            sym_missing = sorted({s for s, _ in missing})
            ym_missing = sorted({ym for _, ym in missing})
            fetch_start = pd.Timestamp(f"{ym_missing[0]}-01")
            fetch_end = (pd.Timestamp(f"{ym_missing[-1]}-01") + pd.offsets.MonthEnd(0)).normalize()
            new_df = self.source.fetch_bars(
                sym_missing, fetch_start, fetch_end, freq_obj, adjust_mode,
            )
            self.bar_store.put_range(
                new_df, kind="intraday", source_version=source_version, **extra_keys,
            )

        df = self.bar_store.get_range(
            kind="intraday", symbols=symbols, start=start, end=end,
            source_version=source_version, **extra_keys,
        )

        if validate and not df.empty:
            validate_schema(df, SCHEMA_INTRADAY_BAR, strict_index=True)
        return df

    def intraday_aligner(self, freq: Freq = Freq.MIN_30) -> IntradayAligner:
        """返回 lazy load 的 IntradayAligner."""

        def loader(symbols: list[str], start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
            return self.intraday(symbols, start, end, freq=freq, validate=False)

        return IntradayAligner(loader, self.calendar)

    # ---- 财务 ---------------------------------------------------------------

    def fundamentals(
        self,
        symbols: list[str],
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        report_types: list[ReportType] | None = None,
        fields: list[str] | None = None,
        validate: bool = True,
    ) -> pd.DataFrame:
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)

        # 缓存键对齐到月边界: fundamental_as_of/ttm 按日滞动查询时,
        # 相邻交易日的 start/end 仅差一天会让键全 miss(雪崩式重取)。
        # 拉宽到[月首, 月末]: 同月内共享一份; 数据含多取的部分不影响下游 PIT
        # (as_of/ttm 均按 available_at <= date 精确截断)。
        q_start = start.to_period("M").start_time
        q_end = end.to_period("M").end_time.normalize()

        key = make_cache_key(
            kind="fundamentals", symbols=sorted(symbols),
            start=str(q_start.date()), end=str(q_end.date()),
            report_types=sorted([r.value for r in (report_types or [])]),
            fields=sorted(fields) if fields else None,
            source_version=getattr(self.source, "source_version", "unknown"),
        )
        if self.store.has(key):
            df = self.store.get(key)
        else:
            df = self.source.fetch_fundamentals(
                symbols, q_start, q_end, report_types, fields
            )
            self.store.put(key, df)

        if validate and not df.empty:
            validate_schema(df, SCHEMA_FUNDAMENTAL, strict_index=False)
        return df

    def fundamental_as_of(
        self,
        symbols: list[str],
        field: str,
        date: pd.Timestamp,
        lookback_days: int = 600,
    ) -> pd.Series:
        """PIT 查询：date 时刻可见的 field 最新值."""
        date = pd.Timestamp(date)
        start = date - pd.Timedelta(days=lookback_days)
        df = self.fundamentals(symbols, start, date, validate=False)
        return latest_fundamental_as_of(df, field, date, symbols)

    def fundamental_ttm(
        self,
        symbols: list[str],
        field: str,
        date: pd.Timestamp,
        lookback_days: int = 730,
    ) -> pd.Series:
        date = pd.Timestamp(date)
        start = date - pd.Timedelta(days=lookback_days)
        df = self.fundamentals(symbols, start, date, validate=False)
        return ttm_value(df, field, date, symbols)

    # ---- 行业 ---------------------------------------------------------------

    def industry(
        self,
        symbols: list[str],
        date: pd.Timestamp,
        system: str = "sw",
        level: int = 1,
        lookback_days: int = 365 * 5,
    ) -> pd.Series:
        """某日各 symbol 的行业归属.

        本方法查的是**某一天**的归属。若数据源提供 ``industry_asof``(单点查询,
        1 次远程), 优先走它 —— 否则回退到"拉 lookback 区间采样再 PIT 过滤"的
        旧路径(5 年区间约 62 次远程请求)。``lookback_days`` 仅回退路径用。
        """
        date = pd.Timestamp(date)

        # 优先走数据源的单点查询(1 次远程), 避免为"某一天"拉整段历史
        asof = getattr(self.source, "industry_asof", None)
        if callable(asof):
            key = make_cache_key(
                kind="industry_asof", symbols=sorted(symbols),
                date=str(date.date()), system=system, level=level,
                source_version=getattr(self.source, "source_version", "unknown"),
            )
            if self.store.has(key):
                df = self.store.get(key)
            else:
                try:
                    df = asof(symbols, date, system, level)
                except (NotImplementedError, AttributeError):
                    df = None
                if df is not None:
                    self.store.put(key, df)
            if df is not None:
                return industry_as_of(df, symbols, date, system, level)

        start = date - pd.Timedelta(days=lookback_days)
        key = make_cache_key(
            kind="industry", symbols=sorted(symbols),
            start=str(start.date()), end=str(date.date()),
            system=system, level=level,
            source_version=getattr(self.source, "source_version", "unknown"),
        )
        if self.store.has(key):
            df = self.store.get(key)
        else:
            df = self.source.fetch_industry_classification(
                symbols, (start, date), system, level
            )
            self.store.put(key, df)

        return industry_as_of(df, symbols, date, system, level)

    # ---- 概念板块 -----------------------------------------------------------

    def concepts(
        self,
        symbols: list[str],
        date: pd.Timestamp,
        source: str | None = None,
        lookback_days: int = 365 * 5,
    ) -> pd.DataFrame:
        """查询某日各 symbol 所属概念（一对多）.

        返回 DataFrame: columns=[symbol, concept_code, concept_name]

        Args:
            source: 概念来源平台。缺省时向数据源取其 ``concept_source``
                (如 :class:`JQDataSource` 为 ``"jq"``), 数据源未声明则回退 ``"eastmoney"``。
                不硬编码默认值是为了避免把聚宽数据标成东财 —— ``source`` 是输出的
                index 一层, 标错会污染多平台口径并存的语义。
            lookback_days: 仅在数据源**不提供** ``concepts_asof`` 时用于回退路径的
                历史回看窗口。数据源提供 ``concepts_asof`` 时本参数被忽略。

        Note:
            本方法查的是**某一天**的归属。若数据源提供 ``concepts_asof``(单点查询,
            1 次远程), 优先走它 —— 否则回退到"拉 lookback 区间历史再 PIT 过滤"的
            旧路径(5 年区间约 60 次远程请求)。
        """
        if source is None:
            source = getattr(self.source, "concept_source", "eastmoney")
        date = pd.Timestamp(date)

        # 优先走数据源的单点查询(1 次远程), 避免为"某一天"拉整段历史
        asof = getattr(self.source, "concepts_asof", None)
        if callable(asof):
            key = make_cache_key(
                kind="concepts_asof", symbols=sorted(symbols),
                date=str(date.date()), source=source,
                source_version=getattr(self.source, "source_version", "unknown"),
            )
            if self.store.has(key):
                df = self.store.get(key)
            else:
                try:
                    df = asof(symbols, date, source)
                except (NotImplementedError, AttributeError):
                    df = None
                if df is not None:
                    self.store.put(key, df)
            if df is not None:
                return concepts_as_of(df, symbols, date, source)

        start = date - pd.Timedelta(days=lookback_days)
        key = make_cache_key(
            kind="concepts", symbols=sorted(symbols),
            start=str(start.date()), end=str(date.date()),
            source=source,
            source_version=getattr(self.source, "source_version", "unknown"),
        )
        if self.store.has(key):
            df = self.store.get(key)
        else:
            try:
                df = self.source.fetch_concepts(symbols, (start, date), source)
            except (NotImplementedError, AttributeError):
                return pd.DataFrame(columns=["symbol", "concept_code", "concept_name"])
            self.store.put(key, df)

        return concepts_as_of(df, symbols, date, source)

    # ---- 公司行为 -----------------------------------------------------------

    def corporate_actions(
        self,
        symbols: list[str],
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
    ) -> pd.DataFrame:
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)
        key = make_cache_key(
            kind="corp_actions", symbols=sorted(symbols),
            start=str(start.date()), end=str(end.date()),
            source_version=getattr(self.source, "source_version", "unknown"),
        )
        if self.store.has(key):
            return self.store.get(key)
        df = self.source.fetch_corporate_actions(symbols, start, end)
        self.store.put(key, df)
        return df

    # ---- 两融 / 资金流 / 集合竞价（时序类，走分片缓存）---------------------

    def _timeseries(
        self,
        kind: str,
        fetch_fn,
        schema,
        symbols: list[str],
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        validate: bool = True,
    ) -> pd.DataFrame:
        """时序类数据的通用编排: 分片缓存 + 缺失补拉 + schema 校验.

        与 daily() 同构，供 margin_trading / money_flow / call_auction 复用。
        """
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)
        source_version = getattr(self.source, "source_version", "unknown")

        missing = self.bar_store.missing_ranges(
            kind=kind, symbols=symbols, start=start, end=end,
            source_version=source_version,
        )
        if missing:
            sym_missing = sorted({s for s, _ in missing})
            ym_missing = sorted({ym for _, ym in missing})
            fetch_start = pd.Timestamp(f"{ym_missing[0]}-01")
            fetch_end = (pd.Timestamp(f"{ym_missing[-1]}-01") + pd.offsets.MonthEnd(0)).normalize()
            new_df = fetch_fn(sym_missing, fetch_start, fetch_end)
            if new_df is not None and not new_df.empty:
                self.bar_store.put_range(
                    new_df, kind=kind, source_version=source_version,
                )

        df = self.bar_store.get_range(
            kind=kind, symbols=symbols, start=start, end=end,
            source_version=source_version,
        )
        if validate and not df.empty:
            validate_schema(df, schema, strict_index=True)
        return df

    def margin_trading(
        self, symbols: list[str], start: str | pd.Timestamp,
        end: str | pd.Timestamp, validate: bool = True,
    ) -> pd.DataFrame:
        """两融(融资融券). 返回 MarginTrading schema."""
        return self._timeseries(
            "margin_trading", self.source.fetch_margin_trading,
            SCHEMA_MARGIN_TRADING, symbols, start, end, validate,
        )

    def money_flow(
        self, symbols: list[str], start: str | pd.Timestamp,
        end: str | pd.Timestamp, validate: bool = True,
    ) -> pd.DataFrame:
        """资金流向. 返回 MoneyFlow schema."""
        return self._timeseries(
            "money_flow", self.source.fetch_money_flow,
            SCHEMA_MONEY_FLOW, symbols, start, end, validate,
        )

    def call_auction(
        self, symbols: list[str], start: str | pd.Timestamp,
        end: str | pd.Timestamp, validate: bool = True,
    ) -> pd.DataFrame:
        """集合竞价. 返回 CallAuction schema."""
        return self._timeseries(
            "call_auction", self.source.fetch_call_auction,
            SCHEMA_CALL_AUCTION, symbols, start, end, validate,
        )

    # ---- 龙虎榜（非分片，按截止日 + 回看天数）-----------------------------

    def billboard(
        self,
        end_date: pd.Timestamp,
        symbols: list[str] | None = None,
        count: int = 5,
        validate: bool = True,
    ) -> pd.DataFrame:
        """龙虎榜. 返回 Billboard schema(RangeIndex)."""
        end_date = pd.Timestamp(end_date)
        key = make_cache_key(
            kind="billboard",
            symbols=sorted(symbols) if symbols else ["__all__"],
            end=str(end_date.date()), count=count,
            source_version=getattr(self.source, "source_version", "unknown"),
        )
        if self.store.has(key):
            df = self.store.get(key)
        else:
            df = self.source.fetch_billboard(end_date, symbols, count)
            self.store.put(key, df)
        if validate and not df.empty:
            validate_schema(df, SCHEMA_BILLBOARD, strict_index=False)
        return df

    # ---- 因子暴露（时序类，列名动态，key 含 factors）----------------------

    def factor_exposure(
        self,
        symbols: list[str],
        factors: list[str],
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        validate: bool = True,
    ) -> pd.DataFrame:
        """因子暴露. 返回 FactorExposure schema(列名 = 因子名).

        因子集合进入缓存键，不同 factors 组合独立缓存。
        """
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)
        key = make_cache_key(
            kind="factor_exposure", symbols=sorted(symbols),
            factors=sorted(factors),
            start=str(start.date()), end=str(end.date()),
            source_version=getattr(self.source, "source_version", "unknown"),
        )
        if self.store.has(key):
            df = self.store.get(key)
        else:
            df = self.source.fetch_factor_exposure(symbols, factors, start, end)
            self.store.put(key, df)
        if validate and not df.empty:
            validate_schema(df, SCHEMA_FACTOR_EXPOSURE, strict_index=True)
        return df
