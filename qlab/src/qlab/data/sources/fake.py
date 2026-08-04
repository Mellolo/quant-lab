"""FakeDataSource — 合成数据源(``DataSource`` Protocol 的参考实现).

确定性（受 seed 控制），可生成完整 schema 的数据。不接入任何外部 API。

**为何在 src/ 而非 tests/**(已有真实数据源后仍保留的理由):

1. **零配置入口** —— README「快速开始」与 ``examples/end_to_end.py``
   直接用它, 保证无聚宽凭证也能跑通全流程; 移入 tests/ 会打断入门路径。
2. **测试独立性** —— 39 个核心算法测试(三重障碍/净化CV/CUSUM/HRP/PBO 等)
   只需“一份数据”即可, 改用真实源会让单测退化为依赖网络/凭证的集成测试。
3. **抽象校验器** —— 与 :class:`JQDataSource` 两个独立实现互相印证,
   是分辨“``DataSource`` 抽象漏了假设”还是“某个实现有 bug”的唯一手段
   (实例: ``industry_as_of`` 的 KeyError 靠 fake 源才确认是既有缺陷)。

Note:
    schema 变更时**本文件需同步**。这不是负担而是保障: 两个实现互相校验,
    漏改会被测试当场拦住, 而非运行时才爆。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from qlab.core.calendar import get_default_calendar
from qlab.core.enums import AdjustMode, Freq, ReportType


class FakeDataSource:
    """合成 A 股数据源."""

    source_version = "fake-v1"

    def __init__(self, seed: int = 42, n_symbols: int = 50,
                 start_year: int = 2018, base_price: float = 10.0,
                 history_end_year: int | None = None):
        self.seed = seed
        self.n_symbols = n_symbols
        self.start_year = start_year
        self.base_price = base_price
        # 历史终点（用于一次性预生成完整价格序列，保证 PIT 一致性）
        # 注意：不能超过 exchange_calendars XSHG 已知的最远日期
        self.history_end_year = history_end_year or pd.Timestamp.today().year
        self._rng = np.random.default_rng(seed)
        self._cal = get_default_calendar()
        self._symbols = self._generate_symbol_list()
        # 懒加载缓存：symbol → 完整历史 DataFrame
        # 关键：同一 symbol 的同一日期，无论何时取，价格/状态都唯一
        self._daily_cache: dict[str, pd.DataFrame] = {}

    def _generate_symbol_list(self) -> list[str]:
        """生成随机但确定的股票代码列表."""
        rng = np.random.default_rng(self.seed)
        symbols = []
        for _i in range(self.n_symbols):
            exchange = rng.choice(["SH", "SZ"])
            if exchange == "SH":
                code = f"6{rng.integers(0, 100000):05d}"
            else:
                code = f"00{rng.integers(0, 10000):04d}"
            symbols.append(f"{code}.{exchange}")
        return sorted(set(symbols))[:self.n_symbols]

    @property
    def all_symbols(self) -> list[str]:
        return self._symbols.copy()

    # ============================================================
    # fetch_bars
    # ============================================================

    def fetch_bars(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
        freq: Freq = Freq.DAILY,
        adjust: AdjustMode = AdjustMode.BACKWARD,
    ) -> pd.DataFrame:
        if Freq(freq) == Freq.DAILY:
            return self._fetch_daily(symbols, start, end, adjust)
        return self._fetch_intraday(symbols, start, end, freq, adjust)

    def _generate_full_history(self, symbol: str) -> pd.DataFrame:
        """一次性生成 [start_year-01-01, history_end_year-12-31] 的完整历史.

        关键性质：对同一 symbol，相同 seed 下产出完全一致；
        任何后续 fetch_bars 只是从中切片，绝不重新生成。
        这保证了"同一 (symbol, date) 价格唯一"——PIT 一致性的最低要求。
        """
        sym_seed = (self.seed + hash(symbol)) & 0xFFFFFFFF
        rng = np.random.default_rng(sym_seed)

        epoch_start = pd.Timestamp(f"{self.start_year}-01-01")
        epoch_end = pd.Timestamp(f"{self.history_end_year}-12-31")
        dates = self._cal.trading_days(epoch_start, epoch_end)
        n = len(dates)
        if n == 0:
            return pd.DataFrame()

        # 模拟价格：几何布朗运动 + 偶尔的跳跃
        log_returns = rng.normal(0.0003, 0.02, n)
        jumps = rng.choice([0, 0, 0, 0.05, -0.05], n, p=[0.96, 0.01, 0.01, 0.01, 0.01])
        log_returns += jumps
        cumret = np.exp(log_returns.cumsum())
        close_raw = self.base_price * cumret

        # OHLV
        daily_vol = rng.uniform(0.005, 0.03, n)
        high_raw = close_raw * (1 + np.abs(rng.normal(0, daily_vol)))
        low_raw = close_raw * (1 - np.abs(rng.normal(0, daily_vol)))
        open_raw = close_raw / (1 + rng.normal(0, daily_vol))
        high_raw = np.maximum.reduce([high_raw, close_raw, open_raw])
        low_raw = np.minimum.reduce([low_raw, close_raw, open_raw])

        volume = rng.integers(1_000_000, 50_000_000, n).astype(np.int64)
        amount = volume * close_raw

        # 复权因子：偶尔有除权事件
        adj_factor = np.ones(n)
        for div_event in rng.choice(n, max(1, n // 250), replace=False):
            adj_factor[div_event:] *= rng.uniform(1.005, 1.02)

        # 状态
        is_suspended = rng.random(n) < 0.005
        is_limit_up = rng.random(n) < 0.01
        is_limit_down = rng.random(n) < 0.005
        is_limit_down = is_limit_down & ~is_limit_up
        is_st_scalar = rng.random() < 0.05
        is_st_arr = np.full(n, is_st_scalar)

        listing_offset = int(rng.integers(100, 5000))
        days_since_listing = np.arange(
            listing_offset, listing_offset + n
        ).astype(np.int32)

        # 股本
        base_shares = rng.integers(1e8, 5e9)
        total_shares = np.full(n, base_shares, dtype=np.int64)
        float_shares = (total_shares * rng.uniform(0.4, 0.9)).astype(np.int64)
        free_float_shares = (float_shares * rng.uniform(0.6, 0.95)).astype(np.int64)

        # 涨跌停参考价（不复权）
        limit_pct = 0.05 if is_st_scalar else 0.1
        prev_close_raw = np.roll(close_raw, 1)
        prev_close_raw[0] = close_raw[0]
        limit_up_price = np.round(prev_close_raw * (1 + limit_pct), 2)
        limit_down_price = np.round(prev_close_raw * (1 - limit_pct), 2)

        # 不变量对齐
        close_raw = np.where(is_limit_up, limit_up_price, close_raw)
        close_raw = np.where(is_limit_down, limit_down_price, close_raw)
        high_raw = np.maximum(high_raw, close_raw)
        low_raw = np.minimum(low_raw, close_raw)
        open_raw = np.clip(open_raw, low_raw, high_raw)
        amount = volume * close_raw

        # 装配 DataFrame（按日期 × 单 symbol）
        idx = pd.MultiIndex.from_product([dates, [symbol]], names=["date", "symbol"])
        close_adj = close_raw * adj_factor
        open_adj = open_raw * adj_factor
        high_adj = high_raw * adj_factor
        low_adj = low_raw * adj_factor
        vwap_adj = np.where(volume > 0, amount / np.maximum(volume, 1), 0.0) * adj_factor

        df = pd.DataFrame({
            "open": open_adj, "high": high_adj, "low": low_adj,
            "close": close_adj, "vwap": vwap_adj,
            "open_raw": open_raw, "high_raw": high_raw, "low_raw": low_raw,
            "close_raw": close_raw,
            "volume": volume, "amount": amount,
            "adj_factor": adj_factor,
            "limit_up_price": limit_up_price, "limit_down_price": limit_down_price,
            "is_suspended": is_suspended,
            "is_limit_up": is_limit_up & ~is_suspended,
            "is_limit_down": is_limit_down & ~is_suspended,
            "is_st": is_st_arr,
            "days_since_listing": days_since_listing,
            "total_shares": total_shares,
            "float_shares": float_shares,
            "free_float_shares": free_float_shares,
        }, index=idx)

        # 停牌日：价格 NaN，volume/amount 清零
        sus_mask = df["is_suspended"]
        for col in ("open", "high", "low", "close", "vwap",
                    "open_raw", "high_raw", "low_raw", "close_raw"):
            df.loc[sus_mask, col] = np.nan
        df.loc[sus_mask, "volume"] = 0
        df.loc[sus_mask, "amount"] = 0.0

        return df

    def _get_or_generate_history(self, symbol: str) -> pd.DataFrame:
        if symbol not in self._daily_cache:
            self._daily_cache[symbol] = self._generate_full_history(symbol)
        return self._daily_cache[symbol]

    def _fetch_daily(self, symbols: list[str], start: pd.Timestamp,
                     end: pd.Timestamp, adjust: AdjustMode) -> pd.DataFrame:
        dates = self._cal.trading_days(start, end)
        if len(dates) == 0:
            cols = SCHEMA_FAKE_DAILY_COLS
            return pd.DataFrame(columns=cols, index=pd.MultiIndex.from_tuples(
                [], names=["date", "symbol"]))

        parts = []
        for symbol in symbols:
            hist = self._get_or_generate_history(symbol)
            if hist.empty:
                continue
            sub = hist.loc[(slice(dates[0], dates[-1]), symbol), :]
            parts.append(sub)
        if not parts:
            cols = SCHEMA_FAKE_DAILY_COLS
            return pd.DataFrame(columns=cols, index=pd.MultiIndex.from_tuples(
                [], names=["date", "symbol"]))
        df = pd.concat(parts).sort_index()
        return df

    def _fetch_intraday(self, symbols: list[str], start: pd.Timestamp,
                        end: pd.Timestamp, freq: Freq, adjust: AdjustMode) -> pd.DataFrame:
        freq_minutes = {
            Freq.MIN_1: 1, Freq.MIN_5: 5, Freq.MIN_15: 15,
            Freq.MIN_30: 30, Freq.MIN_60: 60,
        }[Freq(freq)]
        # A 股每个交易日 4 小时 = 240 分钟
        n_bars_per_day = 240 // freq_minutes
        dates = self._cal.trading_days(start, end)

        rows = []
        for symbol in symbols:
            sym_seed = (self.seed + hash(symbol) + freq_minutes) & 0xFFFFFFFF
            rng = np.random.default_rng(sym_seed)
            running_price = self.base_price
            for date in dates:
                sessions_per_day = n_bars_per_day
                # 简化：bar 时间均匀分布在 9:30-11:30 + 13:00-15:00
                bar_times = []
                morning_bars = sessions_per_day // 2
                afternoon_bars = sessions_per_day - morning_bars
                for i in range(morning_bars):
                    bar_times.append(date + pd.Timedelta(hours=9, minutes=30 + i * freq_minutes))
                for i in range(afternoon_bars):
                    bar_times.append(date + pd.Timedelta(hours=13, minutes=i * freq_minutes))

                for ts in bar_times:
                    ret = rng.normal(0, 0.003)
                    running_price *= (1 + ret)
                    high = running_price * (1 + abs(rng.normal(0, 0.002)))
                    low = running_price * (1 - abs(rng.normal(0, 0.002)))
                    open_ = running_price / (1 + rng.normal(0, 0.002))
                    high = max(high, running_price, open_)
                    low = min(low, running_price, open_)
                    volume = int(rng.integers(10_000, 1_000_000))
                    amount = volume * running_price

                    hour = ts.hour
                    minute = ts.minute
                    if hour == 9 and minute < 30:
                        session = "open_auction"
                    elif hour == 14 and minute >= 57:
                        session = "close_auction"
                    elif hour < 12:
                        session = "morning"
                    else:
                        session = "afternoon"

                    rows.append({
                        "timestamp": ts, "symbol": symbol,
                        "open": float(open_), "high": float(high),
                        "low": float(low), "close": float(running_price),
                        "vwap": float(running_price),
                        "open_raw": float(open_), "high_raw": float(high),
                        "low_raw": float(low), "close_raw": float(running_price),
                        "volume": volume, "amount": float(amount),
                        "adj_factor": 1.0,
                        "session": session,
                    })

        df = pd.DataFrame(rows).set_index(["timestamp", "symbol"]).sort_index()
        return df

    # ============================================================
    # fetch_universe
    # ============================================================

    def fetch_universe(self, spec: str,
                       date_range: tuple[pd.Timestamp, pd.Timestamp]) -> pd.DataFrame:
        start, end = date_range
        dates = self._cal.trading_days(start, end)

        # 简化：所有合成股票都在 universe 内
        size_limit = {
            "csi300": 30, "csi500": 50, "csi800": 80,
            "csi1000": 100, "all_a": self.n_symbols,
        }
        n_in = size_limit.get(spec.split(":")[0], self.n_symbols)
        in_symbols = self._symbols[:n_in]

        rows = []
        equal_weight = 1.0 / len(in_symbols)
        for date in dates:
            for symbol in self._symbols:
                in_u = symbol in in_symbols
                rows.append({
                    "date": date, "symbol": symbol,
                    "in_universe": in_u,
                    "weight": equal_weight if in_u else np.nan,
                })

        df = pd.DataFrame(rows).set_index(["date", "symbol"]).sort_index()
        return df

    # ============================================================
    # fetch_corporate_actions
    # ============================================================

    def fetch_corporate_actions(self, symbols: list[str],
                                start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        dates = self._cal.trading_days(start, end)
        rows = []
        for symbol in symbols:
            sym_seed = (self.seed + hash(symbol) + 1) & 0xFFFFFFFF
            rng = np.random.default_rng(sym_seed)
            n_events = rng.integers(0, max(1, len(dates) // 250 + 1))
            event_indices = rng.choice(len(dates), n_events, replace=False) if n_events else []
            for idx in event_indices:
                ex_date = pd.Timestamp(dates[idx])
                rows.append({
                    "symbol": symbol,
                    "action_type": "cash_dividend",
                    "announce_date": ex_date - pd.Timedelta(days=30),
                    "ex_date": ex_date,
                    "record_date": ex_date - pd.Timedelta(days=1),
                    "pay_date": ex_date + pd.Timedelta(days=7),
                    "cash_per_share": float(rng.uniform(0.1, 1.0)),
                    "bonus_share_ratio": 0.0,
                    "transfer_share_ratio": 0.0,
                    "rights_share_ratio": 0.0,
                    "rights_price": 0.0,
                })
        if not rows:
            return pd.DataFrame(columns=[
                "symbol", "action_type", "announce_date", "ex_date",
                "record_date", "pay_date",
                "cash_per_share", "bonus_share_ratio", "transfer_share_ratio",
                "rights_share_ratio", "rights_price",
            ])
        return pd.DataFrame(rows)

    # ============================================================
    # fetch_fundamentals
    # ============================================================

    def fetch_fundamentals(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
        report_types: list[ReportType] | None = None,
        fields: list[str] | None = None,
    ) -> pd.DataFrame:
        report_types = report_types or [ReportType.OFFICIAL, ReportType.FLASH, ReportType.FORECAST]
        rows = []
        for symbol in symbols:
            sym_seed = (self.seed + hash(symbol) + 2) & 0xFFFFFFFF
            rng = np.random.default_rng(sym_seed)

            year = pd.Timestamp(start).year
            base_revenue = float(rng.uniform(1e8, 1e10))
            base_profit = base_revenue * rng.uniform(0.05, 0.2)
            base_assets = base_revenue * rng.uniform(2, 5)

            while year <= pd.Timestamp(end).year:
                for quarter in (1, 2, 3, 4):
                    report_period = pd.Timestamp(f"{year}-{quarter*3:02d}-{30 if quarter*3 != 12 else 31}")
                    if report_period > pd.Timestamp(end):
                        continue

                    # 各披露类型的披露日
                    days_after = {
                        ReportType.FORECAST: rng.integers(1, 16),
                        ReportType.FLASH: rng.integers(30, 60),
                        ReportType.OFFICIAL: rng.integers(60, 120) if quarter == 4 else rng.integers(30, 60),
                    }

                    # 模拟轻微增长
                    growth = 1 + rng.normal(0.1, 0.05)
                    quarterly_revenue = base_revenue * quarter / 4 * growth
                    quarterly_profit = base_profit * quarter / 4 * growth

                    for rt in report_types:
                        announce_date = report_period + pd.Timedelta(days=int(days_after[rt]))
                        if announce_date > pd.Timestamp(end):
                            continue
                        available_at = announce_date + pd.Timedelta(hours=9, minutes=30)

                        row = {
                            "report_period": report_period,
                            "symbol": symbol,
                            "report_type": rt.value,
                            "announce_date": announce_date,
                            "available_at": available_at,
                            "fiscal_year": int(year),
                            "fiscal_quarter": int(quarter),
                            "source": "fake",
                        }

                        if rt == ReportType.FORECAST:
                            row["forecast_net_profit_min"] = quarterly_profit * 0.9
                            row["forecast_net_profit_max"] = quarterly_profit * 1.1
                            row["forecast_type"] = "preincrease" if growth > 1 else "predecrease"
                            row["forecast_change_pct_min"] = float((growth - 1) * 100 - 5)
                            row["forecast_change_pct_max"] = float((growth - 1) * 100 + 5)
                        else:
                            # 正式 / 快报：填业务字段
                            row["total_assets"] = base_assets * growth
                            row["total_liabilities"] = base_assets * 0.5 * growth
                            row["total_equity"] = base_assets * 0.5 * growth
                            row["equity_to_shareholders"] = base_assets * 0.45 * growth
                            row["cash_and_equivalents"] = base_assets * 0.1 * growth
                            row["current_assets"] = base_assets * 0.4 * growth
                            row["current_liabilities"] = base_assets * 0.3 * growth
                            row["total_revenue"] = quarterly_revenue
                            row["operating_revenue"] = quarterly_revenue
                            row["operating_cost"] = quarterly_revenue * 0.7
                            row["operating_profit"] = quarterly_revenue * 0.15
                            row["net_profit"] = quarterly_profit
                            row["net_profit_to_shareholders"] = quarterly_profit
                            row["net_profit_excl_nonrecurring"] = quarterly_profit * 0.95
                            row["eps_basic"] = float(quarterly_profit / 1e8)
                            row["eps_diluted"] = float(quarterly_profit / 1e8)
                            row["operating_cash_flow"] = quarterly_profit * 0.8
                            row["investing_cash_flow"] = -quarterly_profit * 0.3
                            row["financing_cash_flow"] = quarterly_profit * 0.1

                        rows.append(row)
                year += 1
        if not rows:
            cols = list(SCHEMA_FUNDAMENTAL_COLS)
            return pd.DataFrame(columns=cols)
        df = pd.DataFrame(rows)
        # 字段筛选
        if fields:
            keep = list(set(fields) | {
                "report_period", "symbol", "report_type", "announce_date",
                "available_at", "fiscal_year", "fiscal_quarter",
            })
            df = df[[c for c in keep if c in df.columns]]
        return df

    # ============================================================
    # fetch_industry_classification
    # ============================================================

    def fetch_industry_classification(
        self,
        symbols: list[str],
        date_range: tuple[pd.Timestamp, pd.Timestamp],
        system: str = "sw",
        level: int = 1,
    ) -> pd.DataFrame:
        industries = [
            ("801010", "农林牧渔"), ("801030", "基础化工"), ("801050", "有色金属"),
            ("801080", "电子"), ("801120", "食品饮料"), ("801150", "医药生物"),
            ("801180", "房地产"), ("801200", "商贸零售"), ("801710", "建筑材料"),
            ("801780", "银行"), ("801790", "非银金融"), ("801880", "汽车"),
        ]
        start, end = date_range
        rows = []
        for symbol in symbols:
            rng = np.random.default_rng((self.seed + hash(symbol) + 3) & 0xFFFFFFFF)
            code, name = industries[rng.integers(0, len(industries))]
            # 起始日入行业
            rows.append({
                "date": pd.Timestamp(start), "symbol": symbol,
                "system": system, "level": int(level),
                "industry_code": code, "industry_name": name,
                "parent_code": None,
            })
        df = pd.DataFrame(rows).set_index(["date", "symbol", "system"]).sort_index()
        return df

    # ============================================================
    # fetch_concepts
    # ============================================================

    def fetch_concepts(
        self,
        symbols: list[str],
        date_range: tuple[pd.Timestamp, pd.Timestamp],
        source: str = "eastmoney",
    ) -> pd.DataFrame:
        concepts_pool = [
            ("BK0001", "光伏"), ("BK0002", "AI"), ("BK0003", "新能源车"),
            ("BK0004", "半导体"), ("BK0005", "白酒"), ("BK0006", "医美"),
            ("BK0007", "锂电池"), ("BK0008", "储能"), ("BK0009", "ChatGPT"),
            ("BK0010", "机器人"), ("BK0011", "数据要素"), ("BK0012", "低空经济"),
        ]
        start, end = date_range
        rows = []
        for symbol in symbols:
            rng = np.random.default_rng((self.seed + hash(symbol) + 4) & 0xFFFFFFFF)
            n_concepts = int(rng.integers(1, 4))
            chosen = rng.choice(len(concepts_pool), n_concepts, replace=False)
            for idx in chosen:
                code, name = concepts_pool[idx]
                rows.append({
                    "effective_date": pd.Timestamp(start),
                    "symbol": symbol,
                    "source": source,
                    "concept_code": code,
                    "concept_name": name,
                    "expired_date": pd.NaT,
                })
        if not rows:
            return pd.DataFrame(columns=[
                "effective_date", "symbol", "source",
                "concept_code", "concept_name", "expired_date",
            ])
        df = pd.DataFrame(rows).set_index(["effective_date", "symbol", "source"]).sort_index()
        return df

    # ============================================================
    # fetch_share_capital
    # ============================================================

    def fetch_share_capital(self, symbols: list[str],
                            start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """股本数据已直接在 fetch_bars 里返回，此接口仅为契约完整性."""
        return pd.DataFrame(columns=[
            "date", "symbol", "total_shares", "float_shares", "free_float_shares",
        ])

    # ============================================================
    # fetch_calendar
    # ============================================================

    def fetch_calendar(self, name: str = "SSE"):
        """Fake source 直接复用默认 A 股日历."""
        return self._cal

    # ============================================================
    # fetch_status_overrides
    # ============================================================

    def fetch_status_overrides(self, symbols: list[str],
                               start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """fake source 的状态已经全部在 fetch_bars 里返回，无需覆盖."""
        return pd.DataFrame(columns=["date", "symbol"]).set_index(["date", "symbol"])

    def _biz_days(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
        cal = get_default_calendar()
        return cal.trading_days(pd.Timestamp(start), pd.Timestamp(end))

    def _avail_next(self, day: pd.Timestamp) -> pd.Timestamp:
        """下一交易日 09:30 —— 两融/资金流的 available_at(T 日收盘后发布)."""
        cal = get_default_calendar()
        return cal.next_trading_day(pd.Timestamp(day)) + pd.Timedelta(
            hours=9, minutes=30
        )

    def fetch_margin_trading(self, symbols: list[str],
                             start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """合成两融数据. 返回 MarginTrading schema."""
        days = self._biz_days(start, end)
        rows = []
        for i, sym in enumerate(dict.fromkeys(symbols)):
            base = 1e9 * (i + 1)
            for j, d in enumerate(days):
                rows.append((d, sym, self._avail_next(d), base + j * 1e6, 2e7, 1.8e7,
                             1e5, 5e3, 4e3, base + j * 1e6 + 1e8))
        if not rows:
            return pd.DataFrame()
        cols = ["date", "symbol", "available_at", "fin_balance", "fin_buy",
                "fin_repay", "sec_balance", "sec_sell", "sec_repay", "total_balance"]
        return pd.DataFrame(rows, columns=cols).set_index(["date", "symbol"])

    def fetch_money_flow(self, symbols: list[str],
                         start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """合成资金流向. 返回 MoneyFlow schema(金额为元)."""
        days = self._biz_days(start, end)
        rows = []
        for sym in dict.fromkeys(symbols):
            for j, d in enumerate(days):
                xl, lg = 3e6 * (1 if j % 2 else -1), 1e6
                main = xl + lg  # 主力 = 超大单 + 大单(满足不变量)
                rows.append((d, sym, self._avail_next(d), 0.5, main, 4.0, xl, 3.0,
                             lg, 1.0, -2e6, -1.5, -main + 2e6, -3.5))
        if not rows:
            return pd.DataFrame()
        cols = ["date", "symbol", "available_at", "change_pct",
                "net_amount_main", "net_pct_main",
                "net_amount_xl", "net_pct_xl", "net_amount_l", "net_pct_l",
                "net_amount_m", "net_pct_m", "net_amount_s", "net_pct_s"]
        return pd.DataFrame(rows, columns=cols).set_index(["date", "symbol"])

    def fetch_call_auction(self, symbols: list[str],
                           start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """合成集合竞价. 返回 CallAuction schema."""
        days = self._biz_days(start, end)
        rows = []
        for i, sym in enumerate(dict.fromkeys(symbols)):
            px = 10.0 * (i + 1)
            for d in days:
                book = []
                for k in range(1, 6):
                    book += [px + k * 0.01, 100.0 * k, px - k * 0.01, 100.0 * k]
                # 竞价: **当日** 09:30 即可用(09:25 竞价结束即确定)
                avail = pd.Timestamp(d) + pd.Timedelta(hours=9, minutes=30)
                rows.append((d, sym, avail, px, 1e4, px * 1e4, *book))
        if not rows:
            return pd.DataFrame()
        book_cols = [f"{s}{k}_{v}" for k in range(1, 6)
                     for s, v in [("ask", "price"), ("ask", "volume"),
                                  ("bid", "price"), ("bid", "volume")]]
        cols = ["date", "symbol", "available_at", "auction_price", "auction_volume",
                "auction_amount", *book_cols]
        return pd.DataFrame(rows, columns=cols).set_index(["date", "symbol"])

    def fetch_billboard(self, end_date: pd.Timestamp,
                        symbols: list[str] | None = None, count: int = 5) -> pd.DataFrame:
        """合成龙虎榜. 返回 Billboard schema(RangeIndex)."""
        end_date = pd.Timestamp(end_date)
        syms = symbols or (self.fetch_universe("all", (end_date, end_date))
                           .index.get_level_values("symbol").unique().tolist()[:3])
        rows = []
        for sym in syms:
            for direction in ("BUY", "SELL"):
                bv = 1e7 if direction == "BUY" else 0.0
                sv = 0.0 if direction == "BUY" else 8e6
                rows.append({
                    "date": end_date, "symbol": sym, "direction": direction,
                    "rank": 1, "abnormal_code": "106", "abnormal_name": "异动",
                    "sales_depart": "某营业部", "buy_value": bv, "buy_rate": 5.0,
                    "sell_value": sv, "sell_rate": 3.0, "total_value": bv + sv,
                    "net_value": bv - sv, "amount": 5e7,
                })
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def fetch_factor_exposure(self, symbols: list[str], factors: list[str],
                              start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """合成因子暴露. 返回 FactorExposure schema(列名 = 因子名)."""
        days = self._biz_days(start, end)
        idx = pd.MultiIndex.from_product(
            [days, list(dict.fromkeys(symbols))], names=["date", "symbol"]
        )
        rng = np.random.default_rng(self.seed)
        data = {f: rng.standard_normal(len(idx)) for f in factors}
        return pd.DataFrame(data, index=idx)


# 字段列表常量（避免循环导入）
SCHEMA_FAKE_DAILY_COLS = [
    "open", "high", "low", "close", "vwap",
    "open_raw", "high_raw", "low_raw", "close_raw",
    "volume", "amount", "adj_factor",
    "limit_up_price", "limit_down_price",
    "is_suspended", "is_limit_up", "is_limit_down", "is_st",
    "days_since_listing",
    "total_shares", "float_shares", "free_float_shares",
]

SCHEMA_FUNDAMENTAL_COLS = [
    "report_period", "symbol", "report_type", "announce_date", "available_at",
    "fiscal_year", "fiscal_quarter", "source",
    "total_assets", "total_liabilities", "total_equity", "equity_to_shareholders",
    "cash_and_equivalents", "current_assets", "current_liabilities",
    "total_revenue", "operating_revenue", "operating_cost", "operating_profit",
    "net_profit", "net_profit_to_shareholders", "net_profit_excl_nonrecurring",
    "eps_basic", "eps_diluted",
    "operating_cash_flow", "investing_cash_flow", "financing_cash_flow",
    "forecast_net_profit_min", "forecast_net_profit_max",
    "forecast_type", "forecast_change_pct_min", "forecast_change_pct_max",
]
