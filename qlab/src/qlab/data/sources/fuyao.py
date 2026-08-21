"""FuyaoDataSource — 同花顺扶摇 REST（fuyao.aicubes.cn）.

职责：HTTP 信封拆包 + 字段翻译成 qlab schema。不做落盘缓存
（那是 DataLayer 的 store / bar_store）。

鉴权：请求头 ``X-api-key``。密钥只从构造参数或环境变量 ``FUYAO_API_KEY``
读取，**不要**写进代码或提交到 git。

文档：https://fuyao.aicubes.cn/llms-full.txt
Base URL：https://fuyao.aicubes.cn

覆盖该站全部 REST 分组：价格、元信息、除复权、财务、日历、集合竞价、
特色数据、指数、基金、全市场 dump。

Protocol 里对不上或会静默写错历史的，直接 ``DataUnavailableError``：
宇宙 PIT、席位级龙虎榜、历史集合竞价、热榜历史、基金持仓/新闻历史、
两融 / 资金流 / 因子暴露 / 指数估值。当前截面用 ``current_universe`` /
``dragon_tiger_list`` / ``auction_snapshot``。
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import numpy as np
import pandas as pd

from qlab.core.calendar import Calendar, get_default_calendar
from qlab.core.enums import AdjustMode, Freq, ReportType
from qlab.core.exceptions import DataSourceError, DataUnavailableError

__all__ = [
    "DEFAULT_BASE_URL",
    "ENV_API_KEY",
    "FuyaoAPIError",
    "FuyaoClient",
    "FuyaoDataSource",
]

DEFAULT_BASE_URL = "https://fuyao.aicubes.cn"
ENV_API_KEY = "FUYAO_API_KEY"

_INDEX_UNIVERSE = {
    "csi300": "000300.SH",
    "hs300": "000300.SH",
    "csi500": "000905.SH",
    "zz500": "000905.SH",
    "csi800": "000906.SH",
    "zz800": "000906.SH",
    "csi1000": "000852.SH",
    "zz1000": "000852.SH",
    "cyb": "399006.SZ",
}

_PRICE_COLS = ("open", "high", "low", "close", "vwap")
_RAW_PRICE_COLS = ("open_raw", "high_raw", "low_raw", "close_raw")

# 每个公开 REST 叶子的时间语义。coming-soon（指数概况 / 股票基础信息 /
# 个股所属板块反查）线上 404，不列入。
API_TIME = {
    # 真历史序列 / 可按日回看
    "prices_historical": "history ≤10y, interval=1d",
    "index_prices_historical": "history ≤10y, interval=1d",
    "fund_market_historical": "ETF history ≤5y, interval=1d",
    "dump_download_url": "full-market parquet: 10y daily-k / last 10d / adj factors",
    "adjustment_factors": "event history (from/to dates)",
    "income_statements": "multi-period (limit or start/end ≤10y)",
    "balance_sheets": "multi-period (limit or start/end ≤10y)",
    "cash_flow_statements": "multi-period (limit or start/end ≤10y)",
    "financial_indicators": "one report period, e.g. 2025-1",
    "trading_days": "rolling last 1 year only",
    "limit_up_pool": "one trade date via date_ms; omit = today",
    "limit_down_pool": "one trade date via date_ms; omit = today",
    "limit_break_pool": "one trade date via date_ms; omit = today",
    "limit_up_ladder": "last 30 trade days (fixed window)",
    "dragon_tiger_list": "one trade date; omit = today",
    "hot_stock_list_history": "DISABLED: echoes date, list frozen",
    "hot_stock_rank_trend": "rank path over [start_date, end_date]",
    "fund_nav": "NAV series by range (week…fyear); default latest if omitted",
    "fund_indicators_historical": "daily RSI/Donchian series (start/end)",
    "fund_stock_history": "DISABLED: vendor item=[] even on official examples",
    "fund_bond_history": "DISABLED: vendor item=[] even on official examples",
    "fund_dividends": "dividend event history",
    "fund_income_statements": "multi-period fund financials",
    "fund_balance_sheets": "multi-period fund financials",
    "fund_financial_indicators": "multi-period fund financials",
    # 当天 / 当前快照（没有历史轴）
    "prices_snapshot": "latest quote",
    "index_prices_snapshot": "latest quote",
    "valuations_snapshot": "latest PE/PB/PS; PCF 勿与 jq 对",
    "auction_snapshot": "today auction",
    "auction_short_term_benchmark": "today short-term auction bench",
    "anomaly_analysis_list": "today anomaly list",
    "anomaly_analysis_stock": "today per-stock anomaly",
    "skyrocket_list": "today heat board (period=day|hour)",
    "hot_stock_list": "today heat board (period=day|hour)",
    "index_catalog": "current THS index catalog",
    "index_constituents": "current constituents (not PIT)",
    "search_tickers": "current ticker search",
    "list_tickers": "current ticker directory",
}


class FuyaoAPIError(DataSourceError):
    """扶摇业务信封非 0（HTTP 仍可能是 200）。"""

    def __init__(self, code: int, message: str, request_id: str | None = None):
        extra = f" request_id={request_id}" if request_id else ""
        super().__init__(f"扶摇 API code={code}: {message}{extra}")
        self.code = int(code)
        self.request_id = request_id


class FuyaoClient:
    """薄 HTTP 客户端。返回信封里的 ``data``。"""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 30.0,
        retries: int = 3,
    ) -> None:
        key = api_key or os.environ.get(ENV_API_KEY)
        if not key:
            raise ValueError(
                f"缺少扶摇 API Key。构造时传入 api_key，或设环境变量 {ENV_API_KEY}。"
            )
        self.api_key = key
        self.base_url = base_url.rstrip("/")
        self.timeout = float(timeout)
        self.retries = max(1, int(retries))

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        query = urllib.parse.urlencode(
            {k: _param(v) for k, v in params.items() if v is not None}
        )
        url = self.base_url + path + (("?" + query) if query else "")
        last: Exception | None = None
        for attempt in range(self.retries):
            req = urllib.request.Request(url, headers={"X-api-key": self.api_key})
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    raw = json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                last = DataSourceError(f"扶摇 HTTP {exc.code} {path}: {body[:240]}")
                if exc.code in (429, 500, 502, 503, 504) and attempt + 1 < self.retries:
                    time.sleep(0.4 * (attempt + 1))
                    continue
                raise last from exc
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
                last = DataSourceError(f"扶摇网络失败 {path}: {exc}")
                if attempt + 1 < self.retries:
                    time.sleep(0.4 * (attempt + 1))
                    continue
                raise last from exc
            if not isinstance(raw, dict):
                raise DataSourceError(f"扶摇返回非 JSON 对象: {path}")
            code = int(raw.get("code", -1))
            if code == 4001 and attempt + 1 < self.retries:
                time.sleep(0.8 * (attempt + 1))
                continue
            if code != 0:
                raise FuyaoAPIError(code, str(raw.get("message", "")), raw.get("request_id"))
            data = raw.get("data")
            return data if isinstance(data, dict) else {}
        raise last or DataSourceError(f"扶摇请求失败 {path}")

    def items(self, path: str, **params: Any) -> list[dict[str, Any]]:
        data = self.get(path, **params)
        item = data.get("item")
        if item is None:
            return []
        if isinstance(item, list):
            return [x for x in item if isinstance(x, dict)]
        if isinstance(item, dict):
            return [item]
        return []


def _param(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        return ",".join(str(x) for x in value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _ms_to_ts(ms: object) -> pd.Timestamp:
    return (
        pd.Timestamp(int(ms), unit="ms", tz="Asia/Shanghai")
        .tz_localize(None)
        .normalize()
    )


def _to_ms(ts: object) -> int:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("Asia/Shanghai")
    return int(t.timestamp() * 1000)


def _extract_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("item", "stock_items", "holdings", "stocks", "list", "records"):
        item = data.get(key)
        if isinstance(item, list):
            return [x for x in item if isinstance(x, dict)]
        if isinstance(item, dict):
            return [item]
    return []


def _items_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "thscode" in df.columns and "symbol" not in df.columns:
        df["symbol"] = df["thscode"].astype(str)
    for col in list(df.columns):
        if col.endswith("_ms"):
            dest = col[:-3]
            if dest not in df.columns or df[dest].dtype == object:
                df[dest] = [_as_ts(v) for v in df[col]]
            continue
        if col.endswith("_date") or col in ("nav_date", "estab_date"):
            sample = pd.to_numeric(df[col], errors="coerce")
            if sample.dropna().gt(1e11).any():
                df[col] = [_as_ts(v) for v in df[col]]
    return df


class FuyaoDataSource:
    """同花顺扶摇数据源。"""

    source_version = "fuyao-v1"

    def __init__(
        self,
        api_key: str | None = None,
        *,
        client: FuyaoClient | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.client = client or FuyaoClient(api_key, timeout=timeout)

    # ==================================================================
    # 通用
    # ==================================================================

    def request(self, path: str, **params: Any) -> dict[str, Any]:
        """原始 ``data`` 对象，给文档里尚未单独包一层的端点用。"""
        return self.client.get(path, **params)

    def request_frame(self, path: str, **params: Any) -> pd.DataFrame:
        """``code=3002``（标的在、数据未就绪）当成空表，不当错误。

        信封里 ``item`` 以外的标量（如热榜的 ``date``）会铺到每一行。
        """
        try:
            data = self.client.get(path, **params)
        except FuyaoAPIError as exc:
            if exc.code == 3002:
                return pd.DataFrame()
            raise
        rows = _extract_rows(data)
        extras = {
            k: v
            for k, v in data.items()
            if k not in ("item", "pagination", "timestamp")
            and not isinstance(v, (list, dict))
        }
        if extras and rows:
            for row in rows:
                for k, v in extras.items():
                    row.setdefault(k, v)
        return _items_frame(rows)

    # ==================================================================
    # 价格 / 元信息 / 日历 / 除复权
    # ==================================================================

    def prices_snapshot(self, thscodes: list[str] | None = None,
                        *, limit: int | None = None, offset: int | None = None) -> pd.DataFrame:
        return self.request_frame(
            "/api/a-share/prices/snapshot",
            thscodes=thscodes, limit=limit, offset=offset,
        )

    def prices_historical(
        self, thscode: str, start: object, end: object, *,
        interval: str = "1d", adjust: str = "backward", offset: int = 0,
    ) -> pd.DataFrame:
        """单标的日 K。窗口超过 10 年会在客户端切开（接口上限）。"""
        parts = []
        for a, b in _split_10y(start, end):
            parts.append(self.request_frame(
                "/api/a-share/prices/historical",
                thscode=thscode, interval=interval,
                start=_to_ms(a), end=_to_ms(b),
                adjust=adjust, offset=offset,
            ))
        nonempty = [p for p in parts if not p.empty]
        if not nonempty:
            return pd.DataFrame()
        out = pd.concat(nonempty, ignore_index=True)
        if "date" in out.columns:
            out = out.drop_duplicates(subset=["date"], keep="last")
        return out

    def search_tickers(self, q: str, **params: Any) -> pd.DataFrame:
        return self.request_frame("/api/meta/tickers/search", q=q, **params)

    def list_tickers(
        self, asset_type: str | None = "a-share", *,
        limit: int = 1000, offset: int = 0,
    ) -> pd.DataFrame:
        return self.request_frame(
            "/api/meta/tickers/list",
            asset_type=asset_type, limit=limit, offset=offset,
        )

    def iter_tickers(self, asset_type: str | None = "a-share",
                     *, page_size: int = 1000) -> pd.DataFrame:
        parts: list[pd.DataFrame] = []
        offset = 0
        while True:
            page = self.list_tickers(asset_type, limit=page_size, offset=offset)
            if page.empty:
                break
            parts.append(page)
            if len(page) < page_size:
                break
            offset += page_size
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    def trading_days(self) -> pd.DataFrame:
        """扶摇近一年交易日（固定窗口，无入参）。"""
        return self.request_frame("/api/a-share/calendar/trading-days")

    def adjustment_factors(self, thscode: str, start: object | None = None,
                           end: object | None = None) -> pd.DataFrame:
        params: dict[str, Any] = {"thscode": thscode}
        if start is not None:
            params["from"] = pd.Timestamp(start).strftime("%Y-%m-%d")
        if end is not None:
            params["to"] = pd.Timestamp(end).strftime("%Y-%m-%d")
        return self.request_frame(
            "/api/a-share/corporate-actions/adjustment-factors", **params,
        )

    def dump_download_url(self, kind: str) -> dict[str, Any]:
        """kind: ``daily-k`` / ``daily-k-10d`` / ``adjustment-factors``."""
        return self.client.get(f"/api/dump/market-dumps/{kind}/download-url")

    # ==================================================================
    # 财务
    # ==================================================================

    def income_statements(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self._financials("/api/a-share/financials/income-statements", thscode, **params)

    def balance_sheets(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self._financials("/api/a-share/financials/balance-sheets", thscode, **params)

    def cash_flow_statements(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self._financials("/api/a-share/financials/cash-flow-statements", thscode, **params)

    def financial_indicators(self, thscode: str, report: str) -> pd.DataFrame:
        """指定报告期的五类财务指标。信封是 ``abilities``，不是 ``item``。"""
        try:
            data = self.client.get(
                "/api/a-share/financials/indicators", thscode=thscode, report=report,
            )
        except FuyaoAPIError as exc:
            if exc.code == 3002:
                return pd.DataFrame()
            raise
        rows = []
        for block in data.get("abilities") or []:
            ability = block.get("ability")
            for ind in block.get("indicators") or []:
                rows.append({
                    "thscode": data.get("thscode", thscode),
                    "symbol": data.get("thscode", thscode),
                    "report": data.get("report", report),
                    "ability": ability,
                    "index_id": ind.get("index_id"),
                    "value": _num(ind.get("value")),
                })
        return pd.DataFrame(rows)

    def _financials(self, path: str, thscode: str, **params: Any) -> pd.DataFrame:
        if "start" in params:
            params["start"] = _to_ms(params["start"])
        if "end" in params:
            params["end"] = _to_ms(params["end"])
        params.setdefault("period", "quarterly")
        return self.request_frame(path, thscode=thscode, **params)

    # ==================================================================
    # 集合竞价 / 特色数据
    # ==================================================================

    def auction_snapshot(self, thscodes: list[str], *, stage: str = "final") -> pd.DataFrame:
        return self.request_frame(
            "/api/a-share/auction/snapshot", thscodes=thscodes, stage=stage,
        )

    def auction_short_term_benchmark(self, **params: Any) -> pd.DataFrame:
        return self.request_frame("/api/a-share/auction/short-term-benchmark", **params)

    def anomaly_analysis_list(self, tag_codes: list[str] | str | None = None) -> pd.DataFrame:
        return self.request_frame(
            "/api/a-share/special-data/anomaly-analysis-list", tag_codes=tag_codes,
        )

    def anomaly_analysis_stock(self, thscodes: list[str] | str) -> pd.DataFrame:
        return self.request_frame(
            "/api/a-share/special-data/anomaly-analysis-stock", thscodes=thscodes,
        )

    def dragon_tiger_list(self, date: object | None = None, *,
                          board_type: str = "all") -> pd.DataFrame:
        try:
            data = self.client.get(
                "/api/a-share/special-data/dragon-tiger-list",
                board_type=board_type,
                date=None if date is None else pd.Timestamp(date).strftime("%Y-%m-%d"),
            )
        except FuyaoAPIError as exc:
            if exc.code == 3002:
                return pd.DataFrame()
            raise
        rows = data.get("stock_items") or []
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["trade_date"] = data.get("trade_date")
        df["board_type"] = data.get("board_type", board_type)
        if "thscode" in df.columns:
            df["symbol"] = df["thscode"].astype(str)
        return df

    def limit_up_pool(self, date: object | None = None, **params: Any) -> pd.DataFrame:
        return self._pool("/api/a-share/special-data/limit-up-pool", date, **params)

    def limit_down_pool(self, date: object | None = None, **params: Any) -> pd.DataFrame:
        return self._pool("/api/a-share/special-data/limit-down-pool", date, **params)

    def limit_break_pool(self, date: object | None = None, **params: Any) -> pd.DataFrame:
        return self._pool("/api/a-share/special-data/limit-break-pool", date, **params)

    def limit_up_ladder(self, **params: Any) -> pd.DataFrame:
        """近 30 日连板天梯。把每天的 ``boards`` 拆成一行一只票。"""
        try:
            data = self.client.get("/api/a-share/special-data/limit-up-ladder", **params)
        except FuyaoAPIError as exc:
            if exc.code == 3002:
                return pd.DataFrame()
            raise
        rows: list[dict[str, Any]] = []
        for day in data.get("item") or []:
            if not isinstance(day, dict):
                continue
            date = day.get("date") or day.get("date_ms")
            boards = day.get("boards")
            rows.extend(_ladder_rows(date, boards))
        return _items_frame(rows)

    def skyrocket_list(self, *, period: str = "day", **params: Any) -> pd.DataFrame:
        return self.request_frame(
            "/api/a-share/special-data/skyrocket-list", period=period, **params,
        )

    def hot_stock_list(self, *, period: str = "day", **params: Any) -> pd.DataFrame:
        return self.request_frame(
            "/api/a-share/special-data/hot-stock-list", period=period, **params,
        )

    def hot_stock_list_history(self, date: object, **params: Any) -> pd.DataFrame:
        raise DataUnavailableError(
            "扶摇 hot-stock-list-history 会回显日期但名单不随日变，"
            "热度用 hot_stock_rank_trend，当日榜用 hot_stock_list。"
        )

    def hot_stock_rank_trend(self, thscode: str, start: object, end: object,
                             **params: Any) -> pd.DataFrame:
        return self.request_frame(
            "/api/a-share/special-data/hot-stock-rank-trend",
            thscode=thscode,
            start_date=pd.Timestamp(start).strftime("%Y-%m-%d"),
            end_date=pd.Timestamp(end).strftime("%Y-%m-%d"),
            **params,
        )

    def _pool(self, path: str, date: object | None, **params: Any) -> pd.DataFrame:
        """涨跌停/炸板池。历史日用 ``date_ms``（不是 ``date``），默认可拿满一页。"""
        params.setdefault("page", 1)
        params.setdefault("size", 200)
        if date is not None:
            day = pd.Timestamp(date).normalize()
            params["date_ms"] = _to_ms(day)
        else:
            day = None
        df = self.request_frame(path, **params)
        if not df.empty and day is not None and "date" not in df.columns:
            df["date"] = day
        return df

    # ==================================================================
    # 指数
    # ==================================================================

    def index_catalog(self, tag: str = "cn_concept") -> pd.DataFrame:
        return self.request_frame("/api/a-share-index/catalog/ths-index-list", tag=tag)

    def index_constituents(self, thscode: str) -> pd.DataFrame:
        return self.request_frame(
            "/api/a-share-index/constituents/ths-stock-list", thscode=thscode,
        )

    def index_prices_snapshot(self, thscodes: list[str] | str) -> pd.DataFrame:
        return self.request_frame(
            "/api/a-share-index/prices/snapshot", thscodes=thscodes,
        )

    def index_prices_historical(
        self, thscode: str, start: object, end: object, *,
        interval: str = "1d", **params: Any,
    ) -> pd.DataFrame:
        return self.request_frame(
            "/api/a-share-index/prices/historical",
            thscode=thscode, interval=interval,
            start=_to_ms(start), end=_to_ms(end), **params,
        )

    def valuations_snapshot(self, thscodes: list[str] | str) -> pd.DataFrame:
        """当日 PE/PB/PS 快照。``pcf_ttm`` 与 jq 口径差一个数量级，不要对。"""
        return self.request_frame(
            "/api/a-share/valuations/snapshot", thscodes=thscodes,
        )

    # ==================================================================
    # 基金（文档里 /api/fund/** 叶子端点都有具名方法；也可用 fund()）
    # ==================================================================

    def fund(self, path: str, **params: Any) -> pd.DataFrame:
        """``path`` 为 ``/api/fund/...`` 或 ``profile/detail``。

        非行情接口缺 ``fund_type`` 时默认 ``otc``。
        ``fund_type`` ∈ {otc, exchange, reits}。
        """
        if not path.startswith("/"):
            path = "/api/fund/" + path.lstrip("/")
        if not path.startswith("/api/fund/market") and "fund_type" not in params:
            params["fund_type"] = "otc"
        for key in ("start", "end"):
            val = params.get(key)
            if val is not None and not isinstance(val, (int, float)):
                params[key] = _to_ms(val)
        return self.request_frame(path, **params)

    def fund_profile(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/profile/detail", thscode=thscode, **params)

    def fund_holdings(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/portfolio/holdings", thscode=thscode, **params)

    def fund_stock_history(self, thscode: str, *, report_type: str | None = None,
                           end_date: object | None = None, **params: Any) -> pd.DataFrame:
        raise DataUnavailableError(
            "扶摇基金股票持仓历史官方样例也返回空 item，空表不能当「未持仓」。"
        )

    def fund_stock_report_dates(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/portfolio/stock-report-dates", thscode=thscode, **params)

    def fund_bond_history(self, thscode: str, *, report_type: str | None = None,
                          end_date: object | None = None, **params: Any) -> pd.DataFrame:
        raise DataUnavailableError(
            "扶摇基金债券持仓历史官方样例也返回空 item，空表不能当「未持仓」。"
        )

    def _fund_position_history(
        self, path: str, dates_fn, thscode: str, *,
        report_type: str | None, end_date: object | None, **params: Any,
    ) -> pd.DataFrame:
        if end_date is not None:
            rt = report_type or "quarter"
            return self.fund(
                path, thscode=thscode, report_type=rt,
                end_date=pd.Timestamp(end_date).strftime("%Y-%m-%d"), **params,
            )
        dates = dates_fn(thscode, **params)
        if dates.empty or "end_date" not in dates.columns:
            return pd.DataFrame()
        for rec in dates.to_dict("records"):
            rt = report_type or rec.get("report_type") or "quarter"
            df = self.fund(
                path, thscode=thscode, report_type=rt,
                end_date=pd.Timestamp(rec["end_date"]).strftime("%Y-%m-%d"), **params,
            )
            if not df.empty:
                return df
        return pd.DataFrame()

    def fund_bond_report_dates(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/portfolio/bond-report-dates", thscode=thscode, **params)

    def fund_asset_allocation(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/portfolio/asset-allocation", thscode=thscode, **params)

    def fund_industry_allocation(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/portfolio/industry-allocation", thscode=thscode, **params)

    def fund_nav(self, thscode: str, *, range: str = "year",
                 nav_type: str = "unit,adj", **params: Any) -> pd.DataFrame:
        """净值序列。``range``: week/month/tmonth/hyear/year/twoyear/tyear/fyear。

        不传 ``range`` 时接口只给最新一条。
        """
        return self.fund(
            "/api/fund/performance/nav",
            thscode=thscode, range=range, nav_type=nav_type, **params,
        )

    def fund_returns(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/performance/returns", thscode=thscode, **params)

    def fund_drawdowns(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/performance/drawdowns", thscode=thscode, **params)

    def fund_indicators_historical(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/performance/indicators-historical", thscode=thscode, **params)

    def fund_holders(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/holders/detail", thscode=thscode, **params)

    def fund_holders_top(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/holders/top", thscode=thscode, **params)

    def fund_manager_detail(self, manager_id: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/managers/detail", manager_id=manager_id, **params)

    def fund_manager_experience(self, manager_id: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/managers/experience", manager_id=manager_id, **params)

    def fund_manager_style(self, manager_id: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/managers/investment-style", manager_id=manager_id, **params)

    def fund_manager_performance(self, manager_id: str, *, range: str = "year",
                                 **params: Any) -> pd.DataFrame:
        return self.fund(
            "/api/fund/managers/performance",
            manager_id=manager_id, range=range, **params,
        )

    def fund_company(self, company_id: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/companies/detail", company_id=company_id, **params)

    def fund_diagnostics(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/diagnostics/detail", thscode=thscode, **params)

    def fund_income_statements(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/financials/income-statements", thscode=thscode, **params)

    def fund_balance_sheets(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/financials/balance-sheets", thscode=thscode, **params)

    def fund_financial_indicators(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/financials/indicators", thscode=thscode, **params)

    def fund_dividends(self, thscode: str, **params: Any) -> pd.DataFrame:
        return self.fund("/api/fund/corporate-actions/dividends", thscode=thscode, **params)

    def fund_news(self, thscode: str, **params: Any) -> pd.DataFrame:
        raise DataUnavailableError(
            "扶摇基金新闻官方样例也返回空 item，空表不能当「无新闻」。"
        )

    def fund_offerings(self, *, subscribe: str = "active", **params: Any) -> pd.DataFrame:
        """``subscribe``: ``active`` 当前募集 / ``upcoming`` 即将募集。"""
        return self.fund("/api/fund/offerings/list", subscribe=subscribe, **params)

    def fund_market_snapshot(self, thscode: str, **params: Any) -> pd.DataFrame:
        """仅 ETF。不传 ``fund_type``。"""
        return self.request_frame("/api/fund/market/snapshot", thscode=thscode, **params)

    def fund_market_historical(self, thscode: str, start: object, end: object,
                               *, interval: str = "1d", **params: Any) -> pd.DataFrame:
        """仅 ETF。窗口最长 5 年。"""
        return self.request_frame(
            "/api/fund/market/historical",
            thscode=thscode, interval=interval,
            start=_to_ms(start), end=_to_ms(end), **params,
        )

    # ==================================================================
    # DataSource Protocol
    # ==================================================================

    def fetch_bars(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
        freq: Freq = Freq.DAILY,
        adjust: AdjustMode = AdjustMode.BACKWARD,
    ) -> pd.DataFrame:
        freq = Freq(freq)
        if freq != Freq.DAILY:
            raise DataUnavailableError("扶摇历史 K 线当前只支持日线 interval=1d")
        frames = []
        for symbol in dict.fromkeys(symbols):
            frame = self._one_daily(symbol, start, end)
            if frame is not None:
                frames.append(frame)
        if not frames:
            return pd.DataFrame(
                index=pd.MultiIndex.from_arrays(
                    [pd.DatetimeIndex([]), []], names=["date", "symbol"]
                )
            )
        return pd.concat(frames).sort_index()

    def _one_daily(self, symbol: str, start: object, end: object) -> pd.DataFrame | None:
        post = self.prices_historical(symbol, start, end, adjust="backward")
        raw = self.prices_historical(symbol, start, end, adjust="none")
        if post.empty or raw.empty:
            return None
        post = _index_bars(post)
        raw = _index_bars(raw)
        idx = post.index.intersection(raw.index)
        if len(idx) == 0:
            return None
        post, raw = post.loc[idx], raw.loc[idx]
        raw_close = _bar_col(raw, "close_price", "close").astype("float64").replace(0, np.nan)
        post_close = _bar_col(post, "close_price", "close").astype("float64")
        adj = (post_close / raw_close).replace([np.inf, -np.inf], np.nan).fillna(1.0)
        volume = _bar_col(raw, "volume").fillna(0).round().astype("int64")
        amount = _bar_col(raw, "turnover", "amount", "money").fillna(0.0).astype("float64")
        sus = (volume <= 0) | (amount <= 0)
        volume = volume.where(~sus, 0).astype("int64")
        amount = amount.where(~sus, 0.0)
        vwap = (amount / volume.replace(0, np.nan) * adj).astype("float64")
        out = pd.DataFrame({
            "open": _bar_col(post, "open_price", "open").astype("float64"),
            "high": _bar_col(post, "high_price", "high").astype("float64"),
            "low": _bar_col(post, "low_price", "low").astype("float64"),
            "close": post_close,
            "vwap": vwap,
            "open_raw": _bar_col(raw, "open_price", "open").astype("float64"),
            "high_raw": _bar_col(raw, "high_price", "high").astype("float64"),
            "low_raw": _bar_col(raw, "low_price", "low").astype("float64"),
            "close_raw": _bar_col(raw, "close_price", "close").astype("float64"),
            "volume": volume,
            "amount": amount,
            "adj_factor": adj.astype("float64"),
            "limit_up_price": np.nan,
            "limit_down_price": np.nan,
            "is_suspended": sus.to_numpy(),
            "is_limit_up": False,
            "is_limit_down": False,
            "is_st": False,
            "days_since_listing": np.int32(0),
        }, index=idx)
        if sus.any():
            out.loc[sus, list(_PRICE_COLS + _RAW_PRICE_COLS)] = np.nan
            out.loc[sus, "volume"] = 0
            out.loc[sus, "amount"] = 0.0
        out.index = pd.MultiIndex.from_arrays(
            [pd.DatetimeIndex(out.index), [symbol] * len(out)],
            names=["date", "symbol"],
        )
        return out

    def current_universe(self, spec: str) -> pd.DataFrame:
        """当前截面成员，不是 PIT。index 只有 symbol。"""
        name = spec.strip().lower()
        if name in _INDEX_UNIVERSE or name.endswith((".sh", ".sz", ".ti")):
            code = _INDEX_UNIVERSE.get(name, spec.upper())
            members = self.index_constituents(code)
            symbols = members["symbol"].astype(str).tolist() if not members.empty else []
            weights = np.full(len(symbols), 1.0 / max(len(symbols), 1))
        else:
            from qlab.data.universe import apply_board_filters, parse_broad_universe

            parsed = parse_broad_universe(name)
            if parsed is None:
                raise DataUnavailableError(
                    f"扶摇不认识宇宙 {spec!r}。"
                    "可用 hs_a / main_a / csi300 / csi500 / csi800 / csi1000，"
                    "或直接传指数 thscode（如 000300.SH）。"
                )
            exclude_star, exclude_st = parsed
            tickers = self.iter_tickers("a-share")
            symbols = tickers["symbol"].astype(str).tolist() if not tickers.empty else []
            symbols = apply_board_filters(
                symbols, exclude_bj=True, exclude_star=exclude_star,
            )
            if exclude_st:
                st = _st_symbols(tickers)
                symbols = [s for s in symbols if s not in st]
            weights = np.full(len(symbols), np.nan)
        if not symbols:
            return pd.DataFrame(
                {"in_universe": pd.Series(dtype=bool), "weight": pd.Series(dtype="float64")},
                index=pd.Index([], name="symbol"),
            )
        return pd.DataFrame(
            {"in_universe": True, "weight": weights},
            index=pd.Index(symbols, name="symbol"),
        )

    def fetch_universe(
        self, spec: str, date_range: tuple[pd.Timestamp, pd.Timestamp],
    ) -> pd.DataFrame:
        raise DataUnavailableError(
            "扶摇成分/代码表只有当前快照，不能当 PIT universe。"
            "当日截面用 current_universe / index_constituents / iter_tickers。"
        )

    def fetch_calendar(self, name: str = "SSE") -> Calendar:
        """长历史用 exchange_calendars；扶摇日历接口只有近一年。"""
        return get_default_calendar()

    def fetch_corporate_actions(
        self, symbols: list[str], start: pd.Timestamp, end: pd.Timestamp,
    ) -> pd.DataFrame:
        rows = []
        for symbol in dict.fromkeys(symbols):
            df = self.adjustment_factors(symbol, start, end)
            if df.empty:
                continue
            for rec in df.to_dict("records"):
                ex = rec.get("ex_date", rec.get("ex_date_ms"))
                if ex is None or (isinstance(ex, float) and np.isnan(ex)):
                    continue
                ex_ts = pd.Timestamp(ex) if not isinstance(ex, pd.Timestamp) else ex
                cash = rec.get("dividend_per_share")
                bonus = rec.get("per_share_bonus")
                rows.append({
                    "symbol": symbol,
                    "action_type": "dividend",
                    "announce_date": ex_ts,
                    "ex_date": ex_ts,
                    "record_date": pd.NaT,
                    "pay_date": pd.NaT,
                    "cash_per_share": float(cash) if cash is not None else np.nan,
                    "bonus_share_ratio": float(bonus) if bonus is not None else np.nan,
                    "transfer_share_ratio": np.nan,
                    "rights_share_ratio": np.nan,
                    "rights_price": np.nan,
                })
        if not rows:
            return pd.DataFrame(columns=[
                "symbol", "action_type", "announce_date", "ex_date",
                "record_date", "pay_date", "cash_per_share",
                "bonus_share_ratio", "transfer_share_ratio",
                "rights_share_ratio", "rights_price",
            ])
        return pd.DataFrame(rows)

    def fetch_status_overrides(
        self, symbols: list[str], start: pd.Timestamp, end: pd.Timestamp,
    ) -> pd.DataFrame:
        return pd.DataFrame()

    def fetch_fundamentals(
        self,
        symbols: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
        report_types: list[ReportType] | None = None,
        fields: list[str] | None = None,
    ) -> pd.DataFrame:
        parts = []
        for symbol in dict.fromkeys(symbols):
            inc = self.income_statements(symbol, start=start, end=end, period="quarterly")
            bal = self.balance_sheets(symbol, start=start, end=end, period="quarterly")
            cfs = self.cash_flow_statements(symbol, start=start, end=end, period="quarterly")
            merged = _merge_statements(symbol, inc, bal, cfs)
            if merged is not None:
                parts.append(merged)
        if not parts:
            return pd.DataFrame()
        return pd.concat(parts, ignore_index=True)

    def fetch_industry_classification(
        self, symbols: list[str],
        date_range: tuple[pd.Timestamp, pd.Timestamp],
        system: str = "sw", level: int = 1,
    ) -> pd.DataFrame:
        raise DataUnavailableError(
            "扶摇没有申万行业分类表。行业指数用 index_catalog(tag='industry') "
            "+ index_constituents。"
        )

    def fetch_concepts(
        self, symbols: list[str],
        date_range: tuple[pd.Timestamp, pd.Timestamp],
        source: str = "eastmoney",
    ) -> pd.DataFrame:
        raise DataUnavailableError(
            "扶摇概念是同花顺板块指数，不是个股一对多 PIT 表。"
            "用 index_catalog(tag='cn_concept') + index_constituents。"
        )

    def fetch_share_capital(
        self, symbols: list[str], start: pd.Timestamp, end: pd.Timestamp,
    ) -> pd.DataFrame:
        raise DataUnavailableError("扶摇未提供股本变动接口")

    def fetch_margin_trading(
        self, symbols: list[str], start: pd.Timestamp, end: pd.Timestamp,
    ) -> pd.DataFrame:
        raise DataUnavailableError("扶摇未提供两融接口")

    def fetch_money_flow(
        self, symbols: list[str], start: pd.Timestamp, end: pd.Timestamp,
    ) -> pd.DataFrame:
        raise DataUnavailableError("扶摇未提供资金流向接口")

    def fetch_call_auction(
        self, symbols: list[str], start: pd.Timestamp, end: pd.Timestamp,
    ) -> pd.DataFrame:
        today = pd.Timestamp.now(tz="Asia/Shanghai").tz_localize(None).normalize()
        start_d = pd.Timestamp(start).normalize()
        end_d = pd.Timestamp(end).normalize()
        if start_d != today or end_d != today:
            raise DataUnavailableError(
                "扶摇集合竞价只有当日快照，不能按历史日回放，"
                "也不会把今日竞价标成请求日。"
            )
        snap = self.auction_snapshot(list(symbols), stage="final")
        if snap.empty:
            return pd.DataFrame(
                index=pd.MultiIndex.from_arrays(
                    [pd.DatetimeIndex([]), []], names=["date", "symbol"]
                )
            )
        date = today
        vol = pd.to_numeric(snap.get("auction_volume"), errors="coerce").fillna(0.0)
        amt = pd.to_numeric(snap.get("auction_amount"), errors="coerce").fillna(0.0)
        out = pd.DataFrame({
            "available_at": date + pd.Timedelta(hours=9, minutes=30),
            "auction_price": pd.to_numeric(snap.get("auction_price"), errors="coerce"),
            "auction_volume": vol.astype("float64"),
            "auction_amount": amt.astype("float64"),
        })
        out.index = pd.MultiIndex.from_arrays(
            [[date] * len(snap), snap["symbol"].astype(str)],
            names=["date", "symbol"],
        )
        return out

    def fetch_billboard(
        self, end_date: pd.Timestamp, symbols: list[str] | None = None, count: int = 5,
    ) -> pd.DataFrame:
        raise DataUnavailableError(
            "扶摇龙虎榜是个股汇总，不是席位级 Billboard。"
            "名单和个股买卖额用 dragon_tiger_list，不要当 jq fetch_billboard。"
        )

    def fetch_factor_exposure(
        self, symbols: list[str], factors: list[str],
        start: pd.Timestamp, end: pd.Timestamp,
    ) -> pd.DataFrame:
        raise DataUnavailableError("扶摇未提供因子暴露接口")

    def fetch_index_valuation(
        self, symbol: str, start: pd.Timestamp, end: pd.Timestamp,
    ) -> pd.DataFrame:
        raise DataUnavailableError("扶摇未提供指数估值/换手表")


def _index_bars(df: pd.DataFrame) -> pd.DataFrame:
    if "date" in df.columns:
        idx = pd.DatetimeIndex(df["date"])
    elif "date_ms" in df.columns:
        idx = pd.DatetimeIndex([_ms_to_ts(v) for v in df["date_ms"]])
    else:
        raise DataUnavailableError("历史 K 线缺少 date / date_ms")
    out = df.copy()
    out.index = idx
    return out[~out.index.duplicated(keep="last")].sort_index()


def _bar_col(df: pd.DataFrame, *names: str) -> pd.Series:
    for name in names:
        if name in df.columns:
            return df[name]
    raise DataUnavailableError(f"历史 K 线缺少列 {names}")


def _split_10y(start: object, end: object) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """扶摇历史 K 硬上限 10 年，按 9 年切段。"""
    a = pd.Timestamp(start)
    b = pd.Timestamp(end)
    if b < a:
        return []
    step = pd.DateOffset(years=9)
    out: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    cur = a
    while cur <= b:
        nxt = min(cur + step, b)
        out.append((cur, nxt))
        if nxt >= b:
            break
        cur = nxt + pd.Timedelta(days=1)
    return out


def _st_symbols(tickers: pd.DataFrame) -> set[str]:
    if tickers.empty:
        return set()
    name_col = next(
        (c for c in ("name", "sec_name", "security_name", "ths_name") if c in tickers.columns),
        None,
    )
    if name_col is None or "symbol" not in tickers.columns:
        return set()
    names = tickers[name_col].astype(str)
    mask = names.str.contains(r"ST|\*ST", case=False, regex=True)
    return set(tickers.loc[mask, "symbol"].astype(str))


def _merge_statements(
    symbol: str, inc: pd.DataFrame, bal: pd.DataFrame, cfs: pd.DataFrame,
) -> pd.DataFrame | None:
    def keyed(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        key = df["period_end"] if "period_end" in df.columns else df.get("period_end_ms")
        out = df.copy()
        out["_k"] = pd.to_datetime(key)
        return out

    inc, bal, cfs = keyed(inc), keyed(bal), keyed(cfs)
    if inc.empty and bal.empty and cfs.empty:
        return None
    base = inc if not inc.empty else (bal if not bal.empty else cfs)
    out_rows = []
    for rec in base.to_dict("records"):
        k = rec.get("_k")
        b = _row_by_key(bal, k)
        c = _row_by_key(cfs, k)
        announce = rec.get("report_date", rec.get("report_date_ms"))
        period = rec.get("period_end", rec.get("period_end_ms"))
        if announce is None or period is None:
            continue
        announce_ts = _as_ts(announce)
        period_ts = _as_ts(period)
        if pd.isna(announce_ts) or pd.isna(period_ts):
            continue
        # 扶摇 report_date 常把去年同期的首次披露改成今年同比稿日期（差约一年），
        # 不能当 PIT announce。超过 200 天当不可用。
        if (announce_ts - period_ts).days > 200:
            announce_ts = pd.NaT
        fq = rec.get("fiscal_period") or rec.get("period") or "Q4"
        quarter = _fiscal_quarter(fq)
        available = (
            pd.NaT if pd.isna(announce_ts)
            else announce_ts + pd.Timedelta(hours=9, minutes=30)
        )
        out_rows.append({
            "announce_date": announce_ts,
            "symbol": symbol,
            "report_type": "official",
            "report_period": period_ts,
            "available_at": available,
            "fiscal_year": int(rec.get("fiscal_year") or period_ts.year),
            "fiscal_quarter": np.int8(quarter),
            "source": "fuyao",
            "total_assets": _num(b.get("assets_total")),
            "total_liabilities": _num(b.get("total_debt")),
            "total_equity": _num(b.get("holder_equity_total")),
            "cash_and_equivalents": _num(b.get("cash")),
            "current_assets": _num(b.get("total_current_assets")),
            # 扶摇只有营业收入，没有营业总收入；不要把前者冒充后者。
            "total_revenue": np.nan,
            "operating_revenue": _num(rec.get("operating_income")),
            "operating_cost": _num(rec.get("operating_costs")),
            "operating_profit": _num(rec.get("operating_profit")),
            "net_profit": _num(rec.get("net_profit")),
            "net_profit_to_shareholders": _num(rec.get("parent_holder_net_profit")),
            "eps_basic": _num(rec.get("basic_eps")),
            "operating_cash_flow": _num(c.get("act_cash_flow_net")),
            "investing_cash_flow": _num(c.get("invest_cash_flow_net")),
            "financing_cash_flow": _num(c.get("financing_cash_flow_net")),
        })
    if not out_rows:
        return None
    return pd.DataFrame(out_rows)


def _row_by_key(df: pd.DataFrame, key: object) -> dict[str, Any]:
    if df.empty or key is None or "_k" not in df.columns:
        return {}
    hit = df[df["_k"] == pd.Timestamp(key)]
    if hit.empty:
        return {}
    return hit.iloc[0].to_dict()


def _num(v: object) -> float:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return float("nan")
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("nan")


def _as_ts(v: object) -> pd.Timestamp:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return pd.NaT
    if isinstance(v, pd.Timestamp):
        return v.tz_localize(None) if v.tzinfo else v
    if isinstance(v, (int, float, np.integer, np.floating)):
        if v > 1e11:
            return _ms_to_ts(v)
        return pd.Timestamp(v)
    try:
        return pd.Timestamp(v)
    except (TypeError, ValueError):
        return pd.NaT


def _fiscal_quarter(value: object) -> int:
    text = str(value).strip().upper()
    table = {
        "1": 1, "Q1": 1, "一季": 1, "一季报": 1, "FIRST": 1,
        "2": 2, "Q2": 2, "中报": 2, "二季": 2, "二季报": 2, "SEMI": 2,
        "3": 3, "Q3": 3, "三季": 3, "三季报": 3, "THIRD": 3,
        "4": 4, "Q4": 4, "年报": 4, "四季": 4, "四季报": 4, "FY": 4, "YEAR": 4,
    }
    if text in table:
        return table[text]
    digit = "".join(ch for ch in text if ch.isdigit())
    if digit in table:
        return table[digit]
    return 4


def _ladder_rows(date: object, boards: object) -> list[dict[str, Any]]:
    """boards 是 {two_board: [stock...], three_board: [...]} 或 list。"""
    out: list[dict[str, Any]] = []
    if isinstance(boards, dict):
        # 纯股票 dict（少见）vs 连板桶
        if "thscode" in boards or "ticker" in boards:
            row = dict(boards)
            row.setdefault("date", date)
            return [row]
        for level, stocks in boards.items():
            out.extend(_ladder_stock_rows(date, level, stocks))
        return out
    if isinstance(boards, list):
        for item in boards:
            if isinstance(item, dict) and "thscode" in item:
                row = dict(item)
                row.setdefault("date", date)
                out.append(row)
            else:
                out.extend(_ladder_rows(date, item))
    return out


def _ladder_stock_rows(date: object, level: object, stocks: object) -> list[dict[str, Any]]:
    if not isinstance(stocks, list):
        return []
    rows = []
    for stock in stocks:
        if not isinstance(stock, dict):
            continue
        row = dict(stock)
        row.setdefault("date", date)
        row.setdefault("board", level)
        rows.append(row)
    return rows
