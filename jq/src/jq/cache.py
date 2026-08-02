"""本地数据缓存引擎.

在本地(``CWD/.jqcache/``)缓存聚宽取数结果,避免重复远程查询.
存储由 :mod:`jq.cache_store` 提供(按月分片 + 原子写 + 版本隔离).

缓存目录: ``<CWD>/.jqcache/``(可用环境变量 ``JQ_CACHE_DIR`` 覆盖).
缓存格式: pickle(pandas 原生,零额外依赖). 仅读取本引擎自己写入的文件,
信任边界为本地文件系统.

使用方式::

    from jq.cache import DataCache

    dc = DataCache()
    df = dc.get_price('600519.XSHG', '2026-07-01', '2026-07-21')
    # 首次: 远程取数 + 按月落盘
    # 二次: 直接读分片, 0ms
    # 扩大范围: 仅远程补查缺失的月份

    # 批量(一次远程调用取多只, 分别落盘)
    data = dc.get_price_batch(['600519.XSHG', '000001.XSHE'], '2026-07-01', '2026-07-21')

复权口径(重要)::

    行情缓存统一存 **后复权(fq='post')** 数据 —— 后复权的历史值不随
    未来分红而改变(PIT 稳定), 且自带完整 factor 序列.
    raw / 前复权在**本地**按 factor 推导, 因此:
      - 缓存内容与请求的 fq 无关, 永久有效;
      - 任何 pre_factor_ref_date 都不会污染缓存;
      - 同一 (标的, 日期) 的价格永远唯一(消除了跨基准日拼接的跳变).

管理::

    dc.status()   # 缓存统计
    dc.clear()    # 清空缓存
"""

from __future__ import annotations

import os
import time
from datetime import date
from pathlib import Path

import pandas as pd

from jq.auth import DEFAULT_BASE_URL, CookieStore
from jq.cache_store import (
    CACHE_VERSION,
    MonthlyShardStore,
    SnapshotStore,
    current_month,
    is_today_or_future,
    merge_consecutive_months,
    month_bounds,
    months_between,
    norm_date,
)
from jq.exceptions import JQExecutionError
from jq.runner import JoinQuantRunner
from jq.serialize import PICKLE_END, PICKLE_START, PICKLE_TRAILER, decode_result

DEFAULT_CACHE_DIR = Path(os.environ.get("JQ_CACHE_DIR", Path.cwd() / ".jqcache"))

#: 默认远程执行超时(秒). 比 runner 的 60s 大很多 —— 取数类查询
#: (尤其批量/长区间/分钟级)轻易超过 1 分钟.
DEFAULT_EXEC_TIMEOUT = 300.0

# 随 fq 复权的价格列
_PRICE_COLS = (
    "open", "close", "high", "low", "avg", "pre_close",
    "high_limit", "low_limit",
)

# 行情缓存的完整字段集.
# 聚宽 get_price 不传 fields 时**只返回 6 个基础字段**(无 factor),
# 而本层依赖 factor 做本地复权换算, 故必须显式请求全字段.
# 已实测股票/指数/ETF/科创板四类标的均兼容该字段集.
_BAR_FIELDS = [
    "open", "close", "low", "high", "volume", "money",
    "factor", "high_limit", "low_limit", "avg", "pre_close", "paused",
]

#: 聚合分钟频率(5m/15m/30m/60m/120m)可用的字段集.
#:
#: 聚宽对这些频率硬约束 fields 只能取
#: ``['open','close','high','low','volume','money','open_interest']``,
#: 传 factor/paused/avg 等会直接 AssertionError(实测不传 fields 也报错,
#: 因为本层总是显式注入字段集)。``daily`` 与 ``1m`` 不受此限。
_BAR_FIELDS_AGG_MINUTE = ["open", "close", "high", "low", "volume", "money"]

#: 不受字段限制的频率(可取全 12 字段, 含 factor)
_FULL_FIELD_FREQS = frozenset({"daily", "1m", "1d", "day", "minute"})

#: 聚宽 ``get_fundamentals_continuously`` 的硬性返回行数上限。
#: 超出**静默截断不报错**(实测 100只×242日 理论 24200 行 → 只返 10000 行),
#: 故批量估值必须按此分块并校验行数。
_CONTINUOUSLY_ROW_LIMIT = 10000

#: 聚宽 ``finance.run_query`` 的硬性返回行数上限。
#: 实测: 无论条件多宽、即使显式 ``limit(20000)``, 也只返 5000 行且不报错;
#: ``offset`` 分页可用(实测第 2 页数据不同)。故财务查询必须分页拉完。
_RUN_QUERY_ROW_LIMIT = 5000

#: 各接口的**发布语义** —— 数据日 T 的数据何时实际可用。
#:
#: 这是关于“聚宽这个数据源何时能给到数据”的元知识, 属本层职责——
#: 上层(如 qlab)不应重建交易所发布规则, 只消费 ``available_at``。
#:
#: - ``next_session``: T 日收盘后才发布 → 取**下一交易日**开盘作安全上界。
#:   当晚往往已可得(同花顺/东财当晚更新), 但各商入库时刻不一
#:   (实测聚宽当日 22:28 两融尚未入库), 故不假设当晚可用。
#: - ``same_session``: 当日盘中即可用(如 09:25 集合竞价结束即确定)。
_PUBLICATION_TIMING: dict[str, tuple[str, int, int]] = {
    # func_name: (时点语义, 小时, 分钟)
    "get_mtss": ("next_session", 9, 30),
    "get_money_flow": ("next_session", 9, 30),
    "get_call_auction": ("same_session", 9, 30),
}


def bar_fields_for(frequency: str) -> list[str]:
    """按频率返回可请求的字段集.

    聚合分钟频率拿不到 ``factor``, 因此那些频率的数据**无法在本地换算复权口径**
    (见 :meth:`DataCache._apply_fq` 的报错)。
    """
    return (
        list(_BAR_FIELDS)
        if str(frequency) in _FULL_FIELD_FREQS
        else list(_BAR_FIELDS_AGG_MINUTE)
    )

# 向后兼容: 旧代码/测试可能从本模块导入这两个标记
_PICKLE_START = PICKLE_START
_PICKLE_END = PICKLE_END
_PICKLE_TRAILER = PICKLE_TRAILER


class DataCache:
    """本地数据缓存引擎(按月分片,支持增量补查).

    Args:
        cache_dir: 缓存目录, 默认 ``<CWD>/.jqcache/``.
        runner: JoinQuantRunner 实例, 默认创建 persistent runner.
    """

    def __init__(
        self,
        cache_dir: Path | str | None = None,
        runner: JoinQuantRunner | None = None,
        *,
        exec_timeout: float = DEFAULT_EXEC_TIMEOUT,
    ) -> None:
        self.cache_dir = Path(cache_dir) if cache_dir else DEFAULT_CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.bars = MonthlyShardStore(self.cache_dir)
        self.snapshots = SnapshotStore(self.cache_dir)
        self.exec_timeout = float(exec_timeout)
        self._runner = runner
        self._runner_kwargs = {
            "base_url": DEFAULT_BASE_URL,
            "persistent": True,
        }

    @property
    def runner(self) -> JoinQuantRunner:
        """懒创建 runner —— 纯缓存命中的查询无需建立远程连接."""
        if self._runner is None:
            self._runner = JoinQuantRunner(
                cookie_store=CookieStore(), **self._runner_kwargs
            )
        return self._runner

    # ==================================================================
    # 远程执行
    # ==================================================================

    def _fetch_raw(self, user_code: str, *, timeout: float | None = None):
        """执行远程代码, 返回反序列化的 ``__result__``.

        用 base64 pickle 传输(而非 JSON table), 绕过本地/聚宽 pandas
        版本间的兼容性问题; 详见 :mod:`jq.serialize`.

        Args:
            timeout: 执行超时(秒). 缺省用 ``self.exec_timeout``.
        """
        result = self.runner.run_code(
            user_code + PICKLE_TRAILER,
            timeout=self.exec_timeout if timeout is None else float(timeout),
        )
        return decode_result(result)

    # ==================================================================
    # 工具
    # ==================================================================

    @staticmethod
    def _check_fields(df: pd.DataFrame, fields: list[str] | None) -> pd.DataFrame:
        """按 fields 切片, 请求了不存在的列时立即报错(而非静默丢弃)."""
        if not fields:
            return df
        # 空返回(退市股/无数据区间)是特例: 聚宽返回 0 行且无列,
        # 此时"字段不存在"是因为压根没数据而非拼错字段。
        # 不报错, 返回带请求列结构的空表, 交由调用方按 len(df)==0 处理。
        if len(df) == 0:
            return pd.DataFrame(columns=list(fields), index=df.index)
        missing = [c for c in fields if c not in df.columns]
        if missing:
            raise ValueError(
                f"请求的字段不存在: {missing}. 可用字段: {sorted(df.columns)}"
            )
        return df[list(fields)]

    @staticmethod
    def _to_date_index(
        df: pd.DataFrame, date_col: str, truncate: bool = False
    ) -> pd.DataFrame:
        """确保以 DatetimeIndex 为索引(部分聚宽函数返回数字 index + 日期列)."""
        if isinstance(df.index, pd.DatetimeIndex):
            return df
        if date_col and date_col in df.columns:
            df = df.copy()
            df[date_col] = pd.to_datetime(df[date_col])
            if truncate:
                df[date_col] = df[date_col].dt.normalize()
            df = df.set_index(date_col)
        return df

    # ==================================================================
    # 复权换算(本地) —— 缓存恒为 post 口径
    # ==================================================================

    @staticmethod
    def _apply_fq(
        df: pd.DataFrame, fq: str | None, ref_factor: float | None = None
    ) -> pd.DataFrame:
        """把后复权数据换算到目标口径.

        聚宽复权语义: 价格 × factor, 成交量 ÷ factor(成交额不变).
        以 ``f`` 表示当日后复权因子, ``f_ref`` 表示前复权基准日因子:

        ==========  ==================  ==================  ============
        口径        价格                成交量              factor 列
        ==========  ==================  ==================  ============
        post        post                post                f
        None(raw)   post / f            post × f            1.0
        pre         post / f_ref        post × f_ref        f / f_ref
        ==========  ==================  ==================  ============

        Args:
            df: 后复权口径数据(须含 ``factor`` 列).
            fq: ``'post'`` / ``'pre'`` / ``None``.
            ref_factor: 前复权基准日的 factor(仅 ``fq='pre'`` 需要).

        Raises:
            JQExecutionError: 需要换算但数据不含 ``factor`` 列。聚合分钟频率
                (5m/15m/30m/...)拿不到 factor —— 此时**必须报错**而不能静默
                把后复权价当成其他口径返回。
        """
        if fq not in (None, "pre", "post"):
            raise ValueError(f"fq 只能是 'pre' / 'post' / None, 收到 {fq!r}")
        if fq == "post" or len(df) == 0:
            return df
        if "factor" not in df.columns:
            raise JQExecutionError(
                f"数据不含 factor 列, 无法换算到 fq={fq!r}。\n"
                "  聚宽对聚合分钟频率(5m/15m/30m/60m/120m)只允许 6 个基础字段, 不含 factor。\n"
                "  出路: 用 fq='post' 取后复权数据; 或用 frequency='1m'/'daily'"
                "(它们支持 factor)后自行聚合。"
            )
        out = df.copy()
        f = out["factor"].astype(float)
        if fq is None:
            scale = f
            new_factor = 1.0
        else:  # 'pre'
            if ref_factor is None or ref_factor <= 0:
                raise ValueError("fq='pre' 需要有效的 ref_factor")
            scale = pd.Series(float(ref_factor), index=out.index)
            new_factor = f / float(ref_factor)
        for col in _PRICE_COLS:
            if col in out.columns:
                out[col] = out[col] / scale
        if "volume" in out.columns:
            out["volume"] = out["volume"] * scale
        out["factor"] = new_factor
        return out

    # ==================================================================
    # 通用: 月分片时间序列查询
    # ==================================================================

    def _monthly_query(
        self,
        kind: str,
        security: object,
        start_date: object,
        end_date: object,
        *,
        variant: str = "default",
        call_builder,
        date_col: str = "date",
        truncate_date: bool = False,
    ) -> pd.DataFrame:
        """按月分片的时间序列查询(缺失月才远程补查).

        Args:
            kind: 缓存类别(子目录名), 如 ``get_price``.
            security: 标的(参与缓存路径).
            variant: 同一标的下的变体维度(如 ``daily__post``).
            call_builder: ``callable(security, start, end) -> str``,
                返回远程调用表达式(不含 ``__result__ =`` 前缀).
            date_col: 日期列名(用于把数字 index 转成日期 index);
                空串表示数据天然以日期为 index.
            truncate_date: 日期列含时分秒时截断到日.
        """
        start = norm_date(start_date)
        end = norm_date(end_date)
        if end < start:
            return pd.DataFrame()

        missing = self.bars.missing_months(kind, security, start, end, variant)
        # 当月/未来月的数据按设计不落盘, 必须把刚取到的部分留在内存里并
        # 并入返回值 —— 否则"不落盘"会退化成"读不到", 所有涉及当月的
        # 查询(即所有取最新行情的场景)都会丢数据.
        fresh: list[pd.DataFrame] = []
        for gap_start, gap_end in merge_consecutive_months(missing):
            code = "from jqdata import *\nfrom jqdata.apis import *\n__result__ = " + (
                call_builder(security, gap_start, gap_end)
            )
            df = self._fetch_raw(code)
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame()
            if len(df) > 0 and date_col:
                df = self._to_date_index(df, date_col, truncate_date)
            # 即使无数据也写入(空月标记), 避免上市前/退市后区间反复远程查询
            self.bars.write_range(
                df, kind, security, gap_start, gap_end, variant
            )
            if len(df) > 0:
                fresh.append(df)

        cached = self.bars.read_range(kind, security, start, end, variant)
        return self._combine(cached, fresh, start, end)

    @staticmethod
    def _combine(
        cached: pd.DataFrame,
        fresh: list[pd.DataFrame],
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """合并已落盘数据与本次新取数据(新数据优先), 并按区间切片.

        新取数据里含当月等不落盘部分, 必须从内存带出来.
        """
        if not fresh:
            return cached
        frames = [f for f in ([cached] + fresh) if len(f) > 0]
        if not frames:
            return cached
        merged = pd.concat(frames).sort_index()
        merged = merged[~merged.index.duplicated(keep="last")]
        return merged.loc[start:end]

    # ==================================================================
    # 区间解析
    # ==================================================================

    def _resolve_range(
        self,
        start_date: object | None,
        end_date: object | None,
        count: int | None,
    ) -> tuple[str, str]:
        """把 ``(start_date, end_date, count)`` 归一为具体区间.

        ``end_date`` **始终必填**: “取到最新”是个浮动终点, 会让同一段代码
        在不同日子返回不同区间 —— 回测里这正是未来函数的入口. 想取最新就
        显式传今天的日期.

        ``count`` 与 ``start_date`` 二选一; 指定 ``count`` 时用**已缓存的
        交易日历**在本地换算出起始日(不额外走远程).
        """
        if end_date is None:
            raise ValueError(
                "end_date 必填 —— 不接受“取到最新”的浮动终点。"
                "想取至今天请显式传 end_date=str(datetime.date.today())"
            )
        end = norm_date(end_date)
        if count is not None:
            if start_date is not None:
                raise ValueError("count 与 start_date 不能同时传入")
            if count <= 0:
                raise ValueError(f"count 必须为正整数, 收到 {count}")
            all_days = [norm_date(d) for d in self.get_all_trade_days()]
            hist = [d for d in all_days if d <= end]
            if not hist:
                raise ValueError(f"交易日历中找不到 {end} 之前的交易日")
            return hist[-count], end
        if start_date is None:
            raise ValueError("需提供 (start_date, end_date) 或 (count, end_date)")
        return norm_date(start_date), end

    # ==================================================================
    # 行情
    # ==================================================================

    def get_price(
        self,
        security: str,
        start_date: object | None = None,
        end_date: object | None = None,
        *,
        count: int | None = None,
        frequency: str = "daily",
        fq: str | None = "pre",
        fields: list[str] | None = None,
        skip_paused: bool = False,
        pre_factor_ref_date: object | None = None,
    ) -> pd.DataFrame:
        """缓存版 ``get_price``.

        缓存恒为后复权口径(见模块 docstring), 目标口径本地换算.

        Args:
            security: 单只证券代码.
            start_date / end_date: 日期(接受 str/date/Timestamp).
            count: 取 ``end_date`` 前 N 个交易日, 与 ``start_date`` 二选一
                (本地用交易日历换算). ``end_date`` 仍需显式给出.
            frequency: ``daily`` / ``minute`` / ``weekly`` / ``monthly``.
            fq: ``'pre'``(默认) / ``'post'`` / ``None``.
            fields: 字段子集; 请求不存在的字段会报错.
            skip_paused: 是否剔除停牌日(在本地按 ``paused`` 列过滤).
            pre_factor_ref_date: 前复权基准日, 默认为 ``end_date``.
                因换算在本地进行, 该参数**不影响缓存内容**.
        """
        start_date, end_date = self._resolve_range(start_date, end_date, count)
        variant = f"{frequency}__post"
        req_fields = bar_fields_for(frequency)
        df = self._monthly_query(
            "get_price",
            security,
            start_date,
            end_date,
            variant=variant,
            call_builder=lambda sec, s, e: (
                f"get_price({sec!r}, start_date={s!r}, end_date={e!r}, "
                f"frequency={frequency!r}, fq='post', fields={req_fields!r})"
            ),
            date_col="",  # get_price 天然以日期为 index
        )
        if len(df) == 0:
            return df

        ref_factor = None
        if fq == "pre":
            ref_factor = self._resolve_ref_factor(
                security, frequency, pre_factor_ref_date, end_date, df
            )
        out = self._apply_fq(df, fq, ref_factor)

        if skip_paused and "paused" in out.columns:
            out = out[out["paused"] == 0]
        return self._check_fields(out, fields)

    def _resolve_ref_factor(
        self,
        security: str,
        frequency: str,
        ref_date: object | None,
        end_date: object,
        df: pd.DataFrame,
    ) -> float:
        """取前复权基准日的后复权因子.

        默认基准日为 ``end_date``(落在已取区间内, 零额外开销);
        显式指定区间外的基准日时, 额外取该日所在月的分片.
        """
        target = norm_date(ref_date) if ref_date is not None else norm_date(end_date)
        sub = df.loc[:target]
        if len(sub) > 0 and "factor" in sub.columns:
            return float(sub["factor"].iloc[-1])
        # 基准日在已取区间之前/之后 → 单独取该日所在月
        extra = self._monthly_query(
            "get_price",
            security,
            *month_bounds(target[:7]),
            variant=f"{frequency}__post",
            call_builder=lambda sec, s, e: (
                f"get_price({sec!r}, start_date={s!r}, end_date={e!r}, "
                f"frequency={frequency!r}, fq='post', fields={bar_fields_for(frequency)!r})"
            ),
            date_col="",
        )
        extra = extra.loc[:target]
        if len(extra) == 0 or "factor" not in extra.columns:
            raise JQExecutionError(
                f"无法确定 {security} 在 {target} 的复权因子(该日无数据)"
            )
        return float(extra["factor"].iloc[-1])

    def get_price_batch(
        self,
        securities: list[str],
        start_date: object | None = None,
        end_date: object | None = None,
        *,
        count: int | None = None,
        frequency: str = "daily",
        fq: str | None = "pre",
        fields: list[str] | None = None,
        skip_paused: bool = False,
        chunk_size: int = 50,
        max_symbol_months: int = 400,
    ) -> dict[str, pd.DataFrame]:
        """批量取行情: 一次远程调用取多只, 分别按月落盘.

        缓存布局与 :meth:`get_price` 完全一致 —— 批量取过的标的,
        后续单只查询直接命中缓存(反之亦然).

        Args:
            securities: 证券代码列表.
            count: 同 :meth:`get_price`, 与 ``start_date`` 二选一.
            chunk_size: 单次远程调用的**标的数**上限.
            max_symbol_months: 单次远程调用的**工作量**上限(标的数 × 月数).
                仅限标的数不够: 40 只×24 月(≈万行)会让聚宽 kernel 返回
                **空 stdout 且不报错**(实测), 因此长区间时必须再按工作量细分。
                与 :meth:`_batch_timeout` 同一工作量口径。

        Returns:
            ``{code: DataFrame}``, 键序与入参一致; 无数据的标的值为空 DataFrame.
        """
        start, end = self._resolve_range(start_date, end_date, count)
        variant = f"{frequency}__post"
        codes = list(dict.fromkeys(securities))

        # 1. 按"缺失月集合"分组, 同组标的可共用一次远程调用
        need: dict[tuple[str, str], list[str]] = {}
        for code in codes:
            missing = self.bars.missing_months(
                "get_price", code, start, end, variant
            )
            for gap in merge_consecutive_months(missing):
                need.setdefault(gap, []).append(code)

        # 2. 分块远程取数(当月部分不落盘, 靠 fresh 带出)
        #    分块尺寸同时受两个上限约束: 标的数(chunk_size) 与
        #    工作量(max_symbol_months = 标的数 × 月数)。后者不可缺 ——
        #    否则 40只×24月 会作为单次请求发出并得到空 stdout(无错误)。
        fresh: dict[str, list[pd.DataFrame]] = {}
        for (gap_start, gap_end), group in need.items():
            n_months = max(1, len(months_between(gap_start, gap_end)))
            by_workload = max(1, max_symbol_months // n_months)
            step = max(1, min(chunk_size, by_workload))
            for i in range(0, len(group), step):
                chunk = group[i : i + step]
                code_str = (
                    "from jqdata import *\n"
                    f"__result__ = get_price({chunk!r}, start_date={gap_start!r}, "
                    f"end_date={gap_end!r}, frequency={frequency!r}, "
                    f"fq='post', fields={bar_fields_for(frequency)!r}, panel=False)"
                )
                raw = self._fetch_raw(
                    code_str, timeout=self._batch_timeout(len(chunk), gap_start, gap_end)
                )
                per_code = self._store_batch_result(
                    raw, chunk, gap_start, gap_end, variant
                )
                for code, df in per_code.items():
                    if len(df) > 0:
                        fresh.setdefault(code, []).append(df)

        # 3. 从缓存 + 新取数据组装结果
        out: dict[str, pd.DataFrame] = {}
        for code in codes:
            df = self._combine(
                self.bars.read_range("get_price", code, start, end, variant),
                fresh.get(code, []),
                start,
                end,
            )
            if len(df) > 0:
                ref = None
                if fq == "pre" and "factor" in df.columns:
                    ref = float(df["factor"].iloc[-1])
                df = self._apply_fq(df, fq, ref)
                if skip_paused and "paused" in df.columns:
                    df = df[df["paused"] == 0]
                df = self._check_fields(df, fields)
            out[code] = df
        return out

    def _batch_timeout(self, n_symbols: int, start: str, end: str) -> float:
        """批量查询的自适应超时: 按 (标的数 × 月数) 规模线性放大.

        单次 50 只 × 多年的查询轻易超过固定超时, 因此以
        ``exec_timeout`` 为下限、按工作量递增.
        """
        n_months = max(1, len(months_between(start, end)))
        estimated = 15.0 + 0.35 * n_symbols * n_months
        return max(self.exec_timeout, estimated)

    def _store_batch_result(
        self,
        raw: pd.DataFrame,
        codes: list[str],
        gap_start: str,
        gap_end: str,
        variant: str,
    ) -> dict[str, pd.DataFrame]:
        """把 ``panel=False`` 的批量结果拆成 per-symbol 分片写盘.

        聚宽 ``panel=False`` 返回长表(列含 ``time`` / ``code``);
        为兼容旧版返回的 MultiIndex 列宽表, 两种形态都处理.

        Returns:
            ``{code: DataFrame}`` —— 本次取到的内存副本(含当月等不落盘部分).
        """
        out: dict[str, pd.DataFrame] = {}
        if not isinstance(raw, pd.DataFrame) or len(raw) == 0:
            for code in codes:  # 全部标记为已查(空月)
                self.bars.write_range(
                    pd.DataFrame(), "get_price", code, gap_start, gap_end, variant
                )
                out[code] = pd.DataFrame()
            return out

        if "code" in raw.columns:  # 长表
            time_col = "time" if "time" in raw.columns else raw.columns[0]
            for code in codes:
                sub = raw[raw["code"] == code].drop(columns=["code"])
                sub = self._to_date_index(sub, time_col)
                self.bars.write_range(
                    sub, "get_price", code, gap_start, gap_end, variant
                )
                out[code] = sub
        elif isinstance(raw.columns, pd.MultiIndex):  # (field, code) 宽表
            for code in codes:
                try:
                    sub = raw.xs(code, axis=1, level=-1)
                except KeyError:
                    sub = pd.DataFrame()
                self.bars.write_range(
                    sub, "get_price", code, gap_start, gap_end, variant
                )
                out[code] = sub
        else:
            raise JQExecutionError(
                f"批量取数返回了无法识别的结构, 列: {list(raw.columns)[:10]}"
            )
        return out

    # ==================================================================
    # 其他时间序列
    # ==================================================================

    def get_money_flow(
        self, security: str, start_date: object, end_date: object
    ) -> pd.DataFrame:
        """缓存版 ``get_money_flow``(资金流向). 含 ``available_at``(下一交易日 09:30)."""
        df = self._monthly_query(
            "get_money_flow",
            security,
            start_date,
            end_date,
            call_builder=lambda sec, s, e: (
                f"get_money_flow({sec!r}, start_date={s!r}, end_date={e!r})"
            ),
        )
        return self._attach_available_at(df, "get_money_flow")

    def get_mtss(
        self, security: str, start_date: object, end_date: object
    ) -> pd.DataFrame:
        """缓存版 ``get_mtss``(融资融券). 含 ``available_at``(下一交易日 09:30).

        交易所 T 日收盘后发布当日两融, 故 T 日盘中不可用 —— 见
        :data:`_PUBLICATION_TIMING` 与 :meth:`_attach_available_at`。
        """
        df = self._monthly_query(
            "get_mtss",
            security,
            start_date,
            end_date,
            call_builder=lambda sec, s, e: (
                f"get_mtss({sec!r}, start_date={s!r}, end_date={e!r})"
            ),
        )
        return self._attach_available_at(df, "get_mtss")

    def get_valuation(
        self,
        security: str,
        start_date: object,
        end_date: object,
        *,
        fields: list[str] | None = None,
    ) -> pd.DataFrame:
        """缓存版 ``get_valuation``(市值/估值).

        聚宽不传 fields 时会尝试查不存在的 ``pcf_ratio2``, 故显式传安全字段.
        日期列名为 ``day``.
        """
        safe = [
            "code", "day", "capitalization", "circulating_cap",
            "market_cap", "circulating_market_cap", "turnover_ratio",
            "pe_ratio", "pe_ratio_lyr", "pb_ratio", "ps_ratio", "pcf_ratio",
        ]
        df = self._monthly_query(
            "get_valuation",
            security,
            start_date,
            end_date,
            call_builder=lambda sec, s, e: (
                f"get_valuation({sec!r}, start_date={s!r}, end_date={e!r}, "
                f"fields={safe!r})"
            ),
            date_col="day",
        )
        return self._check_fields(df, fields)

    def _attach_available_at(
        self, df: pd.DataFrame, func_name: str
    ) -> pd.DataFrame:
        """注入 ``available_at`` 列 —— 数据日 T 的数据实际何时可用.

        发布语义见 :data:`_PUBLICATION_TIMING`。本层拥有这份元知识
        (“聚宽何时能给到数据”), 上层只消费结果, 不重建交易所规则。

        无发布规则登记的接口原样返回(不凭空造列)。
        """
        timing = _PUBLICATION_TIMING.get(func_name)
        if timing is None or not isinstance(df, pd.DataFrame) or len(df) == 0:
            return df
        kind, hh, mm = timing
        days = pd.DatetimeIndex(df.index).normalize()
        offset = pd.Timedelta(hours=hh, minutes=mm)
        if kind == "same_session":
            avail = days + offset
        else:  # next_session: 靠交易日历推下一交易日
            all_days = pd.DatetimeIndex(
                [pd.Timestamp(d) for d in self.get_all_trade_days()]
            )
            mapping = {}
            for d in days.unique():
                nxt = all_days[all_days > d]
                mapping[d] = (nxt[0] + offset) if len(nxt) else pd.NaT
            avail = pd.DatetimeIndex([mapping[d] for d in days])
        out = df.copy()
        out["available_at"] = avail
        return out

    def get_valuation_batch(
        self,
        securities: list[str],
        start_date: object,
        end_date: object,
        *,
        fields: list[str] | None = None,
        max_rows: int = _CONTINUOUSLY_ROW_LIMIT,
    ) -> pd.DataFrame:
        """批量取估值(市值/股本) —— 一次远程拿多 code 多日, 返回长表.

        背景: 单参数 :meth:`get_valuation` 不支持批量(传 list 只返回首只),
        但 ``get_fundamentals_continuously(query.filter(code.in_(...)))`` 支持批量
        code + 多日, 一次 RPC 拿回全部。
        批量取 300 只股本从逐只 300 次 RPC 降为数次。

        区间 ``[start, end]`` 内部换算为交易日数(count) ——
        ``get_fundamentals_continuously`` 只接 count 不接 start_date。

        Warning:
            聚宽对本接口有 **10000 行硬上限**, 超出直接**静默截断不报错**
            (实测 100只×242日 理论 24200 行 → 只返 10000 行)。静默少数据比报错
            更危险 —— 股本缺失会让下游市值/换手率静默出错。因此本层:

            1. 按 ``max_rows`` 自动分块(工作量 = 标的数 × 交易日数);
            2. 合并后**校验行数**, 不足预期则报错而非静默接受。

        Args:
            securities: 聚宽代码列表.
            fields: valuation 表字段(如 capitalization/circulating_cap), 返回必含 code/day.
            max_rows: 单次远程调用的行数上限(聚宽硬限 10000).

        Returns:
            长表 DataFrame, 列含 ``code`` / ``day`` / 请求字段; 无数据返回空表。
        """
        s, e = norm_date(start_date), norm_date(end_date)
        codes = list(dict.fromkeys(securities))
        # 区间 -> 交易日数: get_fundamentals_continuously 只接 count 不接 start_date
        # 注: get_trade_days 第一个位置参是 end_date, 必须用关键字参避免参数倒置
        sessions = list(self.get_trade_days(end_date=e, start_date=s))
        count = len(sessions)
        req = list(dict.fromkeys(["code", "day", *(fields or [])]))
        if count == 0 or not codes:
            return pd.DataFrame(columns=req)
        # day 不是 valuation 表的查询字段(由 continuously 自带), 故构造时排除
        query_cols = [c for c in req if c != "day"]
        cols_expr = ", ".join(f"valuation.{c}" for c in query_cols)

        # 按行数上限分块: 每块标的数 = max_rows // 交易日数
        per_chunk = max(1, max_rows // count)
        parts: list[pd.DataFrame] = []
        for i in range(0, len(codes), per_chunk):
            chunk = codes[i : i + per_chunk]
            user_code = (
                "from jqdata import *\n"
                "import pandas as _pd\n"
                f"_codes = {chunk!r}\n"
                f"_q = query({cols_expr}).filter(valuation.code.in_(_codes))\n"
                f"_m = get_fundamentals_continuously(_q, end_date={e!r}, "
                f"count={count}, panel=False)\n"
                # 去重列(聚宽返回 code/code.1/day/day.1), 只留首次出现
                "_m = _m.loc[:, ~_m.columns.duplicated()]\n"
                "if 'day' in _m.columns:\n"
                "    _m['day'] = _pd.to_datetime(_m['day'])\n"
                "__result__ = _m.reset_index(drop=True)"
            )
            key = ("valuation_batch", tuple(chunk), tuple(req), s, e)
            got = self._snapshot(
                "get_valuation_batch", key, user_code, today_key=e
            )
            if isinstance(got, pd.DataFrame) and len(got) > 0:
                parts.append(got)
        if not parts:
            return pd.DataFrame(columns=req)
        result = pd.concat(parts, ignore_index=True)

        # 行数校验: 静默截断必须被发现。
        # 判定依据: 分块后每块的**理论**行数已 ≤ max_rows, 所以只有当
        # 理论值本身就≥上限时, "返回值碰到上限"才意味着可能被截。
        # (否则恰好装满是正常的 —— 如 10只×10日=100 行遇 max_rows=100)
        for part in parts:
            n_codes_in_part = part["code"].nunique() if "code" in part else 0
            theoretical = n_codes_in_part * count
            if len(part) >= max_rows and theoretical > max_rows:
                raise JQExecutionError(
                    f"get_valuation_batch 单块返回 {len(part)} 行(理论 {theoretical} 行), "
                    f"已触及聚宽行数上限({max_rows}) —— 数据被**静默截断**。\n"
                    f"  本块 {n_codes_in_part} 只 × {count} 交易日。"
                    "请调小 max_rows 以加密分块。"
                )
        # 保留 code/day + 请求字段(不能用 _check_fields 切片 —— 那会丢掉 code/day)
        keep = [c for c in req if c in result.columns]
        return result[keep]

    def get_call_auction(
        self,
        security: str,
        start_date: object,
        end_date: object,
        *,
        fields: list[str] | None = None,
    ) -> pd.DataFrame:
        """缓存版 ``get_call_auction``(集合竞价). ``time`` 列含时分秒, 截断到日分片.

        含 ``available_at`` = **当日** 09:30 —— 与两融/资金流不同, 09:25 竞价结束
        即确定, 当日盘中即可用(盘中策略能用它的前提)。

        Warning:
            **只接单标的**。聚宽虽然允许传 list, 但多标的时有
            10000 行硬上限且**静默截断不报错**(实测 50只×242日
            理论 12100 行 → 只返 10000 行)。本层因此 fail-loud 拒绝 list,
            调用方应逐只循环(单只×242日=242 行, 远低于上限)。

        Raises:
            TypeError: ``security`` 为 list/tuple 时 —— 防止静默截断。
        """
        if isinstance(security, (list, tuple, set)):
            raise TypeError(
                f"get_call_auction 只接单标的, 传入了 {type(security).__name__}"
                f"(长度 {len(security)})。\n"
                f"  原因: 聚宽多标的竞价有 {_CONTINUOUSLY_ROW_LIMIT} 行硬上限且"
                "**静默截断不报错**, 会悄悄丢数据。\n"
                "  出路: 逐只循环调用(单只×242日=242 行, 安全)。"
            )
        df = self._monthly_query(
            "get_call_auction",
            security,
            start_date,
            end_date,
            call_builder=lambda sec, s, e: (
                f"get_call_auction({sec!r}, start_date={s!r}, end_date={e!r})"
            ),
            date_col="time",
            truncate_date=True,
        )
        df = self._attach_available_at(df, "get_call_auction")
        if fields:
            # available_at 是本层注入的元数据列, 不应被 fields 切片丢弃
            fields = list(dict.fromkeys([*fields, "available_at"]))
        return self._check_fields(df, fields)

    def get_extras(
        self,
        info: str,
        security_list: object,
        start_date: object,
        end_date: object,
    ) -> pd.DataFrame:
        """缓存版 ``get_extras``(is_st / 基金净值 / 期货等).

        标的列表参与缓存路径(过长自动 hash), 数据天然以日期为 index.
        """
        secs = (
            list(security_list)
            if isinstance(security_list, (list, tuple))
            else [security_list]
        )
        return self._monthly_query(
            "get_extras",
            secs,
            start_date,
            end_date,
            variant=str(info),
            call_builder=lambda sec, s, e: (
                f"get_extras({info!r}, {list(sec)!r}, "
                f"start_date={s!r}, end_date={e!r})"
            ),
            date_col="",
        )

    # ==================================================================
    # 快照类
    # ==================================================================

    def _snapshot(
        self,
        func_name: str,
        key_parts: object,
        user_code: str,
        *,
        today_key: object | None = None,
        asof: bool = False,
    ):
        """快照缓存: 命中即返回, 否则远程取数并写盘.

        Args:
            today_key: 数据对应的时点. 是今天或未来时不落盘(数据可能不完整).
            asof: 该聚宽接口**没有** date 参数, 只能返回"当前状态"(如概念目录).
                此时按**取数日**分片缓存: 当天内命中, 跨天自然换 key 重取,
                历史 key 保留下来形成名录的时点序列.
        """
        if asof:
            parts: tuple = (f"asof-{date.today()}",)
            if key_parts not in (None, "all"):
                extra = key_parts if isinstance(key_parts, tuple) else (key_parts,)
                parts = parts + extra
            key_parts = parts

        cached = self.snapshots.get(func_name, key_parts)
        if cached is not None:
            return cached
        result = self._fetch_raw(user_code)
        # asof 分片的名录类数据当天即完整(不像日线要等收盘), 可以落盘
        volatile = (
            not asof and today_key is not None and is_today_or_future(today_key)
        )
        if not volatile and result is not None:
            self.snapshots.put(func_name, key_parts, result)
        return result

    def run_cached(
        self,
        func_name: str,
        cache_key: object,
        user_code: str,
        today_key: object,
    ):
        """通用缓存执行(适用于 query 对象无法序列化的函数).

        ``get_fundamentals`` / ``finance.run_query`` 的 query 对象无法跨环境传输,
        调用方提供完整构造代码, 本层按 ``cache_key`` 做快照缓存.

        Args:
            func_name: 缓存子目录名.
            cache_key: 缓存键(字符串或元组).
            user_code: 远程代码, 末尾须赋值给 ``__result__``.
            today_key: **必填** —— 这份结果对应的数据时点. 为今天/未来时不落盘.
                本参数故意不给默认值: run_cached 是任意查询的逃生舱, 若允许缺省
                就会把“取到最新”的浮动结果当成永久事实写进缓存.
        """
        return self._snapshot(func_name, cache_key, user_code, today_key=today_key)

    def get_fundamentals(
        self,
        table: str,
        codes: list[str],
        fields: list[str],
        end_date: object,
        start_date: object | None = None,
        *,
        report_type: int | None = None,
    ) -> pd.DataFrame:
        """按披露日(pub_date)取财务报表, 返回长表 DataFrame.

        这是 jq 对 ``finance.run_query`` 的一等封装: 调用方只给表名 + 字段,
        query 对象的构造与跨环境净化都在本层完成(不再需调用方拼代码串)。

        Args:
            table: ``finance`` 下的表名, 如 ``STK_INCOME_STATEMENT`` /
                ``STK_BALANCE_SHEET`` / ``STK_CASHFLOW_STATEMENT`` / ``STK_FIN_FORCAST``.
            codes: 聚宽代码列表(如 ``600519.XSHG``).
            fields: 该表要取的字段名(不含表前缀).
            end_date: **必填** —— 按 ``pub_date <= end_date`` 过滤(PIT 可见性).
            start_date: 可选下限, 按 ``pub_date >= start_date`` 过滤.
            report_type: 只取指定 report_type(正式表传 0 = 本期, 避免 =1 追溯重复).

        Note:
            按披露日而非报告期过滤 —— 这是唯一 PIT 正确的方式("截至某日
            市场能看到的报告")。end_date 为今天/未来时不落盘。

            ``finance.run_query`` 有 **5000 行硬上限**(超出静默截断),
            本层已在远程侧用 ``offset`` 分页拉完 —— 调用方无需关心。

            **缓存边界对齐到月末**: 相邻交易日的 end_date 仅差一天会让缓存键
            全 miss(按日滞动查询时雪崩)。因财报披露是离散事件、月内无新增,
            本层把 **查询与缓存边界都上取整到 end_date 所在月的月末**,
            同月内所有日共享一份缓存。返回的数据可能含 end_date 之后、月末之前
            披露的记录(对 end_date 而言是未来), 故**调用方必须自行按精确时点
            做 PIT 过滤**(见 qlab ``latest_fundamental_as_of`` / ``ttm_value``,
            均按 ``available_at <= date`` 精确截断)。
        """
        e_exact = norm_date(end_date)
        # 对齐到月末: 同月内所有查询共享缓存, PIT 精度由调用方保证
        _, e = month_bounds(e_exact[:7])
        s = norm_date(start_date) if start_date is not None else None
        codes = list(dict.fromkeys(codes))
        # 字段去重且必含 pub_date/end_date/code(供本地对齐与 PIT)
        req_fields = list(
            dict.fromkeys(["code", "pub_date", "end_date", *fields])
        )
        filters = [
            f"_t.code.in_({codes!r})",
            f"_t.pub_date <= {e!r}",
        ]
        if s is not None:
            filters.append(f"_t.pub_date >= {s!r}")
        if report_type is not None:
            filters.append(f"_t.report_type == {report_type!r}")
        user_code = (
            "from jqdata import *\n"
            "import pandas as _pd\n"
            f"_t = finance.{table}\n"
            f"_cols = {req_fields!r}\n"
            "_q = query(*[getattr(_t, _c) for _c in _cols]).filter("
            + ", ".join(filters)
            + ")\n"
            # finance.run_query 有 5000 行硬上限(limit(20000) 也只给 5000),
            # 超出**静默截断不报错** —— 必须用 offset 分页拉完。
            f"_LIM = {_RUN_QUERY_ROW_LIMIT}\n"
            "_pages = []\n"
            "_off = 0\n"
            "while True:\n"
            "    _p = finance.run_query(_q.offset(_off).limit(_LIM))\n"
            "    if _p is None or len(_p) == 0:\n"
            "        break\n"
            "    _pages.append(_p)\n"
            "    if len(_p) < _LIM:\n"
            "        break\n"
            "    _off += _LIM\n"
            "    if _off > 200000:\n"        # 安全阀: 避免条件写错时无限翻页
            "        raise RuntimeError('finance 分页超过 200000 行, 请收紧查询条件')\n"
            "_m = _pd.concat(_pages, ignore_index=True) if _pages else _pd.DataFrame()\n"
            "for _c in ('pub_date', 'end_date', 'report_date', 'start_date'):\n"
            "    if _c in _m.columns:\n"
            "        _m[_c] = _pd.to_datetime(_m[_c])\n"
            "__result__ = _m.reset_index(drop=True)"
        )
        key = (
            table, tuple(codes), tuple(req_fields), s, e, report_type,
        )
        result = self._snapshot(
            f"fundamentals__{table}", key, user_code, today_key=e
        )
        return result if isinstance(result, pd.DataFrame) else pd.DataFrame()

    def get_all_securities(
        self, date: object, types: list[str] | None = None
    ) -> pd.DataFrame:
        """缓存版 ``get_all_securities``.

        Args:
            date: **必填** —— 证券名录随上市/退市变化, 不绑定时点就是未来函数.
        """
        d = norm_date(date)
        return self._snapshot(
            "get_all_securities",
            (types or "default", d),
            f"from jqdata import *\n"
            f"__result__ = get_all_securities(types={types!r}, date={d!r})",
            today_key=d,
        )

    def get_security_info(self, code: str, date: object) -> dict:
        """缓存版 ``get_security_info``. Security 对象不可跨环境 pickle, 远程转 dict.

        Args:
            date: **必填** —— 证券信息(如 ``end_date``)会随退市变化.
        """
        d = norm_date(date)
        return self._snapshot(
            "get_security_info",
            (code, d),
            f"from jqdata import *\n"
            f"_s = get_security_info({code!r}, date={d!r})\n"
            f"__result__ = {{k: getattr(_s, k) for k in "
            f"['code', 'name', 'display_name', 'type', 'start_date', "
            f"'end_date', 'parent'] if hasattr(_s, k)}}",
            today_key=d,
        )

    def get_all_trade_days(self):
        """缓存版 ``get_all_trade_days``(全部交易日).

        聚宽该接口无 date 参数, 返回的日历会向后延伸(实测到两年后)且可能因调休
        变更, 故按取数日分片缓存 —— 当天内命中, 跨天自动刷新.
        """
        return self._snapshot(
            "get_all_trade_days",
            "all",
            "from jqdata import *\n__result__ = get_all_trade_days()",
            asof=True,
        )

    def get_trade_days(
        self,
        end_date: object,
        start_date: object | None = None,
        count: int | None = None,
    ):
        """缓存版 ``get_trade_days``.

        Args:
            end_date: **必填** —— 缺省就是“取到最新”, 那是个浮动区间,
                缓存下来下次就错位了. 为今天/未来时不落盘.
            start_date / count: 二选一(同聚宽原生语义).
        """
        e = norm_date(end_date)
        s = norm_date(start_date) if start_date is not None else None
        return self._snapshot(
            "get_trade_days",
            (s, e, count),
            f"from jqdata import *\n"
            f"__result__ = get_trade_days("
            f"start_date={s!r}, end_date={e!r}, count={count!r})",
            today_key=e,
        )

    def get_industries(self, date: object, name: str = "zjw") -> pd.DataFrame:
        """缓存版 ``get_industries``(行业分类目录).

        Args:
            date: **必填** —— 行业分类目录会调整.
        """
        d = norm_date(date)
        return self._snapshot(
            "get_industries",
            (name, d),
            f"from jqdata import *\n"
            f"__result__ = get_industries(name={name!r}, date={d!r})",
            today_key=d,
        )

    def get_industry_stocks(self, industry_code: str, date: object) -> list[str]:
        """缓存版 ``get_industry_stocks``(行业成分股)."""
        d = norm_date(date)
        return self._snapshot(
            "get_industry_stocks",
            (industry_code, d),
            f"from jqdata.apis import *\n"
            f"__result__ = get_industry_stocks({industry_code!r}, {d!r})",
            today_key=d,
        )

    def get_industry(self, security: object, date: object):
        """缓存版 ``get_industry``(个股全部行业分类). 支持单只或列表."""
        d = norm_date(date)
        return self._snapshot(
            "get_industry",
            (security, d),
            f"from jqdata.apis import *\n"
            f"__result__ = get_industry({security!r}, {d!r})",
            today_key=d,
        )

    def get_concepts(self) -> pd.DataFrame:
        """缓存版 ``get_concepts``(全部概念目录).

        聚宽该接口无 date 参数, **只能返回当前概念目录**(概念会持续新增),
        故按取数日分片缓存 —— 当天内命中, 跨天自动刷新.

        Warning:
            返回值永远是"当前"目录, 不是历史时点的目录. 需要个股在某历史时点的
            概念归属, 用 :meth:`get_concept` (该接口支持 date).
        """
        return self._snapshot(
            "get_concepts",
            "all",
            "from jqdata import *\n__result__ = get_concepts()",
            asof=True,
        )

    def get_concept(self, security: object, date: object):
        """缓存版 ``get_concept``(个股所属概念). 支持单只或列表."""
        d = norm_date(date)
        return self._snapshot(
            "get_concept",
            (security, d),
            f"from jqdata.apis import *\n"
            f"__result__ = get_concept({security!r}, {d!r})",
            today_key=d,
        )

    def get_billboard_list(
        self,
        end_date: object,
        stock_list: list[str] | None = None,
        count: int = 5,
        max_days_per_call: int = 5,
    ) -> pd.DataFrame:
        """缓存版 ``get_billboard_list``(龙虎榜). ``count`` 是天数(非行数).

        Args:
            end_date: **必填** —— 不绑定时点会默认取到最新, 缓存下来就错位了.
            stock_list: 限定标的; None 表示全市场。
            max_days_per_call: 单次远程调用的天数上限。

        Note:
            全市场龙虎榜约 800~900 行/天, ``count=30`` 就是 2.6 万行 ——
            超出聚宽 kernel 输出能力会返回**空 stdout**(实测)。
            故按 ``max_days_per_call`` 分段取数再拼接; 每段独立缓存,
            相邻区间重叠部分能直接命中。
        """
        e = norm_date(end_date)
        if count <= max_days_per_call:
            return self._billboard_chunk(e, stock_list, count)

        # 按天数分段: 从 end_date 往前逐段取, 每段结束日往前移 max_days_per_call 个交易日
        sessions = [pd.Timestamp(d) for d in self.get_all_trade_days()]
        upto = [d for d in sessions if str(d.date()) <= e]
        if not upto:
            return self._billboard_chunk(e, stock_list, count)
        window = upto[-count:] if len(upto) >= count else upto

        parts: list[pd.DataFrame] = []
        i = len(window)
        while i > 0:
            lo = max(0, i - max_days_per_call)
            seg_end = str(window[i - 1].date())
            seg_days = i - lo
            got = self._billboard_chunk(seg_end, stock_list, seg_days)
            if isinstance(got, pd.DataFrame) and len(got) > 0:
                parts.append(got)
            i = lo
        if not parts:
            return pd.DataFrame()
        out = pd.concat(parts, ignore_index=True)
        # 分段边界可能重叠 —— 按全列去重
        return out.drop_duplicates().reset_index(drop=True)

    def _billboard_chunk(
        self, end_date: str, stock_list: list[str] | None, count: int
    ) -> pd.DataFrame:
        """单段龙虎榜取数(带缓存)."""
        return self._snapshot(
            "get_billboard_list",
            (stock_list or "all", end_date, count),
            f"from jqdata import *\n"
            f"__result__ = get_billboard_list("
            f"stock_list={stock_list!r}, end_date={end_date!r}, count={count!r})",
            today_key=end_date,
        )

    def get_factor_values(
        self,
        securities: object,
        factors: object,
        end_date: object,
        start_date: object | None = None,
        count: int | None = None,
    ) -> dict[str, pd.DataFrame]:
        """缓存版 ``jqfactor.get_factor_values``. 返回 dict, 整体快照缓存.

        Args:
            end_date: **必填** —— 缺省会取到最新数据, 缓存下来就错位了.
            start_date / count: 二选一(同聚宽原生语义).
        """
        e = norm_date(end_date)
        s = norm_date(start_date) if start_date is not None else None
        return self._snapshot(
            "get_factor_values",
            (securities, factors or "all", s, e, count),
            f"import jqfactor\n"
            f"__result__ = jqfactor.get_factor_values("
            f"securities={securities!r}, factors={factors!r}, "
            f"start_date={s!r}, end_date={e!r}, count={count!r})",
            today_key=e,
        )

    def get_all_factors(self) -> pd.DataFrame:
        """缓存版 ``jqfactor.get_all_factors``(因子目录).

        聚宽该接口无 date 参数, 按取数日分片缓存(因子清单会新增).
        """
        return self._snapshot(
            "get_all_factors",
            "all",
            "import jqfactor\n__result__ = jqfactor.get_all_factors()",
            asof=True,
        )

    def get_index_stocks(self, index_symbol: str, date: object) -> list[str]:
        """缓存版 ``get_index_stocks``(指数成分股).

        Args:
            date: **必填** —— 成分股随指数调样变化.
        """
        d = norm_date(date)
        return self._snapshot(
            "get_index_stocks",
            (index_symbol, d),
            f"from jqdata import *\n"
            f"__result__ = get_index_stocks({index_symbol!r}, date={d!r})",
            today_key=d,
        )

    def get_index_weights(self, index_symbol: str, date: object) -> pd.DataFrame:
        """缓存版 ``get_index_weights``(指数成分股权重).

        返回 index=代码, 列含 ``date`` / ``weight`` / ``display_name``。

        Args:
            date: **必填** —— 权重随调样与市值变化.

        Note:
            聚宽的权重数据是**月度**的: 返回的 ``date`` 列是该权重的实际生效日
            (月末), 不等于请求的 ``date``。实测: 查 2024-06-03 返回 2024-05-31,
            查 2024-07-15 返回 2024-06-28。因此逐日查询是浪费 —— 按月采样即可。
            ``weight`` 是**百分数**(全部成分合计 ≈ 100)。
        """
        d = norm_date(date)
        return self._snapshot(
            "get_index_weights",
            (index_symbol, d),
            f"from jqdata import *\n"
            f"__result__ = get_index_weights({index_symbol!r}, date={d!r})",
            today_key=d,
        )

    # ==================================================================
    # 缓存管理
    # ==================================================================

    def status(self) -> dict:
        """返回缓存统计信息(按 bars / snapshot 分组)."""
        root = self.cache_dir / CACHE_VERSION
        out: dict = {
            "cache_dir": str(self.cache_dir),
            "version": CACHE_VERSION,
            "current_month": current_month(),
            "total_files": 0,
            "total_size_mb": 0.0,
            "bars": {},
            "snapshot": {},
        }
        if not root.exists():
            return out
        total_size = 0
        total_files = 0
        for section in ("bars", "snapshot"):
            sec_dir = root / section
            if not sec_dir.exists():
                continue
            for kind_dir in sorted(sec_dir.iterdir()):
                if not kind_dir.is_dir():
                    continue
                files = list(kind_dir.rglob("*.pkl"))
                size = sum(f.stat().st_size for f in files)
                total_size += size
                total_files += len(files)
                entry = {"files": len(files), "size_kb": round(size / 1024, 1)}
                if section == "bars":
                    entry["symbols"] = sum(
                        1 for d in kind_dir.iterdir() if d.is_dir()
                    )
                out[section][kind_dir.name] = entry
        out["total_files"] = total_files
        out["total_size_mb"] = round(total_size / 1024 / 1024, 2)
        return out

    def clear(
        self, older_than_days: int | None = None, *, all_versions: bool = False
    ) -> int:
        """清空缓存, 返回删除的文件数.

        Args:
            older_than_days: 仅删 N 天前的文件; None 删全部.
            all_versions: 是否连旧版本目录一起删(默认只删当前版本).
        """
        targets = (
            [self.cache_dir]
            if all_versions
            else [self.cache_dir / CACHE_VERSION]
        )
        cutoff = time.time() - older_than_days * 86400 if older_than_days else None
        count = 0
        for base in targets:
            if not base.exists():
                continue
            for f in base.rglob("*.pkl"):
                if cutoff and f.stat().st_mtime > cutoff:
                    continue
                f.unlink(missing_ok=True)
                count += 1
            # 自底向上清理空目录
            for d in sorted(
                (p for p in base.rglob("*") if p.is_dir()),
                key=lambda p: len(p.parts),
                reverse=True,
            ):
                if not any(d.iterdir()):
                    d.rmdir()
        return count

    def prune_stale_months(self) -> int:
        """删除所有"当月及未来月"的分片(它们本不该落盘, 用于修复历史脏数据)."""
        root = self.cache_dir / CACHE_VERSION / "bars"
        if not root.exists():
            return 0
        cur = current_month()
        count = 0
        for f in root.rglob("*.pkl"):
            if f.stem >= cur:
                f.unlink(missing_ok=True)
                count += 1
        return count


__all__ = [
    "DEFAULT_CACHE_DIR",
    "DataCache",
    "current_month",
    "is_today_or_future",
    "month_bounds",
    "months_between",
    "norm_date",
]
