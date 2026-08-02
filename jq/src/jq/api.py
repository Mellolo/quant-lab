"""jqdata API 参考存根.

聚宽 jqdata 包只在远程研究环境 kernel 里可用,本地无法 import.
本模块不提供可调用实现,只记录所有已实测验证的函数签名、入参、返回值与文档,
作为类型存根与 API 参考供 IDE / agent / 开发者查阅.

本地要**实际取数**请用带缓存的 :class:`jq.cache.DataCache`::

    from jq.cache import DataCache
    dc = DataCache()
    df = dc.get_price('600519.XSHG', '2026-07-01', '2026-07-21')

或用 CLI 在远程 kernel 执行任意 jqdata 代码::

    jqdata run --file my_query.py

查阅签名与文档::

    import jq.api
    help(jq.api.get_price)

所有签名均经 ``jqdata run`` + ``inspect.signature`` 实测确认.
标注 *动态代理* 的函数聚宽通过 ``__getattr__`` 注册,inspect 无法直接获取签名,
此处签名由实测调用确认.

Note:
    本模块的函数**不可调用** —— 直接调用会抛 :class:`NotImplementedError`
    (而非静默返回 ``None``),以免误用.签名、docstring 与 IDE 提示均完整保留.
"""

from __future__ import annotations

import functools
from typing import Any

import pandas as pd

__all__ = [
    "get_price",
    "get_bars",
    "get_all_securities",
    "get_security_info",
    "get_trade_days",
    "get_all_trade_days",
    "get_industries",
    "get_industry_stocks",
    "get_industry",
    "get_concepts",
    "get_concept",
    "get_fundamentals",
    "get_history_fundamentals",
    "get_valuation",
    "run_query",
    "get_billboard_list",
    "get_money_flow",
    "get_mtss",
    "get_call_auction",
    "get_factor_values",
    "get_all_factors",
    "get_index_stocks",
    "normalize_code",
    "get_extras",
]


# ---------------------------------------------------------------------------
# 行情 / K线
# ---------------------------------------------------------------------------


def get_price(  # noqa: D401
    security: str | list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    frequency: str = "daily",
    fields: list[str] | None = None,
    skip_paused: bool = False,
    fq: str | None = "pre",
    count: int | None = None,
    panel: bool = True,
) -> pd.DataFrame:
    """获取历史行情数据 (*动态代理*).

    Args:
        security: 证券代码或代码列表. 深市 ``XXXXXX.XSHE``, 沪市 ``XXXXXX.XSHG``.
        start_date: 开始日期 ``YYYY-MM-DD``.
        end_date: 结束日期 ``YYYY-MM-DD``.
        frequency: ``daily``/``minute``/``weekly``/``monthly``.
        fields: 字段列表, 默认全部. 常用 ``open/close/high/low/volume/money``.
        skip_paused: 跳过停牌日期.
        fq: ``pre`` 前复权(默认)/``post`` 后复权/``None`` 不复权.
        count: 与 ``start_date`` 二选一, 取 ``end_date`` 前 N 个交易日.
        panel: 多股票时返回 MultiIndex.

    Returns:
        单股票 DataFrame(index=日期, columns=fields);
        多股票 MultiIndex DataFrame(columns=(field, code)).

    Note:
        **gotcha**: ``count=N`` 不传 ``end_date`` 时, 默认 end_date **不是今天**
        (实测落到 2015). 取"最近 N 日"务必显式传 ``end_date``.
    """


def get_bars(  # noqa: D401
    security: str,
    count: int,
    unit: str = "1d",
    fields: list[str] | None = None,
    include_now: bool = False,
    end_dt: str | None = None,
    fq_ref_date: str | None = None,
) -> Any:
    """获取K线数据 (*动态代理*).

    Args:
        security: 证券代码.
        count: 返回 K 线根数.
        unit: K线周期 ``1d``/``5m``/``1m``/``30m``/``60m`` 等.
        fields: 字段列表, 常用 ``date/open/high/low/close/volume/money``.
        include_now: 是否包含当日未收盘数据.
        end_dt: 截止时间.
        fq_ref_date: 复权基准日.

    Returns:
        numpy.recarray (非 DataFrame). 用 ``bars['close']`` 访问字段,
        或 ``pd.DataFrame(bars)`` 转换.
    """


# ---------------------------------------------------------------------------
# 股票池 / 证券信息
# ---------------------------------------------------------------------------


def get_all_securities(  # noqa: D401
    types: list[str] | None = None,
    date: str | None = None,
) -> pd.DataFrame:
    """获取所有证券列表 (*动态代理*).

    Args:
        types: 证券类型列表, 可选 ``stock``/``index``/``etf``/``futures`` 等.
            默认 ``['stock']``.
        date: 截止日期, 省略取最新列表.

    Returns:
        DataFrame, columns: ``display_name``(中文) / ``name``(拼音) /
        ``start_date`` / ``end_date`` / ``type``.
    """


def get_security_info(  # noqa: D401
    code: str,
    date: str | None = None,
) -> Any:
    """获取单只证券基本信息.

    Args:
        code: 证券代码, 如 ``'600519.XSHG'``.
        date: 指定日期(证券可能更名/退市).

    Returns:
        Security 对象(非 DataFrame), 属性:
        ``code`` / ``name``(拼音) / ``display_name``(中文) /
        ``type`` / ``start_date`` / ``end_date`` / ``parent``.
    """


# ---------------------------------------------------------------------------
# 交易日历
# ---------------------------------------------------------------------------


def get_all_trade_days() -> Any:
    """获取 A 股截止到今年的所有交易日列表.

    无需传参, 返回完整历史交易日.

    Returns:
        numpy.ndarray, 日期数组.
    """


def get_trade_days(  # noqa: D401
    start_date: str | None = None,
    end_date: str | None = None,
    count: int | None = None,
) -> Any:
    """获取交易日列表.

    Args:
        start_date: 开始日期. 与 end_date 配合查区间; 与 count 配合查从该日起的
            count 个交易日.
        end_date: 结束日期. 与 start_date 配合查区间; 与 count 配合查截止该日
            前 count 个交易日.
        count: 天数.

    Note:
        start_date / end_date / count 三选二, 或单独传 start_date + count.

    Returns:
        numpy.ndarray, 日期数组.

    Examples:
        >>> get_trade_days(start_date='2026-07-01', end_date='2026-07-21')
        >>> get_trade_days(count=10)  # 截止今天的 10 个交易日
    """


# ---------------------------------------------------------------------------
# 行业
# ---------------------------------------------------------------------------


def get_industries(  # noqa: D401
    name: str = "zjw",
    date: str | None = None,
) -> pd.DataFrame:
    """获取行业分类列表.

    Args:
        name: 行业分类体系. ``sw_l1``(申万一级,38) /
            ``sw_l2``(申万二级,182) / ``zjw``(证监会,94,默认).
            ``sw``/``jq``/``zdcy`` 已空.
        date: 截止日期.

    Returns:
        DataFrame, index=行业代码, columns: ``name`` / ``start_date``.
    """


def get_industry_stocks(industry_code: str, date: str) -> list[str]:
    """获取某行业的成分股列表.

    聚宽函数名为 ``get_industry_stocks``(非 get_industry_securities),
    在 ``jqdata.apis`` 子模块中, 需 ``from jqdata.apis import *``.

    Args:
        industry_code: 行业代码, 如 ``'801120'``(申万一级食品饮料).
        date: 查询日期.

    Returns:
        list[code], 证券代码列表.
    """


def get_industry(  # noqa: D401
    security: str,
    date: str,
) -> dict[str, dict[str, dict[str, str]]]:
    """获取个股的全部行业分类 (*动态代理*).

    一次拿全 6 套分类体系, 区别于 ``get_industries``(列全部行业目录).

    Args:
        security: 证券代码.
        date: 查询日期.

    Returns:
        ``{code: {jq_l1/jq_l2/sw_l1/sw_l2/sw_l3/zjw:
        {industry_code, industry_name}}}``.
    """


# ---------------------------------------------------------------------------
# 概念
# ---------------------------------------------------------------------------


def get_concepts() -> pd.DataFrame:
    """获取全部概念列表.

    Returns:
        DataFrame, index=概念代码, columns: ``name`` / ``start_date``.
    """


def get_concept(  # noqa: D401
    security: str,
    date: str,
) -> dict[str, dict[str, list[dict[str, str]]]]:
    """获取个股所属概念 (*动态代理*).

    Args:
        security: 证券代码.
        date: 查询日期.

    Returns:
        ``{code: {jq_concept: [{concept_code, concept_name}]}}``.
    """


# ---------------------------------------------------------------------------
# 财务
# ---------------------------------------------------------------------------


def get_fundamentals(  # noqa: D401
    query_object: Any,
    statDate: str | None = None,  # noqa: N803
) -> pd.DataFrame:
    """获取财务估值数据 (*动态代理*).

    Args:
        query_object: SQLAlchemy query 对象, 字段格式 ``表名.字段``.
            可用表: ``valuation``(估值) / ``indicator``(指标) /
            ``income`` / ``balance`` / ``cash_flow``(简易表).
        statDate: 报告期. ``'YYYYqN'`` 季度(如 ``'2024q1'``)或 ``'YYYY'`` 年报.

    Returns:
        DataFrame, 列为 query 中指定的字段.

    Examples:
        >>> q = query(valuation.code, valuation.pe_ratio) \\
        ...     .filter(valuation.code == '600519.XSHG')
        >>> get_fundamentals(q, statDate='2024q1')
    """


def get_history_fundamentals(  # noqa: D401
    security: str | list[str],
    fields: list[Any],
    watch_date: str | None = None,
    stat_date: str | None = None,
    count: int = 1,
    interval: str = "1q",
    stat_by_year: bool = False,
) -> pd.DataFrame:
    """获取多个季度/年度的三大财务报表和财务指标数据.

    比 ``finance.run_query`` 更适合做时间序列分析——一次取多个报告期.

    Args:
        security: 股票代码或列表.
        fields: 要查询的财务数据列, 示例::

            [
                balance.cash_equivalents,
                cash_flow.net_deposit_increase,
                income.total_operating_revenue,
            ]
        watch_date: 观察日期. 指定后返回该日前(含)发布的报表数据.
        stat_date: 统计日期, ``'2019'``/``'2019q1'``/``'2018q4'`` 等.
            指定后返回该报告期及之前的历史报告期数据.
        count: 查询的历史报告期数量.
        interval: 报告期间隔. ``'1q'`` 一季度(默认) / ``'1y'`` 一年.
        stat_by_year: 是否按年度统计.

    Note:
        ``watch_date`` 和 ``stat_date`` 只能指定一个, 且必须指定一个.

    Returns:
        DataFrame, 包含多个报告期的财务数据.
    """


def get_valuation(  # noqa: D401
    security: str | list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    fields: list[str] | None = None,
    count: int | None = None,
) -> pd.DataFrame:
    """获取市值表数据(估值指标).

    比 ``get_fundamentals`` 更直接——支持时间范围查询, 不需要 statDate.

    Args:
        security: 标的代码或列表.
        start_date: 开始日期, 不能与 count 共用.
        end_date: 结束日期.
        count: 往前查询 N 个交易日的数据.
        fields: 市值表字段, 返回结果总会包含 ``code`` / ``day``. 可用字段::

            code                  股票代码(带后缀)
            day                   日期
            capitalization        总股本(万股)
            circulating_cap       流通股本(万股)
            market_cap            总市值(亿元)
            circulating_market_cap 流通市值(亿元)
            turnover_ratio        换手率(%)
            pe_ratio              市盈率(PE, TTM)
            pe_ratio_lyr          市盈率(PE)
            pb_ratio              市净率(PB)
            ps_ratio              市销率(PS, TTM)
            pcf_ratio             市现率(PCF, 现金净流量TTM)
            pcf_ratio2            市现率(PCF, ...)

    Returns:
        DataFrame, columns 包含 code/day + 指定 fields.
    """


def run_query(query_object: Any) -> pd.DataFrame:
    """执行财务报表查询 (``finance.run_query``).

    Args:
        query_object: SQLAlchemy query 对象.

    Note:
        **签名只接 query_object**, 不接 statDate/count/date.
        限行用 ``query.limit(N)``, 排序用 ``query.order_by()``.

    可用报表(``finance.STK_*``):
        ``STK_INCOME_STATEMENT``(利润表,68列) /
        ``STK_BALANCE_STATEMENT``(资产负债表) /
        ``STK_CASH_FLOW_STATEMENT``(现金流量表).

    关键字段(利润表):
        ``code`` / ``end_date``(报告期) / ``pub_date``(披露日) /
        ``report_type``(0=合并) /
        ``total_operating_revenue``(营业总收入) /
        ``net_profit``(净利润) /
        ``np_parent_company_owners``(归母净利润).

    Examples:
        >>> q = query(finance.STK_INCOME_STATEMENT) \\
        ...     .filter(finance.STK_INCOME_STATEMENT.code == '600519.XSHG') \\
        ...     .order_by(finance.STK_INCOME_STATEMENT.end_date.desc()) \\
        ...     .limit(4)
        >>> finance.run_query(q)
    """


# ---------------------------------------------------------------------------
# 龙虎榜 / 资金流 / 融资融券 / 集合竞价
# ---------------------------------------------------------------------------


def get_billboard_list(  # noqa: D401
    stock_list: list[str] | None = None,
    end_date: str | None = None,
    count: int = 5,
) -> pd.DataFrame:
    """获取龙虎榜数据 (*动态代理*).

    Args:
        stock_list: 股票代码列表, ``None`` 表示全市场.
        end_date: 截止日期.
        count: **天数**(最近 N 日), 非行数.

    Returns:
        DataFrame(14列): ``code`` / ``day`` / ``direction`` / ``rank`` /
        ``abnormal_code`` / ``abnormal_name`` / ``sales_depart_name`` /
        ``buy_value`` / ``buy_rate`` / ``sell_value`` / ``sell_rate`` /
        ``total_value`` / ``net_value`` / ``amount``.

    Note:
        聚宽无 ``get_billboard_detail`` 函数, 明细已含在返回的 14 列中.
    """


def get_money_flow(  # noqa: D401
    security_list: str | list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    fields: list[str] | None = None,
    count: int | None = None,
) -> pd.DataFrame:
    """获取资金流向数据.

    Args:
        security_list: 股票代码或代码列表.
        start_date: 开始日期. **与 count 二选一**.
        end_date: 结束日期, 默认今天.
        fields: 字段列表, 默认全部.
        count: 数量, **与 start_date 二选一**. 返回 end_date 前 count 个交易日.

    Returns:
        DataFrame(13列): ``date`` / ``sec_code`` / ``change_pct``(涨跌幅%) /
        ``net_amount_main`` / ``net_pct_main``(主力) /
        ``net_amount_xl`` / ``net_pct_xl``(超大单) /
        ``net_amount_l`` / ``net_pct_l``(大单) /
        ``net_amount_m`` / ``net_pct_m``(中单) /
        ``net_amount_s`` / ``net_pct_s``(小单).
        净额单位: 万元, 净占比单位: %.
    """


def get_mtss(  # noqa: D401
    security_list: str | list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    fields: list[str] | None = None,
    count: int | None = None,
) -> pd.DataFrame:
    """获取融资融券资金流信息.

    Args:
        security_list: 股票代码或列表.
        start_date: 开始日期. **与 count 二选一**.
        end_date: 结束日期, 默认今天.
        fields: 字段列表, 默认全部.
        count: 数量, **与 start_date 二选一**. 返回 end_date 前 count 个交易日.

    Returns:
        DataFrame(9列): ``date`` / ``sec_code`` /
        ``fin_value``(融资余额) / ``fin_buy_value``(融资买入额) /
        ``fin_refund_value``(融资偿还额) / ``sec_value``(融券余额) /
        ``sec_sell_value``(融券卖出额) / ``sec_refund_value``(融券偿还额) /
        ``fin_sec_value``(融资融券余额).

    Note:
        没有数据的标的不会返回.
    """


def get_call_auction(  # noqa: D401
    security: str | list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    fields: list[str] | None = None,
) -> pd.DataFrame:
    """获取集合竞价时的 tick 数据.

    Args:
        security: 标的代码或列表.
        start_date: 开始日期, 如 ``'2019-01-01'``.
        end_date: 结束日期, 如 ``'2019-02-01'``.
        fields: 行情字段(类似 tick). 可用字段::

            time           时间          datetime
            current        当前价        float
            volume         累计成交量(股) float
            money          累计成交额    float
            b1_v~b5_v     五档买量      float
            b1_p~b5_p     五档买价      float
            a1_v~a5_v     五档卖量      float
            a1_p~a5_p     五档卖价      float

    Returns:
        DataFrame, 集合竞价 tick 数据.
    """


# ---------------------------------------------------------------------------
# 因子
# ---------------------------------------------------------------------------


def get_factor_values(  # noqa: D401
    securities: str | list[str],
    factors: str | list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    count: int | None = None,
) -> dict[str, pd.DataFrame]:
    """获取因子数据 (``jqfactor.get_factor_values``).

    需在远程 kernel 中 ``import jqfactor``(非 jqdata).

    Args:
        securities: 股票代码或列表.
        factors: 因子名称或列表. 不传返回所有因子. 先用
            ``get_all_factors()`` 查 ``factor`` 列再按需传.
        start_date: 开始日期.
        end_date: 结束日期.
        count: 截止 end_date 前的交易日数(含当日).

    Returns:
        ``{因子名: DataFrame}``, DataFrame index=日期, columns=股票代码.
    """


def get_all_factors() -> pd.DataFrame:
    """获取因子库中全部因子名称 (``jqfactor.get_all_factors``).

    需在远程 kernel 中 ``import jqfactor``.

    Returns:
        DataFrame(260 个因子), columns: ``factor``(因子名) /
        ``factor_intro``(说明) / ``category``(分类) / ``category_intro``.
    """


# ---------------------------------------------------------------------------
# 指数成分股 / 工具
# ---------------------------------------------------------------------------


def get_index_stocks(  # noqa: D401
    index_symbol: str,
    date: str | None = None,
) -> list[str]:
    """获取指数成分股列表 (*动态代理*).

    Args:
        index_symbol: 指数代码. 常见: ``000300.XSHG``(沪深300) /
            ``000905.XSHG``(中证500) / ``000852.XSHG``(中证1000) /
            ``399006.XSHE``(创业板指).
        date: 查询日期, 省略取最新成分.

    Returns:
        list[code], 成分股代码列表.
    """


def normalize_code(code: str) -> str:
    """将简写代码归一为聚宽标准格式 (*动态代理*).

    Args:
        code: 简写代码, 如 ``'600519'`` / ``'000001'``.

    Returns:
        带后缀的标准代码, 如 ``'600519.XSHG'`` / ``'000001.XSHE'``.

    Note:
        聚宽无 ``normalize_codes`` 复数形式, 批量用列表推导:
        ``[normalize_code(c) for c in codes]``.
    """


def get_extras(  # noqa: D401
    info: str,
    security_list: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    df: bool = True,
    count: int | None = None,
) -> pd.DataFrame:
    """获取额外信息 (*动态代理*).

    Args:
        info: 信息类型. ``is_st``(是否ST) / ``acc_net_value``(基金累计净值) /
            ``unit_net_value``(基金单位净值) / ``adj_net_value``(基金复权净值) /
            ``futures_sett_price``(期货结算价) /
            ``futures_positions``(期货持仓).
        security_list: 证券代码列表.
        start_date: 开始日期.
        end_date: 结束日期.
        df: 是否返回 DataFrame.
        count: 截止 end_date 前的交易日数.

    Returns:
        DataFrame(index=日期, columns=code). ``is_st`` 时值为 True/False.

    Note:
        **不是除权除息/复权因子**. 复权直接用 ``get_price(fq='pre')``.
    """


# ---------------------------------------------------------------------------
# 存根防护
# ---------------------------------------------------------------------------
#
# 上面的函数只有 docstring 而无实现(有意为之 —— 本模块是类型存根).
# 但裸存根被误调用时会**静默返回 None**, 很难排查;
# 故统一包装为"调用即报错"的存根: functools.wraps 保留了
# __name__ / __doc__ / __wrapped__, 因此 help() 与 inspect.signature()
# 仍能拿到完整的原签名与文档.


def _make_stub(fn):
    """把无实现的参考函数包装成调用即报错的存根."""

    @functools.wraps(fn)
    def _stub(*args: object, **kwargs: object) -> None:
        raise NotImplementedError(
            f"jq.api.{fn.__name__} 是 API 参考存根, 不可调用。\n"
            f"  本地取数(带缓存): from jq.cache import DataCache; "
            f"DataCache().{fn.__name__}(...)\n"
            f"  远程执行任意代码: jqdata run --file <脚本>"
        )

    return _stub


for _name in __all__:
    _fn = globals().get(_name)
    if callable(_fn):
        globals()[_name] = _make_stub(_fn)
del _name, _fn
