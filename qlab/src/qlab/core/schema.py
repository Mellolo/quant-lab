"""Schema definitions and validation.

Schema 即契约（设计原则 P4）。每个数据格式有明确的列名、类型、不变量。
不符的数据在加载时立即抛 SchemaViolationError。
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from qlab.core.exceptions import SchemaViolationError


@dataclass(frozen=True)
class ColumnSpec:
    """一列的规格."""

    name: str
    dtype: str                      # numpy dtype 字符串，如 'float64', 'int64', 'bool', 'str'
    nullable: bool = True
    description: str = ""


@dataclass(frozen=True)
class Schema:
    """一个 DataFrame 的 schema."""

    name: str
    index_names: list[str]          # MultiIndex 各层的名字
    columns: list[ColumnSpec]
    invariants: list[str] = field(default_factory=list)  # 不变量描述，供文档使用

    @property
    def required_columns(self) -> list[str]:
        """非空列名."""
        return [c.name for c in self.columns if not c.nullable]

    @property
    def all_columns(self) -> list[str]:
        return [c.name for c in self.columns]


# ============================================================================
# §3.1 DailyBar
# ============================================================================

SCHEMA_DAILY_BAR = Schema(
    name="DailyBar",
    index_names=["date", "symbol"],
    columns=[
        # 后复权价格
        ColumnSpec("open", "float64", description="后复权开盘价（元）"),
        ColumnSpec("high", "float64", description="后复权最高价（元）"),
        ColumnSpec("low", "float64", description="后复权最低价（元）"),
        ColumnSpec("close", "float64", description="后复权收盘价（元）"),
        ColumnSpec("vwap", "float64", description="后复权 VWAP（元）"),
        # 不复权价格
        ColumnSpec("open_raw", "float64", description="不复权开盘价（元）"),
        ColumnSpec("high_raw", "float64", description="不复权最高价（元）"),
        ColumnSpec("low_raw", "float64", description="不复权最低价（元）"),
        ColumnSpec("close_raw", "float64", description="不复权收盘价（元）"),
        # 成交
        ColumnSpec("volume", "int64", nullable=False, description="成交股数（复权后）"),
        ColumnSpec("amount", "float64", nullable=False, description="成交额（元，与复权无关）"),
        # 复权因子
        ColumnSpec("adj_factor", "float64", nullable=False, description="后复权累积因子"),
        # 涨跌停参考
        ColumnSpec("limit_up_price", "float64", description="涨停参考价（不复权）"),
        ColumnSpec("limit_down_price", "float64", description="跌停参考价（不复权）"),
        # 状态
        ColumnSpec("is_suspended", "bool", nullable=False, description="当日是否停牌"),
        ColumnSpec("is_limit_up", "bool", nullable=False, description="收盘是否涨停"),
        ColumnSpec("is_limit_down", "bool", nullable=False, description="收盘是否跌停"),
        ColumnSpec("is_st", "bool", nullable=False, description="是否 ST/*ST"),
        ColumnSpec("days_since_listing", "int32", nullable=False, description="距上市天数"),
        # 股本
        ColumnSpec("total_shares", "int64", description="总股本（股）"),
        ColumnSpec("float_shares", "int64", description="流通股本（股）"),
        ColumnSpec("free_float_shares", "int64", description="自由流通股本（股）"),
    ],
    invariants=[
        "close ≈ close_raw * adj_factor (tol=1e-4)",
        "low ≤ open ≤ high, low ≤ close ≤ high (同理 _raw)",
        "is_limit_up + is_limit_down ≤ 1",
        "adj_factor > 0",
        "days_since_listing ≥ 0",
        "is_suspended → 价格列为 NaN, volume=0, amount=0",
        "volume > 0 ⇔ amount > 0",
        "free_float_shares ≤ float_shares ≤ total_shares",
    ],
)

# ============================================================================
# §3.2 IntradayBar
# ============================================================================

SCHEMA_INTRADAY_BAR = Schema(
    name="IntradayBar",
    index_names=["timestamp", "symbol"],
    columns=[
        ColumnSpec("open", "float64"),
        ColumnSpec("high", "float64"),
        ColumnSpec("low", "float64"),
        ColumnSpec("close", "float64"),
        ColumnSpec("vwap", "float64"),
        ColumnSpec("open_raw", "float64"),
        ColumnSpec("high_raw", "float64"),
        ColumnSpec("low_raw", "float64"),
        ColumnSpec("close_raw", "float64"),
        ColumnSpec("volume", "int64", nullable=False),
        ColumnSpec("amount", "float64", nullable=False),
        ColumnSpec("adj_factor", "float64", nullable=False),
        ColumnSpec("session", "str", nullable=False, description="open_auction/morning/afternoon/close_auction"),
    ],
    invariants=[
        "同一 (date, symbol) 所有 bar 共享 adj_factor",
        "session ∈ {open_auction, morning, afternoon, close_auction}",
    ],
)

# ============================================================================
# §3.3 Universe
# ============================================================================

SCHEMA_UNIVERSE = Schema(
    name="Universe",
    index_names=["date", "symbol"],
    columns=[
        ColumnSpec("in_universe", "bool", nullable=False),
        ColumnSpec("weight", "float64", description="指数权重，自定义 universe 可 NaN"),
    ],
    invariants=[
        "同一 (date, symbol) 唯一",
        "指数 universe：sum(weight) ≈ 1.0",
    ],
)

# ============================================================================
# §3.5 CorporateAction
# ============================================================================

SCHEMA_CORPORATE_ACTION = Schema(
    name="CorporateAction",
    index_names=[],
    columns=[
        ColumnSpec("symbol", "str", nullable=False),
        ColumnSpec("action_type", "str", nullable=False),
        ColumnSpec("announce_date", "datetime64[ns]", nullable=False),
        ColumnSpec("ex_date", "datetime64[ns]", nullable=False),
        ColumnSpec("record_date", "datetime64[ns]"),
        ColumnSpec("pay_date", "datetime64[ns]"),
        ColumnSpec("cash_per_share", "float64"),
        ColumnSpec("bonus_share_ratio", "float64"),
        ColumnSpec("transfer_share_ratio", "float64"),
        ColumnSpec("rights_share_ratio", "float64"),
        ColumnSpec("rights_price", "float64"),
    ],
    invariants=[
        "ex_date ≥ announce_date",
        "对应字段非负",
    ],
)

# ============================================================================
# §3.14 Fundamental
# ============================================================================

SCHEMA_FUNDAMENTAL = Schema(
    name="Fundamental",
    index_names=["announce_date", "symbol", "report_type"],
    columns=[
        ColumnSpec("report_period", "datetime64[ns]", nullable=False),
        ColumnSpec("report_type", "str", nullable=False),
        ColumnSpec("announce_date", "datetime64[ns]", nullable=False),
        ColumnSpec("available_at", "datetime64[ns]", nullable=False),
        ColumnSpec("fiscal_year", "int16", nullable=False),
        ColumnSpec("fiscal_quarter", "int8", nullable=False),
        ColumnSpec("source", "str"),
        # 业务字段（非穷尽，按需扩展）
        ColumnSpec("total_assets", "float64"),
        ColumnSpec("total_liabilities", "float64"),
        ColumnSpec("total_equity", "float64"),
        ColumnSpec("equity_to_shareholders", "float64"),
        ColumnSpec("cash_and_equivalents", "float64"),
        ColumnSpec("current_assets", "float64"),
        ColumnSpec("current_liabilities", "float64"),
        ColumnSpec("total_revenue", "float64"),
        ColumnSpec("operating_revenue", "float64"),
        ColumnSpec("operating_cost", "float64"),
        ColumnSpec("operating_profit", "float64"),
        ColumnSpec("net_profit", "float64"),
        ColumnSpec("net_profit_to_shareholders", "float64"),
        ColumnSpec("net_profit_excl_nonrecurring", "float64"),
        ColumnSpec("eps_basic", "float64"),
        ColumnSpec("eps_diluted", "float64"),
        ColumnSpec("operating_cash_flow", "float64"),
        ColumnSpec("investing_cash_flow", "float64"),
        ColumnSpec("financing_cash_flow", "float64"),
        # 业绩预告专属
        ColumnSpec("forecast_net_profit_min", "float64"),
        ColumnSpec("forecast_net_profit_max", "float64"),
        ColumnSpec("forecast_type", "str"),
        ColumnSpec("forecast_change_pct_min", "float64"),
        ColumnSpec("forecast_change_pct_max", "float64"),
    ],
    invariants=[
        "announce_date ≥ report_period(仅正式/快报; 预告常在报告期前发布)",
        "available_at ≥ announce_date 09:30",
        "同一 (symbol, report_period) 的 report_type 序列时间单调：forecast ≤ flash ≤ official",
        "金额单位统一为元",
    ],
)

# ============================================================================
# §3.10 Event — Triple-Barrier 输入
# ============================================================================

SCHEMA_EVENT = Schema(
    name="Event",
    index_names=["event_start"],
    columns=[
        ColumnSpec("symbol", "str", nullable=False, description="对应股票代码"),
        ColumnSpec("t1", "datetime64[ns]", description="垂直屏障时刻；NaT 表示无垂直屏障"),
        ColumnSpec("target", "float64", nullable=False, description="屏障宽度基准（如估计日波动率）"),
        ColumnSpec("side", "float64", description="主模型给的方向 +1/-1；None 表示让模型学方向"),
        ColumnSpec(
            "entry_timing",
            "str",
            description="样本起点/入场时点: 'open'(to_event_dataframe 默认) 或 'close'。"
                        "列缺失时 label_events 视为 close，以兼容手写旧事件表",
        ),
        ColumnSpec(
            "event_id",
            "str",
            description="稳定唯一事件键（多标的下 event_start 会重复；join 请用 event_id 或 "
                        "(event_start, symbol)，勿只按 event_start）。可选列，手写旧表可缺。",
        ),
    ],
    invariants=[
        "target > 0",
        "t1 > event_start（若 t1 非 NaT）",
        "event_start 必须是交易时刻",
        "entry_timing ∈ {open, close}（若列存在）",
        "event_id 若存在则应唯一",
    ],
)

# ============================================================================
# §3.11 Label — Triple-Barrier 输出
# ============================================================================

SCHEMA_LABEL = Schema(
    name="Label",
    index_names=["event_start"],
    columns=[
        ColumnSpec("symbol", "str", nullable=False),
        ColumnSpec("t1", "datetime64[ns]"),
        ColumnSpec("target", "float64"),
        ColumnSpec("side", "float64"),
        ColumnSpec("bin", "int8", nullable=False, description="标签：{-1,0,1} 或 {0,1}（meta-labeling）"),
        ColumnSpec("ret", "float64", description="实际实现的收益率"),
        ColumnSpec("touch_time", "datetime64[ns]", description="首次触屏障的时刻"),
        ColumnSpec("touch_type", "str", description="upper/lower/vertical/invalid/no_data/suspended"),
        ColumnSpec(
            "event_id",
            "str",
            description="透传自 Event；可选。join 优先用此列。",
        ),
    ],
    invariants=[
        "touch_time >= event_start 且 touch_time <= t1（若 t1 非 NaT）",
        "ret 的符号与 bin × side（meta-labeling）或 bin（普通模式）一致",
    ],
)

# ============================================================================
# §3.12 SampleWeight
# ============================================================================

SCHEMA_SAMPLE_WEIGHT = Schema(
    name="SampleWeight",
    index_names=["event_start"],
    columns=[
        ColumnSpec("uniqueness", "float64", description="平均唯一性 ū_i ∈ [0,1]"),
        ColumnSpec("return_attr", "float64", description="按收益归因的权重"),
        ColumnSpec("time_decay", "float64", description="时间衰减因子 ∈ [0,1]"),
        ColumnSpec("final_weight", "float64", nullable=False, description="最终权重 (归一到 sum=N)"),
    ],
    invariants=[
        "所有权重列非负",
        "uniqueness, time_decay ∈ [0, 1]",
        "sum(final_weight) ≈ N",
    ],
)

# ============================================================================
# §3.15 IndustryClassification
# ============================================================================

SCHEMA_INDUSTRY = Schema(
    name="IndustryClassification",
    index_names=["date", "symbol", "system"],
    columns=[
        ColumnSpec("system", "str", nullable=False, description="sw / citic / csrc / gics"),
        ColumnSpec("level", "int8", nullable=False),
        ColumnSpec("industry_code", "str", nullable=False),
        ColumnSpec("industry_name", "str", nullable=False),
        ColumnSpec("parent_code", "str"),
    ],
    invariants=[
        "(date, symbol, system, level) 唯一",
        "level > 1 时 parent_code 必须存在",
    ],
)

# ============================================================================
# §3.16 ConceptClassification
# ============================================================================

SCHEMA_CONCEPT = Schema(
    name="ConceptClassification",
    index_names=["effective_date", "symbol", "source"],
    columns=[
        ColumnSpec("concept_code", "str", nullable=False),
        ColumnSpec("concept_name", "str", nullable=False),
        ColumnSpec("expired_date", "datetime64[ns]", description="被移出该概念的日期；NaT 表示仍有效"),
    ],
    invariants=[
        "(effective_date, symbol, source, concept_code) 唯一",
        "expired_date >= effective_date（若非 NaT）",
    ],
)


# ============================================================================
# §3.17 MarginTrading — 两融(融资融券)
# ============================================================================

SCHEMA_MARGIN_TRADING = Schema(
    name="MarginTrading",
    index_names=["date", "symbol"],
    columns=[
        ColumnSpec("available_at", "datetime64[ns]", nullable=False,
                   description="数据实际可用时点(PIT 过滤用, 下一交易日 09:30)"),
        ColumnSpec("fin_balance", "float64", nullable=False, description="融资余额（元）"),
        ColumnSpec("fin_buy", "float64", description="融资买入额（元）"),
        ColumnSpec("fin_repay", "float64", description="融资偿还额（元）"),
        ColumnSpec("sec_balance", "float64", description="融券余量（股）"),
        ColumnSpec("sec_sell", "float64", description="融券卖出量（股）"),
        ColumnSpec("sec_repay", "float64", description="融券偿还量（股）"),
        ColumnSpec("total_balance", "float64", description="融资融券余额（元）"),
    ],
    invariants=[
        "余额(fin_balance/sec_balance/total_balance)非负; 发生额允许负值(交易所冲销修正, 实测存在)",
        "金额单位统一为元（融券余量为股数）",
        "available_at > date —— 交易所 T 日收盘后发布当日数据, 故 T 日**盘中不可用**;"
        "取下一交易日 09:30 作为安全上界(当晚可能已可得, 但各数据商入库时刻不一)",
    ],
)

# ============================================================================
# §3.18 MoneyFlow — 资金流向
# ============================================================================

SCHEMA_MONEY_FLOW = Schema(
    name="MoneyFlow",
    index_names=["date", "symbol"],
    columns=[
        ColumnSpec("available_at", "datetime64[ns]", nullable=False,
                   description="数据实际可用时点(PIT 过滤用, 下一交易日 09:30)"),
        ColumnSpec("change_pct", "float64", description="当日涨跌幅（%）"),
        ColumnSpec("net_amount_main", "float64", description="主力净额（元）"),
        ColumnSpec("net_pct_main", "float64", description="主力净占比（%）"),
        ColumnSpec("net_amount_xl", "float64", description="超大单净额（元）"),
        ColumnSpec("net_pct_xl", "float64", description="超大单净占比（%）"),
        ColumnSpec("net_amount_l", "float64", description="大单净额（元）"),
        ColumnSpec("net_pct_l", "float64", description="大单净占比（%）"),
        ColumnSpec("net_amount_m", "float64", description="中单净额（元）"),
        ColumnSpec("net_pct_m", "float64", description="中单净占比（%）"),
        ColumnSpec("net_amount_s", "float64", description="小单净额（元）"),
        ColumnSpec("net_pct_s", "float64", description="小单净占比（%）"),
    ],
    invariants=[
        "net_amount_main ≈ net_amount_xl + net_amount_l（主力 = 超大单 + 大单）",
        "金额单位统一为元（聚宽原始为万元，接入时需×1e4）",
        "available_at > date —— 当日全天汇总数据, 收盘后才产生, T 日盘中不可用",
    ],
)

# ============================================================================
# §3.19 CallAuction — 集合竞价
# ============================================================================

_AUCTION_BOOK_COLS = [
    spec
    for i in range(1, 6)
    for spec in (
        ColumnSpec(f"ask{i}_price", "float64", description=f"卖{i}价（不复权）"),
        ColumnSpec(f"ask{i}_volume", "float64", description=f"卖{i}量（股）"),
        ColumnSpec(f"bid{i}_price", "float64", description=f"买{i}价（不复权）"),
        ColumnSpec(f"bid{i}_volume", "float64", description=f"买{i}量（股）"),
    )
]

SCHEMA_CALL_AUCTION = Schema(
    name="CallAuction",
    index_names=["date", "symbol"],
    columns=[
        ColumnSpec("available_at", "datetime64[ns]", nullable=False,
                   description="数据可用时点 —— 竞价 09:25 结束即产生, 故为**当日** 09:30"),
        ColumnSpec("auction_price", "float64", description="竞价成交价（不复权）"),
        ColumnSpec("auction_volume", "float64", nullable=False, description="竞价成交量（股）"),
        ColumnSpec("auction_amount", "float64", nullable=False, description="竞价成交额（元）"),
        *_AUCTION_BOOK_COLS,
    ],
    invariants=[
        "价格均为**不复权**口径（竞价时点的真实报价）",
        "bid1_price ≤ ask1_price（如两者均非 NaN）",
        "成交量/额非负",
        "available_at = 当日 09:30 —— 与两融/资金流不同, 竞价数据**当日盘中即可用**"
        "(09:25 集合竞价结束即确定), 这是盘中策略能用它的前提",
    ],
)

# ============================================================================
# §3.20 Billboard — 龙虎榜
# ============================================================================

SCHEMA_BILLBOARD = Schema(
    name="Billboard",
    index_names=[],  # 一个 (date, symbol) 对应多行（每营业部一行），故用 RangeIndex
    columns=[
        ColumnSpec("date", "datetime64[ns]", nullable=False, description="上榜日"),
        ColumnSpec("symbol", "str", nullable=False),
        ColumnSpec("direction", "str", nullable=False, description="BUY / SELL / ALL"),
        ColumnSpec("rank", "int64", description="名次"),
        ColumnSpec("abnormal_code", "str", description="异动类型代码"),
        ColumnSpec("abnormal_name", "str", description="异动类型名称"),
        ColumnSpec("sales_depart", "str", description="营业部名称"),
        ColumnSpec("buy_value", "float64", description="买入额（元）"),
        ColumnSpec("buy_rate", "float64", description="买入占比（%）"),
        ColumnSpec("sell_value", "float64", description="卖出额（元）"),
        ColumnSpec("sell_rate", "float64", description="卖出占比（%）"),
        ColumnSpec("total_value", "float64", description="买卖总额（元）"),
        ColumnSpec("net_value", "float64", description="净买入额（元）"),
        ColumnSpec("amount", "float64", description="当日成交额（元）"),
    ],
    invariants=[
        "direction ∈ {BUY, SELL, ALL}",
        "net_value ≈ buy_value - sell_value",
        "同一 (date, symbol) 可多行（每营业部一行），故 index 不唯一",
    ],
)

# ============================================================================
# §3.21 FactorExposure — 因子暴露
# ============================================================================

SCHEMA_FACTOR_EXPOSURE = Schema(
    name="FactorExposure",
    index_names=["date", "symbol"],
    columns=[],  # 列名即因子名，由调用方指定，均为 float64
    invariants=[
        "所有列为 float64（列名 = 因子名，动态）",
        "同一 (date, symbol) 唯一",
    ],
)


# ============================================================================
# ReviewMarket / ReviewStyle — 日频复盘 ①市场 + ②风格
# ============================================================================

VOLUME_BINS = ("地量", "正常", "放量", "天量", "天量后缩")
EMOTION_BINS = ("冰点/恐慌", "修复", "认知", "乐观", "狂热", "分配", "中性/撕裂")
CLOCK_BINS = ("短博弈", "中线", "混合")
HABITAT_BINS = ("权重", "弹性", "混合")
CROWDING_BINS = ("分散", "正常", "聚集", "拥挤待切")
GROWTH_SOURCES = ("pe", "industry", "")
STYLE_SCORES = frozenset({-2, -1, 0, 1, 2})

SCHEMA_REVIEW_MARKET = Schema(
    name="ReviewMarket",
    index_names=["date"],
    columns=[
        ColumnSpec("amount", "float64", nullable=False, description="活跃子集成交额合计（元）"),
        ColumnSpec("amount_ma20", "float64", description="前 20 日成交额均值（不含 T）"),
        ColumnSpec("amount_ratio", "float64", description="amount / amount_ma20"),
        ColumnSpec("volume_bin", "str", nullable=False, description="地量/正常/放量/天量/天量后缩"),
        ColumnSpec("market_ret", "float64", description="T-1 流通市值加权收益"),
        ColumnSpec("flow", "float64", description="盘面流向 F = amount × sign(R)"),
        ColumnSpec("flow_weighted", "float64", description="F^w = amount × R"),
        ColumnSpec("impact", "float64", description="冲击 I = R / amount_ratio"),
        ColumnSpec("n_up", "int64", nullable=False, description="上涨家数"),
        ColumnSpec("n_down", "int64", nullable=False, description="下跌家数"),
        ColumnSpec("n_tradable", "int64", nullable=False, description="可交易家数（未停牌非 ST、有收盘）"),
        ColumnSpec("advance_ratio", "float64", description="上涨家数 / 可交易家数"),
        ColumnSpec("n_limit_up", "int64", nullable=False, description="收盘涨停家数"),
        ColumnSpec("n_limit_down", "int64", nullable=False, description="收盘跌停家数"),
        ColumnSpec("n_active", "int64", nullable=False, description="活筹比例过半的家数"),
        ColumnSpec("live_ret", "float64", description="活跃市值涨跌：0AMV_t / 0AMV_{t-1} − 1"),
        ColumnSpec("live_gap", "float64", description="活跃市值涨跌 − 流通总市值涨跌（0AMV 相对 0号）"),
        ColumnSpec("attack_defense", "float64", description="高波组 − 低波组当日收益"),
        ColumnSpec("emotion_bin", "str", nullable=False, description="情绪六档或中性/撕裂"),
        ColumnSpec("turnover", "float64", description="成交额 / 可交易流通市值"),
        ColumnSpec("turnover_ratio", "float64", description="换手 / 前 20 日换手均"),
        ColumnSpec("active_mcap", "float64", description="活筹市值 Σ 流通市值×(1−e^{−换手/τ})，0AMV 代理"),
        ColumnSpec("listed_mcap", "float64", description="流通总市值（0号代理）"),
        ColumnSpec("active_share", "float64", description="活筹 / 流通总市值（0AMV / 0号）"),
        ColumnSpec("median_ret", "float64", description="活跃子集收益中位数"),
        ColumnSpec("breadth_gap", "float64", description="市值加权收益 − 中位数（虚涨）"),
        ColumnSpec("ret_dispersion", "float64", description="个股收益截面标准差"),
        ColumnSpec("crowding_top1", "float64", description="成交额 Top1% / 全市场"),
        ColumnSpec("crowding_top10", "float64", description="成交额 Top10% / 全市场"),
        ColumnSpec("crowding_top10_ma20", "float64", description="Top10 集中度前 20 日均（不含 T）"),
        ColumnSpec("crowding_delta", "float64", description="Top10 − 自身 20 日均"),
        ColumnSpec("crowding_bin", "str", nullable=False, description="分散/正常/聚集/拥挤待切"),
        ColumnSpec("fin_balance", "float64", description="已发布融资余额合计（元，PIT）"),
        ColumnSpec("fin_delta", "float64", description="融资余额较上一日变化"),
        ColumnSpec("fin_amount_share", "float64", description="融资买入额 / 市场成交额"),
    ],
    invariants=[
        "volume_bin ∈ {地量, 正常, 放量, 天量, 天量后缩}",
        "emotion_bin ∈ {冰点/恐慌, 修复, 认知, 乐观, 狂热, 分配, 中性/撕裂}",
        "crowding_bin ∈ {分散, 正常, 聚集, 拥挤待切}",
        "n_* ≥ 0；同一 date 唯一",
    ],
)

SCHEMA_REVIEW_STYLE = Schema(
    name="ReviewStyle",
    index_names=["date"],
    columns=[
        ColumnSpec("size_s", "float64", description="小盘 − 大盘"),
        ColumnSpec("size_m5", "float64", description="规模轴近 5 日滚动强度"),
        ColumnSpec("size_score", "int64", nullable=False, description="−2～+2，正=要小盘"),
        ColumnSpec("risk_s", "float64", description="高β − 低β"),
        ColumnSpec("risk_m5", "float64", description="风险轴近 5 日滚动强度"),
        ColumnSpec("risk_score", "int64", nullable=False, description="−2～+2，正=要弹性"),
        ColumnSpec("growth_s", "float64", description="高 PE/成长行业 − 低 PE/价值行业"),
        ColumnSpec("growth_m5", "float64", description="成长轴近 5 日滚动强度"),
        ColumnSpec("growth_score", "int64", nullable=False, description="−2～+2，正=要贵的成长"),
        ColumnSpec("growth_source", "str", nullable=False, description="pe / industry / 空"),
        ColumnSpec("trend_s", "float64", description="昨强组 − 昨弱组的今日收益"),
        ColumnSpec("trend_m5", "float64", description="趋势轴近 5 日滚动强度"),
        ColumnSpec("trend_score", "int64", nullable=False, description="−2～+2，正=强者续强"),
        ColumnSpec("clock_bin", "str", nullable=False, description="短博弈/中线/混合"),
        ColumnSpec("habitat", "str", nullable=False, description="权重/弹性/混合（看五日，不是单日）"),
        ColumnSpec("liq_s", "float64", description="低换手组 − 高换手组"),
        ColumnSpec("liq_m5", "float64", description="流动性轴近 5 日滚动强度"),
        ColumnSpec("liq_score", "int64", nullable=False, description="−2～+2，正=要低换手/配置"),
        ColumnSpec("crowding", "float64", description="成交额 Top10% / 全市场"),
        ColumnSpec("crowding_delta", "float64", description="Top10 − 自身 20 日均"),
        ColumnSpec("crowding_bin", "str", nullable=False, description="分散/正常/聚集/拥挤待切"),
        ColumnSpec("lu_yest_ret", "float64", description="昨涨停今日等权收益"),
        ColumnSpec("lu_yest_s", "float64", description="昨涨停今日 − 等权市场"),
        ColumnSpec("lu_yest_m5", "float64", description="昨涨停相对等权近 5 日强度"),
        ColumnSpec("lu_yest_score", "int64", nullable=False, description="−2～+2，正=昨涨停相对市场在赚"),
        ColumnSpec("n_lu_yest", "int64", nullable=False, description="昨日涨停家数"),
        ColumnSpec("lu_promote", "float64", description="昨涨停今日继续涨停的比例"),
        ColumnSpec("lu_promote_excess", "float64", description="晋级率 − 当日涨停率"),
        ColumnSpec("lu_promote_z", "float64", description="晋级超额相对自身近窗的 z"),
        ColumnSpec("lu_promote_score", "int64", nullable=False, description="−2～+2，晋级相对自身近窗"),
        ColumnSpec("n_board2", "int64", nullable=False, description="连板达到 board_min 的家数"),
        ColumnSpec("board2_share", "float64", description="二板家数 / 今日涨停"),
        ColumnSpec("board2_z", "float64", description="二板占比相对自身近窗的 z"),
        ColumnSpec("board2_score", "int64", nullable=False, description="−2～+2，二板占比相对自身近窗"),
        ColumnSpec("max_board", "int64", nullable=False, description="今日最高连板（旁注，不打分）"),
        ColumnSpec("narrative", "str", nullable=False, description="主洋流一句话"),
    ],
    invariants=[
        "*_score ∈ {-2,-1,0,1,2}",
        "clock_bin ∈ {短博弈, 中线, 混合}",
        "habitat ∈ {权重, 弹性, 混合}",
        "crowding_bin ∈ {分散, 正常, 聚集, 拥挤待切}",
        "growth_source ∈ {pe, industry, ''}",
        "同一 date 唯一",
    ],
)


# ============================================================================
# 校验工具
# ============================================================================


def validate_schema(df: pd.DataFrame, schema: Schema, *,
                    strict_index: bool = True,
                    check_invariants: bool = True) -> None:
    """校验 DataFrame 是否符合 schema.

    参数
    ----
    df : 待校验 DataFrame
    schema : 目标 schema
    strict_index : 是否严格校验索引名（False 时允许 RangeIndex）
    check_invariants : 是否运行运行时不变量检查. 默认开启 (设计原则 P4).
                       极少数性能敏感场景可显式关闭.

    抛出
    ----
    SchemaViolationError : 任一字段不符合规范
    """
    # 1) 索引名
    if strict_index and schema.index_names:
        actual = list(df.index.names) if isinstance(df.index, pd.MultiIndex) else [df.index.name]
        expected = schema.index_names
        if actual != expected:
            raise SchemaViolationError(
                f"[{schema.name}] index 名称不符. 期望 {expected}, 实际 {actual}",
            )

    # 2) 必备列
    missing = [c for c in schema.required_columns if c not in df.columns]
    if missing:
        raise SchemaViolationError(
            f"[{schema.name}] 缺少必备列: {missing}",
            missing_columns=missing,
        )

    # 3) 列 dtype（仅对存在的列检查，不强求出现所有可选列）
    type_errors = []
    for spec in schema.columns:
        if spec.name not in df.columns:
            continue
        actual_dtype = str(df[spec.name].dtype)
        if not _dtype_compatible(actual_dtype, spec.dtype):
            type_errors.append(f"  - {spec.name}: 期望 {spec.dtype}, 实际 {actual_dtype}")
    if type_errors:
        raise SchemaViolationError(
            f"[{schema.name}] dtype 不符:\n" + "\n".join(type_errors),
        )

    if check_invariants:
        _check_invariants(df, schema)


def _dtype_compatible(actual: str, expected: str) -> bool:
    """宽松的 dtype 兼容判断."""
    if actual == expected:
        return True
    # 允许 object 兼容 str
    if expected == "str" and actual in ("object", "string"):
        return True
    # 整数族兼容
    if expected.startswith("int") and actual.startswith("int"):
        return True
    # 浮点族兼容
    if expected.startswith("float") and actual.startswith("float"):
        return True
    # datetime
    return expected.startswith("datetime64") and actual.startswith("datetime64")


def _check_invariants(df: pd.DataFrame, schema: Schema) -> None:
    """运行时不变量检查（按 schema 分派）."""
    if df.empty:
        return
    if schema.name == "DailyBar":
        _check_daily_bar_invariants(df)
    elif schema.name == "IntradayBar":
        _check_intraday_bar_invariants(df)
    elif schema.name == "Universe":
        _check_universe_invariants(df)
    elif schema.name == "Fundamental":
        _check_fundamental_invariants(df)
    elif schema.name == "Event":
        _check_event_invariants(df)
    elif schema.name == "Label":
        _check_label_invariants(df)
    elif schema.name == "SampleWeight":
        _check_sample_weight_invariants(df)
    elif schema.name == "ConceptClassification":
        _check_concept_invariants(df)
    elif schema.name == "IndustryClassification":
        _check_industry_invariants(df)
    elif schema.name == "MarginTrading":
        _check_margin_trading_invariants(df)
    elif schema.name == "MoneyFlow":
        _check_money_flow_invariants(df)
    elif schema.name == "CallAuction":
        _check_call_auction_invariants(df)
    elif schema.name == "Billboard":
        _check_billboard_invariants(df)
    elif schema.name == "FactorExposure":
        _check_factor_exposure_invariants(df)
    elif schema.name == "ReviewMarket":
        _check_review_market_invariants(df)
    elif schema.name == "ReviewStyle":
        _check_review_style_invariants(df)


def _check_daily_bar_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []

    # 1) close ≈ close_raw × adj_factor
    if {"close", "close_raw", "adj_factor"}.issubset(df.columns):
        mask = df[["close", "close_raw", "adj_factor"]].notna().all(axis=1)
        diff = (df.loc[mask, "close"] - df.loc[mask, "close_raw"] * df.loc[mask, "adj_factor"]).abs()
        bad = diff > 1e-4
        if bad.any():
            errs.append(f"close ≠ close_raw × adj_factor: {int(bad.sum())} 行")

    # 2) OHLC 序关系（后复权与不复权各自校验）
    for prefix in ("", "_raw"):
        cols = {f"open{prefix}", f"high{prefix}", f"low{prefix}", f"close{prefix}"}
        if not cols.issubset(df.columns):
            continue
        op = df[f"open{prefix}"]
        hi = df[f"high{prefix}"]
        lo = df[f"low{prefix}"]
        cl = df[f"close{prefix}"]
        present = op.notna() & hi.notna() & lo.notna() & cl.notna()
        eps = 1e-6
        bad = present & (
            (lo > op + eps) | (op > hi + eps) | (lo > cl + eps) | (cl > hi + eps) | (lo > hi + eps)
        )
        if bad.any():
            errs.append(f"OHLC{prefix} 序关系破坏: {int(bad.sum())} 行")

    # 3) is_limit_up + is_limit_down ≤ 1
    if {"is_limit_up", "is_limit_down"}.issubset(df.columns):
        bad = (df["is_limit_up"].fillna(False) & df["is_limit_down"].fillna(False))
        if bad.any():
            errs.append(f"同时涨停+跌停: {int(bad.sum())} 行")

    # 4) adj_factor > 0
    if "adj_factor" in df.columns:
        bad = df["adj_factor"].notna() & (df["adj_factor"] <= 0)
        if bad.any():
            errs.append(f"adj_factor ≤ 0: {int(bad.sum())} 行")

    # 5) days_since_listing ≥ 0
    if "days_since_listing" in df.columns:
        bad = df["days_since_listing"].notna() & (df["days_since_listing"] < 0)
        if bad.any():
            errs.append(f"days_since_listing < 0: {int(bad.sum())} 行")

    # 6) 停牌 → price NaN, volume=0, amount=0
    if "is_suspended" in df.columns and {"volume", "amount"}.issubset(df.columns):
        sus = df[df["is_suspended"].fillna(False)]
        if (sus["volume"].fillna(0) != 0).any() or (sus["amount"].fillna(0) != 0).any():
            errs.append("停牌日 volume/amount 非 0")
        # price NaN
        price_cols_present = [c for c in ("open", "high", "low", "close") if c in sus.columns]
        if price_cols_present and sus[price_cols_present].notna().any().any():
            non_nan = int(sus[price_cols_present].notna().any(axis=1).sum())
            if non_nan:
                errs.append(f"停牌日价格列非 NaN: {non_nan} 行")

    # 7) is_limit_up=True → close_raw 接近 limit_up_price（同理 down）
    for flag, ref in [("is_limit_up", "limit_up_price"),
                      ("is_limit_down", "limit_down_price")]:
        if {flag, ref, "close_raw"}.issubset(df.columns):
            mask = df[flag].fillna(False) & df["close_raw"].notna() & df[ref].notna()
            if mask.any():
                diff = (df.loc[mask, "close_raw"] - df.loc[mask, ref]).abs()
                # A 股报价精度 0.01，容差略放宽
                bad = diff > 1e-2
                if bad.any():
                    errs.append(f"{flag}=True 但 close_raw ≠ {ref}: {int(bad.sum())} 行")

    # 8) volume > 0 ⇔ amount > 0
    if {"volume", "amount"}.issubset(df.columns):
        v = df["volume"].fillna(0)
        a = df["amount"].fillna(0)
        bad = ((v > 0) ^ (a > 0))
        if bad.any():
            errs.append(f"volume>0 与 amount>0 不一致: {int(bad.sum())} 行")

    # 9) free_float ≤ float ≤ total
    if {"free_float_shares", "float_shares", "total_shares"}.issubset(df.columns):
        sub = df[["free_float_shares", "float_shares", "total_shares"]].dropna()
        bad = (sub["free_float_shares"] > sub["float_shares"]) | (sub["float_shares"] > sub["total_shares"])
        if bad.any():
            errs.append(f"股本大小关系破坏: {int(bad.sum())} 行")

    if errs:
        raise SchemaViolationError(
            "[DailyBar] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


_VALID_SESSIONS = {"open_auction", "morning", "afternoon", "close_auction"}


def _check_intraday_bar_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []

    # session 取值合法
    if "session" in df.columns:
        bad_sess = ~df["session"].isin(_VALID_SESSIONS)
        if bad_sess.any():
            uniq = sorted(set(df.loc[bad_sess, "session"].dropna().astype(str)))
            errs.append(f"非法 session: {uniq[:5]}{'...' if len(uniq) > 5 else ''}")

    # 同一 (date, symbol) 共享 adj_factor
    if "adj_factor" in df.columns and isinstance(df.index, pd.MultiIndex) and \
       "timestamp" in df.index.names and "symbol" in df.index.names:
        ts = df.index.get_level_values("timestamp")
        date_lvl = pd.Series(pd.DatetimeIndex(ts).normalize(), index=df.index)
        grouped = df["adj_factor"].groupby([date_lvl, df.index.get_level_values("symbol")]).nunique()
        bad = grouped[grouped > 1]
        if not bad.empty:
            errs.append(f"同一 (date, symbol) adj_factor 不唯一: {len(bad)} 组")

    if errs:
        raise SchemaViolationError(
            "[IntradayBar] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


def _check_universe_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []

    # (date, symbol) 唯一
    if isinstance(df.index, pd.MultiIndex):
        dup = df.index.duplicated()
        if dup.any():
            errs.append(f"(date, symbol) 重复: {int(dup.sum())} 行")

    # 指数 universe：每个 date 上 sum(weight) ≈ 1.0
    if "weight" in df.columns and "in_universe" in df.columns:
        in_uni = df[df["in_universe"].fillna(False)]
        if not in_uni["weight"].dropna().empty and isinstance(in_uni.index, pd.MultiIndex) and "date" in in_uni.index.names:
            daily_sum = in_uni["weight"].dropna().groupby(level="date").sum()
            bad = (daily_sum - 1.0).abs() > 1e-3
            if bad.any():
                n_bad = int(bad.sum())
                sample = daily_sum[bad].head(3).to_dict()
                errs.append(f"指数权重日总和 ≠ 1.0: {n_bad} 日（示例 {sample}）")

    if errs:
        raise SchemaViolationError(
            "[Universe] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


def _check_fundamental_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []

    if {"announce_date", "report_period"}.issubset(df.columns):
        # announce_date ≥ report_period 只对正式/快报成立(先有报告期、后披露)。
        # 业绩预告恰恰相反: 常在报告期结束前就发布(如 7 月预告当年年报),
        # 故预告行不纳入此约束。
        chk = (
            df[df["report_type"] != "forecast"]
            if "report_type" in df.columns
            else df
        )
        bad = chk["announce_date"] < chk["report_period"]
        if bad.any():
            errs.append(f"announce_date < report_period(非预告): {int(bad.sum())} 行")

    if {"announce_date", "available_at"}.issubset(df.columns):
        bad = df["available_at"] < df["announce_date"]
        if bad.any():
            errs.append(f"available_at < announce_date: {int(bad.sum())} 行")

    # 业绩预告 min ≤ max
    if {"forecast_net_profit_min", "forecast_net_profit_max"}.issubset(df.columns):
        sub = df[["forecast_net_profit_min", "forecast_net_profit_max"]].dropna()
        bad = sub["forecast_net_profit_min"] > sub["forecast_net_profit_max"]
        if bad.any():
            errs.append(f"forecast min > max: {int(bad.sum())} 行")

    if errs:
        raise SchemaViolationError(
            "[Fundamental] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


def _check_event_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []

    if "target" in df.columns:
        bad = df["target"].notna() & (df["target"] <= 0)
        if bad.any():
            errs.append(f"target ≤ 0: {int(bad.sum())} 行")

    if "t1" in df.columns:
        # event_start 来自 index（或 index 第一层）
        if isinstance(df.index, pd.MultiIndex):
            event_start = df.index.get_level_values(0)
        else:
            event_start = df.index
        bad_mask = df["t1"].notna() & (df["t1"] <= pd.Series(event_start, index=df.index))
        if bad_mask.any():
            errs.append(f"t1 <= event_start: {int(bad_mask.sum())} 行")

    if "entry_timing" in df.columns:
        ok = {"open", "close"}
        vals = df["entry_timing"].dropna().astype(str)
        bad = ~vals.isin(ok)
        if bad.any():
            errs.append(
                f"entry_timing 非法(应 ∈ {{open, close}}): {int(bad.sum())} 行"
            )

    if "event_id" in df.columns:
        ids = df["event_id"].dropna()
        if ids.duplicated().any():
            errs.append(f"event_id 重复: {int(ids.duplicated().sum())} 行")

    if errs:
        raise SchemaViolationError(
            "[Event] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


def _check_label_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []

    if "bin" in df.columns:
        bad = df["bin"].dropna()
        out_of_range = ~bad.isin([-1, 0, 1])
        if out_of_range.any():
            errs.append(f"bin 越界（应 ∈ {{-1,0,1}}）: {int(out_of_range.sum())} 行")

    # touch_time 在 [event_start, t1] 内
    if {"touch_time"}.issubset(df.columns):
        if isinstance(df.index, pd.MultiIndex):
            event_start = pd.Series(df.index.get_level_values(0), index=df.index)
        else:
            event_start = pd.Series(df.index, index=df.index)
        m = df["touch_time"].notna()
        bad = m & (df["touch_time"] < event_start)
        if bad.any():
            errs.append(f"touch_time < event_start: {int(bad.sum())} 行")
        if "t1" in df.columns:
            m2 = m & df["t1"].notna()
            bad2 = m2 & (df["touch_time"] > df["t1"])
            if bad2.any():
                errs.append(f"touch_time > t1: {int(bad2.sum())} 行")

    if errs:
        raise SchemaViolationError(
            "[Label] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


def _check_sample_weight_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []

    for col in ("uniqueness", "return_attr", "time_decay", "final_weight"):
        if col in df.columns:
            bad = df[col].notna() & (df[col] < 0)
            if bad.any():
                errs.append(f"{col} 负值: {int(bad.sum())} 行")

    for col in ("uniqueness", "time_decay"):
        if col in df.columns:
            bad = df[col].notna() & (df[col] > 1 + 1e-6)
            if bad.any():
                errs.append(f"{col} > 1: {int(bad.sum())} 行")

    # sum(final_weight) ≈ N（允许 5% 偏差，因 NaN 可能让总和略偏）
    if "final_weight" in df.columns:
        s = df["final_weight"].dropna()
        if len(s) > 0:
            total = float(s.sum())
            n = len(df)
            if abs(total - n) > 0.05 * n + 1.0:
                errs.append(f"sum(final_weight)={total:.2f} ≠ N={n}")

    if errs:
        raise SchemaViolationError(
            "[SampleWeight] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


def _check_concept_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []

    if "expired_date" in df.columns:
        if isinstance(df.index, pd.MultiIndex) and "effective_date" in df.index.names:
            eff = pd.Series(df.index.get_level_values("effective_date"), index=df.index)
        else:
            eff = df.get("effective_date")
        if eff is not None:
            m = df["expired_date"].notna()
            bad = m & (df["expired_date"] < eff)
            if bad.any():
                errs.append(f"expired_date < effective_date: {int(bad.sum())} 行")

    if errs:
        raise SchemaViolationError(
            "[ConceptClassification] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


def _check_industry_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []
    flat = df.reset_index() if isinstance(df.index, pd.MultiIndex) else df

    # (date, symbol, system, level) 唯一
    keys = [c for c in ("date", "symbol", "system", "level") if c in flat.columns]
    if len(keys) == 4:
        dup = flat.duplicated(keys)
        if dup.any():
            errs.append(f"(date, symbol, system, level) 重复: {int(dup.sum())} 行")

    # level > 1 时 parent_code 必须存在
    if {"level", "parent_code"}.issubset(flat.columns):
        sub = flat[flat["level"] > 1]
        bad = sub["parent_code"].isna() | (sub["parent_code"].astype(str) == "")
        if bad.any():
            errs.append(f"level>1 但 parent_code 缺失: {int(bad.sum())} 行")

    if errs:
        raise SchemaViolationError(
            "[IndustryClassification] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


def _available_at_series(df: pd.DataFrame) -> tuple[pd.Series, pd.Series] | None:
    """取 (available_at, date) 对 —— date 可能在 index 也可能在列."""
    if "available_at" not in df.columns:
        return None
    if isinstance(df.index, pd.MultiIndex) and "date" in (df.index.names or []):
        d = pd.Series(df.index.get_level_values("date"), index=df.index)
    elif "date" in df.columns:
        d = df["date"]
    else:
        return None
    return df["available_at"], d


def _check_margin_trading_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []
    # 余额类(存量)必非负 —— 负余额无业务意义。
    # 发生额类(fin_buy/fin_repay/sec_sell/sec_repay)**不约束非负**:
    # 实测聚宽原始数据存在负发生额(如 000999 2024-06-14 sec_refund=-66462),
    # 是交易所对前日多计的冲销/红冲, 罕见但合法。
    for col in ("fin_balance", "sec_balance", "total_balance"):
        if col in df.columns:
            bad = df[col].notna() & (df[col] < 0)
            if bad.any():
                errs.append(f"{col} 负值: {int(bad.sum())} 行")
    # available_at 必严格晚于数据日(交易所收盘后发布)
    pair = _available_at_series(df)
    if pair is not None:
        av, d = pair
        bad = av.notna() & (av <= d)
        if bad.any():
            errs.append(
                f"available_at <= date: {int(bad.sum())} 行"
                "(两融 T 日收盘后才发布, 当日盘中不可用)"
            )
    if errs:
        raise SchemaViolationError(
            "[MarginTrading] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


def _check_money_flow_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []
    # 主力 ≈ 超大单 + 大单
    if {"net_amount_main", "net_amount_xl", "net_amount_l"}.issubset(df.columns):
        m = df[["net_amount_main", "net_amount_xl", "net_amount_l"]].notna().all(axis=1)
        diff = (
            df.loc[m, "net_amount_main"]
            - df.loc[m, "net_amount_xl"]
            - df.loc[m, "net_amount_l"]
        ).abs()
        # 容差按金额量级(元)放宽
        bad = diff > 1.0
        if bad.any():
            errs.append(f"net_amount_main ≠ xl + l: {int(bad.sum())} 行")
    # available_at 必严格晚于数据日(当日全天汇总, 收盘后产生)
    pair = _available_at_series(df)
    if pair is not None:
        av, d = pair
        bad = av.notna() & (av <= d)
        if bad.any():
            errs.append(
                f"available_at <= date: {int(bad.sum())} 行(当日汇总数据盘中不可用)"
            )
    if errs:
        raise SchemaViolationError(
            "[MoneyFlow] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


def _check_call_auction_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []
    for col in ("auction_volume", "auction_amount"):
        if col in df.columns:
            bad = df[col].notna() & (df[col] < 0)
            if bad.any():
                errs.append(f"{col} 负值: {int(bad.sum())} 行")
    # bid1 ≤ ask1
    if {"bid1_price", "ask1_price"}.issubset(df.columns):
        m = df["bid1_price"].notna() & df["ask1_price"].notna() & (df["ask1_price"] > 0)
        bad = m & (df["bid1_price"] > df["ask1_price"] + 1e-6)
        if bad.any():
            errs.append(f"bid1_price > ask1_price: {int(bad.sum())} 行")
    # 竞价与两融相反: available_at 应在**当日**(09:25 竞价结束即可用),
    # 故只要求同日且不早于当日零点、不晚于次日零点。
    pair = _available_at_series(df)
    if pair is not None:
        av, d = pair
        m2 = av.notna()
        bad = m2 & ((av < d) | (av >= d + pd.Timedelta(days=1)))
        if bad.any():
            errs.append(
                f"available_at 不在当日内: {int(bad.sum())} 行"
                "(竞价数据当日 09:30 即可用, 不应推到次日)"
            )
    if errs:
        raise SchemaViolationError(
            "[CallAuction] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


_BILLBOARD_DIRECTIONS = {"BUY", "SELL", "ALL"}


def _check_billboard_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []
    if "direction" in df.columns:
        bad = ~df["direction"].isin(_BILLBOARD_DIRECTIONS)
        if bad.any():
            uniq = sorted(set(df.loc[bad, "direction"].dropna().astype(str)))
            errs.append(f"非法 direction: {uniq[:5]}")
    if {"buy_value", "sell_value", "net_value"}.issubset(df.columns):
        m = df[["buy_value", "sell_value", "net_value"]].notna().all(axis=1)
        diff = (
            df.loc[m, "net_value"] - (df.loc[m, "buy_value"] - df.loc[m, "sell_value"])
        ).abs()
        bad = diff > 1.0
        if bad.any():
            errs.append(f"net_value ≠ buy_value - sell_value: {int(bad.sum())} 行")
    if errs:
        raise SchemaViolationError(
            "[Billboard] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


def _check_review_market_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []
    if df.index.has_duplicates:
        errs.append(f"date 重复: {int(df.index.duplicated().sum())} 行")
    if "volume_bin" in df.columns:
        bad = ~df["volume_bin"].isin(VOLUME_BINS)
        if bad.any():
            uniq = sorted(set(df.loc[bad, "volume_bin"].dropna().astype(str)))
            errs.append(f"非法 volume_bin: {uniq[:5]}")
    if "emotion_bin" in df.columns:
        bad = ~df["emotion_bin"].isin(EMOTION_BINS)
        if bad.any():
            uniq = sorted(set(df.loc[bad, "emotion_bin"].dropna().astype(str)))
            errs.append(f"非法 emotion_bin: {uniq[:5]}")
    if "crowding_bin" in df.columns:
        bad = ~df["crowding_bin"].isin(CROWDING_BINS)
        if bad.any():
            uniq = sorted(set(df.loc[bad, "crowding_bin"].dropna().astype(str)))
            errs.append(f"非法 crowding_bin: {uniq[:5]}")
    for col in ("n_up", "n_down", "n_tradable", "n_limit_up", "n_limit_down", "n_active"):
        if col in df.columns and (df[col] < 0).any():
            errs.append(f"{col} 含负数")
    if errs:
        raise SchemaViolationError(
            "[ReviewMarket] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


def _check_review_style_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []
    if df.index.has_duplicates:
        errs.append(f"date 重复: {int(df.index.duplicated().sum())} 行")
    for col in (
        "size_score", "risk_score", "growth_score", "trend_score", "liq_score",
        "lu_yest_score", "lu_promote_score", "board2_score",
    ):
        if col not in df.columns:
            continue
        bad = ~df[col].isin(STYLE_SCORES)
        if bad.any():
            errs.append(f"{col} 超出 −2～+2")
    if "clock_bin" in df.columns:
        bad = ~df["clock_bin"].isin(CLOCK_BINS)
        if bad.any():
            uniq = sorted(set(df.loc[bad, "clock_bin"].dropna().astype(str)))
            errs.append(f"非法 clock_bin: {uniq[:5]}")
    if "habitat" in df.columns:
        bad = ~df["habitat"].isin(HABITAT_BINS)
        if bad.any():
            uniq = sorted(set(df.loc[bad, "habitat"].dropna().astype(str)))
            errs.append(f"非法 habitat: {uniq[:5]}")
    if "crowding_bin" in df.columns:
        bad = ~df["crowding_bin"].isin(CROWDING_BINS)
        if bad.any():
            uniq = sorted(set(df.loc[bad, "crowding_bin"].dropna().astype(str)))
            errs.append(f"非法 crowding_bin: {uniq[:5]}")
    if "growth_source" in df.columns:
        bad = ~df["growth_source"].isin(GROWTH_SOURCES)
        if bad.any():
            uniq = sorted(set(df.loc[bad, "growth_source"].dropna().astype(str)))
            errs.append(f"非法 growth_source: {uniq[:5]}")
    if errs:
        raise SchemaViolationError(
            "[ReviewStyle] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )


def _check_factor_exposure_invariants(df: pd.DataFrame) -> None:
    errs: list[str] = []
    # (date, symbol) 唯一
    if isinstance(df.index, pd.MultiIndex):
        dup = df.index.duplicated()
        if dup.any():
            errs.append(f"(date, symbol) 重复: {int(dup.sum())} 行")
    # 所有列为数值型
    bad_cols = [
        c for c in df.columns
        if not pd.api.types.is_numeric_dtype(df[c])
    ]
    if bad_cols:
        errs.append(f"非数值型因子列: {bad_cols[:5]}")
    if errs:
        raise SchemaViolationError(
            "[FactorExposure] 不变量违反:\n" + "\n".join(f"  - {e}" for e in errs),
            invariant="; ".join(errs),
        )
