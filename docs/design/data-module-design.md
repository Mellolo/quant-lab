# Quant Lab — 数据与特征模块设计文档

> 版本：v0.3  
> 范围：`core/` + `data/` + `features/` 三个模块的接口契约、数据格式、依赖关系。  
> 不在范围内：具体数据源接入实现（用 Protocol 抽象，留待对接 tushare/akshare/wind/自建库时填充）、下游 labeling/weights/models/evaluation 等模块的内部细节（仅描述上下游接口）。  
> 目标读者：负责实现 / 维护 / 扩展本系统的工程师与研究员。

---

## 1. 设计原则

以下原则全文档约束所有决策。后续如有冲突，以原则优先级（自上而下）裁决。

### P1：PIT（Point-in-Time）正确性

任何在时刻 $t$ 可访问的数据，**必须只来自 $t$ 时刻及之前可知的信息**。这是防 look-ahead bias 的最高约束。具体落地：

- Universe 成分股按日变化，回测 $t$ 日时只用 $t$ 日当时的成分股
- 特征声明 `available_at`（如 `today_close` / `next_open`），数据流水线自动按此 shift
- 公司财报、分红等事件按"公告日"而非"报告期末日"对齐
- 历史数据的修订（如 GDP 二次修正）保留多版本，不直接覆盖

### P2：复权是视图，不是属性

同一份原始数据应当能同时以多种复权视角访问。**存储层只存不复权 + 复权因子**，API 层默认输出后复权，但同时携带原始价格列。下游代码 99% 不需要感知复权这件事。

### P3：依赖单向，禁止反向

模块依赖严格遵守 `core ← data ← features ← labeling ← weights ← models ← sizing/evaluation/allocation`。下游绝不能 import 上游不感知的具体实现（用 Protocol 抽象）。CI 强制检查（建议用 `import-linter`）。

### P4：Schema 即契约

所有数据格式有**明确的列名、类型、单位、不变量、缺失值语义**。Schema 违规在加载时立即报错，而非在下游某处莫名其妙地崩。

### P5：A 股特性是一等公民

复权、停牌、涨跌停、ST、上市新股、T+1、集合竞价、半日市——不是"特殊处理"，是**默认必须处理**。任何接口都为这些场景留出明确位置。

### P6：缓存友好 + 版本化

数据/特征的计算昂贵，必须可缓存。缓存键包含**所有影响结果的输入**（包括代码版本）。特征公式改了 → 版本号必须改 → 缓存自动失效。

### P7：接口与实现分离

具体数据源（tushare / akshare / wind / 自建）不进入核心代码。所有数据访问通过 Protocol。这保证：单元测试可用 fake source；换数据源不修改下游一行；多源并存（如行情用 A，财务用 B）。

---

## 2. 模块划分

```
qlab/
├── core/                # 全局共享：类型、协议、日历、并行原语
├── data/                # 干净、对齐、PIT 正确的 OHLCV + 状态
├── features/            # 特征定义、计算、组装、注册（即书里 Ch8 的 features）
├── labeling/            # Ch3: triple-barrier + meta-labeling
├── weights/             # Ch4: uniqueness + sequential bootstrap + ...
├── models/              # Ch6-9: bagging + PurgedKFold + 特征重要性
├── sizing/              # Ch10
├── evaluation/          # Ch11-15: CPCV、SR/PSR/DSR、PBO、策略风险
├── allocation/          # Ch16: HRP
└── deployment/
```

### 依赖图

```
                ┌────────────────────────────┐
                │           core             │
                │ (types, protocols, calendar│
                │  parallel, store, errors)  │
                └────────────────────────────┘
                          ▲
        ┌─────────────────┼────────────────┐
        │                 │                │
   ┌─────────┐      ┌──────────┐    ┌──────────┐
   │  data   │ ◄──  │ features │ ◄─ │ labeling │
   └─────────┘      └──────────┘    └──────────┘
                                          ▲
                                   ┌──────────────┐
                                   │   weights    │
                                   └──────────────┘
                                          ▲
                                   ┌──────────────┐
                                   │    models    │
                                   └──────────────┘
                                          ▲
                          ┌───────────────┼───────────────┐
                          ▼               ▼               ▼
                    ┌─────────┐   ┌────────────┐   ┌──────────────┐
                    │ sizing  │   │ evaluation │   │  allocation  │
                    └─────────┘   └────────────┘   └──────────────┘
```

注：`features` 依赖 `data`，但不依赖 `labeling`/`weights`——特征本身是无监督的对象。`labeling` 同时依赖 `data`（拿价格序列）和 `features`（用特征做事件采样）。

---

## 3. 核心数据格式

本节是文档的主体。每个数据格式给出：**用途、索引结构、列定义、字段语义、不变量、缺失值语义、单位、持久化形式**。

### 3.0 数据格式按性质分类（导航）

| 性质 | 含义 | 包含的 schema |
|---|---|---|
| **Raw（外部输入）** | 来自数据源、本身不依赖系统内任何其他对象 | §3.1 `DailyBar`、§3.2 `IntradayBar`、§3.3 `Universe`、§3.4 `Calendar`、§3.5 `CorporateAction`、§3.14 `Fundamental`、§3.15 `IndustryClassification`、§3.16 `ConceptClassification` |
| **Derived（派生）** | 由 raw 计算得来，但仍属于"数据"范畴 | §3.6 `AdjFactor` |
| **特征体系** | 由 raw / derived 计算出的预测变量及其元数据 | §3.7 `Feature`、§3.8 `FeatureValue`、§3.9 `FeatureMatrix` |
| **下游消费（labeling/weights）** | 由特征 + 价格序列产出的标注与权重 | §3.10 `Event`、§3.11 `Label`、§3.12 `SampleWeight` |
| **运维 / 审计** | 系统运转产生的元数据 | §3.13 `TrialRegistry` |

### 3.1 `DailyBar` — 日线 K 线

**用途**：策略主时间轴。所有日级因子的原料、回测的成交价基准、状态判断的载体。

**索引**：`MultiIndex(date: Timestamp[D], symbol: str)`
- `date`：自然日，仅交易日有值，时区 `Asia/Shanghai`，时间部分固定 `00:00:00`
- `symbol`：6 位股票代码 + 交易所后缀，如 `600519.SH`、`000001.SZ`、`300750.SZ`、`688981.SH`

**列定义**：

| 列名 | 类型 | 单位 | 含义 |
|---|---|---|---|
| `open` | float64 | 元 | **后复权**开盘价 |
| `high` | float64 | 元 | **后复权**最高价 |
| `low` | float64 | 元 | **后复权**最低价 |
| `close` | float64 | 元 | **后复权**收盘价 |
| `vwap` | float64 | 元 | **后复权**成交量加权均价（= amount / volume，注意 volume 也应一致复权） |
| `open_raw` | float64 | 元 | 不复权开盘价（真实成交价口径） |
| `high_raw` | float64 | 元 | 不复权最高价 |
| `low_raw` | float64 | 元 | 不复权最低价 |
| `close_raw` | float64 | 元 | 不复权收盘价 |
| `volume` | int64 | 股 | 成交股数（按复权调整后；除权日前后跨期可比） |
| `amount` | float64 | 元 | 成交额（与复权无关，是真实流转金额） |
| `adj_factor` | float64 | 无 | 后复权累积因子（不复权 × adj_factor = 后复权价） |
| `limit_up_price` | float64 | 元 | 当日涨停参考价（不复权口径，按交易所规则计算） |
| `limit_down_price` | float64 | 元 | 当日跌停参考价（不复权口径） |
| `is_suspended` | bool | — | 当日是否停牌 |
| `is_limit_up` | bool | — | 当日是否收盘涨停（盘中触及不算） |
| `is_limit_down` | bool | — | 当日是否收盘跌停 |
| `is_st` | bool | — | 当日是否为 ST / *ST（影响涨跌幅 ±5%） |
| `days_since_listing` | int32 | 天 | 距首日上市的自然日数（用于新股屏蔽） |
| `total_shares` | int64 | 股 | 当日总股本（含限售）。用于市值类因子分母 |
| `float_shares` | int64 | 股 | 当日流通股本。用于换手率、流通市值 |
| `free_float_shares` | int64 | 股 | 当日自由流通股本（剔除大股东、战略持股）。用于指数权重类因子 |

**不变量**（系统应在加载时校验）：

1. `close ≈ close_raw * adj_factor`，误差容忍 1e-4
2. `low ≤ open ≤ high` 且 `low ≤ close ≤ high`（同理对 `_raw` 列）
3. `is_limit_up + is_limit_down ≤ 1`
4. `adj_factor > 0`
5. `days_since_listing ≥ 0`
6. `is_suspended = True` → 所有价格列为 `NaN`，`volume = 0`，`amount = 0.0`
7. `is_limit_up = True` → `close_raw == limit_up_price`（精度内）
8. `volume > 0` ⇔ `amount > 0`（不可一边为 0 一边非 0）
9. `free_float_shares ≤ float_shares ≤ total_shares`

**缺失值语义**：

- **停牌日**：价格列 `NaN`，`volume = 0`，状态列正常填充。**绝不 forward-fill**（会造成虚假价格序列）。
- **未上市日**：完全不应出现在数据里（数据源应过滤掉）
- **退市后日期**：同上
- **新股上市首日**：所有字段正常填充；下游若需屏蔽用 `days_since_listing < 30` 即可

**持久化**：Parquet（按月分文件）或 HDF5（按 symbol 分组）。优先 Parquet，列存压缩友好。

**示例**（示意，非真实数据）：

```
date        symbol     close   close_raw  adj_factor  volume   is_suspended  is_limit_up
2024-01-02  600519.SH  1685.0  1685.0     1.0000     2_345_678 False          False
2024-01-03  600519.SH  1702.5  1702.5     1.0000     2_100_000 False          False
2024-06-20  600519.SH  1738.4  1700.0     1.0226     1_500_000 False          False  ← 除权日，adj_factor 跳变
2024-06-21  600519.SH  NaN     NaN        1.0226     0          True           False  ← 停牌
```

---

### 3.2 `IntradayBar` — 日内 K 线

**用途**：派生日级特征的原料（如开盘半小时动量、日内波动率结构）。**不直接进入下游模型**，仅由 `features/` 模块消费。

**索引**：`MultiIndex(timestamp: Timestamp[s], symbol: str)`
- `timestamp`：精确到秒/分钟（取决于频率），时区 `Asia/Shanghai`

**频率**：枚举 `Freq = {'1min', '5min', '15min', '30min', '60min'}`。同一份 `IntradayBar` DataFrame 必须是**单一频率**。

**列定义**：

| 列名 | 类型 | 单位 | 含义 |
|---|---|---|---|
| `open`, `high`, `low`, `close`, `vwap` | float64 | 元 | **后复权**价格 |
| `open_raw`, ..., `close_raw` | float64 | 元 | 不复权价格 |
| `volume` | int64 | 股 | 本 bar 成交股数（复权后） |
| `amount` | float64 | 元 | 本 bar 成交额 |
| `adj_factor` | float64 | 无 | 该交易日的后复权因子（同一天内所有 bar 相同） |
| `session` | str | — | 会话标记（见下） |

**`session` 取值**（A 股专属）：

| 取值 | 时段 | 说明 |
|---|---|---|
| `'open_auction'` | 9:15–9:25 | 开盘集合竞价（仅当数据源提供时） |
| `'morning'` | 9:30–11:30 | 上午连续竞价 |
| `'afternoon'` | 13:00–14:57 | 下午连续竞价（除最后 3 分钟） |
| `'close_auction'` | 14:57–15:00 | 收盘集合竞价 |

**不变量**：

1. 同一 `(date, symbol)` 的所有 bar 共享同一个 `adj_factor`
2. 同一 `(date, symbol)` 所有 bar 的 `volume` 之和 ≤ 日线 `volume`（理论上等，实际可能因数据源差异有微小误差）
3. 停牌日**不出现**任何分钟 bar（与日线"出现但全 NaN"的处理不同）

**缺失值语义**：

- 涨停板封死后无成交：当时段的 bar 可能不存在或 `volume=0`
- 数据源可能漏数据：约定 `volume=0` 表示"该分钟无成交"，`NaN` 表示"数据缺失"——两者语义不同，下游可分别处理

**持久化**：Parquet 按 `(symbol, year-month)` 分片。**惰性加载**——计算特征时只读取需要的窗口。

**重要提示**：1 分钟全 A 数据约 100GB/年级别。生产环境**强烈建议起步用 30min 或 60min**，特殊因子才动 1min。

---

### 3.3 `Universe` — 投资域成员关系

**用途**：定义"在某一天，我关心哪些股票"。PIT 正确——历史回测时不能用未来才加入指数的股票。

**逻辑模型**：函数 `members(date) → set[symbol]`

**物理存储**：稀疏格式更友好

| 列名 | 类型 | 含义 |
|---|---|---|
| `date` | Timestamp[D] | 生效日 |
| `symbol` | str | 股票代码 |
| `in_universe` | bool | 该日是否在域内 |
| `weight` | float64 | 指数权重（指数 universe 才有；自定义 universe 留 NaN） |

**索引**：`MultiIndex(date, symbol)`，但通常按 `date` 切片查询。

**常用 universe**：

| 名称 | 含义 |
|---|---|
| `'csi300'` | 沪深 300 |
| `'csi500'` | 中证 500 |
| `'csi800'` | 中证 800 |
| `'csi1000'` | 中证 1000 |
| `'all_a'` | 全 A（剔除 ST、退市、上市未满 N 天） |
| `'all_a_raw'` | 全 A 不过滤 |

**不变量**：

1. 同一 `(date, symbol)` 唯一
2. 当 universe 是指数时 `sum(weight) ≈ 1.0`（精度内）
3. 不能包含未上市或已退市的股票

**陷阱**：常见的"幸存者偏差"来自**用今天的成分股回测历史**。系统应在 API 层拒绝这种用法——`Universe` 必须按日查询，禁止"取一次成分股用到所有日期"的捷径。

---

### 3.4 `Calendar` — 交易日历

**用途**：所有时间运算的真相来源。区分交易日 / 非交易日 / 半日市；提供"N 个交易日之前/之后"的偏移。

**逻辑模型**：

| 字段 | 类型 | 含义 |
|---|---|---|
| `name` | str | 日历标识符（如 `'SSE'` 上交所、`'SZSE'` 深交所；A 股可统一用 `'SSE'`，两市基本一致） |
| `sessions` | DataFrame | 每个交易日的会话时间表 |
| `holidays` | DatetimeIndex | 法定休市日 |
| `half_days` | DatetimeIndex | 半日市（如有；A 股近年已无） |

**`sessions` 表结构**：

| 列名 | 含义 |
|---|---|
| `date` | 交易日 |
| `open_auction_start` | 9:15 |
| `open_auction_end` | 9:25 |
| `morning_open` | 9:30 |
| `morning_close` | 11:30 |
| `afternoon_open` | 13:00 |
| `afternoon_close` | 15:00 |
| `close_auction_start` | 14:57 |

**必备能力**：

- `is_trading_day(date) → bool`
- `prev_trading_day(date, n=1) → date`
- `next_trading_day(date, n=1) → date`
- `trading_days_between(start, end) → DatetimeIndex`
- `count_trading_days(start, end) → int`

**实现建议**：用 `exchange_calendars` 库的 `'XSHG'`，自己维护就太累。

---

### 3.5 `CorporateAction` — 公司行为事件

**用途**：分红、送股、配股、转股的原始事件记录。**`adj_factor` 由此推算**，所以这是 ground truth，必须可信。

**索引**：默认 `Range`，无业务索引（一行一个事件）

**列定义**：

| 列名 | 类型 | 含义 |
|---|---|---|
| `symbol` | str | 股票代码 |
| `action_type` | str | 见下表 |
| `announce_date` | Timestamp[D] | 公告日 |
| `ex_date` | Timestamp[D] | 除权除息日（**复权使用此日期**） |
| `record_date` | Timestamp[D] | 股权登记日 |
| `pay_date` | Timestamp[D] | 派息日（仅分红） |
| `cash_per_share` | float64 | 每股现金分红（元，税前） |
| `bonus_share_ratio` | float64 | 每股送股比例（如送 10 股每 10 股 = 1.0） |
| `transfer_share_ratio` | float64 | 每股转增比例（来自资本公积） |
| `rights_share_ratio` | float64 | 每股配股比例 |
| `rights_price` | float64 | 配股价（元） |

**`action_type` 取值**：

| 值 | 含义 |
|---|---|
| `'cash_dividend'` | 现金分红 |
| `'stock_dividend'` | 送股 |
| `'capital_transfer'` | 转增 |
| `'rights_issue'` | 配股 |
| `'composite'` | 复合事件（同一公告同时含多种） |

**不变量**：

1. `ex_date >= announce_date`
2. 对应字段非负
3. `composite` 事件可能多个比例字段同时非零；非 `composite` 事件只有对应字段非零

---

### 3.6 `AdjFactor` — 复权因子时间序列

**用途**：从 `CorporateAction` 计算而来。**默认只存后复权累积因子**（不变性好，缓存友好）；前复权按需在 API 层即时计算。

**索引**：`MultiIndex(date, symbol)`

**列**：

| 列名 | 类型 | 含义 |
|---|---|---|
| `adj_factor` | float64 | 后复权累积因子。除权日之前的所有日期，因子相同；除权日及之后，因子根据除权方案跳变 |

**计算方式**（后复权）：

- 上市首日：`adj_factor = 1.0`
- 除权日：`new_factor = old_factor × (1 + bonus_ratio + transfer_ratio + rights_ratio × rights_price / pre_ex_price) / (1 - cash_dividend / pre_ex_price + rights_ratio)`（标准计算式，按交易所规则）

**前复权按需计算**：`forward_close[t] = close_raw[t] × adj_factor[t] / adj_factor[T_latest]`

**不变量**：

1. `adj_factor > 0`，单调非减（后复权情况下）
2. 上市首日 `adj_factor = 1.0`
3. 非除权日，`adj_factor[t] == adj_factor[t-1]`

---

### 3.7 `Feature` — 特征定义

**用途**：声明一个特征"是什么、需要什么、何时可用"。这是一个**对象**，不是数据；其计算结果才是 `FeatureValue`。

**字段（FeatureMeta）**：

| 字段 | 类型 | 含义 |
|---|---|---|
| `name` | str | 特征名，全局唯一，蛇形命名（如 `mom_5d`、`intraday_vol_slope_5d`） |
| `version` | str | 公式版本，语义化版本号（如 `'1.2.0'`）。**改公式必须改版本**，否则缓存命中错误结果 |
| `lookback_days` | int | 计算需要的历史天数（用于 warm-up 期判断） |
| `available_at` | enum | `'today_close'` 或 `'next_open'`，决定对齐时是否需要 shift |
| `requires_intraday` | bool | 是否需要日内数据 |
| `intraday_freq` | Freq \| None | 若需要日内，所需频率 |
| `dependencies` | tuple[str, ...] | 依赖的其他特征名（用于拓扑序计算） |
| `universe_filter` | str | 仅在此 universe 内计算（如 `'all_a'`） |
| `output_dtype` | str | 输出数据类型（默认 `'float64'`） |
| `output_range` | tuple \| None | 输出值的合理范围（用于异常检测，可选） |
| `description` | str | 文档字符串，说明因子含义、参考文献 |

**`available_at` 详解**：

- `'today_close'`：用截至 $t$ 日收盘的数据计算，结果可用于 $t$ 日盘后决策。但 A 股 T+1，最早 $t+1$ 日开盘下单。
- `'next_open'`：依赖日内数据，必须等当日收盘后才能算出（如全日 VWAP slope），结果可用于 $t+1$ 日盘前决策。

**这两个值决定了特征矩阵和价格序列对齐时是否要 shift 一行**——**这是防 look-ahead bias 的关键防线**，下游不需要关心。

---

### 3.8 `FeatureValue` — 单个特征的计算结果

**用途**：一个 `Feature` 在特定 `(date, symbol)` 上的具体取值。

**索引**：`MultiIndex(date, symbol)`

**列**：

| 列名 | 类型 | 含义 |
|---|---|---|
| `value` | float64 | 特征值 |

**附加元数据**（不在 DataFrame 内，存于伴随的 metadata 文件）：

| 字段 | 含义 |
|---|---|
| `feature_name` | 对应的特征名 |
| `feature_version` | 计算时的特征版本 |
| `computed_at` | 计算的时间戳 |
| `data_version` | 计算使用的数据版本（用于审计） |
| `pipeline_hash` | 完整计算流水线的哈希（用于缓存） |

**缺失值语义**：

- **Warm-up 期**：第一个有效日期之前的 `lookback_days` 天，`value = NaN`
- **该日股票停牌**：`value = NaN`
- **该日股票不在 `universe_filter` 内**：可省略行（稀疏存储）或填 `NaN`（稠密存储），二选一保持一致
- **计算失败/数值异常**：`value = NaN` + warning 日志记录

---

### 3.9 `FeatureMatrix` — 多特征对齐后的矩阵

**用途**：模型训练/预测的直接输入。多个 `FeatureValue` 横向拼接后的宽表。

**索引**：`MultiIndex(date, symbol)`

**列**：每列一个特征，列名即特征 `name`

**值**：`float64`

**示例结构**：

```
                          mom_5d   vol_20d  intraday_vol_slope  ...
date        symbol
2024-01-02  000001.SZ      0.023    0.018           -0.5       ...
2024-01-02  600519.SH      0.011    0.015            0.3       ...
2024-01-02  300750.SZ      -0.045   0.032            1.2       ...
2024-01-03  000001.SZ      0.018    0.018           -0.4       ...
...
```

**不变量**：

1. 每列的语义由对应特征的 `FeatureMeta` 定义
2. 索引完整覆盖 `(date_range × universe(date))`——稠密版本（议题 8.5 决议）
3. 同一行的所有特征都对齐到同一个 `available_at` 语义（即都是 $t$ 日收盘后可知，或都是 $t+1$ 日开盘前可知）。不能混合。

**配套对象**：

| 对象 | 含义 |
|---|---|
| `FeatureMatrix.meta` | dict[str, FeatureMeta]，记录每列的元数据 |
| `FeatureMatrix.mask` | 同形状 bool DataFrame，标记每个值是否"可信"（如该日涨停 → 可用做特征但不可下单） |

**存储策略**（议题 8.5 决议）：

- **当前**：全稠密。停牌日、不在 universe 的行 → `NaN`。
- **量级估算**：csi500 × 5 年 × 100 因子 ≈ 500 MB，单机内存无压力。
- **拐点**：当出现以下任一情况时切换到 hybrid 稠密/稀疏混合：
  - universe 扩展到全 A（4000+ 标的）
  - 单实验因子数超过 500
  - 矩阵内存占用 > 10 GB
- **接口稳定性**：`FeatureMatrix.get(date, symbol, feature)` 等访问 API **不暴露**底层稠密/稀疏区别。未来切换不影响下游代码。

**持久化**：Parquet。

---

### 3.10 `Event` — Triple-Barrier 的事件输入（labeling 模块）

**用途**：定义"何时开始一次下注、屏障的宽度是多少、方向是什么"。这是 `data/features` 和 `labeling` 的接口。

**索引**：`MultiIndex(event_start: Timestamp, symbol: str)`
- `event_start`：事件触发时刻（通常是某日收盘或某分钟）

**列**：

| 列名 | 类型 | 含义 |
|---|---|---|
| `t1` | Timestamp \| NaT | 垂直屏障时刻（最长持仓到此为止）。`NaT` 表示无垂直屏障 |
| `target` | float64 | 屏障宽度的目标值（通常是估计波动率，如日波动率） |
| `side` | int8 \| None | 主模型决定的方向（+1 多 / -1 空）。`None` 表示无方向（让模型学方向）；非 None 时进入 meta-labeling 模式 |

**不变量**：

1. `target > 0`
2. `t1 > event_start`（如果非 NaT）
3. `event_start` 必须是交易时刻

**对你的策略（3-10 天持仓）的推荐**：

- `event_start`：每个交易日收盘（或经 CUSUM 筛选后的日期）
- `t1 = event_start + 5 ~ 10 trading days`（用 `Calendar.next_trading_day`）
- `target = daily_vol_estimate`（EWM std of daily returns）
- `side`：如果主模型用基本面或技术规则给方向 → 填入；纯 ML 学方向 → `None`

---

### 3.11 `Label` — Triple-Barrier 的标注输出

**用途**：`Event` 经过 triple-barrier 处理后得到的标签。下游模型的训练目标。

**索引**：与对应 `Event` 完全一致

**列**：

| 列名 | 类型 | 含义 |
|---|---|---|
| `bin` | int8 | 标签值。普通标注：`{-1, 0, 1}`；meta-labeling：`{0, 1}` |
| `ret` | float64 | 实际实现的收益率（首个触碰屏障时的收益） |
| `touch_time` | Timestamp | 首次触碰屏障的实际时间 |
| `touch_type` | str | `'upper'` / `'lower'` / `'vertical'`（哪道屏障先碰） |

**`bin` 的语义**：

普通模式（`Event.side` 为 None）：
- `+1`：触上屏障（盈利达到目标）
- `-1`：触下屏障（亏损达到目标）
- `0`：触垂直屏障（持仓到期）

meta-labeling 模式（`Event.side` 已定）：
- `1`：跟随主模型方向下注会盈利
- `0`：跟随主模型方向下注会亏损或归零

**不变量**：

1. `touch_time >= event_start` 且 `touch_time <= t1`（若 `t1` 非 NaT）
2. `ret` 的符号与 `bin × side`（meta-labeling 模式）或 `bin`（普通模式）一致
3. 持仓期内停牌的处理需明确（推荐：标记 `touch_type='suspended'`，`bin = 0`，下游模型忽略此样本）

---

### 3.12 `SampleWeight` — 样本权重

**用途**：纠正非 IID 标签的影响。下游模型训练时的 `sample_weight` 参数。

**索引**：与对应 `Label` 完全一致

**列**：

| 列名 | 类型 | 含义 |
|---|---|---|
| `uniqueness` | float64 | 平均唯一性（书中 $\bar u_i$），∈ [0, 1] |
| `return_attr` | float64 | 按收益归因的权重（书中 $\tilde w_i$） |
| `time_decay` | float64 | 时间衰减因子，∈ [0, 1] |
| `final_weight` | float64 | 最终权重 = (uniqueness **或** return_attr) × time_decay；二者互斥（return attribution 已含 `/c_t`） |

**不变量**：

1. 所有权重列非负
2. `uniqueness, time_decay ∈ [0, 1]`
3. `sum(final_weight) ≈ N`（归一到 N，与 sklearn 默认假设一致）

---

### 3.13 `TrialRegistry` — 实验追踪表

**用途**：记录"在同一份数据上做过多少次实验"——**Marcos 第三定律**的数据基础。Deflated Sharpe Ratio 计算依赖此表。

**索引**：自增 `trial_id`

**列**：

| 列名 | 类型 | 含义 |
|---|---|---|
| `trial_id` | int64 | 主键 |
| `created_at` | Timestamp | 实验开始时间 |
| `dataset_id` | str | 数据集标识（同一数据集的实验才进入同一 DSR 计算） |
| `pipeline_hash` | str | 完整流水线配置的哈希（决定是否为"同一实验"重跑） |
| `pipeline_config` | str (JSON) | 完整配置（特征列表、模型超参、CV 设置等） |
| `metrics` | str (JSON) | 关键指标（SR、PSR、IR、accuracy、F1...） |
| `git_commit` | str | 代码版本（防止"代码改了忘记记录"） |
| `notes` | str | 自由文本 |

**写入时机**：每次 `Pipeline.fit_eval()` 自动写入。研究员不能手动跳过。

**用途场景**：

- 计算 DSR 时自动读取 `dataset_id` 下的所有 `trials`，统计 N 和 V[SR]
- 复现实验：根据 `pipeline_config` 重跑
- 审计：回答"过去 3 个月在 csi500 数据上跑了多少策略"

**存储位置**（议题 8.7 决议）：

`TrialRegistry` **按 workspace 隔离存储**，不跨 workspace 共享。每个 workspace 的 `trials.db` 独立维护，DSR 计算时只统计本 workspace 的 trials。

理由：
- 不同策略实验互不污染——做动量策略的失败 trial 不应影响均值回归策略的 DSR
- 多人/多策略并行研究时各自的"诚实失败次数"清晰可查
- 跨 workspace 的全局审计可单独跑统计脚本聚合

详见 §7.4 Workspace 目录结构。

---

### 3.14 `Fundamental` — 公司财务数据

**用途**：基本面因子（PE、PB、ROE、营收增速、毛利率…）的原料。**派生因子由 `features/` 模块计算，不进 `data/`**——本 schema 只存原始财务字段。

**设计选择**：折中方案（议题 8.1 决议）——区分披露类型、单版本、不存修订历史。理由：抓住"业绩预告/快报/正式"三种披露的时间差 alpha；A 股修订事件少见，单版本足够。

**索引**：`MultiIndex(announce_date: Timestamp[D], symbol: str, report_type: str)`
- `announce_date`：该条记录的披露日
- `report_type`：见下表

注意索引不是 `(report_period, symbol)`——同一 `report_period` 的同一公司可能有 3 条记录（预告 → 快报 → 正式），每条 `announce_date` 不同。

**元数据列**（必备）：

| 列名 | 类型 | 单位 | 含义 |
|---|---|---|---|
| `report_period` | Timestamp[D] | — | 报告期末（如 `2023-09-30` 表示 2023 三季报） |
| `report_type` | str | — | 见下表 |
| `announce_date` | Timestamp[D] | — | 披露日 |
| `available_at` | Timestamp[s] | — | **真正可用的时刻**（披露日 ≤ 9:30 → 当日 9:30；披露日 > 9:30 → 次日 9:30） |
| `fiscal_year` | int16 | — | 财政年度（如 2023） |
| `fiscal_quarter` | int8 | — | 财政季度（1/2/3/4）|
| `source` | str | — | 数据源标识，便于追溯 |

**`report_type` 取值**：

| 值 | 含义 | 披露时间窗口（A 股惯例） |
|---|---|---|
| `'forecast'` | 业绩预告 | 季度结束后 1-15 天，**仅当业绩同比变动 ±50% 以上或亏损/扭亏时强制披露** |
| `'flash'` | 业绩快报 | 季度结束后 30-60 天，自愿披露 |
| `'official'` | 正式财报 | 一季报 4 月底前；中报 8 月底前；三季报 10 月底前；年报 4 月底前 |

**业务字段**：财务报表项目。**schema 不强制穷尽列表**——下游用什么字段就要求数据源提供什么字段。推荐的核心字段集：

资产负债表（部分）：

| 列名 | 含义 |
|---|---|
| `total_assets` | 总资产 |
| `total_liabilities` | 总负债 |
| `total_equity` | 股东权益合计 |
| `equity_to_shareholders` | 归母股东权益 |
| `cash_and_equivalents` | 货币资金 |
| `accounts_receivable` | 应收账款 |
| `inventory` | 存货 |
| `fixed_assets` | 固定资产 |
| `intangible_assets` | 无形资产 |
| `short_term_debt` | 短期借款 |
| `long_term_debt` | 长期借款 |
| `current_assets` | 流动资产 |
| `current_liabilities` | 流动负债 |

利润表（部分）：

| 列名 | 含义 |
|---|---|
| `total_revenue` | 营业总收入 |
| `operating_revenue` | 营业收入 |
| `operating_cost` | 营业成本 |
| `operating_profit` | 营业利润 |
| `total_profit` | 利润总额 |
| `net_profit` | 净利润 |
| `net_profit_to_shareholders` | 归母净利润 |
| `net_profit_excl_nonrecurring` | 扣非净利润 |
| `eps_basic` | 基本每股收益（元/股） |
| `eps_diluted` | 稀释每股收益（元/股） |

现金流量表（部分）：

| 列名 | 含义 |
|---|---|
| `operating_cash_flow` | 经营活动现金流 |
| `investing_cash_flow` | 投资活动现金流 |
| `financing_cash_flow` | 筹资活动现金流 |

**业绩预告专属字段**（仅 `report_type='forecast'` 有效）：

| 列名 | 含义 |
|---|---|
| `forecast_net_profit_min` | 预告净利润下限（元） |
| `forecast_net_profit_max` | 预告净利润上限（元） |
| `forecast_type` | `'preincrease'`/`'predecrease'`/`'preloss'`/`'preturnloss'` 等 |
| `forecast_change_pct_min` | 同比变动下限（%） |
| `forecast_change_pct_max` | 同比变动上限（%） |

**不变量**：

1. `announce_date >= report_period`（披露必晚于报告期末）
2. `available_at >= announce_date 09:30`（披露当日开盘后才可用）
3. 同一 `(symbol, report_period)` 的 `report_type` 序列必须满足时间顺序：`forecast.announce_date ≤ flash.announce_date ≤ official.announce_date`
4. 业务字段单位统一为元（不是万元/亿元——数据源单位转换在接入时完成）
5. 业绩预告的 min/max：`forecast_net_profit_min ≤ forecast_net_profit_max`

**缺失值语义**：

- 数据源未提供的字段 → `NaN`
- 业绩预告类型本身不要求公司报告所有字段（很多只报变动百分比，不报净利润绝对值）
- 不能用前一期的值 forward-fill 来"填补"未披露的字段

**PIT 查询模式**（关键）：

下游使用基本面数据时，几乎总是这个 pattern：

> "在 $t$ 日，该公司**最新可见**的某字段值是什么？"

具体语义：在 `available_at ≤ t` 的所有记录里，按 `report_period` 取最近一期；同一 `report_period` 优先级 `official > flash > forecast`（即正式财报覆盖之前的预告/快报）。

`features/` 模块应当提供一个标准工具 `latest_fundamental_as_of(field, date)` 封装这个逻辑，避免每个因子作者重复写错。

**派生比率的处理**（PE/PB/ROE 等）：

这些**不在 `Fundamental` schema 内**，它们是 `features/` 的产物，由"行情 × 财务 × 股本"组合计算：

- `PE = market_cap / net_profit_ttm`
  - `market_cap = close_raw × total_shares`（行情 × 股本）
  - `net_profit_ttm`：trailing 12 months 归母净利润，由 `Fundamental` 滚动计算
- `PB = market_cap / equity_to_shareholders`
- `ROE = net_profit_ttm / avg_equity_to_shareholders`

由 `features/library/fundamental.py` 实现，本 schema 不污染。

**持久化**：Parquet，按 `(symbol, fiscal_year)` 分片。数据量小（每股票每年 ~20 条记录 × 数百字段），单机完全可处理。

**示例**（同一公司同一报告期的 3 条记录）：

```
announce_date  symbol     report_period  report_type  available_at         net_profit
2023-10-12     600519.SH  2023-09-30     forecast     2023-10-12 09:30:00  NaN          ← 预告，无净利润
2023-10-25     600519.SH  2023-09-30     flash        2023-10-25 09:30:00  4.5e10
2023-10-28     600519.SH  2023-09-30     official     2023-10-28 09:30:00  4.52e10      ← 正式财报修订
```

---

### 3.15 `IndustryClassification` — 行业分类

**用途**：行业中性化（因子计算时去除行业效应）、行业内 ranking、行业轮动策略。**PIT 正确**——公司行业归属历史上会变动。

**索引**：`MultiIndex(date: Timestamp[D], symbol: str, system: str)`
- `system`：分类体系标识，见下表

**列定义**：

| 列名 | 类型 | 含义 |
|---|---|---|
| `system` | str | 分类体系（见下） |
| `level` | int8 | 分类级别（1/2/3，部分体系有多级） |
| `industry_code` | str | 行业代码（如 `'801080'` 表示申万一级"电子"） |
| `industry_name` | str | 行业中文名 |
| `parent_code` | str | 上级行业代码（一级行业为空） |

**`system` 取值**：

| 值 | 含义 | 级别数 |
|---|---|---|
| `'sw'` | 申万行业分类 | 3 级（一级 31 个、二级 134 个、三级 346 个） |
| `'citic'` | 中信行业分类 | 3 级 |
| `'csrc'` | 证监会行业分类 | 1 级（19 个大类） |
| `'gics'` | 全球行业分类（如有） | 4 级 |

**不变量**：

1. 同一 `(date, symbol, system, level)` 唯一
2. `level > 1` 的记录必须有 `parent_code`，且 `parent_code` 在同一 `system` 的 `level-1` 中存在
3. 公司分类变动时，新旧两条记录**都必须保留**，按 `date` 区分

**PIT 查询模式**：

```
industry_as_of(symbol='600519.SH', date='2024-01-01', system='sw', level=1)
→ industry_code='801080', industry_name='电子'
```

实际实现：用 `merge_asof` 找到最近一次变动后的归属。

**变动频率**：低（年级别）。可以稀疏存储（只存变动事件），查询时 forward-fill 到目标日期。

**持久化**：Parquet，按 `system` 分文件。单文件即可（数据量小）。

**陷阱**：

- **回测时不能用今天的行业分类回溯历史**——某公司去年还在"建材"，今年改"新能源"，用今天的分类会污染历史样本
- 中证、申万的分类调整每年都有，必须保留变动历史
- 同一公司可能同时归属多个体系的不同行业（申万归"电子"、证监会归"制造业"）——下游选哪个由策略自己定

---

### 3.16 `ConceptClassification` — 概念板块分类

**用途**：概念板块归属查询、概念轮动策略、概念暴露度特征的原始输入。

**与 `IndustryClassification` 的关键区别**：

| 维度 | `IndustryClassification` | `ConceptClassification` |
|---|---|---|
| 关系 | 互斥（同一 system+level 下只属一个行业） | 一对多（同时属于多个概念） |
| 层级 | 树状（level 1/2/3） | 扁平 |
| 来源 | 权威体系（申万/中信/证监会） | 平台自定义（东方财富/同花顺/韭研公社） |
| 变动频率 | 低（年级别） | 高（月级别） |
| PIT 可靠性 | 高 | 中（各平台历史变更记录不完整） |

**索引**：`MultiIndex(effective_date: Timestamp[D], symbol: str, source: str)`
- `source`：概念来源平台

**列定义**：

| 列名 | 类型 | 含义 |
|---|---|---|
| `source` | str | 来源平台（`eastmoney` / `ths` / `jiuyan` / `jq` / ...） |
| `concept_code` | str | 概念代码（如 `'BK0001'`） |
| `concept_name` | str | 概念名称（如"光伏"、"AI"） |
| `expired_date` | datetime64 | 被移出该概念的日期；`NaT` 表示仍有效 |

**不变量**：

1. 同一 `(effective_date, symbol, source, concept_code)` 唯一
2. `expired_date >= effective_date`（若非 NaT）

**PIT 查询模式**：

```
concepts_as_of(symbols=['600519.SH'], date='2024-01-01', source='eastmoney')
→ DataFrame[symbol, concept_code, concept_name]  （多行）
```

> **`source` 必须与数据源一致**。概念板块没有权威定义，各平台划分不同，
> `source` 是 index 的一层正是为了让多平台口径并存。因此消费端的 `source`
> 参数必须与生产端一致，否则会**静默返回空**（过滤条件不匹配，不报错）。
>
> 各数据源产出的 `source` 值：
>
> | 数据源 | `source` |
> |---|---|
> | `FakeDataSource` | `eastmoney`（合成数据，沿用默认） |
> | `JQDataSource` | `jq`（聚宽 `jq_concept` 体系，见 `JQDataSource.concept_source`） |
>
> 用 `JQDataSource` 时，`concepts_as_of` / `concept_members_as_of` /
> `DataLayer.concepts()` / `FeatureContext.concepts()` 都需显式传 `source='jq'`
> —— 它们的默认值是 `eastmoney`。

`concept_members_as_of(concept_code='BK0001', date='2024-01-01')` 反向查某概念下所有成分股。

**设计说明**：

概念标签本身是别人给的数据，可能滞后或不准。系统忠实记录原始标签，不做对错判断。动态关联性（如滚动相关性聚类、概念暴露度）作为**特征**在 `features/library/` 中实现，不在数据层。

---

## 4. 关键接口契约（Protocol）

代码层面用 Python 的 `Protocol`（PEP 544，结构化子类型）声明，不强制继承。下游测试可用任意 mock 实现。

### 4.1 `DataSource`（外部数据接入点）

**职责**：从外部源（tushare/akshare/wind/本地库）拉取符合 schema 的数据。

**核心方法**：

| 方法 | 输入 | 输出 |
|---|---|---|
| `fetch_bars` | symbols, start, end, freq, adjust | `DailyBar` 或 `IntradayBar` |
| `fetch_universe` | spec, date_range | `Universe` |
| `fetch_corporate_actions` | symbols, start, end | `CorporateAction` |
| `fetch_calendar` | name | `Calendar` |
| `fetch_status_overrides` | symbols, start, end | DataFrame，补充状态信息（如部分数据源不给 is_st） |
| `fetch_fundamentals` | symbols, start, end, report_types, fields | `Fundamental` |
| `fetch_industry_classification` | symbols, date_range, system, level | `IndustryClassification` |
| `fetch_concepts` | symbols, date_range, source | `ConceptClassification` |
| `fetch_share_capital` | symbols, start, end | DataFrame（用于回填 `DailyBar` 的股本字段） |

**契约**：所有方法返回的数据格式符合本文 §3 定义；schema 不符在加载时抛 `SchemaViolationError`。

**财务数据相关方法说明**：

- `fetch_fundamentals`：`report_types` 参数允许调用方指定要哪些披露类型（默认全部）；`fields` 参数允许只拉取需要的字段以节省带宽。
- `fetch_industry_classification`：`level` 参数指定要哪一级（默认 1）；对于无层级的体系（如 csrc），`level` 参数被忽略。
- `fetch_share_capital`：股本变动数据。结果会被 `data/` 加载层 join 到 `DailyBar` 的 `total_shares` / `float_shares` / `free_float_shares` 列。**调用方不直接使用此方法**——只在数据加载流水线内部使用。

**实现优先级**：
1. `FakeDataSource`（测试用，生成合成数据，确定性）
2. `LocalParquetDataSource`（读本地 Parquet，最快路径）
3. `TushareDataSource` / `AkshareDataSource`（实际数据接入）

### 4.2 `BarStore`（缓存层）

**职责**：缓存 `DataSource` 拉来的数据，避免重复请求。

**核心方法**：

| 方法 | 用途 |
|---|---|
| `has(key)` | 查询缓存是否命中 |
| `get(key)` | 读取缓存 |
| `put(key, df)` | 写入缓存 |
| `invalidate(key_pattern)` | 失效特定缓存 |

**缓存键**：`(data_type, symbol_set_hash, freq, date_range, adjust_mode, source_version)`。任一变化都视为不同缓存。

**实现优先级**：
1. `InMemoryBarStore`（开发用）
2. `HDF5BarStore`（生产用，按 §3 推荐的分片策略）

### 4.3 `Feature`（特征定义）

**职责**：声明 `FeatureMeta` + 提供 `compute` 方法。

**核心方法**：

| 方法 | 输入 | 输出 |
|---|---|---|
| `compute(ctx: FeatureContext)` | 上下文（提供数据访问） | `FeatureValue` |

**契约**：
- `compute` 不能直接 import 数据源——只通过 `ctx`
- `compute` 必须是**纯函数**（同样输入 → 同样输出），便于缓存和并行
- 抛出的所有异常应是 `FeatureComputationError` 或其子类，便于上层统一处理

### 4.4 `FeatureContext`（特征计算上下文）

**职责**：提供数据访问 + 计算服务的句柄。`Feature.compute` 的唯一入口。

**核心方法**：

| 方法 | 用途 |
|---|---|
| `daily(fields, lookback_days=None)` | 取日线数据，自动按 universe 和 PIT 切片 |
| `intraday_rolling(compute_fn, lookback_days, freq)` | 日内派生因子的标准入口，自动并行 + lazy load |
| `fundamental(field, as_of_logic='latest')` | PIT 查询财务数据，封装 §3.14 的"取 available_at ≤ t 最新一条"逻辑 |
| `industry(system, level, date=None)` | PIT 查询行业归属 |
| `upstream(name)` | 取依赖特征的值（已按拓扑序计算好） |
| `calendar` | 访问交易日历 |

**核心属性**（议题 8.6 决议）：

| 属性 | 类型 | 含义 |
|---|---|---|
| `mode` | `Literal['batch', 'incremental']` | 当前计算模式 |
| `target_dates` | `DatetimeIndex` | 本次要算的日期（batch 模式 = 完整范围；incremental 模式 = 增量日期） |
| `history_from` | `Timestamp` | 历史数据起点（确保 `target_dates - lookback_days` 内的数据都可访问） |
| `current_universe` | `Universe` | 当前计算的股票集合 |

**`mode` 详解**（议题 8.6 决议：接口预留，实现分阶段）：

- `mode='batch'`：一次性计算 `target_dates` 全部日期。**当前阶段唯一实现**。研究 / 回测使用。
- `mode='incremental'`：仅计算 `target_dates`（通常 = 单日或近几日），但允许访问 `history_from` 起的历史。**接口预留，实现延后**。实盘服务每日盘后增量更新使用。

**对 `Feature.compute` 实现者的硬约束**：

`compute` 必须是**无状态的**——同样的 `(target_dates, history_from, universe)` 在 batch 与 incremental 模式下**必须产出完全一致的结果**。这是保证"回测↔实盘信号一致"的根基。

具体做法：
- ❌ 错：用 `ctx.daily(['close'])` 拿全量然后自己 rolling N 天
- ✅ 对：用 `ctx.daily(['close'], lookback_days=N)`，让 ctx 决定切片范围

后者在 batch 模式下返回完整序列，在 incremental 模式下只返回 `target_dates - N` 起的数据——但 rolling N 的计算结果相同。

**测试要求**：每个 Feature 必须有 batch ↔ incremental 等价性测试（CI 强制）。

### 4.5 `FeatureStore`（特征结果缓存）

**职责**：缓存 `FeatureValue`，避免重复计算。

**缓存键**：`(feature_name, feature_version, dataset_id, universe_id, date_range, dependencies_hashes)`

**实现**：HDF5 或 Parquet，按特征分文件。

### 4.6 `Universe`（成分股查询）

| 方法 | 输入 | 输出 |
|---|---|---|
| `members(date)` | 某日期 | `list[str]` 成分股代码 |
| `weights(date)` | 某日期 | `Series[symbol → weight]`（指数 universe 有效） |
| `as_dataframe()` | — | 完整的 PIT 成员表（§3.3 格式） |

---

## 5. 端到端数据流示例

以一个具体场景说明：**"用 csi500 上过去 5 年的数据训练一个 3-10 天持仓的模型"**。

### Step 1: 配置

```yaml
# experiments/exp_001.yaml
dataset:
  source: tushare
  universe: csi500
  date_range: ['2019-01-01', '2024-12-31']
  cache: ./store

features:
  - mom_5d
  - mom_20d
  - vol_20d
  - turnover_ratio_5d
  - intraday_vol_slope_5d   # 需日内数据
  - close_auction_imbalance # 需日内数据

labeling:
  method: triple_barrier
  pt_sl: [2, 1]           # 止盈 2 倍波动率，止损 1 倍
  t1_days: 7              # 持仓至多 7 个交易日
  target: ewm_daily_vol   # 用日波动率作为屏障宽度基准
  
weights:
  uniqueness: true
  return_attribution: true
  time_decay: 0.5

model:
  type: bagging_rf
  n_estimators: 1000
  max_samples: avgU       # = uniqueness 均值
  class_weight: balanced

cv:
  method: purged_kfold
  n_splits: 10
  embargo_pct: 0.01

evaluation:
  method: cpcv
  N: 10
  k: 2
```

### Step 2: 数据加载（data/ 模块负责）

```
DataPipeline 读取配置 →
    universe = data.universe.fetch('csi500', '2019-01-01' to '2024-12-31')  
        # 得到 Universe 对象，按日变化的成分股集合
    daily = data.bars.fetch_daily(universe, range, adjust='backward')        
        # 得到 DailyBar DataFrame，符合 §3.1 schema
    minute_lazy = data.bars.lazy_intraday(universe, freq='30min')             
        # 不立即加载，仅返回访问句柄
```

### Step 3: 特征计算（features/ 模块负责）

```
FeatureMatrixBuilder([mom_5d, ..., close_auction_imbalance]) →
    1. 拓扑排序：检查依赖关系（本例无依赖，简单序列）
    2. 对每个特征：
       a. 查 FeatureStore 缓存
       b. 未命中 → 构造 FeatureContext，调用 feature.compute(ctx)
       c. 写入 FeatureStore
       d. 日内派生因子内部用 IntradayAligner 拉分钟数据 → 并行算 → 对齐到日
    3. 横向拼接所有 FeatureValue → FeatureMatrix（§3.9 schema）
    4. 应用 universe_filter 和 status_mask
```

### Step 4: 事件采样（labeling/ 模块负责）

```
EventSampler → Event DataFrame（§3.10 schema）
    event_start: 每个交易日 15:00（或经 CUSUM 筛选）
    t1:          event_start + 7 个交易日（用 Calendar.next_trading_day）
    target:      data.daily.close 算出的 EWM 日波动率
    side:        None（让 ML 学方向）
```

### Step 5: 标注（labeling/ 模块负责）

```
TripleBarrier(pt_sl=[2,1]) →
    输入：Event + DailyBar（取价格序列，注意用 close_raw 算 PnL）
    输出：Label DataFrame（§3.11 schema）
```

### Step 6: 样本权重（weights/ 模块负责）

```
SampleWeighter →
    输入：Event + Label
    输出：SampleWeight DataFrame（§3.12 schema）
```

### Step 7: 训练（models/ 模块负责）

```
TrainingTable = FeatureMatrix join Label on (date, symbol), filter where Label.bin notna →
    X: shape (N_events, N_features)
    y: shape (N_events,)
    w: shape (N_events,) from SampleWeight.final_weight
    
PurgedKFold + BaggingClassifier on (X, y, w)
```

### Step 8: 回测 + 评估（evaluation/ 模块负责）

```
CPCV(N=10, k=2) generates 9 backtest paths →
    每条路径计算 SR
    汇总 → SR 经验分布
    
DSR：从 TrialRegistry 读取本数据集的所有 trials，计算 V[SR] 和 N
    DSR = PSR[SR*]，其中 SR* 由 DSR 公式确定
    
写入 TrialRegistry（§3.13 schema）
```

---

## 6. A 股特殊性处理速查表

| 问题 | 处理位置 | 涉及的数据格式 | 处理方式 |
|---|---|---|---|
| 复权 | `data/adjust` | `DailyBar`, `IntradayBar`, `AdjFactor` | 存原始 + adj_factor，API 默认输出后复权同时携带 `_raw` 列 |
| 停牌 | `data/bars` | `DailyBar.is_suspended`, `FeatureValue=NaN` | 价格 NaN，绝不 ffill；下游计算自动跳过 |
| 涨停板 | `data/bars` | `DailyBar.is_limit_up`, `FeatureMatrix.mask` | 特征可算，但回测下单需检查 mask |
| 跌停板 | 同上 | `DailyBar.is_limit_down` | 同上，且卖出受阻 |
| ST 状态 | `data/bars` | `DailyBar.is_st` | 涨跌幅 ±5% 限制；下游可选择屏蔽 |
| 新股 | `data/bars` | `DailyBar.days_since_listing` | 上市前 N 天屏蔽（默认 30） |
| 退市 | `data/universe` | `Universe.members(date)` 不包含 | PIT universe 自然排除 |
| T+1 | `evaluation` | 回测引擎规则 | 当日买入次日可卖；当日不可平仓 |
| 集合竞价 | `data/bars` | `IntradayBar.session` | 单独标记，特征可选择是否使用 |
| 半日市 | `Calendar.half_days` | — | A 股近年已无；接口预留 |
| 公司行为 | `data/corp_actions` | `CorporateAction` | 推算 adj_factor 的输入 |
| 股本变动 | `data/bars` | `DailyBar.{total,float,free_float}_shares` | 每日 as-of 值，市值因子分母 |
| 财报披露日 ≠ 报告期末 | `data/fundamentals` | `Fundamental.available_at` | 用披露日 +1 个开盘对齐；PIT 查询取 `available_at ≤ t` 最新一条 |
| 业绩预告/快报/正式 | `data/fundamentals` | `Fundamental.report_type` | 三种类型并存，下游查询按 `report_type` 优先级排序 |
| 行业重分类 | `data/industry` | `IndustryClassification`（PIT） | 公司行业变动保留历史记录，回测用当时归属 |
| 行业体系选择 | features 层 | — | 同一公司多体系并存（申万/中信/证监会），策略自选 |
| 派生比率（PE/PB/ROE） | `features/library/fundamental.py` | `FeatureValue` | 由行情 × 财务 × 股本组合计算，不进 `data/` |

---

## 7. 缓存与版本化策略

### 7.1 三层缓存

L1 与 L2 **全局共享**（跨 workspace 共用），L3 **按 workspace 隔离**（议题 8.7 决议）。

```
┌──────────────────────────────────────────────────┐
│ L1: BarStore（原始数据缓存）  — 全局共享          │
│   - 按 (source, symbol, freq, year-month) 分片    │
│   - 缓存键含 source 版本，数据源更新时自动失效     │
│   - 路径: store/bars/                             │
└──────────────────────────────────────────────────┘
                       ↓
┌──────────────────────────────────────────────────┐
│ L2: FeatureStore（特征计算结果缓存）— 全局共享    │
│   - 按 (feature_name, feature_version,            │
│         dataset_id, date_range) 分片              │
│   - 缓存键含特征版本，公式改了自动失效             │
│   - 路径: store/features/                         │
└──────────────────────────────────────────────────┘
                       ↓
┌──────────────────────────────────────────────────┐
│ L3: ModelArtifacts + TrialRegistry — 按 workspace │
│   - 模型 / 评估结果 / 实验记录                    │
│   - 路径: workspaces/<name>/                      │
└──────────────────────────────────────────────────┘
```

**为什么 L1/L2 全局共享**：行情和特征值对所有策略都一样，重复计算浪费。

**为什么 L3 按 workspace 隔离**：每个 workspace 代表独立的研究线，trial 历史必须隔离才能让 DSR 算对（详见 §3.13 和 §7.4）。

### 7.2 版本化原则

| 对象 | 版本字段 | 更新触发 |
|---|---|---|
| `DataSource` 实现 | `source_version` | 数据源 API 升级、字段含义变化 |
| `Feature` | `meta.version` | **公式改了必改**，否则缓存 hit 错结果 |
| `Pipeline` 配置 | `pipeline_hash` | 任何配置变化 |
| 代码 | `git_commit` | 自动获取，写入 TrialRegistry |

### 7.3 增量更新

- **L1**：按 `(symbol, year-month)` 增量；新月份到来仅拉新月份
- **L2**：按 `date_range` 增量；后复权因子的"过去稳定"特性使得增量正确
- **L3**：trial 是 append-only，从不更新已有记录

### 7.4 Workspace 目录结构（议题 8.7 决议）

**Workspace 模型**：每个独立的实验 / 策略 / 研究线对应一个 workspace 目录。共享数据 + 隔离状态。

**根目录布局**：

```
quant-lab/
├── store/                       # 全局共享缓存（L1 + L2）
│   ├── bars/                    # L1: 原始行情 / 财务 / 行业 等
│   │   ├── daily/
│   │   ├── intraday/
│   │   ├── fundamentals/
│   │   └── industry/
│   └── features/                # L2: 特征计算结果
│       ├── mom_5d/
│       ├── intraday_vol_slope/
│       └── ...
│
└── workspaces/                  # 按 workspace 隔离（L3）
    ├── exp_momentum_v1/         # 一个实验/研究线
    │   ├── config.yaml          # 实验配置（特征列表、模型超参等）
    │   ├── trials.db            # 本 workspace 的 TrialRegistry
    │   ├── artifacts/           # 训练好的模型 + 评估结果
    │   │   ├── trial_001/
    │   │   ├── trial_002/
    │   │   └── ...
    │   ├── logs/                # 运行日志
    │   └── notes.md             # 研究员的自由笔记
    ├── exp_meanrev_v1/
    │   └── ...
    └── prod_strategy_001/       # 上线策略
        └── ...
```

**Workspace 操作**：

| 操作 | 行为 |
|---|---|
| **创建** | `qlab workspace create <name>` → 初始化目录 + 空 `config.yaml` + 空 `trials.db` |
| **运行实验** | `qlab run --workspace <name>` → 读 `config.yaml`，调用 Pipeline，写 `trials.db` 和 `artifacts/` |
| **克隆** | `qlab workspace clone <src> <dst>` → 复制配置但**不复制 trials**（新实验线，DSR 重新算） |
| **fork-trial** | `qlab workspace fork-trial <src> <dst> --keep-history` → 同上但**带 trial 历史**（继续同一研究线） |
| **删除** | `qlab workspace rm <name>` → 删除目录；不影响全局 `store/` |
| **审计** | `qlab workspace audit` → 跨 workspace 聚合 trial 统计 |

**多 workspace 共存的约束**：

1. **L1/L2 缓存键必须唯一确定**——不同 workspace 用同一个特征定义时**必须命中同一份缓存**（节省计算）
2. **TrialRegistry 不共享**——`exp_momentum_v1` 的 trial 不影响 `exp_meanrev_v1` 的 DSR
3. **配置变化在 workspace 内追踪**——同一 workspace 反复迭代 = 同一研究线，所有 trial 进同一 DSR 池
4. **不同 workspace 不能跨写**——CLI 工具拒绝跨 workspace 写操作

**短期不支持**：
- 多用户权限管理（YAGNI；单人或小团队信任模型）
- workspace 跨机器同步（用 git/rsync 用户自管）
- workspace 间的 trial 自动迁移

---

## 8. 议题决议记录

> 本节记录设计中曾经的开放问题及最终决议。**所有议题已在 v0.3 全部决议**——保留本节作为决策依据的历史档案。

### 8.1 财务数据（Fundamental Data） — **已决议（v0.2）**

**决议**：采用折中方案（区分披露类型、单版本、不存修订历史）。详见 §3.14 `Fundamental` 与 §3.15 `IndustryClassification`。

**关键设计**：
- 索引 `(announce_date, symbol, report_type)`——同一报告期可能多条记录
- `available_at` 字段统一 PIT 对齐逻辑
- 派生比率（PE/PB/ROE）由 `features/library/fundamental.py` 计算，不入 `data/`
- 行业分类作为独立 schema，PIT 正确，支持多体系并存
- `DailyBar` 新增 `total_shares`/`float_shares`/`free_float_shares`，市值因子分母用

**仍待落实**（实现阶段决定）：
- 是否区分"权益法/成本法"等会计准则变动（短期不做）
- 港股通/沪股通持股数据是否纳入（短期不做）
- 财务数据的"季度同比"等常用变换是否在 `data/` 提供 helper（建议在 `features/` 做）

### 8.2 另类数据（Alternative Data） — **已决议（v0.3）：当前不做**

**决议**：当前阶段不支持另类数据（新闻情感、研报、社交媒体、卫星图像等）。

**理由**：
1. A 股 alt data 数据源质量参差不齐，需要大量预处理（NLP/CV）
2. 对日级 + 3-10 天持仓的中频策略，alt data 边际价值有限——更适合高频和事件驱动
3. 现阶段精力应集中在行情 + 财务的扎实落地

**未来扩展原则**：如果未来要做，**独立 `alt_data/` 顶级模块**，不混进 `data/`。理由：alt data 的预处理 pipeline、存储格式、版本化策略都与行情完全不同，混入会让 `data/` 变成大杂烩。

### 8.3 Level-2 数据 — **已决议（v0.3）：当前不做**

**决议**：当前阶段不支持 Level-2 数据（委托单簿、逐笔成交、大单流向）。

**理由**：
1. 数据量级 TB/年，存储与计算成本高
2. 对日级策略价值有限——主要服务于微观结构因子（如 VPIN、订单流毒性）和日内策略
3. 当前主线场景（3-10 天持仓）几乎用不到

**未来扩展原则**：如果未来要做某个特定微结构因子（如书 Ch19 的 VPIN），**单独为该因子设计数据接入**，不进通用 schema。即"按需局部支持"，而非"全面 Level-2 化"。

### 8.4 跨市场（港股、美股） — **已决议（v0.3）：当前 A 股 only，接口为多市场预留**

**决议**：当前实现仅支持 A 股；架构上为跨市场预留。

**已预留**：
- `Calendar.name` 参数（当前 `'SSE'`，未来可扩展 `'HKEX'` / `'NYSE'` 等）
- `DataSource` Protocol 与具体市场解耦

**未来需新增**：
- 港股专属字段（无涨跌停、配股复杂等）→ 可能用 `HKDailyBar` 子类型
- 美股专属字段（LULD 机制、拆股频繁）→ 同上
- 多币种统一计价（汇率序列 + 计价货币标记）

**约束**：当前设计**不**为了"未来可能跨市场"而牺牲 A 股特性的清晰度。`DailyBar` 的 `is_limit_up`、`is_st`、`days_since_listing` 等 A 股专属字段保留——未来跨市场时按子类型隔离即可。

### 8.5 特征矩阵的稠密 vs 稀疏 — **已决议（v0.3）：起步全稠密**

**决议**：起步采用全稠密存储；接口不暴露稠密/稀疏区别；规模上来后再切换 hybrid。详见 §3.9。

**关键边界**（重申）：
- csi500 + 5 年 + 100 因子 ≈ 500 MB，稠密无压力
- 切换拐点：universe 扩到全 A、单实验因子 > 500、矩阵 > 10 GB

### 8.6 实时 vs 离线 — **已决议（v0.3）：接口预留 incremental，实现分阶段**

**决议**：

| 阶段 | 内容 |
|---|---|
| **当前** | `FeatureContext` 接口上把 `mode: 'batch' \| 'incremental'`、`target_dates`、`history_from` 作为一等概念；仅实现 `batch` 模式 |
| **实盘前** | 实现 `incremental` 模式；新增 `deployment/` 模块封装实时数据接入与每日触发 |

**对 Feature 实现者的硬约束**：`compute` 必须无状态，batch ↔ incremental 等价性由 CI 测试强制。详见 §4.4。

**核心理念**：离线和实盘**共用同一套 Feature 定义**——不允许两套实现各自漂移。这是回测↔实盘信号一致性的根基。

### 8.7 多账户 / 多策略并行 — **已决议（v0.3）：Workspace 模型**

**决议**：采用 Workspace 模型——全局共享 L1/L2 缓存，按 workspace 隔离 L3（TrialRegistry + ModelArtifacts + 配置）。详见 §7.4。

**关键约束**：
- L1/L2 跨 workspace 命中同一缓存（节省计算）
- TrialRegistry 严格按 workspace 隔离（保证 DSR 正确）
- 短期不做多用户权限管理（YAGNI）

---

## 8a. 未来开放议题

> 本节为下一阶段（labeling/weights/models 等模块设计时）的待讨论事项，与本文档当前范围正交。

- **报告期同比变换**：放 `data/fundamentals` 提供 helper 还是 `features/library/fundamental.py`？倾向后者，但需在 features 模块设计时确认。
- **指数权重的获取**：`Universe.weights(date)` 接口设计完成，但成分股权重数据的接入优先级如何？
- **数据质量监控**：是否需要单独的 `data/quality.py` 做数据完整性检查（缺失天数、异常值、与他源对账）？短期可放在数据源适配器内，未来可能独立。

---

## 9. 文档约定

- **必须 / 应当 / 可以**：分别表示 MUST / SHOULD / MAY（RFC 2119 语义）
- **数据格式**全部用 PascalCase（如 `DailyBar`、`FeatureMatrix`）
- **字段名**全部用 snake_case（如 `is_suspended`、`adj_factor`）
- **模块/包名**全部用小写（如 `data`、`features`）
- 中文术语：尽量使用书中（López de Prado, 2018）的中文译法

---

## 10. 修订历史

| 版本 | 日期 | 修改 | 作者 |
|---|---|---|---|
| v0.1 | 2026-05-29 | 首版草案 | Claude + mellolo |
| v0.2 | 2026-05-29 | 议题 8.1 决议：新增 §3.14 `Fundamental`、§3.15 `IndustryClassification`；`DailyBar` 增加股本字段；`DataSource` 增加 `fetch_fundamentals` 等方法；A 股特殊性表扩展财务相关条目 | Claude + mellolo |
| v0.3 | 2026-05-29 | 议题 8.2–8.7 全部决议：另类数据 / Level-2 / 跨市场暂不做但接口预留；§3.9 FeatureMatrix 起步全稠密；§4.4 FeatureContext 增加 `mode`/`target_dates`/`history_from` 预留 incremental 模式；§3.13 TrialRegistry 按 workspace 隔离；新增 §7.4 Workspace 目录结构；§8 由"未决议题"改为"议题决议记录"，新增 §8a 未来开放议题 | Claude + mellolo |
| v0.4 | 2026-06-03 | 新增 §3.16 `ConceptClassification`（概念板块分类）；`DataSource` 增加 `fetch_concepts` 方法 | Claude + mellolo |
