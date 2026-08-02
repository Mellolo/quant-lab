# Quant Lab — 下游模块设计文档

> 版本：v0.1
> 范围：`labeling/` + `weights/` + `models/` + `sizing/` + `evaluation/` + `allocation/` 六个模块的接口契约、数据格式、依赖关系。
> 上游契约（`core/` + `data/` + `features/`）参见 `data-module-design.md`，本文档假定读者已熟悉其 schema 与原则。
> 目标读者：负责实现 / 维护 / 扩展本系统的工程师与研究员。

---

## 0. 设计原则

### P0：上游 schema 不可商量

所有下游模块的输入必须符合 `core/schema.py` 里定义的 Schema。任何不符的输入应在模块入口立即抛 `SchemaViolationError`，不允许"默默吃下不合规数据再算到一半崩溃"。

### P1：继承数据侧七原则

数据模块的 P1–P7（PIT、复权视图、依赖单向、Schema 即契约、A 股一等公民、缓存版本化、接口/实现分离）在下游同样有效。下游不能引入"接受 future leak 的快捷方式"。

### P2：sample weight 一等公民

模型层的 `fit` 必须支持 `sample_weight` 透传——不接受 sample_weight 的实现等于把 Ch4 的成果丢掉。`MyPipeline` 是这层约束的代码化。

### P3：CV 永远是 PurgedKFold（或更严格）

任何形如 sklearn `KFold` / `train_test_split` 的随机切分在金融场景**禁止**使用。下游模块统一通过 `qlab.models.cv.purged_kfold.PurgedKFold` 取得训练/测试索引。

### P4：试验都进 TrialRegistry

任何调用 `models.pipeline` 或 `evaluation.backtest` 的入口最终都必须落 trial（pipeline 配置 + 性能指标 + git commit）。DSR 的正确性依赖此口径。

### P5：模块依赖严格单向

`core ← data ← features ← labeling ← weights ← models ← sizing/evaluation/allocation`。**严禁倒灌**：例如 sizing 不能 import labeling，evaluation 不能 import features 内部。CI 应通过 `import-linter` 强制。

---

## 1. 依赖图

```
                ┌──────────────────────────────┐
                │  上游：core / data / features │
                └───────────────┬──────────────┘
                                ▼
                          ┌───────────┐
                          │ labeling  │ (Event / Label)
                          └─────┬─────┘
                                ▼
                          ┌───────────┐
                          │  weights  │ (SampleWeight)
                          └─────┬─────┘
                                ▼
                          ┌───────────┐
                          │  models   │ (PurgedKFold + Bagging
                          │           │  + FeatImportance + HyperSearch)
                          └─────┬─────┘
                                │
                  ┌─────────────┼──────────────┐
                  ▼             ▼              ▼
            ┌─────────┐  ┌────────────┐  ┌──────────────┐
            │ sizing  │  │ evaluation │  │  allocation  │
            └─────────┘  └────────────┘  └──────────────┘
```

- `sizing`：依赖模型预测概率（pred + proba + side），不直接依赖数据/特征。
- `evaluation`：依赖模型预测产出的 PnL/收益序列；与 TrialRegistry 双向（写入 + 读取以算 DSR）。
- `allocation`：依赖资产收益矩阵（一般来自 evaluation 的回测产出，或直接来自原始 returns）。

---

## 2. labeling 模块

### 2.1 数据流概述

```
价格 + features → EventSampler → Event → TripleBarrier → Label
                                              │
                                              └→ (meta-labeling 模式时) MetaLabel
```

### 2.2 Event Schema（再述 §3.10）

| 字段 | 来源 |
|---|---|
| `event_start` (index) | EventSampler 输出的触发时刻 |
| `symbol` | 标的代码 |
| `t1` | 垂直屏障时刻；用 `Calendar.next_trading_day(event_start, t1_days)` 计算 |
| `target` | 屏障宽度基准。推荐用 `daily_ewm_vol`（在 `labeling/thresholds.py`） |
| `side` | 主模型方向 +1/-1，或 NaN 表示让 ML 学方向 |

**不变量**：`target > 0`；`t1 > event_start`（如非 NaT）。

`labeling.events.to_event_dataframe(pairs, target, t1_days, side, calendar)` 是把 CUSUMFilter 的 `(timestamp, symbol)` 对扩展为 Event schema 的标准入口；它内部自动调 `validate_schema(SCHEMA_EVENT)`。

### 2.3 EventSampler Protocol

```python
class EventSampler(ABC):
    @abstractmethod
    def sample(self, prices: pd.DataFrame | pd.Series) -> pd.DatetimeIndex: ...
```

唯一现成实现：`CUSUMFilter(h, expected=None)`（书 Ch2 §2.5）。多 symbol 时用 `sample_per_symbol(prices_wide) -> DataFrame[timestamp, symbol]`。

**扩展原则**：未来加入 Bollinger / RSI / 事件驱动型采样器时，必须实现 `sample` 并保持"返回触发时间戳"的语义。

### 2.4 TripleBarrier 接口

```python
TripleBarrier(pt: float, sl: float)
label_events(events, close, barrier, *, num_threads=1, is_meta_labeling=None) -> Label
```

- `pt`：止盈倍数（× target）；0 表示无上屏障
- `sl`：止损倍数（× target）；0 表示无下屏障
- `is_meta_labeling`：`None` 时自动判断——`events` 含非空 `side` → meta-labeling
- 输出 schema 是 `SCHEMA_LABEL`，在 `label_events` 末尾强制校验

**`bin` 语义**：

| 模式 | bin 取值 | 含义 |
|---|---|---|
| 普通（side=None） | -1 / 0 / +1 | 触下 / 持仓到期 / 触上 |
| meta-labeling | 0 / 1 | 跟主模型方向是否盈利 |

### 2.5 A 股注意

- **停牌穿仓**：屏障期内停牌的样本，当前实现把 `touch_type` 标为 `'no_data'`、`bin=0`。下游训练时应过滤这类（`labels[labels.touch_type != 'no_data']`）。
- **T+1 限制**：`event_start` 若是当日收盘，最早 `event_start + 1 trading day` 才能下单。`t1` 用 `Calendar.next_trading_day` 计算可天然规避此问题，**不要用 `pd.Timedelta(days=N)`**。
- **涨停板封死**：`event_start` 若在涨停日，应在 EventSampler 阶段就过滤（通过 `FeatureMatrix.mask`），不应进入 Event。

### 2.6 未来扩展

- `BollingerBandSampler`：用波动率通道触发
- `EarningsSurpriseSampler`：用 Fundamental `forecast_change_pct_max - actual` 触发
- `to_event_dataframe` 增加 per-symbol target 的便捷接口

---

## 3. weights 模块

### 3.1 SampleWeight Schema（再述 §3.12）

| 列 | 含义 | 范围 |
|---|---|---|
| `uniqueness` | 平均唯一性 ū_i | [0, 1] |
| `return_attr` | 按收益归因的权重 | ≥ 0 |
| `time_decay` | 时间衰减因子 | [0, 1] |
| `final_weight` | 三者乘积，归一到 sum=N | ≥ 0 |

`sample_weights(labels, close, ...)` 是统一入口，输出前自动调 `validate_schema(SCHEMA_SAMPLE_WEIGHT)`。

### 3.2 核心公式（实现引用）

| 函数 | 公式 |
|---|---|
| `num_concurrent_events(t1, close_idx)` | $c_t = \sum_i \mathbb{1}[t_{i,0} \le t \le t_{i,1}]$ |
| `average_uniqueness(t1, c)` | $\bar u_i = \frac{1}{|[t_{i,0},t_{i,1}]|} \sum_{t \in [t_{i,0},t_{i,1}]} 1/c_t$ |
| `return_attribution_weights(t1, c, close)` | $\tilde w_i = \left| \sum_{t \in [t_{i,0},t_{i,1}]} r_{t-1,t} / c_t \right|$ |
| `time_decay_factors(u, clf_last_w)` | 老样本的权重按 `clf_last_w ∈ [0, 1]` 线性衰减 |

### 3.3 Sequential Bootstrap

```python
ind_mat = build_indicator_matrix(close_idx, t1)  # T × N 的 0/1 指示
sample_idx = seq_bootstrap_sample(ind_mat, sample_length=N, random_state=...)
```

用于代替"标准 bootstrap"——bagging 时减少重叠样本的偏差。`BaggingClassifier` 的 `max_samples` 推荐设为 `mean(uniqueness)`（书 Ch6 §6.3.3）。

### 3.4 多 symbol 注意

`sample_weights` 支持 `labels.index` 重复（同一 event_start 多个 symbol），但 `time_decay` 在多 symbol 下按"事件出现顺序"衰减，而非按 (symbol, date) 排序。如果你需要每个 symbol 独立衰减，先按 symbol 分组分别调用 `sample_weights`，再 concat。

---

## 4. models 模块

### 4.1 PurgedKFold

```python
PurgedKFold(n_splits=3, t1: pd.Series, pct_embargo=0.0)
```

- `t1` 必须是 `pd.Series indexed by event_start`，值为 `event_end`
- `split(X)` 要求 `X.index.equals(t1.index)`（同 index 才能 purge）
- yield `(train_idx, test_idx)`：训练集中已删除"标签区间与测试区间重叠"的样本（Ch7 Snippet 7.1）
- `pct_embargo ∈ [0, 1]`：测试集结束后再屏蔽多大比例的训练样本，防止 lag effect

**与 sklearn 互操作**：可直接传给 `GridSearchCV(..., cv=PurgedKFold(...))`。

### 4.2 CombinatorialPurgedCV

```python
cpcv = CombinatorialPurgedCV(N, k, t1, embargo_pct=0.0)
for split_id, (train, test) in enumerate(cpcv.split(X)):
    ...
paths = cpcv.assemble_paths(preds_per_split)
```

- $\varphi(N, k) = \frac{k}{N} \binom{N}{N-k}$ 条独立回测路径
- 推荐 $k \le N/2$
- 每条路径产出一组 PnL，汇总为 SR 分布——这是 Deflated SR 的输入

### 4.3 Bagging

```python
build_bagging_classifier(
    base_estimator=DecisionTreeClassifier(class_weight=class_weight),
    n_estimators=1000,
    max_samples=avgU,            # ⚠️ 必须用 uniqueness 均值，否则 IID 假设破坏
    max_features=1.0,
    class_weight='balanced',
    n_jobs=-1,
)
```

- 金融场景 **bagging > boosting**：过拟合风险高于欠拟合，bagging 的方差缩减更有用
- `max_samples` 必须 ≤ `mean(SampleWeight.uniqueness)`，否则 bagging 的 in-bag 样本会有不必要的重叠（Ch6 §6.3.3）

### 4.4 Feature Importance

| 函数 | 适用场景 |
|---|---|
| `feat_imp_mdi(clf, feat_names)` | tree-based 模型的 in-sample 重要性。速度最快，但偏向高基数特征 |
| `feat_imp_mda(clf, X, y, cv, sample_weight)` | out-of-sample 准确率下降幅度。**金融场景推荐**，但贵 |
| `feat_imp_sfi(clf_factory, X, y, cv, sample_weight)` | 单变量 OOS 重要性（一次只放一个特征），最贵但最不受共线性影响 |
| `orthogonalize_features(X)` | PCA 正交化后再算 MDI/MDA，去共线 |

**典型流水线**：MDA 选 top-N → orthogonalize → 再 MDA 复核。

### 4.5 超参搜索

```python
clf_hyper_fit(
    feat, lbl, t1, pipe_clf, param_grid,
    cv=PurgedKFold(t1=t1, pct_embargo=0.01),
    rnd_search_iter=20,           # >0 → RandomizedSearch；=0 → GridSearch
    sample_weight=w,
)
```

- `pipe_clf` 必须是 `MyPipeline`（或本身就是单一 estimator），保证 sample_weight 转发
- `log_uniform(a, b)`：scipy 风格的对数均匀分布，给 RandomizedSearch 用
- 内部用 `PurgedKFold` 作 CV split，不允许任何其它 CV scheme

### 4.6 MyPipeline

```python
MyPipeline([('scaler', StandardScaler()), ('clf', RandomForestClassifier())])
pipe.fit(X, y, sample_weight=w)  # ✅ 自动转发给最后一步
```

是 `sklearn.Pipeline` 的子类，唯一改动是在 `fit` 里把 `sample_weight` 转换为 `<last_step>__sample_weight` 参数。所有需要权重训练的场景都应用 `MyPipeline` 而非 sklearn 原生 `Pipeline`。

---

## 5. sizing 模块

### 5.1 从概率到下注规模

```python
bet_size_from_probability(prob, pred, num_classes=2, side=None) -> [-1, 1]
```

公式（二分类）：

$$z = \frac{p - 1/2}{\sqrt{p(1-p)}}, \quad m = 2 \Phi(z) - 1$$

- 高概率（接近 1）→ |m| 接近 1（满仓）；概率接近 0.5 → m 接近 0（不下注）
- `side` 用于 meta-labeling：m 乘以主模型的方向

### 5.2 信号平均与离散化

| 函数 | 用途 |
|---|---|
| `avg_active_signals(signals, t1)` | 同一时刻多个未平仓信号取平均（避免单笔事件支配仓位） |
| `discretize_signal(signal, step_size=0.1)` | 把 [-1, 1] 连续仓位离散成 step 的倍数，降低换手 |

### 5.3 动态仓位 + 限价单

```python
dynamic_position(current_position, max_position, forecast_price, market_price, w) -> int
limit_price(target_position, current_position, max_position, forecast_price, w) -> float
```

公式族基于 sigmoid `m = x / sqrt(w + x²)`，其中 `x = forecast_price - market_price`（书 Ch10 §10.6）。`w` 用 `calibrate_sigmoid_w(x, m)` 校准。

### 5.4 A 股注意

- **T+1**：当日买入次日才能卖出。`dynamic_position` 返回的 target 是"次日开盘后的目标仓位"，调用方需自行排程
- **涨跌停板**：触及板时 limit_price 计算应被 mask 阻断；本模块不知道板的存在，由上层（回测引擎）负责防御

---

## 6. evaluation 模块

### 6.1 backtest

| 入口 | 用途 |
|---|---|
| `walk_forward_backtest(X, y, model_factory, n_splits, sample_weight)` | 经典前推回测；单条路径 |
| `CombinatorialPurgedCV` | 多路径 CPCV，见 §4.2 |

回测产出的 PnL 序列要喂给 §6.2 的 statistics 模块。

### 6.2 statistics

| 函数 | 含义 |
|---|---|
| `sharpe_ratio(returns)` | 原始 SR（同采样频率） |
| `annualized_sharpe(returns, periods_per_year=252)` | 年化 SR |
| `probabilistic_sharpe_ratio(returns, sr_benchmark=0.0)` | PSR：考虑小样本 + 偏度 + 峰度 |
| `deflated_sharpe_ratio(returns, n_trials, var_trials_sr)` | DSR：PSR + 多重检验校正 |
| `returns_hhi(returns)` | 收益集中度（HHI 指数），越高越脆 |
| `compute_dd_tuw(returns)` | 最大回撤 + 水下时间 |
| `classification_scores(y_true, y_pred, sample_weight)` | 精度/召回/F1 + AUC |

**DSR 用法**：
```python
n_trials = trial_registry.count(dataset_id=...)
var_sr = trial_registry.var_sr(dataset_id=...)
dsr = deflated_sharpe_ratio(returns, n_trials, var_sr)
```

### 6.3 risk

策略风险（不是组合风险）—— 策略长期失败的概率。

| 函数 | 用途 |
|---|---|
| `implied_precision(sl, pt, freq, target_sr)` | 反推达成目标 SR 所需的最小预测精度 |
| `prob_strategy_failure(...)` | 给定策略统计量，估算 PnL 长期为负的概率 |

### 6.4 PBO (CSCV)

```python
result = compute_pbo(performance_matrix: T × N_strategies, n_splits=16)
# returns: {'pbo': float ∈ [0,1], 'logits': array}
```

PBO 接近 0.5 → 选 in-sample 最好的策略，在 OOS 大概率会变成"中位以下"，是过拟合的硬证据。

### 6.5 TrialRegistry（再述 §3.13）

- SQLite 实现，按 workspace 隔离
- 自动写入：`Pipeline.fit_eval()` 完成时
- 字段：`dataset_id, pipeline_hash, pipeline_config (JSON), metrics (JSON), git_commit`
- 关键查询：`n_trials(dataset_id)`、`var_sr(dataset_id)` 给 DSR 用

---

## 7. allocation 模块

### 7.1 HRP（Hierarchical Risk Parity，书 Ch16）

```python
hrp = HierarchicalRiskParity(linkage_method='single')
weights = hrp.allocate(returns: pd.DataFrame)  # columns=assets, returns 矩阵
# 或：weights = hrp.allocate_from_cov(cov, corr)
```

**三阶段**：
1. **Tree clustering**：从 correlation 矩阵构造层次聚类
2. **Quasi-diagonalization**：按聚类顺序重排协方差矩阵
3. **Recursive bisection**：自顶向下二分配权

**优势**：不需要协方差矩阵可逆——即使奇异矩阵也能算（传统 Markowitz 在此场景下崩溃）。

### 7.2 IVP（Inverse Variance Portfolio）

```python
weights = inverse_variance_portfolio(cov: pd.DataFrame)  # w_i ∝ 1/σ_i²
```

HRP 的内部子例程，也可独立使用作为简单基准。

### 7.3 用法注意

- HRP 输入的 `returns` 应当是**去重后的同步收益**——多策略 CPCV 路径需先对齐时间索引
- A 股内：行业中性后再 HRP 效果更好（先把每个行业当一个 asset 聚类，再在行业内部分配）

---

## 8. A 股特殊性二级速查表（下游侧补充）

| 问题 | 下游处理位置 |
|---|---|
| 停牌期内 event 屏障穿仓 | `triple_barrier._apply_barriers_single` 标 `touch_type='no_data'` |
| 涨跌停下不能下单 | 在 EventSampler 前用 `FeatureMatrix.mask` 过滤 event_start |
| T+1 不能当日平仓 | `t1` 用 `Calendar.next_trading_day` 算交易日数，**禁止用 `pd.Timedelta(days=N)`** |
| ST 股 ±5% 涨跌幅 | DailyBar 自带 `is_st`，回测引擎根据此切换 limit_pct |
| 半日市 | `Calendar.half_days`（接口预留，当前 A 股已无） |
| 新股屏蔽 | `DailyBar.days_since_listing < 30` 过滤 |
| 多策略同一 dataset | 每个策略一个 workspace，各自维护 TrialRegistry，DSR 各算各的（详见 `data-module-design.md §7.4`） |

---

## 9. 待补议题

- **meta-labeling 与 sizing 的接合**：sizing 的 `bet_size_from_probability(side=...)` 在 meta-labeling 输出（`prob=P(profit), pred ∈ {0,1}, side=主模型方向`）下的最佳 size 公式应有专文。
- **多策略组合时的 trial 共享**：当前 workspace 模型让 DSR 严格隔离。如果两个 workspace 在做"同一标的、不同因子"，是否需要共享 `dataset_id` 以让 DSR 反映真实 N_trials？目前不支持，先按 YAGNI 处理。
- **回测中的滑点/手续费模型**：当前 evaluation/backtest 假设无摩擦。生产化需加入 A 股的双向印花税、过户费、券商佣金模型。
- **行业中性 / 风格中性**：作为特征处理（去除行业 alpha）放在 `features/library/`，作为组合约束（行业权重上限）放在 `allocation/`——需要单独决议。
- **数据快照与回测可重现**：trial 当前只记录 pipeline_hash 与 git_commit，未冻结输入数据快照。生产化需引入 dataset_snapshot_hash。

---

## 10. 文档约定

- 与 `data-module-design.md` 完全一致：MUST / SHOULD / MAY、PascalCase / snake_case / 模块名小写
- 数据格式名（如 `Event`、`Label`、`SampleWeight`）在文档中作为术语使用，对应 `core.schema` 中的 `SCHEMA_*` 对象

---

## 11. 修订历史

| 版本 | 日期 | 修改 | 作者 |
|---|---|---|---|
| v0.1 | 2026-06-01 | 首版，覆盖 labeling/weights/models/sizing/evaluation/allocation 六模块 | Claude + mellolo |
