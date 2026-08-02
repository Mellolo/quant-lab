# 第 3 章：标注（Labeling）

> 把第 2 章产出的特征矩阵 X 配上标签 y。这一章是全书最具实战价值的章节之一——**Triple-Barrier 方法**与 **Meta-Labeling** 都首次在此提出。

## 一、被滥用的"固定时间窗"标注法

文献中几乎所有金融 ML 论文都用**固定时间窗（Fixed-Time Horizon）**标注：

$$
y_i = \begin{cases} -1 & r_{t_{i,0}, t_{i,0}+h} < -\tau \\ 0 & |r_{t_{i,0}, t_{i,0}+h}| \leq \tau \\ 1 & r_{t_{i,0}, t_{i,0}+h} > \tau \end{cases}
$$

这种做法有三个致命缺陷：

1. **用 time bars**：第 2 章已说过 time bars 统计性质差。
2. **用固定阈值 $\tau$**：不考虑波动率。波动率 1E-4 时（夜盘）和 1E-2 时（开盘）用同一个阈值，会让绝大多数标签变成 0，哪怕收益是可预测且显著的。
3. **不考虑路径**：每个真实策略都有止损线（PM 自定的、风控强制的、或保证金通知触发的）。**根本不存在"中间被止损但最终回到目标价"的可成交策略**。文献几乎从不考虑这一点，反映了投资学术界的现状。

两个最起码的修补：
- 阈值用 EWM 标准差动态化：$\tau \to \sigma_{t_{i,0}}$
- 用 volume / dollar bars（其波动率更接近齐次方差）

但这两条还是没解决路径问题。

## 二、动态阈值（Dynamic Thresholds）

利润目标和止损线应当是**仓位风险的函数**。书中给的日波动率估计代码（用日级 EWM std）：

```python
def getDailyVol(close, span0=100):
    df0 = close.index.searchsorted(close.index - pd.Timedelta(days=1))
    df0 = df0[df0 > 0]
    df0 = pd.Series(close.index[df0-1],
                    index=close.index[close.shape[0]-df0.shape[0]:])
    df0 = close.loc[df0.index] / close.loc[df0.values].values - 1
    df0 = df0.ewm(span=span0).std()
    return df0
```

这个函数的关键不是公式本身，而是它的索引技巧——把"昨天的收盘"对齐到"今天的索引"上，得到一个 reindex 到收盘的日收益序列。

## 三、Triple-Barrier 方法 ★

作者明确说**这个方法在文献里找不到**，是他本人的方法。

### 3.1 三道屏障的定义

- 两道**水平屏障**：止盈线（pt）、止损线（sl）。宽度 = 估计波动率 × 倍数。
- 一道**垂直屏障**：从开仓起 $h$ 根 bar 后到期。

**标注规则**（看哪道屏障先被触碰）：
- 上水平先碰 → $y = 1$
- 下水平先碰 → $y = -1$  
- 垂直先碰 → 可选 $y = \text{sign}(r)$ 或 $y = 0$。作者偏好前者（"在限内实现盈亏"），但实务可两种都试。

**这是路径依赖的标注**——要看整个 $[t_{i,0}, t_{i,0}+h]$ 区间的价格路径。

### 3.2 八种屏障组态

记屏障三元组 $[pt, sl, t_1]$，0 = 关闭，1 = 启用：

| 组态 | 含义 | 评级 |
|---|---|---|
| `[1,1,1]` | 标准：有止盈、止损、到期 | **最常用** |
| `[0,1,1]` | 到期前止损，无止盈 | 有用 |
| `[1,1,0]` | 有止盈止损，无到期 | 有用但持仓可能很久 |
| `[0,0,1]` | 等价于固定时间窗（但用在 dollar/info-driven bars 上有意义） | 偶尔可用 |
| `[1,0,1]` | 等止盈或到期 | 不现实，忽略浮亏 |
| `[1,0,0]` | 只等止盈 | 不现实，可能被锁仓数年 |
| `[0,1,0]` | 只等止损 | 无意义 |
| `[0,0,0]` | 没有屏障 | 永远没标签 |

### 3.3 核心实现（applyPtSlOnT1）

```python
def applyPtSlOnT1(close, events, ptSl, molecule):
    events_ = events.loc[molecule]
    out = events_[['t1']].copy(deep=True)
    pt = ptSl[0] * events_['trgt'] if ptSl[0] > 0 else pd.Series(index=events.index)
    sl = -ptSl[1] * events_['trgt'] if ptSl[1] > 0 else pd.Series(index=events.index)
    for loc, t1 in events_['t1'].fillna(close.index[-1]).iteritems():
        df0 = close[loc:t1]
        df0 = (df0 / close[loc] - 1) * events_.at[loc, 'side']
        out.loc[loc, 'sl'] = df0[df0 < sl[loc]].index.min()
        out.loc[loc, 'pt'] = df0[df0 > pt[loc]].index.min()
    return out
```

注意：参数 `side`（多/空方向）会乘到路径收益上。这意味着同一段下行价格路径，对多头是"逼近止损"，对空头是"逼近止盈"——同一函数自动处理双向。

`molecule` 参数是并行分片用的（见第 20 章）。如果 $I = 10^6$、$h = 10^3$，单个合约要算 $10^9$ 量级的条件——必须并行。

## 四、学习方向 + 大小（Side and Size）

若**无先验方向模型**，必须让 ML 同时学方向和大小。此时：
- 水平屏障必须**对称**（不知道方向就无法区分止盈/止损）。
- 标签是 $\{-1, 0, 1\}$。

`getEvents` 函数（找首次触碰时间）输入：
- `close`：价格序列
- `tEvents`：种子事件时间戳（来自第 2 章 CUSUM 等采样器）
- `ptSl`：屏障宽度倍数
- `t1`：垂直屏障时间序列
- `trgt`：以绝对收益表示的目标
- `minRet`：触发的最小目标收益门槛
- `numThreads`：并行线程数

输出：`events` 表（`t1` = 首碰时间，`trgt` = 目标宽度）。然后 `getBins` 计算实际收益并打标签。

## 五、Meta-Labeling ★★（书中最受欢迎的概念之一）

### 5.1 问题设置

你已有**主模型（primary model）**决定方向（买/卖），但不知道下多大。`Meta-labeling` 是构建一个**次级 ML 模型**，专门学"在主模型已经决定方向的前提下，要不要下注 / 下多大"。

输出标签是 $\{0, 1\}$：
- 1：跟随主模型下注
- 0：放弃这次机会

下注大小可由次级模型的预测概率推导（见第 10 章）。

### 5.2 实现要点

把 `getEvents` 扩展成可接受 `side` 参数：
- `side is None` → 普通学习方向+大小
- `side` 提供 → meta-labeling 模式，水平屏障**可以不对称**

把 `getBins` 扩展成：
- 主模型提供 side 时，把收益乘 side（方向标准化）
- 若 ret ≤ 0 → bin = 0（不下注），否则 bin = 1

### 5.3 为什么 Meta-Labeling 强大？四个理由

1. **白盒兜底**：可以在白盒（如基本面模型或经济学方程）之上加一层 ML，绕开"ML 黑盒"的批评。
2. **过拟合受限**：ML 只决定下注大小，不决定方向，相比同时决定两者，过拟合空间更小。
3. **方向和大小解耦**：可以为多头和空头分别开发独立的主模型 + 次级模型。导致涨/跌的特征往往不一样。
4. **专攻 sizing**：作者强调"小注押对、大注押错足以毁掉账户"。把 sizing 单独训成 ML 任务能显著提升稳健性。

### 5.4 ROC、Precision/Recall 与 F1 视角

二分类的 ROC 曲线 / 混淆矩阵：
- **Precision** = TP / (TP+FP)
- **Recall** = TP / (TP+FN)（统计学里的 power）
- **Accuracy** = (TP+TN) / Total
- **F1** = Precision 和 Recall 的调和平均

**Meta-labeling 的本质**：先训一个**高 recall、低 precision 的主模型**（宁可错抓也不放过），再用次级模型把假阳性过滤掉，最终拿到高 F1。

## 六、Quantamental 路径

近年来许多对冲基金（包括最传统的那些）投入巨资搞 quantamental——用人类专家 + 量化结合。Meta-labeling 正好契合这种模式：

1. 主模型可以是 ML、计量方程、技术规则、基本面分析，**甚至是 PM 凭直觉的判断**。
2. Meta-labeling 的次级模型用市场数据 + **生物特征数据 + 心理评估**等特征。例如：发现 PM 在结构性突变期判断尤其准（更快感知 regime 切换）、或在睡眠不足/疲劳/体重变化时预测变差。
3. 作者预测："未来每家主观对冲基金都会变成 quantamental，meta-labeling 是它们最清晰的过渡路径。"

## 七、Dropping Unnecessary Labels（处理类别不平衡）

某些 ML 分类器对类别极度不平衡敏感。提供了一个递归函数：把出现频率 < `minPct` 的类的样本删掉，直到只剩两类或所有类都达标。

```python
def dropLabels(events, minPct=.05):
    while True:
        df0 = events['bin'].value_counts(normalize=True)
        if df0.min() > minPct or df0.shape[0] < 3: break
        events = events[events['bin'] != df0.argmin()]
    return events
```

另一个动机：sklearn 有个已知 bug（issue #8566）——根源是 sklearn 用 numpy array 而非结构化 array / pandas object 作为标签容器。短期内不会修。后续章节会教如何绕开。

## 八、本章实操要点

1. **抛弃固定时间窗 + 固定阈值标注**——它在金融场景下结构性失败。
2. **使用 Triple-Barrier**：止盈、止损、到期三选一先碰为准；阈值随波动率动态化。
3. **没有方向模型 → 学方向+大小**：水平屏障对称，标签 $\{-1, 0, 1\}$。
4. **有方向模型 → Meta-Labeling**：水平屏障可不对称，标签 $\{0, 1\}$，让 ML 专心学"做或不做 / 下多大"。
5. **F1 提升公式**：主模型追高 recall + 次级模型补 precision。
6. **类别不平衡** → 直接删掉极稀有标签，比硬调权重稳。

## 九、关联阅读

- 事件采样：[[chapter-02-financial-data-structures]] 的 CUSUM filter
- 标签产生后的样本权重（解决重叠区间的非 IID）：[[chapter-04-sample-weights]]
- 把 meta-labeling 输出的概率转成下注大小：[[chapter-10-bet-sizing]]
- Triple-barrier 是路径依赖标注，CV 时必须 purge：[[chapter-07-cross-validation]]
- F1-Score 在策略评估中：[[chapter-14-backtest-statistics]]
- 把策略风险量化：[[chapter-15-strategy-risk]]
