# 第 2 章：金融数据结构（Financial Data Structures）

> 把非结构化的原始金融数据，整理成可供 ML 算法直接消费的"行 × 列"表格。本章核心是 **Bars（K 线的本质）的多种构造法**，以及多产品序列的处理。

## 一、为什么不要直接用别人处理好的数据集

作者开宗明义：**不要消费别人已经处理过的数据集**——你最多发现别人已经知道或马上要知道的东西。你的起点应该是一堆原始、非结构化的数据，由你自己处理出独特的特征。

## 二、金融数据的四大类（按多样性递增排序）

| 类型 | 内容 | 特点与陷阱 |
|---|---|---|
| **Fundamental** | 资产、负债、销售、成本、宏观变量等 | 季度低频、规范化；**有滞后**（Bloomberg 索引到报告期末，但实际发布滞后约 1.5 个月）；常被 backfill / reinstate，因子投资文献里大量论文因数据未对齐而无法复现 |
| **Market** | 价格、成交量、报价/撤单、aggressor side、未平仓量等 | 一天 10+ TB；FIX 报文可重建订单簿；每个参与者都留下"足迹"（TWAP、人类圆整手数等可被识别） |
| **Analytics** | 分析师推荐、信用评级、新闻情感等衍生信号 | 信号已被别人提取过；贵、可能有偏、方法论不透明、不止你一个用户 |
| **Alternative** | 卫星图、CCTV、Google 搜索、Twitter、地理位置、传感器 | **真正的"一手信息"**——油轮、油井、停车场占用率在 Exxon Mobile 财报披露前几个月就有变化；缺点是贵 + 隐私合规风险 |

**经验之谈**：让你的数据基建团队头痛的数据集，往往最值钱——因为竞争对手很可能因为麻烦而放弃，或半途处理错了。

## 三、Bars 的构造：从"时间等间隔"到"信息驱动"

ML 算法假设输入是规整的表格。一行就叫一根 bar。作者把方法分成两大类：**标准 bar** 与 **信息驱动 bar**。

### 3.1 标准 Bars（Standard Bars）

#### 3.1.1 Time Bars（时间等间隔，最常用但最差）
按固定时间间隔（如每分钟）采样 VWAP、OHLC、成交量等。**两大缺陷**：
- 市场不在恒定时间间隔里产生信息：开盘后一小时远比午盘活跃。算法主导的市场更看 CPU 周期而非时钟。time bars 在低活跃期过采样、高活跃期欠采样。
- 时间采样序列常出现**序列相关、异方差、收益非正态**——GARCH 模型就是为了修这些采样错误造成的异方差。

#### 3.1.2 Tick Bars（每 N 笔成交采一根）
每 1000 笔成交采一次。Mandelbrot & Taylor (1967) 已证明：**按成交笔数采样的收益更接近 IID 高斯分布**。

陷阱：开盘/收盘集合竞价会输出"一笔超大成交"——一笔却相当于几千笔的信息量，会造成异常值。

#### 3.1.3 Volume Bars（每成交 V 单位采一根）
解决 tick 数被订单碎片化扭曲的问题：
- 一笔 10 手买盘对 1 个 10 手卖单 = 1 个 tick；对 10 个 1 手卖单 = 10 个 ticks，信息量是一样的。
- 撮合引擎也会把一笔拆成多笔人为部分成交。

Clark (1973) 证明 volume bars 的统计性质比 tick bars 更接近 IID 高斯。

#### 3.1.4 Dollar Bars（每成交 $X 金额采一根）★ 推荐
理由：
- 股价翻倍后，卖出 1000 美元只需买入时一半的股数。**成交股数本身是价值的函数**。
- 公司经常增发、回购、拆股，会扭曲 tick/volume，但 dollar bars 对这些公司行为最 robust。
- 实证：E-mini S&P 500 上跑 tick / volume / dollar bars，**dollar bars 的每日 bar 数量长期最稳定**（见 Figure 2.1）。

进阶：bar size 可以是动态的——按公司自由流通市值或债券存续金额调整。

### 3.2 信息驱动 Bars（Information-Driven Bars）

核心动机：**有信息（informed trader）入场时多采样**。当 signed volume 出现持续失衡时，意味着信息交易者在动手，采样应当加密。

约定：tick rule 给每笔成交打方向

$$
b_t = \begin{cases} b_{t-1} & \text{if } \Delta p_t = 0 \\ \frac{|\Delta p_t|}{\Delta p_t} & \text{if } \Delta p_t \neq 0 \end{cases}
$$

#### 3.2.1 Tick Imbalance Bars (TIB)
失衡指标 $\theta_T = \sum_{t=1}^{T} b_t$。当 $|\theta_T|$ 超过 $E_0[T]\cdot|2P[b_t=1]-1|$ 就截一根 bar。

$E_0[T]$ 用历史 bar 长度的指数加权移动平均估，$2P[b_t=1]-1$ 用历史 $b_t$ 的 EWMA 估。

直觉：每根 TIB 包含**等量信息**（与成交量、价格无关）。

#### 3.2.2 Volume / Dollar Imbalance Bars (VIB / DIB)
把 $b_t$ 换成 $b_t v_t$（VIB 用成交量，DIB 用美元金额）。

$$
T^* = \arg\min_T \{|\theta_T| \geq E_0[T] |2v^+ - E_0[v_t]|\}
$$

其中 $v^+ = P[b_t=1]E_0[v_t|b_t=1]$。

DIB 同时解决了 tick 碎片化、异常值、公司行为三个问题，bar size 是动态的。

#### 3.2.3 Tick / Volume / Dollar Runs Bars (TRB / VRB / DRB)
监控"连续买"或"连续卖"的长度（允许序列中断；不抵消，只计数）：

$$
\theta_T = \max\left\{\sum_{t|b_t=1} b_t, -\sum_{t|b_t=-1} b_t\right\}
$$

当一边的累计 runs 超出预期就采一根。这种 bar 对大单扫盘、冰山单、母单拆子单的"足迹"特别敏感。

## 四、多产品序列处理

### 4.1 The ETF Trick（用 1 美元 NAV 模拟篮子）

**痛点**：交易期货价差、股票/债券篮子时
1. 权重在变，价差自身可能"非交易性收敛"，会误导模型。
2. 价差可能为负，违反多数模型对正价格的假设。
3. 各成分交易时刻不对齐，"最新报价"不可成交。
4. 还要考虑执行成本（穿越买卖价差）。

**解法**：把篮子封装成一个"$1 投资的总收益 ETF"序列 $\{K_t\}$。

需要的字段：开盘价 $o_{i,t}$、收盘价 $p_{i,t}$、点值（含汇率）$\varphi_{i,t}$、成交量 $v_{i,t}$、carry/分红 $d_{i,t}$。

持仓 $h_{i,t}$：
$$
h_{i,t} = \begin{cases} \dfrac{\omega_{i,t} K_t}{o_{i,t+1}\varphi_{i,t}\sum_i|\omega_{i,t}|} & t \in B (\text{再平衡}) \\ h_{i,t-1} & \text{otherwise} \end{cases}
$$

NAV 推进：
$$
K_t = K_{t-1} + \sum_{i=1}^{I} h_{i,t-1}\varphi_{i,t}(\delta_{i,t} + d_{i,t})
$$

其中 $\delta_{i,t}$ 在 roll 日（前一日属于 $B$）用 $p_{i,t}-o_{i,t}$，其他时刻用 $\Delta p_{i,t}$。

**为什么要 de-lever**（即除以 $\sum|\omega|$）：保证按 \$1 净敞口计算持仓，分红已嵌入 $K_t$。

**三个附加变量**（不嵌入 $K_t$，以免做空再平衡时产生虚假盈亏）：
1. **再平衡成本** $c_t = \sum_i (|h_{i,t-1}|p_{i,t} + |h_{i,t}|o_{i,t+1})\varphi_{i,t}\tau_i$，当作负"分红"
2. **买卖价差成本** $\tilde c_t = \sum_i |h_{i,t-1}|p_{i,t}\varphi_{i,t}\tau_i$
3. **可成交单位数** $v_t = \min_i \frac{v_{i,t}}{|h_{i,t-1}|}$（最小活跃成员决定）

### 4.2 PCA Weights（风险按主成分分配）

对协方差 $V$ 做谱分解 $V = W\Lambda W'$。给一组目标风险分配 $R$（满足 $R'\mathbf{1}=1$），求权重：

$$
\beta = \sigma \cdot \left\{\sqrt{R_n/\Lambda_{n,n}}\right\}_{n=1,\dots,N}, \quad \omega = W\beta
$$

如果 `riskDist=None`（默认），意味着把所有风险分配到最小特征值对应的主成分上——这就是经典 PCA 组合。

```python
def pcaWeights(cov, riskDist=None, riskTarget=1.):
    eVal, eVec = np.linalg.eigh(cov)
    indices = eVal.argsort()[::-1]
    eVal, eVec = eVal[indices], eVec[:, indices]
    if riskDist is None:
        riskDist = np.zeros(cov.shape[0]); riskDist[-1] = 1.
    loads = riskTarget * (riskDist / eVal) ** .5
    wghts = np.dot(eVec, np.reshape(loads, (-1, 1)))
    return wghts
```

### 4.3 Single Future Roll（单合约 roll）
对单一期货可以走更直接的路径：
1. 计算每次 roll 时的 gap（前合约收 → 新合约开）
2. 累加 gap 序列，从原价格序列里减去——得到"rolled price"

`matchEnd=True` 是后向 roll（保持当前价不变），`matchEnd=False` 是前向 roll。

**注意**：rolled price 可能变成负数（接触/天然气长期 contango 卖出后），所以：
- **模拟 PnL / mark-to-market**：用 rolled price
- **仓位 sizing / 资金占用**：用原始 raw price
- **想得到非负序列**：用 `returns = rolled_price.diff() / raw_price.shift(1)`，再 `(1+r).cumprod()`

## 五、Sampling Features（事件驱动采样）

bar 出来之后不要直接 dump 给 ML。理由：
1. 一些算法（如 SVM）随样本量扩展性差。
2. ML 在"相关样本"上准确率更高。问"随机时刻下一段 ±5% 收益的方向"准确率会很差；问"在某个催化事件之后下一段 ±5% 收益的方向"会好得多。

### 5.1 Sampling for Reduction（降采样）
- **Linspace 采样**：等间距，简单但步长任意，依赖种子点。
- **Uniform 采样**：均匀随机抽，不依赖步长，但都没保证抽到"信息量最大"的样本。

### 5.2 Event-Based Sampling（事件驱动）

PM 通常在某事件后下注：结构性突变（第 17 章）、信号抽取（第 18 章）、市场微观结构异常（第 19 章）。事件可能是宏观数据发布、波动率跳升、价差远离均衡等。

#### CUSUM Filter
质量管理里的经典工具，检测度量值偏离目标值：

$$
S_t = \max\{0, S_{t-1} + y_t - E_{t-1}[y_t]\}, \quad S_0 = 0
$$

当 $S_t \geq h$ 触发一个事件，并重置 $S_t = 0$。**关键性质**：$S_t$ 不会落到零以下，所以下行偏离会被"过滤"掉一部分；只在累计上行偏离达到阈值时触发。

对称版本同时跟踪上下行：
$$
S_t^+ = \max\{0, S_{t-1}^+ + y_t - E_{t-1}[y_t]\}
$$
$$
S_t^- = \min\{0, S_{t-1}^- + y_t - E_{t-1}[y_t]\}
$$
$$
S_t = \max\{S_t^+, -S_t^-\}
$$

```python
def getTEvents(gRaw, h):
    tEvents, sPos, sNeg = [], 0, 0
    diff = gRaw.diff()
    for i in diff.index[1:]:
        sPos = max(0, sPos + diff.loc[i])
        sNeg = min(0, sNeg + diff.loc[i])
        if sNeg < -h:
            sNeg = 0; tEvents.append(i)
        elif sPos > h:
            sPos = 0; tEvents.append(i)
    return pd.DatetimeIndex(tEvents)
```

**相比 Bollinger Band 的优势**：价格在阈值附近"反复横跳"不会触发多次事件——必须累计一个长度 $h$ 的 run 才会触发。这避免了 Bollinger 之类指标常见的"信号灯灯灯灯"问题。

$S_t$ 也可以替换成结构性突变统计量、熵、市场微观结构指标等。

## 六、本章实操要点

1. **首选 dollar bars** 作为标准 bar 构造法；避免 time bars。
2. **进阶用 information-driven bars**（TIB/VIB/DIB/Runs Bars）来同步采样与信息流动。
3. **多产品建模一律转 ETF trick**——让所有策略代码假设自己只交易一个永不到期的"现金 ETF"。
4. **采样要事件驱动**——用 CUSUM Filter 抽出"值得学习"的时刻，再交给后续标注 / 训练。
5. **rolled price 和 raw price 用途不同**——别混。

## 七、关联阅读

- 事件采样后下一步：标注 → [[chapter-03-labeling]]
- 多产品建模与非 IID 样本权重 → [[chapter-04-sample-weights]]
- 价格序列保留长记忆的转换 → [[chapter-05-fractional-differentiation]]
- 结构性突变检测（CUSUM 的高级版本）→ [[chapter-17-structural-breaks]]
- 熵驱动事件 → [[chapter-18-entropy-features]]
- 市场微观结构特征 → [[chapter-19-microstructural-features]]
