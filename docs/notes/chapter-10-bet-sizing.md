# 第 10 章：下注规模（Bet Sizing）

> ML 准确率再高，下注规模错了照样亏。本章把"预测概率 → 下注大小"的映射拆成几种方法，并给出动态下注 + 限价单的完整流程。

## 一、动机

作者观察：很多顶级 PM 是优秀的扑克玩家，胜过象棋玩家——核心是**下注规模感**。德州扑克给了 sizing 训练场。

哪怕 ML 分类器准确率很高，没把握好仓位规模，策略仍会亏钱。

## 二、与策略本身无关的 sizing 思路

### 2.1 反例：同方向预测，不同 sizing 决定盈亏

价格 $[1, 0.5, 1.25]$（最终涨 25%）。两个策略都看对方向：
- 策略 1：sizes = $[0.5, 1, 0]$ → 赚 0.5
- 策略 2：sizes = $[1, 0.5, 0]$ → 亏 0.125

差别在于：策略 1 留了仓位余地等信号变强，策略 2 一上来就满仓。

### 2.2 方法 A：用 bet concurrency + Gaussian Mixture

定义 $c_t = c_{t,l} - c_{t,s}$（同时刻活跃多仓数 − 空仓数），类似第 4 章的 label concurrency。

对 $\{c_t\}$ 拟合两个高斯的混合（López de Prado & Foreman 2014 的 EF3M）。bet size：

$$
m_t = \begin{cases} \dfrac{F[c_t] - F[0]}{1 - F[0]} & c_t \geq 0 \\ \dfrac{F[c_t] - F[0]}{F[0]} & c_t < 0 \end{cases}
$$

直觉：当观察到更强信号的概率仅 0.1 时，下注 0.9。信号越强，"继续变强"的概率越小，所以下注越大。

### 2.3 方法 B：预算法（Budgeting）

$\bar c_l = \max_i c_{i,l}$，$\bar c_s = \max_i c_{i,s}$。
$$
m_t = \frac{c_{t,l}}{\bar c_l} - \frac{c_{t,s}}{\bar c_s}
$$
目标：在最后一个并发信号触发之前不会达到满仓。

### 2.4 方法 C：Meta-Labeling（推荐）★

用第 3 章的 meta-labeling 分类器输出"非误分类"的概率，把概率直接映射成下注大小。

**两大优势**：
1. 决定 sizing 的 ML 算法与主模型独立 → 可以纳入"假阳性预测"特征。
2. 概率可以直接翻译成 size。

## 三、从预测概率到下注规模

### 3.1 二分类
$x \in \{-1, 1\}$，原假设 $H_0: p[x=1] = 1/2$。

检验统计量：
$$
z = \frac{p[x=1] - 1/2}{\sqrt{p[x=1](1-p[x=1])}} = \frac{2p[x=1]-1}{2\sqrt{p[x=1](1-p[x=1])}} \sim Z
$$

下注大小：
$$
m = 2Z[z] - 1, \quad m \in [-1, 1]
$$

### 3.2 多分类（one-vs-rest）

标签 $X = \{-1, \ldots, 0, \ldots, 1\}$，预测概率 $p_i$。取 $\tilde p = \max_i p_i$，原假设 $H_0: \tilde p = 1/\|X\|$。

$$
z = \frac{\tilde p - 1/\|X\|}{\sqrt{\tilde p (1-\tilde p)}}, \quad z \in [0, +\infty)
$$

$$
m = x \cdot (2Z[z] - 1)
$$

其中 $x$ 提供方向，$(2Z[z]-1) \in [0, 1]$ 提供大小。

```python
def getSignal(events, stepSize, prob, pred, numClasses, numThreads, **kargs):
    if prob.shape[0] == 0: return pd.Series()
    # 1) OvR t-stat
    signal0 = (prob - 1./numClasses) / (prob * (1.-prob))**.5
    signal0 = pred * (2*norm.cdf(signal0) - 1)  # signal = side * size
    if 'side' in events:
        signal0 *= events.loc[signal0.index, 'side']  # meta-labeling
    # 2) average active + discretize
    df0 = signal0.to_frame('signal').join(events[['t1']], how='left')
    df0 = avgActiveSignals(df0, numThreads)
    signal1 = discreteSignal(signal0=df0, stepSize=stepSize)
    return signal1
```

## 四、Averaging Active Bets

每个 bet 有持仓周期 $[t_0, t_1]$（第 3 章 triple-barrier 决定）。新信号到来时若简单覆盖旧信号 → 换手过度。

**做法**：把当前所有"仍活跃"的 bet 的 size 取平均。"活跃" = 信号已发出 AND 还没触碰 $t_1$（或 $t_1$ 未知）。

```python
def mpAvgActiveSignals(signals, molecule):
    out = pd.Series()
    for loc in molecule:
        df0 = (signals.index.values <= loc) & ((loc < signals['t1']) | pd.isnull(signals['t1']))
        act = signals[df0].index
        if len(act) > 0: out[loc] = signals.loc[act, 'signal'].mean()
        else: out[loc] = 0
    return out
```

## 五、Size Discretization（避免过度交易）

平均之后仍会有微小变动触发小交易。离散化：
$$
m^* = \text{round}\left(\frac{m}{d}\right) \cdot d, \quad d \in (0, 1]
$$

$d$ 决定离散粒度（如 $d=0.2$ → size 只能是 -1, -0.8, ..., 0.8, 1）。

```python
def discreteSignal(signal0, stepSize):
    signal1 = (signal0 / stepSize).round() * stepSize
    signal1[signal1 > 1] = 1
    signal1[signal1 < -1] = -1
    return signal1
```

## 六、动态下注 + 限价单 ★（本章核心实战）

### 6.1 问题设置

$q_t$：当前仓位；$Q$：最大绝对仓位；$f_i$：第 $i$ 个预测对应的目标价；$p_t$：当前市价。

目标仓位：
$$
\hat q_{i,t} = \text{int}[m[\omega, f_i - p_t] \cdot Q]
$$

Sigmoid 仓位函数：
$$
m[\omega, x] = \frac{x}{\sqrt{\omega + x^2}}
$$

$\omega$ 控制 sigmoid 的宽度；$x = f_i - p_t$ 是市价偏离目标价的程度。

**关键性质**：当 $p_t \to f_i$ 时 $\hat q_{i,t} \to 0$ —— 算法**主动了结盈利**。

### 6.2 限价（避免亏损了结）

为防止在 $p_t \to f_i$ 的过程中实际下单产生亏损，需要算一个"保本限价" $\bar p$：

$$
\bar p = \frac{1}{|\hat q_{i,t} - q_t|} \sum_{j=|q_t + \text{sgn}[\hat q_{i,t}-q_t]|}^{|\hat q_{i,t}|} L\left[f_i, \omega, \frac{j}{Q}\right]
$$

其中 $L[f_i, \omega, m]$ 是 $m[\omega, f_i - p_t]$ 对 $p_t$ 的反函数：
$$
L[f_i, \omega, m] = f_i - m \sqrt{\frac{\omega}{1 - m^2}}
$$

由于此函数单调，算法在 $p_t \to f_i$ 时不会实际产生亏损。

### 6.3 校准 $\omega$

给定一对 $(x, m^*)$（如 "价格偏离 10 时下注 0.95 倍"），反推 $\omega$：
$$
\omega = x^2 (m^{*-2} - 1)
$$

### 6.4 完整代码

```python
def betSize(w, x):
    return x * (w + x**2) ** -.5

def getTPos(w, f, mP, maxPos):
    return int(betSize(w, f - mP) * maxPos)

def invPrice(f, w, m):
    return f - m * (w / (1 - m**2)) ** .5

def limitPrice(tPos, pos, f, w, maxPos):
    sgn = 1 if tPos >= pos else -1
    lP = 0
    for j in range(abs(pos+sgn), abs(tPos+1)):
        lP += invPrice(f, w, j/float(maxPos))
    lP /= tPos - pos
    return lP

def getW(x, m):
    return x**2 * (m**-2 - 1)

# 示例
pos, maxPos, mP, f = 0, 100, 100, 115
wParams = {'divergence': 10, 'm': .95}
w = getW(wParams['divergence'], wParams['m'])
tPos = getTPos(w, f, mP, maxPos)
lP = limitPrice(tPos, pos, f, w, maxPos)
# 若 f=110 → tPos = 95（一致于校准）
# 若 f=115, tPos-pos=97 → 限价 pt < 112.3657 < f
```

### 6.5 替代函数：Power Function

$$
\tilde m[\omega, x] = \text{sgn}[x] \cdot |x|^\omega, \quad \omega \geq 0, \quad x \in [-1, 1]
$$

优势：
- $\tilde m[\omega, -1] = -1$、$\tilde m[\omega, 1] = 1$（端点对齐）
- 曲率直接由 $\omega$ 控制
- $\omega > 1$ 时**先凹后凸**（拐点附近近乎平坦），与 sigmoid 的"先凸后凹"相反

## 七、本章实操要点

1. **决定方向 ≠ 决定盈亏** —— sizing 是策略生死线。
2. **三大方法**：bet concurrency + Gaussian mixture / Budgeting / Meta-Labeling。优先 Meta-Labeling。
3. **概率 → size 的公式**：二分类 $m = 2Z[z]-1$，多分类一对其余。
4. **活跃仓位取平均** → 减少换手。
5. **size 离散化** → 防止微调过度交易。
6. **动态 sizing + 反函数限价**：当市价向目标价靠近时自动减仓，并用反函数算限价防亏损。
7. **校准 $\omega$**：用一对样本点（典型偏离 + 期望 size）反推。

## 八、关联阅读

- 上游：meta-labeling 提供概率 → [[chapter-03-labeling]]
- 上游：bet concurrency 与 label concurrency 同源 → [[chapter-04-sample-weights]]
- 平行：策略风险量化 → [[chapter-15-strategy-risk]]
- 评估：F1 / log-loss → [[chapter-09-hyperparameter-tuning]]、[[chapter-14-backtest-statistics]]
- 多产品 sizing 转 ETF trick → [[chapter-02-financial-data-structures]]
- 参考文献：López de Prado & Foreman (2014) EF3M 算法
