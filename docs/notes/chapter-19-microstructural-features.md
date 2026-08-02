# 第 19 章：市场微观结构特征（Microstructural Features）

> 微观结构数据（FIX 报文级别）是 ML 预测特征最重要的原材料之一。本章按"三代理论"梳理：第一代用价格序列（Tick Rule, Roll, Corwin-Schultz），第二代用成交量（Kyle's λ, Amihud's λ, Hasbrouck's λ），第三代序贯模型（PIN, VPIN）。最后给出**作者自创的"微观结构信息"定义**——基于市场做市商交叉熵损失。

## 一、动机与文献分代

市场微观结构研究"在显式交易规则下交易资产的过程与结果"。微观结构数据集包括撤单、双向拍卖簿、队列、部分成交、aggressor side、修正、替换等。主要来源是 FIX 报文（可向交易所购买）。

三代演化：
- **第一代**：仅用价格信息。代表：tick rule + Roll (1984) 模型。
- **第二代**：成交量数据可用后，研究 volume → price 影响。代表：Kyle (1985)、Amihud (2002)。
- **第三代**（1996 后）：PIN 理论（Easley, O'Hara 等）—— 序贯交易模型。

## 二、第一代：价格序列模型

### 2.1 Tick Rule（确定 aggressor side）

$$
b_t = \begin{cases} 1 & \Delta p_t > 0 \\ -1 & \Delta p_t < 0 \\ b_{t-1} & \Delta p_t = 0 \end{cases}
$$

简单但分类准确率高（Aitken & Frino 1996）。竞争方法：Lee-Ready 1991、Easley et al. 2016。

**从 $\{b_t\}$ 派生的特征**：
- Kalman filter 预测 $E_t[b_{t+1}]$
- 这些预测上的结构性突变（第 17 章）
- $\{b_t\}$ 序列的熵（第 18 章）
- Wald-Wolfowitz runs 检验 t 值
- 累计 $\sum b_t$ 的**分数阶差分**（第 5 章）

### 2.2 Roll Model（用价格估计 bid-ask spread）

中价 $m_t = m_{t-1} + u_t$，$\Delta m_t \sim N[0, \sigma_u^2]$。观测价 $p_t = m_t + b_t c$（$c$ = 半 spread）。

假设：买卖等概率、不相关、与噪声独立。则：
$$
\sigma^2[\Delta p_t] = 2c^2 + \sigma_u^2, \quad \sigma[\Delta p_t, \Delta p_{t-1}] = -c^2
$$

解出：
$$
c = \sqrt{\max\{0, -\sigma[\Delta p_t, \Delta p_{t-1}]\}}, \quad \sigma_u^2 = \sigma^2[\Delta p_t] + 2\sigma[\Delta p_t, \Delta p_{t-1}]
$$

**用途**：流动性差的债券（公司债、市政债）通常没可信报价 → Roll 给出有效 spread 估计。

### 2.3 High-Low Volatility Estimator（Beckers / Parkinson）

对几何布朗运动连续观测：
$$
\sigma_{HL}^2 = \frac{1}{k_1} E\left[\frac{1}{T}\sum \log\left[\frac{H_t}{L_t}\right]^2\right], \quad k_1 = 4\log[2]
$$

比标准 close-to-close 波动率更准确。

### 2.4 Corwin-Schultz Spread Estimator

两条原理：
1. 高价几乎对应 offer 成交，低价几乎对应 bid 成交 → high-low 比反映 spread + volatility
2. high-low 比中波动率分量随时间成比例增长

$$
S = \frac{2(e^\alpha - 1)}{1 + e^\alpha}
$$
$$
\alpha = \frac{\sqrt{2\beta} - \sqrt{\beta}}{3 - 2\sqrt{2}} - \sqrt{\frac{\gamma}{3 - 2\sqrt{2}}}
$$
$$
\beta = E\left[\sum_{j=0}^{1} \log\left[\frac{H_{t-j}}{L_{t-j}}\right]^2\right], \quad \gamma = \log\left[\frac{H_{t-1,t}}{L_{t-1,t}}\right]^2
$$

负 $\alpha$ 设为 0。

**副产品**：能反推 Becker-Parkinson volatility。

```python
def corwinSchultz(series, sl=1):
    beta = getBeta(series, sl)
    gamma = getGamma(series)
    alpha = getAlpha(beta, gamma)
    spread = 2 * (np.exp(alpha) - 1) / (1 + np.exp(alpha))
    return pd.concat([spread, ...], axis=1)
```

特别适用于公司债（BWIC 撮合，没集中订单簿）。可以滚动窗口 + Kalman filter 平滑。

## 三、第二代：策略交易模型

第二代关注 illiquidity（流动性溢价是一类风险，本身就是 ML 特征）。

**实务经验**：用 **t 值** 而不是均值作为特征——t 值已经按估计误差的标准差重标度，含信息更多。

### 3.1 Kyle's Lambda

模型：风险资产终值 $v \sim N[p_0, \Sigma_0]$。两类交易者：
- 噪声交易者 $u \sim N[0, \sigma_u^2]$
- 知情交易者知道 $v$，需求 $x$

市商观测总订单流 $y = x + u$，按 $p = \lambda y + \mu$ 调价。Kyle 求解均衡：
$$
\lambda = \frac{1}{2}\sqrt{\frac{\Sigma_0}{\sigma_u^2}}, \quad \beta = \sqrt{\frac{\sigma_u^2}{\Sigma_0}}
$$

知情交易者期望利润：
$$
E[\pi] = \frac{1}{4\lambda}(v - p_0)^2
$$

知情交易者三大利润来源：
- 证券错价
- 噪声交易者的方差（越大越能隐藏意图）
- 终值方差的倒数

**作为特征**：
$$
\Delta p_t = \lambda (b_t V_t) + \varepsilon_t
$$

### 3.2 Amihud's Lambda

每美元成交量对应的日内价格响应：
$$
|\Delta \log[\tilde p_\tau]| = \lambda \sum_{t \in B_\tau} (p_t V_t) + \varepsilon_\tau
$$

Hasbrouck (2009) 发现日 Amihud's λ 与日内 effective spread 高度秩相关。

### 3.3 Hasbrouck's Lambda（带平方根）

$$
\log[\tilde p_{i,\tau}] - \log[\tilde p_{i,\tau-1}] = \lambda_i \sum_{t \in B_{i,\tau}} (b_{i,t} \sqrt{p_{i,t} V_{i,t}}) + \varepsilon_{i,\tau}
$$

用 Gibbs sampler 贝叶斯估计。**作者建议用第 2 章的"市场活动同步采样"代替 5 分钟时间 bar**。

## 四、第三代：序贯交易模型

策略模型有"单一知情交易者多次交易"；序贯模型则是"随机选中的交易者依次独立到达"。

### 4.1 PIN（Probability of Informed Trading）

证券价格在新信息到达后跳到 $S_B$（坏消息，概率 $\delta$）或 $S_G$（好消息）。$\alpha$ 是事件概率。

期望价格：
$$
E[S_t] = (1 - \alpha_t) S_0 + \alpha_t [\delta_t S_B + (1 - \delta_t) S_G]
$$

知情交易者按 $\mu$ 到达，未知情按 $\varepsilon$。市商在 bid/ask 上 break-even：
$$
E[A_t - B_t] = \frac{\alpha_t \mu (1 - \delta_t)}{\varepsilon + \alpha_t \mu (1 - \delta_t)} (S_G - E[S_t]) + \frac{\alpha_t \mu \delta_t}{\varepsilon + \alpha_t \mu \delta_t}(E[S_t] - S_B)
$$

$\delta = 1/2$ 时：
$$
\text{PIN}_t = \frac{\alpha_t \mu}{\alpha_t \mu + 2\varepsilon}
$$

估计四个未观测参数 $\{\alpha, \delta, \mu, \varepsilon\}$ 用三高斯混合：
$$
P[V^B, V^S] = (1-\alpha) P[V^B, \varepsilon] P[V^S, \varepsilon] + \alpha (\delta \cdot ... + (1-\delta) \cdot ...)
$$

### 4.2 VPIN（Volume-Synchronized PIN）★

Easley et al. (2011) 证明 $E[|V^B - V^S|] \approx \alpha\mu$。高频估计：

$$
\text{VPIN}_\tau = \frac{\sum_\tau |V_\tau^B - V_\tau^S|}{nV}
$$

在 volume clock 下，$V$ 固定 → PIN 直接计算。

**实证**：Andersen & Bondarenko (2013) 说 VPIN 不能预测波动率，但**线性回归**做的；多项后续研究（Abad & Yague 2012, Bethel et al. 2012, Cheung et al. 2015, Kim et al. 2014, Song et al. 2014, Van Ness et al. 2017, Wei et al. 2013）用更复杂方法发现 VPIN 有预测力。

作者评论：18 世纪就有的线性回归看不出 21 世纪市场的非线性模式不奇怪。

## 五、其他微观结构特征

理论之外的特征——让 ML 自己学。

### 5.1 订单大小分布

Easley et al. (2016)：**圆整大小**异常频繁。E-mini S&P 500：
- size 10 比 size 9 高 2.9 倍
- size 50 比 size 49 高 10.9 倍
- size 100 比 size 99 高 16.8 倍
- size 200 比 size 199 高 27.2 倍
- size 250 比 size 249 高 32.5 倍
- size 500 比 size 499 高 57.1 倍

成因：**鼠标 / GUI 交易者**（人类点 GUI 下单）。"硅基交易者"通常 randomize size 隐藏足迹。

**特征**：监控圆整大小成交比例与正常水平的偏离。
- 高于正常 → 人类基本面派多 → 趋势可能形成
- 低于正常 → 硅基派多 → 横盘可能

### 5.2 撤单率、限价单、市价单

Eisler et al. (2012)：小盘与大盘对这些事件响应不同。

Easley et al. (2012) 提出四种**掠夺式算法**：
- **Quote stuffers**：latency arbitrage，用海量报文淹没交易所，拖慢竞争对手
- **Quote danglers**：发报价迫使受压交易者追价
- **Liquidity squeezers**：跟随大资金清仓方向，抽走流动性放大反向
- **Pack hunters**：独立掠夺者协同行动制造级联（NANEX 2011 记录的 stop loss 触发）

**特征**：撤单率、限价单/市价单比例的多种统计量。

### 5.3 TWAP 算法识别

Easley et al. (2012)：E-mini 全天 24 小时分秒分析后发现**每分钟的前几秒成交量异常高**——TWAP 算法在固定时间间隔切单的痕迹。

时段：00:00–01:00 GMT（亚洲开盘）、05:00–09:00（欧洲开盘）、13:00–15:00（美国开盘）、20:00–21:00（美国收盘）。

**特征**：每分钟开始时的 order imbalance，若有持续性可以**前抢大资金的 TWAP**。

### 5.4 Options Market

Muravyev et al. (2013)：美股和美股期权出现分歧时，**通常股价是对的**（期权报价不含经济显著信息），但**期权成交含股票价没有的信息**。

Cremers & Weinbaum (2010)：高波动率 spread 的股票相对高隐含 put 价的股票每周跑赢 50 bps。期权流动性高、股票流动性低时这种可预测性更强。

**特征**：put-call 隐含股价、隐含分布、Greek 模式。

### 5.5 signed order flow 的序列相关

Toth et al. (2011) LSE 数据：**order signs 正自相关持续多天**。来源（不到几小时尺度上）主要是 **order splitting 而不是 herding**。

**特征**：signed volume 的一阶序列相关，作为持续性度量。

## 六、什么是微观结构信息？（作者自创定义）

作者批评文献：大量用"信息不对称"概念，但**从未给"信息"下精确定义**。

### 6.1 作者的定义

**微观结构信息 = 市商决策模型面临的复杂度**。

操作化步骤：
1. 构造特征矩阵 $X = \{X_t\}$（含 VPIN、Kyle's λ、撤单率等）
2. 给每个观测打标签 $y_t$：1 = 市商盈利，0 = 市商亏损（第 3 章方法）
3. 在训练集上 fit 分类器
4. OOS 预测 $\hat y_\tau = E_\tau[y_\tau | X]$
5. 算 cross-entropy loss $L_\tau$（第 9 章）
6. 对 $\{-L_t\}$ 做 KDE，得 CDF $F$
7. **微观结构信息** $\phi_\tau = F[-L_\tau] \in (0, 1)$

### 6.2 物理意义

- 正常市况：市商预测好 → 低 cross-entropy loss → 提供流动性赚钱
- 知情交易者出现：市商预测差（被 adverse-selected）→ 高 cross-entropy loss → $\phi_\tau$ 上升

**微观结构信息只能相对于"市商预测能力"定义**——它本质上是"市商被打懵的程度"。

### 6.3 2010 Flash Crash 应用

市商错误预测"被动 bid 报价能再以更高价卖回"——崩盘不是单个错误造成的，而是几千个预测错误的累积（Easley et al. 2011）。

**如果市商监控了上升的 cross-entropy loss**，本会识别知情交易者出现 + adverse selection 概率升高 → 应当扩 spread 直到 imbalance 停止。但他们没监控，结果继续低价提供流动性直到被迫止损，触发整个流动性危机。

## 七、本章实操要点

1. **三代特征都用**，不要只盯一代：tick rule + Roll + Corwin-Schultz + Kyle/Amihud/Hasbrouck + VPIN。
2. **优先用 t 值** 而不是均值——含信息更多。
3. **Hasbrouck 的 5min time bar 建议** → 改用 dollar/volume bars（第 2 章）。
4. **圆整 size 异常 = 人类交易者比例信号**——监控其与正常水平的偏离。
5. **撤单率高 = 流动性低警告**。
6. **TWAP 识别 → 每分钟前几秒的 imbalance**。
7. **期权特征值得纳入**：put-call 隐含分布远比 futures 期望值含信息。
8. **作者的 $\phi_\tau$ 是杀手锏特征**——直接量化"市商被适应选择的危险程度"。
9. **flash crash 类事件可以提前预警**——只要监控自家预测的 cross-entropy。

## 八、关联阅读

- 上游：tick rule 用于事件采样 → [[chapter-02-financial-data-structures]]
- 上游：bar 构造（VPIN 必用 volume bar）→ [[chapter-02-financial-data-structures]]
- 上游：cross-entropy loss → [[chapter-09-hyperparameter-tuning]]
- 平行：结构性突变 → [[chapter-17-structural-breaks]]
- 平行：熵特征（VPIN 的熵补充）→ [[chapter-18-entropy-features]]
- 下游：把这些特征喂给 ML → [[chapter-06-ensemble-methods]]、[[chapter-08-feature-importance]]
- 参考：O'Hara (1995) Market Microstructure
- 参考：Easley, López de Prado, O'Hara (2013) High-Frequency Trading
