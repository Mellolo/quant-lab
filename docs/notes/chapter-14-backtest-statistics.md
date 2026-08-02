# 第 14 章：回测统计量（Backtest Statistics）

> 本章是回测结果的"度量衣柜"——通用特征、表现、Runs/回撤、执行损耗、风险调整效率、分类得分、归因。引出 **Marcos 第三定律**：每个回测结果必须连同所有尝试次数一起报告。**Probabilistic / Deflated Sharpe Ratio** 是本章压轴的核心方法。

## 一、动机与分类

不管你用哪种回测范式（WF / CV / CPCV / 合成数据），最终要用一组指标向投资者报告。本章覆盖：通用特征、表现、Runs/DD、执行损耗、效率、分类得分、归因。其中部分指标包含在 GIPS 标准内，但 ML 策略需要额外的专用指标。

## 二、通用特征（General Characteristics）

| 指标 | 说明 |
|---|---|
| Time range | 起止日期。要覆盖足够多 regime |
| Average AUM | 多空仓位都按正绝对值计算 |
| Capacity | 维持目标风险调整业绩的最大 AUM。AUM 太小没法 sizing/分散；AUM 太大成本变高、换手率下降 |
| Leverage | 平均仓位 / 平均 AUM。若用了杠杆，必须计成本 |
| Maximum dollar position size | **应接近平均 AUM**——否则说明策略依赖极端事件（可能是 outlier） |
| Ratio of longs | 多头比例。market-neutral 策略应接近 0.5；否则有方向偏置 |
| Frequency of bets | 每年下注数。**同方向连续仓位算一注**——bet 数 < trade 数 |
| Average holding period | 高频策略秒级，低频策略月级。短持仓限制 capacity |
| Annualized turnover | 年成交额 / 年 AUM。bet 少也可能 turnover 高（频繁调整持仓） |
| Correlation to underlying | 与底层宇宙的收益相关系数。**显著正或负 → 策略其实只是在持有/做空指数** |

### 2.1 bet 检测（Snippet 14.1）
flatten（仓位归零）或 flip（仓位反向）就算一注结束：
```python
df0 = tPos[tPos == 0].index
df1 = tPos.shift(1); df1 = df1[df1 != 0].index
bets = df0.intersection(df1)  # flattening
df0 = tPos.iloc[1:] * tPos.iloc[:-1].values
bets = bets.union(df0[df0 < 0].index).sort_values()  # flips
```

### 2.2 持仓期估计（Snippet 14.2）
按"平均开仓时间"算法配对计算。仓位增加 → 更新 entry time 加权平均；仓位减少（含 flip）→ 算出该子段的持仓时长。

## 三、Performance（不调风险的盈亏）

- PnL（含末端平仓）
- 多头贡献 PnL
- 年化总收益率
- Hit ratio：盈利 bet 数 / 总 bet 数
- 盈利 bet 平均回报、亏损 bet 平均回报

### 3.1 TWRR（时间加权收益率）

GIPS 要求按 TWRR 计算业绩，对外部现金流逐日加权：
$$
r_{i,t} = \frac{\pi_{i,t}}{K_{i,t}}
$$
其中 $\pi_{i,t}$ 是 MtM 盈亏，$K_{i,t}$ 是该子区间的资产市值，包括为新增买入而准备的资金（max{0, ...} 项）。

子区间收益用几何方式串联：
$$
\varphi_{i,T} = \prod_{t=1}^{T} (1 + r_{i,t})
$$
年化：$R_i = \varphi_{i,T}^{-y_i} - 1$。

**假设**：现金流入算在日初，现金流出算在日末。

## 四、Runs（连续同号收益）

金融收益序列几乎从不 IID，所以会有 runs（连续同向回报序列）。Runs 推高下行风险。

### 4.1 Returns Concentration（HHI 风格）

把正收益、负收益分别归一为权重：
$$
w_t^+ = r_t^+ \left(\sum_t r_t^+\right)^{-1}
$$

类 Herfindahl-Hirschman 指数：
$$
h^+ = \frac{\sum_t (w_t^+)^2 - \|w^+\|^{-1}}{1 - \|w^+\|^{-1}}
$$

性质：
- $h^+ \in [0, 1]$
- $h^+ = 0$ ↔ 所有权重相等（均匀回报）
- $h^+ = 1$ ↔ 只有一笔非零回报

理想策略：
- 高 SR
- 每年 bet 数高
- Hit ratio 高（$\|r^-\|$ 相对低）
- $h^+$ 低（无右尾）
- $h^-$ 低（无左尾）
- $h[t]$ 低（bet 不集中在时间上）

```python
def getHHI(betRet):
    if betRet.shape[0] <= 2: return np.nan
    wght = betRet / betRet.sum()
    hhi = (wght**2).sum()
    hhi = (hhi - betRet.shape[0]**-1) / (1. - betRet.shape[0]**-1)
    return hhi

rHHIPos = getHHI(ret[ret >= 0])
rHHINeg = getHHI(ret[ret < 0])
tHHI = getHHI(ret.groupby(pd.TimeGrouper(freq='M')).count())
```

### 4.2 Drawdown 和 Time under Water

- **DD**：连续两个 HWM（high-watermark）之间的最大亏损
- **TuW**：从 HWM 到 PnL 重新突破该 HWM 之间的时长

```python
def computeDD_TuW(series, dollars=False):
    df0 = series.to_frame('pnl')
    df0['hwm'] = series.expanding().max()
    df1 = df0.groupby('hwm').min().reset_index()
    df1.columns = ['hwm', 'min']
    df1.index = df0['hwm'].drop_duplicates(keep='first').index
    df1 = df1[df1['hwm'] > df1['min']]
    if dollars: dd = df1['hwm'] - df1['min']
    else: dd = 1 - df1['min'] / df1['hwm']
    tuw = ((df1.index[1:] - df1.index[:-1]) / np.timedelta64(1, 'Y')).values
    tuw = pd.Series(tuw, index=df1.index[:-1])
    return dd, tuw
```

### 4.3 Runs 评估指标
- 正收益 HHI / 负收益 HHI / 月度 bet 集中度 HHI
- 95% 分位 DD / 95% 分位 TuW

## 五、Implementation Shortfall（执行损耗）

- Broker fees per turnover
- Average slippage per turnover：执行价偏离 mid-price 的部分
- **Dollar performance per turnover**：盈亏（含 fee 和 slippage） / 总换手——衡量"成本可以再贵多少策略才打平"
- **Return on execution costs**：盈亏 / 总执行成本——应该是大倍数（保证比预期更差的执行环境下策略仍能存活）

## 六、Efficiency（风险调整效率）

### 6.1 Sharpe Ratio

$$
\text{SR} = \mu / \sigma
$$

假设超额收益 IID 高斯。但 $\mu, \sigma$ 未知，所以 SR 计算存在显著估计误差。

### 6.2 Probabilistic Sharpe Ratio（PSR）

修正短时间段、偏度、肥尾导致的 SR 虚高：

$$
\widehat{\text{PSR}}[\text{SR}^*] = Z\left[\frac{(\widehat{\text{SR}} - \text{SR}^*)\sqrt{T-1}}{\sqrt{1 - \hat\gamma_3 \widehat{\text{SR}} + \frac{\hat\gamma_4 - 1}{4}\widehat{\text{SR}}^2}}\right]
$$

其中：
- $Z[.]$：标准正态 CDF
- $T$：观测数
- $\hat\gamma_3$：偏度
- $\hat\gamma_4$：峰度（高斯下为 3）
- $\text{SR}^*$：基准 SR（可设为 0）

**PSR 增长方向**：$\widehat{\text{SR}}$ 高、$T$ 长、$\hat\gamma_3 > 0$（右偏）；**PSR 下降方向**：$\hat\gamma_4$ 大（肥尾）。

### 6.3 Deflated Sharpe Ratio（DSR）★

PSR 还没考虑"多重检验"。DSR = PSR，但 $\text{SR}^*$ 不是用户给定，而是**估算多次试验下最大 SR 的期望值**：

$$
\text{SR}^* = \sqrt{V[\{\widehat{\text{SR}}_n\}]} \cdot \left((1 - \gamma)Z^{-1}\left[1 - \frac{1}{N}\right] + \gamma Z^{-1}\left[1 - \frac{1}{N}e^{-1}\right]\right)
$$

- $V[\{\widehat{\text{SR}}_n\}]$：各次试验的 SR 方差
- $N$：独立试验数
- $\gamma \approx 0.5772$：Euler-Mascheroni 常数

直觉：真 SR=0 时，$N$ 次试验中最大 SR 的期望显著大于 0。**DSR 把这个"假阳性基线"作为门槛**。

### 6.4 Marcos 第三定律

> 每个回测结果必须连同其生产过程中所有的试验次数一起报告。没有这个信息，无法评估回测的"假发现"概率。

### 6.5 Efficiency 指标汇总
- **Annualized SR**：乘 $\sqrt{a}$（$a$ 为年观测数）
- **Information Ratio**：SR 的相对基准版本（excess return / tracking error）
- **PSR**：应 > 0.95（5% 显著性水平）
- **DSR**：应 > 0.95

## 七、Classification Scores（meta-labeling 专用）

主模型识别机会，次模型决定是否跟随。次模型本身要单独评估：

- **Accuracy** = (TP + TN) / 全部
- **Precision** = TP / (TP + FP)
- **Recall** = TP / (TP + FN)
- **F1** = $2 \cdot \dfrac{\text{precision} \cdot \text{recall}}{\text{precision} + \text{recall}}$（精度和召回的调和平均）
- **Neg log-loss**：见第 9 章 4 节——考虑预测概率

### 7.1 F1 在退化情形下的陷阱

| 退化情形 | 退化 | Accuracy | Precision | Recall | F1 |
|---|---|---|---|---|---|
| 观察值全为 1 | TN=FP=0 | =recall | 1 | [0,1] | [0,1] |
| 观察值全为 0 | TP=FN=0 | [0,1] | 0 | NaN | NaN |
| 预测值全为 1 | TN=FN=0 | =precision | [0,1] | 1 | [0,1] |
| 预测值全为 0 | TP=FP=0 | [0,1] | NaN | 0 | NaN |

- 观察值全 1：F1 = $2 \cdot \text{recall}/(1 + \text{recall}) \geq \text{recall}$
- 预测值全 1：F1 = $2 \cdot \text{precision}/(1 + \text{precision}) \geq \text{precision}$
- sklearn 在两种 NaN 情况下打 `UndefinedMetricWarning`，把 F1 设为 0

**关键警告**：如果正例 >> 负例，分类器全猜正例时 F1 仍然很高——这时应当**切换正负例定义**。

## 八、Attribution（归因）

把 PnL 分解到风险类（duration / credit / liquidity / sector / currency / sovereign / issuer 等）。

注意：**风险类之间不正交**——例如高流动性债券倾向于短久期、高评级、大型发行人、美元计价。所以归因 PnL 加和不等于总 PnL，但能给每个风险类一个 SR（或 IR）。

Barra 多因子方法是最流行的代表（Barra 1998, 2013；Zhang & Rachev 2004）。

### 8.1 类内归因（防重叠）
1. 让每个证券任意时点都只属于一个类别（用 disjoint partition）
2. 对每个风险类，按组合权重构造一个类别指数（权重归一）
3. 重复 2，但用宇宙基准的权重（如 Markit iBoxx IG）
4. 用前述指标算每类的 returns 和 excess returns

## 九、本章实操要点

1. **回测报告必须涵盖六大类指标**：通用 / Performance / Runs / 执行损耗 / Efficiency / 分类 / 归因。
2. **bet 数比 trade 数更能反映独立机会**——不要把它们混用。
3. **Max position ≈ Avg AUM** 才稳健；否则警惕 outlier 依赖。
4. **Correlation to underlying** 接近 1 → 策略其实只是 beta。
5. **HHI 指数**——把收益集中度和时间集中度都量化。
6. **报告 95-percentile DD/TuW** 而不只是历史最大值。
7. **效率指标用 PSR/DSR 而不是 raw SR**：raw SR 会因小样本、偏度、肥尾、多次试验而虚高。
8. **DSR 必须报告**——并伴随 $V[\widehat{\text{SR}}]$ 和 $N$。
9. **meta-labeling 评分用 F1**，注意退化情形。
10. **归因防重叠**：disjoint partition + 类别指数 + 加权重缩放。

## 十、关联阅读

- 上游：回测三大范式 → [[chapter-11-dangers-of-backtesting]]、[[chapter-12-backtesting-cv]]、[[chapter-13-synthetic-backtesting]]
- 平行：策略风险量化 → [[chapter-15-strategy-risk]]
- 平行：资产配置（与归因有关）→ [[chapter-16-asset-allocation]]
- 评分（F1 / log-loss）来源 → [[chapter-09-hyperparameter-tuning]]、[[chapter-03-labeling]]
- 参考：Bailey & López de Prado (2012) "Sharpe ratio efficient frontier"
- 参考：Bailey & López de Prado (2014) "The Deflated Sharpe Ratio"
- 参考：CFA Institute (2010) GIPS standards
