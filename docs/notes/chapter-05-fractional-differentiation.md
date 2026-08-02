# 第 5 章：分数阶差分特征（Fractionally Differentiated Features）

> 解决金融时序里最根本的矛盾：**价格有记忆但非平稳，收益率平稳但失忆**。作者提出 **Fixed-Width Window Fracdiff (FFD)** —— 在 ML 平稳化要求与保留预测信息之间找到最优折衷。

## 一、问题：平稳性 vs 记忆性的两难

金融时序通常非平稳——根源是它"有记忆"（每个值依赖于很长的历史水平）。统计推断需要不变性，所以业界习惯做整数阶差分（如 $\Delta \log p_t = r_t$）得到平稳的收益率。

**但这是个非常激进的代价**：收益率把所有记忆都擦掉了。

- 价格序列：有记忆，**非平稳**
- 收益率（一阶差分）：平稳，**无记忆**

**关键问题**：使价格序列平稳所需的**最小**差分量是多少？换句话说，能不能在保留记忆的前提下让序列平稳？

作者的论点更尖锐：**为什么整数阶 1 是"最优"的？整数 0 和整数 1 一样武断**。这两个极端之间有一整片连续区间，叫**分数阶差分**。

ML 需要平稳特征（否则无法把新观测映射到训练样本）；但平稳是必要非充分条件——过度差分会摧毁预测力。

## 二、文献回顾的尴尬

- 金融时序文献几乎全是整数阶差分（Hamilton 1994 是代表）。
- 分数阶差分在时序分析里出现至少可追溯到 Hosking (1981)——他把 ARIMA 推广到分数阶差分。
- Hosking 自己说："除了 Granger (1978) 顺带一提外，分数阶差分在时序分析里几乎没被讨论过。"
- 此后 30 多年只有 8 篇期刊论文，作者总共 9 人。

作者大胆推测：**过度差分是文献长期偏袒有效市场假说的原因之一**。

## 三、数学：分数阶差分算子

后移算子 $B$：$B^k X_t = X_{t-k}$。

整数情形：$(1 - B)^d X_t$ 用二项式定理展开就是经典差分。比如 $(1-B)^2 X_t = X_t - 2X_{t-1} + X_{t-2}$。

把 $d$ 推广到实数：
$$
(1 - B)^d = \sum_{k=0}^{\infty} \binom{d}{k} (-B)^k = \sum_{k=0}^{\infty} (-B)^k \prod_{i=0}^{k-1} \frac{d-i}{k-i}
$$
$$
= 1 - dB + \frac{d(d-1)}{2!}B^2 - \frac{d(d-1)(d-2)}{3!}B^3 + \cdots
$$

### 3.1 长记忆与权重序列

$$
\tilde X_t = \sum_{k=0}^{\infty} \omega_k X_{t-k}
$$
权重：
$$
\omega = \left\{1, -d, \frac{d(d-1)}{2!}, -\frac{d(d-1)(d-2)}{3!}, \ldots, (-1)^k \prod_{i=0}^{k-1}\frac{d-i}{k!}, \ldots\right\}
$$

**当 $d$ 是正整数时**，从 $k > d$ 起所有权重为 0 —— 记忆被截断。整数 $d=1$ 时 $\omega = \{1, -1, 0, 0, \ldots\}$（就是收益率）。

**当 $d$ 是分数时**，权重序列无限延伸但快速衰减——记忆被"软"保留。

### 3.2 迭代生成权重

$$
\omega_k = -\omega_{k-1} \frac{d-k+1}{k}, \quad \omega_0 = 1
$$

```python
def getWeights(d, size):
    w = [1.]
    for k in range(1, size):
        w_ = -w[-1] / k * (d - k + 1)
        w.append(w_)
    return np.array(w[::-1]).reshape(-1, 1)
```

### 3.3 收敛性

对 $k > d$，$|\omega_k / \omega_{k-1}| = |(d-k+1)/k| < 1$，权重以单位圆内的因子无限相乘，**渐进收敛到 0**。

特殊情况 $d \in (0, 1)$：所有 $k > 0$ 的权重满足 $-1 < \omega_k < 0$。这种符号交替使得长期记忆"互相抵消"，保证 $\{\tilde X_t\}$ 的平稳性。

## 四、实现：两种窗口策略

### 4.1 Expanding Window（标准做法，有缺陷）

$T$ 个观测值，权重 $\{\omega_k\}$ 实际上要被截断。最后一个点 $\tilde X_T$ 用 $T$ 个权重，更早的点 $\tilde X_{T-l}$ 只用 $T-l$ 个权重。

定义**相对权重损失**：
$$
\lambda_l = \frac{\sum_{j=T-l}^{T-1} |\omega_j|}{\sum_{i=0}^{T-1} |\omega_i|}
$$

给定容忍度 $\tau \in [0, 1]$，找到 $l^*$ 使 $\lambda_{l^*} \leq \tau$ 而 $\lambda_{l^*+1} > \tau$。前 $l^*$ 个值被丢弃。

```python
def fracDiff(series, d, thres=.01):
    w = getWeights(d, series.shape[0])
    w_ = np.cumsum(abs(w))
    w_ /= w_[-1]
    skip = w_[w_ > thres].shape[0]
    df = {}
    for name in series.columns:
        seriesF = series[[name]].fillna(method='ffill').dropna()
        df_ = pd.Series()
        for iloc in range(skip, seriesF.shape[0]):
            loc = seriesF.index[iloc]
            if not np.isfinite(series.loc[loc, name]): continue
            df_[loc] = np.dot(w[-(iloc+1):, :].T, seriesF.loc[:loc])[0, 0]
        df[name] = df_.copy(deep=True)
    return pd.concat(df, axis=1)
```

**核心缺陷**：因为窗口在扩张，**每个时间点用的权重数量不同**，会引入**负漂移**——早期负权重不断累积。

### 4.2 Fixed-Width Window Fracdiff (FFD) ★ 作者推荐

把权重在 $|\omega_k| < \tau$ 处截断，得到固定长度的权重序列：

$$
\tilde \omega_k = \begin{cases} \omega_k & k \leq l^* \\ 0 & k > l^* \end{cases}
$$
$$
\tilde X_t = \sum_{k=0}^{l^*} \tilde \omega_k X_{t-k}, \quad t = T - l^* + 1, \ldots, T
$$

**所有时间点用同一组权重** → 没有负漂移问题。

```python
def fracDiff_FFD(series, d, thres=1e-5):
    w = getWeights_FFD(d, thres)
    width = len(w) - 1
    df = {}
    for name in series.columns:
        seriesF = series[[name]].fillna(method='ffill').dropna()
        df_ = pd.Series()
        for iloc1 in range(width, seriesF.shape[0]):
            loc0, loc1 = seriesF.index[iloc1-width], seriesF.index[iloc1]
            if not np.isfinite(series.loc[loc1, name]): continue
            df_[loc1] = np.dot(w.T, seriesF.loc[loc0:loc1])[0, 0]
        df[name] = df_.copy(deep=True)
    return pd.concat(df, axis=1)
```

FFD 输出是**水平 + 噪声的无漂移混合**。分布不再高斯（记忆带来偏度和超额峰度），但**平稳**。

## 五、最大记忆保留的平稳化（核心结果）

对每个序列：
- 应用 FFD(d)
- 找出最小的 $d^*$，使序列通过 ADF 平稳性检验

$d^*$ 量化"为达到平稳必须移除的记忆量"：
- $d^* = 0$ → 原序列已平稳
- $d^* < 1$ → 含单位根
- $d^* > 1$ → 爆破式行为（如泡沫）
- $0 < d^* \ll 1$ → 原序列"轻度非平稳"，整数差分会过度抹去信息

### 5.1 E-mini S&P 500 实证（核心数字）
- 原始 log-price：ADF = -0.3387（远未平稳）
- 整数 1 阶差分（收益率）：ADF = -46.9114（远超 95% 临界值 -2.8623）
- **FFD 在 $d \approx 0.35$ 时 ADF 越过临界值，达到平稳**
- 此时与原序列的相关系数 = **0.995**（几乎完全保留信息）
- 收益率与原序列相关系数 = **0.03**（信息几乎全没了）

### 5.2 跨品种验证（Table 5.1）

87 个全球最流动期货合约：
- **全部都在 $d < 0.6$ 时达到平稳**
- 绝大多数在 $d < 0.3$ 时就平稳
- 部分品种（橙汁 JO1、活牛 LC1）**完全不需要差分**

**结论**：业界标准 $d = 1$ **全部都过度差分**，无意义地摧毁了记忆。

## 六、与 Box-Jenkins / Engle-Granger 的对比

经典计量两大范式：
1. **Box-Jenkins**：收益率平稳但失忆。
2. **Engle-Granger**：log-price 有记忆但非平稳，靠**协整**做技巧让回归成立。协整变量数量有限，**协整向量也声名狼藉地不稳定**。

**FFD 的优势**：
- 不需要放弃所有记忆来换平稳
- 不需要协整这种"trick"
- ML 预测任务里更直接、更通用

## 七、实操方法论

作者给的特征工程模板：
1. 计算时间序列的**累积和**——保证至少需要某种程度的差分。
2. 对 $d \in [0, 1]$ 各值计算 FFD(d) 序列。
3. 找出最小 $d$ 使 ADF 检验 p-value < 5%。
4. **把这个 FFD(d) 序列作为预测特征**。

## 八、本章实操要点

1. **不要默认用收益率做特征**——它已经把记忆全擦了。
2. **用 FFD，找最小 $d^*$**——通常 0.2 ~ 0.5 就够了。
3. **FFD 优于 Expanding Window**：避免负漂移，所有时间点同权重。
4. **阈值参数**：FFD 用 $\tau = 10^{-5}$ 这样的小值（决定窗口长度）；Expanding window 用 $\tau = 0.01$ 这样的大值（决定丢弃多少前期点）。
5. **保留记忆但分布非高斯**：偏度、峰度会高，准备好用对偏离正态稳健的模型（树模型 / 等）。
6. **平稳化后不一定有预测力**——平稳只是必要条件。下一步靠特征重要性分析。

## 九、关联阅读

- 上游：把价格转成 bars → [[chapter-02-financial-data-structures]]（dollar bars、ETF trick）
- 下游：用 FFD 特征训模型 → [[chapter-06-ensemble-methods]]、[[chapter-08-feature-importance]]
- 平稳性突变检测 → [[chapter-17-structural-breaks]]
- ADF 检验的扩展版本（SADF） → [[chapter-17-structural-breaks]]
- Hosking (1981) "Fractional differencing", Biometrika
- López de Prado (2015) "The Future of Empirical Finance", JPM
