# 第 17 章：结构性突变（Structural Breaks）

> 结构性突变（如均值回归 → 动量切换）是高 R/R 机会的来源——大多数参与者措手不及。本章给出 **CUSUM 类检验** 与 **爆破式检验（SADF / SMT）** 两大族结构性突变检测方法，用于构造特征。

## 一、动机

ML 策略下注的最佳时机是**多种因素共同指向有利结果**。结构性突变（market regime 切换）正是这样的时机：
- 均值回归模式让位于动量模式（或反之）
- 错的一方会**先抵抗 → 加仓死扛 → 最终强制止损**
- 这种"非理性持仓"是很多盈利策略的猎物

本章的目标：构造能度量结构性突变可能性的特征。

## 二、两大类检验

| 类别 | 检测目标 | 子类 |
|---|---|---|
| **CUSUM 类** | 预测误差累计是否显著偏离白噪声 | Brown-Durbin-Evans / Chu-Stinchcombe-White |
| **Explosiveness 类** | 是否有指数级增长或崩溃（不可持续）| 右尾单位根检验 / Sub-/Super-Martingale 检验 |

后者可进一步分为"检测单个泡沫"和"检测多个泡沫"。**单泡沫检验在"泡沫-破裂-泡沫"循环中会被骗成"平稳"**。

## 三、CUSUM 类检验

### 3.1 Brown-Durbin-Evans（基于递归残差）

设 $y_t = \beta_t' x_t + \varepsilon_t$。在 $[1, k+1], [1, k+2], \ldots, [1, T]$ 上拟合 RLS，得 $T-k$ 个估计 $\hat\beta_t$。

标准化 1 步预测残差：
$$
\hat\omega_t = \frac{y_t - \hat\beta_{t-1}' x_t}{\sqrt{f_t}}, \quad f_t = \hat\sigma_\varepsilon^2 [1 + x_t'(X_t'X_t)^{-1} x_t]
$$

CUSUM 统计量：
$$
S_t = \sum_{j=k+1}^{t} \frac{\hat\omega_j}{\hat\sigma_\omega}, \quad \hat\sigma_\omega^2 = \frac{1}{T-k}\sum (\hat\omega_t - E[\hat\omega_t])^2
$$

$H_0: \beta_t = \beta$（常数） → $S_t \sim N[0, t-k-1]$。

**缺点**：起点选取武断，结果可能不一致。

### 3.2 Chu-Stinchcombe-White（基于水平值）

简化：去掉 $x_t$，假设 $H_0: \beta_t = 0$（即 $E_{t-1}[\Delta y_t] = 0$）。直接用 log-price $y_t$：

$$
S_{n,t} = \frac{y_t - y_n}{\hat\sigma_t \sqrt{t - n}}, \quad \hat\sigma_t^2 = \frac{1}{t-1}\sum_{i=2}^{t}(\Delta y_i)^2
$$

$H_0$ 下 $S_{n,t} \sim N[0, 1]$。单边临界值：
$$
c_\alpha[n, t] = \sqrt{b_\alpha + \log[t - n]}
$$
Monte Carlo 给出 $b_{0.05} = 4.6$。

**$y_n$ 选取武断**？解法：在回溯窗口 $n \in [1, t]$ 上算所有 $S_{n,t}$，取上确界 $S_t = \sup_n S_{n,t}$。

## 四、Explosiveness 类检验

### 4.1 Chow-Type Dickey-Fuller

考虑 AR(1)：$y_t = \rho y_{t-1} + \varepsilon_t$。$H_0: \rho = 1$（随机游走）。$H_1$：在 $\tau^* T$ 时切换为爆破：
$$
y_t = \begin{cases} y_{t-1} + \varepsilon_t & t \leq \tau^* T \\ \rho y_{t-1} + \varepsilon_t, \rho > 1 & t > \tau^* T \end{cases}
$$

拟合：$\Delta y_t = \delta y_{t-1} D_t[\tau^*] + \varepsilon_t$，$D_t[\tau^*]$ 是 dummy。
$$
\text{DFC}_{\tau^*} = \frac{\hat\delta}{\hat\sigma_\delta}, \quad H_0: \delta = 0, \quad H_1: \delta > 1
$$

**$\tau^*$ 未知**？Andrews (1993) 提出试所有 $\tau^* \in [\tau_0, 1 - \tau_0]$：
$$
\text{SDFC} = \sup_{\tau^*} \{\text{DFC}_{\tau^*}\}
$$

**仍有缺陷**：只考虑一个切换点，且假设泡沫一直持续到样本末尾。

### 4.2 Supremum Augmented Dickey-Fuller (SADF) ★

Phillips, Wu & Yu (2011) 指出："标准单位根/协整检验不能区分平稳过程与周期性破裂的泡沫——泡沫数据看起来更像单位根或平稳 AR 过程。"

回归式：
$$
\Delta y_t = \alpha + \beta y_{t-1} + \sum_{l=1}^{L} \gamma_l \Delta y_{t-l} + \varepsilon_t
$$
检验 $H_0: \beta \leq 0$，$H_1: \beta > 0$。

**SADF 在每个时点 $t$**，**向后扩张起点 $t_0$**：
$$
\text{SADF}_t = \sup_{t_0 \in [1, t-\tau]} \{\text{ADF}_{t_0, t}\} = \sup_{t_0 \in [1, t-\tau]} \left\{\frac{\hat\beta_{t_0, t}}{\hat\sigma_{\beta_{t_0, t}}}\right\}
$$

与 SDFC 的关键差异：
- SADF 在**每个 $t$** 都算；SDFC 只在 $T$ 算一次
- SADF 递归扩张起点（不引入 dummy）
- **不假设切换次数和断点位置**

SADF 在 E-mini S&P 500 上能清楚识别泡沫期的尖峰。

### 4.3 Raw vs Log Prices（关键设计选择）

对**原始价**做 ADF：$H_0$ 拒绝意味着价格平稳，方差有限 → **回报率的方差必须随价格水平变化**（结构性异方差）。

对 **log price** 做 ADF：
$$
\Delta \log[y_t] \propto \log[y_{t-1}]
$$
变量替换 $x_t = k y_t$：$\Delta \log[x_t] \propto \log[y_{t-1}]$ —— **价格水平只调制回报均值，不调制回报波动率**。

对于跨越数十年、泡沫导致不同 regime 间 $k$ 差异巨大的样本，**必用 log price**。

### 4.4 计算复杂度（HPC 警告）

SADF 是 $\mathcal{O}(n^2)$。对 $T = 356,631$、$N=3$，一次 SADF 更新需 **2.035 TFLOPs**，整条 SADF 序列 **242 PFLOPs**。**必须并行 + HPC 集群**（第 20 章）。

每次 ADF 估计的 FLOPs：
| 操作 | FLOPs |
|---|---|
| $X'y$ | $(2T-1)N$ |
| $X'X$ | $(2T-1)N^2$ |
| 求逆 $(X'X)^{-1}$ | $N^3 + N^2 + N$ |
| $(X'X)^{-1}(X'y)$ | $2N^2 - N$ |
| 残差 | $T + (2N-1)T$ |
| 残差平方和 | $2T - 1$ |
| Sigma | $2 + N^2$ |
| beta/std | $1$ |

总：$f(N, T) = N^3 + N^2(2T+3) + N(4T-1) + 2T + 2$

### 4.5 指数行为的三种 regime

$\Delta \log[y_t] = \alpha + \beta \log[y_{t-1}] + \varepsilon_t$ 等价于 $\log[\tilde y_t] = (1+\beta)\log[\tilde y_{t-1}] + \varepsilon_t$，其中 $\log[\tilde y_t] = \log[y_t] + \alpha/\beta$。

- **Steady**：$\beta < 0$ → $\lim E[\log[y_t]] = -\alpha/\beta$。半生命期 $t = -\log[2]/\log[1+\beta]$。
- **Unit-root**：$\beta = 0$，非平稳鞅。
- **Explosive**：$\beta > 0$ → $\lim E[\log[y_t]] = \pm\infty$（取决于 $\log[y_0]$ 与 $\alpha/\beta$ 的大小关系）。

### 4.6 SADF 的 robustness 改进

#### Quantile ADF (QADF)
SADF 用上确界，对采样频率和具体时点敏感。改用分位数：
$$
Q_{t,q} = Q[s_t, q], \quad s_t = \{\text{ADF}_{t_0, t}\}
$$
散度：$\dot Q_{t,q,v} = Q_{t,q+v} - Q_{t,q-v}$。如 $q=0.95, v=0.025$。SADF 是 QADF 的特例（$q=1$）。

#### Conditional ADF (CADF)
$$
C_{t,q} = K^{-1} \int_{Q_{t,q}}^\infty x f[x] dx
$$
$$
\dot C_{t,q} = \sqrt{K^{-1} \int_{Q_{t,q}}^\infty (x - C_{t,q})^2 f[x] dx}
$$
其中 $K = \int_{Q_{t,q}}^\infty f[x] dx$。

**$(SADF_t - C_{t,q}) / \dot C_{t,q}$** 可以达到很大值——揭示 SADF 被 outlier 推高的程度。

### 4.7 SADF 实现

```python
def get_bsadf(logP, minSL, constant, lags):
    y, x = getYX(logP, constant=constant, lags=lags)
    startPoints = range(0, y.shape[0] + lags - minSL + 1)
    bsadf, allADF = None, []
    for start in startPoints:
        y_, x_ = y[start:], x[start:]
        bMean_, bStd_ = getBetas(y_, x_)
        bMean_, bStd_ = bMean_[0, 0], bStd_[0, 0]**.5
        allADF.append(bMean_ / bStd_)
        if allADF[-1] > bsadf: bsadf = allADF[-1]
    return {'Time': logP.index[-1], 'gsadf': bsadf}

def getBetas(y, x):
    xy = np.dot(x.T, y)
    xx = np.dot(x.T, x)
    xxinv = np.linalg.inv(xx)
    bMean = np.dot(xxinv, xy)
    err = y - np.dot(x, bMean)
    bVar = np.dot(err.T, err) / (x.shape[0] - x.shape[1]) * xxinv
    return bMean, bVar
```

`constant` 参数：`'nc'` 无趋势 / `'ct'` 加线性趋势 / `'ctt'` 加二阶趋势。

## 五、Sub-/Super-Martingale Tests（不依赖 ADF 框架）

不要求 ADF specification，直接检测爆破式趋势。$H_0: \beta = 0$，$H_1: \beta \neq 0$。

四种 specification：
- **SM-Poly1**：$y_t = \alpha + \gamma t + \beta t^2 + \varepsilon_t$
- **SM-Poly2**：$\log[y_t] = \alpha + \gamma t + \beta t^2 + \varepsilon_t$
- **SM-Exp**：$y_t = \alpha e^{\beta t} + \varepsilon_t$ → $\log[y_t] = \log[\alpha] + \beta t + \xi_t$
- **SM-Power**：$y_t = \alpha t^\beta + \varepsilon_t$ → $\log[y_t] = \log[\alpha] + \beta \log[t] + \xi_t$

类似 SADF，每个端点 $t$ 上回溯起点 $t_0$，取 $|\hat\beta|$ 的 t-stat 上确界。**用绝对值**——爆破式增长和崩溃同等关心。

### 5.1 时长偏置与修正

$\sigma_{\hat\beta_{t_0, t}}$ 随样本长度变小（标准结果）→ 长期弱泡沫的 $\sigma_\beta$ 比短期强泡沫的还小 → SMT 偏袒长期泡沫。

修正：用惩罚项 $(t - t_0)^\varphi$，$\varphi \in [0, 1]$：
$$
\text{SMT}_t = \sup_{t_0} \left\{\frac{|\hat\beta_{t_0, t}|}{\hat\sigma_{\beta_{t_0, t}} (t - t_0)^\varphi}\right\}
$$

- $\varphi = 0.5$：在简单回归下补偿长样本的低 $\sigma$
- $\varphi \to 0$：偏向长期趋势，长期泡沫掩盖短期
- $\varphi \to 1$：噪声更大，短期泡沫被偏袒

**特征工程小贴士**：可以同时用多个 $\varphi$ 的 SMT 作为特征，让 ML 选合适的"目标持仓周期"敏感度。

## 六、本章实操要点

1. **结构性突变检测 = 高 R/R 信号来源**——值得作为特征工程的优先方向。
2. **两类检验互补**：
   - CUSUM（Brown-Durbin-Evans / Chu-Stinchcombe-White）：检测预测误差累计偏离
   - Explosiveness（SADF / SMT）：检测指数级增长/崩溃
3. **永远用 log price** 跑 SADF——避免结构性异方差。
4. **SADF 计算量大**，必须并行 + 注意 $\mathcal{O}(n^2)$。
5. **SADF 上确界对 outlier 敏感** → 用 QADF（分位数）或 CADF（条件矩）做 robustness check。
6. **SMT 修正长度偏置**用 $(t-t_0)^\varphi$ 惩罚项，$\varphi$ 选 0.5 是个安全的起点。
7. **作为特征**：把 SADF / QADF / CADF / SMT 在多 $\varphi$ 下的值都喂给 ML，让特征重要性筛。
8. **breakdown 也是机会** —— "崩溃"和"上涨泡沫"对称，SMT 用 $|\hat\beta|$ 自然覆盖；SADF 想检测 breakdown 要做 $y_t \to 1/y_t$ 变换或单独建模。

## 七、关联阅读

- 上游：CUSUM 在事件采样中的应用 → [[chapter-02-financial-data-structures]]
- 平行：熵特征（另一类信息论度量）→ [[chapter-18-entropy-features]]
- 平行：市场微观结构特征 → [[chapter-19-microstructural-features]]
- 用于事件采样的 SADF → [[chapter-02-financial-data-structures]]（Sampling Features 节）
- 长记忆与平稳性的平衡 → [[chapter-05-fractional-differentiation]]
- HPC：SADF 计算优化 → [[chapter-20-multiprocessing]]
- 参考：Phillips, Wu & Yu (2011) "Explosive behavior in the 1990s Nasdaq"
- 参考：Homm & Breitung (2012) 比较各种泡沫检测方法
