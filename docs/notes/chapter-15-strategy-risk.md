# 第 15 章：理解策略风险（Understanding Strategy Risk）

> **策略风险 ≠ 组合风险**。组合风险关心"持仓波动"，策略风险关心"策略本身失败的概率"。本章用二项过程把"精度 / 频率 / 盈亏比"打通，给出策略失败概率 $P[p < p_{\theta^*}]$ 的计算方法。

## 一、动机

策略实质上都是"开仓 → 等待止盈或止损"。即便不显式设止损，**总有隐式止损**（保证金通知 / 投资者承受不住的浮亏）。所以可以用二项过程建模整个策略的盈亏分布。

目标：评估策略对精度、频率、盈亏比的小幅变化有多脆弱。

## 二、对称收益（Symmetric Payouts）

每年 $n$ 次 IID 下注，每次结果 $X_i \in \{\pi, -\pi\}$，$P[X_i = \pi] = p$。

把 $p$ 想成二分类器的**precision**：positive = 下注，true positive 得 $\pi$，false positive 失 $\pi$；negative 不计入。

- 单注期望：$E[X_i] = \pi(2p - 1)$
- 单注方差：$V[X_i] = 4\pi^2 p(1 - p)$
- 年化 SR：
$$
\theta[p, n] = \frac{nE[X_i]}{\sqrt{nV[X_i]}} = \underbrace{\frac{2p - 1}{2\sqrt{p(1-p)}}}_{\text{t-value of } p \text{ under } H_0: p=1/2} \cdot \sqrt{n}
$$

**$\pi$ 在公式里消掉了**（因为对称）。这就是**高频交易的经济基础**——$p$ 只需略高于 0.5，只要 $n$ 足够大，SR 就能很高。SR 是 precision 的函数，不是 accuracy 的函数。

### 2.1 例子
$p = 0.55$ → $\frac{2p-1}{2\sqrt{p(1-p)}} = 0.1005$ → 年化 SR = 2 需要 **396 次下注/年**。

### 2.2 反解 p

$$
p = \frac{1}{2}\left(1 + \sqrt{1 - \frac{n}{\theta^2 + n}}\right)
$$

- 周频策略（$n = 52$）想要 SR = 2 → 需要 $p = 0.6336$。

## 三、非对称收益（Asymmetric Payouts）

$X_i \in \{\pi_+, \pi_-\}$，$\pi_- < \pi_+$（注意 $\pi_- < 0$）。

- 期望：$E[X_i] = (\pi_+ - \pi_-)p + \pi_-$
- 方差：$V[X_i] = (\pi_+ - \pi_-)^2 p(1-p)$
- SR：
$$
\theta[p, n, \pi_-, \pi_+] = \frac{(\pi_+ - \pi_-)p + \pi_-}{(\pi_+ - \pi_-)\sqrt{p(1-p)}} \sqrt{n}
$$

验证：$\pi_- = -\pi_+$ 时退化为对称情形。

### 3.1 例子（极敏感）
$n=260, \pi_-=-0.01, \pi_+=0.005, p=0.7$ → $\theta = 1.173$。
$n=260, \pi_-=-0.01, \pi_+=0.005, p=0.72$ → $\theta = 2.0$。

**仅 2 个百分点的 $p$ 变化把 SR 从 1.17 拉到 2.0** —— 也说明这个策略**极度脆弱**。

### 3.2 反解 p

给定 $\{\pi_-, \pi_+, n\}$ 与目标 SR $\theta^*$，求最小 $p$：
$$
p = \frac{-b + \sqrt{b^2 - 4ac}}{2a}
$$
其中 $a = (n + \theta^2)(\pi_+ - \pi_-)^2$，$b = [2n\pi_- - \theta^2(\pi_+ - \pi_-)](\pi_+ - \pi_-)$，$c = n\pi_-^2$。

```python
def binHR(sl, pt, freq, tSR):
    a = (freq + tSR**2) * (pt - sl)**2
    b = (2 * freq * sl - tSR**2 * (pt - sl)) * (pt - sl)
    c = freq * sl**2
    p = (-b + (b**2 - 4*a*c)**.5) / (2. * a)
    return p
```

### 3.3 反解 n
给定 $p, \pi_-, \pi_+, \theta^*$ 求所需频率：
$$
n = \frac{\theta^{*2}(\pi_+ - \pi_-)^2 p(1-p)}{((\pi_+ - \pi_-)p + \pi_-)^2}
$$

```python
def binFreq(sl, pt, p, tSR):
    freq = (tSR*(pt-sl))**2 * p*(1-p) / ((pt-sl)*p + sl)**2
    if not np.isclose(binSR(sl, pt, freq, p), tSR): return  # 检查 extraneous root
    return freq
```

## 四、策略失败概率

### 4.1 哪些参数 PM 控制？

- **PM 控制**：$\pi_+$（止盈）、$\pi_-$（止损）、$n$（机会频率）
- **PM 不控制**：$p$（市场决定）、$\theta^*$（投资者期望）

$p$ 是随机变量，期望 $E[p]$。定义 **$p_{\theta^*}$** 为使策略 SR 等于目标 $\theta^*$ 的临界精度：
$$
p_{\theta^*} = \max\{p \mid \theta \leq \theta^*\}
$$

- 例子续：$p_{\theta^*=0} = 2/3$ → $p < 2/3$ 时 SR ≤ 0。原本 $p=0.7$ 的策略，$p$ 只要降到 0.67 就全军覆没。

### 4.2 关键概念区分

> **策略风险 ≠ 组合风险**。
> - 组合风险：持仓波动（CRO 关心）。
> - 策略风险：策略长期失败的概率（CIO 关心）。

策略风险 = $P[p < p_{\theta^*}]$。

### 4.3 算法

给定 bet 时序 $\{\pi_t\}_{t=1,\ldots,T}$：

1. 估 $\pi_- = E[\pi_t | \pi_t \leq 0]$，$\pi_+ = E[\pi_t | \pi_t > 0]$。可选：用 EF3M 算法拟合两高斯混合（López de Prado & Foreman 2014）。
2. 年化频率 $n = T/y$。
3. 用 bootstrap 推 $p$ 的分布：
   - 第 $i$ 次迭代：有放回抽 $\lfloor nk \rfloor$ 个样本（$k$ = 投资者评估的年数，如 2）
   - $p_i = \frac{1}{\lfloor nk \rfloor} \|\{\pi_j^{(i)} > 0\}\|$
4. 对 $\{p_i\}_{i=1,\ldots,I}$ 做 KDE 得 $f[p]$。大 $k$ 时可近似 $f[p] \sim N[\bar p, \bar p(1-\bar p)]$。
5. 给定 $\theta^*$ 反推 $p_{\theta^*}$。
6. **策略风险** = $\int_{-\infty}^{p_{\theta^*}} f[p] dp$。

### 4.4 实现

```python
def mixGaussians(mu1, mu2, sigma1, sigma2, prob1, nObs):
    ret1 = np.random.normal(mu1, sigma1, size=int(nObs*prob1))
    ret2 = np.random.normal(mu2, sigma2, size=int(nObs)-ret1.shape[0])
    ret = np.append(ret1, ret2, axis=0)
    np.random.shuffle(ret)
    return ret

def probFailure(ret, freq, tSR):
    rPos, rNeg = ret[ret > 0].mean(), ret[ret <= 0].mean()
    p = ret[ret > 0].shape[0] / float(ret.shape[0])
    thresP = binHR(rNeg, rPos, freq, tSR)
    risk = ss.norm.cdf(thresP, p, p*(1-p))  # 近似 bootstrap
    return risk

def main():
    mu1, mu2, sigma1, sigma2, prob1, nObs = .05, -.1, .05, .1, .75, 2600
    tSR, freq = 2., 260
    ret = mixGaussians(mu1, mu2, sigma1, sigma2, prob1, nObs)
    print 'Prob strategy will fail', probFailure(ret, freq, tSR)
```

**决策规则**：$P[p < p_{\theta^*}] > 0.05$ → 即使持仓的波动很低也应**视为太冒险**而弃用。原因：哪怕亏不多，达不到目标的概率太高。

### 4.5 与 PSR 的关系

| 工具 | 关注 | 差别 |
|---|---|---|
| **PSR**（第 14 章） | 真 SR 超过阈值的概率（含非高斯校正） | 不区分 PM 可控 vs 不可控参数 |
| **本章** $P[p<p_{\theta^*}]$ | 策略失败概率（基于二项过程） | **允许 PM 在 $\{\pi_-, \pi_+, n\}$ 上调整以改善生存概率** |

两者互补。

## 五、本章实操要点

1. **策略风险 ≠ 组合风险**——大多数公司只算组合风险，错过了策略本身的脆弱性。
2. **对称收益**：SR = $\frac{2p-1}{2\sqrt{p(1-p)}}\sqrt{n}$ —— 高频 ($n$ 大) 是低胜率小盈利的生存之道。
3. **非对称收益**：$p$ 的小变化对 SR 影响巨大——尤其是 $\pi_+ < |\pi_-|$ 时。
4. **用 binHR 工具反推所需 $p$**：明知 $p$ 上限，可调整 $\{\pi_-, \pi_+, n\}$ 让策略可行。
5. **用 probFailure 量化策略风险**：> 5% 即弃。
6. **诊断 OTR**：第 13 章的热图非对称形状，本质上反映了不同 $\{p, \pi_-, \pi_+\}$ 组合下的可行域。
7. **关键参数排序**：在精度难以提升时，可以从频率、止盈、止损三个方向找"最低悬挂的果子"。

## 六、关联阅读

- 上游：bet sizing 与精度关系 → [[chapter-10-bet-sizing]]
- 上游：triple-barrier 的 $\pi_+, \pi_-$ 直接对应这里的参数 → [[chapter-03-labeling]]
- 平行：OTR 给出 $(\pi_+, \pi_-)$ 的最优搜索方法 → [[chapter-13-synthetic-backtesting]]
- 平行：PSR / DSR → [[chapter-14-backtest-statistics]]
- 参考：López de Prado & Foreman (2014) EF3M
- 参考：López de Prado & Peijan (2004) "Measuring loss potential of hedge fund strategies"
