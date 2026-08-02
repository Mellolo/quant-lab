# 第 12 章：通过交叉验证回测（Backtesting through Cross-Validation）

> 走出"历史模拟 = 回测"的思维定势。本章介绍三种回测范式：**Walk-Forward / Cross-Validation / Combinatorial Purged CV (CPCV)**。CPCV 是作者推荐的范式，能生成多条回测路径以解决过拟合。

## 一、动机：回测的两种解读

观测可以这样用：
1. **狭义**——历史模拟（"如果策略当年运行会怎样"）= Walk-Forward
2. **广义**——模拟过去没发生的场景 = CV / CPCV

绝大多数人只用第一种，以致于"回测"成了"历史模拟"的同义词。第二种被严重忽视。

## 二、Walk-Forward 方法（WF）

最常用的回测方法。每个决策基于在该决策之前的观测数据。完美的 WF 回测需要熟知数据源、市场微观结构、风险管理、业绩衡量标准（GIPS）、多重检验方法、实验数学等——**没有通用模板**，每个回测都要定制。

### 2.1 WF 的优势
1. **历史解释清晰**：业绩可以与 paper trading 对应。
2. **滤波（filtration）保证不泄漏**：只要正确 purging（第 7 章），训练永远在测试之前。
3. **不需要 embargo**：训练永远早于测试。

### 2.2 WF 的三大缺陷
1. **只测试单一场景（历史路径）**——很容易过拟合（Bailey et al. 2014）。
2. **历史不一定代表未来**——结果会被数据点的特定顺序偏置。  
   - WF 支持者常说"反向预测会过度乐观"。但事实上，**反向回测一样容易过拟合**——你能 fit 出 WF 的好策略，也能 fit 出 walk-backward 的好策略。
   - 反例：S&P 500 从 2007-01-01 起的 WF。到 2009-03-15 前，模型学到"市场中性、低置信度"。之后长牛主导，到 2017 年模型几乎只发出买信号。**反向播放（2017→2007）**：先是长牛后是急跌——模型会学出截然不同的策略。利用一条特定路径选出来的策略可能给未来挖坑。
3. **初期决策建立在小样本上**。设 warm-up 期为 $t_0$，总样本 $T$，则一半的决策平均只用了
   $$\frac{3}{4} \cdot \frac{t_0}{T} + \frac{1}{4}$$
   比例的数据。warm-up 越长，回测越短，trade-off 很难取舍。

## 三、Cross-Validation 方法（CV）

把数据集分两段：测试段（如 2008 危机）和训练段（其他）。**用 2008 后才有的数据训练一个分类器，然后在 2008 上测试**——这不是历史准确的，但回答的不是"历史会怎样"，而是**"如果策略不知道 2008，遇到 2008 会怎样"**。

CV 回测的目的：**不是复现历史业绩，而是从多个 OOS 场景推断未来业绩**。

### 3.1 优势
1. 测试不依赖单一（历史）场景——CV 测试 $k$ 个不同场景，其中只有一个对应历史顺序。
2. 每个决策基于相同样本量——结果在期间间可比。
3. 每个观测属于且仅属于一个测试集 → **无 warm-up、最长 OOS 模拟**。

### 3.2 缺陷
1. 仍然只有一条回测路径（不是历史的那条）——每个观测只产生一个预测。
2. 没有清晰的历史解释。
3. 训练集不在测试集之前，必须靠 purge + embargo 防泄漏（第 7 章）。

## 四、Combinatorial Purged Cross-Validation（CPCV）★ 作者主推

### 4.1 核心思想

WF 和 CV 都只有**一条路径**。CPCV 通过组合训练/测试集，**生成多条回测路径**。

### 4.2 组合数学

把 $T$ 观测**不打乱**地分 $N$ 组，前 $N-1$ 组各 $\lfloor T/N \rfloor$，最后一组 $T - \lfloor T/N \rfloor (N-1)$。

测试集大小 $k$ 组，则训练/测试拆分数为：
$$
\binom{N}{N-k} = \frac{\prod_{i=0}^{k-1}(N-i)}{k!}
$$

被测试的组次共 $k \binom{N}{N-k}$。这些被测试的组**均匀分布在 $N$ 个组上**——每个组属于相同数量的训练 / 测试集。

每个组属于的测试集数（也是可生成的路径数）：
$$
\varphi[N, k] = \frac{k}{N} \binom{N}{N-k} = \frac{\prod_{i=1}^{k-1}(N-i)}{(k-1)!}
$$

### 4.3 例子：$N=6, k=2$

共 $\binom{6}{4} = 15$ 个拆分（S1–S15）。每组属于 5 个测试集 → 可生成 **5 条回测路径**。

每个 (Group, Split) 对应一个被测试组次。把它们重新组合为 5 条路径：
- Path 1 = (G1,S1) + (G2,S1) + (G3,S2) + (G4,S3) + (G5,S4) + (G6,S5)
- Path 2 = (G1,S2) + (G2,S6) + (G3,S6) + (G4,S7) + (G5,S8) + (G6,S9)
- ...

每条路径覆盖整个 $T$，相当于完整的回测；5 条路径相当于 5 个独立的回测场景。

### 4.4 训练集比例与路径数

- 训练集占比 $\theta = 1 - k/N$（每个组合）。理论上 $\theta < 1/2$ 可行，但实务上假设 $k \leq N/2$。
- 路径数 $\varphi[N, k]$ 随 $N \to T$ 增、随 $k \to N/2$ 增。
- 极限：$N=T, k=T/2$ → 最大路径数，代价是每个组合只用一半数据训练。

### 4.5 CPCV 算法

1. 把 $T$ 观测不打乱地分 $N$ 组。
2. 计算所有 $\binom{N}{N-k}$ 种训练/测试拆分。
3. 对每对 $(y_i, y_j)$，若 $y_i$ 训练、$y_j$ 测试，且 $y_i$ 的区间与 $y_j$ 重叠 → 用 PurgedKFold 类 purge $y_i$。同时对测试样本之前的训练样本 embargo。
4. 在 $\binom{N}{N-k}$ 个训练集上训分类器，在对应测试集上预测。
5. 重组成 $\varphi[N, k]$ 条回测路径。每条算一个 Sharpe Ratio → **得到 SR 的经验分布**，不是单个值。

### 4.6 常用配置

- $k=1$ → $\varphi = 1$，退化为 CV。
- $k=2$ → $\varphi = N-1$，**最实用**。设 $N = \varphi + 1$，训练用 $\theta = 1 - 2/N$ 的数据。
- 极限：$N=T, k=2 \to T-1$ 条路径，每条用 $1 - 2/T$ 的数据训练。
- $k>2$ 仅在需要更多路径时使用。

## 五、CPCV 如何对抗回测过拟合

### 5.1 理论：最大 SR 的期望

IID 标准正态样本 $x_i \sim Z, i = 1, \ldots, I$，最大值的期望（Bailey et al. 2014）：

$$
E[\max\{x_i\}] \approx (1 - \gamma) Z^{-1}\left[1 - \frac{1}{I}\right] + \gamma Z^{-1}\left[1 - \frac{1}{I}e^{-1}\right] \leq \sqrt{2\log I}
$$

其中 $\gamma \approx 0.5772$ 是 Euler-Mascheroni 常数。**$I$ 个真 SR=0 的策略中，挑最大那个就能"发现"显著 SR**——这就是多重检验下的假阳性。

### 5.2 WF 的高方差问题
WF 的 SR 估计方差很大：少数早期观测对最终 SR 影响过大；warm-up 加长会缩短回测、进一步推高方差。研究员选 SR 最大的回测 → 假发现。

### 5.3 CPCV 的方差降低

CPCV 给每个策略产生 $\varphi$ 个路径，每条路径一个 SR。样本均值的方差：
$$
\sigma^2[\mu_i] = \varphi^{-1} \sigma_i^2 (1 + (\varphi - 1) \bar\rho_i)
$$

其中 $\bar\rho_i$ 是路径间 SR 的平均相关。只要 $\bar\rho_i < 1$：
$$
\varphi^{-1} \sigma_i^2 \leq \sigma^2[\mu_i] < \sigma_i^2
$$

路径越不相关，样本均值方差越小。极限 $\varphi \to \infty$ 时 $\sigma^2[\mu_i] \to 0$ —— **CPCV 报告的就是真实 SR，没有选择偏差**。

实际上 $\varphi$ 有上界 $\varphi[T, T/2]$，但只要足够大就能把方差压到假发现概率可忽略的水平。

### 5.4 实务效用

期刊可以要求作者用指定 $(N, k)$ 重新跑 CPCV。**研究员不能预先知道路径数和特性，他的过拟合努力会被瓦解**。希望 CPCV 能减少期刊里的假发现。

## 六、本章实操要点

1. **三种范式选择**：
   - **WF**：历史 paper trading 复现 → 用 WF，但只用作 sanity check
   - **CV**：单一场景 stress test → 用 CV
   - **CPCV**：策略选择与发布 → 必选 CPCV
2. **CPCV 默认 $k=2$**，$N = \varphi + 1$（$\varphi$ 是想要的路径数）。
3. **报告 SR 分布**，不是单个 SR 值——给均值 + 标准差 + 分位数。
4. **WF 反向回测做不出对称 SR 就是过拟合证据**。
5. **每个 train/test 都要 purge + embargo**（PurgedKFold 类）。
6. **作者立场**：CPCV 应当成为期刊审稿和监管尽调的标准。

## 七、关联阅读

- 上游：Purge + Embargo → [[chapter-07-cross-validation]]
- 上游：回测的危险与 PBO → [[chapter-11-dangers-of-backtesting]]
- 平行：合成数据回测（另一种"非历史"路径生成方法）→ [[chapter-13-synthetic-backtesting]]
- 下游：Deflated Sharpe Ratio（校正多重检验）→ [[chapter-14-backtest-statistics]]
- 参考：Bailey, López de Prado (2014) "The Deflated Sharpe Ratio"
- 参考：Bailey, Borwein, López de Prado, Zhu (2017) "The probability of backtest overfitting"
