# 第 21 章：暴力搜索与量子计算（Brute Force and Quantum Computers）

> 把"动态组合优化 + 任意 t-cost 函数 + 非凸目标"这个传统 NP-hard 问题转成离散整数优化形式，使其可被**量子退火器（quantum annealer）**求解。本章思路：任何 ML/金融难题都能用同样套路"离散化 → 量子暴力搜索"。

## 一、动机：离散数学在 ML 中的普遍性

ML 里常见的离散问题：层次聚类、grid search、阈值决策、整数优化。这些有时没有解析解、甚至没有启发式——只能暴力搜索。

经典电脑评估和存储可行解是**串行**的。**量子电脑**用 qubits（量子比特）能持有 $\{0, 1\}$ 的**线性叠加态**——理论上可以"同时评估所有可行解"。这种线性叠加性使量子计算天然适合 NP-hard 组合优化。

本章用一个例子展示如何把**动态组合优化 + 通用 t-cost 函数**改造成量子可解形式。

## 二、目标函数

资产 $X = \{x_i\}, i = 1, \ldots, N$，时间段 $h = 1, \ldots, H$。**Trading trajectory** $\omega$ 是 $N \times H$ 矩阵，每列是该时间段的资本分配比例。

每段有 $\mu_h$（均值）、$V_h$（方差）、$\tau_h[\omega]$（t-cost 函数）。

期望投资回报：
$$
r = \text{diag}[\mu' \omega] - \tau[\omega]
$$

通用 t-cost 形式（**非凸、非连续**）：
$$
\tau_1[\omega] = \sum_{n=1}^{N} c_{n,1} \sqrt{|\omega_{n,1} - \omega_n^*|}
$$
$$
\tau_h[\omega] = \sum_{n=1}^{N} c_{n,h} \sqrt{|\omega_{n,h} - \omega_{n,h-1}|}, \quad h \geq 2
$$

注意 $\sqrt{\cdot}$ 让 t-cost 关于交易量是凹函数（典型的 market impact）。

SR：
$$
\text{SR}[r] = \frac{\sum_h \mu_h'\omega_h - \tau_h[\omega]}{\sqrt{\sum_h \omega_h' V_h \omega_h}}
$$

## 三、问题与传统方法的失败

$$
\max_\omega \text{SR}[r], \quad \text{s.t. } \sum_i |\omega_{i,h}| = 1, \forall h
$$

不是凸优化，三大原因：
1. 收益**不同分布**（$\mu_h, V_h$ 随 $h$ 变）
2. **t-cost 非连续**且随 $h$ 变
3. SR 目标函数本身非凸

传统 mean-variance 只能找**单期局部最优**。本章追求**全局动态最优**。

## 四、整数优化逼近

### 4.1 Pigeonhole Partitions

把 $K$ 单位资本分给 $N$ 资产的方式数：$x_1 + \ldots + x_N = K$ 的非负整数解数 = $\binom{K+N-1}{N-1}$。

**与传统数论 partition 的差别**：**顺序在金融里有意义**——(1, 2, 3) 与 (3, 2, 1) 不同（哪种资产拿到 1 单位资本是关键）。

```python
from itertools import combinations_with_replacement

def pigeonHole(k, n):
    for j in combinations_with_replacement(range(n), k):
        r = [0] * n
        for i in j: r[i] += 1
        yield r
```

### 4.2 Feasible Static Solutions（单期所有可行解）

每个 partition $\{p_i\}$ 转成绝对权重 $|\omega_i| = p_i/K$，满足 $\sum |\omega_i| = 1$。

**全投资约束**允许每个权重为正或为负 → 每个绝对权重向量对应 $2^N$ 个签名权重。整个**单期可行集** $\Omega$：

$$
\Omega = \left\{\left.\left\{\frac{s_j}{K} p_i\right\}_i \right| \{s_j\} \in \{-1, 1\}^N, \{p_i\} \in p_{K,N}\right\}
$$

```python
def getAllWeights(k, n):
    parts, w = pigeonHole(k, n), None
    for part_ in parts:
        w_ = np.array(part_) / float(k)
        for prod_ in product([-1, 1], repeat=n):
            w_signed = (w_ * prod_).reshape(-1, 1)
            if w is None: w = w_signed.copy()
            else: w = np.append(w, w_signed, axis=1)
    return w
```

### 4.3 Evaluating Trajectories（多期暴力搜索）

所有 trajectory = $\Omega$ 的 $H$-重笛卡尔积 $\Phi$。对每条 trajectory 算 t-cost 和 SR，选最大者。

```python
def evalTCosts(w, params):
    tcost = np.zeros(w.shape[1])
    w_ = np.zeros(shape=w.shape[0])
    for i in range(tcost.shape[0]):
        c_ = params[i]['c']
        tcost[i] = (c_ * abs(w[:,i] - w_)**.5).sum()
        w_ = w[:,i].copy()
    return tcost

def evalSR(params, w, tcost):
    mean, cov = 0, 0
    for h in range(w.shape[1]):
        params_ = params[h]
        mean += np.dot(w[:,h].T, params_['mean'])[0] - tcost[h]
        cov += np.dot(w[:,h].T, np.dot(params_['cov'], w[:,h]))
    return mean / cov**.5

def dynOptPort(params, k=None):
    if k is None: k = params[0]['mean'].shape[0]
    n = params[0]['mean'].shape[0]
    w_all, sr = getAllWeights(k, n), None
    for prod_ in product(w_all.T, repeat=len(params)):
        w_ = np.array(prod_).T
        tcost_ = evalTCosts(w_, params)
        sr_ = evalSR(params, w_, tcost_)
        if sr is None or sr < sr_:
            sr, w = sr_, w_.copy()
    return w
```

### 4.4 优劣

- **优势**：协方差矩阵病态、t-cost 函数非连续 → 都能算。**完全不依赖凸优化的解析性**。
- **代价**：计算量等同 traveling-salesman——**经典电脑算不动**。
- **量子电脑优势**：线性叠加使得"一次评估所有 trajectory"成为可能。

Rosenberg et al. (2016) 用 **D-Wave quantum annealer** 解决了这个问题。同样思路可推广到任何路径依赖的金融问题。

## 五、数值示例（经典电脑实现）

### 5.1 生成已知 rank 的随机矩阵

```python
def rndMatWithRank(nSamples, nCols, rank, sigma=0, homNoise=True):
    rng = np.random.RandomState()
    U, _, _ = np.linalg.svd(rng.randn(nCols, nCols))
    x = np.dot(rng.randn(nSamples, rank), U[:,:rank].T)
    if homNoise:
        x += sigma * rng.randn(nSamples, nCols)
    else:
        sigmas = sigma * (rng.rand(nCols) + .5)
        x += rng.randn(nSamples, nCols) * sigmas
    return x
```

### 5.2 生成参数

```python
size, horizon = 3, 2
params = []
for h in range(horizon):
    x = rndMatWithRank(1000, 3, 3, 0.)
    mean_, cov_ = genMean(size), np.cov(x, rowvar=False)
    c_ = np.random.uniform(size=cov_.shape[0]) * np.diag(cov_)**.5
    params.append({'mean': mean_, 'cov': cov_, 'c': c_})
```

### 5.3 Static Solution（每段独立最优）

```python
def statOptPortf(cov, a):
    cov_inv = np.linalg.inv(cov)
    w = np.dot(cov_inv, a)
    w /= np.dot(np.dot(a.T, cov_inv), a)
    w /= abs(w).sum()
    return w

w_stat = None
for params_ in params:
    w_ = statOptPortf(cov=params_['cov'], a=params_['mean'])
    if w_stat is None: w_stat = w_.copy()
    else: w_stat = np.append(w_stat, w_, axis=1)
tcost_stat = evalTCosts(w_stat, params)
sr_stat = evalSR(params, w_stat, tcost_stat)
```

### 5.4 Dynamic Solution（全局最优）

```python
w_dyn = dynOptPort(params)
tcost_dyn = evalTCosts(w_dyn, params)
sr_dyn = evalSR(params, w_dyn, tcost_dyn)
```

Dynamic SR 严格 ≥ static SR——这正是 Markowitz 类方法忽视的部分。

## 六、本章实操要点

1. **任何路径依赖优化问题都能被改造成"离散化 + 暴力搜索"**——前提是离散化可控。
2. **顺序问题不能用经典 partition 公式**——必须考虑 permutation。
3. **全投资 + 多空允许** → 绝对权重 × $\{-1, 1\}^N$ 符号枚举。
4. **t-cost 用 $\sqrt{}$ 形式** 是经典 market impact 模型——凹函数。
5. **dyn vs static 的差距** 是 Markowitz 方法忽视的 alpha 来源。
6. **经典电脑只在小规模可解**：$N=3, H=2$ 是教学规模；实战必须用 quantum annealer（D-Wave 等）。
7. **思路推广**：层次聚类、组合特征选择、HRP 的某些扩展都可改造成量子可解形式。

## 七、关联阅读

- 上游：HRP 提供另一种规避矩阵求逆的思路 → [[chapter-16-asset-allocation]]
- 上游：可并行化的暴力搜索 → [[chapter-20-multiprocessing]]
- 下游：HPC 与量子的实际工程 → [[chapter-22-hpc-forecasting]]
- 参考：Rosenberg et al. (2016) "Solving the optimal trading trajectory problem using a quantum annealer", IEEE
- 参考：Williams (2010) Explorations in Quantum Computing
- 参考：Garleanu & Pedersen (2012) "Dynamic trading with predictable returns and transaction costs"（IID 高斯假设下的版本）
