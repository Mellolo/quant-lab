# 第 13 章：合成数据回测（Backtesting on Synthetic Data）

> 历史只是一条随机路径。本章给出第三条回测之路：**从历史估计随机过程参数，再用合成数据找最优交易规则（Optimal Trading Rule, OTR）**。避免对单一历史路径过拟合。聚焦在 Ornstein-Uhlenbeck 过程上推导止盈/止损的 OTR。

## 一、动机与定位

回测的本质问题：
- WF 在单一历史路径上 fit
- CV / CPCV 用历史的"切片重组"
- **本章**：直接从历史**估计随机过程参数**，然后在 **大量合成路径**上找最优规则

这样**最优交易规则（OTR）**不依赖任何一条具体历史路径，从根本上避免对历史的过拟合。

本章以"如何最优退出已有仓位"为核心问题（不是入场规则），符合执行交易员的视角。

## 二、Trading Rule 的定义与过拟合

### 2.1 基础定义

策略 $S$ 进行 $i = 1, \ldots, I$ 次下注，每次开仓 $m_i$ 单位证券 X，开仓均价 $P_{i,0}$。$t$ 时刻 MtM 盈亏：
$$
\pi_{i,t} = m_i(P_{i,t} - P_{i,0})
$$

退出条件二选一：
- $\pi_{i,T_i} \geq \bar\pi$（止盈，$\bar\pi > 0$）
- $\pi_{i,T_i} \leq \underline\pi$（止损，$\underline\pi < 0$）

**Trading Rule** $R := \{\underline\pi, \bar\pi\}$。

### 2.2 暴力校准的诱惑（也是过拟合的根源）

1. 定义参数空间 $\Omega := \{R\}$
2. 对每个 $R \in \Omega$ 做历史回测
3. 选最优 $R^*$

$$
R^* = \arg\max_{R \in \Omega} \text{SR}_R, \quad \text{SR}_R = \frac{E[\pi_{i,T_i}|R]}{\sigma[\pi_{i,T_i}|R]}
$$

**问题**：两个自由参数对 $I$ 个样本做最大化，很容易"找到"那对正好打中几个 outlier 的 $(\underline\pi, \bar\pi)$。

### 2.3 过拟合的形式化定义

$R^*$ 是过拟合的，当且仅当 OOS 上的 SR 期望低于参数空间中位数：
$$
E\left[\frac{E[\pi_{j,T_j}|R^*]}{\sigma[\pi_{j,T_j}|R^*]}\right] < \text{Me}_\Omega\left[E\left[\frac{E[\pi_{j,T_j}|R]}{\sigma[\pi_{j,T_j}|R]}\right]\right]
$$

即"IS 最优规则在 OOS 上低于中位数"。这是第 11 章 PBO 的同款定义。

## 三、建模框架：离散 O-U 过程

价格服从离散 Ornstein-Uhlenbeck：
$$
P_{i,t} = (1 - \varphi) E_0[P_{i,T_i}] + \varphi P_{i,t-1} + \sigma \varepsilon_{i,t}
$$
$\varepsilon_{i,t} \sim N(0, 1)$，$\varphi \in (-1, 1)$ 保证平稳。

- $P_{i,0}$：开仓价
- $E_0[P_{i,T_i}]$：目标长期均衡
- $\varphi$：均值回归速度（$\varphi \to 1$ → 接近随机游走）
- **半生命期** $\tau = -\log[2] / \log[\varphi]$（$\varphi \in (0, 1)$ 时），反推 $\varphi = 2^{-1/\tau}$

PnL 分布（来自 Bailey & López de Prado 2013）：
$$
\pi_{i,t} \sim N\left[m_i\left((1-\varphi)E_0[P_{i,T_i}] \sum_{j=0}^{t-1}\varphi^j - P_{i,0}\right), m_i^2 \sigma^2 \sum_{j=0}^{t-1}\varphi^{2j}\right]
$$

## 四、OTR 算法（核心五步）

### Step 1：估计 $\{\sigma, \varphi\}$

把方程线性化：
$$
P_{i,t} = E_0[P_{i,T_i}] + \varphi(P_{i,t-1} - E_0[P_{i,T_i}]) + \xi_t
$$

按所有 opportunities 拼成向量 $X$（输入）、$Y$（被解释）、$Z$（forecast 序列），OLS 估计：
$$
\hat\varphi = \frac{\text{cov}[Y, X]}{\text{cov}[X, X]}, \quad \hat\xi_t = Y - Z - \hat\varphi X, \quad \hat\sigma = \sqrt{\text{cov}[\hat\xi_t, \hat\xi_t]}
$$

### Step 2：构造 trading rules 网格
笛卡尔积 $\underline\pi \in \{-\sigma/2, -\sigma, \ldots, -10\sigma\}$，$\bar\pi \in \{\sigma/2, \sigma, \ldots, 10\sigma\}$ → 20×20 = 400 条规则。

### Step 3：生成合成路径
用 $\{\hat\sigma, \hat\varphi\}$ 模拟 100,000 条 $\pi_{i,t}$ 路径。种子条件用观察到的 $\{P_{i,0}, E_0[P_{i,T_i}]\}$。**设最大持仓期**（如 100 个 bar）作为垂直屏障——即使没触碰止盈/止损也强制平仓。

### Step 4：在 20×20 网格上评估
每条规则、每条路径 → 一个 $\pi_{i,T_i}$。每条规则汇总 100,000 个 $\pi_{i,T_i}$ → 一个 SR。

### Step 5：找最优
- **5a**：直接选 SR 最大的 $(\underline\pi, \bar\pi)$
- **5b**：若 $\bar\pi_i$ 已由策略指定，找最优 $\underline\pi_i$
- **5c**：若 $\underline\pi_i$ 已由风控强制，在 $[0, \underline\pi_i]$ 范围内找最优 $\bar\pi_i$

### 4.1 代码骨架

```python
def main():
    rPT = rSLm = np.linspace(0, 10, 21)
    for prod_ in product([10, 5, 0, -5, -10], [5, 10, 25, 50, 100]):
        coeffs = {'forecast': prod_[0], 'hl': prod_[1], 'sigma': 1}
        output = batch(coeffs, nIter=1e5, maxHP=100, rPT=rPT, rSLm=rSLm)

def batch(coeffs, nIter=1e5, maxHP=100, rPT=..., rSLm=..., seed=0):
    phi = 2 ** (-1./coeffs['hl'])
    output1 = []
    for comb_ in product(rPT, rSLm):
        output2 = []
        for iter_ in range(int(nIter)):
            p, hp = seed, 0
            while True:
                p = (1-phi)*coeffs['forecast'] + phi*p + coeffs['sigma']*gauss(0, 1)
                cP = p - seed; hp += 1
                if cP > comb_[0] or cP < -comb_[1] or hp > maxHP:
                    output2.append(cP); break
        mean, std = np.mean(output2), np.std(output2)
        output1.append((comb_[0], comb_[1], mean, std, mean/std))
    return output1
```

## 五、实验结果（热图启示）

作者跑了 5 组 forecast × 5 组半生命期 = 25 张热图。SR 用灰度展示，浅 = 高，深 = 低。

### 5.1 长期均衡 = 0（做市商场景）

- **$\tau$ 小**（如 5）：高 SR 集中在"小止盈 + 大止损"的窄区——典型做市商风格（持仓等待小盈利，宽容大浮亏）。最佳 SR ≈ 3.2。
- **对角线附近**（对称止盈/止损）SR 接近零——**第 3 章 triple-barrier 用对称屏障时要警惕**。
- **$\tau$ 增大**（10 → 25 → 50 → 100）：高/低 SR 区域逐渐扩散，SR 整体下降，因为 $\varphi \to 1$ 进入随机游走。
- **$\tau = 100$ 时无识别区** → 没有最优规则。**如果在随机游走上历史校准 trading rule = 必然过拟合**。

### 5.2 长期均衡 > 0（仓位持有方场景：对冲基金/资管）

- **$\{5, 5, 1\}$**：最优集中在"止盈 ≈ 6 + 止损 4-10"的**矩形区**，SR ≈ 12。
- 矩形区源自"宽止损 + 窄止盈"。
- **$\tau$ 增大** → 矩形变方形，最优止盈范围扩、止损范围收，SR 下降。
- **$\tau = 100$ 时**：SR 仅 0.32，几乎不可识别。
- **$E_0[P_{i,T_i}] = 10$** 时模式类似，但 SR 更高（开始时 ≈ 20）。

### 5.3 长期均衡 < 0（已有亏损仓位的止损场景）

- 与正均衡情况是**旋转互补**关系——盈亏角色对调。
- $\{-5, 5, 1\}$：最差 SR 区域是矩形（止损 ≈ 6，止盈 4-10），SR ≈ -12。
- 实务意义：理性投资者不会主动建立期望亏损的仓位，但**当被迫面对必亏的仓位时，如何最小化损失**仍需 OTR。
- $\tau \to 100$ → SR 平坦化，最优解失去意义（与正均衡情况一样）。

## 六、关键结论与猜想

### 6.1 OTR 猜想

> 对服从离散 O-U 过程的价格，**存在唯一一对最优 $(\underline\pi, \bar\pi)$ 使规则 SR 最大化**。

作者承认这只是经验观察（experimental conjecture），证明可能需要数年。但他给出操作主义的辩护：

> "这个猜想为假的概率，远低于因为忽略它而过拟合 trading rule 的概率。理性做法是假设它成立，用合成数据求 OTR。最坏情况，规则次优——但仍几乎必然优于过拟合的规则。"

### 6.2 方法的普适性

- 本章只用 O-U 是为教学清晰，方法可推广到任何随机过程。
- 同样可加垂直屏障作为第三维（变成 20×20×20 mesh）。

### 6.3 与 triple-barrier method 的连接
- triple barrier 也用 $(\bar\pi, \underline\pi, h)$ 三个屏障
- maxHP（最大持仓期）= triple barrier 中的垂直屏障

## 七、本章实操要点

1. **训练 trading rule 不要在历史路径上 grid search**——必然过拟合。
2. **拟合一个随机过程**（O-U 是常用起点，但不是唯一）
3. **生成 10 万条合成路径**，在合成路径上做 grid search
4. **报告热图**：直接看 SR 在 $(\underline\pi, \bar\pi)$ 平面的几何形状
5. **几何启示**：
   - 矩形区 → 真的有 OTR
   - 整个平面扁平 → 当前 regime 接近随机游走，**不要交易**
6. **make-market（$E_0 = 0$）和 take-position（$E_0 > 0$）有截然不同的最优规则形状**——不要照搬。
7. **对称止盈/止损 = 几乎零 SR**——triple-barrier 默认对称要警惕。
8. **半生命期 $\tau$ 是关键参数**——通过 OLS 估计的 $\varphi$ 反推。
9. **如果已经面对必亏仓位**，OTR 仍能告诉你如何最小化损失。

## 八、关联阅读

- 上游：triple-barrier 三屏障概念 → [[chapter-03-labeling]]
- 平行：组合化 CV → [[chapter-12-backtesting-cv]]
- 评估：SR 与 Probabilistic / Deflated SR → [[chapter-14-backtest-statistics]]
- 风险量化 → [[chapter-15-strategy-risk]]
- HPC：合成路径模拟可大规模并行 → [[chapter-20-multiprocessing]]
- 参考：Bailey & López de Prado (2013) "Drawdown-based stop-outs and the triple penance rule"
- 参考：Bertram (2009) "Analytic solutions for optimal statistical arbitrage trading"（入场规则的解析解）
