# 第 16 章：机器学习资产配置（Machine Learning Asset Allocation）

> 这是 Part 3 的压轴章节。作者提出 **Hierarchical Risk Parity (HRP)**——一种**不需要协方差矩阵可逆**的资产配置方法，用图论 + 聚类替代 Markowitz 的二次规划。Monte Carlo 显示 HRP 在 OOS 上方差比 CLA 低 72%、比传统 IVP 低 38%。

## 一、动机：Markowitz 配置的三大病

凸优化（Markowitz CLA = Critical Line Algorithm）的三大问题：
1. **Instability**（不稳定）
2. **Concentration**（仓位集中）
3. **Underperformance**（OOS 表现差）

CLA 是 Markowitz 在 RAND 时开发的二次优化算法，专门处理不等式约束的组合优化，能保证有限步收敛到精确解。但它（以及任何 quadratic optimizer）都依赖**协方差矩阵求逆**——这是病根。

## 二、Markowitz 的诅咒

**协方差矩阵的 condition number** = 最大特征值 / 最小特征值。
- 对角矩阵：condition number 最低（自身就是逆）
- 加入相关投资 → condition number 急升 → 逆矩阵不稳定 → 任何小扰动产生截然不同的逆

**Markowitz 诅咒**：**投资越相关，分散化的需求越大；但 condition number 也越高，逆矩阵越不稳定，估计误差就吞掉分散化收益**。

更糟糕的是：维度 $N$ 越大每个协方差系数的自由度越少。要让 $N \times N$ 协方差矩阵不奇异，至少需要 $\frac{1}{2}N(N+1)$ 个 IID 观测。**$N = 50$ → 至少 5 年日 IID 数据**——而金融相关结构在 5 年内显然不稳定。

De Miguel et al. (2009) 的实证：**简单的等权 1/N 组合在 OOS 上居然击败 mean-variance 和 risk-based 优化**。

## 三、从几何到层次结构

主流应对（约束、Bayesian prior、Ledoit-Wolf shrinkage）都基于经典数学（几何 / 线性代数 / 微积分）。

**深层原因**：协方差矩阵把投资宇宙建模为**完全图**——每个节点都和其他所有节点相连。$N=50$ 就有 1225 条边。任何小扰动都会被这种密集结构放大。

**层次结构（树）**的两个优势：
1. $N-1$ 条边连接 $N$ 个节点——权重只在同层 peers 间再平衡
2. **权重自上而下分配**——和资管业实务一致（资产类 → 行业 → 个券）

## 四、HRP 三阶段算法 ★

### 4.1 阶段 1：Tree Clustering

**Step 1**：对相关性 $\rho_{i,j}$，定义距离：
$$
d_{i,j} = \sqrt{\frac{1}{2}(1 - \rho_{i,j})}
$$
这是真正的度量（满足非负性、同一性、对称性、三角不等式 —— 见附录 16.A.1 证明）。

**Step 2**：再定义"距离的距离"：
$$
\tilde d_{i,j} = \sqrt{\sum_{n=1}^N (d_{n,i} - d_{n,j})^2}
$$
注意 $d_{i,j}$ 定义在 $X$ 的列向量上，$\tilde d_{i,j}$ 定义在 $D$ 的列向量上——每个 $\tilde d_{i,j}$ 是**整个相关矩阵**的函数。

**Step 3**：找 $\tilde d_{i,j}$ 最小的列对 $(i^*, j^*)$，作为第一个 cluster $u[1]$。

**Step 4**：定义新 cluster 与剩余 item 的距离（linkage criterion）。书中用 **single linkage**（nearest point）：
$$
\dot d_{i, u[1]} = \min[\{\tilde d_{i,j}\}_{j \in u[1]}]
$$

**Step 5**：更新 $\tilde d$ 矩阵：添加 $\dot d_{i,u[1]}$，删除已 cluster 的列。

**Step 6**：递归。N-1 步后所有 item 合成一个 cluster。

```python
import scipy.cluster.hierarchy as sch
cov, corr = x.cov(), x.corr()
dist = ((1-corr)/2.) ** .5
link = sch.linkage(dist, 'single')
```

输出 **linkage matrix** $Y$（$(N-1) \times 4$）：每行 $(y_{m,1}, y_{m,2}, y_{m,3}, y_{m,4})$ = 两个被合并的成分、合并距离、cluster 内原始 item 数。

### 4.2 阶段 2：Quasi-Diagonalization

重新排列协方差矩阵的行和列，**让大值落在对角线上**——相似投资聚在一起。

算法：对 linkage matrix 从最后一行（最末合并）开始，递归把 cluster 替换为它的成分，直到没有 cluster 剩下。

```python
def getQuasiDiag(link):
    link = link.astype(int)
    sortIx = pd.Series([link[-1,0], link[-1,1]])
    numItems = link[-1,3]
    while sortIx.max() >= numItems:
        sortIx.index = range(0, sortIx.shape[0]*2, 2)
        df0 = sortIx[sortIx >= numItems]
        i = df0.index; j = df0.values - numItems
        sortIx[i] = link[j, 0]
        df0 = pd.Series(link[j, 1], index=i+1)
        sortIx = sortIx.append(df0).sort_index()
        sortIx.index = range(sortIx.shape[0])
    return sortIx.tolist()
```

### 4.3 阶段 3：Recursive Bisection

对角化后，**inverse-variance allocation 对对角矩阵是最优的**（附录 16.A.2 证明）。

算法：
1. 初始化 $L = \{L_0\}$，所有 item 权重 = 1
2. 若所有 $|L_i| = 1$，停止
3. 对 $|L_i| > 1$ 的每个 $L_i$：
   - **二分** $L_i = L_i^{(1)} \cup L_i^{(2)}$，前半 = $\text{int}[|L_i|/2]$
   - 计算每半的"cluster variance"（inverse-variance 加权后的二次型）：
     $$
     \tilde V_i^{(j)} = \tilde w_i^{(j)'} V_i^{(j)} \tilde w_i^{(j)}, \quad \tilde w_i^{(j)} = \frac{\text{diag}[V_i^{(j)}]^{-1}}{\text{tr}[\text{diag}[V_i^{(j)}]^{-1}]}
     $$
   - 计算分裂因子：
     $$
     \alpha_i = 1 - \frac{\tilde V_i^{(1)}}{\tilde V_i^{(1)} + \tilde V_i^{(2)}}
     $$
   - 重缩 $L_i^{(1)}$ 的权重 $\times \alpha_i$，$L_i^{(2)}$ $\times (1 - \alpha_i)$
4. 回到步骤 2

```python
def getRecBipart(cov, sortIx):
    w = pd.Series(1, index=sortIx)
    cItems = [sortIx]
    while len(cItems) > 0:
        cItems = [i[j:k] for i in cItems
                  for j, k in ((0, len(i)//2), (len(i)//2, len(i)))
                  if len(i) > 1]
        for i in range(0, len(cItems), 2):
            cVar0 = getClusterVar(cov, cItems[i])
            cVar1 = getClusterVar(cov, cItems[i+1])
            alpha = 1 - cVar0 / (cVar0 + cVar1)
            w[cItems[i]] *= alpha
            w[cItems[i+1]] *= 1 - alpha
    return w
```

**性质**：
- 所有 $w_i \in [0, 1]$，$\sum w_i = 1$（每次只分裂从上层来的权重）
- 复杂度：最好 $\mathcal{O}(\log_2 n)$，最坏 $\mathcal{O}(n)$ deterministic 时间
- **不需要协方差矩阵可逆**——即使奇异也能算

## 五、数值例子

构造 10×10 的相关矩阵（前 5 列独立，后 5 列是前 5 列的扰动），condition number = 150.93（不算高）。

| Weight # | CLA | HRP | IVP |
|---|---|---|---|
| 1 | 14.44% | 7.00% | 10.36% |
| 2 | 19.93% | 7.59% | 10.28% |
| 3 | 19.73% | 10.84% | 10.36% |
| 4 | 19.87% | 19.03% | 10.25% |
| 5 | 18.68% | 9.72% | 10.31% |
| 6 | 0.00% | 10.19% | 9.74% |
| 7 | 5.86% | 6.62% | 9.80% |
| 8 | 1.49% | 9.10% | 9.65% |
| 9 | 0.00% | 7.12% | 9.64% |
| 10 | 0.00% | 12.79% | 9.61% |

观察：
- **CLA 把 92.66% 集中在 top-5**，3 个仓位为 0
- **HRP 只集中 62.57% 在 top-5**
- **IVP 几乎均匀**，忽略相关结构

In-sample 风险：$\sigma_{\text{HRP}} = 0.4640$，$\sigma_{\text{CLA}} = 0.4486$（CLA 几乎相同但抛弃了半个宇宙）。

## 六、Out-of-Sample Monte Carlo（核心实证）

模拟：10 个高斯收益序列、520 观测（2 年日数据）、加随机相关结构、随机冲击。每 22 个观测（月频）重新估计 + 调仓。10,000 次 Monte Carlo。

OOS 方差结果：
| 方法 | OOS 方差 |
|---|---|
| **CLA** | 0.1157 |
| IVP | 0.0928 |
| **HRP** | 0.0671 |

- **HRP 比 CLA 低 72.47%**
- HRP 比 IVP 低 38.24%
- HRP 把 CLA 策略的 OOS SR 提升约 31.3%

**反直觉**：CLA 的优化目标就是最小方差，但它在 OOS 上方差最高。

### 6.1 三种方法的特性对比

| 冲击类型 | CLA | IVP | HRP |
|---|---|---|---|
| 单个投资特异冲击 | 因集中而暴露 | 把仓位分给所有 | 把仓位分给**相关但未受冲击的** |
| 多个相关投资共同冲击 | 因集中而暴露 | 把仓位均匀转移（忽略相关结构） | 把仓位转移到**不相关、低方差的** |
| 调仓成本 | **极高**（频繁、剧烈） | 中等 | 低（层次化、稳定） |

HRP 在两种冲击下都有更好的保护，因为它在"全投资分散"和"跨 cluster 分散"之间找到平衡。

## 七、进一步研究

- **distance metrics**：可用 $\sqrt{1 - |\rho|}$（descends to true metric on $\mathbb{Z}/2\mathbb{Z}$ quotient）
- **linkage criterion**：可用其他算法（complete, average, Ward, biclustering, Fiedler vector, Stewart spectral）
- **stage 3 的变体**：可用其他 $\tilde w_m$ 函数、$\alpha$ 函数、带约束的递归
- 可整合 forecasted returns、Ledoit-Wolf shrinkage、Black-Litterman views
- 实证应用：Kolanovic et al. (2017, JPM)、Raffinot (2017) 都证实 HRP 持续优于 MV 与其他 risk-based 方法

## 八、HRP 的应用范围（超越 portfolio construction）

作者明示：HRP 本质上是**一种避免矩阵求逆的稳健流程**，同样思路可以替代很多不稳定的计量回归（VAR、VECM）。具体应用：

1. 组合构造（本章核心）
2. PM 间的资本分配
3. 多策略间的资金分配
4. ML 信号的 bagging / boosting
5. RF 的预测整合
6. 替代不稳定的计量模型（VAR、VECM）

## 九、附录数学补充

### 9.1 相关距离的正确性
对 z-score 标准化向量 $x, y$，欧氏距离：
$$
d[x, y] = \sqrt{2T(1 - \rho[x, y])} = \sqrt{4T} \cdot d[X, Y]
$$
即所定义的 $d[X, Y]$ 是 z 标准化向量欧氏距离的线性倍数 → 继承欧氏距离的真度量性质。

### 9.2 Inverse Variance 在对角矩阵上是最优的

二次优化 $\min_\omega \omega' V \omega$ s.t. $\omega' \mathbf{1} = 1$，解 $\omega = V^{-1}\mathbf{1} / (\mathbf{1}' V^{-1}\mathbf{1})$。对角矩阵下 $\omega_n = V_{n,n}^{-1} / \sum_i V_{i,i}^{-1}$。

$N=2$ 时：$\omega_1 = \frac{1/V_{1,1}}{1/V_{1,1} + 1/V_{2,2}} = 1 - \frac{V_{1,1}}{V_{1,1} + V_{2,2}}$，正是阶段 3 的 $\alpha$ 公式。

## 十、本章实操要点

1. **抛弃 mean-variance 优化**——OOS 不稳定，对相关性高估或低估都崩。
2. **HRP 三步走**：tree clustering → quasi-diagonalization → recursive bisection。
3. **distance 用 $\sqrt{(1-\rho)/2}$**——真度量，对真正的负相关也鲁棒。
4. **linkage 用 single** 作为起点——可以尝试 average、Ward、complete。
5. **HRP 不依赖协方差可逆**——即使奇异矩阵也行。
6. **报告 OOS Monte Carlo 方差**——in-sample 方差比较没意义。
7. **杠杆型 risk parity 必用 HRP**——OOS 方差降低意味着杠杆下回撤大幅减少。
8. **HRP 也是替代不稳定计量回归（VAR/VECM）的工具**。

## 十一、关联阅读

- 上游：金融大数据的 PCA 失败 → [[chapter-08-feature-importance]]（正交化讨论）
- 上游：协方差矩阵的 PCA 权重（CLA 的传统对手）→ [[chapter-02-financial-data-structures]]
- 平行：策略风险评估 → [[chapter-15-strategy-risk]]
- 平行：回测三大范式 → [[chapter-11-dangers-of-backtesting]]、[[chapter-12-backtesting-cv]]、[[chapter-13-synthetic-backtesting]]
- 性能评估 → [[chapter-14-backtest-statistics]]
- 参考：Markowitz (1952) "Portfolio Selection"
- 参考：De Miguel, Garlappi, Uppal (2009) "Optimal vs Naive Diversification"
- 参考：Kolanovic et al. (2017) JP Morgan HRP white paper
