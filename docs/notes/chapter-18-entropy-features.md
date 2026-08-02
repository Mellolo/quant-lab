# 第 18 章：熵特征（Entropy Features）

> 用信息论度量价格序列的信息含量。**高熵 = 难以预测 = 市场效率高**；**低熵 = 模式重复 = 市场被压缩**。本章给出 Shannon 熵、Plug-In、Lempel-Ziv（含 Kontoyiannis 改进版）估计器，以及在 portfolio 集中度、PIN / VPIN 等金融场景的应用。

## 一、动机

完美市场上价格不可预测——每个观测传输全部已知信息。不完美市场下价格只含部分信息，掌握更多信息的代理人可以利用不对称。

熵能告诉我们：
- 价格序列含有多少信息？
- ML 可以学到："价格信息少时 → 动量 bet 更赚；价格信息多时 → 均值回归 bet 更赚"。

## 二、Shannon 熵基础

### 2.1 定义

离散随机变量 $X$ 取值 $x \in A$，概率 $p[x]$：
$$
H[X] = -\sum_{x \in A} p[x] \log_2 p[x]
$$

性质：
- $0 \leq H[X] \leq \log_2[\|A\|]$
- $H[X] = 0$ ↔ 存在 $x$ 使 $p[x] = 1$（确定性）
- $H[X] = \log_2[\|A\|]$ ↔ $p[x] = 1/\|A\|$（均匀分布，最大熵）

直觉："低概率事件信息量大，高概率事件信息量小"——$\log_2 \frac{1}{p[x]}$ 度量信息量。

### 2.2 冗余度

$$
R[X] = 1 - \frac{H[X]}{\log_2[\|A\|]}, \quad R[X] \in [0, 1]
$$

Kolmogorov (1965) 把冗余度与马尔可夫信息源的复杂度联系起来。

### 2.3 互信息

$$
\text{MI}[X, Y] = E_{f[x,y]} \log\left[\frac{f[x,y]}{f[x]f[y]}\right] = H[X] + H[Y] - H[X, Y]
$$

非负、对称，$=0$ ↔ $X \perp Y$。对正态变量：
$$
\text{MI}[X, Y] = -\frac{1}{2}\log[1 - \rho^2]
$$

所以 MI 是 Pearson 相关的非线性推广，**线性和非线性关系都能捕获**。

## 三、Plug-In（最大似然）估计

数据序列 $x_1^n$，长度 $w < n$ 的所有 word 字典 $A^w$。任一 word $y_1^w$ 的经验概率 $\hat p_w[y_1^w]$ = 出现频率。

熵率估计：
$$
\hat H_{n,w} = -\frac{1}{w} \sum_{y_1^w \in A^w} \hat p_w[y_1^w] \log_2 \hat p_w[y_1^w]
$$

要求：$w$ 足够大才接近真熵；$n$ 远大于 $w$ 保证经验分布接近真分布。

```python
def plugIn(msg, w):
    pmf = pmf1(msg, w)
    out = -sum([pmf[i] * np.log2(pmf[i]) for i in pmf]) / w
    return out, pmf

def pmf1(msg, w):
    lib = {}
    if not isinstance(msg, str): msg = ''.join(map(str, msg))
    for i in range(w, len(msg)):
        msg_ = msg[i-w:i]
        if msg_ not in lib: lib[msg_] = [i-w]
        else: lib[msg_] = lib[msg_] + [i-w]
    pmf = float(len(msg) - w)
    return {i: len(lib[i]) / pmf for i in lib}
```

## 四、Lempel-Ziv 类估计器

### 4.1 直觉
熵是复杂度的度量。LZ 算法把消息分解为**不重复子串**，复杂消息的 LZ 字典相对消息长度更大。

```python
def lempelZiv_lib(msg):
    i, lib = 1, [msg[0]]
    while i < len(msg):
        for j in range(i, len(msg)):
            msg_ = msg[i:j+1]
            if msg_ not in lib:
                lib.append(msg_); break
        i = j + 1
    return lib
```

### 4.2 Kontoyiannis (1998) 改进 ★

$L_i^n = 1 +$ 在 $i$ 之前 $n$ bits 内能找到的最长匹配长度：
$$
L_i^n = 1 + \max\{l \mid x_i^{i+l} = x_j^{j+l}, \, i-n \leq j \leq i-1, \, l \in [0, n]\}
$$

```python
def matchLength(msg, i, n):
    subS = ''
    for l in range(n):
        msg1 = msg[i:i+l+1]
        for j in range(i-n, i):
            msg0 = msg[j:j+l+1]
            if msg1 == msg0:
                subS = msg1; break
    return len(subS) + 1, subS
```

Ornstein & Weiss (1993) 证明：
$$
\lim_{n \to \infty} \frac{L_i^n}{\log_2[n]} = \frac{1}{H}
$$

### 4.3 滑动窗口估计器

$$
\hat H_{n,k} = \left[\frac{1}{k} \sum_{i=1}^{k} \frac{L_i^n}{\log_2[n]}\right]^{-1}
$$

### 4.4 扩张窗口估计器
$$
\hat H_n = \left[\frac{1}{n} \sum_{i=2}^{n} \frac{L_i^i}{\log_2[i]}\right]^{-1}
$$

### 4.5 避开 Doeblin 条件的改进版（Gao et al. 2008）★

$$
\tilde H_{n,k} = \frac{1}{k} \sum_{i=1}^{k} \frac{\log_2[n]}{L_i^n}, \quad \tilde H_n = \frac{1}{n} \sum_{i=2}^{n} \frac{\log_2[i]}{L_i^i}
$$

### 4.6 窗口大小选择

bias 是 $\mathcal{O}(1/\log_2[n])$，variance 是 $\mathcal{O}(1/k)$。
**bias/variance 平衡点**：$k \approx (\log_2[n])^2$，即 $N \approx n + (\log_2[n])^2$。

例：$N = 2^8 = 256$ → $n \approx 198, k \approx 58$。

### 4.7 实现（Konto 函数）

```python
def konto(msg, window=None):
    out = {'num': 0, 'sum': 0, 'subS': []}
    if not isinstance(msg, str): msg = ''.join(map(str, msg))
    if window is None:
        points = range(1, len(msg)//2 + 1)
    else:
        window = min(window, len(msg)//2)
        points = range(window, len(msg) - window + 1)
    for i in points:
        if window is None:
            l, msg_ = matchLength(msg, i, i)
            out['sum'] += np.log2(i + 1) / l
        else:
            l, msg_ = matchLength(msg, i, window)
            out['sum'] += np.log2(window + 1) / l
        out['subS'].append(msg_)
        out['num'] += 1
    out['h'] = out['sum'] / out['num']
    out['r'] = 1 - out['h'] / np.log2(len(msg))
    return out
```

### 4.8 三个 caveat
1. 熵率定义在极限上——短消息时收敛慢，可重复消息多次。
2. 匹配窗口对称要求 → 偶数长度（奇数则去掉首位）。
3. 末尾若紧接着不规则序列会被忽略（同因对称要求）→ 若末尾关键，可对**反向消息**做熵估计。

## 五、Encoding 方案

熵估计要求离散化。三种常见编码：

### 5.1 Binary Encoding
$r_t > 0 \to 1$，$r_t < 0 \to 0$，去掉 $r_t = 0$。

自然适合三屏障 bar（$|r_t|$ 近似恒定）。

**警告**：$|r_t|$ 范围广时会丢信息。intraday time bars 的异方差会让 binary 编码效果差。**用 volume / dollar bars 可以规整 $|r_t|$ 分布**。

### 5.2 Quantile Encoding
按训练集分位数赋码。各 letter 等概率（IS）→ **倾向于推高熵读数**。

### 5.3 Sigma Encoding
固定步长 $\sigma$，按 $\lfloor (r_t - \min r)/\sigma \rfloor$ 赋码。每个 code 覆盖相同 range，但不均匀分布 → **倾向于压低熵读数；但稀有 code 出现会引发熵尖峰**。

**作者建议**：编码对象**用分数阶差分序列**（第 5 章）而不是整数差分序列——保留记忆。

## 六、高斯过程的熵（标杆）

IID 正态过程：
$$
H = \frac{1}{2} \log[2\pi e \sigma^2]
$$
标准正态：$H \approx 1.42$。

用途：
1. **检验估计器**：从标准正态采样，看哪种 estimator + message length + encoding 接近 $H \approx 1.42$。
2. **熵 → 波动率反推**：$\sigma_H = \frac{e^{H - 1/2}}{\sqrt{2\pi}}$ —— 给出 **entropy-implied volatility**。

实验：100 长度消息上的 Kontoyiannis 估计，**字母数 ≥ 10 时**估计接近真值；**字母数 ≤ 5 时**严重低估熵。

## 七、熵与广义均值

广义加权均值：
$$
M_q[x, p] = \left(\sum_{i=1}^n p_i x_i^q\right)^{1/q}
$$

特例：
- $q \to -\infty$：最小值
- $q = -1$：调和平均
- $q \to 0$：几何平均
- $q = 1$：算术平均（等权时）
- $q = 2$：均方根
- $q \to +\infty$：最大值

特例 $x = \{p_i\}$ 时，定义 $N_q[p] = 1/M_{q-1}[p, p]$。

$N_q[p]$ 给出 $p$ 中"有效元素数量" / "多样性"。等权 $k$ 个、其余为零的分布下 $N_q[p] = k, \forall q > 1$。

### 7.1 Shannon 熵的广义均值视角

$$
H[p] = -\sum p_i \log p_i = \log\left[\lim_{q \to 1} N_q[p]\right]
$$

**Shannon 熵 = log of effective number of items**（$q \to 1$）。

**含义**：熵就是一种多样性度量。Shannon 熵是 $q=1$ 的特例——可以定义 $q \neq 1$ 的其他多样性度量。

## 八、金融应用

### 8.1 市场效率
完美套利 → 价格瞬间反映所有信息 → 鞅 → **无模式**。
不完美套利 → 信息不完全 → 可预测模式 → 字符串可压缩。

**高熵 = 解压缩市场 = 效率高 = 非冗余信息**
**低熵 = 压缩市场 = 效率低 = 信息冗余**
**泡沫在低熵市场中形成**。

### 8.2 最大熵生成（Fiedor 2014）
从所有可能的未来路径中，**熵最大的路径最难被频率派统计模型预测** → 最可能是"黑天鹅"路径 → 触发止损 → 反馈机制放大趋势 → returns 符号 runs。

### 8.3 组合集中度（Meucci 2009）★

协方差矩阵 $V$ 谱分解 $VW = W\Lambda$。因子载荷 $f_\omega = W'\omega$。第 $i$ 主成分贡献的风险：
$$
\theta_i = \frac{[f_\omega]_i^2 \Lambda_{i,i}}{\sum_n [f_\omega]_n^2 \Lambda_{n,n}}
$$

满足 $\sum \theta_i = 1$。**组合集中度**：
$$
H = 1 - \frac{e^{-\sum_n \theta_i \log[\theta_i]}}{N}
$$

虽然 $\theta_i$ 不是概率，但通过广义均值的视角，"熵"作为"有效项数的对数"在这里完美适用。

### 8.4 市场微观结构（PIN / VPIN）

Easley et al. (1996, 1997) 的 PIN：
$$
\text{PIN} = \frac{\alpha\mu}{\alpha\mu + 2\varepsilon}
$$
- $\mu$：informed trader 到达率
- $\varepsilon$：uninformed trader 到达率
- $\alpha$：信息事件概率

在 volume bar 时钟下，$E[V_\tau^B + V_\tau^S] = V$ 是外生固定的，PIN 退化为：
$$
\text{VPIN} = \frac{\alpha\mu}{V} \approx \frac{1}{V} E[|2V_\tau^B - V|] = E[|2v_\tau^B - 1|]
$$

$2v_\tau^B - 1 \in [-1, 1]$ 就是**order flow imbalance** $\text{OI}_\tau$。

**关键观察**：持续的 OI 是必要非充分条件——只有 OI **难以预测**时市场做市商才会被 adverse-selected。用熵来度量"OI 的不可预测程度"：

1. 算 $v_\tau^B \in [0, 1]$
2. $q$ 分位数把 $\{v_\tau^B\}$ 分成 $q$ 个 disjoint 子集
3. 把每个 $v_\tau^B$ 映射到对应子集编号 → quantized message $X$
4. 用 Kontoyiannis 算 $H[X]$
5. 算 $F[H[X]]$ 的 CDF 序列 → 作为 adverse selection 的预测特征

## 九、本章实操要点

1. **优先用 dollar / volume bars 上的 binary 编码**——$|r_t|$ 近似恒定，避免异方差。
2. **复杂回报分布** → 用 quantile encoding（10+ letters）或 sigma encoding。
3. **熵估计用 Kontoyiannis (Gao 2008 改进版)**——比 plug-in 在短消息上更稳健。
4. **窗口大小**：$N \approx n + (\log_2 n)^2$ 平衡 bias/variance。
5. **检验估计器质量**：跑 IID 高斯样本，看是否复现 $H \approx 1.42$。
6. **特征工程方向**：
   - 全市场熵 → 市场效率指标
   - 组合熵 → 集中度风险
   - OI 的熵 → adverse selection 风险
   - SR 的不同 $q$-多样性 → 风险分散度
7. **关注短消息的偏差**：消息长 < 200 时考虑重复消息或反向消息。
8. **互信息 (MI) 替代相关性**——能捕获非线性关系。

## 十、关联阅读

- 上游：长记忆保留的分数阶差分 → [[chapter-05-fractional-differentiation]]
- 上游：dollar/volume bars 为 binary encoding 提供齐次基础 → [[chapter-02-financial-data-structures]]
- 平行：结构性突变检测（另一类信息度量）→ [[chapter-17-structural-breaks]]
- 平行：市场微结构 PIN/VPIN 完整介绍 → [[chapter-19-microstructural-features]]
- 应用：组合集中度可融入 HRP 的诊断 → [[chapter-16-asset-allocation]]
- 应用：CUSUM 中 $S_t$ 可换为熵 → [[chapter-02-financial-data-structures]]
- 参考：MacKay (2003) Information Theory, Inference, and Learning Algorithms
- 参考：Gao, Kontoyiannis, Bienestock (2008) "Estimating the entropy of binary time series"
- 参考：Meucci (2009) "Managing diversification"
