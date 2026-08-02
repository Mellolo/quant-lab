# 第 4 章：样本权重（Sample Weights）

> 解决金融 ML 的另一个核心痛点：**标签不是 IID** —— 重叠的预测区间会让样本互相"污染"。本章给出唯一性度量、Sequential Bootstrap、按收益归因加权、时间衰减、类别权重等一整套修补方案。

## 一、问题：重叠区间导致的非 IID

第 3 章产出的标签 $y_i$ 是区间 $[t_{i,0}, t_{i,1}]$ 上路径的函数。若 $t_{i,1} > t_{j,0}$（$i < j$），那么 $y_i$ 与 $y_j$ 都依赖于公共收益 $r_{t_{j,0}, \min\{t_{i,1}, t_{j,1}\}}$ —— **样本不再 IID**。

能不能限制 $t_{i,1} \leq t_{i+1,0}$ 来消除重叠？理论上可以，但代价不可接受：
- 如果你想研究月度结果，采样频率最多每月一次，等于丢掉日内信息。
- 对路径依赖标注（如 triple-barrier），采样频率会被第一个屏障触碰决定，几乎不可控。

作者用一个生动的比喻：常规 ML 是给 N 个病人各抽一管血测胆固醇；金融 ML 是**别人不小心把每管血都倒了一些到右边相邻 9 管里**，你必须在不知道精确胆固醇值的情况下，找出与高胆固醇相关的特征。而且金融场景里"溢出模式"还是非确定性的、未知的。

## 二、并发标签数（Number of Concurrent Labels）

第一步：对每个时间点 $t$，建立指示矩阵 $\{1_{t,i}\}$：

$$
1_{t,i} = \begin{cases} 1 & [t_{i,0}, t_{i,1}] \text{ 与 } [t-1, t] \text{ 重叠} \\ 0 & \text{否则} \end{cases}
$$

第二步：每个时刻并发标签数 $c_t = \sum_{i=1}^{I} 1_{t,i}$。

```python
def mpNumCoEvents(closeIdx, t1, molecule):
    t1 = t1.fillna(closeIdx[-1])
    t1 = t1[t1 >= molecule[0]]
    t1 = t1.loc[:t1[molecule].max()]
    iloc = closeIdx.searchsorted(np.array([t1.index[0], t1.max()]))
    count = pd.Series(0, index=closeIdx[iloc[0]:iloc[1]+1])
    for tIn, tOut in t1.iteritems():
        count.loc[tIn:tOut] += 1.
    return count.loc[molecule[0]:t1[molecule].max()]
```

注意：**unclosed events（还没结束的事件）也要算进去**，因为它们会影响其他事件的权重计算。

## 三、标签的平均唯一性（Average Uniqueness）

时刻 $t$ 上标签 $i$ 的"唯一性"：
$$
u_{t,i} = \frac{1_{t,i}}{c_t}
$$

标签 $i$ 整个生命周期的平均唯一性：
$$
\bar u_i = \frac{\sum_t u_{t,i}}{\sum_t 1_{t,i}}
$$

这等价于 $c_t$ 沿生命周期的**调和平均数的倒数**。$\bar u_i \in [0, 1]$ —— 1 表示完全独立，越小表示与其他标签纠缠越深。

**为什么没有信息泄漏**：$\bar u_i$ 用的是 **训练阶段** 的标签信息，不用于预测，所以不会泄露未来。

## 四、Bagging 与 Uniqueness：Sequential Bootstrap

### 4.1 标准 Bootstrap 在非 IID 下的失败

对 $I$ 个样本做 $I$ 次有放回抽样，某样本一次都不被抽中的概率：
$$
(1 - I^{-1})^I \xrightarrow{I \to \infty} e^{-1}
$$
所以期望抽出的不重复样本数 ≈ $(1 - e^{-1})I \approx \frac{2}{3} I$。

但若**实际最大非重叠数** $K \leq I$，正确比例应该是 $1 - e^{-K/I}$，**而不是 $1 - e^{-1}$**。

后果：当 $I^{-1}\sum \bar u_i \ll 1$（高重叠），标准 Bootstrap 严重过采样，in-bag 样本互为冗余，**out-of-bag 与 in-bag 太像，OOB 准确率被严重高估**。在随机森林里，所有树几乎都是同一颗过拟合树的副本。

### 4.2 简单修补：限制 max_samples
sklearn 的 `BaggingClassifier(max_samples=out['tW'].mean())` 可以把抽样比例压到 uniqueness 均值附近。RandomForest 没有 `max_samples` 参数，作者建议改用 BaggingClassifier 套一堆决策树。

### 4.3 Sequential Bootstrap ★（最佳解）

核心：**每抽一个样本就更新一次概率，让"与已抽样本重叠多的"再被抽中的概率下降**。

初始 $\delta_i^{(1)} = 1/I$。

抽出第一个样本 $\varphi^{(1)} = \{i\}$ 后，候选 $j$ 的瞬时唯一性：
$$
u_{t,j}^{(2)} = \frac{1_{t,j}}{1 + \sum_{k \in \varphi^{(1)}} 1_{t,k}}
$$
平均唯一性 $\bar u_j^{(2)}$，归一化为概率：
$$
\delta_j^{(2)} = \frac{\bar u_j^{(2)}}{\sum_k \bar u_k^{(2)}}
$$
继续抽，每次更新，共抽 $I$ 次。**仍然允许重复**，但重复的概率随次数递减。

### 4.4 数值例子

3 个标签，区间 [0,3] [2,4] [4,6]，指示矩阵：
$$
\begin{pmatrix}
1 & 0 & 0 \\
1 & 0 & 0 \\
1 & 1 & 0 \\
0 & 1 & 0 \\
0 & 0 & 1 \\
0 & 0 & 1
\end{pmatrix}
$$
假设第一抽抽到 2。则更新后：
- $\bar u_1^{(2)} = \frac{1}{3}(1 + 1 + 1/2) = 5/6$
- $\bar u_2^{(2)} = \frac{1}{2}(1/2 + 1/2) = 1/2$ （已选 2，唯一性下降）
- $\bar u_3^{(2)} = 1$ （和 2 无重叠）

归一化 $\delta^{(2)} = (5/14, 3/14, 6/14)$。
- 第一抽中过的 2 拿到最低概率 3/14。
- 与 2 无重叠的 3 拿到最高概率 6/14。

### 4.5 实现要点

```python
def getIndMatrix(barIx, t1):
    indM = pd.DataFrame(0, index=barIx, columns=range(t1.shape[0]))
    for i, (t0, t1_) in enumerate(t1.iteritems()):
        indM.loc[t0:t1_, i] = 1.
    return indM

def getAvgUniqueness(indM):
    c = indM.sum(axis=1)
    u = indM.div(c, axis=0)
    return u[u > 0].mean()

def seqBootstrap(indM, sLength=None):
    if sLength is None: sLength = indM.shape[1]
    phi = []
    while len(phi) < sLength:
        avgU = pd.Series()
        for i in indM:
            indM_ = indM[phi + [i]]
            avgU.loc[i] = getAvgUniqueness(indM_).iloc[-1]
        prob = avgU / avgU.sum()
        phi += [np.random.choice(indM.columns, p=prob)]
    return phi
```

### 4.6 Monte Carlo 验证

`numObs=10, numBars=100, maxH=5`，$10^6$ 次迭代：
- 标准 Bootstrap 唯一性中位数 ≈ 0.6
- Sequential Bootstrap 唯一性中位数 ≈ 0.7

ANOVA 检验 p 值 ≈ 0 —— 统计上显著更接近 IID。

注：24 核服务器跑 6 小时；单核要 6 天。

## 五、Return Attribution（按收益绝对值加权）

仅按 uniqueness 加权还不够：**带来大幅收益的标签应当比微小收益的标签更重要**。

定义：
$$
\tilde w_i = \left| \sum_{t=t_{i,0}}^{t_{i,1}} \frac{r_{t-1,t}}{c_t} \right|
$$
归一化（让总和为 $I$，因为 sklearn 默认权重为 1）：
$$
w_i = \tilde w_i \cdot I \cdot \left( \sum_j \tilde w_j \right)^{-1}
$$

直觉：每个时刻的收益按当时的并发标签数 $c_t$ 摊分给各事件，再沿事件生命周期累加。**用 log-return** 保证可加。

```python
def mpSampleW(t1, numCoEvents, close, molecule):
    ret = np.log(close).diff()
    wght = pd.Series(index=molecule)
    for tIn, tOut in t1.loc[wght.index].iteritems():
        wght.loc[tIn] = (ret.loc[tIn:tOut] / numCoEvents.loc[tIn:tOut]).sum()
    return wght.abs()
```

**陷阱**：如果有"neutral / 收益小于阈值"的标签，这种方法会**反向**给它们高权重——所以作者强烈建议**抛弃 neutral 标签**，用 "低置信度的 ±1 预测" 来表达中性。

## 六、Time Decay（时间衰减）

市场是自适应系统（Lo, 2017），旧样本的相关性会下降。

定义衰减因子 $d[x] \geq 0$，$x \in [0, \sum_i \bar u_i]$：
- 最新样本 $d[\sum_i \bar u_i] = 1$
- 其他位置按 $c \in (-1, 1]$ 参数线性衰减

分段线性 $d = \max\{0, a + bx\}$，参数：
- $c \in [0, 1]$：$d[1] = c$，所有样本都有正权重（最老的获得 $c$）
- $c \in (-1, 0)$：$d[-c \sum_i \bar u_i] = 0$，**最老 $cT$ 的样本权重归零**（等于被遗忘）

```python
def getTimeDecay(tW, clfLastW=1.):
    clfW = tW.sort_index().cumsum()
    if clfLastW >= 0:
        slope = (1. - clfLastW) / clfW.iloc[-1]
    else:
        slope = 1. / ((clfLastW + 1) * clfW.iloc[-1])
    const = 1. - slope * clfW.iloc[-1]
    clfW = const + slope * clfW
    clfW[clfW < 0] = 0
    return clfW
```

**关键设计选择**：衰减按"**累计唯一性**"而不是"按时间"进行。这避免了在冗余观察密集时权重衰减得太快。

四类情形：
- $c = 1$：无衰减
- $0 < c < 1$：线性衰减，全员有正权重
- $c = 0$：最老样本权重线性收敛到 0
- $c < 0$：最老的 $cT$ 比例样本被"擦除"

$c > 1$ 也合法——可以让权重随时间增大（很少用）。

## 七、Class Weights（类别权重）

类别极不平衡时（如 flash crash 等罕见事件），不加 class weight，分类器会把它们当 outlier 处理。

sklearn 用法：`class_weight[j]` 替代默认的 1。若 class_weight 不归一化到 $J$，效果等价于改变正则强度。

金融 ML 标签通常 $\{-1, 1\}$（中性由 "概率略高于 0.5 + 中性阈值" 推断），**没理由偏袒一侧**——默认 `class_weight='balanced'`。

Bagging 场景可用 `class_weight='balanced_subsample'` —— 平衡只在 in-bag bootstrap 样本上做，而不是整个数据集。注意 sklearn issue #4324。

## 八、本章实操要点

1. **承认非 IID**：金融场景几乎所有标签都重叠，IID 假设是错的。
2. **算 uniqueness**：`mpNumCoEvents` → `mpSampleTW`，得到每个样本的 $\bar u_i$。
3. **用 Sequential Bootstrap 替代标准 Bootstrap**：尤其是当 BaggingClassifier 或 RandomForest 时；至少把 `max_samples` 设到平均 uniqueness。
4. **按收益归因加权**：`mpSampleW` —— 同时考虑唯一性和收益规模。
5. **加时间衰减**：用累计 uniqueness 衡量"老化"，而不是按日历。
6. **类别不平衡** → `class_weight='balanced'` 或 `'balanced_subsample'`。
7. **丢掉 "neutral" 标签**——它会让"按绝对收益加权"反向出错。

## 九、关联阅读

- 上游：标签来源 → [[chapter-03-labeling]]
- 下游：Bagging / Random Forest 在金融中的正确做法 → [[chapter-06-ensemble-methods]]
- 重叠区间也意味着 CV 必须 purge → [[chapter-07-cross-validation]]
- 多核并行计算（mpPandasObj） → [[chapter-20-multiprocessing]]
- 自适应市场假说背景：Lo (2017) Adaptive Markets
