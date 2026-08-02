# 第 6 章：集成方法（Ensemble Methods）

> 本章重点：在**金融场景**下为什么 Bagging 和 Boosting 会失败，以及怎么修。作者默认你已经知道 RF / AdaBoost 是什么——这章讲的是"为什么有效"和"怎样避免常见误用"。

## 一、误差的三个来源

ML 模型误差 = Bias + Variance + Noise：

$$
E[(y_i - \hat f[x_i])^2] = \underbrace{(E[\hat f[x_i] - f[x_i]])^2}_{\text{bias}} + \underbrace{V[\hat f[x_i]]}_{\text{variance}} + \underbrace{\sigma_\varepsilon^2}_{\text{noise}}
$$

- **Bias（偏差）**：用了不现实的假设，欠拟合。
- **Variance（方差）**：对训练集小变动太敏感，过拟合，把噪声当信号。
- **Noise（噪声）**：不可约误差，没有任何模型能解释。

集成方法的目标是**降 bias 或降 variance**，从而把弱学习器组成强学习器。

## 二、Bagging（Bootstrap Aggregation）

### 2.1 流程
1. 有放回抽样产生 $N$ 个训练集
2. 在每个集上独立训练一个基学习器（可并行）
3. 集成预测 = $N$ 个学习器的简单平均（分类用 majority voting 或平均概率）

### 2.2 方差降低公式（核心）

$$
V\left[\frac{1}{N}\sum_{i=1}^{N} \varphi_i[c]\right] = \bar\sigma^2 \left(\bar\rho + \frac{1 - \bar\rho}{N}\right)
$$

其中：
- $\bar\sigma$：单个学习器预测的平均标准差
- $\bar\rho$：学习器之间预测的平均相关系数
- $N$：基学习器数量

**关键结论**：当 $\bar\rho \to 1$，集成方差 $\to \bar\sigma^2$ —— **bagging 失效**。Bagging 起效的核心条件是 $\bar\rho < 1$。

这就是第 4 章 Sequential Bootstrap 的根本动机——降低 $\bar\rho$。

### 2.3 准确率提升

$N$ 个独立分类器在 $k$ 类问题上做 majority voting。基分类器准确率 $p$，则集成准确率：

$$
P[X > N/k] = 1 - \sum_{i=0}^{\lfloor N/k \rfloor} \binom{N}{i} p^i (1-p)^{N-i}
$$

只要 $p > 1/k$ 且 $N$ 充分大（$N > p(p - 1/k)^{-2}$），集成准确率 > 个体平均准确率。

**重要警告**：若 $p \leq 1/k$（基分类器比瞎猜还差），bagging 也没用——只是降方差，不能补 bias。所以 **bagging 主要降方差，不能降 bias**。

参考：Condorcet 陪审团定理（同一逻辑应用在政治学投票决策上）。

### 2.4 金融场景的观测冗余问题

第 4 章已讨论。两大不良后果：

1. **样本几乎一样** → $\bar\rho \approx 1$ → 增大 $N$ 也救不了。
   - 例：标签是 $[t, t+100]$ 区间收益 → 每个 bagged 学习器最多只应抽 1% 的样本（不然样本高度重叠）。
   - 解法：`max_samples = out['tW'].mean()` 或用 Sequential Bootstrap。

2. **OOB 准确率被严重高估** → in-bag 与 out-of-bag 太像，所谓"OOB 测试"实质上还是 in-sample。
   - 解法：用 `StratifiedKFold(n_splits=k, shuffle=False)` 跑 CV，**忽略 OOB 结果**。$k$ 应取小（大了会让测试集与训练集太像）。

## 三、Random Forest（RF）

### 3.1 与 Bagging 的关系
RF = Bagging（决策树） + 在**每个节点分裂时只评估特征子集**，多出一层随机性来进一步降低 $\bar\rho$。

### 3.2 RF 的三个优势
- 降方差但不易过拟合（同样需要 $\bar\rho < 1$）
- 自带**特征重要性**评估（第 8 章详述）
- 自带 OOB 准确率估计（但金融场景下不可信）

但 RF **不一定降 bias**。

### 3.3 金融场景下 RF 过拟合的修补方案

RF 的 bootstrap 样本大小等于训练集大小，无法直接调小。可选解法：

1. **降低 `max_features`**：强制让树彼此差异化。
2. **早停**：`min_weight_fraction_leaf` 设高（如 5%），让 OOB 收敛到 OOS 准确率。
3. **BaggingClassifier 套 DecisionTree**：
   ```python
   clf = DecisionTreeClassifier(criterion='entropy', max_features='auto', class_weight='balanced')
   bc = BaggingClassifier(base_estimator=clf, n_estimators=1000, max_samples=avgU, max_features=1.)
   ```
4. **BaggingClassifier 套 RandomForest(n_estimators=1, bootstrap=False)**：
   ```python
   clf = RandomForestClassifier(n_estimators=1, criterion='entropy',
                                bootstrap=False, class_weight='balanced_subsample')
   bc = BaggingClassifier(base_estimator=clf, n_estimators=1000, max_samples=avgU, max_features=1.)
   ```
5. **修改 RF 源码**，把标准 bootstrap 换成 sequential bootstrap。

### 3.4 实战 tips
- **在 PCA 后的特征上跑 RF**：特征空间对齐轴向后，树需要的层数变少，加速 + 降过拟合。
- **`class_weight='balanced_subsample'`**：处理少数类别。

## 四、Boosting

### 4.1 流程（AdaBoost 为例）
1. 按当前样本权重（初始均匀）有放回抽样
2. 训一个基学习器
3. 准确率超过阈值（如二分类的 50%）就保留，否则丢弃
4. **错分样本权重上调，对分样本权重下调**
5. 重复直到 $N$ 个学习器
6. 集成预测 = $N$ 个学习器的加权平均（按个体准确率加权）

### 4.2 与 Bagging 的核心差别
- 基学习器**顺序**训练，不能并行
- 表现差的基学习器会被丢弃
- 每轮观测权重不同
- 集成是**加权**平均，不是简单平均

## 五、Bagging vs Boosting 在金融

| 维度 | Bagging | Boosting |
|---|---|---|
| 主要解决 | Variance（过拟合） | Bias（欠拟合）+ 部分 Variance |
| 副作用 | 不能降 bias | **更易过拟合** |
| 并行性 | 完全并行 | 顺序运行 |

**作者的金融场景偏好**：**Bagging 通常优于 Boosting**。理由：
- 金融数据信噪比低，过拟合远比欠拟合常见。
- Boosting 修 bias 的代价是增加过拟合风险，在金融里得不偿失。
- Bagging 可并行，工程友好。

## 六、Bagging for Scalability（一个聪明的应用）

SVM 这类算法在百万样本上根本跑不动。技巧：

1. **基学习器用 SVM**，但施加**严格的早停**：把 `max_iter` 调低（如 1E5），或把 `tol` 调高。
2. **外层用 BaggingClassifier 并行**多个这样"半生不熟"的 SVM。

每个基学习器方差大没关系——bagging 的方差降低足以抵消。结果是**把一个大顺序任务变成一群小并行任务**，能在巨型数据集上跑出快且稳健的估计。

同样适用于 RF（`max_depth` 早停）等其他算法。

## 七、本章实操要点

1. **金融场景默认选 Bagging，不选 Boosting**：过拟合风险高于欠拟合风险。
2. **Bagging 起效的关键是 $\bar\rho < 1$**：所以必须配合 Sequential Bootstrap（第 4 章）。
3. **千万别信 OOB 准确率**：金融场景下严重高估。改用 `StratifiedKFold(shuffle=False)`。
4. **`max_samples = avgUniqueness`** 是个不可省的设置。
5. **RF 的 `n_estimators` 要设大**（默认 10 太少）；**`min_weight_fraction_leaf`** 早停防过拟合。
6. **PCA + RF** 是个被忽视的速度/稳健性提升组合。
7. **SVM 等"不可扩展"算法** → 套 BaggingClassifier + 早停。

## 八、关联阅读

- 上游：样本权重与 Sequential Bootstrap → [[chapter-04-sample-weights]]
- 平行：金融 CV 必须 Purge + Embargo → [[chapter-07-cross-validation]]
- 配套：特征重要性 → [[chapter-08-feature-importance]]
- 超参调优 → [[chapter-09-hyperparameter-tuning]]
- 并行计算实现 → [[chapter-20-multiprocessing]]
