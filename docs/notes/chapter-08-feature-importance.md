# 第 8 章：特征重要性（Feature Importance）

> 作者亲笔写下的 **"Marcos 回测第一定律"**：**Backtesting is not a research tool. Feature importance is.** 本章给出三种互补的特征重要性方法（MDI / MDA / SFI），以及如何用 PCA 正交化来抵御替代效应。

## 一、核心论点：用回测做研究是科学欺诈

金融研究中最普遍的错误：跑数据 → 训模型 → 回测 → 不好看再跑一次 → 直到出现一个"漂亮的回测"。这就是 walk-forward OOS 也救不了的"同一数据集反复测试"。

- 美国统计协会的伦理准则（2016 Discussion #4）明确把这种做法定为科学欺诈。
- 在 5% 显著性水平下，**只需大约 20 次迭代就能"发现"一个假阳性策略**。

正确路径：**先做特征重要性分析，再考虑回测**。理解特征贡献能：
- 加入更多强化信号的特征
- 删除只贡献噪声的特征
- 打开"黑盒"——看到算法到底在用什么模式
- 探究特征在不同环境/资产/时间下的稳健性
- "猎手不会盲目吃掉聪明猎犬叼回来的所有东西"

## 二、Substitution Effects（替代效应）

类似计量经济学里的"多重共线性"——一个特征的重要性被另一个相关特征"稀释"。
- 解决线性替代效应的标准方法：**先 PCA 正交化**，再做特征重要性分析。

按是否受替代效应影响，可以把方法分为两类。

## 三、受替代效应影响的方法

### 3.1 MDI（Mean Decrease Impurity）★ RF 默认

**性质**：快、**样本内**（IS）、仅适用于树模型。

**原理**：在每棵决策树的每个节点，被选中的特征通过分裂减少了不纯度。把所有节点的不纯度减少量按特征聚合，再在森林里求平均。

**实施要点**：
1. **避免 masking**：sklearn 里设 `max_features=int(1)` —— 每层只看一个随机特征，保证每个特征都有机会出场。
2. **0 值要替换为 NaN**：因为 0 只意味着"这个特征从没被随机选中"，不是"没有重要性"。
3. 重要性总和 = 1，每个 ∈ [0, 1]。
4. 替代效应稀释——两个完全相同的特征会平分重要性（各 0.5）。
5. Strobl et al. (2007) 实验发现 MDI 偏袒某些预测变量；White & Liu (1994) 解释这是流行不纯度函数对多类别预测变量的不公平加成。

```python
def featImpMDI(fit, featNames):
    df0 = {i: tree.feature_importances_ for i, tree in enumerate(fit.estimators_)}
    df0 = pd.DataFrame.from_dict(df0, orient='index')
    df0.columns = featNames
    df0 = df0.replace(0, np.nan)  # because max_features=1
    imp = pd.concat({'mean': df0.mean(),
                     'std': df0.std() * df0.shape[0] ** -.5}, axis=1)
    imp /= imp['mean'].sum()
    return imp
```

### 3.2 MDA（Mean Decrease Accuracy）★ 通用

**性质**：慢、**样本外**（OOS）、适用于任何分类器。也叫 **Permutation Importance**。

**原理**：
1. 训练分类器，得到 OOS baseline 得分。
2. 把特征矩阵的某一列**随机洗牌**，重新计算 OOS 得分。
3. 重要性 = 洗牌前后的得分差距占可达最大改进的比例。

**实施要点**：
1. 评分函数不限于 accuracy（meta-labeling 场景下 F1 更合适）。
2. 同样受替代效应影响——两个完全相同的特征会同时被判为"无关紧要"（洗任一个对方都能补位）。
3. 与 MDI 不同：MDA 可能判断**所有特征都不重要**（OOS 真不准就是真不准）。
4. **CV 必须 purged + embargoed**（第 7 章）。
5. 改进可能是**负的** —— 意味着该特征实际损害预测力。

```python
def featImpMDA(clf, X, y, cv, sample_weight, t1, pctEmbargo, scoring='neg_log_loss'):
    cvGen = PurgedKFold(n_splits=cv, t1=t1, pctEmbargo=pctEmbargo)
    scr0, scr1 = pd.Series(), pd.DataFrame(columns=X.columns)
    for i, (train, test) in enumerate(cvGen.split(X=X)):
        X0, y0, w0 = X.iloc[train,:], y.iloc[train], sample_weight.iloc[train]
        X1, y1, w1 = X.iloc[test,:], y.iloc[test], sample_weight.iloc[test]
        fit = clf.fit(X=X0, y=y0, sample_weight=w0.values)
        # baseline OOS score
        if scoring == 'neg_log_loss':
            prob = fit.predict_proba(X1)
            scr0.loc[i] = -log_loss(y1, prob, sample_weight=w1.values, labels=clf.classes_)
        else:
            pred = fit.predict(X1); scr0.loc[i] = accuracy_score(y1, pred, sample_weight=w1.values)
        # permutation per feature
        for j in X.columns:
            X1_ = X1.copy(deep=True)
            np.random.shuffle(X1_[j].values)
            if scoring == 'neg_log_loss':
                prob = fit.predict_proba(X1_)
                scr1.loc[i,j] = -log_loss(y1, prob, sample_weight=w1.values, labels=clf.classes_)
            else:
                pred = fit.predict(X1_); scr1.loc[i,j] = accuracy_score(y1, pred, sample_weight=w1.values)
    imp = (-scr1).add(scr0, axis=0)
    if scoring == 'neg_log_loss': imp = imp / -scr1
    else: imp = imp / (1. - scr1)
    return pd.concat({'mean': imp.mean(), 'std': imp.std() * imp.shape[0]**-.5}, axis=1), scr0.mean()
```

## 四、不受替代效应影响的方法

### 4.1 SFI（Single Feature Importance）

**性质**：横截面、OOS、适用于任何分类器。

**原理**：每个特征单独训练一个分类器，OOS 得分作为重要性。

**优点**：完全没有替代效应——每次只看一个特征。

**缺点**：丢失了**联合效应（joint effects）和层次重要性**。
- 例：特征 B 单独无用，但与 A 联合时极有用。
- 例：特征 B 在解释 A 的分裂时有用，单独却不准。
- 子集枚举不可行（组合爆炸）。

**用途**：作为 MDI / MDA 的补充——三者会因不同问题而漏报不同特征。

### 4.2 Orthogonal Features（PCA 正交化）

对受替代效应影响的 MDI / MDA 的补救：先把特征 PCA 正交化。

步骤：
1. 标准化：$Z_{t,n} = \sigma_n^{-1}(X_{t,n} - \mu_n)$
2. 谱分解：$Z'Z W = W\Lambda$，$\Lambda$ 按降序排列
3. 正交特征：$P = ZW$，验证 $P'P = \Lambda$

为什么在 $Z$ 而非 $X$ 上做：
- **中心化**：保证第一主成分对齐主方向（等价于线性回归加截距）
- **重标度**：让 PCA 关注**相关性**而非**方差**（否则高方差列会主导）

```python
def get_eVec(dot, varThres):
    eVal, eVec = np.linalg.eigh(dot)
    idx = eVal.argsort()[::-1]
    eVal, eVec = eVal[idx], eVec[:,idx]
    eVal = pd.Series(eVal, index=['PC_'+str(i+1) for i in range(eVal.shape[0])])
    eVec = pd.DataFrame(eVec, index=dot.index, columns=eVal.index)
    cumVar = eVal.cumsum() / eVal.sum()
    dim = cumVar.values.searchsorted(varThres)
    return eVal.iloc[:dim+1], eVec.iloc[:,:dim+1]

def orthoFeats(dfX, varThres=.95):
    dfZ = dfX.sub(dfX.mean(), axis=1).div(dfX.std(), axis=1)
    dot = pd.DataFrame(np.dot(dfZ.T, dfZ), index=dfX.columns, columns=dfX.columns)
    eVal, eVec = get_eVec(dot, varThres)
    return np.dot(dfZ, eVec)
```

**额外好处**：
1. 降维（删掉小特征值对应列），加速 ML 收敛
2. 分析对象是"被设计用来解释数据结构的特征"
3. **过拟合诊断**：PCA 是无监督的，没看过标签。如果 MDI/MDA/SFI（用了标签）选出的重要特征**恰好就是 PCA 选出的主成分**，这是有力证据说明 ML 不是在 fit 噪声。
   - 推荐用**加权 Kendall's tau**（强调高重要性特征的一致性）量化这种一致性，越接近 1 越好。
   - 作者书里给的例子：相关系数 0.8491（p < 1E-150），加权 Kendall's tau = 0.8206。

```python
from scipy.stats import weightedtau
weightedtau(featImp, pcRank**-1.)[0]
```

## 五、Parallelized vs Stacked Feature Importance

两种跨标的研究方式：

### 5.1 Parallelized（并行）
对每个标的 $i$，独立训练 $(X_i, y_i)$，得到重要性 $\lambda_{i,j,k}$，最后聚合得到 $\Lambda_{j,k}$。
- 优：可并行，计算快
- 劣：替代效应让同一特征在不同标的间"换排名"，提升方差。投资宇宙足够大时这个问题会被均值平滑掉。

### 5.2 Stacked（堆叠）★ 作者推荐
把所有 $(\tilde X_i, y_i)$ 标准化后纵向堆成一个超大数据集 $(X, y)$，分类器在所有标的上同时学。

**优点**：
1. 训练集远大于并行方案
2. 重要性直接得出，不需要加权
3. 结论更通用，受单标的异常值影响小
4. **没有跨标的均值平滑** → 替代效应不会稀释分数

**缺点**：吃内存吃资源 → 配合第 20-22 章的 HPC 技术。

作者表态：**他在所有可以跨标的训练的场景下都偏好 Stacking**（不仅是特征重要性，模型预测也是）。

## 六、合成数据实验

构造三类特征验证三种方法：
- **Informative**（10 个）：真正决定标签的特征
- **Redundant**（10 个）：informative 的随机线性组合 → 制造替代效应
- **Noise**（20 个）：与标签无关

10000 观测，结果：

| 方法 | 表现 | 失误 |
|---|---|---|
| MDI | 几乎全部 informative + redundant 都进入显著区间 | R_5 略低于阈值 |
| MDA | 与 MDI 一致，所有 informative + redundant 高于 noise | R_6 失利（替代效应）；mean 的 std 较高 |
| SFI | 各 informative 单独看排名低于 noise | I_6/I_2/I_9/I_1/I_3/R_5 因联合效应而失利 |

结论：三种方法**互补**——每种都有自己的盲点。

## 七、本章实操要点

1. **不要把回测当研究工具**——研究阶段用特征重要性。
2. **三种方法同时跑**：MDI（快，IS）+ MDA（OOS）+ SFI（横截面）。互补使用。
3. **MDI 设 `max_features=1`，0 值替换为 NaN**。
4. **MDA 必须用 PurgedKFold + embargo**。
5. **想破替代效应** → 先 PCA 正交化再做特征重要性。
6. **过拟合诊断技巧**：算"特征重要性排名"与"PCA 排名"的加权 Kendall's tau，越接近 1 越证明 ML 没在 fit 噪声。
7. **跨标的研究优先用 Stacked Approach**（内存够的话）。

## 八、关联阅读

- 上游：CV 框架 → [[chapter-07-cross-validation]]
- 平行：集成方法 → [[chapter-06-ensemble-methods]]
- 下游：超参调优 → [[chapter-09-hyperparameter-tuning]]
- 关键：回测过拟合的危险 → [[chapter-11-dangers-of-backtesting]]
- 进阶：合成数据回测 → [[chapter-13-synthetic-backtesting]]
- HPC：堆叠数据集的并行计算 → [[chapter-20-multiprocessing]]
