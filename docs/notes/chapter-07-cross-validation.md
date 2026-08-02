# 第 7 章：金融中的交叉验证（Cross-Validation in Finance）

> **标准 K-Fold CV 在金融场景下是失败的**。CV 不仅检测不到过拟合，还会**通过超参调优制造过拟合**。本章给出 **Purged K-Fold CV + Embargo** 的修正方案。

## 一、CV 的目的与标准做法

CV 的目的是估计 ML 算法在**未见数据上的泛化误差**，防止过拟合。如果用训练集自我评估，等同于"有损压缩"——保真度极高，预测力为零。

标准 K-Fold CV：
1. 把数据集均分为 $k$ 份
2. 第 $i$ 轮：用除 $i$ 之外的所有子集训练，在子集 $i$ 上测试
3. 输出 $k$ 个验证指标

金融场景下 CV 主要用于两个地方：
- **模型开发**（如超参调优）— 本章主题
- **回测** — 第 10-16 章主题

## 二、为什么 K-Fold CV 在金融失败？

两个根本原因：

### 2.1 观测不 IID（本章重点）
**信息泄漏**（Leakage）：训练集里包含了测试集里也出现的信息。

考虑一个序列相关特征 $X$ 和重叠区间标签 $Y$：
- 序列相关：$X_t \approx X_{t+1}$
- 重叠数据点：$Y_t \approx Y_{t+1}$

如果把 $t$ 和 $t+1$ 分到不同集合 → 信息泄漏。即使 $X$ 是无关特征，分类器训练时见过 $(X_t, Y_t)$，预测 $E[Y_{t+1}|X_{t+1}]$ 时也容易 "蒙对"。

**最危险的不是泄漏放大真信号，而是泄漏把无关特征伪装成有预测力的——制造假发现。**

减少泄漏的两条路径：
1. 从训练集删掉 $Y_i$ 与 $Y_j$（$j \in $ 测试集）共享信息的样本 → **Purging**
2. 防过拟合（早停、bagging + 控制冗余），让即使有泄漏分类器也无法利用

### 2.2 测试集被多次使用（第 11-13 章重点）
多次测试和选择偏差。

**关键澄清**：$X_i \approx X_j$ 或 $Y_i \approx Y_j$ 都**不足以**算泄漏。必须是 $(X_i, Y_i) \approx (X_j, Y_j)$ 才算泄漏。

## 三、解决方案：Purged K-Fold CV

### 3.1 Purging（清理训练集）

测试样本 $j$ 的标签 $Y_j$ 基于信息集 $\Phi_j$。从训练集中删除任何 $Y_i$ 基于的 $\Phi_i$ 与 $\Phi_j$ 有交集的样本。

具体地，对 triple-barrier 类标签 $Y_i = f[[t_{i,0}, t_{i,1}]]$，与 $Y_j = f[[t_{j,0}, t_{j,1}]]$ 重叠的三个充分条件：
1. $t_{j,0} \leq t_{i,0} \leq t_{j,1}$（训练样本起点落在测试区间内）
2. $t_{j,0} \leq t_{i,1} \leq t_{j,1}$（训练样本终点落在测试区间内）
3. $t_{i,0} \leq t_{j,0} \leq t_{j,1} \leq t_{i,1}$（训练样本包住测试样本）

```python
def getTrainTimes(t1, testTimes):
    trn = t1.copy(deep=True)
    for i, j in testTimes.iteritems():
        df0 = trn[(i <= trn.index) & (trn.index <= j)].index  # train starts within test
        df1 = trn[(i <= trn) & (trn <= j)].index              # train ends within test
        df2 = trn[(trn.index <= i) & (j <= trn)].index        # train envelops test
        trn = trn.drop(df0.union(df1).union(df2))
    return trn
```

**诊断技巧**：若有泄漏，随 $k \to T$ 性能会持续提升（因为更多重叠样本进训练集）。Purge 之后性能改善到某个 $k^*$ 就饱和——证明回测没在吃泄漏。

### 3.2 Embargo（禁运区）

Purge 之后仍可能有泄漏：金融特征常含 ARMA 类序列相关，**紧跟测试集结束之后的训练样本**可能携带测试信息（因为标签往后延展）。

只需对**测试集之后**的训练样本设禁运区，长度 $h$。
- 不需要对**测试集之前**的训练样本禁运（因为这些样本的信息在测试时刻已可用）。
- 实现：把 $Y_j$ 改成 $f[[t_{j,0}, t_{j,1} + h]]$，再 purge。
- 实证：$h \approx 0.01T$ 通常足够。

```python
def getEmbargoTimes(times, pctEmbargo):
    step = int(times.shape[0] * pctEmbargo)
    if step == 0:
        mbrg = pd.Series(times, index=times)
    else:
        mbrg = pd.Series(times[step:], index=times[:-step])
        mbrg = mbrg.append(pd.Series(times[-1], index=times[-step:]))
    return mbrg
```

### 3.3 PurgedKFold 类

继承 sklearn 的 `_BaseKFold`，重写 `split`：

```python
class PurgedKFold(_BaseKFold):
    def __init__(self, n_splits=3, t1=None, pctEmbargo=0.):
        if not isinstance(t1, pd.Series):
            raise ValueError('Label Through Dates must be a pd.Series')
        super().__init__(n_splits, shuffle=False, random_state=None)
        self.t1 = t1
        self.pctEmbargo = pctEmbargo
    
    def split(self, X, y=None, groups=None):
        indices = np.arange(X.shape[0])
        mbrg = int(X.shape[0] * self.pctEmbargo)
        test_starts = [(i[0], i[-1]+1) for i in np.array_split(np.arange(X.shape[0]), self.n_splits)]
        for i, j in test_starts:
            t0 = self.t1.index[i]
            test_indices = indices[i:j]
            maxT1Idx = self.t1.index.searchsorted(self.t1[test_indices].max())
            train_indices = self.t1.index.searchsorted(self.t1[self.t1 <= t0].index)
            if maxT1Idx < X.shape[0]:
                train_indices = np.concatenate((train_indices, indices[maxT1Idx+mbrg:]))
            yield train_indices, test_indices
```

**重要假设**：`shuffle=False` —— 金融场景下绝不能 shuffle。

## 四、sklearn 的两个 CV bug

作者直接在书里点名：

1. **Issue #6231**：评分函数不识别 `classes_`（因为 sklearn 用 numpy array 而非 pandas series）。
2. **Issue #9144**：`cross_val_score` 把 `sample_weight` 传给 `fit`，但**不传给 `log_loss`** —— 结果不一致。

**解决方案**：不要用 `cross_val_score`，自己写：

```python
def cvScore(clf, X, y, sample_weight, scoring='neg_log_loss',
            t1=None, cv=None, cvGen=None, pctEmbargo=None):
    if cvGen is None:
        cvGen = PurgedKFold(n_splits=cv, t1=t1, pctEmbargo=pctEmbargo)
    score = []
    for train, test in cvGen.split(X=X):
        fit = clf.fit(X=X.iloc[train, :], y=y.iloc[train],
                      sample_weight=sample_weight.iloc[train].values)
        if scoring == 'neg_log_loss':
            prob = fit.predict_proba(X.iloc[test, :])
            score_ = -log_loss(y.iloc[test], prob,
                               sample_weight=sample_weight.iloc[test].values,
                               labels=clf.classes_)
        else:
            pred = fit.predict(X.iloc[test, :])
            score_ = accuracy_score(y.iloc[test], pred,
                                    sample_weight=sample_weight.iloc[test].values)
        score.append(score_)
    return np.array(score)
```

## 五、Shuffle 在金融里为什么是大坑

Shuffle 的本意是打散数据集，让训练 / 测试集分布相似。但在金融里：

- 价格、特征都序列相关 → shuffle 之后，几乎每个测试样本的"近邻"都在训练集里
- 这就是**最严重形式的信息泄漏**
- 不 shuffle 的 CV 与 shuffle 的 CV 在金融数据上**结果会差异巨大**——shuffle 版本严重高估性能

**所以**：`shuffle=False` 不仅是建议，是硬性要求。

## 六、本章实操要点

1. **金融场景下 shuffle=False 是硬性要求**。
2. **永远用 PurgedKFold 替代 KFold**（包括超参调优、特征选择、回测）。
3. **加 1% 的 embargo**：`pctEmbargo=0.01`。
4. **不要信 sklearn 的 `cross_val_score`**，自己用 `cvScore`。
5. **诊断泄漏**：增大 $k$ 看性能是否持续提升——是 → 仍有泄漏。
6. 即便如此，**CV 失败的第二大原因（选择偏差）本章未解决**，要看第 11-13 章。

## 七、关联阅读

- 上游：标签的重叠区间问题 → [[chapter-04-sample-weights]]
- 平行：Bagging / RF 的样本设计 → [[chapter-06-ensemble-methods]]
- 后续：用 PurgedKFold 做特征重要性 → [[chapter-08-feature-importance]]
- 后续：用 PurgedKFold 做超参调优 → [[chapter-09-hyperparameter-tuning]]
- 后续：用 PurgedKFold 做回测 → [[chapter-12-backtesting-cv]]
- 选择偏差与多重检验问题 → [[chapter-11-dangers-of-backtesting]]
