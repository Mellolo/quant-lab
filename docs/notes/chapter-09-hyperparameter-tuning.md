# 第 9 章：超参数调优与交叉验证（Hyper-Parameter Tuning with CV）

> 超参调优的本质是在 PurgedKFold 上做 Grid / Random Search，配上**正确的评分函数**。本章引入两个看似冷门但关键的工具：**Log-Uniform 分布**与**Neg Log-Loss 评分**。

## 一、动机

超参调优做不好，ML 会过拟合，实盘表现会让人失望。文献里大量关注"交叉验证调超参"——但金融场景下 CV 本身就难（第 7 章），不能照搬其他领域的做法。

## 二、Grid Search CV（带 Purge）

`GridSearchCV` 接受 CV generator 参数，所以**直接把 PurgedKFold 传进去**就能避免泄漏。

```python
def clfHyperFit(feat, lbl, t1, pipe_clf, param_grid, cv=3,
                bagging=[0, None, 1.], n_jobs=-1, pctEmbargo=0, **fit_params):
    if set(lbl.values) == {0, 1}:
        scoring = 'f1'  # meta-labeling
    else:
        scoring = 'neg_log_loss'  # symmetric

    inner_cv = PurgedKFold(n_splits=cv, t1=t1, pctEmbargo=pctEmbargo)
    gs = GridSearchCV(estimator=pipe_clf, param_grid=param_grid,
                      scoring=scoring, cv=inner_cv, n_jobs=n_jobs, iid=False)
    gs = gs.fit(feat, lbl, **fit_params).best_estimator_

    if bagging[1] > 0:
        gs = BaggingClassifier(base_estimator=MyPipeline(gs.steps),
                               n_estimators=int(bagging[0]),
                               max_samples=float(bagging[1]),
                               max_features=float(bagging[2]),
                               n_jobs=n_jobs)
        gs = gs.fit(feat, lbl,
                    sample_weight=fit_params[gs.base_estimator.steps[-1][0]+'__sample_weight'])
        gs = Pipeline([('bag', gs)])
    return gs
```

### 2.1 评分函数选择
- **Meta-labeling（标签 {0, 1}）** → 用 `'f1'`。理由：负例占绝大多数时，"全部预测 0"的分类器 accuracy / neg_log_loss 都很高，但 recall 为零、precision 未定义。F1 修正这种"虚高"。
- **其他情况** → `'neg_log_loss'`（不是 'accuracy'，理由见第 4 节）。

### 2.2 sklearn Pipeline 的 bug 与修补

sklearn 的 `Pipeline.fit` 不接受 `sample_weight` 参数，必须通过 `fit_params` 关键字传递。这个 bug 在 GitHub 上已被报告，但短期内不会修。绕开办法：继承一个新类。

```python
class MyPipeline(Pipeline):
    def fit(self, X, y, sample_weight=None, **fit_params):
        if sample_weight is not None:
            fit_params[self.steps[-1][0]+'__sample_weight'] = sample_weight
        return super().fit(X, y, **fit_params)
```

## 三、Randomized Search CV

参数多了之后 Grid Search 会爆炸。Random Search（Bergstra et al. 2011, 2012）从分布里采样，有两个好处：
1. **可控的计算预算**——固定迭代次数，与参数维度无关。
2. **不相关参数不浪费时间**——Grid Search 在不相关维度上要枚举所有值。

```python
def clfHyperFit(feat, lbl, t1, pipe_clf, param_grid, cv=3,
                bagging=[0, None, 1.], rndSearchIter=0,
                n_jobs=-1, pctEmbargo=0, **fit_params):
    # ... scoring 选择同上 ...
    inner_cv = PurgedKFold(n_splits=cv, t1=t1, pctEmbargo=pctEmbargo)
    if rndSearchIter == 0:
        gs = GridSearchCV(estimator=pipe_clf, param_grid=param_grid, ...)
    else:
        gs = RandomizedSearchCV(estimator=pipe_clf, param_distributions=param_grid,
                                scoring=scoring, cv=inner_cv, n_jobs=n_jobs,
                                iid=False, n_iter=rndSearchIter)
    # ... 后面 bagging 同上 ...
```

### 3.1 Log-Uniform 分布 ★（自定义关键工具）

SVC 的 `C`、RBF kernel 的 `gamma` 等参数只接受**非负值**，且响应是**非线性**的——`C` 从 0.01 到 1 的变化和从 1 到 100 的变化对模型可能同样重要。

如果用 `U[0, 100]` 均匀采样，99% 的样本会落在 > 1 的区域，浪费了 < 1 的可行域。

**Log-Uniform 分布**：$\log[x] \sim U[\log[a], \log[b]]$。

CDF：
$$
F[x] = \frac{\log[x] - \log[a]}{\log[b] - \log[a]}, \quad a \leq x \leq b
$$

PDF：
$$
f[x] = \frac{1}{x \log[b/a]}, \quad a \leq x \leq b
$$

注意：**CDF 与对数底无关**（$\log_c(x/a)/\log_c(b/a)$ 等价），所以 base 选什么都可以。

```python
class logUniform_gen(rv_continuous):
    def _cdf(self, x):
        return np.log(x / self.a) / np.log(self.b / self.a)

def logUniform(a=1, b=np.exp(1)):
    return logUniform_gen(a=a, b=b, name='logUniform')

# 用法
vals = logUniform(a=1E-3, b=1E3).rvs(size=10000)
# np.log(vals) 应当均匀分布在 [log(1E-3), log(1E3)]
```

## 四、为什么用 Neg Log-Loss 而不是 Accuracy（关键！）

### 4.1 直觉论证
假设 ML 策略以高置信度预测要"买入"，你按置信度大举建多仓。
- 若预测错误（市场反向），**亏一大笔**。
- 但 **accuracy 对"高置信度错买"和"低置信度错买"一视同仁**。
- 而且 accuracy 允许"高置信度的错"被"低置信度的对"抵消。

**真实的投资逻辑**：
- 高置信度对 = 大赚
- 高置信度错 = 大亏
- 高置信度对的赚 < 高置信度错的亏 → accuracy 严重偏离真实 PnL

### 4.2 数学

Log-loss（交叉熵损失）：
$$
L[Y, P] = -\log[P[Y|P]] = -\frac{1}{N} \sum_{n=0}^{N-1} \sum_{k=0}^{K-1} y_{n,k} \log[p_{n,k}]
$$

其中：
- $p_{n,k}$：预测 $n$ 为类别 $k$ 的概率
- $Y$：one-hot 二进制指示矩阵

**例子**：分类器输出两次"1"，真实标签是 [1, 0]。accuracy = 50%。但当这两次预测的概率分别是 0.5 和 0.9 时：
- 第一次"高置信度对" → 贡献小
- 第二次"高置信度错" → log-loss 爆炸

accuracy 看不到这个差别。

### 4.3 第二个理由：样本权重的语义

CV 通过 `sample_weight` 给样本加权（第 4 章）。样本权重来自**绝对收益**。所以**样本加权的 cross-entropy loss** 实际上估计的是：
- 正确标签 → 给出"方向"
- 概率 → 给出"仓位大小"
- 样本权重 → 给出"收益规模"

**这三者相乘正好是 PnL 的组成部分**。所以 sample-weighted neg log-loss **是金融场景下唯一在估算 PnL 的评分函数**。

### 4.4 "neg" 前缀的原因
sklearn 约定 `score` 越大越好，所以 log-loss 加负号。注意 sklearn issue #9144（cross_val_score 不传 sample_weight 给 log_loss）—— 用第 7 章的 `cvScore` 函数绕开。

## 五、本章实操要点

1. **超参调优用 GridSearchCV 或 RandomizedSearchCV**，但必须传 `PurgedKFold` 作为 CV generator。
2. **meta-labeling 评分用 `'f1'`，其他用 `'neg_log_loss'`**——避免类别不平衡或仓位规模错配。
3. **Pipeline + sample_weight** → 自己继承一个 `MyPipeline`。
4. **非负、非线性参数（如 SVC 的 C, gamma）** → 用 `logUniform` 分布采样。
5. **永远不用 `accuracy` 作为金融策略评分**——它和真实 PnL 不一致。
6. **永远不用 `sklearn.cross_val_score`** —— 改用自己的 `cvScore`。
7. **可以套一层 Bagging**：调好超参后再 bag，能进一步降方差。

## 六、关联阅读

- 上游：CV 框架与 PurgedKFold → [[chapter-07-cross-validation]]
- 平行：特征重要性（同样用 PurgedKFold）→ [[chapter-08-feature-importance]]
- 评分相关：F1、precision、recall 详解 → [[chapter-14-backtest-statistics]]（14.8 节）
- meta-labeling 上下文 → [[chapter-03-labeling]]
- 样本权重生成（决定 PnL 估计的"规模"）→ [[chapter-04-sample-weights]]
- 下游：调好的模型用于回测 → [[chapter-12-backtesting-cv]]、[[chapter-13-synthetic-backtesting]]
