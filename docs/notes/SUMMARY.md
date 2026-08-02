# Advances in Financial Machine Learning — 全书总览

> **作者**：Marcos López de Prado（2018, Wiley）  
> **核心论点**：金融机器学习是一门独立学科，而不是把通用 ML 套到金融数据上。本书是一份**研究工厂的操作手册**——它教你怎么搭一条从原始数据到上线策略的流水线。  
> **本笔记**：22 章深度笔记 + 全书总览。每章笔记在 1500-3000 字之间，保留关键公式（LaTeX）与算法伪代码。  
> **作者两条铁律**：
> - **Marcos 第一定律**：回测不是研究工具，特征重要性才是。
> - **Marcos 第二定律**：边研究边回测 = 酒驾。
> - **Marcos 第三定律**：每个回测结果必须与所有尝试次数一起报告。

## 一、阅读路径推荐

### 1.1 完整线性阅读（强推）
按 1 → 22 章顺序读。作者明确说**每章假设前面章节已读**。

### 1.2 按"问题驱动"路径
- **数据问题** → 第 2、5、17、18、19 章
- **标注与样本权重** → 第 3、4 章
- **模型与 CV** → 第 6、7、8、9 章
- **回测与防过拟合** → 第 11、12、13、14 章
- **下注 sizing** → 第 10 章
- **风险与配置** → 第 15、16 章
- **HPC 工程** → 第 20、21、22 章

### 1.3 按"角色"路径
- **新手 PM/Quant**：第 1 章 + Part 3（回测）+ 第 10 章（sizing）
- **数据工程师**：Part 1（第 2、3、4、5 章）+ Part 5（HPC）
- **ML 工程师**：Part 2（第 6-9 章）+ 第 8 章 + 第 16 章
- **CIO/管理者**：第 1 章 + 第 11 章 + 第 15 章

## 二、章节地图

### Part 0（Preamble）
- [Chapter 1: 金融机器学习是一门独立学科](chapter-01-financial-ml-as-distinct-subject.md) — 西西弗斯范式 vs Meta-Strategy；研究工厂分工

### Part 1 数据分析（Data Analysis）
- [Chapter 2: 金融数据结构](chapter-02-financial-data-structures.md) — Dollar bars、信息驱动 bars、ETF trick、CUSUM 采样
- [Chapter 3: 标注](chapter-03-labeling.md) — **Triple-Barrier method**、**Meta-Labeling**
- [Chapter 4: 样本权重](chapter-04-sample-weights.md) — Uniqueness、**Sequential Bootstrap**、Return Attribution、Time Decay
- [Chapter 5: 分数阶差分特征](chapter-05-fractional-differentiation.md) — **FFD**：在平稳与记忆间找最优 $d^*$

### Part 2 建模（Modelling）
- [Chapter 6: 集成方法](chapter-06-ensemble-methods.md) — Bagging 在金融为何首选；RF 修复
- [Chapter 7: 金融中的 CV](chapter-07-cross-validation.md) — **Purged K-Fold + Embargo**
- [Chapter 8: 特征重要性](chapter-08-feature-importance.md) — MDI / MDA / SFI + PCA 正交化
- [Chapter 9: 超参调优](chapter-09-hyperparameter-tuning.md) — Log-Uniform 分布 + Neg Log-Loss

### Part 3 回测（Backtesting）
- [Chapter 10: 下注规模](chapter-10-bet-sizing.md) — Meta-labeling 概率 → 仓位；动态 sizing + 限价
- [Chapter 11: 回测的危险](chapter-11-dangers-of-backtesting.md) — 七宗罪、PBO（CSCV）
- [Chapter 12: 通过 CV 回测](chapter-12-backtesting-cv.md) — **CPCV**：多路径回测
- [Chapter 13: 合成数据回测](chapter-13-synthetic-backtesting.md) — O-U 过程 → OTR 热图
- [Chapter 14: 回测统计量](chapter-14-backtest-statistics.md) — DD/TuW/PSR/**DSR**/F1
- [Chapter 15: 理解策略风险](chapter-15-strategy-risk.md) — $P[p < p_{\theta^*}]$：策略失败概率
- [Chapter 16: ML 资产配置](chapter-16-asset-allocation.md) — **HRP**：层次化风险平价

### Part 4 有用的金融特征（Useful Financial Features）
- [Chapter 17: 结构性突变](chapter-17-structural-breaks.md) — CUSUM、**SADF**、SMT
- [Chapter 18: 熵特征](chapter-18-entropy-features.md) — Shannon、Lempel-Ziv、组合集中度
- [Chapter 19: 市场微观结构特征](chapter-19-microstructural-features.md) — 三代理论；作者自创"微观结构信息"定义

### Part 5 HPC 配方（High-Performance Computing Recipes）
- [Chapter 20: 多进程与向量化](chapter-20-multiprocessing.md) — mpPandasObj 详解
- [Chapter 21: 暴力搜索与量子计算](chapter-21-brute-force-quantum.md) — 离散化 + quantum annealer
- [Chapter 22: HPC 与预测技术](chapter-22-hpc-forecasting.md) — 客座章节，LBNL CIFT 项目

## 三、五大跨章节关键创新（必读）

### 1. Triple-Barrier 标注法 + Meta-Labeling（第 3 章）
取代固定时间窗 + 固定阈值标注。**Meta-labeling 是把"是否下注"和"下注多大"从主模型解耦**——可在白盒模型（如基本面/技术分析/PM 直觉）之上加一层 ML。

### 2. 分数阶差分 FFD（第 5 章）
全行业 1 阶差分（取收益率）= 把记忆全擦了。FFD 在保留长记忆的前提下达成平稳——E-mini S&P 500 仅需 $d^* \approx 0.35$（不是 1）。

### 3. Purged K-Fold CV + Embargo（第 7 章）
金融场景下标准 K-Fold 必然过拟合（标签重叠 → 信息泄漏）。这一套 CV 框架是后续所有"特征重要性 / 超参调优 / 回测"的基础。

### 4. CPCV（Combinatorial Purged CV，第 12 章）
WF/CV 只有一条回测路径；CPCV 通过组合训练/测试集**生成多条路径**，得到 SR 的经验分布而不是单值。Marcos 主张这应成为期刊审稿和监管尽调的标准。

### 5. HRP（Hierarchical Risk Parity，第 16 章）
Markowitz 二次优化的诅咒：投资越相关，越需要分散，但协方差矩阵越病态。HRP 用图论 + 聚类替代矩阵求逆，**OOS 方差比 CLA 低 72%**，比传统 IVP 低 38%。**不需要协方差矩阵可逆**——奇异矩阵也能算。

## 四、跨章节贯穿主题

### 4.1 防过拟合（贯穿全书）

| 层面 | 解法 | 章节 |
|---|---|---|
| 数据 | dollar bars + 事件驱动采样 | 2 |
| 标注 | triple-barrier + meta-labeling | 3 |
| 样本权重 | uniqueness + sequential bootstrap | 4 |
| 模型 | bagging + RF 早停 + PCA | 6, 16 |
| CV | Purged K-Fold + Embargo | 7 |
| 特征选择 | MDI/MDA/SFI 互补 + Stacked | 8 |
| 超参 | PurgedKFold + neg log-loss | 9 |
| 回测 | CPCV + 合成数据 + DSR | 11, 12, 13, 14 |
| 资产配置 | HRP | 16 |

### 4.2 跨章节工具链

```
原始数据
  ↓ Ch2 dollar bars + CUSUM 采样
特征矩阵 X
  ↓ Ch5 FFD 平稳化
  ↓ Ch17/18/19 结构性突变/熵/微观结构特征
标注 y
  ↓ Ch3 triple-barrier + meta-labeling
样本权重 w
  ↓ Ch4 uniqueness + return attribution + time decay
训练
  ↓ Ch6 bagging
  ↓ Ch7 PurgedKFold
  ↓ Ch8 特征重要性
  ↓ Ch9 超参调优（neg log-loss）
模型
  ↓ Ch10 概率 → bet size（动态 sizing + 限价）
策略
  ↓ Ch12 CPCV 回测（多路径）
  ↓ Ch13 合成数据回测
  ↓ Ch14 backtest 统计 + DSR
  ↓ Ch15 策略风险评估
组合
  ↓ Ch16 HRP 资产配置
上线
  ↓ Embargo → Paper trading → Graduation → Re-allocation → Decommission
```

### 4.3 全书反复出现的工程工具

- **`mpPandasObj`**（第 20 章详解）：第 3、4、8、13、17 章都在用
- **`PurgedKFold`**（第 7 章）：第 8、9、12 章直接复用
- **`cvScore`**（第 7 章）：绕开 sklearn 的 cross_val_score bug

## 五、作者反复警告的常见陷阱

| # | 陷阱 | 章节 |
|---|---|---|
| 1 | 用别人处理过的数据集 | 2 |
| 2 | time bars + 固定阈值标注 | 2, 3 |
| 3 | 假设 IID 做 bagging / CV | 4, 6, 7 |
| 4 | 整数差分（收益率）= 抹掉记忆 | 5 |
| 5 | shuffle 的 CV | 7 |
| 6 | sklearn 的 cross_val_score bug | 7, 9 |
| 7 | OOB accuracy 在金融场景被高估 | 6 |
| 8 | accuracy 作为评分函数 | 9 |
| 9 | 边研究边回测 | 11 |
| 10 | walk-forward 当唯一真理 | 11, 12 |
| 11 | 报告单个 SR 不报告 DSR | 14 |
| 12 | 混淆策略风险与组合风险 | 15 |
| 13 | mean-variance 优化 | 16 |
| 14 | 只看一代微结构理论 | 19 |
| 15 | 没并行就跑大 ML 任务 | 20 |

## 六、对管理与协作的启示

- **不要让 PhD 单打独斗**——研究工厂分工：Data Curators / Feature Analysts / Strategists / Backtesters / Deployment / Portfolio Oversight。
- **特征工厂 vs 策略工厂**：Feature Analysts 产出特征库，不产出策略；Strategists 把特征组合为带经济解释的策略。
- **回测结果不外流到其他工位**——避免选择偏差。
- **数据是护城河**：用独特、难复制、难被操纵的数据。让数据基建团队头痛的数据，往往最值钱。
- **HPC 不是奢侈品，是必需品**——很多大数据集没有并行根本算不动。

## 七、对未来研究方向的预测（作者观点）

- **Quantamental 是未来主流**——meta-labeling 是过渡最清晰的技术路径。
- **金融需要它的 Kepler**：从圆轨道思维（线性回归）切换到椭圆轨道（ML 的非线性）。
- **CSCV / PBO / DSR 应成为审稿和监管标准**——杜绝多重检验的假发现。
- **量子计算**会让一类此前 intractable 的组合优化变得可解（Rosenberg et al. 2016 已用 D-Wave 验证）。
- **HPC 价值最高的场景**：监管响应（flash crash 类）、实时风险预警。

## 八、参考与延伸阅读

### 8.1 配套资源
- 全书代码：作者网站 [http://www.QuantResearch.org/](http://www.QuantResearch.org/)
- 协作者列表：[http://www.quantresearch.org/Co-authors.htm](http://www.quantresearch.org/Co-authors.htm)
- CIFT 项目（第 22 章）：[http://crd.lbl.gov/cift/](http://crd.lbl.gov/cift/)

### 8.2 前置阅读建议
- Hastie, Tibshirani, Friedman (2016) *The Elements of Statistical Learning*
- Geron (2017) *Hands-On ML with Scikit-Learn and TensorFlow*
- López de Prado (2015) "The Future of Empirical Finance" - 本书理论背景
- Easley, López de Prado, O'Hara (2013) *High-Frequency Trading*

### 8.3 后续延伸
- López de Prado 后续书：《Machine Learning for Asset Managers》(2020) - 本书的简化进阶版
- Bailey & López de Prado (2014) "The Deflated Sharpe Ratio" - 第 14 章核心
- Rosenberg et al. (2016) "Solving the optimal trading trajectory problem using a quantum annealer" - 第 21 章实施

## 九、本笔记编写说明

- **总字数**：22 章 × 平均 ~2200 字 ≈ 4.8 万字
- **保留**：关键 LaTeX 公式、Python 算法伪代码、章节关键数值结果
- **省略**：图片、附录数学推导细节
- **每章末**："关联阅读"指向相关章节，按 `[[chapter-XX-name]]` Wiki 风格链接
- **建议工作流**：先读 SUMMARY → 按问题或角色路径选章节 → 实战时回查具体代码段
