# Quant Lab

A-share quantitative research framework based on López de Prado's *Advances in Financial Machine Learning* (2018).

> 文档：
> - [全书章节笔记](notes/SUMMARY.md)
> - [数据与特征模块设计文档](docs/data-module-design.md)

## 设计原则

1. **PIT（Point-in-Time）正确性**：任何 $t$ 时刻可访问的数据只来自 $t$ 之前。
2. **复权是视图，不是属性**：存储不复权 + adj_factor；API 默认输出后复权。
3. **依赖单向**：`core ← data ← features ← labeling ← weights ← models ← (sizing, evaluation, allocation)`。
4. **Schema 即契约**：每个数据格式有明确的列、类型、不变量。
5. **A 股特性是一等公民**：复权、停牌、涨跌停、ST、新股、T+1、集合竞价默认处理。
6. **缓存友好 + 版本化**：特征公式改 → 版本号变 → 缓存自动失效。
7. **接口与实现分离**：数据源用 Protocol 抽象，下游可换源不改业务代码。

## 安装

在 monorepo 根目录（`quant-lab/`）执行：

```bash
pip install -e "./qlab[dev]"
# 可选：聚宽数据源
pip install -e "./jq[dev]"
```

## 快速开始

```python
from qlab.data import DataLayer
from qlab.data.sources import FakeDataSource
from qlab.features import FeatureMatrix
from qlab.features.library import Momentum, Volatility

data = DataLayer(source=FakeDataSource(seed=42))
features = FeatureMatrix(
    data=data,
    features=[Momentum(window=5), Volatility(window=20)],
    universe='all_a',
    date_range=('2022-01-01', '2024-12-31'),
).build()
```

完整端到端流水线示例见 `tests/examples/end_to_end.py`。

## 目录结构

```
qlab/
├── pyproject.toml
├── src/qlab/
│   ├── core/         # 类型、协议、日历、并行
│   ├── data/         # 数据层（行情/财务/行业/股本）
│   ├── features/     # 特征框架与因子库
│   ├── labeling/     # 三屏障 + meta-labeling
│   ├── weights/      # 非 IID 样本权重
│   ├── models/       # PurgedCV + 集成 + 特征重要性
│   ├── sizing/       # 仓位分配
│   ├── evaluation/   # CPCV + 统计 + TrialRegistry
│   ├── allocation/   # HRP
│   └── workspace/    # Workspace 管理
└── tests/
```

## 测试

```bash
cd qlab
pytest                                   # 单元测试（无外部依赖）
pytest -m realdata                       # 真实数据回归（需网络 + 聚宽凭证）
pytest -m realdata -k RegressionFeatures # 只回归某个模块
```

真实数据测试默认跳过。覆盖维度、已知数据边界（聚宽的隐式行数上限、
各标的类型的支持矩阵、数值容差）见
[docs/design/regression-baseline.md](docs/design/regression-baseline.md) —— 
**接入新数据源前建议先读这份**。

## 设计文档

- [数据模块设计](docs/design/data-module-design.md)
- [下游模块设计](docs/design/downstream-modules-design.md)
- [回归测试基线](docs/design/regression-baseline.md)
