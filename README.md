# Quant Lab (monorepo)

A-share quantitative research workspace.

```
quant-lab/
├── qlab/          # 研究框架（数据 / 特征 / 标注 / 模型 / 回测）
├── jq/            # 聚宽 JupyterHub 远程执行桥（可选数据源）
├── docs/          # 设计文档与笔记
├── Makefile       # 常用开发命令
└── requirements.txt
```

## 安装

在仓根创建虚拟环境并安装两个包（editable）：

```bash
make install
# 等价于:
#   python3.11 -m venv .venv
#   .venv/bin/pip install -e "./qlab[dev]"
#   .venv/bin/pip install -e "./jq[dev]"
```

只装研究框架（无聚宽）：

```bash
pip install -e "./qlab[dev]"
```

## 文档

- [qlab 包说明](qlab/README.md)
- [jq 包说明](jq/README.md)
- [数据模块设计](docs/design/data-module-design.md)
- [下游模块设计](docs/design/downstream-modules-design.md)
- [回归测试基线](docs/design/regression-baseline.md)

## 测试

```bash
make test                 # qlab + jq 单元测试
make test-qlab            # 仅 qlab
make test-jq              # 仅 jq
cd qlab && pytest -m realdata   # qlab 真实数据回归（需聚宽凭证）
```
