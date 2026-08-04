PYTHON := .venv/bin/python

.PHONY: help venv install test test-qlab test-jq lint format clean example

help:
	@echo "Quant Lab — 开发命令"
	@echo ""
	@echo "环境："
	@echo "  make venv                 - 创建 Python 3.11 虚拟环境"
	@echo "  make install              - 安装 qlab + jq（含开发依赖）"
	@echo ""
	@echo "开发："
	@echo "  make test                 - 运行 qlab + jq 单元测试"
	@echo "  make test-qlab            - 仅 qlab 单元测试"
	@echo "  make test-jq              - 仅 jq 单元测试"
	@echo "  make lint                 - 代码风格检查（ruff）"
	@echo "  make format               - 代码格式化（ruff --fix）"
	@echo "  make example              - 运行端到端示例"
	@echo ""
	@echo "清理："
	@echo "  make clean                - 清理缓存和临时文件"

venv:
	python3.11 -m venv .venv
	$(PYTHON) -m pip install --upgrade pip

install: venv
	$(PYTHON) -m pip install -e "./qlab[dev]"
	$(PYTHON) -m pip install -e "./jq[dev]"

test: test-qlab test-jq

test-qlab:
	$(PYTHON) -m pytest qlab/tests/ -v --tb=short --disable-warnings

test-jq:
	$(PYTHON) -m pytest jq/tests/ -v --tb=short --disable-warnings

lint:
	$(PYTHON) -m ruff check qlab/src/ qlab/tests/ jq/src/ jq/tests/

format:
	$(PYTHON) -m ruff check --fix qlab/src/ qlab/tests/ jq/src/ jq/tests/

example:
	$(PYTHON) qlab/tests/examples/end_to_end.py

clean:
	@echo "清理缓存和临时文件..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache/ .mypy_cache/ .ruff_cache/
	rm -rf build/ dist/ qlab/build/ qlab/dist/ jq/build/ jq/dist/
	rm -f .coverage .coverage.*
	@echo "清理完成！"
