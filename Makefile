PYTHON := .venv/bin/python

.PHONY: help venv install test lint format clean example

help:
	@echo "Quant Lab — 开发命令"
	@echo ""
	@echo "环境："
	@echo "  make venv                 - 创建 Python 3.11 虚拟环境"
	@echo "  make install              - 安装所有依赖（含开发依赖）"
	@echo ""
	@echo "开发："
	@echo "  make test                 - 运行所有测试"
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
	$(PYTHON) -m pip install -r requirements.txt
	$(PYTHON) -m pip install -e .

test:
	$(PYTHON) -m pytest tests/ -v --tb=short --disable-warnings

lint:
	$(PYTHON) -m ruff check src/ tests/

format:
	$(PYTHON) -m ruff check --fix src/ tests/

example:
	$(PYTHON) tests/examples/end_to_end.py

clean:
	@echo "清理缓存和临时文件..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache/ .mypy_cache/ .ruff_cache/
	rm -rf build/ dist/
	rm -f .coverage .coverage.*
	@echo "清理完成！"
