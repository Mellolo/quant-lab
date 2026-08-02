"""Workspace 管理 — §7.4 决议.

目录结构::

    quant-lab/
    ├── store/                       # 全局共享缓存
    │   ├── bars/
    │   └── features/
    └── workspaces/
        └── <name>/
            ├── config.yaml
            ├── trials.db
            ├── artifacts/
            ├── logs/
            └── notes.md
"""

from __future__ import annotations

import shutil
from pathlib import Path

from qlab.core.exceptions import QlabError
from qlab.data.store import ParquetBarStore, ParquetShardedBarStore
from qlab.evaluation.trial_registry import TrialRegistry
from qlab.features.store import ParquetFeatureStore
from qlab.workspace.config import ExperimentConfig, load_config, save_config


class Workspace:
    """一个工作空间."""

    def __init__(self, root: str | Path, name: str):
        self.root = Path(root)
        self.name = name
        self.path = self.root / "workspaces" / name

    # ---- 路径 ---------------------------------------------------------------

    @property
    def config_path(self) -> Path:
        return self.path / "config.yaml"

    @property
    def trials_db_path(self) -> Path:
        return self.path / "trials.db"

    @property
    def artifacts_dir(self) -> Path:
        return self.path / "artifacts"

    @property
    def logs_dir(self) -> Path:
        return self.path / "logs"

    @property
    def shared_bar_store(self) -> Path:
        return self.root / "store" / "bars"

    @property
    def shared_feature_store(self) -> Path:
        return self.root / "store" / "features"

    # ---- 生命周期 -----------------------------------------------------------

    def init(self, config: ExperimentConfig | None = None) -> None:
        if self.path.exists():
            raise QlabError(f"Workspace 已存在: {self.path}")
        self.path.mkdir(parents=True)
        self.artifacts_dir.mkdir()
        self.logs_dir.mkdir()
        self.shared_bar_store.mkdir(parents=True, exist_ok=True)
        self.shared_feature_store.mkdir(parents=True, exist_ok=True)
        if config is not None:
            save_config(config, self.config_path)
        # 触发 trial registry 初始化
        TrialRegistry(self.trials_db_path)
        # 空 notes
        (self.path / "notes.md").write_text(f"# {self.name}\n\n")

    def exists(self) -> bool:
        return self.path.exists()

    def load_config(self) -> ExperimentConfig:
        return load_config(self.config_path)

    def save_config(self, config: ExperimentConfig) -> None:
        save_config(config, self.config_path)

    def trial_registry(self) -> TrialRegistry:
        return TrialRegistry(self.trials_db_path)

    def bar_store(self) -> ParquetShardedBarStore:
        """K 线分片缓存（生产用）."""
        return ParquetShardedBarStore(self.shared_bar_store)

    def kv_store(self) -> ParquetBarStore:
        """通用 key-based 缓存（universe / corp_actions / industry / fundamentals）."""
        return ParquetBarStore(self.root / "store" / "kv")

    def feature_store(self) -> ParquetFeatureStore:
        return ParquetFeatureStore(self.shared_feature_store)

    # ---- 操作 ---------------------------------------------------------------

    def clone(self, dst_name: str, keep_history: bool = False) -> Workspace:
        """复制 workspace.

        keep_history=False : 不复制 trials.db（新研究线）
        keep_history=True  : 复制 trials.db（同一研究线分支）
        """
        dst = Workspace(self.root, dst_name)
        if dst.exists():
            raise QlabError(f"目标 workspace 已存在: {dst_name}")
        shutil.copytree(self.path, dst.path)
        if not keep_history and dst.trials_db_path.exists():
            dst.trials_db_path.unlink()
            TrialRegistry(dst.trials_db_path)  # 重新初始化空表
        return dst

    def remove(self, force: bool = False) -> None:
        if not self.exists():
            return
        if not force:
            n = self.trial_registry().n_trials(self.load_config().dataset_id)
            if n > 0:
                raise QlabError(
                    f"Workspace '{self.name}' 有 {n} 条 trial 记录. "
                    f"force=True 强制删除."
                )
        shutil.rmtree(self.path)

    def __repr__(self) -> str:
        return f"<Workspace name='{self.name}' path='{self.path}'>"
