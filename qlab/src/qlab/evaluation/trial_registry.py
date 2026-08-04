"""TrialRegistry — §3.13.

Marcos 第三定律的数据基础：每次 pipeline 运行自动记录，便于事后算 DSR。
按 workspace 隔离存储。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pandas as pd


class TrialRegistry:
    """SQLite 实现的实验追踪表."""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS trials (
        trial_id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        dataset_id TEXT NOT NULL,
        pipeline_hash TEXT NOT NULL,
        pipeline_config TEXT NOT NULL,
        metrics TEXT NOT NULL,
        git_commit TEXT,
        notes TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_dataset ON trials (dataset_id);
    CREATE INDEX IF NOT EXISTS idx_pipeline ON trials (pipeline_hash);
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(str(self.path))
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._conn() as conn:
            conn.executescript(self.SCHEMA)

    @staticmethod
    def _compute_pipeline_hash(config: dict[str, Any]) -> str:
        norm = json.dumps(config, sort_keys=True, default=str)
        return hashlib.sha1(norm.encode()).hexdigest()[:16]

    def record(
        self,
        dataset_id: str,
        pipeline_config: dict[str, Any],
        metrics: dict[str, float],
        git_commit: str | None = None,
        notes: str | None = None,
    ) -> int:
        """记录一次实验. 返回 trial_id."""
        ts = pd.Timestamp.now().isoformat()
        pipeline_hash = self._compute_pipeline_hash(pipeline_config)
        config_str = json.dumps(pipeline_config, sort_keys=True, default=str)
        metrics_str = json.dumps(metrics, default=str)

        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO trials (created_at, dataset_id, pipeline_hash,
                                    pipeline_config, metrics, git_commit, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, dataset_id, pipeline_hash, config_str, metrics_str,
                 git_commit, notes),
            )
            return cur.lastrowid

    def list_trials(self, dataset_id: str | None = None) -> pd.DataFrame:
        with self._conn() as conn:
            if dataset_id:
                rows = conn.execute(
                    "SELECT * FROM trials WHERE dataset_id = ?", (dataset_id,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM trials").fetchall()
            cols = [d[0] for d in conn.execute("SELECT * FROM trials LIMIT 0").description]
        df = pd.DataFrame(rows, columns=cols)
        for col in ("pipeline_config", "metrics"):
            if col in df.columns:
                df[col] = df[col].apply(lambda s: json.loads(s) if s else None)
        return df

    def get_sr_distribution(self, dataset_id: str) -> pd.Series:
        """读取本 dataset 的所有 trial 的 SR. 用于 DSR 计算."""
        df = self.list_trials(dataset_id)
        if df.empty:
            return pd.Series(dtype=float)
        srs = df["metrics"].apply(lambda m: m.get("sharpe_ratio") if isinstance(m, dict) else None)
        return srs.dropna().astype(float)

    def n_trials(self, dataset_id: str) -> int:
        with self._conn() as conn:
            (n,) = conn.execute(
                "SELECT COUNT(*) FROM trials WHERE dataset_id = ?", (dataset_id,),
            ).fetchone()
        return int(n)
