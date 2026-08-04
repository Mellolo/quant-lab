"""FeatureStore — 特征计算结果缓存.

缓存键：(feature_name, feature_version, dataset_id, universe_id, date_range)
公式改 → 版本号变 → 缓存自动失效。

伴随元数据（FeatureValueMeta，§3.8）：
- InMemoryFeatureStore: 存在内存 dict
- ParquetFeatureStore: 以 `{key}.meta.json` sidecar 持久化
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Protocol, runtime_checkable

import pandas as pd

from qlab.features.base import FeatureMeta, FeatureValueMeta


def make_feature_key(meta: FeatureMeta, dataset_id: str,
                     universe_id: str, date_range: tuple) -> str:
    spec = {
        "name": meta.name,
        "version": meta.version,
        "dataset_id": dataset_id,
        "universe_id": universe_id,
        "start": str(date_range[0])[:10],
        "end": str(date_range[1])[:10],
    }
    return hashlib.sha1(json.dumps(spec, sort_keys=True).encode()).hexdigest()[:16]


@runtime_checkable
class FeatureStore(Protocol):
    def has(self, key: str) -> bool: ...
    def get(self, key: str) -> pd.Series: ...
    def put(self, key: str, value: pd.Series, meta: FeatureValueMeta | None = None) -> None: ...
    def get_meta(self, key: str) -> FeatureValueMeta | None: ...
    def invalidate(self, key_pattern: str) -> None: ...


class InMemoryFeatureStore:
    def __init__(self) -> None:
        self._store: dict[str, pd.Series] = {}
        self._meta: dict[str, FeatureValueMeta] = {}

    def has(self, key: str) -> bool:
        return key in self._store

    def get(self, key: str) -> pd.Series:
        return self._store[key].copy()

    def put(self, key: str, value: pd.Series, meta: FeatureValueMeta | None = None) -> None:
        self._store[key] = value.copy()
        if meta is not None:
            self._meta[key] = meta

    def get_meta(self, key: str) -> FeatureValueMeta | None:
        return self._meta.get(key)

    def invalidate(self, key_pattern: str) -> None:
        self._store.pop(key_pattern, None)
        self._meta.pop(key_pattern, None)

    def clear(self) -> None:
        self._store.clear()
        self._meta.clear()


class ParquetFeatureStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / f"{key}.parquet"

    def _meta_path(self, key: str) -> Path:
        return self.root / f"{key}.meta.json"

    def has(self, key: str) -> bool:
        return self._path(key).exists()

    def get(self, key: str) -> pd.Series:
        df = pd.read_parquet(self._path(key))
        # Series 存为 single-column DataFrame
        return df.iloc[:, 0]

    def put(self, key: str, value: pd.Series, meta: FeatureValueMeta | None = None) -> None:
        value.to_frame(name=value.name or "value").to_parquet(self._path(key))
        if meta is not None:
            self._meta_path(key).write_text(
                json.dumps(meta.to_json_dict(), indent=2, ensure_ascii=False)
            )

    def get_meta(self, key: str) -> FeatureValueMeta | None:
        p = self._meta_path(key)
        if not p.exists():
            return None
        return FeatureValueMeta.from_json_dict(json.loads(p.read_text()))

    def invalidate(self, key_pattern: str) -> None:
        for p in self.root.glob(f"{key_pattern}*.parquet"):
            p.unlink()
        for p in self.root.glob(f"{key_pattern}*.meta.json"):
            p.unlink()
