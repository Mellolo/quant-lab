"""BarStore 实现 — 内存 + Parquet 两种后端.

设计原则 P6: 缓存键含所有影响结果的输入，包括数据源版本。

两类存储：
- BarStore (key-based): 用于 universe / corp_actions / fundamentals / industry
- ShardedBarStore: K 线专用，按 (kind, symbol, year-month) 分片
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from qlab.data.interfaces import BarStore, ShardedBarStore

logger = logging.getLogger(__name__)


def make_cache_key(**kwargs: Any) -> str:
    """生成确定性的缓存键.

    将 kwargs 序列化为规范化 JSON 后 SHA1。
    """
    norm = json.dumps(kwargs, sort_keys=True, default=str)
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()[:16]


def _safe_segment(value: Any) -> str:
    """把任意值编码为文件名安全的路径段."""
    s = str(value)
    return "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in s)


def _unique(symbols: list[str]) -> list[str]:
    """去重并保序.

    重复的 symbol 会让同一分片被读多次 → 返回的 DataFrame 出现重复行 →
    下游 ``unstack`` 抛 ``ValueError: Index contains duplicate entries``。
    重复读同一分片没有任何意义, 故在入口统一去重。
    """
    return list(dict.fromkeys(symbols))


def _inclusive_end(end: pd.Timestamp) -> pd.Timestamp:
    """把“纯日期”的 end 撑到当日末尾, 使区间对日内数据也是闭区间.

    intraday 的索引是带时分秒的 ``timestamp``(如10:00/15:00), 而调用方传的
    ``end='2024-06-03'`` 解析为当日 **00:00** —— 直接用 ``<= end`` 会把整天
    的 bar 全部排除, 导致 ``intraday(D, D)`` 返回空(日频无此问题, 因为它的
    索引就是当日 00:00)。

    仅当 end 没有时分秒时才扩展; 显式传了时刻(如 ``13:00``)则尊重原值。
    """
    if end == end.normalize():
        return end + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    return end


def _months_between(start: pd.Timestamp, end: pd.Timestamp) -> list[str]:
    """返回 [start, end] 覆盖的所有月份 (YYYY-MM)."""
    start = pd.Timestamp(start).to_period("M")
    end = pd.Timestamp(end).to_period("M")
    months = pd.period_range(start, end, freq="M")
    return [str(p) for p in months]


def _series_year_month(idx: pd.DatetimeIndex) -> pd.Index:
    return pd.Index([f"{d.year:04d}-{d.month:02d}" for d in idx])


class InMemoryBarStore(BarStore):
    """内存缓存. 开发/测试用."""

    def __init__(self) -> None:
        self._store: dict[str, pd.DataFrame] = {}

    def has(self, key: str) -> bool:
        return key in self._store

    def get(self, key: str) -> pd.DataFrame:
        return self._store[key].copy()

    def put(self, key: str, df: pd.DataFrame) -> None:
        self._store[key] = df.copy()

    def invalidate(self, key_pattern: str) -> None:
        # 简单实现：精确匹配
        self._store.pop(key_pattern, None)

    def clear(self) -> None:
        self._store.clear()

    def __len__(self) -> int:
        return len(self._store)


class ParquetBarStore(BarStore):
    """Parquet 持久化缓存. 生产用.

    存储路径：{root}/{key}.parquet
    """

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / f"{key}.parquet"

    def has(self, key: str) -> bool:
        """键是否存在**且可读** —— 损坏文件视为不存在(会重取并覆写)."""
        p = self._path(key)
        if not p.exists():
            return False
        try:
            pd.read_parquet(p)
        except Exception:  # noqa: BLE001 - 读不出就当缓存不可用
            logger.warning("缓存文件损坏, 将重取并覆写: %s", p)
            return False
        return True

    def get(self, key: str) -> pd.DataFrame:
        return pd.read_parquet(self._path(key))

    def put(self, key: str, df: pd.DataFrame) -> None:
        df.to_parquet(self._path(key))

    def invalidate(self, key_pattern: str) -> None:
        # 支持简单的 glob 风格
        for p in self.root.glob(f"{key_pattern}*.parquet"):
            p.unlink()


class InMemoryShardedBarStore(ShardedBarStore):
    """测试用的分片缓存（内存版）.

    内部按 (kind, **extra_keys 排序后, source_version, symbol, year-month) 切片成 DataFrame，
    满足 ShardedBarStore Protocol.
    """

    def __init__(self) -> None:
        # _store[shard_key] = DataFrame slice
        self._store: dict[str, pd.DataFrame] = {}

    @staticmethod
    def _shard_key(kind: str, source_version: str, symbol: str, year_month: str,
                   **extra: Any) -> str:
        extra_str = "/".join(f"{k}={_safe_segment(extra[k])}" for k in sorted(extra))
        return f"{kind}/{_safe_segment(source_version)}/{extra_str}/{_safe_segment(symbol)}/{year_month}"

    def get_range(self, *, kind, symbols, start, end, source_version, **extra_keys):
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)
        months = _months_between(start, end)
        parts: list[pd.DataFrame] = []
        for sym in _unique(symbols):
            for ym in months:
                k = self._shard_key(kind, source_version, sym, ym, **extra_keys)
                if k in self._store:
                    parts.append(self._store[k])
        if not parts:
            return pd.DataFrame()
        df = pd.concat(parts).sort_index()
        # 收口到 [start, end]
        if isinstance(df.index, pd.MultiIndex):
            date_lvl = df.index.get_level_values(0)
            mask = (date_lvl >= start) & (date_lvl <= _inclusive_end(end))
            df = df.loc[mask]
        return df

    def put_range(self, df, *, kind, source_version, **extra_keys):
        if df is None or df.empty:
            return
        if not isinstance(df.index, pd.MultiIndex) or len(df.index.names) < 2:
            raise ValueError("put_range expects MultiIndex(date|timestamp, symbol)")
        date_lvl = df.index.get_level_values(0)
        sym_lvl = df.index.get_level_values(1)
        date_lvl_dt = pd.DatetimeIndex(date_lvl)
        ym_lvl = _series_year_month(date_lvl_dt)
        grouped = df.groupby([sym_lvl, ym_lvl], sort=False)
        for (sym, ym), part in grouped:
            k = self._shard_key(kind, source_version, str(sym), str(ym), **extra_keys)
            existing = self._store.get(k)
            if existing is not None:
                # 合并去重
                merged = pd.concat([existing, part])
                merged = merged[~merged.index.duplicated(keep="last")]
                self._store[k] = merged.sort_index()
            else:
                self._store[k] = part.sort_index()

    def missing_ranges(self, *, kind, symbols, start, end, source_version, **extra_keys):
        months = _months_between(pd.Timestamp(start), pd.Timestamp(end))
        missing: list[tuple[str, str]] = []
        for sym in _unique(symbols):
            for ym in months:
                k = self._shard_key(kind, source_version, sym, ym, **extra_keys)
                if k not in self._store:
                    missing.append((sym, ym))
        return missing

    def invalidate_range(self, *, kind, symbols=None, source_version=None, **extra_keys):
        prefix = f"{kind}/"
        if source_version is not None:
            prefix += f"{_safe_segment(source_version)}/"
        sym_filter = None
        if symbols is not None:
            sym_filter = set(_safe_segment(s) for s in symbols)
        for k in list(self._store):
            if not k.startswith(prefix):
                continue
            if sym_filter is not None:
                parts = k.split("/")
                # 倒数第二段是 symbol
                if len(parts) < 2 or parts[-2] not in sym_filter:
                    continue
            del self._store[k]


class ParquetShardedBarStore(ShardedBarStore):
    """K 线分片缓存（Parquet 落盘版）.

    目录结构::

        root/
          {kind}/{source_version}/{extra=val}/.../{symbol}/{YYYY-MM}.parquet

    `extra_keys` 形如 `freq='1d'`, `adjust='backward'`：按 key 字典序展开为子目录段。
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _shard_dir(self, kind: str, source_version: str, symbol: str,
                   extra: dict[str, Any]) -> Path:
        path = self.root / _safe_segment(kind) / _safe_segment(source_version)
        for k in sorted(extra):
            path = path / f"{_safe_segment(k)}={_safe_segment(extra[k])}"
        path = path / _safe_segment(symbol)
        return path

    def _shard_path(self, kind, source_version, symbol, year_month, extra) -> Path:
        return self._shard_dir(kind, source_version, symbol, extra) / f"{year_month}.parquet"

    def _read_shard(self, path: Path) -> pd.DataFrame | None:
        """读单个分片; 文件损坏时返回 None 而不抛异常.

        分片缓存本就可从远程重建 —— “跑一半被 Ctrl-C”留下的半写 parquet
        应自动作废重取, 而不是让整个研究流程死在
        ``ArrowInvalid: Parquet magic bytes not found`` 上(用户也看不出该删哪个文件)。
        与 jq 缓存层的行为保持一致。
        """
        try:
            return pd.read_parquet(path)
        except Exception:  # noqa: BLE001 - 任何读失败都当作分片不可用
            logger.warning(
                "分片损坏, 已忽略并将从远程重取: %s", path,
            )
            return None

    def get_range(self, *, kind, symbols, start, end, source_version, **extra_keys):
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)
        months = _months_between(start, end)
        parts: list[pd.DataFrame] = []
        for sym in _unique(symbols):
            for ym in months:
                p = self._shard_path(kind, source_version, sym, ym, extra_keys)
                if p.exists():
                    got = self._read_shard(p)
                    if got is not None:
                        parts.append(got)
        if not parts:
            return pd.DataFrame()
        df = pd.concat(parts).sort_index()
        if isinstance(df.index, pd.MultiIndex):
            date_lvl = pd.DatetimeIndex(df.index.get_level_values(0))
            mask = (date_lvl >= start) & (date_lvl <= _inclusive_end(end))
            df = df.loc[mask]
        return df

    def put_range(self, df, *, kind, source_version, **extra_keys):
        """写入分片.

        Raises:
            OSError: 缓存目录不可写(权限/只读挂载/盘满)。不包一层的话
                只能看到一个裸的 ``PermissionError: …/000858.SZ``,
                看不出是缓存层写不进去。
        """
        if df is None or df.empty:
            return
        if not isinstance(df.index, pd.MultiIndex) or len(df.index.names) < 2:
            raise ValueError("put_range expects MultiIndex(date|timestamp, symbol)")
        date_lvl = pd.DatetimeIndex(df.index.get_level_values(0))
        sym_lvl = df.index.get_level_values(1)
        ym_lvl = _series_year_month(date_lvl)
        grouped = df.groupby([sym_lvl, ym_lvl], sort=False)
        for (sym, ym), part in grouped:
            shard_dir = self._shard_dir(kind, source_version, str(sym), extra_keys)
            try:
                shard_dir.mkdir(parents=True, exist_ok=True)
                p = shard_dir / f"{ym}.parquet"
                existing = self._read_shard(p) if p.exists() else None
                if existing is not None:
                    merged = pd.concat([existing, part])
                    merged = merged[~merged.index.duplicated(keep="last")].sort_index()
                else:
                    # 不存在或已损坏 —— 直接用新数据覆写, 以修复半写文件
                    merged = part.sort_index()
                merged.to_parquet(p)
            except OSError as ex:
                raise OSError(
                    f"分片缓存写入失败: {shard_dir}\n"
                    f"  原因: {type(ex).__name__}: {ex}\n"
                    f"  常见成因: 缓存根目录({self.root})不可写 / 只读挂载 / 磁盘已满。\n"
                    "  出路: 换一个可写的目录, 或改用 InMemoryShardedBarStore(不落盘)。"
                ) from ex

    def missing_ranges(self, *, kind, symbols, start, end, source_version, **extra_keys):
        """列出缺失分片。**损坏的分片也算缺失** —— 否则会被当成已缓存
        而不重取, 最终在 :meth:`get_range` 静默丢数据。
        """
        months = _months_between(pd.Timestamp(start), pd.Timestamp(end))
        missing: list[tuple[str, str]] = []
        for sym in _unique(symbols):
            for ym in months:
                p = self._shard_path(kind, source_version, sym, ym, extra_keys)
                if not p.exists() or self._read_shard(p) is None:
                    missing.append((sym, ym))
        return missing

    def invalidate_range(self, *, kind, symbols=None, source_version=None, **extra_keys):
        base = self.root / _safe_segment(kind)
        if not base.exists():
            return
        if source_version is not None:
            base = base / _safe_segment(source_version)
        if not base.exists():
            return
        # symbols 过滤：递归扫描 leaf 子目录
        if symbols is None:
            # 整段删除
            import shutil
            shutil.rmtree(base, ignore_errors=True)
            return
        sym_set = {_safe_segment(s) for s in symbols}
        for path in base.rglob("*"):
            if path.is_dir() and path.name in sym_set:
                import shutil
                shutil.rmtree(path, ignore_errors=True)
