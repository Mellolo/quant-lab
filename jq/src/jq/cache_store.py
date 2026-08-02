"""缓存存储原语 — 路径规划、原子写、按月分片、日期归一化.

本模块只负责"把 DataFrame 安全地按月存取到磁盘",不含任何聚宽业务逻辑.

设计要点:

- **按月分片**: 时间序列按 ``{标的}/{变体}/{YYYY-MM}.pkl`` 存储.
  好处: 天然知道缺哪个月(无需元数据)、单月写入不影响其他月、
  部分更新无需重写整段.
- **原子写**: 先写 ``.tmp`` 再 ``os.replace()``,并发/中断不会留下坏文件.
- **空月标记**: 查过但无数据的月份也写入空 DataFrame,
  避免上市前/退市后区间每次都重复远程查询.
- **当月不信任**: 当月及未来月的分片永远视为缺失(数据可能不完整).
- **版本隔离**: 路径含 ``CACHE_VERSION`` 段,格式变更时旧缓存自动失效.
- **长 key 收敛**: 文件名超长时改用 sha1 摘要,避免 OSError.
"""

from __future__ import annotations

import hashlib
import os
import pickle
import warnings
from datetime import date
from pathlib import Path

import pandas as pd

CACHE_VERSION = "v2"

# 文件名单段最大长度(ext4/APFS 单文件名上限 255,留余量给后缀与分隔符)
_MAX_KEY_LEN = 80


# ======================================================================
# 日期归一化
# ======================================================================


def norm_date(d: object) -> str:
    """把任意日期表示归一化为 ``YYYY-MM-DD`` 字符串.

    接受 ``str`` / ``datetime`` / ``date`` / ``pd.Timestamp``.
    非零填充的 ``'2026-7-1'`` 也能正确处理.

    Raises:
        ValueError: 无法解析为日期.
    """
    if d is None:
        raise ValueError("日期不能为 None")
    try:
        ts = pd.Timestamp(d)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"无法解析日期: {d!r}") from exc
    if pd.isna(ts):
        raise ValueError(f"无法解析日期: {d!r}")
    return ts.strftime("%Y-%m-%d")


def is_today_or_future(d: object) -> bool:
    """日期是否是今天或未来(此类数据不可信,不应长期缓存)."""
    try:
        return pd.Timestamp(norm_date(d)).date() >= date.today()
    except ValueError:
        return False


def month_of(d: object) -> str:
    """取日期所属月份 ``YYYY-MM``."""
    return norm_date(d)[:7]


def current_month() -> str:
    """当前月份 ``YYYY-MM``."""
    return date.today().strftime("%Y-%m")


def months_between(start: object, end: object) -> list[str]:
    """列出 ``[start, end]`` 覆盖的所有月份 ``YYYY-MM`` (升序)."""
    s = pd.Period(month_of(start), freq="M")
    e = pd.Period(month_of(end), freq="M")
    if e < s:
        return []
    return [str(p) for p in pd.period_range(s, e, freq="M")]


def month_bounds(ym: str) -> tuple[str, str]:
    """月份 ``YYYY-MM`` 的首末自然日 ``(YYYY-MM-01, YYYY-MM-DD)``."""
    p = pd.Period(ym, freq="M")
    return str(p.start_time.date()), str(p.end_time.date())


def merge_consecutive_months(months: list[str]) -> list[tuple[str, str]]:
    """把月份列表压缩成连续区间 ``[(start_date, end_date), ...]``.

    用于把"缺失的 N 个月"合并成尽量少的远程查询区间.

    Examples:
        >>> merge_consecutive_months(['2020-01', '2020-02', '2020-05'])
        [('2020-01-01', '2020-02-29'), ('2020-05-01', '2020-05-31')]
    """
    if not months:
        return []
    ordered = sorted(set(months))
    groups: list[list[str]] = [[ordered[0]]]
    for ym in ordered[1:]:
        prev = pd.Period(groups[-1][-1], freq="M")
        cur = pd.Period(ym, freq="M")
        if (cur - prev).n == 1:
            groups[-1].append(ym)
        else:
            groups.append([ym])
    return [(month_bounds(g[0])[0], month_bounds(g[-1])[1]) for g in groups]


# ======================================================================
# 路径与安全 key
# ======================================================================


def safe_key(value: object, max_len: int = _MAX_KEY_LEN) -> str:
    """把任意值编码为文件名安全的字符串.

    过长时(如传入数千只股票的列表)退化为 ``前缀_sha1摘要``,
    避免超出文件名长度限制.
    """
    raw = (
        "_".join(str(v) for v in value)
        if isinstance(value, (list, tuple, set))
        else str(value)
    )
    cleaned = "".join(
        c if c.isalnum() or c in ("-", "_") else "_" for c in raw
    )
    if len(cleaned) <= max_len:
        return cleaned or "empty"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]  # noqa: S324
    return f"{cleaned[: max_len - 17]}_{digest}"


# ======================================================================
# 原子读写
# ======================================================================


def atomic_write(path: Path, obj: object) -> None:
    """原子写入 pickle: 先写同目录 ``.tmp`` 再 ``os.replace``.

    保证并发写或中途中断都不会产生半截文件.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    try:
        with open(tmp, "wb") as fh:
            pickle.dump(obj, fh, protocol=4)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def read_pkl(path: Path, *, warn: bool = True) -> object | None:
    """读取 pickle. 文件不存在返回 None;损坏时告警后返回 None."""
    if not path.exists():
        return None
    try:
        with open(path, "rb") as fh:
            return pickle.load(fh)  # noqa: S301
    except Exception as exc:  # noqa: BLE001
        if warn:
            warnings.warn(
                f"缓存文件损坏,将重新远程取数: {path} ({type(exc).__name__}: {exc})",
                RuntimeWarning,
                stacklevel=2,
            )
        return None


# ======================================================================
# 月分片存储
# ======================================================================


class MonthlyShardStore:
    """按 ``{root}/{version}/bars/{kind}/{标的}/{变体}/{YYYY-MM}.pkl`` 分片存储.

    Args:
        root: 缓存根目录.
    """

    def __init__(self, root: Path):
        self.root = Path(root)

    # ---------------- 路径 ----------------

    def shard_dir(self, kind: str, security: object, variant: str = "default") -> Path:
        return (
            self.root
            / CACHE_VERSION
            / "bars"
            / safe_key(kind)
            / safe_key(security)
            / safe_key(variant)
        )

    def shard_path(
        self, kind: str, security: object, ym: str, variant: str = "default"
    ) -> Path:
        return self.shard_dir(kind, security, variant) / f"{ym}.pkl"

    # ---------------- 查询缺失 ----------------

    def missing_months(
        self,
        kind: str,
        security: object,
        start: object,
        end: object,
        variant: str = "default",
    ) -> list[str]:
        """返回需要远程取数的月份列表.

        缺失判定:
        - 分片文件不存在 → 缺失(从未查过)
        - 月份 >= 当前月 → 缺失(数据可能不完整,永不信任)
        """
        cur = current_month()
        out = []
        for ym in months_between(start, end):
            if ym >= cur or not self.shard_path(kind, security, ym, variant).exists():
                out.append(ym)
        return out

    # ---------------- 读 ----------------

    def read_range(
        self,
        kind: str,
        security: object,
        start: object,
        end: object,
        variant: str = "default",
    ) -> pd.DataFrame:
        """读取 ``[start, end]`` 覆盖月份的所有已缓存数据(不含缺失月).

        返回按 index 升序、已按 [start, end] 精确切片的 DataFrame.
        """
        frames = []
        for ym in months_between(start, end):
            df = read_pkl(self.shard_path(kind, security, ym, variant))
            if isinstance(df, pd.DataFrame) and len(df) > 0:
                frames.append(df)
        if not frames:
            return pd.DataFrame()
        merged = pd.concat(frames).sort_index()
        merged = merged[~merged.index.duplicated(keep="last")]
        return merged.loc[norm_date(start) : norm_date(end)]

    # ---------------- 写 ----------------

    def write_range(
        self,
        df: pd.DataFrame,
        kind: str,
        security: object,
        covered_start: object,
        covered_end: object,
        variant: str = "default",
    ) -> None:
        """把 ``df`` 按月切片写入.

        ``covered_start/end`` 是本次远程查询**声明覆盖**的区间:
        该区间内没有数据的月份也会写入空 DataFrame 作为"已查过"标记,
        避免上市前/退市后/长期停牌区间被反复远程查询.

        当月及未来月不写入(数据不完整).
        """
        cur = current_month()
        has_index = isinstance(df, pd.DataFrame) and isinstance(
            df.index, pd.DatetimeIndex
        )
        for ym in months_between(covered_start, covered_end):
            if ym >= cur:
                continue
            if has_index:
                lo, hi = month_bounds(ym)
                part = df.loc[lo:hi]
            else:
                part = pd.DataFrame()
            atomic_write(self.shard_path(kind, security, ym, variant), part)


# ======================================================================
# 快照存储(无日期区间概念的 key-value 缓存)
# ======================================================================


class SnapshotStore:
    """``{root}/{version}/snapshot/{func}/{key}.pkl`` 形式的 key-value 缓存."""

    def __init__(self, root: Path):
        self.root = Path(root)

    def path(self, func: str, key_parts: object) -> Path:
        if not isinstance(key_parts, (list, tuple)):
            key_parts = (key_parts,)
        name = "__".join(safe_key(p) for p in key_parts)
        return (
            self.root / CACHE_VERSION / "snapshot" / safe_key(func) / f"{safe_key(name)}.pkl"
        )

    def get(self, func: str, key_parts: object) -> object | None:
        return read_pkl(self.path(func, key_parts))

    def put(self, func: str, key_parts: object, obj: object) -> None:
        atomic_write(self.path(func, key_parts), obj)
