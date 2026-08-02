"""高层 runner：run_code / run_dataframe / KernelSession.

约定：
  - ``run_code(code)`` 一次性起 kernel → 执行 → 关 kernel，返回原始输出 dict
  - ``run_dataframe(code)`` 约定用户代码把目标 DataFrame 赋给 ``__result__``，
    runner 自动追加序列化语句，本地反序列化为 pandas.DataFrame
  - ``KernelSession`` 长连 kernel，多次 run_code 复用同一 kernel（快但有状态）

聚宽 jqdata 用法示例（在 run_dataframe 里）::

    runner.run_dataframe('''
        from jqdata import get_price
        __result__ = get_price("000001.XSHE",
                                start_date="2024-01-01",
                                end_date="2024-06-30",
                                frequency="daily")
    ''')
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pandas as pd

from jq.auth import (
    DEFAULT_BASE_URL,
    CookieStore,
    RefreshCallback,
)
from jq.client import JupyterHubClient
from jq.exceptions import JQExecutionError
from jq.serialize import PICKLE_TRAILER, decode_result

try:  # 文件锁: POSIX 有 fcntl, Windows 退化为无锁(单机单进程仍安全)
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

# 持久化 kernel 状态文件:跨命令复用 kernel,省冷启动 + import jqdata 固定开销。
KERNEL_STATE_PATH = Path.home() / ".jq" / "kernel.json"


class KernelRegistry:
    """持久化 kernel 状态管理(~/.jq/kernel.json).

    记录最近复用的 kernel_id + base_url + kernel_name + 时间戳,
    让 CLI 跨命令复用同一 kernel,省掉每次 start_kernel + import jqdata
    的固定开销(实测约 2.7s)。

    写入是**原子**的(tmp + os.replace), 并提供跨进程锁,
    避免并发创建 kernel 时互相覆盖状态导致服务端 kernel 泄漏。
    """

    def __init__(self, path: Path = KERNEL_STATE_PATH) -> None:
        self.path = path

    def load(self) -> dict | None:
        if not self.path.exists():
            return None
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            return None

    def _write(self, data: dict) -> None:
        """原子写: 先写同目录 tmp 再 replace, 避免并发/中断留下坏 JSON."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(f".json.tmp.{os.getpid()}")
        try:
            tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
            os.replace(tmp, self.path)
        finally:
            tmp.unlink(missing_ok=True)

    def save(self, kernel_id: str, base_url: str, kernel_name: str) -> None:
        self._write(
            {
                "kernel_id": kernel_id,
                "base_url": base_url,
                "kernel_name": kernel_name,
                "created_at": time.time(),
                "last_used": time.time(),
            }
        )

    def touch(self) -> None:
        """更新 last_used(复用时调用)。"""
        data = self.load()
        if data:
            data["last_used"] = time.time()
            self._write(data)

    def clear(self) -> None:
        """删除状态文件(kernel 死亡或显式关闭时调用)。"""
        self.path.unlink(missing_ok=True)

    @contextmanager
    def lock(self) -> Iterator[None]:
        """跨进程互斥锁(仅保护"创建 kernel"这一小段临界区).

        并发的多个进程若同时发现无可用 kernel, 会各自创建一个并互相覆盖
        registry —— 被覆盖的那个 kernel 在服务端泄漏. 加锁后只有一个进程
        创建, 其余进程进入临界区后会读到已就绪的状态并直接复用.
        """
        if fcntl is None:  # pragma: no cover
            yield
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self.path.with_suffix(".lock")
        with open(lock_path, "w") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)


def _extract_dataframe(result: dict) -> pd.DataFrame:
    """从 execute 结果里提取 ``__result__``.

    约定用户代码已把目标对象赋给 ``__result__``,
    runner 追加的 trailer 会把它 base64-pickle 到 stdout 标记区内.

    用 pickle 而非 ``to_json(orient='table')``: 保留 dtype/时区/MultiIndex,
    且支持非 DataFrame 结果(dict / list / ndarray).
    """
    return decode_result(result)


class JoinQuantRunner:
    """聚宽代码执行 runner（一次性 kernel 模式）.

    每次调用都起一个新 kernel、执行、关闭——无状态、最干净，适合间歇取数。
    批量连续取数请用 :class:`KernelSession`。
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        cookie_store: CookieStore | None = None,
        kernel_name: str = "python3",
        refresh_callback: RefreshCallback | None = None,
        timeout: int = 30,
        persistent: bool = False,
    ) -> None:
        self.client = JupyterHubClient(
            base_url=base_url,
            cookie_store=cookie_store or CookieStore(),
            timeout=timeout,
            refresh_callback=refresh_callback,
        )
        self.kernel_name = kernel_name
        self.persistent = persistent
        self.registry = KernelRegistry()

    def run_code(self, code: str, *, timeout: float = 60.0) -> dict:
        """执行任意代码，返回原始输出 dict（见 JupyterHubClient.execute）.

        - ``persistent=False``（默认）：一次性，起 kernel → 执行 → 关 kernel。
        - ``persistent=True``：复用持久化 kernel（~/.jq/kernel.json），
          首次创建时预 ``from jqdata import *``，后续命令直接执行，
          省掉每次 start_kernel + import 的固定开销（实测约 2.7s）。

        持久模式走**乐观路径**: 直接在记录的 kernel 上执行, 不先探活
        (省一次 list_kernels 往返, 实测约 40ms/次); 只有执行抛异常时才探活,
        确认 kernel 已死则重建并重试一次。
        """
        if not self.persistent:
            kernel = self.client.start_kernel(self.kernel_name)
            kid = kernel["id"]
            try:
                return self.client.execute(kid, code, timeout=timeout)
            finally:
                self.client.delete_kernel(kid)

        kid = self._get_or_create_persistent_kernel()
        try:
            result = self.client.execute(kid, code, timeout=timeout)
        except Exception:
            # 乐观路径失败: kernel 还活说明是其他问题(代码错/超时),直接上抛;
            # 确认已死则重建并重试一次(服务端 cull 后的典型场景)。
            if self.kernel_alive(kid):
                raise
            self.registry.clear()
            kid = self._get_or_create_persistent_kernel()
            result = self.client.execute(kid, code, timeout=timeout)
        self.registry.touch()
        return result

    def kernel_alive(self, kernel_id: str) -> bool:
        """探活:kernel_id 是否在服务端活跃列表中。"""
        try:
            kernels = self.client.list_kernels()
        except Exception:
            return False
        return any(k.get("id") == kernel_id for k in kernels)

    def _get_or_create_persistent_kernel(self) -> str:
        """复用持久化 kernel 或创建新(含预 import jqdata).

        乐观路径: 记录存在且 base_url 匹配则**直接复用**, 不发探活请求
        (死 kernel 由 :meth:`run_code` 的异常分支兼容). 创建路径加跨进程锁,
        避免并发创建造成服务端 kernel 泄漏。
        """
        data = self.registry.load()
        if data and data.get("base_url") == self.client.base_url:
            kid = data.get("kernel_id", "")
            if kid:
                return kid

        with self.registry.lock():
            # 临界区内重读: 可能已被其他进程创建好了
            data = self.registry.load()
            if data and data.get("base_url") == self.client.base_url:
                kid = data.get("kernel_id", "")
                if kid and self.kernel_alive(kid):
                    return kid
                self.registry.clear()
            # 新建 + 预 import jqdata(付一次 ~1.9s,后续命令省)
            kid = self.client.start_kernel(self.kernel_name)["id"]
            self.client.execute(kid, "from jqdata import *", timeout=60.0)
            self.registry.save(kid, self.client.base_url, self.kernel_name)
            return kid

    def close_persistent(self) -> bool:
        """显式关闭持久化 kernel。返回是否有关闭过。"""
        data = self.registry.load()
        if not data:
            return False
        kid = data.get("kernel_id", "")
        if kid:
            self.client.delete_kernel(kid)
        self.registry.clear()
        return True

    def warmup(self) -> str:
        """预创建持久化 kernel + 预 import jqdata(不执行用户代码).

        已有存活 kernel 则复用,否则新建 + 预 import。
        让首次 ``run`` 也享复用速度(省创建 + import 约 2.7s)。
        """
        self.persistent = True
        return self._get_or_create_persistent_kernel()

    def run_dataframe(self, code: str, *, timeout: float = 60.0) -> pd.DataFrame:
        """执行代码并返回 DataFrame.

        约定：用户代码须把目标 DataFrame 赋给变量 ``__result__``。
        runner 自动追加序列化语句，本地还原为 pandas.DataFrame。
        """
        result = self.run_code(code + PICKLE_TRAILER, timeout=timeout)
        return _extract_dataframe(result)


@contextmanager
def persistent_kernel(runner: JoinQuantRunner) -> Iterator[str]:
    """获取持久化 kernel_id 的上下文(不删除,供需要 kid 的场景)。"""
    if not runner.persistent:
        raise JQExecutionError("runner 未开启 persistent 模式")
    kid = runner._get_or_create_persistent_kernel()
    try:
        yield kid
    finally:
        runner.registry.touch()


class KernelSession:
    """长连 kernel，多次 run_code 复用同一 kernel（快但有状态）.

    用法::

        with KernelSession(runner) as sess:
            sess.run_code("from jqdata import get_price")
            df = sess.run_dataframe('__result__ = get_price("000001.XSHE", ...)')

    状态会累积（变量保留），适合连续交互；退出 with 块自动关 kernel。
    """

    def __init__(
        self,
        runner: JoinQuantRunner,
        kernel_name: str | None = None,
    ) -> None:
        self.runner = runner
        self.kernel_name = kernel_name or runner.kernel_name
        self._kernel_id: str | None = None

    def __enter__(self) -> KernelSession:
        self._kernel_id = self.runner.client.start_kernel(self.kernel_name)["id"]
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """关闭 kernel（幂等）。"""
        if self._kernel_id is not None:
            try:
                self.runner.client.delete_kernel(self._kernel_id)
            finally:
                self._kernel_id = None

    @property
    def kernel_id(self) -> str | None:
        return self._kernel_id

    def run_code(self, code: str, *, timeout: float = 60.0) -> dict:
        """在复用 kernel 上执行代码。"""
        if self._kernel_id is None:
            raise JQExecutionError("KernelSession 未进入 with 块或已关闭")
        return self.runner.client.execute(self._kernel_id, code, timeout=timeout)

    def run_dataframe(self, code: str, *, timeout: float = 60.0) -> pd.DataFrame:
        """在复用 kernel 上执行并返回 DataFrame（约定 ``__result__``）。"""
        result = self.run_code(code + PICKLE_TRAILER, timeout=timeout)
        return _extract_dataframe(result)


@contextmanager
def kernel_session(
    runner: JoinQuantRunner,
    kernel_name: str | None = None,
) -> Iterator[KernelSession]:
    """KernelSession 的函数式等价（兼容 with 用法）。"""
    with KernelSession(runner, kernel_name=kernel_name) as sess:
        yield sess
