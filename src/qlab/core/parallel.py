"""Parallel primitives — mpPandasObj from Ch20.

并行原语。书中 Ch20 的 mpPandasObj 重命名为 mp_pandas_obj（PEP8 风格）。
"""

from __future__ import annotations

import multiprocessing as mp
import time
from collections.abc import Callable, Sequence
from typing import Any

import numpy as np
import pandas as pd


def linear_partitions(num_atoms: int, num_threads: int) -> np.ndarray:
    """线性等分原子任务为 num_threads 个分子.

    返回长度 num_threads+1 的索引数组，相邻两元素构成一个分子的 [start, end) 区间。
    """
    parts = np.linspace(0, num_atoms, min(num_threads, num_atoms) + 1)
    return np.ceil(parts).astype(int)


def nested_partitions(num_atoms: int, num_threads: int,
                      upper_triang: bool = False) -> np.ndarray:
    """嵌套循环（三角形结构）的均衡分割.

    适用于：SADF、triple-barrier、错位序列协方差等
    内层循环长度随外层变化的场景。

    upper_triang=True 用于上三角（内层 j = i, ..., N）。
    """
    parts: list[float] = [0.0]
    num_threads_ = min(num_threads, num_atoms)
    for _ in range(num_threads_):
        part = 1 + 4 * (parts[-1] ** 2 + parts[-1] +
                        num_atoms * (num_atoms + 1.0) / num_threads_)
        part = (-1 + part ** 0.5) / 2.0
        parts.append(part)
    parts_arr = np.round(parts).astype(int)
    if upper_triang:
        parts_arr = np.cumsum(np.diff(parts_arr)[::-1])
        parts_arr = np.append(np.array([0]), parts_arr)
    return parts_arr


def _expand_call(kargs: dict[str, Any]) -> Any:
    """展开 kargs 的回调调用. 内部使用."""
    func = kargs["func"]
    del kargs["func"]
    return func(**kargs)


def _report_progress(job_num: int, num_jobs: int, time0: float, task: str) -> None:
    elapsed_min = (time.time() - time0) / 60.0
    pct = job_num / num_jobs
    remain_min = elapsed_min * (1 / pct - 1) if pct > 0 else 0
    msg = (f"  {pct*100:5.1f}% {task} | "
           f"elapsed {elapsed_min:.2f}m | remain {remain_min:.2f}m")
    end = "\r" if job_num < num_jobs else "\n"
    print(msg, end=end, flush=True)


def _process_jobs_serial(jobs: list[dict[str, Any]]) -> list[Any]:
    """串行执行（调试模式）."""
    return [_expand_call(job) for job in jobs]


def _guard_spawn_reentry(num_threads: int) -> None:
    """拦住“脚本忘写 ``if __name__ == '__main__':``”导致的递归建进程.

    macOS/Windows 默认 spawn: 子进程会 **重新 import ``__main__``**。
    若调用方把 ``num_threads > 1`` 的调用写在模块顶层, 子进程 import 时
    会再建一批进程 —— CPython 会抛一个长达百行、每个子进程各刷一遍的
    ``freeze_support`` 堆栈, 真正的成因埋在里面看不出来。这里提前报一句人话。
    """
    if getattr(mp.current_process(), "_inheriting", False):
        raise RuntimeError(
            f"检测到在子进程 import __main__ 的过程中又要建 {num_threads} 个进程 ——"
            "调用方脚本忘了把入口包起来。\n"
            "  macOS/Windows 的 multiprocessing 用 spawn, 子进程会重新 import 你的脚本。\n"
            "  改法(二选一):\n"
            "    1. 把入口包进 if __name__ == '__main__': 里;\n"
            "    2. 传 num_threads=1 跑串行。\n"
            "  (Jupyter / pytest 下无此问题。)"
        )


def _process_jobs_parallel(jobs: list[dict[str, Any]], *,
                           task: str | None = None,
                           num_threads: int = 24,
                           verbose: bool = True) -> list[Any]:
    """并行执行."""
    if task is None:
        task = jobs[0]["func"].__name__
    _guard_spawn_reentry(num_threads)
    with mp.Pool(processes=num_threads) as pool:
        # 用 imap 而非 imap_unordered: 必须保证**返回顺序 == 提交顺序**。
        # 下游的 pd.concat 靠这个顺序拼回原始行序, 而当索引有重复值时
        # sort_index 无法从乱序中恢复 —— 乱序会让结果静默错配到别的行上。
        # imap 仍然全并行执行, 只是按序 yield, 吞吐不受影响。
        outputs = pool.imap(_expand_call, jobs)
        time0 = time.time()
        out: list[Any] = []
        for i, result in enumerate(outputs, 1):
            out.append(result)
            if verbose:
                _report_progress(i, len(jobs), time0, task)
    return out


def mp_pandas_obj(
    func: Callable[..., Any],
    pd_obj: tuple[str, Sequence[Any]],
    num_threads: int = 1,
    mp_batches: int = 1,
    linear: bool = True,
    verbose: bool = False,
    **kargs: Any,
) -> Any:
    """并行调用 func，把任务按 pd_obj[1] 分割为多个 molecule.

    参数
    ----
    func : 回调函数。必须接受 pd_obj[0] 作为关键字参数（传入 molecule）。
    pd_obj : (arg_name, atoms_list)。arg_name 是 func 中接收 molecule 的参数名；
             atoms_list 是要分割的原子任务列表（通常是 index）。
    num_threads : 并行进程数。1 表示串行执行（调试用）。

        .. warning::
           > 1 时走 multiprocessing, 有两个约束:

           1. ``func`` 必须是**模块级**函数 —— 局部函数/lambda 无法 pickle,
              会报 ``Can't pickle local object``。
           2. macOS/Windows 默认 spawn, 子进程重新导入 ``__main__``,
              调用方脚本需把入口包在 ``if __name__ == "__main__":`` 里,
              否则无限递归创建进程。（Jupyter / pytest 下无此问题。）
    mp_batches : 每核任务批数。> 1 时分子数 > 核心数，负载更均匀。
    linear : True 用线性分割；False 用嵌套（三角形）分割。
    verbose : 是否打印进度。
    **kargs : 透传给 func 的其他参数。

    返回
    ----
    若 func 返回 DataFrame/Series，则拼接后排序返回；否则返回 list。

    .. note::
       子任务结果按**提交顺序**拼接, 排序用稳定排序 —— 因此当索引含
       重复值时(如多标的共享同一 ``event_start``), 相同索引内部的相对顺序
       与 ``num_threads`` **无关**。不保证这一点的话, 调用方按位置对齐
       结果时会静默错配。
    """
    arg_name, atoms = pd_obj[0], list(pd_obj[1])
    num_atoms = len(atoms)
    if num_atoms == 0:
        return pd.DataFrame()

    n_partitions = min(num_threads * mp_batches, num_atoms)
    if linear:
        parts = linear_partitions(num_atoms, n_partitions)
    else:
        parts = nested_partitions(num_atoms, n_partitions)

    jobs: list[dict[str, Any]] = []
    for i in range(1, len(parts)):
        job = {arg_name: atoms[parts[i - 1]:parts[i]], "func": func}
        job.update(kargs)
        jobs.append(job)

    if num_threads <= 1:
        out = _process_jobs_serial(jobs)
    else:
        out = _process_jobs_parallel(jobs, num_threads=num_threads, verbose=verbose)

    # 拼接输出
    if not out:
        return pd.DataFrame()

    sample = out[0]
    if isinstance(sample, pd.DataFrame | pd.Series):
        # kind="stable": 默认 quicksort 在重复索引下**不稳定**, 会把已经排好的
        # 同索引行重新洗牌, 让输出顺序随分片数变化。
        return pd.concat(out, axis=0).sort_index(kind="stable")
    return out
