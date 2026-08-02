"""远程结果序列化 — 在聚宽 kernel 与本地之间安全传输 Python 对象.

聚宽研究环境是 **Python 3.6 + 老版 pandas**, 本地通常是 3.10+ / pandas 2.x.
跨这条边界传输 DataFrame 有两个坑:

1. ``DataFrame.to_json(orient='table')`` 会丢失时区、把整型误判为浮点、
   对 MultiIndex 与非标量列(如 dict/list)支持不佳;
2. 直接 pickle 老版 pandas 对象, 其 ``Int64Index`` / ``Float64Index`` 等类
   在 pandas 2.x 已被删除, 反序列化抛
   ``ModuleNotFoundError: pandas.core.indexes.numeric``(空 DataFrame 必现).

本模块的方案: **远程侧先净化再 base64-pickle**, 本地从 stdout 标记区解码.
净化只降级"两版本不共存"的 index 类型, 不改动数据本身.

信任边界: 只解码本连接器自己在受信远程 kernel 中生成的 payload.
"""

from __future__ import annotations

import base64
import pickle

from jq.exceptions import JQExecutionError

# 用唯一 token 把 payload 从用户 stdout 里隔离出来, 避免用户 print 干扰解析.
PICKLE_START = "@@@JQ_CACHE_START@@@"
PICKLE_END = "@@@JQ_CACHE_END@@@"

# 远程侧净化: 两件事——
# (1) 把老版 pandas 专有的 index 类型降级为两版本都存在的类型;
# (2) 把 object 列里的**非内置类型值**(如 finance.run_query 返回的
#     sqlalchemy 包装对象)转为纯 Python 值, 否则 pickle 回本地报
#     ``ModuleNotFoundError: sqlalchemy`` 之类。
_SANITIZE_CODE = '''
import pandas as _pd_sz
_LEGACY_IDX = ('Int64Index', 'UInt64Index', 'Float64Index')
_SAFE_MODS = ('builtins', 'datetime', 'decimal', 'numpy', 'pandas')


def _jq_scalar(v):
    """把可能携带第三方依赖的标量降级为纯 Python/numpy 值."""
    mod = type(v).__module__.split('.')[0]
    if v is None or mod in _SAFE_MODS:
        return v
    # 未知来源的对象: 优先试数值, 否则落到字符串
    try:
        f = float(v)
        return f
    except (TypeError, ValueError):
        return str(v)


def _jq_clean_cols(o):
    """对 object 列逐元素净化(只处理含非安全类型的列, 避免全表扫描)."""
    for _c in o.columns:
        if o[_c].dtype != object:
            continue
        _s = o[_c]
        _nonnull = _s.dropna()
        if len(_nonnull) == 0:
            continue
        _mod = type(_nonnull.iloc[0]).__module__.split('.')[0]
        if _mod not in _SAFE_MODS:
            o[_c] = _s.map(_jq_scalar)
    return o


def _jq_sanitize(o):
    if isinstance(o, _pd_sz.DataFrame):
        if len(o) == 0 or o.index.__class__.__name__ in _LEGACY_IDX:
            o = o.reset_index(drop=True)
        o = o.copy()
        # 列名强制转纯 str: finance.run_query 返回的列名是
        # sqlalchemy.sql.elements.quoted_name, 直接 pickle 会嵌入该类引用,
        # 本地无 sqlalchemy 则 loads 报 ModuleNotFoundError。
        o.columns = [str(c) for c in o.columns]
        o = _jq_clean_cols(o)
        return o
    if isinstance(o, _pd_sz.Series):
        if len(o) == 0 or o.index.__class__.__name__ in _LEGACY_IDX:
            o = o.reset_index(drop=True)
        if o.name is not None:
            o = o.rename(str(o.name))
        if o.dtype == object and len(o.dropna()) > 0:
            _mod = type(o.dropna().iloc[0]).__module__.split('.')[0]
            if _mod not in _SAFE_MODS:
                o = o.map(_jq_scalar)
        return o
    if isinstance(o, dict):
        return dict((str(k), _jq_sanitize(v)) for k, v in o.items())
    if isinstance(o, list):
        return [_jq_sanitize(v) for v in o]
    return o


__result__ = _jq_sanitize(__result__)
'''

#: 追加到用户代码末尾的序列化片段. 用户代码须把目标对象赋给 ``__result__``.
PICKLE_TRAILER = (
    _SANITIZE_CODE
    + "\nimport pickle as _pk, base64 as _b64, sys as _sys\n"
    + f"_sys.stdout.write('\\n{PICKLE_START}\\n')\n"
    + "_sys.stdout.write(_b64.b64encode(_pk.dumps(__result__, protocol=4)).decode())\n"
    + f"_sys.stdout.write('\\n{PICKLE_END}\\n')\n"
)


def raise_if_remote_error(result: dict) -> None:
    """远程执行有异常时抛 :class:`JQExecutionError`(含 traceback)."""
    err = result.get("error")
    if not err:
        return
    tb = "\n".join(err.get("traceback", []))
    raise JQExecutionError(
        f"远程执行报错: {err.get('ename')}: {err.get('evalue')}\n{tb}"
    )


def decode_result(result: dict):
    """从 execute 结果的 stdout 标记区解码 ``__result__``.

    Raises:
        JQExecutionError: 远程报错、返回空输出(数据量过大/kernel 异常),
            或未找到序列化标记(用户代码没给 ``__result__`` 赋值)。
    """
    raise_if_remote_error(result)
    out = result.get("stdout", "")
    i = out.find(PICKLE_START)
    j = out.find(PICKLE_END)
    if i < 0 or j < 0:
        # 区分两种截然不同的原因 —— 早前统一报"忘赋值 __result__"
        # 会把排查引向错误方向(实测曾因此误判为序列化 bug)。
        if not out.strip():
            raise JQExecutionError(
                "远程返回空输出(stdout 为空且无错误) —— 代码很可能未真正执行。\n"
                "  常见原因(按可能性排序):\n"
                "  1. 单次请求数据量过大 —— 如 get_price 取 40 只×24 月(≈万行),"
                "聚宽 kernel 会静默返回空。请缩小标的数或时间跨度后重试\n"
                "     (批量接口可调 max_symbol_months / chunk_size 参数)。\n"
                "  2. kernel 已死掉或连接被断 —— 试 `jqdata kernel restart`。\n"
                "  3. 执行超时被中断(本次超时设置可能偏小)。"
            )
        raise JQExecutionError(
            "未找到序列化标记 —— 请确认代码已把目标对象赋给 `__result__`。\n"
            f"stdout 末尾:\n{out[-500:]}"
        )
    payload = out[i + len(PICKLE_START) : j].strip()
    if not payload:
        raise JQExecutionError("序列化内容为空(__result__ 是否为 None?)")
    return pickle.loads(base64.b64decode(payload))  # noqa: S301


def encode_for_test(obj: object) -> str:
    """把对象编码成远程侧输出格式 —— 仅供测试构造假响应."""
    payload = base64.b64encode(pickle.dumps(obj)).decode()
    return f"\n{PICKLE_START}\n{payload}\n{PICKLE_END}\n"
