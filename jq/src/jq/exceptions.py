"""jq 包异常体系。

独立于 qlab.core —— jq 是自包含的包，不依赖 qlab。
按错误来源分层；调用方用 isinstance 判断时只需关心顶层 ``JQError``。
"""

from __future__ import annotations


class JQError(Exception):
    """所有 jq 异常的基类."""


class JQAuthError(JQError):
    """鉴权/cookie 相关错误（cookie 过期、缺失、损坏、主站登录态失效）."""


class JQExecutionError(JQError):
    """远程 kernel 执行错误（HTTP 失败、WS 连接失败、代码异常、反序列化失败）."""
