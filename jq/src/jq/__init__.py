"""jq — 聚宽 JoinQuant Jupyter 代码执行桥.

不是 jqdata 的本地 SDK（聚宽没提供，jqdata 只在聚宽研究环境 kernel 里可用），
而是更底层的"远程 Jupyter 代码执行"原语：通过聚宽研究环境（标准 JupyterHub）
的 REST + WebSocket API，在远程 kernel 里执行任意 Python 代码并取回输出。

上层可用它封装出数据取用逻辑（调用 jqdata 的 get_price 等）。

依赖：
    pip install jq
    # 即 requests + websocket-client + pandas
"""

from jq.auth import (
    DEFAULT_BASE_URL,
    DEFAULT_COOKIE_PATH,
    CookieExpiredError,
    CookieStore,
    parse_cookie_string,
)
from jq.client import JupyterHubClient
from jq.exceptions import JQAuthError, JQError, JQExecutionError
from jq.runner import (
    JoinQuantRunner,
    KernelSession,
)

__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_COOKIE_PATH",
    "CookieExpiredError",
    "CookieStore",
    "JQAuthError",
    "JQError",
    "JQExecutionError",
    "JupyterHubClient",
    "JoinQuantRunner",
    "KernelSession",
    "parse_cookie_string",
]
