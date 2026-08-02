"""JupyterHub REST + WebSocket 客户端（含 OAuth 自动续期）.

聚宽研究环境是标准 JupyterHub，暴露这套 API：
  - GET    /api/kernels              列 kernel
  - POST   /api/kernels              起 kernel（body {"name":"python3"}）
  - DELETE /api/kernels/{id}         关 kernel
  - WS     /api/kernels/{id}/channels  执行代码（Jupyter Messaging Protocol v5）

鉴权：Cookie 头 + X-XSRFToken 头（POST/DELETE 必需）。

**OAuth 自动续期**（关键设计）：
聚宽 JupyterHub 的 `user-{uid}` 会话 cookie 会定期过期，过期时任何
`/user/{uid}/api/*` 请求返回 302 → `/hub/api/oauth2/authorize`。只要主站
登录态（`token`/`PHPSESSID`）还活着，跟着这条重定向链走完 OAuth 回调，
JupyterHub 会**自动重发一个新鲜的 `user-{uid}` cookie**。本客户端在检测到
302→oauth 时，用一个 GET 探针（allow_redirects=True）走完该链，把刷新后的
cookie 同步回 CookieStore，再重试原请求——对调用方完全透明。

若主站登录态也失效，OAuth 会进入重定向死循环（TooManyRedirects），此时抛
CookieExpiredError 提示手动重新登录（或触发 refresh_callback）。

cookie 路径要点：`user-{uid}` 必须种到 `/user/{uid}/`（与服务端一致），
否则 OAuth 新发的同 path cookie 会与旧 path=/ 的 cookie 共存冲突。
"""

from __future__ import annotations

import contextlib
import json
import time
import uuid
from typing import Any
from urllib.parse import urlparse

import requests
import websocket  # type: ignore[import-untyped]

from jq.auth import (
    CookieExpiredError,
    CookieStore,
    RefreshCallback,
    is_expired_response,
    refresh_cookies_via_browser,
)
from jq.exceptions import JQExecutionError


class JupyterHubClient:
    """聚宽 JupyterHub API 客户端（同步，线程不安全——单实例单线程用）.

    Parameters
    ----------
    base_url:
        用户 JupyterHub 根，形如 ``https://www.joinquant.com/user/38001222945``。
    cookie_store:
        Cookie 读写器。默认用 :data:`~jq.auth.DEFAULT_COOKIE_PATH`。
    refresh_callback:
        可选 browser-use 刷新回调。当主站登录态也失效（OAuth 死循环）时触发。
    """

    # 瞬态/hub 内部 cookie，不持久化回 store
    _TRANSIENT_COOKIE_MARKERS = ("oauth-state", "jupyter-hub-token")

    def __init__(
        self,
        base_url: str,
        cookie_store: CookieStore | None = None,
        timeout: int = 30,
        refresh_callback: RefreshCallback | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.cookies = cookie_store or CookieStore()
        self.timeout = timeout
        self.refresh_callback = refresh_callback

        parsed = urlparse(self.base_url)
        self._host = parsed.netloc
        # uid = base_url 末段，如 38001222945
        self._uid = self.base_url.rsplit("/", 1)[-1]
        self._user_cookie_name = f"user-{self._uid}"
        self._user_cookie_path = f"/user/{self._uid}/"

        # 持久 Session：跨请求累积 cookie，承载 OAuth 续期
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
            ),
            "Referer": self.base_url + "/tree?",
        })
        self._last_synced_user: str | None = None
        self._load_cookies_into_session()

    # ------------------------------------------------------------------
    # cookie 装载 / 同步
    # ------------------------------------------------------------------
    def _load_cookies_into_session(self) -> None:
        """从 store 加载 cookie 到 Session jar，user-{uid} 种到正确路径."""
        cookies = self.cookies.load()
        self._session.cookies.clear()
        for name, value in cookies.items():
            path = self._user_cookie_path if name.startswith("user-") else "/"
            self._session.cookies.set(
                name, value, domain=self._host, path=path
            )
        self._last_synced_user = cookies.get(self._user_cookie_name)

    def _sync_cookies_to_store(self) -> None:
        """把 Session jar 里刷新后的 cookie 同步回 store（仅 user-{uid} 变化时写盘）."""
        jar_user = self._session.cookies.get(self._user_cookie_name) or ""
        if not jar_user or jar_user == self._last_synced_user:
            return
        persisted: dict[str, str] = {}
        for c in self._session.cookies:
            if not c.value:
                continue
            if any(m in c.name for m in self._TRANSIENT_COOKIE_MARKERS):
                continue
            persisted[c.name] = c.value
        if persisted:
            self.cookies.save(persisted)
            self._last_synced_user = jar_user

    def _xsrf(self) -> str:
        """从 Session jar 读 _xsrf（OAuth 续期后可能更新）；缺失则回退 store."""
        xsrf = self._session.cookies.get("_xsrf")
        if xsrf:
            return xsrf
        # 回退：store 里的 _xsrf（首次加载时已进 jar，理论上不会走到这）
        return self.cookies.xsrf()

    def _refresh_session(self) -> None:
        """GET 探针走完 OAuth 重定向链，刷新 user-{uid} cookie.

        主站登录态失效时 OAuth 会死循环 → TooManyRedirects。
        OAuth 偶发首次未发新 cookie（瞬态），重试最多 3 次，仍无变化
        视为主站登录态失效。
        """
        old_user = self._session.cookies.get(self._user_cookie_name) or ""
        for _ in range(3):
            try:
                self._session.get(
                    self.base_url + "/api/kernelspecs",
                    headers={"X-XSRFToken": self._xsrf()},
                    timeout=self.timeout,
                    allow_redirects=True,
                )
            except requests.TooManyRedirects:
                # OAuth 死循环 = 主站登录态也失效
                if self.refresh_callback is not None:
                    refresh_cookies_via_browser(self.refresh_callback, self.cookies)
                    self._load_cookies_into_session()
                    return
                raise CookieExpiredError(
                    "主站登录态已失效（OAuth 重定向死循环）。\n"
                    "请重新登录 joinquant.com 后执行：jqdata cookies '<新 cookie>'"
                ) from None
            new_user = self._session.cookies.get(self._user_cookie_name) or ""
            if new_user and new_user != old_user:
                # cookie 真刷新了
                self._sync_cookies_to_store()
                return
            # 未刷新，重试（瞬态：OAuth 探针偶发首次未发新 cookie）
        # 3 次都未刷新 → 主站登录态失效
        if self.refresh_callback is not None:
            refresh_cookies_via_browser(self.refresh_callback, self.cookies)
            self._load_cookies_into_session()
            return
        raise CookieExpiredError(
            "OAuth 续期 3 次未刷新 user cookie，主站登录态可能已失效。\n"
            "请重新登录 joinquant.com 后执行：jqdata cookies '<新 cookie>'"
        )

    # ------------------------------------------------------------------
    # 请求辅助
    # ------------------------------------------------------------------
    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict | None = None,
        _attempts: int = 0,
    ) -> Any:
        url = f"{self.base_url}{path}"
        resp = self._session.request(
            method,
            url,
            json=json_body,
            headers={"X-XSRFToken": self._xsrf()},
            timeout=self.timeout,
            allow_redirects=False,  # 自己控制：过期时走 _refresh_session 探针
        )
        body_preview = resp.text[:2048] if resp.text else ""
        location = resp.headers.get("Location", "")
        if is_expired_response(resp.status_code, body_preview, location):
            # user-{uid} 过期 → OAuth 续期后重试（最多 2 次续期机会，应对瞬态）
            if _attempts >= 2:
                raise CookieExpiredError(
                    f"cookie 过期，OAuth 续期 {_attempts} 次后仍失败。URL={url}"
                )
            self._refresh_session()
            return self._request(
                method, path, json_body=json_body, _attempts=_attempts + 1
            )
        # 续期/普通请求后，把可能刷新的 cookie 同步回 store
        self._sync_cookies_to_store()
        if not resp.ok:
            raise JQExecutionError(
                f"JupyterHub {method} {path} 失败：HTTP {resp.status_code} {resp.reason}\n"
                f"{resp.text[:500]}"
            )
        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError:
            return resp.text

    # ------------------------------------------------------------------
    # kernels REST
    # ------------------------------------------------------------------
    def list_kernels(self) -> list[dict]:
        """列出当前活跃 kernel。返回 [{id, name, execution_state, last_activity}, ...]."""
        data = self._request("GET", "/api/kernels")
        if isinstance(data, dict):
            return data.get("value", data.get("kernels", [])) or []
        return data or []

    def list_kernelspecs(self) -> dict:
        """列可用 kernel 规格。返回 kernelspecs 原始 dict."""
        return self._request("GET", "/api/kernelspecs") or {}

    def start_kernel(self, name: str = "python3") -> dict:
        """启动 kernel，返回 {id, name}."""
        return self._request("POST", "/api/kernels", json_body={"name": name})

    def delete_kernel(self, kernel_id: str) -> None:
        """关闭 kernel。404 视为已删（幂等）。"""
        try:
            self._request("DELETE", f"/api/kernels/{kernel_id}")
        except JQExecutionError as e:
            if "404" in str(e):
                return
            raise

    def interrupt_kernel(self, kernel_id: str) -> None:
        """中断 kernel 当前执行（用于超时清理）."""
        with contextlib.suppress(JQExecutionError):
            self._request("POST", f"/api/kernels/{kernel_id}/interrupt")

    # ------------------------------------------------------------------
    # execute via WebSocket
    # ------------------------------------------------------------------
    def execute(
        self,
        kernel_id: str,
        code: str,
        *,
        timeout: float = 60.0,
        silent: bool = False,
    ) -> dict:
        """在指定 kernel 上执行代码，返回结构化输出.

        Returns
        -------
        dict with keys:
            - ``stdout``: str   合并的 stdout
            - ``stderr``: str   合并的 stderr
            - ``result``: str   execute_result 的 text/plain（最后一个）
            - ``error``: dict | None  形如 {ename, evalue, traceback}
            - ``status``: str   "ok" / "error" / "timeout"

        Notes
        -----
        按 Jupyter Messaging Protocol v5：发 ``execute_request``（shell 通道），
        收 ``stream`` / ``execute_result`` / ``error`` / ``status(idle)``，
        以 parent_header.msg_id 过滤本请求回包。WS 鉴权用 Session jar 里
        （已 OAuth 续期的）cookie。
        """
        ws_url = (
            self.base_url.replace("http://", "ws://").replace("https://", "wss://")
            + f"/api/kernels/{kernel_id}/channels"
        )
        # 从 Session jar 构造 cookie 头（含续期后的新鲜 user-{uid}）
        cookie_header = "; ".join(
            f"{c.name}={c.value}" for c in self._session.cookies if c.value
        )
        xsrf = self._xsrf()
        msg_id = str(uuid.uuid4())

        request = {
            "header": {
                "msg_id": msg_id,
                "username": "jq",
                "msg_type": "execute_request",
                "version": "5.3",
            },
            "parent_header": {},
            "metadata": {},
            "channel": "shell",
            "content": {
                "code": code,
                "silent": silent,
                "store_history": not silent,
                "user_expressions": {},
                "allow_stdin": False,
                "stop_on_error": True,
            },
        }

        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []
        result_text: str = ""
        error: dict | None = None
        status = "ok"

        try:
            ws = websocket.create_connection(
                ws_url,
                header=[f"Cookie: {cookie_header}", f"X-XSRFToken: {xsrf}"],
                timeout=timeout,
            )
        except Exception as e:
            raise JQExecutionError(
                f"无法连接 kernel WebSocket：{e}\nURL={ws_url}"
            ) from e

        try:
            ws.send(json.dumps(request))
            deadline = time.time() + timeout
            while time.time() < deadline:
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    status = "timeout"
                    self.interrupt_kernel(kernel_id)
                    break
                if not raw:
                    continue
                try:
                    msg = json.loads(raw)
                except (ValueError, json.JSONDecodeError):
                    continue

                parent = msg.get("parent_header", {}) or {}
                parent_id = parent.get("msg_id")
                if parent_id and parent_id != msg_id:
                    continue

                mtype = msg.get("msg_type") or msg.get("header", {}).get("msg_type", "")
                content = msg.get("content", {}) or {}

                if mtype == "stream":
                    text = content.get("text", "")
                    if content.get("name") == "stderr":
                        stderr_chunks.append(text)
                    else:
                        stdout_chunks.append(text)
                elif mtype == "execute_result":
                    data = content.get("data", {}) or {}
                    if "text/plain" in data:
                        result_text = data["text/plain"]
                elif mtype == "error":
                    error = {
                        "ename": content.get("ename", ""),
                        "evalue": content.get("evalue", ""),
                        "traceback": content.get("traceback", []),
                    }
                    status = "error"
                elif mtype == "status" and content.get("execution_state") == "idle":
                    break
            else:
                status = "timeout"
                self.interrupt_kernel(kernel_id)
        finally:
            with contextlib.suppress(Exception):
                ws.close()

        return {
            "stdout": "".join(stdout_chunks),
            "stderr": "".join(stderr_chunks),
            "result": result_text,
            "error": error,
            "status": status,
        }
