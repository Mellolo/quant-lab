"""Cookie 管理 + 过期检测。

聚宽研究环境是标准 JupyterHub，鉴权基于浏览器 cookie：
  - `user-<uid>`：JupyterHub 会话
  - `token`：聚宽主站登录态
  - `_xsrf`：Jupyter 防 CSRF，所有 POST/DELETE 必须带 `X-XSRFToken` 头
  - `PHPSESSID`：主站 PHP 会话

存储策略：`~/.jq/cookies.json`，文件权限 600，不入 git。
若新路径不存在但检测到旧路径 `~/.qlab/jq_cookies.json`（从 qlab 抽包前的遗留），
会一次性自动迁移到新路径，老用户无感。

过期策略（混合方案）：默认手动 `jqdata cookies '<raw>'` 更新；
检测到过期时可触发 `refresh_cookies_via_browser()` 钩子（browser-use fallback）。
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from jq.exceptions import JQAuthError


class CookieExpiredError(JQAuthError):
    """Cookie 过期或缺失，需更新后重试。"""


# 默认存储路径：~/.jq/cookies.json（权限 600，跨项目共享，不入 git）
DEFAULT_COOKIE_PATH = Path.home() / ".jq" / "cookies.json"

# 抽包前的旧路径（qlab 时代）。用于一次性迁移。
_LEGACY_COOKIE_PATH = Path.home() / ".qlab" / "jq_cookies.json"

# 默认 base_url：聚宽研究环境（JupyterHub）。
# 优先读环境变量 JQ_BASE_URL（他人使用时配置自己的 uid）；
# 未配置时 fallback 到作者账号，方便本仓库自用不破坏。
# 放这里是因为它只依赖字符串常量，不引入 requests/websocket，
# 便于 cli.py 在不触发可选依赖时也能拿到默认值。
DEFAULT_BASE_URL = os.environ.get(
    "JQ_BASE_URL", "https://www.joinquant.com/user/38001222945"
)


def parse_cookie_string(raw: str) -> dict[str, str]:
    """把浏览器复制的 `k1=v1; k2=v2; ...` 字符串解析成 dict.

    容忍空白、换行、多余分号；非 `k=v` 形式的片段静默跳过。
    """
    cookies: dict[str, str] = {}
    for part in re.split(r"[;\n\r]", raw):
        part = part.strip()
        if not part or "=" not in part:
            continue
        key, _, val = part.partition("=")
        key = key.strip()
        if key:
            cookies[key] = val.strip()
    return cookies


def is_expired_response(status: int, body: str, location: str = "") -> bool:
    """判断 HTTP 响应是否表示 cookie 已过期.

    判据：
      - 401/403
      - 405 + JupyterHub HTML（聚宽对过期的 POST/DELETE 偶发返回 405 而非 403）
      - 3xx 重定向到登录/oauth（JupyterHub 会话失效时 GET 常返回
        302 → /hub/api/oauth2/authorize，body 为空）
      - 响应体是 HTML 且明显是登录页

    JupyterHub 过期通常表现为：GET 302 跳 oauth、POST 403/405。
    """
    if status in (401, 403):
        return True
    # 聚宽 JupyterHub 对过期的 POST/DELETE 偶发返回 405 + HTML（而非 403）
    if status == 405:
        head = body[:2048].lower()
        if "<html" in head and ("jupyterhub" in head or "/hub/" in head):
            return True
    if 300 <= status < 400 and location:
        loc = location.lower()
        if "login" in loc or "oauth" in loc or "/hub/" in loc:
            return True
    head = body[:2048].lower()
    return "<html" in head and ("login" in head or "/hub/login" in head)


class CookieStore:
    """聚宽 cookie 读写.

    线程安全由调用方保证（CLI 场景单线程，runner 场景建议单 store 实例）。
    """

    def __init__(self, path: Path | str | None = None) -> None:
        self.path: Path = Path(path) if path else DEFAULT_COOKIE_PATH

    def load(self) -> dict[str, str]:
        """读取 cookie dict. 文件不存在或为空抛 CookieExpiredError.

        若当前用默认路径且新文件不存在，但检测到旧路径
        ``~/.qlab/jq_cookies.json``（抽包前遗留），则一次性迁移到新路径。
        """
        if not self.path.exists():
            # 一次性迁移：旧路径 ~/.qlab/jq_cookies.json → 新路径 ~/.jq/cookies.json
            if self.path == DEFAULT_COOKIE_PATH and _LEGACY_COOKIE_PATH.exists():
                legacy = CookieStore(_LEGACY_COOKIE_PATH)
                cookies = legacy.load()
                self.save(cookies)  # 顺便落盘到新路径
                return cookies
            raise CookieExpiredError(
                f"cookie 文件不存在: {self.path}\n"
                f"请先从已登录浏览器复制 cookie，再执行：\n"
                f"  jqdata cookies '<复制的 cookie 字符串>'"
            )
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            raise CookieExpiredError(f"cookie 文件损坏: {self.path} ({e})") from e
        cookies = data.get("cookies", {})
        if not cookies:
            raise CookieExpiredError(f"cookie 文件为空: {self.path}")
        return cookies

    def save(self, cookies: dict[str, str]) -> None:
        """原子写入 cookie dict（权限 600，含更新时间戳）."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "cookies": cookies,
            "updated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
        # 用 os.open + 0o600 确保新建文件权限即为 600
        fd = os.open(
            self.path,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            0o600,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            os.close(fd)
            raise
        # 已存在文件 chmod 兜底
        os.chmod(self.path, 0o600)

    def update_from_raw(self, raw: str) -> dict[str, str]:
        """解析 `k1=v1; k2=v2` 字符串并存盘，返回解析后的 dict."""
        cookies = parse_cookie_string(raw)
        if not cookies:
            raise CookieExpiredError("cookie 字符串解析后为空，请检查格式")
        self.save(cookies)
        return cookies

    def cookie_header(self) -> str:
        """拼成 HTTP `Cookie` 头格式：`k1=v1; k2=v2`."""
        return "; ".join(f"{k}={v}" for k, v in self.load().items())

    def xsrf(self) -> str:
        """取 `_xsrf` 值。POST/DELETE 请求必须带 `X-XSRFToken` 头."""
        xsrf = self.load().get("_xsrf")
        if not xsrf:
            raise CookieExpiredError("cookie 中缺少 `_xsrf`，请重新复制完整 cookie")
        return xsrf

    def updated_at(self) -> str | None:
        """返回上次更新时间（ISO 字符串），便于诊断是否陈旧."""
        if not self.path.exists():
            return None
        try:
            return json.loads(self.path.read_text(encoding="utf-8")).get("updated_at")
        except (json.JSONDecodeError, OSError):
            return None


# ---- browser-use fallback 钩子 --------------------------------------------
#
# 混合方案：cookie 过期时默认提示用户手动更新；
# 若配置了 browser-use 自动刷新回调，则触发浏览器登录抓取新 cookie。
# 回调签名：() -> str   返回新的 cookie 原始字符串。
# 实现示例（不在本仓库内，需接入 browser-use MCP）：
#   def joinquant_login_via_browser() -> str:
#       # 1. navigate 到 https://www.joinquant.com/
#       # 2. fill 账号/密码，click 登录
#       # 3. evaluate_script("document.cookie") 取 cookie
#       # 4. return cookie 字符串
RefreshCallback = Callable[[], str]


def refresh_cookies_via_browser(
    callback: RefreshCallback | None,
    store: CookieStore,
) -> dict[str, str]:
    """触发 browser-use 刷新 cookie。无回调时抛 CookieExpiredError 提示手动更新.

    这是轻量 stub：实际浏览器登录流程需调用方注入 `callback`（见上方注释）。
    保持这样是为符合"轻量优先"——不把 browser-use 作为硬依赖，
    只在确实配置了回调时才走自动刷新路径。
    """
    if callback is None:
        raise CookieExpiredError(
            "cookie 已过期，且未配置 browser-use 自动刷新回调。\n"
            "请手动执行：jqdata cookies '<新 cookie 字符串>'\n"
            "（或通过 JoinQuantRunner(refresh_callback=...) 接入 browser-use）"
        )
    raw = callback()
    return store.update_from_raw(raw)
