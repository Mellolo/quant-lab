"""jq CLI 入口：聚宽 JoinQuant Jupyter 操作.

用法::

    jqdata cookies 'user-...=...; _xsrf=...; ...'   # 更新 cookie
    jqdata test                                      # 测连接 + 列 kernel
    jqdata run 'print(1+1)'                          # 跑代码
    jqdata run --file my_analysis.py                 # 从文件跑
    jqdata run --kernel python2new '...'             # 指定 kernel
"""

from __future__ import annotations

import argparse
import sys
import time

from jq.auth import (
    DEFAULT_BASE_URL,
    CookieExpiredError,
    CookieStore,
)

# 注意：JoinQuantRunner 的导入放在各 cmd 函数内（lazy），
# 因为它依赖可选的 requests/websocket-client。
# 这样 `jq --help` 注册时不触发可选依赖，
# 没装依赖时 CLI 主体仍可用（仅 `jqdata run`/`jqdata test` 执行时报缺依赖）。


def cmd_cookies(args: argparse.Namespace) -> int:
    store = CookieStore()
    cookies = store.update_from_raw(args.raw)
    print(f"已保存 {len(cookies)} 个 cookie → {store.path}")
    keys = ", ".join(cookies.keys())
    print(f"字段：{keys}")
    return 0


def cmd_test(args: argparse.Namespace) -> int:
    from jq.runner import JoinQuantRunner

    runner = JoinQuantRunner(base_url=args.base_url, kernel_name=args.kernel)
    try:
        specs = runner.client.list_kernelspecs()
        ks = specs.get("kernelspecs", {})
        print("连接 OK. 可用 kernel 规格：")
        for name in ks:
            display = ks[name].get("spec", {}).get("display_name", name)
            default = " (default)" if name == specs.get("default") else ""
            print(f"  - {name}: {display}{default}")

        kernels = runner.client.list_kernels()
        print(f"\n当前活跃 kernel：{len(kernels)} 个")
        for k in kernels:
            print(f"  - {k.get('id', '?')[:8]}... ({k.get('name', '?')})")
        return 0
    except CookieExpiredError as e:
        print(f"[cookie 问题] {e}", file=sys.stderr)
        return 2


def cmd_run(args: argparse.Namespace) -> int:
    from jq.runner import JoinQuantRunner

    if args.file:
        with open(args.file, encoding="utf-8") as f:
            code = f.read()
    elif args.code:
        code = args.code
    else:
        print("需要提供代码字符串或 --file", file=sys.stderr)
        return 1

    runner = JoinQuantRunner(
        base_url=args.base_url, kernel_name=args.kernel, timeout=args.timeout,
        persistent=not args.ephemeral,
    )
    try:
        result = runner.run_code(code, timeout=args.exec_timeout)
    except CookieExpiredError as e:
        print(f"[cookie 问题] {e}", file=sys.stderr)
        return 2

    if result["stdout"]:
        sys.stdout.write(result["stdout"])
        if not result["stdout"].endswith("\n"):
            sys.stdout.write("\n")
    if result["stderr"]:
        sys.stderr.write(result["stderr"])
    if result["result"]:
        print(f"\n[result] {result['result']}")
    if result["error"]:
        print(
            f"\n[ERROR] {result['error']['ename']}: {result['error']['evalue']}",
            file=sys.stderr,
        )
        for line in result["error"].get("traceback", []):
            print(line, file=sys.stderr)
        return 1
    return 0


def cmd_kernel(args: argparse.Namespace) -> int:
    from jq.runner import JoinQuantRunner, KernelRegistry

    runner = JoinQuantRunner(base_url=args.base_url, kernel_name=args.kernel)
    reg = KernelRegistry()

    if args.kernel_cmd == "status":
        data = reg.load()
        if not data:
            print("无持久化 kernel")
            return 0
        kid = data.get("kernel_id", "")
        alive = bool(kid) and runner.kernel_alive(kid)
        print(f"kernel_id:  {kid[:8]}..." if kid else "kernel_id:  (空)")
        print(f"base_url:   {data.get('base_url')}")
        print(f"kernel:     {data.get('kernel_name')}")
        print(f"创建:       {time.ctime(data.get('created_at', 0))}")
        print(f"上次使用:   {time.ctime(data.get('last_used', 0))}")
        print(f"状态:       {'存活 ✓' if alive else '已死亡 ✗'}")
        return 0

    if args.kernel_cmd == "close":
        closed = runner.close_persistent()
        print("已关闭持久化 kernel" if closed else "无持久化 kernel")
        return 0

    if args.kernel_cmd == "warmup":
        kid = runner.warmup()
        print(f"已预热持久化 kernel: {kid[:8]}... (jqdata 已预加载,首次 run 即享复用速度)")
        return 0

    if args.kernel_cmd == "list":
        try:
            kernels = runner.client.list_kernels()
        except CookieExpiredError as e:
            print(f"[cookie 问题] {e}", file=sys.stderr)
            return 2
        print(f"活跃 kernel: {len(kernels)} 个")
        for k in kernels:
            print(f"  - {k.get('id', '?')[:8]}... ({k.get('name', '?')})")
        return 0
    return 1


def cmd_cache(args: argparse.Namespace) -> int:
    from jq.cache import DataCache

    dc = DataCache()

    if args.cache_cmd == "status":
        info = dc.status()
        print(f"缓存目录: {info['cache_dir']}")
        print(f"布局版本: {info['version']}")
        print(f"文件总数: {info['total_files']}")
        print(f"总大小:   {info['total_size_mb']} MB")
        if info["bars"]:
            print("\n行情/时序类(按月分片):")
            for name, stat in info["bars"].items():
                print(
                    f"  {name}: {stat['symbols']} 标的, "
                    f"{stat['files']} 分片, {stat['size_kb']} KB"
                )
        if info["snapshot"]:
            print("\n快照类:")
            for name, stat in info["snapshot"].items():
                print(f"  {name}: {stat['files']} 文件, {stat['size_kb']} KB")
        if not info["bars"] and not info["snapshot"]:
            print("\n(空)")
        return 0

    if args.cache_cmd == "clear":
        older = args.older_than
        n = dc.clear(older_than_days=older, all_versions=args.all_versions)
        parts = []
        if older:
            parts.append(f">{older}天前")
        if args.all_versions:
            parts.append("含旧版本目录")
        suffix = f" ({', '.join(parts)})" if parts else ""
        print(f"已清除 {n} 个缓存文件{suffix}")
        return 0

    if args.cache_cmd == "prune":
        n = dc.prune_stale_months()
        print(f"已删除 {n} 个当月/未来月分片(数据不完整,下次查询会重取)")
        return 0

    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="jqdata", description="聚宽 JoinQuant Jupyter 操作"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # jqdata cookies '<raw>'
    p_cookies = sub.add_parser("cookies", help="更新 cookie（从浏览器复制的字符串）")
    p_cookies.add_argument("raw", help="cookie 字符串，如 'user-...=...; _xsrf=...; ...'")
    p_cookies.set_defaults(func=cmd_cookies)

    # jqdata test
    p_test = sub.add_parser("test", help="测试连接 + 列可用 kernel")
    p_test.add_argument("--base-url", default=DEFAULT_BASE_URL, help="JupyterHub 根 URL")
    p_test.add_argument("--kernel", default="python3", help="默认 kernel 名（仅展示）")
    p_test.set_defaults(func=cmd_test)

    # jqdata run '<code>' [--file F] [--kernel K] [--timeout T]
    p_run = sub.add_parser("run", help="在聚宽 Jupyter 执行代码")
    p_run.add_argument("code", nargs="?", help="代码字符串（与 --file 二选一）")
    p_run.add_argument("--file", "-f", help="从文件读代码")
    p_run.add_argument("--kernel", default="python3", help="kernel 名（python3/python2/python2new）")
    p_run.add_argument("--timeout", type=int, default=30, help="HTTP 超时（秒）")
    p_run.add_argument("--exec-timeout", type=float, default=60.0, help="执行超时（秒）")
    p_run.add_argument("--base-url", default=DEFAULT_BASE_URL, help="JupyterHub 根 URL")
    p_run.add_argument("--ephemeral", action="store_true", help="一次性模式(不复用持久化 kernel,执行后删除)")
    p_run.set_defaults(func=cmd_run)

    # jqdata kernel {status|close|list}
    p_kernel = sub.add_parser("kernel", help="持久化 kernel 管理")
    p_kernel.add_argument("--base-url", default=DEFAULT_BASE_URL, help="JupyterHub 根 URL")
    p_kernel.add_argument("--kernel", default="python3", help="kernel 名")
    ks = p_kernel.add_subparsers(dest="kernel_cmd", required=True)
    ks.add_parser("status", help="查持久化 kernel 状态")
    ks.add_parser("close", help="关闭并清理持久化 kernel")
    ks.add_parser("warmup", help="预创建持久化 kernel + 预加载 jqdata(让首次 run 也快)")
    ks.add_parser("list", help="列所有活跃 kernel")
    p_kernel.set_defaults(func=cmd_kernel)

    # jqdata cache {status|clear|prune}
    p_cache = sub.add_parser("cache", help="本地数据缓存管理")
    cs = p_cache.add_subparsers(dest="cache_cmd", required=True)
    cs.add_parser("status", help="查看缓存统计")
    p_cache_clear = cs.add_parser("clear", help="清空缓存")
    p_cache_clear.add_argument("--older-than", type=int, default=None,
                               help="仅清除 N 天前的缓存")
    p_cache_clear.add_argument("--all-versions", action="store_true",
                               help="连旧布局版本目录一起清除")
    cs.add_parser("prune", help="删除当月/未来月分片(修复不完整缓存)")
    p_cache.set_defaults(func=cmd_cache)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
