"""qlab CLI 入口."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def cmd_workspace_create(args: argparse.Namespace) -> None:
    from qlab.workspace import Workspace
    ws = Workspace(args.root, args.name)
    ws.init()
    print(f"创建 workspace: {ws.path}")


def cmd_workspace_list(args: argparse.Namespace) -> None:
    root = Path(args.root) / "workspaces"
    if not root.exists():
        print("(no workspaces)")
        return
    for p in sorted(root.iterdir()):
        if p.is_dir():
            print(f"- {p.name}")


def cmd_workspace_remove(args: argparse.Namespace) -> None:
    from qlab.workspace import Workspace
    ws = Workspace(args.root, args.name)
    ws.remove(force=args.force)
    print(f"已删除 workspace: {args.name}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="qlab")
    parser.add_argument("--root", default=".", help="quant-lab 项目根目录")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # workspace 子命令
    ws = sub.add_parser("workspace", help="Workspace 管理")
    ws_sub = ws.add_subparsers(dest="ws_cmd", required=True)

    p_create = ws_sub.add_parser("create", help="创建 workspace")
    p_create.add_argument("name")
    p_create.set_defaults(func=cmd_workspace_create)

    p_list = ws_sub.add_parser("list", help="列出所有 workspace")
    p_list.set_defaults(func=cmd_workspace_list)

    p_rm = ws_sub.add_parser("rm", help="删除 workspace")
    p_rm.add_argument("name")
    p_rm.add_argument("--force", action="store_true")
    p_rm.set_defaults(func=cmd_workspace_remove)

    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
