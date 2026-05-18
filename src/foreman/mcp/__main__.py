"""Entry: `python -m foreman.mcp`. Boots the stdio MCP server.

Flags:
    --list-tools   Print registered tool names and exit (no Databricks calls).
"""

from __future__ import annotations

import asyncio
import sys

from foreman.mcp.server import mcp


def _list_tools() -> int:
    tools = asyncio.run(mcp.list_tools())
    for t in tools:
        print(t.name)
    return 0


def main() -> int:
    if "--list-tools" in sys.argv[1:]:
        return _list_tools()
    mcp.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
