"""
Entry point for onspy.

Usage:
    onspy mcp | serve | server          # Run as MCP server (stdio)
    onspy [call-tool|list-tools|...]    # Run generated CLI
    python -m onspy [...]               # Alternative invocation
"""

import sys


def main():
    """Main entry point - detects MCP mode vs CLI mode."""
    if len(sys.argv) > 1 and sys.argv[1] in ("mcp", "serve", "server"):
        sys.argv.pop(1)

        from .server import mcp

        mcp.run()
    else:
        from .cli import app

        app()


if __name__ == "__main__":
    main()
