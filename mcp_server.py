#!/usr/bin/env python3
"""CineCross MCP Server — zero-dependency stdio JSON-RPC for Model Context Protocol.

Exposes your movie collection, ratings, recommendations, and library management
to LLM clients (Claude Desktop, Kiro, etc.) via the MCP standard.

Usage:
    python3 mcp_server.py --server https://tools.ecb.pm/cinecross --user ecb
"""
import json, sys, urllib.request, urllib.parse, os

SERVER = os.environ.get("CINECROSS_URL", "https://tools.ecb.pm/cinecross")
USER = os.environ.get("CINECROSS_USER", "ecb")

TOOLS = [
    {"name": "search_titles", "description": "Search movies/TV shows by title, director, actor, genre, or keyword. Returns matching titles with ratings and library status.",
     "inputSchema": {"type": "object", "properties": {"query": {"type": "string", "description": "Search query (title, person, genre, keyword)"}, "limit": {"type": "integer", "description": "Max results (default 20)", "default": 20}}, "required": ["query"]}},
    {"name": "get_title", "description": "Get full details for a specific title by IMDB ID (tt1234567). Returns metadata, ratings, streaming, library info.",
     "inputSchema": {"type": "object", "properties": {"id": {"type": "string", "description": "IMDB ID (e.g. tt0111161)"}}, "required": ["id"]}},
    {"name": "get_recommendations", "description": "Get personalized movie recommendations based on the user's taste profile. Returns 5 categories: DNA Match, Director's Chair, Community, Unanimous Hits, Blast from the Past.",
     "inputSchema": {"type": "object", "properties": {"count": {"type": "integer", "description": "Total recommendations (split across 5 categories)", "default": 20}}}},
    {"name": "get_stats", "description": "Get library and ratings statistics: counts, genre distribution, decade distribution, quality breakdown.",
     "inputSchema": {"type": "object", "properties": {}}},
    {"name": "queue_task", "description": "Queue a task for the LAN agent (running on the media server). Task types: mediainfo, contact_sheet, check_quality, hash_files, size_files, scan_extra, diag.",
     "inputSchema": {"type": "object", "properties": {"type": {"type": "string", "description": "Task type: mediainfo, contact_sheet, check_quality, hash_files, size_files, scan_extra, diag"}, "params": {"type": "object", "description": "Task parameters (e.g. {paths: [...]} for mediainfo)"}}, "required": ["type"]}},
]


def api_get(endpoint, params=None):
    """Call CineCross API."""
    url = f"{SERVER}/{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CineCross-MCP/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}


def handle_tool(name, args):
    if name == "search_titles":
        return api_get("api/search", {"q": args["query"], "user": USER, "limit": args.get("limit", 20)})
    elif name == "get_title":
        return api_get("api/title", {"id": args["id"], "user": USER})
    elif name == "get_recommendations":
        return api_get("api/recommendations", {"user": USER, "n": args.get("count", 20)})
    elif name == "get_stats":
        return api_get("api/stats", {"user": USER})
    elif name == "queue_task":
        return api_get("api/queue_task", {"type": args["type"], "params": json.dumps(args.get("params", {}))})
    return {"error": f"Unknown tool: {name}"}


def write_msg(msg):
    out = json.dumps(msg)
    sys.stdout.write(f"Content-Length: {len(out)}\r\n\r\n{out}")
    sys.stdout.flush()


def read_msg():
    headers = {}
    while True:
        line = sys.stdin.readline()
        if not line:
            return None
        line = line.strip()
        if not line:
            break
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    length = int(headers.get("content-length", 0))
    if length:
        body = sys.stdin.read(length)
        return json.loads(body)
    return None


def main():
    global SERVER, USER
    import argparse
    parser = argparse.ArgumentParser(description="CineCross MCP Server")
    parser.add_argument("--server", default=SERVER, help="CineCross server URL")
    parser.add_argument("--user", default=USER, help="CineCross username")
    args = parser.parse_args()
    SERVER = args.server.rstrip("/")
    USER = args.user

    while True:
        msg = read_msg()
        if msg is None:
            break
        method = msg.get("method", "")
        mid = msg.get("id")

        if method == "initialize":
            write_msg({"jsonrpc": "2.0", "id": mid, "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "cinecross", "version": "1.0.0"}
            }})
        elif method == "notifications/initialized":
            pass  # No response needed
        elif method == "tools/list":
            write_msg({"jsonrpc": "2.0", "id": mid, "result": {"tools": TOOLS}})
        elif method == "tools/call":
            tool_name = msg.get("params", {}).get("name", "")
            tool_args = msg.get("params", {}).get("arguments", {})
            result = handle_tool(tool_name, tool_args)
            text = json.dumps(result, indent=2) if isinstance(result, dict) else str(result)
            write_msg({"jsonrpc": "2.0", "id": mid, "result": {
                "content": [{"type": "text", "text": text}]
            }})
        elif mid is not None:
            write_msg({"jsonrpc": "2.0", "id": mid, "result": {}})


if __name__ == "__main__":
    main()
