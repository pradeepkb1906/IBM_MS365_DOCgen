#!/usr/bin/env python3
"""
Seed script for IBM DocGen with Images tool.
Last synced: 2026-04-19 15:35 IST (circuit breaker + host blocklist + reordered sources)
Run with OWUI's Python:
  /Users/pradeepbasavarajappa/.local/share/uv/tools/open-webui/bin/python seed_openwebui.py
"""
import json
import sqlite3
import time
from pathlib import Path

DB = "/Users/pradeepbasavarajappa/.local/share/uv/tools/open-webui/lib/python3.12/site-packages/open_webui/data/webui.db"
TOOL_FILE = Path(__file__).parent / "IBM_DocGen_WithImages_v2.py"
TOOL_ID = "ibm_docgen_with_images"

SPECS = [
    {
        "name": "prepare_content_from_knowledge",
        "description": (
            "Source mode: OWUI Knowledge Collection. Retrieves text chunks and ranked images "
            "from an OWUI knowledge collection for use in assemble_document."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query for retrieval."},
                "collection_id": {"type": "string", "description": "OWUI knowledge collection ID."},
                "max_images": {"type": "integer", "description": "Max image candidates (default 10)."},
            },
            "required": ["query", "collection_id"],
        },
    },
    {
        "name": "prepare_content_from_notes",
        "description": (
            "Source mode: OWUI Notes. Reads the user's notes (Notes panel on the "
            "left side of Open WebUI). Pass note_ids for specific notes, or omit "
            "to read all notes owned by the current user (most recent 50). "
            "Returns text chunks for assemble_document. No image fetching."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Topic/question — used to rank note excerpts."},
                "note_ids": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Optional. OWUI note IDs. Omit to pull all user's notes.",
                },
                "max_chunks": {"type": "integer", "description": "Max text chunks (default 12)."},
            },
            "required": ["query"],
        },
    },
    {
        "name": "prepare_content_from_folder",
        "description": (
            "Source mode: OWUI Folder. Pulls content from every chat grouped "
            "under a folder, plus any notes or files referenced by the folder. "
            "Chat messages are joined and chunked for ranking. Use this when "
            "the user asks you to base a doc on a whole folder."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Topic/question — used to rank folder excerpts."},
                "folder_id": {"type": "string", "description": "OWUI folder ID (required)."},
                "max_chunks": {"type": "integer", "description": "Max text chunks (default 12)."},
            },
            "required": ["query", "folder_id"],
        },
    },
    {
        "name": "prepare_content_from_attachments",
        "description": (
            "Source mode: Chat Attachments. Extracts text AND images from ALL files attached "
            "to this chat (PDF, DOCX, PPTX, XLSX, JPG/PNG/WEBP, SVG). Auto-detects chat "
            "attachments — you normally do NOT need to pass attachment_file_ids. Returns a "
            "package for assemble_document."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Topic/question used to rank images for relevance."},
                "attachment_file_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional. OWUI file IDs for files attached to this chat. If omitted the tool auto-detects chat attachments.",
                },
                "max_images": {"type": "integer", "description": "Max image candidates (default 10)."},
            },
            "required": ["query"],
        },
    },
    {
        "name": "prepare_content_from_web_search",
        "description": (
            "Source mode: Web Search via Google Programmable Search. Fetches text snippets and "
            "images from the web. Requires google_api_key and google_cx valves to be set."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query."},
                "num_text_results": {"type": "integer", "description": "Text results to include (1-10, default 6)."},
                "num_image_results": {"type": "integer", "description": "Image results to download (1-10, default 10)."},
            },
            "required": ["query"],
        },
    },
    {
        "name": "list_mcp_tools",
        "description": (
            "List tools advertised by configured MCP servers (ICA Context Forge, etc.). "
            "Use this to discover what an MCP server can do before calling prepare_content_from_mcp."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "server_id": {
                    "type": "string",
                    "description": "Optional — if provided, list only that server's tools. Else list all configured.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "prepare_content_from_mcp",
        "description": (
            "Source mode: MCP server (ICA Context Forge or any Streamable-HTTP MCP). "
            "Calls one tool on one MCP server, parses text and images, returns a package for assemble_document."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "What the user is asking — used for image ranking."},
                "server_id": {"type": "string", "description": "ID of a configured MCP server (from mcp_servers_json valve)."},
                "tool_name": {"type": "string", "description": "Name of the tool to call on that server."},
                "tool_arguments": {
                    "type": "object",
                    "description": "Arguments to pass to the tool (schema is server-specific).",
                },
                "max_images": {"type": "integer", "description": "Max image candidates (default 10)."},
            },
            "required": ["query", "server_id", "tool_name"],
        },
    },
    {
        "name": "prepare_content_mixed",
        "description": (
            "Source mode: mix multiple sources in one call. Merges results from knowledge collections, "
            "chat attachments, MCP calls, and/or web search so sections can draw on any combination."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Topic / question."},
                "knowledge_collection_id": {"type": "string", "description": "Optional OWUI knowledge collection ID."},
                "attachment_file_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of chat-attached file IDs.",
                },
                "mcp_calls": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": 'Optional list of {"server_id": "...", "tool_name": "...", "arguments": {...}}.',
                },
                "web_search": {"type": "boolean", "description": "If true, also run a Google search."},
                "max_images": {"type": "integer", "description": "Total image candidates in final output (default 10)."},
            },
            "required": ["query"],
        },
    },
    {
        "name": "prepare_content_auto",
        "description": (
            "Auto-routing MCP mode: the tool itself picks which MCP tools to call. "
            "Lists all tools on every configured MCP server, ranks them by query relevance, "
            "and invokes the top N with heuristically-derived arguments."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "User question / topic."},
                "preferred_servers": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional server IDs to prioritise.",
                },
                "max_tools_to_call": {"type": "integer", "description": "Max MCP tools to invoke (default 3)."},
                "max_images": {"type": "integer", "description": "Max image candidates (default 10)."},
            },
            "required": ["query"],
        },
    },
    {
        "name": "prepare_content_smart",
        "description": (
            "One-call smart mode. Automatically pulls from whichever sources are relevant: "
            "attachments (if file IDs given), knowledge collection (if ID given), "
            "MCP auto-routing (if servers configured), and optional web search."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "User question / topic."},
                "knowledge_collection_id": {"type": "string", "description": "Optional OWUI knowledge collection ID."},
                "attachment_file_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional chat-attached file IDs.",
                },
                "note_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional OWUI note IDs. If omitted and use_notes=true, pulls all user notes.",
                },
                "folder_id": {
                    "type": "string",
                    "description": "Optional OWUI folder ID. When present, pulls chats + notes + files under that folder.",
                },
                "use_notes": {"type": "boolean", "description": "Pull OWUI Notes (default true)."},
                "use_mcp_auto": {"type": "boolean", "description": "Auto-route to MCP tools (default true)."},
                "use_web_search": {"type": "boolean", "description": "Also run Google search (default false)."},
                "max_mcp_tools": {"type": "integer", "description": "Max MCP tools to invoke (default 3)."},
                "max_images": {"type": "integer", "description": "Total image candidates (default 10)."},
            },
            "required": ["query"],
        },
    },
    {
        "name": "assemble_document",
        "description": (
            "Build and render the final DOCX, PPTX or XLSX inline in chat. "
            "Call after one of the prepare_content_* methods. Pass sections_json as a JSON string array "
            "referencing image IDs from the prepare step. For format=xlsx you may also pass workbook_json "
            "to specify explicit sheets; if omitted, sheets are auto-derived from sections_json. "
            "Each section may ALSO include an 'svg' field containing raw SVG markup — the tool will "
            "rasterize it to PNG and embed it as the section image (great for architecture diagrams)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string", "description": "Unique ID for this document session (e.g. uuid4)."},
                "format": {"type": "string", "enum": ["docx", "pptx", "xlsx"], "description": "Output format."},
                "title": {"type": "string", "description": "Document / deck / workbook title."},
                "client_name": {"type": "string", "description": "Client name shown on IBM-branded cover / header."},
                "sections_json": {
                    "type": "string",
                    "description": (
                        "JSON array of section objects. Each: "
                        '{"title": "...", "paragraphs": [...], "bullets": [...], '
                        '"table": {"headers": [...], "rows": [[...]]} | null, '
                        '"image_id": "IMG1" | null, '
                        '"svg": "<svg viewBox=...>...</svg>" | null}. '
                        "If both image_id and svg are present, svg wins."
                    ),
                },
                "workbook_json": {
                    "type": "string",
                    "description": (
                        "XLSX only (optional). JSON string of the form "
                        '{"sheets": [{"title": "...", "headers": [...], "rows": [[...]], "notes": "..."}]}. '
                        "If omitted for format=xlsx, sheets are auto-derived from sections_json."
                    ),
                },
            },
            "required": ["session_id", "format", "title", "client_name", "sections_json"],
        },
    },
]

META = {
    "description": (
        "Single-file IBM document generator. Generates IBM-branded DOCX and PPTX with "
        "RAG-grounded text and runtime-extracted relevant images. Sources: OWUI knowledge "
        "collections, chat attachments, web search, or MCP servers (ICA Context Forge / "
        "any Streamable-HTTP MCP). Renders inline in chat with download button."
    ),
    "manifest": {
        "title": "IBM DocGen with Images (MCP-aware)",
        "author": "Deepu",
        "version": "2.0",
        "description": (
            "Single-file tool that generates IBM-branded DOCX and PPTX with RAG-grounded "
            "text AND runtime-extracted relevant images."
        ),
    },
}


def main():
    content = TOOL_FILE.read_text(encoding="utf-8")
    now = int(time.time())

    con = sqlite3.connect(DB, timeout=30)
    con.execute("PRAGMA busy_timeout=30000")

    admin_id = con.execute(
        "SELECT id FROM user WHERE role='admin' ORDER BY created_at LIMIT 1"
    ).fetchone()[0]

    existing = con.execute("SELECT id FROM tool WHERE id=?", (TOOL_ID,)).fetchone()

    if existing:
        con.execute(
            "UPDATE tool SET name=?, content=?, specs=?, meta=?, updated_at=? WHERE id=?",
            (
                "IBM DocGen with Images",
                content,
                json.dumps(SPECS),
                json.dumps(META),
                now,
                TOOL_ID,
            ),
        )
        print(f"Updated tool '{TOOL_ID}'")
    else:
        con.execute(
            "INSERT INTO tool (id, user_id, name, content, specs, meta, created_at, updated_at, valves)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)",
            (
                TOOL_ID,
                admin_id,
                "IBM DocGen with Images",
                content,
                json.dumps(SPECS),
                json.dumps(META),
                now,
                now,
            ),
        )
        print(f"Inserted tool '{TOOL_ID}'")

    con.commit()
    con.close()

    # Verify
    con2 = sqlite3.connect(DB)
    row = con2.execute("SELECT id, name, updated_at FROM tool WHERE id=?", (TOOL_ID,)).fetchone()
    con2.close()
    print(f"DB row: id={row[0]}, name={row[1]}, updated_at={row[2]}")
    print("Done. Restart OWUI to pick up the new tool.")


if __name__ == "__main__":
    main()
