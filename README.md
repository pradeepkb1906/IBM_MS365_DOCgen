# IBM DocGen with Images — Open WebUI Tool

Single-file IBM-Carbon-branded **DOCX / PPTX / XLSX** generator for Open WebUI, with
runtime image sourcing, inline diagram rendering, and parallel multi-source
research.

## What it does

Given a user prompt in Open WebUI ("generate 5 slides about IBM history"), the tool:

1. Pulls content from any available source — chat attachments, OWUI knowledge
   collections, configured MCP servers, or the open web.
2. Fetches or generates accompanying images — Wikipedia → Wikimedia Commons →
   DuckDuckGo → pure-Python IBM Carbon placeholder (always succeeds).
3. Rasterizes any LLM-generated SVG diagrams to PNG via cairosvg.
4. Assembles an IBM-branded DOCX / PPTX / XLSX with cover, sections, footer
   logo, and inline preview rendered in the chat iframe.

## Architecture (at a glance)

```
                ┌─────────────────────────┐
   user prompt  │ Open WebUI (localhost)  │
                └──────────┬──────────────┘
                           │ tool call
                ┌──────────▼──────────────┐
                │  IBM_DocGen_WithImages  │
                │   Tools class           │
                └──────────┬──────────────┘
           ┌───────────────┼───────────────┐
     prep  │          enrich / generate    │ assemble
           ▼               ▼               ▼
  ┌───────────────┐  ┌───────────────┐ ┌───────────────┐
  │ attachments / │  │ Google /      │ │ OOXML build   │
  │ knowledge /   │  │ Wikipedia /   │ │ (pure Python) │
  │ MCP / web     │  │ Wikimedia /   │ │               │
  │               │  │ DuckDuckGo /  │ │ HTMLResponse  │
  │               │  │ Pillow ph.    │ │ inline iframe │
  └───────────────┘  └───────────────┘ └───────────────┘
```

## Files

| File | Purpose |
|---|---|
| `IBM_DocGen_WithImages_v2.py` | The tool itself — a single OWUI Tool class with 12 methods. |
| `seed_openwebui.py` | Seeds the tool content + spec into OWUI's SQLite DB. |
| `system_prompt.txt` | Full system prompt to paste into OWUI Model → Advanced Params → System Prompt. |
| `find_syntax_warnings.py` | Scans the tool file for Py 3.13+-incompatible escape sequences. |
| `IBM_DocGen_WithImages_v2_Model_Prompt.md` | Legacy / reference prompt notes. |

## Tool methods (12)

| Method | Purpose |
|---|---|
| `prepare_content_from_attachments` | Extract text + images from chat-attached PDF/DOCX/PPTX/XLSX. |
| `prepare_content_from_knowledge` | Pull from OWUI knowledge collection. |
| `prepare_content_from_web_search` | Google Programmable Search → Wikipedia → Wikimedia → DuckDuckGo. |
| `prepare_content_from_mcp` | Call a specific MCP server + tool. |
| `list_mcp_tools` | Discover what MCP servers advertise. |
| `prepare_content_auto` | Auto-route to MCP tools ranked by query. |
| `prepare_content_smart` | Do-the-right-thing: attachments + knowledge + MCP + web. |
| `prepare_content_mixed` | Mix explicit sources in one call. |
| `generate_image` | Single-image fetch: photo (web) or illustration (MCP image-gen). |
| `enrich_sections_with_images` | Batch-enrich a draft sections array in parallel. |
| `assemble_document` | Build DOCX / PPTX / XLSX and return inline HTMLResponse. |
| `render_visualization` | Standalone inline SVG / HTML embed with IBM theming. |

## Quick install

```bash
# OWUI's own Python env — not system python
OWUI_PY=/Users/<you>/.local/share/uv/tools/open-webui/bin/python

# Deps
$OWUI_PY -m pip install pymupdf pillow requests openpyxl cairosvg cairocffi

# Homebrew libraries (macOS — for SVG rasterization)
brew install cairo pango gdk-pixbuf libffi

# Seed into OWUI's DB
$OWUI_PY seed_openwebui.py

# No restart needed — OWUI hot-reloads tool content from the DB on each call.
```

## Speed optimizations (April 2026)

| Change | Speedup |
|---|---|
| Parallel `enrich_sections_with_images` via `asyncio.gather` + semaphore | 5× on multi-slide decks |
| Shared `requests.Session` with connection pool | 2-5× on same-host batches |
| Circuit breaker per image source (429 → 30s cooldown) | 33% on Wikimedia-heavy batches |
| Host blocklist (emvigotech, artificall, alamy, shutterstock, ...) | avoids 10s timeouts |
| **Image-density rule**: `ceil(n_sections / 5)` images | 10-14× on large decks |
| Sources reordered: Wikipedia first, DDG last | fewer failed downloads |

End-to-end: a 10-slide deck went from **193s → ~60-100s** (Bedrock LLM latency
dominates the remainder).

## License

Internal — IBM Consulting.
