"""
# Last synced to OWUI DB: 2026-04-19 15:45 IST (image-density rule: ceil(n/5) — huge speed win)
title: IBM DocGen with Images (MCP-aware)
author: Deepu
version: 2.0
description: Single-file tool that generates IBM-branded DOCX and PPTX with RAG-grounded text AND runtime-extracted relevant images. Sources: OWUI knowledge collections, chat attachments, web search, or MCP servers (ICA Context Forge / any MCP Streamable-HTTP server). Extracts images from MCP tool results and ui:// resources. Renders inline in chat with download button. Fully in-memory, no disk writes.
requirements: pymupdf, pillow, requests
"""
import re
import json
import base64
import uuid
import io
import time
import threading
import traceback
import zipfile
import asyncio
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Literal, Any
from xml.etree import ElementTree as ET
from urllib.parse import urlparse

import requests
from pydantic import BaseModel, Field
from PIL import Image

import fitz  # PyMuPDF

from fastapi.responses import HTMLResponse

try:
    from openpyxl import load_workbook, Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    HAS_XLSX = True
except ImportError:
    HAS_XLSX = False

try:
    # macOS strips DYLD_* env vars from child processes launched via
    # launchd / GUI, so OWUI often has no cairo library search path. Fix by
    # discovering libcairo on disk and adding its directory to
    # DYLD_FALLBACK_LIBRARY_PATH BEFORE importing cairosvg (cairocffi reads
    # it inside cffi.dlopen on each module import).
    import os as _os
    _CAIRO_CANDIDATES = [
        "/opt/homebrew/lib/libcairo.2.dylib",   # Homebrew (Apple Silicon)
        "/usr/local/lib/libcairo.2.dylib",      # Homebrew (Intel)
        "/opt/local/lib/libcairo.2.dylib",      # MacPorts
        "/usr/lib/libcairo.so.2",               # Linux system
    ]
    for _p in _CAIRO_CANDIDATES:
        if _os.path.exists(_p):
            _dir = _os.path.dirname(_p)
            _existing = _os.environ.get("DYLD_FALLBACK_LIBRARY_PATH", "")
            if _dir not in _existing.split(":"):
                _os.environ["DYLD_FALLBACK_LIBRARY_PATH"] = (
                    _dir + (":" + _existing if _existing else "")
                )
            # Linux equivalent
            _existing_ld = _os.environ.get("LD_LIBRARY_PATH", "")
            if _dir not in _existing_ld.split(":"):
                _os.environ["LD_LIBRARY_PATH"] = (
                    _dir + (":" + _existing_ld if _existing_ld else "")
                )
            break
    del _os, _CAIRO_CANDIDATES

    import cairosvg
    HAS_CAIROSVG = True
except (ImportError, OSError) as _cairo_err:
    # OSError: libcairo not found; ImportError: cairosvg package missing
    HAS_CAIROSVG = False
    print(f"[DocGen] cairosvg unavailable: {_cairo_err}")


# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════
PDF_EXT = {".pdf"}
DOCX_EXT = {".docx"}
PPTX_EXT = {".pptx"}
XLSX_EXT = {".xlsx", ".xlsm"}
IMG_EXT = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}
SVG_EXT = {".svg"}
ALL_DOC_EXT = PDF_EXT | DOCX_EXT | PPTX_EXT | XLSX_EXT
ALL_IMG_EXT = IMG_EXT | SVG_EXT

NS_A = "{http://schemas.openxmlformats.org/drawingml/2006/main}"
NS_R = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
NS_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"

# IBM Carbon palette
IBM_BLUE_60 = "#0F62FE"
IBM_BLUE_70 = "#0043CE"
IBM_GRAY_100 = "#161616"
IBM_GRAY_70 = "#525252"
IBM_GRAY_20 = "#E0E0E0"
IBM_GRAY_10 = "#F4F4F4"
IBM_WHITE = "#FFFFFF"


# ═══════════════════════════════════════════════════════════════════════════
# SVG / Architecture-diagram engine (ported from Inline_Visualizer_v5)
# ═══════════════════════════════════════════════════════════════════════════
# Design system for inline SVG diagrams, architecture maps, flow charts, KPI
# dashboards. When the LLM provides raw SVG or HTML via the render_visualization
# tool, OR emits section.svg in assemble_document sections, this shell wraps it
# with IBM Carbon theming, auto-sizing, and SVG/PNG/JPG download buttons.

SVG_THEME_CSS = """
:root {
  --color-text-primary: #161616;
  --color-text-secondary: #525252;
  --color-text-tertiary: #8D8D8D;
  --color-text-info: #0043CE;
  --color-text-success: #24A148;
  --color-text-warning: #F1C21B;
  --color-text-danger: #DA1E28;
  --color-bg-primary: #FFFFFF;
  --color-bg-secondary: #F4F4F4;
  --color-bg-tertiary: #E0E0E0;
  --color-border-tertiary: #E0E0E0;
  --color-border-secondary: #8D8D8D;
  --color-border-primary: #525252;
  --font-sans: 'IBM Plex Sans', system-ui, -apple-system, sans-serif;
  --font-mono: 'IBM Plex Mono', 'SF Mono', Menlo, Consolas, monospace;
  --primary: #0F62FE;
  --accent: #0F62FE;
  --ramp-purple-fill:#EEEDFE; --ramp-purple-stroke:#534AB7; --ramp-purple-th:#3C3489; --ramp-purple-ts:#534AB7;
  --ramp-teal-fill:#E1F5EE;   --ramp-teal-stroke:#0F6E56;   --ramp-teal-th:#085041;   --ramp-teal-ts:#0F6E56;
  --ramp-coral-fill:#FAECE7;  --ramp-coral-stroke:#993C1D;  --ramp-coral-th:#712B13;  --ramp-coral-ts:#993C1D;
  --ramp-pink-fill:#FBEAF0;   --ramp-pink-stroke:#993556;   --ramp-pink-th:#72243E;   --ramp-pink-ts:#993556;
  --ramp-gray-fill:#F1EFE8;   --ramp-gray-stroke:#5F5E5A;   --ramp-gray-th:#444441;   --ramp-gray-ts:#5F5E5A;
  --ramp-blue-fill:#E6F1FB;   --ramp-blue-stroke:#185FA5;   --ramp-blue-th:#0C447C;   --ramp-blue-ts:#185FA5;
  --ramp-green-fill:#EAF3DE;  --ramp-green-stroke:#3B6D11;  --ramp-green-th:#27500A;  --ramp-green-ts:#3B6D11;
  --ramp-amber-fill:#FAEEDA;  --ramp-amber-stroke:#854F0B;  --ramp-amber-th:#633806;  --ramp-amber-ts:#854F0B;
  --ramp-red-fill:#FCEBEB;    --ramp-red-stroke:#A32D2D;    --ramp-red-th:#791F1F;    --ramp-red-ts:#A32D2D;
}
:root[data-theme="dark"] {
  --color-text-primary: #F4F4F4; --color-text-secondary: #C6C6C6; --color-text-tertiary: #8D8D8D;
  --color-bg-primary: #161616; --color-bg-secondary: #262626; --color-bg-tertiary: #393939;
  --color-border-tertiary: #393939; --color-border-secondary: #525252; --color-border-primary: #8D8D8D;
  --ramp-purple-fill:#3C3489; --ramp-purple-stroke:#AFA9EC; --ramp-purple-th:#CECBF6; --ramp-purple-ts:#AFA9EC;
  --ramp-teal-fill:#085041;   --ramp-teal-stroke:#5DCAA5;   --ramp-teal-th:#9FE1CB;   --ramp-teal-ts:#5DCAA5;
  --ramp-coral-fill:#712B13;  --ramp-coral-stroke:#F0997B;  --ramp-coral-th:#F5C4B3;  --ramp-coral-ts:#F0997B;
  --ramp-pink-fill:#72243E;   --ramp-pink-stroke:#ED93B1;   --ramp-pink-th:#F4C0D1;   --ramp-pink-ts:#ED93B1;
  --ramp-gray-fill:#444441;   --ramp-gray-stroke:#B4B2A9;   --ramp-gray-th:#D3D1C7;   --ramp-gray-ts:#B4B2A9;
  --ramp-blue-fill:#0C447C;   --ramp-blue-stroke:#85B7EB;   --ramp-blue-th:#B5D4F4;   --ramp-blue-ts:#85B7EB;
  --ramp-green-fill:#27500A;  --ramp-green-stroke:#97C459;  --ramp-green-th:#C0DD97;  --ramp-green-ts:#97C459;
  --ramp-amber-fill:#633806;  --ramp-amber-stroke:#EF9F27;  --ramp-amber-th:#FAC775;  --ramp-amber-ts:#EF9F27;
  --ramp-red-fill:#791F1F;    --ramp-red-stroke:#F09595;    --ramp-red-th:#F7C1C1;    --ramp-red-ts:#F09595;
}
"""

SVG_CLASSES_CSS = """
.t  { font: 400 14px/1.4 var(--font-sans); fill: var(--color-text-primary); }
.ts { font: 400 12px/1.4 var(--font-sans); fill: var(--color-text-secondary); }
.th { font: 500 14px/1.4 var(--font-sans); fill: var(--color-text-primary); }
.box    { fill: var(--color-bg-secondary); stroke: var(--color-border-tertiary); stroke-width: 0.5; }
.node   { cursor: pointer; }
.node:hover { opacity: 0.85; }
.arr    { stroke: var(--color-border-secondary); stroke-width: 1.5; fill: none; }
.leader { stroke: var(--color-text-tertiary); stroke-width: 0.5; stroke-dasharray: 3 2; fill: none; }
.c-purple>rect,.c-purple>circle,.c-purple>ellipse{fill:var(--ramp-purple-fill);stroke:var(--ramp-purple-stroke);stroke-width:.5}
.c-purple>.th{fill:var(--ramp-purple-th)!important} .c-purple>.ts{fill:var(--ramp-purple-ts)!important}
.c-teal>rect,.c-teal>circle,.c-teal>ellipse{fill:var(--ramp-teal-fill);stroke:var(--ramp-teal-stroke);stroke-width:.5}
.c-teal>.th{fill:var(--ramp-teal-th)!important} .c-teal>.ts{fill:var(--ramp-teal-ts)!important}
.c-coral>rect,.c-coral>circle,.c-coral>ellipse{fill:var(--ramp-coral-fill);stroke:var(--ramp-coral-stroke);stroke-width:.5}
.c-coral>.th{fill:var(--ramp-coral-th)!important} .c-coral>.ts{fill:var(--ramp-coral-ts)!important}
.c-pink>rect,.c-pink>circle,.c-pink>ellipse{fill:var(--ramp-pink-fill);stroke:var(--ramp-pink-stroke);stroke-width:.5}
.c-pink>.th{fill:var(--ramp-pink-th)!important} .c-pink>.ts{fill:var(--ramp-pink-ts)!important}
.c-gray>rect,.c-gray>circle,.c-gray>ellipse{fill:var(--ramp-gray-fill);stroke:var(--ramp-gray-stroke);stroke-width:.5}
.c-gray>.th{fill:var(--ramp-gray-th)!important} .c-gray>.ts{fill:var(--ramp-gray-ts)!important}
.c-blue>rect,.c-blue>circle,.c-blue>ellipse{fill:var(--ramp-blue-fill);stroke:var(--ramp-blue-stroke);stroke-width:.5}
.c-blue>.th{fill:var(--ramp-blue-th)!important} .c-blue>.ts{fill:var(--ramp-blue-ts)!important}
.c-green>rect,.c-green>circle,.c-green>ellipse{fill:var(--ramp-green-fill);stroke:var(--ramp-green-stroke);stroke-width:.5}
.c-green>.th{fill:var(--ramp-green-th)!important} .c-green>.ts{fill:var(--ramp-green-ts)!important}
.c-amber>rect,.c-amber>circle,.c-amber>ellipse{fill:var(--ramp-amber-fill);stroke:var(--ramp-amber-stroke);stroke-width:.5}
.c-amber>.th{fill:var(--ramp-amber-th)!important} .c-amber>.ts{fill:var(--ramp-amber-ts)!important}
.c-red>rect,.c-red>circle,.c-red>ellipse{fill:var(--ramp-red-fill);stroke:var(--ramp-red-stroke);stroke-width:.5}
.c-red>.th{fill:var(--ramp-red-th)!important} .c-red>.ts{fill:var(--ramp-red-ts)!important}
"""

SVG_BASE_STYLES = """
* { box-sizing: border-box; margin: 0; font-family: var(--font-sans); }
html, body { overflow: hidden; }
body { background: var(--color-bg-primary); color: var(--color-text-primary); line-height: 1.5; padding: 8px; }
svg { overflow: visible; }
svg text { fill: var(--color-text-primary); }
h1 { font-size: 22px; font-weight: 500; margin-bottom: 12px; }
h2 { font-size: 18px; font-weight: 500; margin-bottom: 8px; }
h3 { font-size: 16px; font-weight: 500; margin-bottom: 6px; }
p  { font-size: 14px; color: var(--color-text-secondary); margin-bottom: 8px; }
button { background: transparent; border: 0.5px solid var(--color-border-secondary); border-radius: 8px; padding: 6px 14px; font-size: 13px; color: var(--color-text-primary); cursor: pointer; }
button:hover { background: var(--color-bg-secondary); }
.responsive-container { width: 100%; max-width: 100%; overflow-x: auto; }
"""

SVG_BODY_SCRIPTS = """
<script>
// SVG download bar — auto-injected when an SVG is present.
window.addEventListener('load',function(){document.querySelectorAll('svg[viewBox]').forEach(function(svg,i){
var bar=document.createElement('div');bar.style.cssText='display:flex;gap:6px;padding:6px 0;margin-bottom:8px';
['SVG','PNG','JPG'].forEach(function(f){var b=document.createElement('button');b.textContent='\\u2913 '+f;
b.style.cssText='background:#0f62fe;color:#fff;border:none;border-radius:4px;padding:4px 12px;font-size:11px;cursor:pointer;font-weight:600';
b.onclick=function(){_dlS(svg,f,i)};bar.appendChild(b)});svg.parentNode.insertBefore(bar,svg.nextSibling)})});
function _dlB(b,n){var u=URL.createObjectURL(b),a=document.createElement('a');a.href=u;a.download=n;document.body.appendChild(a);a.click();document.body.removeChild(a);URL.revokeObjectURL(u)}
function _dlS(svg,fmt,idx){var cl=svg.cloneNode(true),vb=svg.viewBox.baseVal,w=vb&&vb.width>0?vb.width:680,h=vb&&vb.height>0?vb.height:400;
cl.setAttribute('xmlns','http://www.w3.org/2000/svg');
var sr=svg.querySelectorAll('rect,circle,ellipse,path,line,polyline,polygon,text'),cr=cl.querySelectorAll('rect,circle,ellipse,path,line,polyline,polygon,text');
for(var i=0;i<sr.length;i++){var cs=getComputedStyle(sr[i]),ce=cr[i];ce.style.fill=ce.style.fill||cs.fill;if(ce.tagName==='text')ce.style.fontSize=ce.style.fontSize||cs.fontSize;else ce.style.stroke=ce.style.stroke||cs.stroke}
var xml=new XMLSerializer().serializeToString(cl),nm='diagram_'+(idx+1);
if(fmt==='SVG'){_dlB(new Blob([xml],{type:'image/svg+xml'}),nm+'.svg');return}
var img=new Image();img.onload=function(){var c=document.createElement('canvas');c.width=w*2;c.height=h*2;var ctx=c.getContext('2d');
if(fmt==='JPG'){ctx.fillStyle='#fff';ctx.fillRect(0,0,c.width,c.height)}ctx.drawImage(img,0,0,c.width,c.height);
c.toBlob(function(bl){_dlB(bl,nm+'.'+(fmt==='JPG'?'jpg':'png'))},fmt==='JPG'?'image/jpeg':'image/png',.95)};
img.src='data:image/svg+xml;base64,'+btoa(unescape(encodeURIComponent(xml)))}
// Iframe height resizer — works when allow-same-origin is on (our patched OWUI default).
(function(){function fit(){try{var fe=window.frameElement;if(fe){var h=Math.max(720, document.documentElement.scrollHeight);fe.style.height=h+'px';fe.style.minHeight=h+'px';fe.style.width='100%';}}catch(e){}}
fit();setTimeout(fit,100);setTimeout(fit,500);window.addEventListener('load',fit);
new MutationObserver(fit).observe(document.documentElement,{attributes:true,childList:true,subtree:true});})();
</script>
"""

_SVG_WRAPPER_TAG_RE = re.compile(
    r"<!DOCTYPE[^>]*>|</?html[^>]*>|</?head[^>]*>|</?body[^>]*>|<meta[^>]*/?>",
    re.IGNORECASE,
)

def _sanitize_svg_content(content: str) -> str:
    """Strip document wrapper tags that LLMs sometimes include."""
    if not content:
        return ""
    content = _SVG_WRAPPER_TAG_RE.sub("", content)
    content = re.sub(r"\n{3,}", "\n\n", content)
    return content.strip()

def _build_svg_shell(content: str, title: str = "Diagram") -> str:
    """Wrap raw SVG/HTML fragment with IBM Carbon theming + SVG download bar."""
    content = _sanitize_svg_content(content)
    return (
        "<!DOCTYPE html><html><head><meta charset=\"utf-8\">"
        f"<title>{title}</title>"
        f"<style>{SVG_THEME_CSS}\n{SVG_CLASSES_CSS}\n{SVG_BASE_STYLES}</style>"
        "</head><body>"
        f"<div class=\"responsive-container\">{content}</div>"
        f"{SVG_BODY_SCRIPTS}"
        "</body></html>"
    )

def _svg_to_png_bytes(svg_str: str, output_width: int = 1200) -> Optional[bytes]:
    """Rasterize an SVG string to PNG bytes. Returns None if cairosvg is unavailable."""
    if not HAS_CAIROSVG or not svg_str:
        return None
    try:
        svg_clean = _sanitize_svg_content(svg_str)
        if not svg_clean.lstrip().startswith("<svg"):
            return None
        return cairosvg.svg2png(bytestring=svg_clean.encode("utf-8"), output_width=output_width)
    except Exception as e:
        print(f"[DocGen] SVG rasterization failed: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Process-wide in-memory store (survives across tool invocations in same pod)
# ═══════════════════════════════════════════════════════════════════════════
class _ImageStore:
    MAX_IMAGES = 600
    MAX_BYTES = 500 * 1024 * 1024  # 500 MB cap across all stored PNGs
    TTL_SECONDS = 3600  # 60 min

    def __init__(self):
        self._store = {}
        self._lock = threading.Lock()

    def put(self, img_id: str, png_bytes: bytes, metadata: dict):
        with self._lock:
            self._store[img_id] = {
                "png_bytes": png_bytes,
                "metadata": metadata,
                "created_at": time.time(),
                "pinned": False,
            }
            self._evict()

    def pin(self, img_id: str, display_id: Optional[str] = None):
        """Mark an image as pinned so byte-budget eviction never drops it.
        Also refreshes created_at so TTL eviction gives it the full hour.
        Optionally records the display_id (e.g. 'IMG1') in metadata so the
        LLM can reference it by that short alias in sections_json.
        """
        with self._lock:
            rec = self._store.get(img_id)
            if not rec:
                return
            rec["pinned"] = True
            rec["created_at"] = time.time()
            if display_id:
                rec["metadata"]["display_id"] = display_id

    def get_bytes(self, img_id: str) -> Optional[bytes]:
        with self._lock:
            rec = self._store.get(img_id)
            return rec["png_bytes"] if rec else None

    def get_metadata(self, img_id: str) -> Optional[dict]:
        with self._lock:
            rec = self._store.get(img_id)
            return rec["metadata"] if rec else None

    def get_by_display_id(self, display_id: str) -> tuple:
        """Fallback: scan for an entry whose metadata display_id matches (e.g. 'IMG1')."""
        with self._lock:
            for rec in self._store.values():
                if rec["metadata"].get("display_id") == display_id:
                    return rec["png_bytes"], rec["metadata"]
        return None, None

    def _evict(self):
        now = time.time()
        # TTL: never evict pinned entries.
        expired = [k for k, v in self._store.items()
                   if not v.get("pinned") and now - v["created_at"] > self.TTL_SECONDS]
        for k in expired:
            del self._store[k]
        # Count-based: only evict unpinned.
        def _unpinned_oldest():
            cands = [(k, v["created_at"]) for k, v in self._store.items() if not v.get("pinned")]
            return min(cands, key=lambda p: p[1])[0] if cands else None
        while len(self._store) > self.MAX_IMAGES:
            k = _unpinned_oldest()
            if not k:
                break
            del self._store[k]
        # Byte-budget: only evict unpinned.
        def _total():
            return sum(len(v["png_bytes"]) for v in self._store.values())
        while _total() > self.MAX_BYTES and self._store:
            k = _unpinned_oldest()
            if not k:
                break
            del self._store[k]


_IMAGE_STORE = _ImageStore()


# ═══════════════════════════════════════════════════════════════════════════
# Per-process extraction cache — keyed on a stable hash of file bytes so that
# a second request on the same PPTX/DOCX/PDF reuses the already-extracted
# (text_chunks, image_candidates) instead of re-rendering every slide.
# ═══════════════════════════════════════════════════════════════════════════
class _ExtractionCache:
    TTL_SECONDS = 3600
    MAX_ENTRIES = 64

    def __init__(self):
        self._store = {}
        self._lock = threading.Lock()

    def _hash(self, file_bytes: bytes) -> str:
        # First + last 64KB + length is collision-resistant for real documents
        # and ~500× faster than hashing the whole file.
        h = hashlib.sha1()
        h.update(str(len(file_bytes)).encode())
        h.update(file_bytes[:65536])
        h.update(file_bytes[-65536:])
        return h.hexdigest()

    def get(self, file_bytes: bytes):
        key = self._hash(file_bytes)
        with self._lock:
            rec = self._store.get(key)
            if not rec:
                return None
            if time.time() - rec["t"] > self.TTL_SECONDS:
                del self._store[key]
                return None
            return rec["text"], rec["images"]

    def put(self, file_bytes: bytes, text_chunks: list, images: list):
        key = self._hash(file_bytes)
        with self._lock:
            self._store[key] = {"t": time.time(), "text": text_chunks, "images": images}
            while len(self._store) > self.MAX_ENTRIES:
                oldest = min(self._store.keys(), key=lambda k: self._store[k]["t"])
                del self._store[oldest]


_EXTRACT_CACHE = _ExtractionCache()


# ═══════════════════════════════════════════════════════════════════════════
# Per-session document buffer (same lifetime pattern as Inline_Visualizer_v5)
# ═══════════════════════════════════════════════════════════════════════════
class _DocBuffer:
    TTL_SECONDS = 600

    def __init__(self):
        self._sessions = {}
        self._lock = threading.Lock()

    def ensure(self, session_id: str, title: str, fmt: str):
        with self._lock:
            self._cleanup()
            if session_id not in self._sessions:
                self._sessions[session_id] = {
                    "title": title,
                    "format": fmt,
                    "pages": {},  # page_num → elements list
                    "image_refs": [],  # image IDs used (for tracking)
                    "created_at": time.time(),
                }
            return self._sessions[session_id]

    def add_page(self, session_id: str, page_num: int, elements: list):
        with self._lock:
            if session_id in self._sessions:
                self._sessions[session_id]["pages"][page_num] = elements

    def pop(self, session_id: str):
        with self._lock:
            return self._sessions.pop(session_id, None)

    def get(self, session_id: str):
        with self._lock:
            return self._sessions.get(session_id)

    def _cleanup(self):
        now = time.time()
        expired = [k for k, v in self._sessions.items() if now - v["created_at"] > self.TTL_SECONDS]
        for k in expired:
            del self._sessions[k]


_DOC_BUFFER = _DocBuffer()


# ═══════════════════════════════════════════════════════════════════════════
# MCP client — supports BOTH transports
#
# 1. Classic SSE transport (IBM ContextForge / agent-studio, MCP 2024-11-05):
#    - URL ends in /sse. GET opens a persistent event stream.
#    - First event: "event: endpoint\ndata: <messages_url_with_session_id>"
#    - Client POSTs JSON-RPC to that messages URL. Responses arrive on the GET stream.
#
# 2. Streamable-HTTP transport (MCP 2025-06-18):
#    - Single endpoint (often /mcp). POST JSON-RPC directly.
#    - Response is JSON, or SSE on the same POST connection.
#    - Session tracked via Mcp-Session-Id header.
#
# We auto-detect by URL shape (/sse suffix → classic) with fallback probing.
# Dependency-free: just 'requests'. No 'mcp' SDK required — single file.
# ═══════════════════════════════════════════════════════════════════════════
class _MCPClient:
    """
    Dual-transport MCP client. One instance per MCP server URL.
    Use transport= to force a mode, else auto-detect.
    """

    TRANSPORT_SSE = "sse"            # classic SSE (GET /sse + POST /messages)
    TRANSPORT_STREAMABLE = "stream"  # streamable-HTTP (POST /mcp with SSE body)

    def __init__(self, url: str, headers: Optional[dict] = None,
                 timeout: int = 60, transport: Optional[str] = None):
        self.url = url.rstrip("/")
        self.headers = dict(headers or {})
        # For Streamable-HTTP the POST body is JSON; for classic SSE the POST body
        # is also JSON. Accept covers both response types.
        self.headers.setdefault("Accept", "application/json, text/event-stream")
        self.timeout = timeout

        self._transport = transport or self._detect_transport()
        self._lock = threading.Lock()
        self._next_id = 0
        self._initialized = False

        # Streamable-HTTP state
        self._session_id: Optional[str] = None

        # Classic SSE state
        self._sse_thread: Optional[threading.Thread] = None
        self._sse_response = None
        self._messages_url: Optional[str] = None
        self._pending: dict = {}            # rpc_id → threading.Event
        self._results: dict = {}            # rpc_id → parsed result or exception
        self._endpoint_event = threading.Event()
        self._stream_error: Optional[str] = None

    # ── Transport detection ──
    def _detect_transport(self) -> str:
        low = self.url.lower()
        if low.endswith("/sse") or "/sse?" in low:
            return self.TRANSPORT_SSE
        if low.endswith("/mcp") or "/mcp?" in low:
            return self.TRANSPORT_STREAMABLE
        # Default to Streamable-HTTP (newer spec). Callers can override via transport=.
        return self.TRANSPORT_STREAMABLE

    def _rpc_id(self) -> int:
        with self._lock:
            self._next_id += 1
            return self._next_id

    # ═════════════════════════════════════════════════════════════════════
    # Classic SSE transport: open GET stream, then POST to messages URL
    # ═════════════════════════════════════════════════════════════════════
    def _ensure_sse_stream(self):
        if self._sse_thread and self._sse_thread.is_alive() and self._messages_url:
            return
        if self._stream_error:
            raise RuntimeError(self._stream_error)

        get_headers = {k: v for k, v in self.headers.items() if k.lower() != "content-type"}
        get_headers["Accept"] = "text/event-stream"

        try:
            response = requests.get(self.url, headers=get_headers,
                                     stream=True, timeout=self.timeout)
        except Exception as e:
            raise RuntimeError(f"MCP SSE connect failed: {e}")
        if response.status_code != 200:
            raise RuntimeError(f"MCP SSE HTTP {response.status_code}: {response.text[:300]}")

        self._sse_response = response
        self._endpoint_event.clear()

        def pump():
            try:
                self._pump_sse(response)
            except Exception as e:
                self._stream_error = str(e)
                self._endpoint_event.set()

        self._sse_thread = threading.Thread(target=pump, daemon=True)
        self._sse_thread.start()

        # Wait up to 15s for endpoint event (tells us where to POST)
        if not self._endpoint_event.wait(timeout=15):
            raise RuntimeError("MCP SSE: no endpoint event received in 15s")
        if not self._messages_url:
            raise RuntimeError(f"MCP SSE: endpoint event missing URL ({self._stream_error or ''})")

    def _pump_sse(self, response):
        """Walk an SSE stream, dispatching events to waiting RPC callers."""
        event_name = "message"
        data_buf = []

        # chunk_size=1 is required: the default buffers until ~8KB fills, which
        # defeats real-time SSE delivery. Keep-alive comments then keep the
        # endpoint event trapped for many seconds.
        for raw in response.iter_lines(chunk_size=1, decode_unicode=True):
            if raw is None:
                continue
            if raw == "":
                # End of event — dispatch
                data_str = "\n".join(data_buf).strip()
                data_buf = []
                if not data_str:
                    event_name = "message"
                    continue

                if event_name == "endpoint":
                    # data: /messages/?session_id=xxx    (relative)
                    # or    https://.../messages/...     (absolute)
                    ep = data_str
                    if ep.startswith("/"):
                        parsed = urlparse(self.url)
                        self._messages_url = f"{parsed.scheme}://{parsed.netloc}{ep}"
                    elif ep.startswith("http"):
                        self._messages_url = ep
                    else:
                        # Relative to current URL
                        base = self.url.rsplit("/", 1)[0]
                        self._messages_url = f"{base}/{ep}"
                    self._endpoint_event.set()

                elif event_name in ("message", "") and data_str.startswith("{"):
                    try:
                        msg = json.loads(data_str)
                    except Exception:
                        event_name = "message"
                        continue
                    mid = msg.get("id")
                    if mid is not None and mid in self._pending:
                        self._results[mid] = msg
                        self._pending[mid].set()

                event_name = "message"
                continue

            if raw.startswith(":"):
                # SSE comment / keepalive — ignore
                continue
            if raw.startswith("event:"):
                event_name = raw[6:].strip()
            elif raw.startswith("data:"):
                data_buf.append(raw[5:].lstrip())
            elif raw.startswith("id:") or raw.startswith("retry:"):
                pass  # ignored
            # Anything else is ignored

    def _rpc_via_sse(self, method: str, params: Optional[dict] = None,
                      is_notification: bool = False) -> dict:
        self._ensure_sse_stream()
        rid = None if is_notification else self._rpc_id()
        payload = {"jsonrpc": "2.0", "method": method}
        if rid is not None:
            payload["id"] = rid
        if params is not None:
            payload["params"] = params

        post_headers = dict(self.headers)
        post_headers["Content-Type"] = "application/json"
        post_headers.setdefault("Accept", "application/json, text/event-stream")

        if is_notification:
            try:
                requests.post(self._messages_url, json=payload,
                              headers=post_headers, timeout=10)
            except Exception:
                pass
            return {}

        event = threading.Event()
        self._pending[rid] = event
        try:
            r = requests.post(self._messages_url, json=payload,
                              headers=post_headers, timeout=self.timeout)
            if r.status_code not in (200, 202):
                raise RuntimeError(f"MCP SSE POST HTTP {r.status_code}: {r.text[:300]}")
            # Some implementations return the JSON-RPC result inline in the POST
            # response; others only stream it back via the GET.
            if r.content:
                ct = (r.headers.get("Content-Type") or "").lower()
                if "application/json" in ct:
                    try:
                        msg = r.json()
                        if isinstance(msg, dict) and msg.get("id") == rid:
                            self._results[rid] = msg
                            event.set()
                    except Exception:
                        pass

            if not event.wait(timeout=self.timeout):
                raise RuntimeError(f"MCP SSE: no response for {method} within {self.timeout}s")
            msg = self._results.pop(rid, None)
            if msg is None:
                raise RuntimeError(f"MCP SSE: empty result for {method}")
            if "error" in msg:
                raise RuntimeError(f"MCP error: {msg['error']}")
            return msg.get("result", {})
        finally:
            self._pending.pop(rid, None)

    # ═════════════════════════════════════════════════════════════════════
    # Streamable-HTTP transport
    # ═════════════════════════════════════════════════════════════════════
    def _rpc_via_streamable(self, method: str, params: Optional[dict] = None,
                              is_notification: bool = False) -> dict:
        payload = {"jsonrpc": "2.0", "method": method}
        if not is_notification:
            payload["id"] = self._rpc_id()
        if params is not None:
            payload["params"] = params

        headers = dict(self.headers)
        headers["Content-Type"] = "application/json"
        if self._session_id:
            headers["Mcp-Session-Id"] = self._session_id

        try:
            r = requests.post(self.url, json=payload, headers=headers,
                               timeout=self.timeout, stream=True)
        except Exception as e:
            raise RuntimeError(f"MCP streamable transport error: {e}")

        if is_notification:
            return {}

        if r.status_code == 404 and self._session_id:
            # Session expired — one retry
            self._session_id = None
            self._initialized = False
            self.initialize()
            headers["Mcp-Session-Id"] = self._session_id or ""
            r = requests.post(self.url, json=payload, headers=headers,
                               timeout=self.timeout, stream=True)

        if r.status_code >= 400:
            raise RuntimeError(f"MCP HTTP {r.status_code}: {r.text[:500]}")

        new_sid = r.headers.get("Mcp-Session-Id") or r.headers.get("mcp-session-id")
        if new_sid:
            self._session_id = new_sid

        ct = (r.headers.get("Content-Type") or "").lower()
        want_id = payload["id"]
        if "text/event-stream" in ct:
            return self._parse_sse_inline(r, want_id)
        try:
            data = r.json()
        except Exception:
            raise RuntimeError(f"MCP: non-JSON response: {r.text[:300]}")
        if "error" in data:
            raise RuntimeError(f"MCP error: {data['error']}")
        return data.get("result", {})

    def _parse_sse_inline(self, response, want_id: int) -> dict:
        """Walk the SSE body of a Streamable-HTTP POST, pick the matching response."""
        data_buf = []
        for raw in response.iter_lines(chunk_size=1, decode_unicode=True):
            if raw is None:
                continue
            if raw == "":
                data_str = "\n".join(data_buf).strip()
                data_buf = []
                if data_str and data_str.startswith("{"):
                    try:
                        msg = json.loads(data_str)
                    except Exception:
                        continue
                    if msg.get("id") == want_id:
                        if "error" in msg:
                            raise RuntimeError(f"MCP error: {msg['error']}")
                        return msg.get("result", {})
                continue
            if raw.startswith("data:"):
                data_buf.append(raw[5:].lstrip())
        raise RuntimeError("MCP streamable SSE ended without matching response")

    # ═════════════════════════════════════════════════════════════════════
    # Unified entry points
    # ═════════════════════════════════════════════════════════════════════
    def _rpc(self, method: str, params: Optional[dict] = None,
              is_notification: bool = False) -> dict:
        if self._transport == self.TRANSPORT_SSE:
            return self._rpc_via_sse(method, params, is_notification)
        return self._rpc_via_streamable(method, params, is_notification)

    def initialize(self) -> dict:
        """MCP handshake — cached per client."""
        if self._initialized:
            return {}
        result = self._rpc("initialize", {
            "protocolVersion": "2025-06-18",
            "capabilities": {"sampling": {}, "roots": {"listChanged": False}},
            "clientInfo": {"name": "ibm-docgen-withimages", "version": "2.1"},
        })
        # Required: send initialized notification (fire-and-forget)
        try:
            self._rpc("notifications/initialized", is_notification=True)
        except Exception:
            pass
        self._initialized = True
        return result

    def list_tools(self) -> list[dict]:
        self.initialize()
        result = self._rpc("tools/list")
        return result.get("tools", [])

    def call_tool(self, name: str, arguments: Optional[dict] = None) -> dict:
        self.initialize()
        return self._rpc("tools/call", {
            "name": name,
            "arguments": arguments or {},
        })

    def read_resource(self, uri: str) -> dict:
        self.initialize()
        return self._rpc("resources/read", {"uri": uri})


# Cache: url → _MCPClient (one per server, reused across calls in this process)
_MCP_CLIENTS: dict = {}
_MCP_CLIENTS_LOCK = threading.Lock()


def _get_mcp_client(url: str, headers: Optional[dict] = None,
                     transport: Optional[str] = None) -> _MCPClient:
    key = url.rstrip("/")
    with _MCP_CLIENTS_LOCK:
        if key not in _MCP_CLIENTS:
            _MCP_CLIENTS[key] = _MCPClient(url, headers=headers, transport=transport)
        return _MCP_CLIENTS[key]


# ═══════════════════════════════════════════════════════════════════════════
# Tools
# ═══════════════════════════════════════════════════════════════════════════
class Tools:
    """
    Single-file IBM document generator.

    Flow:
      1. prepare_content_from_{knowledge|attachments|web_search}  → text + image candidates
      2. assemble_document(session_id, format, title, sections)   → inline DOCX/PPTX

    The model picks which images go in which sections, emitting a sections
    array that references image IDs from step 1. This tool handles everything
    else — extraction, embedding, rendering.
    """

    class Valves(BaseModel):
        owui_base_url: str = Field(default="http://localhost:8080")
        google_api_key: str = Field(
            default="",
            description="Google Programmable Search API key (web search mode). Empty = disabled.",
        )
        google_cx: str = Field(
            default="",
            description="Google Programmable Search engine ID (CX).",
        )
        mcp_servers_json: str = Field(
            default="",
            description=(
                'JSON array of MCP servers to make callable. Each entry: '
                '{"id": "<short-id>", "name": "<display>", "url": "<streamable-http URL>", '
                '"auth_header": "<optional Authorization header value>", "tools": ["tool1","tool2"] (optional filter)}. '
                'Example: [{"id":"ibm_hr","name":"IBM HR","url":"https://contextforge.ibm.com/hr/mcp","auth_header":"Bearer xyz"}]. '
                'Leave empty to disable MCP mode.'
            ),
        )
        mcp_max_image_extract_per_call: int = Field(
            default=3,
            description="Max images to extract from a single MCP tool result (safety cap).",
        )
        mcp_catalog_ttl_seconds: int = Field(
            default=600,
            description="How long to cache each MCP server's tool list before re-listing (seconds).",
        )
        max_text_chunks: int = Field(default=12)
        max_image_candidates: int = Field(default=10)
        min_image_width: int = Field(default=100)
        min_image_height: int = Field(default=100)
        max_image_aspect_ratio: float = Field(default=6.0)
        max_image_bytes: int = Field(default=3_500_000)
        web_image_fetch_timeout: int = Field(
            default=3,
            description="Per-image HTTP download timeout (seconds). 3s forces fast fallthrough on blocked/slow hosts. Good hosts (Wikimedia, CloudFront) return in <1s anyway.",
        )
        request_timeout: int = Field(
            default=30,
            description="Generic HTTP request timeout (seconds). API calls to Google / DuckDuckGo / Wikipedia.",
        )
        image_fetch_parallelism: int = Field(
            default=4,
            description="Parallel workers when downloading images WITHIN one source's candidate list. Lowered from 8 to 4 to avoid tripping Wikimedia 429 rate-limits.",
        )
        enrich_parallelism: int = Field(
            default=4,
            description="Parallel image fetches when enriching a batch of sections at once. Each worker can hit multiple sources serially. Lowered from 6 to 4 to spread Wikimedia load.",
        )
        skip_vision_rank_for_web: bool = Field(
            default=True,
            description="When source is web/DuckDuckGo and the query was targeted (e.g. 'Red Fort Delhi'), skip the vision-ranking round-trip — trust the search. Big latency win. Attachment/knowledge sources still run vision ranking.",
        )
        security_level: Literal["strict", "balanced", "none"] = Field(default="strict")
        vision_rank_enabled: bool = Field(
            default=True,
            description="If true, send extracted image thumbnails to the multimodal base model so it captions + scores them. Descriptions improve ranking and image captions in the final doc.",
        )
        vision_rank_model: str = Field(
            default="claude-opus-4-6",
            description="Model ID used for vision captioning/ranking. Must support image input.",
        )
        vision_rank_max_images: int = Field(
            default=24,
            description="Max images to send to the vision model per request (safety cap).",
        )
        vision_rank_thumb_px: int = Field(
            default=512,
            description="Longest-edge px for the thumbnail sent to the vision model. Smaller = cheaper/faster.",
        )

    def __init__(self):
        self.valves = self.Valves()
        self._logo_png_cache: Optional[bytes] = None
        self._phase: str = ""
        self._phase_started: float = 0.0
        # Shared HTTP session with connection pool — reused across all image
        # downloads in this tool instance. requests.Session keeps TCP / TLS
        # connections alive, so a batch of image fetches from the same host
        # (Wikimedia, Unsplash, CDN, etc.) reuses the socket instead of
        # re-handshaking every time. Yields 2-5x speedup on parallel ingest.
        from requests.adapters import HTTPAdapter
        try:
            from urllib3.util.retry import Retry
            _retry = Retry(total=1, backoff_factor=0.2, status_forcelist=[502, 503, 504])
        except Exception:
            _retry = None
        self._http = requests.Session()
        _adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16,
                                max_retries=_retry) if _retry else HTTPAdapter(
                                pool_connections=16, pool_maxsize=16)
        self._http.mount("http://", _adapter)
        self._http.mount("https://", _adapter)

    # ── IBM logo (black) footer asset ─────────────────────────────────────
    # Primary source: Wikipedia's IBM_logo.svg (the iconic 8-bar design).
    # We pull a PNG raster and re-color it to pure black so it looks consistent
    # on both white DOCX footers and any PPTX slide background.
    #
    # Fallback: an embedded base64 PNG rendered with Pillow's 8-bar glyph.
    # Needed because upload.wikimedia.org rate-limits / 403s unauthenticated
    # bot requests, AND because IBM Beta/Prod environments may be air-gapped.
    _LOGO_URL = (
        "https://upload.wikimedia.org/wikipedia/commons/thumb/5/51/"
        "IBM_logo.svg/600px-IBM_logo.svg.png"
    )
    _LOGO_FALLBACK_B64 = (
        "iVBORw0KGgoAAAANSUhEUgAAAlgAAADwCAYAAADcthp2AAAEhklEQVR42u3cAYrEIBBE0dSS+1+59goTkNDG9w4gKvTwMTBpewEA"
        "sM6fKwAAEFgAAAILAEBgAQAgsAAABBYAgMACAEBgAQAILACA/d2rF0zir+EZoW2m7MVcYC7MBWfNhRcsAACBBQAgsAAABBYAAAIL"
        "AEBgAQAILAAABBYAgMACABBYAAAILAAAgQUAILAAAAQWAAACCwBgrLR1CwAAlxcsAACBBQBwinv1gkl8c2SEtpmyl93n4sld7nTW"
        "X8/1pd81cwHvzIUXLAAAgQUAILAAAAQWAAACCwBAYAEACCwAAAQWAIDAAgAQWAAACCwAAIEFACCwAAAEFgAAAgsAYKy0dQsAAJcX"
        "LAAAgQUAcIp79YJJfHNkhLaZspfd5+LJXe501l/P9aXfNXMB78yFFywAAIEFACCwAAAEFgAAAgsAQGABAAgsAAAEFgCAwAIAEFgA"
        "AAgsAACBBQAgsAAABBYAAAILAGCstHULAACXFywAAIEFAHCKe/WCSXxzZIS2mbKX3efiyV3udNZfz/Wl3zVzAe/MhRcsAACBBQAg"
        "sAAABBYAAAILAEBgAQAILAAABBYAgMACABBYAAAILAAAgQUAILAAAAQWAAACCwBgrLR1CwAAlxcsAACBBQBwinv1gkl8c2SEtpmy"
        "F3OBuTAXnDUXXrAAAAQWAIDAAgAQWAAACCwAAIEFACCwAAAQWAAAAgsAQGABACCwAAAEFgCAwAIAEFgAAAgsAICx0tYtAABcXrAA"
        "AAQWAMAp7tULJvHNkRHaZspedp+LJ3e501l/PdeXftfMBbwzF16wAAAEFgCAwAIAEFgAAAgsAACBBQAgsAAAEFgAAAILAEBgAQAg"
        "sAAABBYAgMACABBYAAAILACAsdLWLQAAXF6wAAAEFgDAKe7VCybxzZER2mbKXnafiyd3udNZfz3Xl37XzAW8MxdesAAABBYAgMAC"
        "ABBYAAAILAAAgQUAILAAABBYAAACCwBAYAEAILAAAAQWAIDAAgAQWAAACCwAgLHS1i0AAFxesAAABBYAwCnu1Qsm8c2REdpmyl7M"
        "BebCXHDWXHjBAgAQWAAAAgsAQGABACCwAAAEFgCAwAIAQGABAAgsAACBBQCAwAIAEFgAAAILAEBgAQAgsAAAxkpbtwAAcHnBAgAQ"
        "WAAAAgsAAIEFACCwAAAEFgAAAgsAQGABAAgsAAAEFgCAwAIAEFgAAAILAACBBQAgsAAABBYAAAILAEBgAQAILAAABBYAgMACABBY"
        "AAAILAAAgQUAILAAAAQWAAACCwBAYAEACCwAAAQWAIDAAgAQWAAACCwAAIEFACCwAAAQWAAAAgsAQGABAAgsAAAEFgCAwAIAEFgA"
        "AAgsAACBBQAgsAAAEFgAAAILAEBgAQAILAAABBYAgMACABBYAAAILAAAgQUAILAAABBYAACCBQDw1Gv+AwZFpM3uAIqvAAAAAElF"
        "TkSuQmCC"
    )

    def _get_ibm_logo_png(self) -> Optional[bytes]:
        """Return the IBM logo PNG (cached). Wikipedia first, embedded fallback second."""
        if self._logo_png_cache:
            return self._logo_png_cache
        # 1. Try Wikimedia (compliant UA — see https://meta.wikimedia.org/wiki/User-Agent_policy)
        try:
            r = requests.get(
                self._LOGO_URL,
                headers={"User-Agent": "IBM-DocGen/2.0 (https://ibm.com; IBM Consulting) python-requests"},
                timeout=self.valves.web_image_fetch_timeout,
            )
            if r.status_code == 200 and r.content:
                raw = r.content
                im = Image.open(io.BytesIO(raw)).convert("RGBA")
                # Vectorized recolor: composite a solid-black image through the
                # original alpha channel. One C-level call — ~200× faster than
                # a nested per-pixel Python loop.
                _, _, _, alpha = im.split()
                black = Image.new("RGBA", im.size, (0, 0, 0, 255))
                black.putalpha(alpha)
                out = io.BytesIO()
                black.save(out, format="PNG", optimize=True)
                self._logo_png_cache = out.getvalue()
                return self._logo_png_cache
            print(f"[DocGen] IBM logo remote fetch HTTP {r.status_code}, using offline fallback")
        except Exception as e:
            print(f"[DocGen] IBM logo remote fetch failed: {e} — using offline fallback")
        # 2. Fallback: decode the embedded base64 PNG (always works, no network needed).
        try:
            self._logo_png_cache = base64.b64decode(self._LOGO_FALLBACK_B64)
            return self._logo_png_cache
        except Exception as e:
            print(f"[DocGen] IBM logo fallback decode failed: {e}")
            return None

    def _get_ibm_logo_dims(self) -> tuple:
        """Return (width, height) of the cached logo PNG, or a safe default."""
        png = self._get_ibm_logo_png()
        if not png:
            return (600, 240)
        try:
            im = Image.open(io.BytesIO(png))
            return im.size
        except Exception:
            return (600, 240)

    # ══════════════════════════════════════════════════════════════════════
    # PUBLIC METHODS
    # ══════════════════════════════════════════════════════════════════════

    async def prepare_content_from_knowledge(
        self,
        query: str,
        collection_id: str,
        max_images: int = 10,
        __user__: Optional[dict] = None,
        __request__=None,
        __event_emitter__=None,
    ) -> str:
        """
        Source mode: OWUI Knowledge Collection.

        :param query: Search query to use for retrieval.
        :param collection_id: OWUI knowledge collection ID.
        :param max_images: Max image candidates to return (default 10).
        """
        try:
            await self._emit(__event_emitter__, "🔍 Retrieving from knowledge collection...")
            auth = self._auth_from_request(__request__)

            text_chunks = self._retrieve_text_from_collection(query, collection_id, auth)
            collection_files = self._list_collection_files(collection_id, auth)
            await self._emit(__event_emitter__,
                f"📚 {len(text_chunks)} chunks · {len(collection_files)} files")

            if not text_chunks and not collection_files:
                return json.dumps({"error": "No content in collection", "text_chunks": [], "images": []})

            await self._emit(__event_emitter__, "🖼️ Extracting images...")
            all_images = self._extract_from_collection(text_chunks, collection_files, auth)
            all_images = await self._vision_rank_async(query, all_images, auth)
            ranked = self._rank_images(query, all_images)[:max_images]
            for i, img in enumerate(ranked):
                img["display_id"] = f"IMG{i+1}"
                _IMAGE_STORE.pin(img.get("id",""), f"IMG{i+1}")

            await self._emit(__event_emitter__, f"✨ {len(ranked)} images ready", done=True)
            return self._package(query, text_chunks, ranked, source="knowledge_collection")
        except Exception:
            return json.dumps({"error": traceback.format_exc(), "text_chunks": [], "images": []})

    async def prepare_content_from_attachments(
        self,
        query: str,
        attachment_file_ids: Optional[list[str]] = None,
        max_images: int = 10,
        __user__: Optional[dict] = None,
        __request__=None,
        __files__=None,
        __event_emitter__=None,
    ) -> str:
        """
        Source mode: Chat Attachments.

        :param query: Topic/question — used to rank images for relevance.
        :param attachment_file_ids: OWUI file IDs for files attached to this chat.
            If omitted, auto-detected from the chat's attached files.
        :param max_images: Max image candidates to return.
        """
        _hb = None
        try:
            # Auto-detect OWUI-injected attachments when caller did not pass IDs.
            if not attachment_file_ids and __files__:
                auto_ids = []
                for f in __files__:
                    if isinstance(f, dict):
                        fid = f.get("id") or (f.get("file") or {}).get("id")
                        if fid:
                            auto_ids.append(fid)
                attachment_file_ids = auto_ids
            if not attachment_file_ids:
                return json.dumps({
                    "error": "No attachment_file_ids provided and no files attached to chat.",
                    "text_chunks": [], "images": []
                })
            await self._emit(__event_emitter__, f"📎 Processing {len(attachment_file_ids)} attachment(s)...")
            _hb = self._start_heartbeat(__event_emitter__)
            auth = self._auth_from_request(__request__)

            text_chunks, all_images = await asyncio.to_thread(
                self._extract_attachments_parallel, attachment_file_ids, auth, 4
            )

            await self._emit(__event_emitter__, f"📚 {len(text_chunks)} chunks · {len(all_images)} raw images")

            if self.valves.vision_rank_enabled and all_images:
                await self._emit(__event_emitter__, f"👁️ Vision-ranking {min(len(all_images), self.valves.vision_rank_max_images)} images...")
                all_images = await self._vision_rank_async(query, all_images, auth)
            ranked = self._rank_images(query, all_images)[:max_images]
            for i, img in enumerate(ranked):
                img["display_id"] = f"IMG{i+1}"
                _IMAGE_STORE.pin(img.get("id",""), f"IMG{i+1}")
            text_chunks = self._rank_text(query, text_chunks)[:self.valves.max_text_chunks]

            await self._emit(__event_emitter__, f"✨ {len(ranked)} images ready", done=True)
            await self._stop_heartbeat(_hb)
            return self._package(query, text_chunks, ranked, source="chat_attachments")
        except Exception:
            try: await self._stop_heartbeat(_hb)
            except Exception: pass
            return json.dumps({"error": traceback.format_exc(), "text_chunks": [], "images": []})

    async def prepare_content_from_web_search(
        self,
        query: str,
        num_text_results: int = 6,
        num_image_results: int = 10,
        __user__: Optional[dict] = None,
        __request__=None,
        __event_emitter__=None,
    ) -> str:
        """
        Source mode: Web Search (Google Programmable Search).

        :param query: Search query.
        :param num_text_results: Text results to include (1–10).
        :param num_image_results: Image results to download (1–10).
        """
        _hb = None
        try:
            await self._emit(__event_emitter__, f"🌐 Searching: {query}")
            _hb = self._start_heartbeat(__event_emitter__)
            text_chunks = self._web_search_text(query, num_text_results)
            await self._emit(__event_emitter__, f"📚 {len(text_chunks)} web results")

            await self._emit(__event_emitter__, "🖼️ Fetching images...")
            image_candidates = self._web_search_images(query, min(num_image_results, 10))
            await self._emit(__event_emitter__, f"🎨 Downloading {len(image_candidates)} images...")

            ingested = await asyncio.to_thread(
                self._ingest_images_parallel, image_candidates, 6
            )

            if self.valves.vision_rank_enabled and ingested:
                ingested = await self._vision_rank_async(query, ingested, self._auth_from_request(__request__))
            ranked = self._rank_images(query, ingested)[:num_image_results]
            for i, img in enumerate(ranked):
                img["display_id"] = f"IMG{i+1}"
                _IMAGE_STORE.pin(img.get("id",""), f"IMG{i+1}")

            await self._emit(__event_emitter__, f"✨ {len(ranked)} images ready", done=True)
            await self._stop_heartbeat(_hb)
            return self._package(query, text_chunks, ranked, source="web_search")
        except Exception:
            try: await self._stop_heartbeat(_hb)
            except Exception: pass
            return json.dumps({"error": traceback.format_exc(), "text_chunks": [], "images": []})

    async def list_mcp_tools(
        self,
        server_id: Optional[str] = None,
        __user__: Optional[dict] = None,
        __request__=None,
        __event_emitter__=None,
    ) -> str:
        """
        List tools advertised by configured MCP servers (ICA Context Forge, etc.).
        Use this to discover what an MCP server can do before calling it.

        :param server_id: Optional — if provided, list only that server's tools. Else list all configured.
        """
        try:
            servers = self._load_mcp_servers(__request__)
            if not servers:
                return json.dumps({
                    "error": (
                        "No MCP servers discovered. In ICA/OWUI, ask your admin to add the "
                        "MCP server under Admin Settings → External Tools (it will then be auto-detected). "
                        "For local/dev use, set the mcp_servers_json valve."
                    ),
                    "servers": [],
                })

            out = []
            for srv in servers:
                if server_id and srv["id"] != server_id:
                    continue
                await self._emit(__event_emitter__, f"🔌 Listing tools on {srv['name']}...")
                try:
                    client = _get_mcp_client(srv["url"], headers=self._mcp_headers(srv),
                                              transport=srv.get("transport"))
                    tools = client.list_tools()
                    allow = set(srv.get("tools", []) or [])
                    if allow:
                        tools = [t for t in tools if t.get("name") in allow]
                    out.append({
                        "server_id": srv["id"],
                        "server_name": srv["name"],
                        "url": srv["url"],
                        "tool_count": len(tools),
                        "tools": [
                            {
                                "name": t.get("name"),
                                "description": t.get("description", ""),
                                "input_schema": t.get("inputSchema", {}),
                                "has_ui_resource": bool(
                                    (t.get("_meta") or {}).get("ui", {}).get("resourceUri")
                                ),
                                "ui_resource_uri": (t.get("_meta") or {}).get("ui", {}).get("resourceUri"),
                            }
                            for t in tools
                        ],
                    })
                except Exception as e:
                    out.append({
                        "server_id": srv["id"],
                        "server_name": srv["name"],
                        "error": str(e),
                    })

            await self._emit(__event_emitter__, "✅ MCP tool listing complete", done=True)
            return json.dumps({"servers": out}, indent=2)
        except Exception:
            return json.dumps({"error": traceback.format_exc(), "servers": []})

    async def prepare_content_from_mcp(
        self,
        query: str,
        server_id: str,
        tool_name: str,
        tool_arguments: Optional[dict] = None,
        max_images: int = 10,
        __user__: Optional[dict] = None,
        __event_emitter__=None,
    ) -> str:
        """
        Source mode: MCP server (ICA Context Forge or any Streamable-HTTP MCP).
        Calls one tool on one MCP server, parses its result into text_chunks and images,
        then returns a package in the same shape as the other prepare_content_* methods.

        Works with:
          - Plain MCP tools that return text / images / resources in content[]
          - "MCP Apps" tools that declare _meta.ui.resourceUri → we fetch and extract from that UI HTML

        :param query: What the user is asking — used for image ranking.
        :param server_id: ID of a configured MCP server (from mcp_servers_json valve).
        :param tool_name: Name of the tool to call on that server.
        :param tool_arguments: Arguments to pass to the tool (schema is server-specific).
        :param max_images: Max image candidates to include.
        """
        try:
            await self._emit(__event_emitter__, f"🔌 Calling {server_id}.{tool_name}...")
            servers = {s["id"]: s for s in self._load_mcp_servers()}
            if server_id not in servers:
                return json.dumps({
                    "error": f"MCP server '{server_id}' not configured.",
                    "available_server_ids": list(servers.keys()),
                    "text_chunks": [], "images": [],
                })

            srv = servers[server_id]
            client = _get_mcp_client(srv["url"], headers=self._mcp_headers(srv),
                                      transport=srv.get("transport"))

            # Fetch tool definitions once to detect UI Apps metadata
            tool_def = None
            try:
                all_tools = client.list_tools()
                tool_def = next((t for t in all_tools if t.get("name") == tool_name), None)
            except Exception as e:
                print(f"[MCP] tools/list failed on {server_id}: {e}")

            # Call the tool
            result = client.call_tool(tool_name, tool_arguments or {})
            await self._emit(__event_emitter__, "📦 Parsing MCP result...")

            src_meta = {
                "source": f"mcp://{server_id}/{tool_name}",
                "ext": ".mcp",
                "doc_type": "mcp",
                "mcp_server_id": server_id,
                "mcp_tool_name": tool_name,
            }

            text_chunks, all_images = self._parse_mcp_result(
                result=result,
                tool_def=tool_def,
                client=client,
                src_meta=src_meta,
                query=query,
            )

            await self._emit(__event_emitter__,
                f"📚 {len(text_chunks)} text chunks · {len(all_images)} raw images")

            # If an MCP App UI resource was declared but not embedded in the result,
            # fetch it and extract images from its HTML too
            if tool_def and not all_images:
                ui_uri = (tool_def.get("_meta") or {}).get("ui", {}).get("resourceUri")
                if ui_uri:
                    await self._emit(__event_emitter__, f"🎨 Fetching MCP UI resource {ui_uri}...")
                    try:
                        res = client.read_resource(ui_uri)
                        extra_text, extra_img = self._extract_from_mcp_resource_contents(
                            res.get("contents", []), src_meta, client
                        )
                        text_chunks.extend(extra_text)
                        all_images.extend(extra_img)
                    except Exception as e:
                        print(f"[MCP] UI resource fetch failed: {e}")

            ranked = self._rank_images(query, all_images)[:max_images]
            for i, img in enumerate(ranked):
                img["display_id"] = f"IMG{i+1}"
                _IMAGE_STORE.pin(img.get("id",""), f"IMG{i+1}")
            text_chunks = self._rank_text(query, text_chunks)[:self.valves.max_text_chunks]

            await self._emit(__event_emitter__,
                f"✨ {len(ranked)} images from MCP ready", done=True)
            return self._package(query, text_chunks, ranked, source=f"mcp:{server_id}/{tool_name}")
        except Exception:
            return json.dumps({"error": traceback.format_exc(),
                               "text_chunks": [], "images": []})

    async def prepare_content_mixed(
        self,
        query: str,
        knowledge_collection_id: Optional[str] = None,
        attachment_file_ids: Optional[list[str]] = None,
        mcp_calls: Optional[list[dict]] = None,
        web_search: bool = False,
        max_images: int = 10,
        __user__: Optional[dict] = None,
        __request__=None,
        __event_emitter__=None,
    ) -> str:
        """
        Source mode: mix multiple sources in one call. Merges results so sections can draw
        on text and images from any combination of knowledge, attachments, MCP calls, and web.

        :param query: Topic / question.
        :param knowledge_collection_id: Optional OWUI knowledge collection ID.
        :param attachment_file_ids: Optional list of chat-attached file IDs.
        :param mcp_calls: Optional list of {"server_id": "...", "tool_name": "...", "arguments": {...}}.
        :param web_search: If True, also run a Google search (requires google_api_key/cx in valves).
        :param max_images: Total image candidates in final output.
        """
        _hb = None
        try:
            auth = self._auth_from_request(__request__)
            all_text, all_images = [], []
            _hb = self._start_heartbeat(__event_emitter__)

            if knowledge_collection_id:
                await self._emit(__event_emitter__, "🔍 Knowledge collection...")
                tc = self._retrieve_text_from_collection(query, knowledge_collection_id, auth)
                cf = self._list_collection_files(knowledge_collection_id, auth)
                all_text.extend(tc)
                all_images.extend(self._extract_from_collection(tc, cf, auth))

            if attachment_file_ids:
                await self._emit(__event_emitter__, f"📎 {len(attachment_file_ids)} attachment(s)...")
                tc, ti = await asyncio.to_thread(
                    self._extract_attachments_parallel, attachment_file_ids, auth, 4
                )
                all_text.extend(tc)
                all_images.extend(ti)

            if mcp_calls:
                servers = {s["id"]: s for s in self._load_mcp_servers()}
                for call in mcp_calls:
                    sid = call.get("server_id")
                    tname = call.get("tool_name")
                    args = call.get("arguments", {})
                    if sid not in servers:
                        continue
                    await self._emit(__event_emitter__, f"🔌 MCP {sid}.{tname}...")
                    srv = servers[sid]
                    try:
                        client = _get_mcp_client(srv["url"], headers=self._mcp_headers(srv),
                                                  transport=srv.get("transport"))
                        tool_def = None
                        try:
                            all_tools = client.list_tools()
                            tool_def = next((t for t in all_tools if t.get("name") == tname), None)
                        except Exception:
                            pass
                        result = client.call_tool(tname, args or {})
                        src_meta = {
                            "source": f"mcp://{sid}/{tname}",
                            "ext": ".mcp", "doc_type": "mcp",
                            "mcp_server_id": sid, "mcp_tool_name": tname,
                        }
                        tc, ti = self._parse_mcp_result(result, tool_def, client, src_meta, query)
                        all_text.extend(tc)
                        all_images.extend(ti)
                    except Exception as e:
                        print(f"[MCP mixed] {sid}.{tname} failed: {e}")

            if web_search:
                await self._emit(__event_emitter__, "🌐 Web search...")
                all_text.extend(self._web_search_text(query, 6))
                cands = self._web_search_images(query, 10)
                all_images.extend(await asyncio.to_thread(
                    self._ingest_images_parallel, cands, 6))

            if self.valves.vision_rank_enabled and all_images:
                await self._emit(__event_emitter__, f"👁️ Vision-ranking {min(len(all_images), self.valves.vision_rank_max_images)} images...")
                all_images = await self._vision_rank_async(query, all_images, auth)
            ranked = self._rank_images(query, all_images)[:max_images]
            for i, img in enumerate(ranked):
                img["display_id"] = f"IMG{i+1}"
                _IMAGE_STORE.pin(img.get("id",""), f"IMG{i+1}")
            text_chunks = self._rank_text(query, all_text)[:self.valves.max_text_chunks]

            await self._emit(__event_emitter__,
                f"✨ Mixed mode: {len(text_chunks)} chunks, {len(ranked)} images", done=True)
            await self._stop_heartbeat(_hb)
            return self._package(query, text_chunks, ranked, source="mixed")
        except Exception:
            try: await self._stop_heartbeat(_hb)
            except Exception: pass
            return json.dumps({"error": traceback.format_exc(),
                               "text_chunks": [], "images": []})

    async def prepare_content_auto(
        self,
        query: str,
        preferred_servers: Optional[list[str]] = None,
        max_tools_to_call: int = 3,
        max_images: int = 10,
        __user__: Optional[dict] = None,
        __event_emitter__=None,
    ) -> str:
        """
        Auto-routing MCP mode: the tool itself picks which MCP tools to call.

        Use this when you want content from MCP servers but don't want to
        specify tool_name/arguments yourself. The tool:
          1. Lists all tools on every configured MCP server (cached 10 min)
          2. Ranks each tool against the query (name + description + schema match)
          3. Invokes the top N tools with heuristically-derived arguments
          4. Parses every result into the usual {text_chunks, images} package

        :param query: User's question / topic — drives tool ranking and arg-filling.
        :param preferred_servers: Optional list of server IDs to restrict to.
        :param max_tools_to_call: How many top-ranked tools to actually invoke.
        :param max_images: Total image candidates returned.
        """
        try:
            await self._emit(__event_emitter__, "🧭 Discovering MCP tools...")
            servers = self._load_mcp_servers()
            if preferred_servers:
                servers = [s for s in servers if s["id"] in preferred_servers]
            if not servers:
                return json.dumps({
                    "error": "No MCP servers configured (or none match preferred_servers).",
                    "text_chunks": [], "images": [],
                })

            catalog = self._discover_mcp_catalog(servers, __event_emitter__)
            if not catalog:
                return json.dumps({
                    "error": "Could not discover tools on any configured MCP server.",
                    "text_chunks": [], "images": [],
                })

            await self._emit(__event_emitter__,
                f"🧮 Ranking {len(catalog)} tools against the query...")
            ranked_tools = self._rank_mcp_tools(query, catalog)[:max_tools_to_call]
            if not ranked_tools:
                return json.dumps({
                    "error": "No MCP tools scored above the relevance floor for this query.",
                    "text_chunks": [], "images": [],
                })

            picks = [f"{t['server_id']}.{t['name']} ({t['_score']:.2f})" for t in ranked_tools]
            await self._emit(__event_emitter__, f"🎯 Picked: {', '.join(picks)}")

            all_text: list[dict] = []
            all_images: list[dict] = []
            invocation_log: list[dict] = []
            servers_by_id = {s["id"]: s for s in servers}

            for t in ranked_tools:
                sid = t["server_id"]
                srv = servers_by_id.get(sid)
                if not srv:
                    continue
                args = self._auto_fill_tool_args(query, t.get("inputSchema") or {})
                await self._emit(__event_emitter__,
                    f"🔌 Calling {sid}.{t['name']} {args or '{}'}...")
                try:
                    client = _get_mcp_client(srv["url"], headers=self._mcp_headers(srv),
                                              transport=srv.get("transport"))
                    result = client.call_tool(t["name"], args)
                    src_meta = {
                        "source": f"mcp://{sid}/{t['name']}",
                        "ext": ".mcp", "doc_type": "mcp",
                        "mcp_server_id": sid, "mcp_tool_name": t["name"],
                    }
                    tc, ti = self._parse_mcp_result(result, t, client, src_meta, query)
                    all_text.extend(tc)
                    all_images.extend(ti)
                    invocation_log.append({
                        "server_id": sid, "tool_name": t["name"],
                        "arguments": args, "score": round(t["_score"], 3),
                        "text_chunks_yielded": len(tc), "images_yielded": len(ti),
                    })
                except Exception as e:
                    invocation_log.append({
                        "server_id": sid, "tool_name": t["name"],
                        "arguments": args, "error": str(e)[:300],
                    })

            ranked_imgs = self._rank_images(query, all_images)[:max_images]
            for i, img in enumerate(ranked_imgs):
                img["display_id"] = f"IMG{i+1}"
                _IMAGE_STORE.pin(img.get("id",""), f"IMG{i+1}")
            text_chunks = self._rank_text(query, all_text)[:self.valves.max_text_chunks]

            await self._emit(__event_emitter__,
                f"✨ Auto-route: {len(text_chunks)} chunks, {len(ranked_imgs)} images from "
                f"{len([e for e in invocation_log if 'error' not in e])}/{len(invocation_log)} tools",
                done=True)

            pkg = json.loads(self._package(query, text_chunks, ranked_imgs, source="mcp_auto"))
            pkg["mcp_invocations"] = invocation_log
            return json.dumps(pkg, indent=2)
        except Exception:
            return json.dumps({"error": traceback.format_exc(),
                               "text_chunks": [], "images": []})

    async def prepare_content_smart(
        self,
        query: str,
        knowledge_collection_id: Optional[str] = None,
        attachment_file_ids: Optional[list[str]] = None,
        use_mcp_auto: bool = True,
        use_web_search: bool = False,
        max_mcp_tools: int = 3,
        max_images: int = 10,
        __user__: Optional[dict] = None,
        __request__=None,
        __files__=None,
        __event_emitter__=None,
    ) -> str:
        """
        One-call "do the right thing" mode. Tool figures out where to pull from:
          - Attachments (if file IDs given)
          - Knowledge collection (if ID given)
          - MCP auto-routing (tries all configured MCP servers, picks best tools)
          - Web search (if enabled)

        This is the method to use when the user's intent is clear but you don't
        want to micromanage which source answers it.

        :param query: User's question / topic.
        :param knowledge_collection_id: Optional OWUI knowledge collection.
        :param attachment_file_ids: Optional chat-attached file IDs.
        :param use_mcp_auto: If True (default), auto-route to MCP tools.
        :param use_web_search: If True, also run Google search.
        :param max_mcp_tools: Max number of MCP tools to invoke.
        :param max_images: Total image candidates.
        """
        _hb = None
        try:
            auth = self._auth_from_request(__request__)
            all_text: list[dict] = []
            all_images: list[dict] = []
            source_log: list[str] = []
            _hb = self._start_heartbeat(__event_emitter__)

            # Auto-detect chat attachments from OWUI-injected __files__.
            if not attachment_file_ids and __files__:
                auto_ids = []
                for f in __files__:
                    if isinstance(f, dict):
                        fid = f.get("id") or (f.get("file") or {}).get("id")
                        if fid:
                            auto_ids.append(fid)
                if auto_ids:
                    attachment_file_ids = auto_ids
                    await self._emit(__event_emitter__,
                        f"📎 Auto-detected {len(auto_ids)} chat attachment(s)")

            if knowledge_collection_id:
                await self._emit(__event_emitter__, "🔍 Knowledge collection...")
                tc = self._retrieve_text_from_collection(query, knowledge_collection_id, auth)
                cf = self._list_collection_files(knowledge_collection_id, auth)
                all_text.extend(tc)
                all_images.extend(self._extract_from_collection(tc, cf, auth))
                source_log.append(f"knowledge:{knowledge_collection_id}")

            if attachment_file_ids:
                await self._emit(__event_emitter__, f"📎 {len(attachment_file_ids)} attachment(s)...")
                tc, ti = await asyncio.to_thread(
                    self._extract_attachments_parallel, attachment_file_ids, auth, 4
                )
                all_text.extend(tc)
                all_images.extend(ti)
                source_log.append(f"attachments:{len(attachment_file_ids)}")

            if use_mcp_auto:
                servers = self._load_mcp_servers()
                if servers:
                    catalog = self._discover_mcp_catalog(servers, __event_emitter__)
                    if catalog:
                        ranked_tools = self._rank_mcp_tools(query, catalog)[:max_mcp_tools]
                        servers_by_id = {s["id"]: s for s in servers}
                        for t in ranked_tools:
                            srv = servers_by_id.get(t["server_id"])
                            if not srv: continue
                            args = self._auto_fill_tool_args(query, t.get("inputSchema") or {})
                            await self._emit(__event_emitter__,
                                f"🔌 MCP {t['server_id']}.{t['name']}...")
                            try:
                                client = _get_mcp_client(srv["url"],
                                                          headers=self._mcp_headers(srv),
                                                          transport=srv.get("transport"))
                                result = client.call_tool(t["name"], args)
                                src_meta = {
                                    "source": f"mcp://{t['server_id']}/{t['name']}",
                                    "ext": ".mcp", "doc_type": "mcp",
                                    "mcp_server_id": t["server_id"],
                                    "mcp_tool_name": t["name"],
                                }
                                tc, ti = self._parse_mcp_result(result, t, client,
                                                                  src_meta, query)
                                all_text.extend(tc)
                                all_images.extend(ti)
                                source_log.append(f"mcp:{t['server_id']}.{t['name']}")
                            except Exception as e:
                                print(f"[smart] {t['server_id']}.{t['name']} failed: {e}")

            # Auto-fallback: if no content from any other source, use web search so
            # the document always has at least images/text from Wikipedia/Wikimedia.
            web_only_images = False  # track whether all imagery came from web (skip vision-ranking)
            if use_web_search or (not all_text and not all_images):
                await self._emit(__event_emitter__, "🌐 Web search (parallel text + images)...")
                # Run text + image fetch in parallel instead of sequentially.
                text_task = asyncio.to_thread(self._web_search_text, query, 6)
                cands_task = asyncio.to_thread(self._web_search_images, query, max_images)
                web_texts, cands = await asyncio.gather(text_task, cands_task)
                all_text.extend(web_texts)
                web_imgs = await asyncio.to_thread(
                    self._ingest_images_parallel, cands, self.valves.image_fetch_parallelism)
                all_images.extend(web_imgs)
                source_log.append("web_search")
                # If ALL images came from web (attachments / knowledge / MCP added none),
                # trust the search — skip vision-ranking entirely. Saves 20-40s on Bedrock.
                if len(web_imgs) == len(all_images):
                    web_only_images = True

            if not all_text and not all_images:
                return json.dumps({
                    "error": "No content retrieved from any source.",
                    "sources_tried": source_log,
                    "text_chunks": [], "images": [],
                })

            # Vision-rank ONLY when images came from attachments/knowledge/MCP.
            # Web results are already targeted to the query — trust the search.
            if (self.valves.vision_rank_enabled and all_images
                    and not (web_only_images and self.valves.skip_vision_rank_for_web)):
                await self._emit(__event_emitter__,
                    f"👁️ Vision-ranking {min(len(all_images), self.valves.vision_rank_max_images)} images...")
                all_images = await self._vision_rank_async(query, all_images, auth)
            elif web_only_images:
                await self._emit(__event_emitter__, "⚡ Skipping vision-rank (web-only results)")

            ranked_imgs = self._rank_images(query, all_images)[:max_images]
            for i, img in enumerate(ranked_imgs):
                img["display_id"] = f"IMG{i+1}"
                _IMAGE_STORE.pin(img.get("id",""), f"IMG{i+1}")
            text_chunks = self._rank_text(query, all_text)[:self.valves.max_text_chunks]

            await self._emit(__event_emitter__,
                f"✨ Smart: {len(text_chunks)} chunks, {len(ranked_imgs)} images "
                f"from {len(source_log)} sources", done=True)

            pkg = json.loads(self._package(query, text_chunks, ranked_imgs, source="smart"))
            pkg["sources_used"] = source_log
            await self._stop_heartbeat(_hb)
            return json.dumps(pkg, indent=2)
        except Exception:
            try: await self._stop_heartbeat(_hb)
            except Exception: pass
            return json.dumps({"error": traceback.format_exc(),
                               "text_chunks": [], "images": []})

    async def assemble_document(
        self,
        session_id: str,
        format: Literal["docx", "pptx", "xlsx"],
        title: str,
        client_name: str,
        sections_json: str,
        workbook_json: Optional[str] = None,
        __user__: Optional[dict] = None,
        __event_emitter__=None,
    ):
        """
        Build and render the final DOCX, PPTX or XLSX inline in chat.

        sections_json must be a JSON array of section objects:
        [
          {
            "title": "01  Executive Summary",
            "paragraphs": ["...", "..."],
            "bullets": ["...", "..."],
            "table": { "headers": ["A","B"], "rows": [["x","y"]] } | null,
            "image_id": "IMG1" | null,
            "image_caption": "Figure — ..." | null,
            "speaker_notes": "..." | null
          }
        ]

        For format="xlsx" you may ALSO pass workbook_json — a JSON object
        describing explicit sheets. If omitted, sheets are derived from
        sections_json (one sheet per section plus a Summary sheet).

        workbook_json shape:
          {
            "sheets": [
              {
                "title": "Sheet name",
                "headers": ["Col A", "Col B"],
                "rows": [["v1","v2"], ["v3","v4"]],
                "notes": "Optional one-line note"
              }
            ]
          }

        :param session_id: Unique ID for this document build.
        :param format: "docx", "pptx" or "xlsx".
        :param title: Document title (used for filename and cover).
        :param client_name: Client/audience name for header/cover.
        :param sections_json: JSON array of section objects (see above).
        :param workbook_json: Optional JSON workbook spec (xlsx only).
        """
        _hb = None
        try:
            await self._emit(__event_emitter__, f"📝 Assembling {format.upper()}...")
            _hb = self._start_heartbeat(__event_emitter__)

            try:
                sections = json.loads(sections_json) if isinstance(sections_json, str) else sections_json
            except json.JSONDecodeError as e:
                return f"❌ sections_json is not valid JSON: {e}"

            if not isinstance(sections, list) or not sections:
                return "❌ sections_json must be a non-empty array"

            # Resolve image_ids to actual bytes from the store.
            # Accepts either the opaque store id or the display_id (e.g. "IMG1").
            resolved_sections = []
            missing_images = []
            for s in sections:
                resolved = dict(s)
                # SVG-first: if the section carries raw SVG for a diagram/chart,
                # rasterize it and treat the result as the embedded image.
                svg_str = s.get("svg")
                if svg_str and not resolved.get("_img_bytes"):
                    png = _svg_to_png_bytes(svg_str, output_width=1600)
                    if png:
                        resolved["_img_bytes"] = png
                        resolved["_img_width"] = 1600
                        resolved["_img_height"] = 1000
                        resolved["_img_source"] = "generated:svg"
                    else:
                        await self._emit(__event_emitter__,
                            "⚠️ SVG present but cairosvg unavailable — diagram not rasterized")
                if s.get("image_id") and not resolved.get("_img_bytes"):
                    img_bytes = _IMAGE_STORE.get_bytes(s["image_id"])
                    img_meta = _IMAGE_STORE.get_metadata(s["image_id"])
                    if not img_bytes:
                        # Fallback: the LLM may have used display_id ("IMG1") instead of store key
                        img_bytes, img_meta = _IMAGE_STORE.get_by_display_id(s["image_id"])
                    if img_bytes and img_meta:
                        resolved["_img_bytes"] = img_bytes
                        resolved["_img_width"] = img_meta.get("width", 1200)
                        resolved["_img_height"] = img_meta.get("height", 800)
                        resolved["_img_source"] = img_meta.get("source", "")
                    else:
                        missing_images.append(s["image_id"])
                resolved_sections.append(resolved)

            if missing_images:
                await self._emit(__event_emitter__,
                    f"⚠️ {len(missing_images)} image(s) expired/missing — continuing without them")

            if format == "docx":
                html_response = self._build_and_render_docx(
                    session_id, title, client_name, resolved_sections, __event_emitter__
                )
            elif format == "xlsx":
                wb_spec = None
                if workbook_json:
                    try:
                        wb_spec = json.loads(workbook_json) if isinstance(workbook_json, str) else workbook_json
                    except json.JSONDecodeError as e:
                        return f"❌ workbook_json is not valid JSON: {e}"
                html_response = self._build_and_render_xlsx(
                    session_id, title, client_name, resolved_sections, wb_spec, __event_emitter__
                )
            else:
                html_response = self._build_and_render_pptx(
                    session_id, title, client_name, resolved_sections, __event_emitter__
                )

            await self._emit(__event_emitter__, f"✅ {format.upper()} ready", done=True)
            await self._stop_heartbeat(_hb)
            return html_response

        except Exception:
            try: await self._stop_heartbeat(_hb)
            except Exception: pass
            _tb = traceback.format_exc()
            return "❌ Assembly failed:\n" + _tb

    # ══════════════════════════════════════════════════════════════════════
    # INLINE VISUALIZER — architecture diagrams, flow charts, KPI dashboards
    # ══════════════════════════════════════════════════════════════════════
    async def render_visualization(
        self,
        html_code: str,
        title: str = "Visualization",
        __event_emitter__=None,
    ):
        """
        Render an interactive HTML or SVG visualization inline in the chat.

        Use this for architecture diagrams, flow charts, process maps, KPI
        dashboards, gauge displays, anything that's easier to show than write.
        The LLM supplies the SVG markup; this tool wraps it with IBM Carbon
        theming, auto-sizing, and SVG/PNG/JPG download buttons.

        Design system available to your SVG markup:

        - Utility classes on child groups:
            class="t"   — default text
            class="ts"  — secondary text
            class="th"  — heading text (bolder)
            class="box" — subtle background rectangle
            class="arr" — connector line (use marker-end for arrow)
            class="leader" — dashed leader line
            class="node" — hover-able node
        - Color ramps (apply on parent <g>):
            c-purple  c-teal  c-coral  c-pink  c-gray
            c-blue    c-green c-amber  c-red
          These auto-adapt to light/dark theme.
        - IBM Carbon primary: #0F62FE  (accent: var(--primary))
        - Recommended SVG setup:
            <svg viewBox="0 0 1200 700" xmlns="http://www.w3.org/2000/svg">
              <defs>
                <marker id="arr" viewBox="0 0 10 10" refX="9" refY="5"
                        markerWidth="6" markerHeight="6" orient="auto">
                  <path d="M0,0 L10,5 L0,10 z" fill="#8D8D8D"/>
                </marker>
              </defs>
              ...
            </svg>

        :param html_code: HTML or SVG fragment. Use a viewBox. Don't include
            <html>/<head>/<body> — those are added for you.
        :param title: Short descriptive title for the visualization.
        :return: Inline-rendered iframe embed.
        """
        _hb = None
        try:
            await self._emit(__event_emitter__, f"🎨 Creating visualization: {title}")
            _hb = self._start_heartbeat(__event_emitter__)
            html = _build_svg_shell(html_code, title=title)
            await self._emit(__event_emitter__, f"✅ {title} ready", done=True)
            await self._stop_heartbeat(_hb)
            return HTMLResponse(content=html, headers={"Content-Disposition": "inline"})
        except Exception:
            try: await self._stop_heartbeat(_hb)
            except Exception: pass
            return "❌ render_visualization failed:\n" + traceback.format_exc()

    # ══════════════════════════════════════════════════════════════════════
    # BATCH IMAGE ENRICHMENT — one image per section, guaranteed
    # ══════════════════════════════════════════════════════════════════════
    async def enrich_sections_with_images(
        self,
        sections_json: str,
        default_kind: Literal["auto", "photo", "illustration"] = "auto",
        max_images: Optional[int] = None,
        __user__: Optional[dict] = None,
        __request__=None,
        __event_emitter__=None,
    ) -> str:
        """
        Take a DRAFT sections array and return the SAME array with images
        populated on a CURATED SUBSET of sections (not all). This is a speed
        optimization: a 5-slide deck needs maybe 1 hero image, a 10-slide deck
        needs 2-3, a 20-slide deck needs 4-5. Fewer image fetches = faster.

        Image-count rule (auto when max_images=None):
          1-5   sections  →  1 image
          6-10  sections  →  2 images
          11-15 sections  →  3 images
          ...   (ceil(n/5))

        Which sections get the images:
          1. Sections with explicit "image_hint" are top priority (LLM picked
             them as visual-worthy).
          2. Remaining quota is filled by evenly spacing across the rest of
             the deck so images aren't all clustered at the front.
          3. Cover (if present at index 0) is de-prioritized since the cover
             already has IBM branding.

        Per-section image routing:
          - If section has "image_id", "svg", or "_img_bytes" → skip (done).
          - If section has "image_hint" → use as the generate_image prompt.
          - Else auto-derive from title + first paragraph.

        Image sources tried (with circuit breaker + host blocklist):
          Google → Wikipedia article lead → Wikimedia Commons → DuckDuckGo
          → pure-Python IBM-Carbon placeholder (always succeeds).

        :param sections_json: JSON array of section objects.
        :param default_kind: "auto" | "photo" | "illustration".
        :param max_images: Hard cap on images to fetch. If None, auto: ceil(n/5).
        """
        _hb = None
        try:
            try:
                sections = json.loads(sections_json) if isinstance(sections_json, str) else sections_json
            except json.JSONDecodeError as e:
                return json.dumps({"error": f"sections_json is not valid JSON: {e}"})
            if not isinstance(sections, list) or not sections:
                return json.dumps({"error": "sections_json must be a non-empty array"})

            await self._emit(__event_emitter__,
                f"🎨 Enriching {len(sections)} section(s) with images")
            _hb = self._start_heartbeat(__event_emitter__)

            # Partition: skip sections that already have imagery; enqueue the rest.
            # THEN apply image-count cap — a 5-slide deck only needs ~1 image, not 5.
            # This is the big speed lever: fewer image fetches = way less latency.
            import math
            stats = {"enriched": 0, "failed": 0, "skipped": 0, "capped": 0}
            need_images: list[tuple[int, dict, str, str, str]] = []  # (idx, out, prompt, kind, caption)
            enriched: list[dict] = [None] * len(sections)  # type: ignore[list-item]
            for i, s in enumerate(sections):
                if not isinstance(s, dict):
                    enriched[i] = s; continue
                out = dict(s)
                if out.get("image_id") or out.get("svg") or out.get("_img_bytes"):
                    stats["skipped"] += 1
                    enriched[i] = out; continue
                prompt = (out.get("image_hint") or "").strip()
                has_explicit_hint = bool(prompt)
                if not prompt:
                    title = re.sub(r"^\d+\s*[-–—]?\s*", "", (out.get("title") or "")).strip()
                    first_para = ""
                    paras = out.get("paragraphs") or []
                    if paras and isinstance(paras[0], str):
                        first_para = paras[0][:160]
                    prompt = (title + (" — " + first_para if first_para else "")).strip() or title or f"Section {i+1}"
                resolved_kind = default_kind
                if resolved_kind == "auto":
                    resolved_kind = self._classify_image_kind(prompt)
                caption = self._build_image_caption(prompt, resolved_kind)
                need_images.append((i, out, prompt, resolved_kind, caption, has_explicit_hint))

            # Compute image quota. Rule: ceil(n/5), clamped to [1, n].
            n = len(need_images)
            if max_images is None:
                quota = max(1, math.ceil(n / 5)) if n > 0 else 0
            else:
                quota = max(0, min(max_images, n))

            # Pick which sections get images:
            #   1. Sections with explicit image_hint — highest priority.
            #   2. Then evenly spaced across remaining sections (skipping cover at idx 0 when possible).
            chosen_ids: set = set()
            # Pass 1: explicit hints
            for item in need_images:
                idx, _, _, _, _, has_hint = item
                if has_hint and len(chosen_ids) < quota:
                    chosen_ids.add(idx)
            # Pass 2: even spacing across the remainder
            remaining_slots = quota - len(chosen_ids)
            if remaining_slots > 0:
                unpicked = [item[0] for item in need_images if item[0] not in chosen_ids]
                # De-prioritize index 0 (cover) until all others are picked
                if len(unpicked) > remaining_slots and 0 in unpicked:
                    unpicked = [i for i in unpicked if i != 0] + [0]
                # Even spacing
                step = max(1, len(unpicked) // max(1, remaining_slots))
                for i, idx in enumerate(unpicked):
                    if len(chosen_ids) >= quota: break
                    if i % step == 0:
                        chosen_ids.add(idx)
                # Fill any leftover (edge cases)
                for idx in unpicked:
                    if len(chosen_ids) >= quota: break
                    chosen_ids.add(idx)

            # Build the actual fetch list; sections NOT chosen will remain image-less
            # (the LLM can still put text/bullets on those slides — that's the speed win).
            to_fetch: list[tuple[int, dict, str, str, str]] = []
            for idx, out, prompt, kind, caption, _ in need_images:
                if idx in chosen_ids:
                    to_fetch.append((idx, out, prompt, kind, caption))
                else:
                    stats["capped"] += 1
                    enriched[idx] = out  # text-only section — no image

            await self._emit(__event_emitter__,
                f"🖼️ Fetching {len(to_fetch)} image(s) — quota={quota} for {n} section(s); {stats['capped']} kept text-only for speed")

            # Worker: runs the full fallback chain for one section (blocking → thread)
            def _one(prompt: str, kind: str, caption: str) -> Optional[dict]:
                image_rec = None
                if kind == "illustration":
                    try: image_rec = self._generate_image_via_mcp(prompt, caption)
                    except Exception as e: print(f"[DocGen] mcp_img err: {e}")
                if image_rec is None:
                    try: image_rec = self._fetch_image_via_web(prompt, caption)
                    except Exception as e: print(f"[DocGen] web_img err: {e}")
                if image_rec is None:
                    try: image_rec = self._generate_placeholder_image(prompt, caption)
                    except Exception as e: print(f"[DocGen] placeholder err: {e}")
                return image_rec

            # Bounded parallelism with a semaphore so we don't overwhelm Wikimedia/DDG.
            semaphore = asyncio.Semaphore(self.valves.enrich_parallelism)
            async def _worker(item):
                idx, out, prompt, kind, caption = item
                async with semaphore:
                    image_rec = await asyncio.to_thread(_one, prompt, kind, caption)
                return (idx, out, prompt, caption, image_rec)

            results = await asyncio.gather(*[_worker(it) for it in to_fetch])

            # Stitch results back into the enriched array in original order
            for idx, out, prompt, caption, image_rec in results:
                if image_rec:
                    display_id = f"IMG{int(time.time() * 1000) % 100000}{idx+1:02d}"
                    image_rec["display_id"] = display_id
                    image_rec["caption"] = caption
                    _IMAGE_STORE.pin(image_rec.get("id", ""), display_id)
                    out["image_id"] = display_id
                    out["image_caption"] = caption
                    stats["enriched"] += 1
                else:
                    stats["failed"] += 1
                enriched[idx] = out

            await self._emit(__event_emitter__,
                f"✅ Enriched {stats['enriched']}/{len(sections)}  (skipped {stats['skipped']}, failed {stats['failed']})",
                done=True)
            await self._stop_heartbeat(_hb)
            return json.dumps({
                "sections": enriched,
                "stats": stats,
                "next_step": (
                    "Pass this enriched sections array back into assemble_document "
                    "as sections_json. Every section now has image_id + image_caption."
                ),
            }, indent=2)
        except Exception:
            try: await self._stop_heartbeat(_hb)
            except Exception: pass
            return json.dumps({"error": traceback.format_exc()})

    # ══════════════════════════════════════════════════════════════════════
    # IMAGE GENERATION — proactive per-section image fetch
    # ══════════════════════════════════════════════════════════════════════
    async def generate_image(
        self,
        prompt: str,
        kind: Literal["auto", "photo", "illustration"] = "auto",
        caption_hint: Optional[str] = None,
        __user__: Optional[dict] = None,
        __request__=None,
        __event_emitter__=None,
    ) -> str:
        """
        Generate or fetch a single image for a document section. Ingests the
        image into the in-memory store and returns its display_id (e.g. IMG1)
        so the LLM can reference it in sections_json.

        Routing rules:
          - kind="photo"        → web search (Google/Wikimedia). Best for real
                                  places, landmarks, products, company logos
                                  (e.g. "Mysore Palace", "Red Fort Delhi",
                                  "IBM Consulting logo").
          - kind="illustration" → tries configured MCP image-generator servers
                                  first (any MCP tool whose name contains
                                  'generate_image', 'image_gen', 'text_to_image',
                                  'dall', 'sdxl', 'stable_diffusion'), then
                                  falls back to web search. Best for abstract
                                  concepts, stylized diagrams, process art.
          - kind="auto"         → routes automatically: proper-noun / place /
                                  brand / product → photo; abstract concept or
                                  any "diagram/chart/architecture" style prompt
                                  → illustration.

        The returned JSON contains the new display_id, the caption (~20 words
        auto-generated or from caption_hint), and the source. Use the
        display_id as the section's image_id field in assemble_document.

        :param prompt: What to generate/find (e.g. "Mysore Palace at dusk").
        :param kind: "auto" | "photo" | "illustration".
        :param caption_hint: Optional caption text (~20 words). If omitted, one
            is generated from the prompt.
        """
        _hb = None
        try:
            await self._emit(__event_emitter__, f"🖼️ Generating image: {prompt[:60]}")
            _hb = self._start_heartbeat(__event_emitter__)
            auth = self._auth_from_request(__request__)

            resolved_kind = kind
            if kind == "auto":
                resolved_kind = self._classify_image_kind(prompt)
                await self._emit(__event_emitter__, f"🧭 Auto-routed to {resolved_kind}")

            caption = (caption_hint or self._build_image_caption(prompt, resolved_kind))

            image_rec = None
            source_used = None

            if resolved_kind == "illustration":
                # Try MCP image-gen servers first
                image_rec = await asyncio.to_thread(
                    self._generate_image_via_mcp, prompt, caption
                )
                if image_rec:
                    source_used = "mcp:image_generator"

            if image_rec is None:
                # Fallback / photo path — web search
                image_rec = await asyncio.to_thread(
                    self._fetch_image_via_web, prompt, caption
                )
                if image_rec:
                    source_used = "web_search"

            if image_rec is None:
                # Last-resort: generate a pure-Python IBM-Carbon placeholder PNG
                # so the document section still gets an image, never empty. This
                # guarantees generate_image always succeeds even in locked-down
                # IBM sandboxes with no network / no MCP image-gen / no Google keys.
                await self._emit(__event_emitter__,
                    "🎨 No external image sources — generating IBM Carbon placeholder")
                image_rec = await asyncio.to_thread(
                    self._generate_placeholder_image, prompt, caption
                )
                if image_rec:
                    source_used = "placeholder:ibm_carbon"

            if image_rec is None:
                await self._stop_heartbeat(_hb)
                return json.dumps({
                    "error": "Image generation failed at every fallback (MCP, web, and placeholder).",
                    "hint": "This indicates Pillow is not importable — check open-webui's Python env.",
                    "display_id": None,
                    "caption": caption,
                })

            # Pin + assign display_id
            display_id = f"IMG{int(time.time() * 1000) % 100000}"
            image_rec["display_id"] = display_id
            image_rec["caption"] = caption
            _IMAGE_STORE.pin(image_rec.get("id", ""), display_id)

            await self._emit(__event_emitter__, f"✅ Image ready: {display_id}", done=True)
            await self._stop_heartbeat(_hb)
            return json.dumps({
                "display_id": display_id,
                "caption": caption,
                "source": source_used,
                "width": image_rec.get("width"),
                "height": image_rec.get("height"),
                "next_step": (
                    "Reference this display_id in sections_json as image_id when "
                    "calling assemble_document. Add the caption as image_caption "
                    "(~20 words)."
                ),
            }, indent=2)
        except Exception:
            try: await self._stop_heartbeat(_hb)
            except Exception: pass
            return json.dumps({"error": traceback.format_exc(), "display_id": None})

    def _classify_image_kind(self, prompt: str) -> str:
        """Heuristic: proper-noun / place / brand → photo; else illustration."""
        p = (prompt or "").strip()
        if not p:
            return "illustration"
        lower = p.lower()
        # Abstract / diagram-ish hints → illustration
        abstract_hints = [
            "diagram", "architecture", "flow", "process", "topology", "schema",
            "pipeline", "workflow", "concept", "dashboard", "icon", "illustration",
            "art", "stylized", "abstract",
        ]
        for h in abstract_hints:
            if h in lower:
                return "illustration"
        # Proper-noun / place / landmark signals → photo
        # If most words are capitalized, or contains known place/landmark words
        words = p.split()
        capped = sum(1 for w in words if w[:1].isupper())
        if words and capped / max(1, len(words)) >= 0.5:
            return "photo"
        place_hints = [
            "palace", "temple", "fort", "monument", "museum", "building",
            "landmark", "skyline", "city", "logo", "brand", "product",
            "office", "headquarters", "campus",
        ]
        for h in place_hints:
            if h in lower:
                return "photo"
        return "illustration"

    def _build_image_caption(self, prompt: str, kind: str) -> str:
        """Produce a ~20-word caption for the image."""
        p = (prompt or "").strip()
        if not p:
            return "Visual reference."
        # Already short enough? just prepend a descriptor
        if len(p.split()) >= 15:
            return p[:140]
        if kind == "photo":
            return f"Iconic view of {p} — captured to anchor the section with a recognisable visual reference for the reader."[:140]
        return f"Illustrative visual representing {p} — generated to reinforce the section's narrative with a purpose-built graphic."[:140]

    def _generate_image_via_mcp(self, prompt: str, caption: str) -> Optional[dict]:
        """Try every configured MCP server's image-gen-ish tool. Returns an image_rec."""
        servers = self._load_mcp_servers()
        if not servers:
            return None
        gen_tool_keywords = [
            "generate_image", "image_generator", "image_gen", "text_to_image",
            "txt2img", "dall_e", "dalle", "sdxl", "stable_diffusion",
            "create_image", "draw_image",
        ]
        catalog = self._discover_mcp_catalog(servers)
        for t in catalog:
            name = (t.get("name") or "").lower()
            if not any(k in name for k in gen_tool_keywords):
                continue
            try:
                srv = next((s for s in servers if s["id"] == t["server_id"]), None)
                if not srv: continue
                client = _get_mcp_client(srv["url"],
                                          headers=self._mcp_headers(srv),
                                          transport=srv.get("transport"))
                # Build args — most image gens take {"prompt": "..."} or {"text": "..."}
                schema = t.get("inputSchema") or {}
                args = self._auto_fill_tool_args(prompt, schema)
                if "prompt" not in args and "text" not in args:
                    args["prompt"] = prompt
                result = client.call_tool(t["name"], args)
                src_meta = {
                    "source": f"mcp://{t['server_id']}/{t['name']}",
                    "ext": ".mcp_img", "doc_type": "mcp_generated",
                    "mcp_tool_name": t["name"],
                }
                tc, imgs = self._parse_mcp_result(result, t, client, src_meta, prompt)
                # Many image-gen MCPs return the image as a URL inside a text
                # block / structuredContent rather than a proper image/resource
                # block (e.g. ICA: pre-signed S3 URL pointing to a .png).
                # If no inline images came back, hunt for URLs and download.
                if not imgs:
                    url_imgs = self._extract_images_from_mcp_urls(result, src_meta, prompt)
                    imgs = url_imgs
                if imgs:
                    img = imgs[0]
                    img["caption"] = caption
                    img["context"] = prompt
                    return img
            except Exception as e:
                print(f"[DocGen] MCP image-gen via {t.get('name')} failed: {e}")
                continue
        return None

    _IMG_URL_RE = re.compile(
        r"https?://[^\s\"'<>)\}]+?\.(?:png|jpe?g|webp|gif|bmp|tiff?)(?:\?[^\s\"'<>)\}]*)?",
        re.IGNORECASE,
    )
    # Also match pre-signed S3 URLs that don't have an obvious image extension
    # but do have querystring signature params (AWSAccessKey, Signature, etc.)
    _PRESIGNED_URL_RE = re.compile(
        r"https?://[^\s\"'<>)\}]+?\?[^\s\"'<>)\}]*(?:AWSAccessKeyId|X-Amz-Signature|Signature=)[^\s\"'<>)\}]*",
        re.IGNORECASE,
    )

    def _extract_images_from_mcp_urls(self, result: dict, src_meta: dict, prompt: str) -> list[dict]:
        """Scan an MCP result (text blocks + structuredContent) for image URLs
        and presigned-S3-style URLs, download each, and ingest as images.

        This catches image-generator MCPs (e.g. ICA) that return their output
        as a URL in a text block rather than as a proper image/resource block.
        """
        out: list[dict] = []
        # Collect candidate URLs from all text sources
        texts: list[str] = []
        for block in (result.get("content") or []):
            if block.get("type") == "text":
                t = block.get("text") or ""
                if t: texts.append(t)
        sc = result.get("structuredContent")
        if sc:
            try:
                texts.append(json.dumps(sc))
            except Exception:
                pass

        candidates: list[str] = []
        seen: set = set()
        for txt in texts:
            for m in self._IMG_URL_RE.finditer(txt):
                u = m.group(0)
                if u not in seen:
                    seen.add(u); candidates.append(u)
            for m in self._PRESIGNED_URL_RE.finditer(txt):
                u = m.group(0)
                if u not in seen:
                    seen.add(u); candidates.append(u)

        if not candidates:
            return out

        # Download each (first successful one is enough for image-gen)
        for u in candidates[:3]:
            rec = self._ingest_remote_image(u, {
                "url": u,
                "title": src_meta.get("mcp_tool_name", "generated"),
                "snippet": prompt,
                "source_page": f"mcp://{src_meta.get('source','')}",
            })
            if rec:
                out.append(rec)
                break
        return out

    # Circuit-breaker state (class-level so all parallel workers share it).
    # When a source returns 429 / 5xx / bulk failures, we trip the breaker for
    # `_SOURCE_COOLDOWN_S` seconds so subsequent workers skip that source
    # immediately instead of piling on and wasting request_timeout × N.
    _SOURCE_COOLDOWN_S = 30.0
    _source_breakers: dict = {}   # {source_name: unix_ts_until_which_open}
    _breakers_lock = threading.Lock()

    @classmethod
    def _breaker_is_open(cls, source_name: str) -> bool:
        with cls._breakers_lock:
            until = cls._source_breakers.get(source_name, 0)
            return time.time() < until

    @classmethod
    def _breaker_trip(cls, source_name: str, cooldown: Optional[float] = None):
        with cls._breakers_lock:
            cls._source_breakers[source_name] = time.time() + (cooldown or cls._SOURCE_COOLDOWN_S)
        print(f"[DocGen] circuit-breaker TRIPPED for '{source_name}' — skipping for {cooldown or cls._SOURCE_COOLDOWN_S:.0f}s")

    # Domains known to block bots / rate-limit / hotlink-forbid. Downloading
    # from these reliably fails after a long timeout, so skip them upfront.
    _BLOCKED_IMAGE_HOSTS = frozenset([
        "emvigotech.com",
        "artificall.com",
        "sp-uploads.s3.amazonaws.com",
        "alamy.com", "c8.alamy.com",
        "shutterstock.com", "istockphoto.com", "gettyimages.com",
        "adobe.com", "stock.adobe.com",
        "wallpaperaccess.com",
        "logodix.com",
        "pinterest.com", "pinimg.com",
    ])

    def _prefilter_candidates(self, candidates: list[dict]) -> list[dict]:
        """Drop candidates whose host is on the blocklist."""
        out = []
        for c in candidates:
            url = (c.get("url") or "").lower()
            host = urlparse(url).netloc.lower().lstrip("www.")
            # Match suffix — "c8.alamy.com" matches "alamy.com"
            blocked = any(host == h or host.endswith("." + h) for h in self._BLOCKED_IMAGE_HOSTS)
            if not blocked:
                out.append(c)
        return out

    def _fetch_image_via_web(self, prompt: str, caption: str) -> Optional[dict]:
        """Fetch an image via web search with per-source fallback on DOWNLOAD failure.

        Order tried (Wikipedia first — reliable & keyless, no blocked hosts):
          1. Google Programmable Search (if valves set)
          2. Wikipedia article lead-images (upload.wikimedia.org — high relevance for
             proper nouns, landmarks, brands, people; never blocks bots)
          3. Wikimedia Commons file search (broader bitmap search)
          4. DuckDuckGo Images (LAST — returns lots of blocked-host URLs)

        On each source: pre-filter candidates (drop known-blocked hosts), then
        download in parallel. First source to successfully download wins.
        """
        source_attempts: list[tuple[str, callable]] = []
        if self.valves.google_api_key and self.valves.google_cx:
            source_attempts.append(("google", lambda: self._google_search_images(prompt, 5)))
        # Wikipedia FIRST — known-good host (upload.wikimedia.org), compliant UA,
        # exact proper-noun match. Catches 80% of IBM/Accenture/Red Fort-type queries.
        source_attempts.append(("wikipedia", lambda: self._wikipedia_lead_images(prompt, 5)))
        source_attempts.append(("wikimedia", lambda: self._wikimedia_search_images(prompt, 5)))
        # DuckDuckGo LAST — returns lots of low-quality / blocked-host URLs
        # that waste time with timeouts.
        source_attempts.append(("duckduckgo", lambda: self._duckduckgo_search_images(prompt, 5)))

        for source_name, fetch in source_attempts:
            # Circuit breaker — skip if this source recently failed (rate-limited)
            if self._breaker_is_open(source_name):
                continue
            try:
                candidates = fetch() or []
            except Exception as e:
                print(f"[DocGen] {source_name} search for {prompt!r} failed: {e}")
                # Trip the breaker on exception (likely API down)
                self._breaker_trip(source_name)
                continue
            if not candidates:
                continue
            # Pre-filter: drop blocked hosts before wasting connect-timeout on them
            filtered = self._prefilter_candidates(candidates)
            if not filtered:
                print(f"[DocGen] {source_name} returned {len(candidates)} URLs but all on blocklist; next source")
                continue
            ingested = self._ingest_images_parallel(
                filtered, max_workers=self.valves.image_fetch_parallelism
            )
            if ingested:
                top = ingested[0]
                top["caption"] = caption
                top["context"] = prompt
                top["source_resolved"] = source_name
                return top
            print(f"[DocGen] {source_name} returned {len(filtered)} URLs for {prompt!r} but none downloaded; trying next source")
            # Trip breaker if all downloads failed — signals rate-limit / bulk block
            self._breaker_trip(source_name)
        return None

    def _generate_placeholder_image(self, prompt: str, caption: str) -> Optional[dict]:
        """Last-resort: generate an IBM-Carbon-styled placeholder PNG in pure
        Python when neither MCP nor web search is available (e.g. locked-down
        IBM sandboxes). Never returns None — always produces something embedable.
        """
        try:
            from PIL import Image as _PILImage, ImageDraw, ImageFont
            W, H = 1600, 900
            # IBM Carbon-inspired gradient background
            img = _PILImage.new("RGB", (W, H), (15, 98, 254))  # IBM blue 60
            draw = ImageDraw.Draw(img)
            # Darker top band
            for y in range(0, 180):
                shade = int(15 * (1 - y / 180))
                draw.rectangle([0, y, W, y + 1], fill=(shade, int(50 + 48 * y / 180), int(150 + 104 * y / 180)))
            # White content panel
            draw.rounded_rectangle([80, 220, W - 80, H - 80], radius=12, fill=(255, 255, 255))
            # IBM Carbon accent strip
            draw.rectangle([80, 220, W - 80, 280], fill=(15, 98, 254))

            # Try to load a bold sans-serif font; fall back to default
            font_large = font_small = None
            for font_path in [
                "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
                "/System/Library/Fonts/Helvetica.ttc",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            ]:
                try:
                    font_large = ImageFont.truetype(font_path, 64)
                    font_small = ImageFont.truetype(font_path, 28)
                    break
                except Exception:
                    continue
            if font_large is None:
                font_large = ImageFont.load_default()
                font_small = ImageFont.load_default()

            # Wrap the prompt for the title
            words = (prompt or "Generated").split()
            lines, current = [], ""
            for w in words:
                test = (current + " " + w).strip()
                if len(test) <= 28:
                    current = test
                else:
                    if current: lines.append(current)
                    current = w
            if current: lines.append(current)
            lines = lines[:3]
            # Draw title
            y = 340
            for line in lines:
                draw.text((140, y), line, fill=(22, 22, 22), font=font_large)
                y += 80
            # Draw caption (wrapped)
            cap_words = caption.split()
            cap_lines = []
            cur = ""
            for w in cap_words:
                test = (cur + " " + w).strip()
                if len(test) <= 68:
                    cur = test
                else:
                    if cur: cap_lines.append(cur)
                    cur = w
            if cur: cap_lines.append(cur)
            y = max(y + 40, 620)
            for line in cap_lines[:3]:
                draw.text((140, y), line, fill=(82, 82, 82), font=font_small)
                y += 40
            # IBM Consulting tag
            draw.text((140, H - 140), "IBM Consulting", fill=(15, 98, 254), font=font_small)

            src = {
                "source": "generated:placeholder",
                "ext": ".png",
                "doc_type": "generated",
            }
            rec = self._store_image(img, src, caption[:200], prompt[:200],
                                     "generated: placeholder (no external image source available)",
                                     "placeholder")
            if rec:
                rec["caption"] = caption
                rec["context"] = prompt
                rec["width"] = W
                rec["height"] = H
            return rec
        except Exception as e:
            print(f"[DocGen] placeholder image generation failed: {e}")
            return None

    # ══════════════════════════════════════════════════════════════════════
    # MCP HELPERS — parse results, extract images from content/resources
    # ══════════════════════════════════════════════════════════════════════
    def _load_mcp_servers(self) -> list[dict]:
        """
        Parse the mcp_servers_json valve. Empty → [].

        Supported entry shape (matches the ICA / ContextForge config pattern):
          {
            "id":          "<short-id>",
            "name":        "<display name>",
            "url":         "<MCP server URL, e.g. https://.../servers/<uuid>/sse>",
            "type":        "sse" | "streamable-http"  (optional — auto-detected from URL),
            "auth_header": "<full Authorization header, e.g. 'Bearer <token>'>",
            "headers":     { "X-Custom": "..." }      (optional extra headers),
            "tools":       ["tool1", "tool2"]         (optional allowlist)
          }
        """
        raw = (self.valves.mcp_servers_json or "").strip()
        if not raw:
            return []
        try:
            data = json.loads(raw)

            # Also accept the IBM/ContextForge style {"servers": {...}} wrapper
            if isinstance(data, dict):
                if "servers" in data and isinstance(data["servers"], dict):
                    flat = []
                    for sid, entry in data["servers"].items():
                        if not isinstance(entry, dict):
                            continue
                        merged = dict(entry)
                        merged["id"] = sid
                        flat.append(merged)
                    data = flat
                elif "mcpServers" in data and isinstance(data["mcpServers"], dict):
                    flat = []
                    for sid, entry in data["mcpServers"].items():
                        if not isinstance(entry, dict):
                            continue
                        merged = dict(entry)
                        merged["id"] = sid
                        flat.append(merged)
                    data = flat
                else:
                    data = [data]

            if not isinstance(data, list):
                return []

            out = []
            for entry in data:
                if not isinstance(entry, dict):
                    continue
                if not entry.get("id") or not entry.get("url"):
                    continue

                # Transport: explicit type > URL suffix > default streamable
                t = (entry.get("type") or "").lower()
                if t in ("sse",):
                    transport = _MCPClient.TRANSPORT_SSE
                elif t in ("streamable-http", "streamable_http", "http", "mcp"):
                    transport = _MCPClient.TRANSPORT_STREAMABLE
                else:
                    transport = None  # let client auto-detect

                # Auth can be in "auth_header" (full value) or "headers.Authorization"
                headers = dict(entry.get("headers") or {})
                auth = (entry.get("auth_header") or "").strip()
                if auth and "Authorization" not in headers:
                    headers["Authorization"] = auth

                out.append({
                    "id": entry["id"],
                    "name": entry.get("name", entry["id"]),
                    "url": entry["url"],
                    "transport": transport,
                    "headers": headers,
                    "tools": entry.get("tools", []) or [],
                })
            return out
        except Exception as e:
            print(f"[MCP] mcp_servers_json parse failed: {e}")
            return []

    def _mcp_headers(self, srv: dict) -> dict:
        """Return full header dict for a server config (Authorization + extras)."""
        return dict(srv.get("headers") or {})

    # ── Auto-routing helpers ──

    def _discover_mcp_catalog(self, servers: list[dict], emitter=None) -> list[dict]:
        """
        List tools across all configured servers. Results cached on the Tools
        instance for the configured TTL to avoid hammering servers on every call.
        Returns a flat list of {server_id, name, description, inputSchema, _meta, ...}.
        """
        now = time.time()
        if not hasattr(self, "_mcp_catalog_cache"):
            self._mcp_catalog_cache: dict = {}

        ttl = getattr(self.valves, "mcp_catalog_ttl_seconds", 600)
        catalog: list[dict] = []
        for srv in servers:
            cache_key = srv["url"].rstrip("/")
            cached = self._mcp_catalog_cache.get(cache_key)
            if cached and now - cached["at"] < ttl:
                catalog.extend(cached["tools"])
                continue
            try:
                client = _get_mcp_client(srv["url"], headers=self._mcp_headers(srv),
                                          transport=srv.get("transport"))
                tools = client.list_tools()
                allow = set(srv.get("tools", []) or [])
                if allow:
                    tools = [t for t in tools if t.get("name") in allow]
                # Flatten with server context
                flat = []
                for t in tools:
                    flat.append({
                        "server_id": srv["id"],
                        "server_name": srv["name"],
                        "name": t.get("name", ""),
                        "description": t.get("description", ""),
                        "inputSchema": t.get("inputSchema", {}),
                        "_meta": t.get("_meta", {}),
                    })
                self._mcp_catalog_cache[cache_key] = {"at": now, "tools": flat}
                catalog.extend(flat)
            except Exception as e:
                print(f"[MCP auto] catalog fetch failed on {srv['id']}: {e}")
        return catalog

    def _rank_mcp_tools(self, query: str, catalog: list[dict]) -> list[dict]:
        """
        Score each tool against the query. Higher is better.
        Factors:
          - Lexical overlap of query tokens with tool name + description
          - Name-match bonus (query tokens appearing in tool name count double)
          - Action-verb alignment (search, get, fetch, list, analyze, etc.)
          - Schema-compatibility bonus (tool has an arg we can plausibly fill)
        """
        q_tokens = set(re.findall(r"\w{3,}", query.lower()))
        if not q_tokens:
            return []

        action_verbs = {
            "search": {"search", "find", "lookup", "query", "look"},
            "get":    {"get", "fetch", "retrieve", "read", "show", "display"},
            "list":   {"list", "enumerate", "browse"},
            "analyze":{"analyze", "analyse", "evaluate", "assess", "review", "score"},
            "generate":{"generate", "create", "produce", "build", "draft"},
            "summarize":{"summarize","summarise","summary","brief"},
        }
        query_intent = set()
        for intent, synonyms in action_verbs.items():
            if synonyms & q_tokens:
                query_intent.add(intent)

        scored = []
        for t in catalog:
            name = (t.get("name") or "").lower()
            desc = (t.get("description") or "").lower()
            name_tokens = set(re.findall(r"\w{3,}", name))
            desc_tokens = set(re.findall(r"\w{3,}", desc))

            # Base: topical overlap with description
            desc_overlap = len(q_tokens & desc_tokens)
            # Name overlap weighted heavier
            name_overlap = len(q_tokens & name_tokens) * 2.0

            # Intent alignment: reward if the tool name/description uses the same action verbs
            intent_bonus = 0.0
            for intent, synonyms in action_verbs.items():
                if intent not in query_intent:
                    continue
                if synonyms & (name_tokens | desc_tokens):
                    intent_bonus += 1.5

            # Schema compatibility: small bonus if the tool has no required args
            # (safe to call blind), smaller bonus if we can fill required args
            schema = t.get("inputSchema") or {}
            required = schema.get("required") or []
            props = (schema.get("properties") or {})
            if not required:
                schema_bonus = 0.8
            else:
                can_fill = sum(1 for r in required
                               if self._can_infer_arg(query, r, props.get(r, {})))
                schema_bonus = (can_fill / len(required)) * 1.2

            score = desc_overlap + name_overlap + intent_bonus + schema_bonus
            if score <= 0:
                continue
            t_copy = dict(t)
            t_copy["_score"] = score
            scored.append(t_copy)

        scored.sort(key=lambda x: x["_score"], reverse=True)
        return scored

    def _can_infer_arg(self, query: str, arg_name: str, arg_schema: dict) -> bool:
        """Quick check — can we plausibly fill this argument from the query?"""
        arg_type = (arg_schema.get("type") or "string").lower()
        arg_name_l = arg_name.lower()

        # Very permissive for string args — most can be filled with the query itself
        if arg_type == "string":
            # These special names are usually fillable from free-text queries
            if any(k in arg_name_l for k in (
                "query", "question", "text", "content", "topic",
                "keyword", "search", "term", "prompt", "input",
                "url", "link", "title", "name"
            )):
                return True
            # ID-ish args need the query to contain an ID pattern
            if "id" in arg_name_l:
                return bool(re.search(r"\b[A-Z0-9]{3,}[-_]?\d+\b|\b[a-f0-9]{8,}\b", query))
            # Otherwise it's still a string; we'll pass the query and hope
            return True

        if arg_type in ("integer", "number"):
            return bool(re.search(r"\b\d+\b", query))

        if arg_type == "boolean":
            return True  # default to False, schema usually has defaults

        if arg_type == "array":
            return True  # we'll pass [] or split the query

        return False  # object/unknown — skip

    def _auto_fill_tool_args(self, query: str, input_schema: dict) -> dict:
        """
        Produce an argument dict for a tool given only the query.
        Heuristic and intentionally simple — covers 90% of real MCP tools
        whose schemas look like {query: str} or {topic: str, limit?: int}.
        Anything more complex falls through with sensible defaults.
        """
        if not input_schema or not isinstance(input_schema, dict):
            return {}
        props = input_schema.get("properties") or {}
        required = set(input_schema.get("required") or [])
        if not props:
            return {}

        out: dict = {}
        for arg_name, schema in props.items():
            arg_type = (schema.get("type") or "string").lower()
            arg_name_l = arg_name.lower()
            default = schema.get("default")
            is_required = arg_name in required

            # Prefer explicit default when present and not required
            if not is_required and default is not None:
                out[arg_name] = default
                continue

            if arg_type == "string":
                # URL-shaped args — look for a URL in the query
                if any(k in arg_name_l for k in ("url", "link", "href")):
                    m = re.search(r"https?://\S+", query)
                    if m: out[arg_name] = m.group(0); continue
                    if is_required: out[arg_name] = query  # fallback
                    continue
                # ID-shaped args — look for ID patterns
                if re.search(r"(_|^)id(_|$)", arg_name_l) or arg_name_l.endswith("_id"):
                    m = re.search(r"\b([A-Z]+[-_]?\d{2,}|[a-f0-9]{8,})\b", query)
                    if m: out[arg_name] = m.group(0); continue
                    if is_required: out[arg_name] = ""  # fallback
                    continue
                # Date-shaped args — only fill if we find an actual date pattern
                if any(k in arg_name_l for k in ("date", "from", "to", "since", "until", "time")):
                    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", query)
                    if m: out[arg_name] = m.group(1); continue
                    # No date in query — skip (let server default take over)
                    if is_required:
                        # Only fill if required, use today
                        out[arg_name] = time.strftime("%Y-%m-%d")
                    continue
                # Email / phone patterns — only fill if matched
                if "email" in arg_name_l:
                    m = re.search(r"\b[\w.+-]+@[\w-]+\.\w+\b", query)
                    if m: out[arg_name] = m.group(0); continue
                    if is_required: out[arg_name] = ""
                    continue
                # "limit"-ish (shouldn't be string but some tools have it)
                # Default: use the query as the string (best-fit for query/topic/keyword args)
                if is_required or any(k in arg_name_l for k in (
                    "query", "question", "text", "content", "topic",
                    "keyword", "search", "term", "prompt", "input", "title", "name"
                )):
                    out[arg_name] = query
                # Otherwise skip — let server default handle it
                continue

            if arg_type in ("integer", "number"):
                # Pull first integer from query, else use schema default or 10
                m = re.search(r"\b(\d+)\b", query)
                if m:
                    val = int(m.group(1))
                    out[arg_name] = val if arg_type == "integer" else float(val)
                elif default is not None:
                    out[arg_name] = default
                elif is_required:
                    # reasonable default for "limit", "max_results", "top_k", etc.
                    low_name = arg_name_l
                    if any(k in low_name for k in ("limit", "max", "top", "count", "k")):
                        out[arg_name] = 10
                    else:
                        out[arg_name] = 1
                continue

            if arg_type == "boolean":
                if default is not None:
                    out[arg_name] = default
                elif is_required:
                    out[arg_name] = False
                continue

            if arg_type == "array":
                items = schema.get("items", {}) or {}
                item_type = (items.get("type") or "string").lower()
                if item_type == "string":
                    # Split query on commas if any, else send single-item array
                    if "," in query:
                        out[arg_name] = [p.strip() for p in query.split(",") if p.strip()]
                    else:
                        out[arg_name] = [query] if is_required else []
                else:
                    out[arg_name] = []
                continue

            # Object/unknown — skip unless required
            if is_required:
                out[arg_name] = {}

        return out

    # ── End auto-routing helpers ──

    def _parse_mcp_result(self, result: dict, tool_def: Optional[dict],
                           client: Any, src_meta: dict,
                           query: str) -> tuple[list[dict], list[dict]]:
        """
        Parse a tool call result per the MCP spec.

        The result shape:
          {
            "content": [
              {"type": "text", "text": "..."},
              {"type": "image", "data": "<b64>", "mimeType": "image/png"},
              {"type": "resource", "resource": {"uri": "...", "text": "..." | "blob": "<b64>", "mimeType": "..."}},
            ],
            "structuredContent": {...},   // optional, model-facing structured data
            "isError": false,
            "_meta": {...}
          }

        We extract:
          - text_chunks: from every text block + from structuredContent (stringified) + from ui:// HTML
          - images:      from every image block + from <img> tags inside any HTML resource + from image resources
        """
        text_chunks: list[dict] = []
        images: list[dict] = []
        image_budget = self.valves.mcp_max_image_extract_per_call

        # 1) Walk content blocks
        for block in (result.get("content") or []):
            btype = block.get("type")
            if btype == "text":
                txt = (block.get("text") or "").strip()
                if txt:
                    for sub in self._chunk_text(txt):
                        text_chunks.append({
                            "content": sub, "source": src_meta["source"],
                            "page": 0, "doc_type": "mcp",
                        })
            elif btype == "image":
                if len(images) >= image_budget:
                    continue
                rec = self._ingest_mcp_image_block(block, src_meta,
                                                     caption=src_meta.get("mcp_tool_name", ""))
                if rec:
                    images.append(rec)
            elif btype == "resource":
                res = block.get("resource") or {}
                # Recurse into resource contents
                tc, ti = self._extract_from_mcp_resource_contents([res], src_meta, client)
                text_chunks.extend(tc)
                for r in ti:
                    if len(images) < image_budget:
                        images.append(r)

        # 2) structuredContent — stringify as one extra text chunk
        sc = result.get("structuredContent")
        if sc:
            try:
                sc_text = json.dumps(sc, indent=2)[:4000]
                text_chunks.append({
                    "content": f"Structured output:\n{sc_text}",
                    "source": src_meta["source"],
                    "page": 0, "doc_type": "mcp",
                })
            except Exception:
                pass

        # 3) If tool has _meta.ui.resourceUri and we haven't already harvested it,
        #    fetch and parse (caller also does this when images is empty — that's fine,
        #    the url-based caching inside client makes it cheap)
        if tool_def and not images:
            ui_uri = (tool_def.get("_meta") or {}).get("ui", {}).get("resourceUri")
            if ui_uri:
                try:
                    res = client.read_resource(ui_uri)
                    tc, ti = self._extract_from_mcp_resource_contents(
                        res.get("contents", []), src_meta, client
                    )
                    text_chunks.extend(tc)
                    for r in ti:
                        if len(images) < image_budget:
                            images.append(r)
                except Exception as e:
                    print(f"[MCP] UI resource fetch in _parse_mcp_result failed: {e}")

        # 4) Fallback: scan text + structuredContent for image URLs (covers ICA
        # image-generator returning a pre-signed S3 URL in a text block, and
        # any MCP tool that returns URLs to images in its text output).
        if len(images) < image_budget:
            url_imgs = self._extract_images_from_mcp_urls(result, src_meta, query)
            for r in url_imgs:
                if len(images) < image_budget:
                    images.append(r)

        return text_chunks, images

    def _extract_from_mcp_resource_contents(
        self, contents: list[dict], src_meta: dict, client: Any
    ) -> tuple[list[dict], list[dict]]:
        """
        A resource's 'contents' is a list of embedded parts. Each part can be:
          - text:  {"uri": "...", "mimeType": "text/plain"|"text/html"|"application/json", "text": "..."}
          - blob:  {"uri": "...", "mimeType": "image/png"|..., "blob": "<b64>"}
        HTML content (including text/html;profile=mcp-app) is scanned for <img> tags
        and <svg> blocks, which we extract as images.
        """
        text_chunks: list[dict] = []
        images: list[dict] = []

        for part in (contents or []):
            mime = (part.get("mimeType") or "").lower()
            uri = part.get("uri", "")
            is_ui = uri.startswith("ui://") or "mcp-app" in mime or "skybridge" in mime

            if part.get("blob"):
                # Binary resource (often an image)
                if mime.startswith("image/"):
                    try:
                        img_bytes = base64.b64decode(part["blob"])
                        rec = self._ingest_raw_image_bytes(
                            img_bytes, src_meta,
                            caption=self._humanize(uri.split("/")[-1] or "mcp_resource"),
                            context=uri,
                            location=uri or "mcp resource",
                        )
                        if rec:
                            images.append(rec)
                    except Exception as e:
                        print(f"[MCP] blob image decode failed: {e}")

            elif part.get("text") is not None:
                txt = part["text"]
                if mime.startswith("text/html") or is_ui:
                    # Parse HTML for inline images + text
                    tchunks, iimgs = self._extract_from_html(txt, src_meta, base_uri=uri)
                    text_chunks.extend(tchunks)
                    images.extend(iimgs)
                elif mime == "application/json":
                    try:
                        parsed = json.loads(txt)
                        pretty = json.dumps(parsed, indent=2)[:4000]
                        text_chunks.append({
                            "content": pretty, "source": src_meta["source"],
                            "page": 0, "doc_type": "mcp",
                        })
                    except Exception:
                        text_chunks.append({
                            "content": txt[:4000], "source": src_meta["source"],
                            "page": 0, "doc_type": "mcp",
                        })
                else:
                    # Plain text
                    for sub in self._chunk_text(txt):
                        text_chunks.append({
                            "content": sub, "source": src_meta["source"],
                            "page": 0, "doc_type": "mcp",
                        })

        return text_chunks, images

    def _extract_from_html(self, html: str, src_meta: dict,
                            base_uri: str = "") -> tuple[list[dict], list[dict]]:
        """
        Scan HTML for <img> tags and inline <svg> blocks. Pull visible text as chunks.
        Lightweight — uses regex, not a full DOM parser (which would be overkill here).
        """
        text_chunks: list[dict] = []
        images: list[dict] = []

        # --- Extract <img> src values ---
        for m in re.finditer(r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>', html, re.I):
            src = m.group(1).strip()
            if not src:
                continue
            # alt text for caption
            alt_m = re.search(r'alt=["\']([^"\']*)["\']', m.group(0), re.I)
            alt = alt_m.group(1) if alt_m else ""
            rec = self._ingest_html_image_src(src, src_meta, caption=alt, context=base_uri)
            if rec:
                images.append(rec)

        # --- Extract inline <svg>...</svg> blocks, rasterize if possible ---
        for m in re.finditer(r'<svg[\s>].*?</svg>', html, re.I | re.S):
            svg_text = m.group(0)
            try:
                if HAS_CAIROSVG:
                    png_bytes = cairosvg.svg2png(bytestring=svg_text.encode("utf-8"),
                                                  output_width=1200)
                    rec = self._ingest_raw_image_bytes(
                        png_bytes, src_meta,
                        caption="inline SVG",
                        context=svg_text[:400],
                        location=base_uri or "inline SVG",
                    )
                    if rec:
                        images.append(rec)
            except Exception as e:
                print(f"[MCP] inline SVG rasterize failed: {e}")

        # --- Visible text from the HTML body ---
        # Strip tags very loosely; good enough for lexical ranking
        stripped = re.sub(r'<script.*?</script>', ' ', html, flags=re.I | re.S)
        stripped = re.sub(r'<style.*?</style>', ' ', stripped, flags=re.I | re.S)
        stripped = re.sub(r'<[^>]+>', ' ', stripped)
        stripped = re.sub(r'\s+', ' ', stripped).strip()
        if stripped:
            for sub in self._chunk_text(stripped):
                text_chunks.append({
                    "content": sub, "source": src_meta["source"],
                    "page": 0, "doc_type": "mcp",
                })

        return text_chunks, images

    def _ingest_mcp_image_block(self, block: dict, src_meta: dict,
                                  caption: str = "") -> Optional[dict]:
        """An MCP content block of type 'image' carries base64 + mimeType."""
        data = block.get("data")
        if not data:
            return None
        try:
            img_bytes = base64.b64decode(data)
        except Exception:
            return None
        return self._ingest_raw_image_bytes(
            img_bytes, src_meta,
            caption=caption or "mcp image",
            context=src_meta.get("mcp_tool_name", ""),
            location=src_meta.get("source", "mcp"),
        )

    def _ingest_raw_image_bytes(self, img_bytes: bytes, src_meta: dict,
                                  caption: str, context: str,
                                  location: str) -> Optional[dict]:
        """Generic ingest path — used by MCP image blocks, resource blobs, and HTML <img>."""
        try:
            pil = Image.open(io.BytesIO(img_bytes)).convert("RGB")
        except Exception:
            return None
        if not self._passes_filter(pil):
            return None
        return self._store_image(pil, src_meta, caption, context, location,
                                  tag=src_meta.get("mcp_tool_name", "mcp"))

    def _ingest_html_image_src(self, src: str, src_meta: dict,
                                 caption: str, context: str) -> Optional[dict]:
        """Handle both data: URIs and http(s): URLs inside an HTML <img>."""
        src = src.strip()
        if src.startswith("data:"):
            # data:image/png;base64,xxxx  — decode directly
            try:
                _, b64_part = src.split(",", 1)
                img_bytes = base64.b64decode(b64_part)
            except Exception:
                return None
            return self._ingest_raw_image_bytes(
                img_bytes, src_meta, caption=caption or "inline data URI",
                context=context, location=src[:60] + "...",
            )
        if src.startswith(("http://", "https://")):
            try:
                r = requests.get(src, timeout=self.valves.web_image_fetch_timeout,
                                 headers={"User-Agent": "Mozilla/5.0 (ibm-docgen)"})
                r.raise_for_status()
                if not r.content:
                    return None
                return self._ingest_raw_image_bytes(
                    r.content, src_meta,
                    caption=caption or urlparse(src).path.rsplit("/", 1)[-1],
                    context=context or src,
                    location=f"url: {urlparse(src).netloc}",
                )
            except Exception as e:
                print(f"[MCP] HTML image fetch failed for {src}: {e}")
                return None
        # Unsupported scheme (e.g. cid:, file:) — skip
        return None

    # ══════════════════════════════════════════════════════════════════════
    # OWUI API
    # ══════════════════════════════════════════════════════════════════════
    def _auth_from_request(self, request) -> dict:
        headers = {}
        if request and hasattr(request, "headers"):
            auth = request.headers.get("authorization")
            if auth:
                headers["Authorization"] = auth
        return headers

    def _retrieve_text_from_collection(self, query: str, collection_id: str, auth: dict) -> list[dict]:
        url = f"{self.valves.owui_base_url}/api/v1/retrieval/query/collection"
        payload = {
            "collection_names": [collection_id],
            "query": query,
            "k": self.valves.max_text_chunks,
        }
        try:
            r = requests.post(url, json=payload, headers=auth, timeout=self.valves.request_timeout)
            r.raise_for_status()
            data = r.json()
            docs = data.get("documents", [[]])[0] if data.get("documents") else []
            metas = data.get("metadatas", [[]])[0] if data.get("metadatas") else []
            return [
                {
                    "content": d,
                    "source": m.get("source") or m.get("name") or m.get("file_id", "unknown"),
                    "file_id": m.get("file_id"),
                    "page": m.get("page", 0),
                    "doc_type": self._classify(m.get("source", "")),
                }
                for d, m in zip(docs, metas)
            ]
        except Exception as e:
            print(f"[DocGen] Retrieval failed: {e}")
            return []

    def _list_collection_files(self, collection_id: str, auth: dict) -> list[dict]:
        url = f"{self.valves.owui_base_url}/api/v1/knowledge/{collection_id}"
        try:
            r = requests.get(url, headers=auth, timeout=self.valves.request_timeout)
            r.raise_for_status()
            data = r.json()
            files = data.get("files", []) or data.get("data", {}).get("files", [])
            out = []
            for f in files:
                fid = f.get("id") or f.get("file_id")
                name = (
                    f.get("meta", {}).get("name")
                    or f.get("name")
                    or f.get("filename", "")
                )
                if fid and name:
                    out.append({
                        "file_id": fid,
                        "source": name,
                        "ext": self._ext(name),
                        "doc_type": self._classify(name),
                    })
            return out
        except Exception as e:
            print(f"[DocGen] List collection failed: {e}")
            return []

    def _local_upload_path(self, file_id: str) -> Optional[str]:
        """Locate the OWUI-stored file on disk by file_id prefix.

        Avoids calling OWUI's own HTTP API from within an async tool handler
        (which deadlocks the event loop on large files).
        """
        try:
            import os, glob
            # Try each of the plausible uploads roots.
            roots = []
            try:
                import open_webui  # type: ignore
                pkg_dir = os.path.dirname(open_webui.__file__)
                roots.append(os.path.join(pkg_dir, "data", "uploads"))
            except Exception:
                pass
            roots.append(os.path.expanduser("~/.local/share/uv/tools/open-webui/lib/python3.12/site-packages/open_webui/data/uploads"))
            for root in roots:
                if not root or not os.path.isdir(root):
                    continue
                hits = glob.glob(os.path.join(root, f"{file_id}_*"))
                if hits:
                    return hits[0]
            return None
        except Exception as e:
            print(f"[DocGen] local_upload_path failed for {file_id}: {e}")
            return None

    def _fetch_file_metadata(self, file_id: str, auth: dict) -> Optional[dict]:
        # Prefer direct disk lookup — avoids self-request deadlock.
        local = self._local_upload_path(file_id)
        if local:
            import os
            fname = os.path.basename(local)
            # OWUI format is "<uuid>_<original filename>" — strip the uuid_ prefix.
            if fname.startswith(f"{file_id}_"):
                fname = fname[len(file_id) + 1:]
            return {"name": fname, "id": file_id}
        url = f"{self.valves.owui_base_url}/api/v1/files/{file_id}"
        try:
            r = requests.get(url, headers=auth, timeout=self.valves.request_timeout)
            r.raise_for_status()
            data = r.json()
            return {
                "name": data.get("filename") or data.get("meta", {}).get("name") or file_id,
                "id": file_id,
            }
        except Exception as e:
            print(f"[DocGen] File metadata fetch failed for {file_id}: {e}")
            return None

    def _fetch_file_bytes(self, file_id: str, auth: dict) -> Optional[bytes]:
        # Prefer direct disk lookup — avoids self-request deadlock.
        local = self._local_upload_path(file_id)
        if local:
            try:
                with open(local, "rb") as fh:
                    return fh.read()
            except Exception as e:
                print(f"[DocGen] local file read failed for {file_id}: {e}")
        url = f"{self.valves.owui_base_url}/api/v1/files/{file_id}/content"
        try:
            r = requests.get(url, headers=auth, timeout=self.valves.request_timeout)
            r.raise_for_status()
            return r.content
        except Exception as e:
            print(f"[DocGen] File bytes fetch failed for {file_id}: {e}")
            return None

    def _extract_one_attachment(self, fid: str, auth: dict) -> tuple[list[dict], list[dict]]:
        """Fetch + extract text & images for one attachment. Cached by file hash.

        Returns (text_chunks, images). Designed to be called concurrently from
        ThreadPoolExecutor — network/disk I/O + CPU-bound decode both release
        the GIL (requests, Pillow, PyMuPDF, zipfile).
        """
        try:
            meta = self._fetch_file_metadata(fid, auth)
            if not meta:
                return [], []
            fbytes = self._fetch_file_bytes(fid, auth)
            if not fbytes:
                return [], []
            cached = _EXTRACT_CACHE.get(fbytes)
            if cached is not None:
                return cached
            src = {
                "file_id": fid,
                "source": meta.get("name", fid),
                "ext": self._ext(meta.get("name", "")),
                "doc_type": self._classify(meta.get("name", "")),
            }
            text_chunks = self._extract_text_from_bytes(fbytes, src)
            images = self._extract_images_from_bytes(fbytes, src)
            _EXTRACT_CACHE.put(fbytes, text_chunks, images)
            return text_chunks, images
        except Exception as e:
            print(f"[DocGen] _extract_one_attachment({fid}) failed: {e}")
            return [], []

    def _extract_attachments_parallel(self, file_ids: list[str], auth: dict,
                                      max_workers: int = 4) -> tuple[list[dict], list[dict]]:
        """Parallel fan-out of _extract_one_attachment. 4× speedup on 4+ files."""
        all_text, all_images = [], []
        if not file_ids:
            return all_text, all_images
        if len(file_ids) == 1:
            return self._extract_one_attachment(file_ids[0], auth)
        with ThreadPoolExecutor(max_workers=min(max_workers, len(file_ids))) as ex:
            futures = {ex.submit(self._extract_one_attachment, fid, auth): fid
                       for fid in file_ids}
            for fut in as_completed(futures):
                try:
                    tc, ti = fut.result()
                    all_text.extend(tc)
                    all_images.extend(ti)
                except Exception as e:
                    print(f"[DocGen] parallel extract failed for {futures[fut]}: {e}")
        return all_text, all_images

    def _ingest_images_parallel(self, candidates: list[dict], max_workers: int = 6) -> list[dict]:
        """Parallel download/ingest of web image candidates. 5-6× speedup."""
        out: list[dict] = []
        if not candidates:
            return out
        with ThreadPoolExecutor(max_workers=min(max_workers, len(candidates))) as ex:
            futures = [ex.submit(self._ingest_remote_image, c["url"], c) for c in candidates]
            for fut in as_completed(futures):
                try:
                    rec = fut.result()
                    if rec:
                        out.append(rec)
                except Exception as e:
                    print(f"[DocGen] parallel ingest failed: {e}")
        return out

    # ══════════════════════════════════════════════════════════════════════
    # COLLECTION EXTRACTION
    # ══════════════════════════════════════════════════════════════════════
    def _extract_from_collection(self, text_chunks, collection_files, auth) -> list[dict]:
        all_images = []
        referenced_names = {c.get("source") for c in text_chunks}
        for f in collection_files:
            is_ref = f["source"] in referenced_names
            is_standalone = f["ext"] in ALL_IMG_EXT
            is_doc = f["ext"] in ALL_DOC_EXT
            if (is_ref and is_doc) or is_standalone:
                fbytes = self._fetch_file_bytes(f["file_id"], auth)
                if fbytes:
                    all_images.extend(self._extract_images_from_bytes(fbytes, f))
        return all_images

    def _extract_text_from_bytes(self, file_bytes: bytes, src: dict) -> list[dict]:
        """Used for chat attachments (not indexed by OWUI)."""
        ext = src["ext"]
        chunks = []
        try:
            if ext in PDF_EXT:
                doc = fitz.open(stream=file_bytes, filetype="pdf")
                try:
                    for page_num, page in enumerate(doc):
                        text = page.get_text("text").strip()
                        if text:
                            for sub in self._chunk_text(text):
                                chunks.append({
                                    "content": sub, "source": src["source"],
                                    "page": page_num + 1, "doc_type": src["doc_type"],
                                })
                finally:
                    doc.close()
            elif ext in DOCX_EXT:
                txt = self._docx_extract_text(file_bytes)
                for sub in self._chunk_text(txt):
                    chunks.append({"content": sub, "source": src["source"], "page": 0, "doc_type": src["doc_type"]})
            elif ext in PPTX_EXT:
                txt = self._pptx_extract_text(file_bytes)
                for sub in self._chunk_text(txt):
                    chunks.append({"content": sub, "source": src["source"], "page": 0, "doc_type": src["doc_type"]})
            elif ext in XLSX_EXT and HAS_XLSX:
                txt = self._xlsx_extract_text(file_bytes)
                for sub in self._chunk_text(txt):
                    chunks.append({"content": sub, "source": src["source"], "page": 0, "doc_type": src["doc_type"]})
        except Exception as e:
            print(f"[DocGen] Text extraction failed for {src['source']}: {e}")
        return chunks

    def _chunk_text(self, text: str, size: int = 1000, overlap: int = 200) -> list[str]:
        words = text.split()
        out, i = [], 0
        while i < len(words):
            out.append(" ".join(words[i:i+size]))
            i += size - overlap
        return [c for c in out if len(c.strip()) > 50]

    def _extract_images_from_bytes(self, file_bytes: bytes, src: dict) -> list[dict]:
        ext = src["ext"]
        images: list[dict] = []
        try:
            if ext in PDF_EXT:
                images.extend(self._extract_pdf_images(file_bytes, src))
                # Also render each PDF page as a full-page image so text-heavy
                # docs still produce useful visuals for the output.
                images.extend(self._render_pdf_pages(file_bytes, src))
            elif ext in DOCX_EXT:
                images.extend(self._extract_docx_images(file_bytes, src))
                images.extend(self._render_office_pages(file_bytes, src, ".docx"))
            elif ext in PPTX_EXT:
                images.extend(self._extract_pptx_images(file_bytes, src))
                images.extend(self._render_office_pages(file_bytes, src, ".pptx"))
            elif ext in XLSX_EXT and HAS_XLSX:
                images.extend(self._extract_xlsx_images(file_bytes, src))
            elif ext in IMG_EXT:
                images.extend(self._ingest_standalone_image(file_bytes, src))
            elif ext in SVG_EXT:
                images.extend(self._ingest_svg_image(file_bytes, src))
        except Exception as e:
            print(f"[DocGen] Image extraction dispatch failed: {e}")
        return images

    # ── Page-render helpers (PPTX/DOCX → PDF → PNG per page) ──
    def _soffice_binary(self) -> Optional[str]:
        import os, shutil
        for cand in (
            "/Applications/LibreOffice.app/Contents/MacOS/soffice",
            "/opt/homebrew/bin/soffice",
            "/usr/local/bin/soffice",
            shutil.which("soffice") or "",
            shutil.which("libreoffice") or "",
        ):
            if cand and os.path.isfile(cand):
                return cand
        return None

    def _office_to_pdf(self, file_bytes: bytes, suffix: str) -> Optional[bytes]:
        """Convert a PPTX/DOCX byte blob to PDF via headless LibreOffice."""
        soffice = self._soffice_binary()
        if not soffice:
            return None
        import os, subprocess, tempfile, uuid
        with tempfile.TemporaryDirectory(prefix="docgen_") as td:
            in_path = os.path.join(td, f"in_{uuid.uuid4().hex}{suffix}")
            with open(in_path, "wb") as fh:
                fh.write(file_bytes)
            try:
                subprocess.run(
                    [soffice, "--headless", "--norestore", "--convert-to", "pdf",
                     "--outdir", td, in_path],
                    check=True, timeout=180,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                )
            except Exception as e:
                print(f"[DocGen] soffice conversion failed: {e}")
                return None
            # soffice names output <in-stem>.pdf
            pdf_path = os.path.splitext(in_path)[0] + ".pdf"
            if not os.path.exists(pdf_path):
                return None
            with open(pdf_path, "rb") as fh:
                return fh.read()

    def _render_pdf_pages(self, pdf_bytes: bytes, src: dict,
                          max_pages: int = 40, dpi: int = 110) -> list[dict]:
        """Render each page of a PDF to PNG and store as an image candidate."""
        try:
            import fitz  # PyMuPDF
        except ImportError:
            return []
        out: list[dict] = []
        try:
            probe = fitz.open(stream=pdf_bytes, filetype="pdf")
            n = min(len(probe), max_pages)
            probe.close()
        except Exception as e:
            print(f"[DocGen] PDF page render failed: {e}")
            return out
        zoom = dpi / 72.0

        def _render_one(i: int):
            try:
                d = fitz.open(stream=pdf_bytes, filetype="pdf")
                mat = fitz.Matrix(zoom, zoom)
                page = d.load_page(i)
                pix = page.get_pixmap(matrix=mat, alpha=False)
                png = pix.tobytes("png")
                d.close()
                img_id = f"pagerender_{src.get('file_id','x')}_p{i+1}_{uuid.uuid4().hex[:6]}"
                meta = {
                    "source": src.get("source"),
                    "doc_type": src.get("doc_type"),
                    "page": i + 1,
                    "kind": "page_render",
                    "caption": f"{src.get('source','')} — page {i+1}",
                }
                _IMAGE_STORE.put(img_id, png, meta)
                return {"id": img_id, "png_bytes": png, "metadata": meta}
            except Exception as e:
                print(f"[DocGen] page render p{i+1} failed: {e}")
                return None

        # PyMuPDF is not thread-safe across a shared Document, so each worker
        # opens its own Document from the same bytes. Pixmap encode releases
        # the GIL in PyMuPDF — real parallelism.
        with ThreadPoolExecutor(max_workers=min(4, n or 1)) as ex:
            for rec in ex.map(_render_one, range(n)):
                if rec:
                    out.append(rec)
        out.sort(key=lambda r: r.get("metadata", {}).get("page", 0))
        return out

    def _render_office_pages(self, office_bytes: bytes, src: dict,
                             suffix: str) -> list[dict]:
        """Page/slide rendering for PPTX/DOCX.

        Order (pure-Python FIRST, LibreOffice only as fallback):
          1. Pure-Python path (no external deps, works in locked-down IBM envs):
             - PPTX: composite each slide via Pillow (_render_pptx_slides_pure_python).
             - DOCX: no pure-Python layout engine available.
          2. LibreOffice path (richer fidelity): PPTX/DOCX → PDF → page PNGs.
             Used only if pure-Python yielded nothing.
        """
        # 1. Pure-Python first.
        if suffix.lower() == ".pptx":
            snaps = self._render_pptx_slides_pure_python(office_bytes, src)
            if snaps:
                return snaps
        # 2. LibreOffice fallback.
        pdf = self._office_to_pdf(office_bytes, suffix)
        if pdf:
            return self._render_pdf_pages(pdf, src)
        return []

    def _render_pptx_slides_pure_python(self, pptx_bytes: bytes, src: dict,
                                        max_slides: int = 40,
                                        width_px: int = 1280,
                                        height_px: int = 720) -> list[dict]:
        """Pure-Python PPTX slide snapshot (no LibreOffice required).

        Composites each slide into a PNG: solid background + title text + any
        embedded slide image tiled in. Low fidelity but guaranteed to work in
        restricted IBM environments where soffice is not installed. Critical
        for Transition project deliverables.
        """
        out: list[dict] = []
        try:
            from PIL import ImageDraw, ImageFont  # Pillow ships with OWUI
        except Exception:
            return out
        try:
            zf = zipfile.ZipFile(io.BytesIO(pptx_bytes))
        except Exception as e:
            print(f"[DocGen] PPTX snapshot zip open failed: {e}")
            return out
        names = zf.namelist()
        slide_names = sorted(
            [n for n in names if n.startswith("ppt/slides/slide") and n.endswith(".xml")],
            key=lambda n: int(re.search(r"slide(\d+)\.xml", n).group(1)) if re.search(r"slide(\d+)\.xml", n) else 0,
        )[:max_slides]
        ns_a = "{http://schemas.openxmlformats.org/drawingml/2006/main}"

        # Build a mapping of slide number -> candidate background image bytes.
        def _slide_bg_image(slide_name: str) -> Optional[bytes]:
            rels_name = slide_name.replace("ppt/slides/", "ppt/slides/_rels/") + ".rels"
            if rels_name not in names:
                return None
            try:
                rels_xml = zf.read(rels_name).decode("utf-8", errors="ignore")
            except Exception:
                return None
            # Relationship targets for media (e.g. ../media/image3.png)
            targets = re.findall(r'Target="([^"]+media/[^"]+)"', rels_xml)
            for t in targets:
                # Normalise to ppt/media/<file>
                tn = t.replace("../", "ppt/")
                if tn in names:
                    try:
                        b = zf.read(tn)
                        if len(b) > 5000:  # skip tiny logos
                            return b
                    except Exception:
                        continue
            return None

        try:
            font = ImageFont.load_default()
        except Exception:
            font = None

        # Pre-read per-slide bytes under the zipfile lock (ZipFile is not
        # thread-safe for concurrent reads), then render in parallel.
        slide_jobs = []
        for i, slide_name in enumerate(slide_names, start=1):
            try:
                slide_xml_bytes = zf.read(slide_name)
            except Exception:
                slide_xml_bytes = b""
            bg_bytes = _slide_bg_image(slide_name)
            slide_jobs.append((i, slide_xml_bytes, bg_bytes))

        def _render_one(job):
            i, slide_xml_bytes, bg = job
            try:
                # Light theme: white body + IBM-blue accents. Prevents the
                # "all-black rectangle" that happened when a slide had no
                # large embedded image and the navy fallback showed through.
                canvas = Image.new("RGB", (width_px, height_px), (255, 255, 255))
                draw = ImageDraw.Draw(canvas)
                if bg:
                    try:
                        bgim = Image.open(io.BytesIO(bg)).convert("RGB")
                        bgim.thumbnail((width_px, height_px - 140))
                        bx = (width_px - bgim.size[0]) // 2
                        by = 96 + ((height_px - 140 - bgim.size[1]) // 2)
                        canvas.paste(bgim, (bx, by))
                    except Exception:
                        pass
                try:
                    root = ET.fromstring(slide_xml_bytes.decode("utf-8", errors="ignore"))
                    texts = [t.text for t in root.iter(f"{ns_a}t") if t.text]
                except Exception:
                    texts = []
                title = (texts[0] if texts else f"Slide {i}").strip()[:140]
                body_lines = [t.strip() for t in texts[1:8] if t and t.strip()]
                # IBM blue title band
                draw.rectangle([0, 0, width_px, 80], fill=(15, 98, 254))
                draw.text((28, 28), title, fill=(255, 255, 255), font=font)
                # Bullet-style body below the band when no bg image
                if not bg and body_lines:
                    y = 110
                    for line in body_lines[:10]:
                        draw.text((40, y), "• " + line[:160], fill=(30, 40, 60), font=font)
                        y += 34
                        if y > height_px - 60:
                            break
                elif bg and body_lines:
                    # Footer strip with first-bullet context
                    draw.rectangle([0, height_px - 60, width_px, height_px], fill=(240, 243, 250))
                    draw.text((28, height_px - 44), body_lines[0][:180], fill=(30, 40, 60), font=font)
                # Source corner tag (subtle grey)
                draw.text((width_px - 240, height_px - 22),
                          f"{(src.get('source') or '')[:30]} · p{i}",
                          fill=(140, 150, 170), font=font)
                buf = io.BytesIO()
                canvas.save(buf, format="PNG", optimize=True)
                png = buf.getvalue()
                img_id = f"slidesnap_{src.get('file_id','x')}_p{i}_{uuid.uuid4().hex[:6]}"
                meta = {
                    "source": src.get("source"),
                    "doc_type": src.get("doc_type"),
                    "page": i,
                    "kind": "slide_snapshot",
                    "caption": f"{src.get('source','')} — slide {i}: {title}",
                }
                _IMAGE_STORE.put(img_id, png, meta)
                return {"id": img_id, "png_bytes": png, "metadata": meta,
                        "caption": meta["caption"], "source": src.get("source"),
                        "doc_type": src.get("doc_type")}
            except Exception as e:
                print(f"[DocGen] slide snapshot p{i} failed: {e}")
                return None

        # Pillow releases the GIL on encode/resize/paste — real parallelism.
        with ThreadPoolExecutor(max_workers=min(4, len(slide_jobs) or 1)) as ex:
            for rec in ex.map(_render_one, slide_jobs):
                if rec:
                    out.append(rec)
        # Keep slides in page order regardless of completion order.
        out.sort(key=lambda r: r.get("metadata", {}).get("page", 0))
        try:
            zf.close()
        except Exception:
            pass
        return out

    # ── PDF ──
    def _extract_pdf_images(self, pdf_bytes: bytes, src: dict) -> list[dict]:
        out = []
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        try:
            for page_num, page in enumerate(doc):
                for img_idx, img in enumerate(page.get_images(full=True)):
                    xref = img[0]
                    try:
                        base = doc.extract_image(xref)
                        pil = Image.open(io.BytesIO(base["image"])).convert("RGB")
                    except Exception:
                        continue
                    if not self._passes_filter(pil):
                        continue
                    caption = self._pdf_caption(page, img)
                    context = page.get_text("text")[:800]
                    rec = self._store_image(pil, src, caption, context,
                                             f"page {page_num+1}", f"p{page_num}_i{img_idx}")
                    if rec: out.append(rec)
        finally:
            doc.close()
        return out

    def _pdf_caption(self, page, img) -> str:
        try:
            bbox = page.get_image_bbox(img)
            blocks = page.get_text("blocks")
            below = sorted([b for b in blocks if b[1] > bbox.y1 and b[1] - bbox.y1 < 80], key=lambda b: b[1])
            if below: return below[0][4].strip().replace("\n", " ")
            above = [b for b in blocks if b[3] < bbox.y0 and bbox.y0 - b[3] < 60]
            if above: return above[-1][4].strip().replace("\n", " ")
        except Exception: pass
        return ""

    # ── DOCX ──
    def _extract_docx_images(self, docx_bytes: bytes, src: dict) -> list[dict]:
        out = []
        # Filename-only junk filter (never run against paragraph text).
        fn_junk = re.compile(r"(logo|icon|bullet|divider|watermark|thumbnail)", re.I)
        try:
            with zipfile.ZipFile(io.BytesIO(docx_bytes)) as zf:
                rel_map = {}
                try:
                    rels_xml = zf.read("word/_rels/document.xml.rels").decode("utf-8", "ignore")
                    rels_root = ET.fromstring(rels_xml)
                    for rel in rels_root.findall("{http://schemas.openxmlformats.org/package/2006/relationships}Relationship"):
                        if "image" in rel.get("Type", ""):
                            rel_map[rel.get("Id")] = rel.get("Target")
                except Exception: pass

                try:
                    doc_xml = zf.read("word/document.xml").decode("utf-8", "ignore")
                    doc_root = ET.fromstring(doc_xml)
                except Exception:
                    return out

                paragraphs = []
                for p in doc_root.iter(f"{NS_W}p"):
                    text = "".join(t.text or "" for t in p.iter(f"{NS_W}t"))
                    rids = [blip.get(f"{NS_R}embed") for blip in p.iter(f"{NS_A}blip") if blip.get(f"{NS_R}embed")]
                    paragraphs.append({"text": text.strip(), "rids": rids})

                for p_idx, para in enumerate(paragraphs):
                    for rid in para["rids"]:
                        target = rel_map.get(rid)
                        if not target: continue
                        media_path = f"word/{target}" if not target.startswith("word/") else target
                        leaf = media_path.rsplit("/", 1)[-1]
                        if fn_junk.search(leaf):
                            continue
                        try: blob = zf.read(media_path)
                        except KeyError:
                            try: blob = zf.read(target)
                            except KeyError: continue
                        try: pil = Image.open(io.BytesIO(blob)).convert("RGB")
                        except Exception: continue
                        if not self._passes_filter(pil): continue

                        caption = para["text"]
                        if not caption:
                            for q in range(p_idx + 1, min(p_idx + 3, len(paragraphs))):
                                if paragraphs[q]["text"]:
                                    caption = paragraphs[q]["text"]; break

                        ctx_parts = [paragraphs[q]["text"]
                                     for q in range(max(0, p_idx-2), min(len(paragraphs), p_idx+3))
                                     if paragraphs[q]["text"]]
                        context = " ".join(ctx_parts)[:800]

                        rec = self._store_image(pil, src, caption[:400], context,
                                                 f"paragraph {p_idx}", f"docx_{rid}")
                        if rec: out.append(rec)
        except Exception as e:
            print(f"[DocGen] DOCX image extraction error: {e}")
        return out

    def _docx_extract_text(self, docx_bytes: bytes) -> str:
        try:
            with zipfile.ZipFile(io.BytesIO(docx_bytes)) as zf:
                doc_xml = zf.read("word/document.xml").decode("utf-8", "ignore")
                root = ET.fromstring(doc_xml)
                return "\n".join(t.text for t in root.iter(f"{NS_W}t") if t.text)
        except Exception as e:
            print(f"[DocGen] DOCX text failed: {e}")
            return ""

    # ── PPTX ──
    def _extract_pptx_images(self, pptx_bytes: bytes, src: dict) -> list[dict]:
        """Extract embedded images from a PPTX deck.

        Strategy:
          1. Pass A — walk every slide's blips, pair each image with its slide
             title + body text so ranking has real semantic context.
          2. Pass B — sweep anything under ppt/media/* that Pass A missed
             (images referenced only by masters/layouts/themes, or unused but
             present). Captioned with the deck name so they still rank.
          3. Junk filter runs on the media **filename** only — never on slide
             text, since e.g. a slide titled "Header-less Observability" was
             previously dropping its diagrams.
        """
        out: list[dict] = []
        seen_media: set = set()
        # Filename-only junk filter. Matches common chrome like logo.png,
        # bullet-square.png, divider.svg — never false-positives on real content.
        fn_junk = re.compile(r"(logo|icon|bullet|divider|watermark|thumbnail)", re.I)
        try:
            with zipfile.ZipFile(io.BytesIO(pptx_bytes)) as zf:
                names = zf.namelist()
                # Pass A: per-slide blips, with slide-title + body as context.
                slide_files = sorted(
                    [n for n in names if re.match(r"ppt/slides/slide\d+\.xml$", n)],
                    key=lambda n: int(re.search(r"slide(\d+)", n).group(1))
                )
                for slide_idx, slide_name in enumerate(slide_files):
                    try:
                        slide_xml = zf.read(slide_name).decode("utf-8", "ignore")
                        slide_root = ET.fromstring(slide_xml)
                    except Exception:
                        continue

                    texts = [t.text for t in slide_root.iter(f"{NS_A}t") if t.text]
                    slide_text = " | ".join(texts)[:800]
                    title = (texts[0] if texts else "").strip()

                    rels_name = slide_name.replace("ppt/slides/", "ppt/slides/_rels/").replace(".xml", ".xml.rels")
                    rel_map = {}
                    try:
                        rels_xml = zf.read(rels_name).decode("utf-8", "ignore")
                        rels_root = ET.fromstring(rels_xml)
                        for rel in rels_root.findall("{http://schemas.openxmlformats.org/package/2006/relationships}Relationship"):
                            if "image" in rel.get("Type", ""):
                                rel_map[rel.get("Id")] = rel.get("Target")
                    except Exception:
                        pass

                    for shape_idx, blip in enumerate(slide_root.iter(f"{NS_A}blip")):
                        rid = blip.get(f"{NS_R}embed")
                        if not rid or rid not in rel_map:
                            continue
                        target = rel_map[rid]
                        # Resolve relative path inside the PPTX zip.
                        if target.startswith("../"):
                            media_path = "ppt/" + target[3:]
                        elif target.startswith("/"):
                            media_path = target.lstrip("/")
                        else:
                            media_path = f"ppt/slides/{target}"
                        if media_path not in names:
                            leaf = target.rsplit("/", 1)[-1]
                            match = next((n for n in names if n.endswith(leaf) and "media" in n), None)
                            if match:
                                media_path = match
                            else:
                                continue
                        # Junk filter on filename only.
                        if fn_junk.search(media_path.rsplit("/", 1)[-1]):
                            continue
                        if media_path in seen_media:
                            continue
                        seen_media.add(media_path)
                        try:
                            blob = zf.read(media_path)
                            pil = Image.open(io.BytesIO(blob)).convert("RGB")
                        except Exception:
                            continue
                        if not self._passes_filter(pil):
                            continue
                        # Caption = slide title; context = full slide text so
                        # ranking has real matchable tokens.
                        caption = title or (slide_text[:160]) or f"Slide {slide_idx+1}"
                        rec = self._store_image(pil, src, caption[:400], slide_text,
                                                 f"slide {slide_idx+1}",
                                                 f"slide{slide_idx}_{shape_idx}")
                        if rec:
                            out.append(rec)

                # Pass B: sweep anything under ppt/media/ that Pass A skipped
                # (masters/layouts/themes-only images, or orphans). These still
                # often contain the most relevant diagrams for templated decks.
                deck_name = (src.get("source") or "").rsplit(".", 1)[0]
                for n in names:
                    if not n.startswith("ppt/media/"):
                        continue
                    if n in seen_media:
                        continue
                    leaf = n.rsplit("/", 1)[-1]
                    if fn_junk.search(leaf):
                        continue
                    if not re.search(r"\.(png|jpe?g|webp|bmp|tiff?|gif)$", leaf, re.I):
                        continue
                    try:
                        blob = zf.read(n)
                        pil = Image.open(io.BytesIO(blob)).convert("RGB")
                    except Exception:
                        continue
                    if not self._passes_filter(pil):
                        continue
                    seen_media.add(n)
                    caption = f"{deck_name} — {leaf}"
                    rec = self._store_image(pil, src, caption[:400], deck_name,
                                             "deck media", f"media_{leaf}")
                    if rec:
                        out.append(rec)
        except Exception as e:
            print(f"[DocGen] PPTX image extraction error: {e}")
        return out

    def _pptx_extract_text(self, pptx_bytes: bytes) -> str:
        try:
            with zipfile.ZipFile(io.BytesIO(pptx_bytes)) as zf:
                parts = []
                for name in sorted(zf.namelist()):
                    if re.match(r"ppt/slides/slide\d+\.xml$", name):
                        try:
                            xml = zf.read(name).decode("utf-8", "ignore")
                            root = ET.fromstring(xml)
                            texts = [t.text for t in root.iter(f"{NS_A}t") if t.text]
                            parts.append(" ".join(texts))
                        except Exception: continue
                return "\n\n".join(parts)
        except Exception as e:
            print(f"[DocGen] PPTX text failed: {e}")
            return ""

    # ── XLSX ──
    def _extract_xlsx_images(self, xlsx_bytes: bytes, src: dict) -> list[dict]:
        out = []
        if not HAS_XLSX: return out
        try:
            wb = load_workbook(io.BytesIO(xlsx_bytes), data_only=True)
            for sheet in wb.sheetnames:
                ws = wb[sheet]
                sheet_text_parts = []
                for row in ws.iter_rows(max_row=30, max_col=15, values_only=True):
                    for v in row:
                        if v is not None and isinstance(v, str) and len(v.strip()) > 2:
                            sheet_text_parts.append(str(v).strip())
                sheet_text = " | ".join(sheet_text_parts)[:600]
                for img_idx, img in enumerate(ws._images):
                    try:
                        blob = img._data()
                        pil = Image.open(io.BytesIO(blob)).convert("RGB")
                    except Exception: continue
                    if not self._passes_filter(pil): continue
                    caption = f"{sheet}: {sheet_text_parts[0] if sheet_text_parts else ''}"[:200]
                    rec = self._store_image(pil, src, caption, sheet_text,
                                             f"sheet '{sheet}'", f"{sheet}_{img_idx}")
                    if rec: out.append(rec)
        except Exception as e:
            print(f"[DocGen] XLSX image extraction error: {e}")
        return out

    def _xlsx_extract_text(self, xlsx_bytes: bytes) -> str:
        try:
            wb = load_workbook(io.BytesIO(xlsx_bytes), data_only=True)
            parts = []
            for sheet in wb.sheetnames:
                ws = wb[sheet]
                row_parts = []
                for row in ws.iter_rows(max_row=200, max_col=30, values_only=True):
                    cells = [str(v) for v in row if v is not None]
                    if cells: row_parts.append(" | ".join(cells))
                parts.append(f"[Sheet: {sheet}]\n" + "\n".join(row_parts))
            return "\n\n".join(parts)
        except Exception as e:
            print(f"[DocGen] XLSX text failed: {e}")
            return ""

    # ── Standalone images ──
    def _ingest_standalone_image(self, img_bytes: bytes, src: dict) -> list[dict]:
        try: pil = Image.open(io.BytesIO(img_bytes)).convert("RGB")
        except Exception as e:
            print(f"[DocGen] Standalone open failed: {e}"); return []
        if not self._passes_filter(pil): return []
        caption = self._humanize(src["source"].rsplit(".", 1)[0])
        rec = self._store_image(pil, src, caption, "", "standalone image", "standalone")
        return [rec] if rec else []

    def _ingest_svg_image(self, svg_bytes: bytes, src: dict) -> list[dict]:
        svg_text = svg_bytes.decode("utf-8", errors="ignore")[:2000]
        pil = None
        if HAS_CAIROSVG:
            try:
                png_bytes = cairosvg.svg2png(bytestring=svg_bytes, output_width=1200)
                pil = Image.open(io.BytesIO(png_bytes)).convert("RGB")
            except Exception as e:
                print(f"[DocGen] SVG rasterize failed: {e}")
        if pil is None or not self._passes_filter(pil): return []
        caption = self._humanize(src["source"].rsplit(".", 1)[0])
        rec = self._store_image(pil, src, caption, svg_text, "SVG", "svg")
        return [rec] if rec else []

    def _ingest_remote_image(self, url: str, cand: dict) -> Optional[dict]:
        try:
            # Wikimedia requires a descriptive User-Agent (429 otherwise).
            # Use the compliant UA for wikimedia hosts, generic UA elsewhere.
            host = urlparse(url).netloc.lower()
            if "wikimedia.org" in host or "wikipedia.org" in host:
                ua = "IBM-DocGen/2.0 (https://ibm.com; IBM Consulting) python-requests"
            else:
                ua = "Mozilla/5.0 (ibm-docgen)"
            # Use the pooled session — reuses TCP/TLS connections across calls
            r = self._http.get(url, timeout=self.valves.web_image_fetch_timeout,
                                headers={"User-Agent": ua, "Accept": "image/*,*/*"})
            # On 429, trip the breaker for whichever image-source this host serves
            if r.status_code == 429:
                if "wikimedia.org" in host or "wikipedia.org" in host:
                    self._breaker_trip("wikipedia"); self._breaker_trip("wikimedia")
                print(f"[DocGen] 429 rate-limit from {host} on {url[:80]}")
                return None
            r.raise_for_status()
            if not r.content: return None
            try: pil = Image.open(io.BytesIO(r.content)).convert("RGB")
            except Exception: return None
            if not self._passes_filter(pil): return None

            src = {
                "source": cand.get("source_page") or urlparse(url).netloc,
                "ext": "." + url.rsplit(".", 1)[-1].split("?")[0].lower() if "." in url else ".jpg",
                "doc_type": "web",
            }
            return self._store_image(pil, src, cand.get("title", "")[:400],
                                     cand.get("snippet", "")[:800],
                                     f"web: {urlparse(url).netloc}", "web")
        except Exception as e:
            print(f"[DocGen] Remote image failed for {url}: {e}")
            return None

    # ══════════════════════════════════════════════════════════════════════
    # STORAGE & RANKING
    # ══════════════════════════════════════════════════════════════════════
    def _store_image(self, pil, src, caption, context, location, tag) -> Optional[dict]:
        pil = self._downscale(pil)
        buf = io.BytesIO()
        pil.save(buf, format="PNG", optimize=True)
        png_bytes = buf.getvalue()
        if len(png_bytes) > self.valves.max_image_bytes:
            pil.thumbnail((1024, 1024), Image.Resampling.LANCZOS)
            buf = io.BytesIO()
            pil.save(buf, format="PNG", optimize=True)
            png_bytes = buf.getvalue()
            if len(png_bytes) > self.valves.max_image_bytes:
                return None

        stem = (src.get("source", "unknown") or "unknown").rsplit(".", 1)[0]
        safe_stem = re.sub(r"[^a-zA-Z0-9_-]", "_", stem)[:40]
        img_id = f"{safe_stem}_{tag}_{uuid.uuid4().hex[:8]}"
        metadata = {
            "id": img_id, "caption": (caption or "")[:400],
            "context": (context or "")[:800],
            "source": src.get("source", ""), "location": location,
            "doc_type": src.get("doc_type", "general"),
            "source_format": src.get("ext", ""),
            "width": pil.width, "height": pil.height,
            "byte_size": len(png_bytes),
        }
        _IMAGE_STORE.put(img_id, png_bytes, metadata)
        return metadata

    def _downscale(self, pil):
        max_dim = 1600
        if pil.width > max_dim or pil.height > max_dim:
            pil = pil.copy()
            pil.thumbnail((max_dim, max_dim), Image.Resampling.LANCZOS)
        return pil

    def _passes_filter(self, pil):
        if pil.width < self.valves.min_image_width or pil.height < self.valves.min_image_height:
            return False
        aspect = pil.width / pil.height
        if aspect > self.valves.max_image_aspect_ratio or aspect < 1/self.valves.max_image_aspect_ratio:
            return False
        return True

    # ── Vision-model caption + re-rank (multimodal base model) ──
    def _png_thumbnail(self, png_bytes: bytes, max_px: int) -> bytes:
        """Downscale a PNG so the longest edge <= max_px. RGB output for smaller payload."""
        try:
            im = Image.open(io.BytesIO(png_bytes))
            im.thumbnail((max_px, max_px))
            out = io.BytesIO()
            im.convert("RGB").save(out, format="PNG", optimize=True)
            return out.getvalue()
        except Exception:
            return png_bytes

    def _vision_rank_sync(self, query: str, images: list, auth: dict) -> list:
        """Blocking helper: POST to OWUI chat completions with image inputs,
        parse a JSON array of {idx, caption, score} back, and merge into images.
        Returned list is re-ordered: vision-ranked picks first, then the rest.
        """
        if not self.valves.vision_rank_enabled or not images:
            return images
        max_n = max(1, int(self.valves.vision_rank_max_images))
        pick = images[:max_n]
        rest = images[max_n:]

        image_parts = []
        for i, img in enumerate(pick):
            png = img.get("png_bytes")
            if not png:
                png = _IMAGE_STORE.get_bytes(img.get("id", "")) if img.get("id") else None
            if not png:
                continue
            thumb = self._png_thumbnail(png, int(self.valves.vision_rank_thumb_px))
            b64 = base64.b64encode(thumb).decode("ascii")
            image_parts.append((i, b64))
        if not image_parts:
            return images

        instruction = (
            f'Query: "{query}"\n\n'
            f"You will see {len(image_parts)} numbered images. For EACH image, produce one entry:\n"
            '  {"idx": <int>, "caption": "<<=15 words>>", "score": <0-10 int>}\n'
            "score = visual relevance to the Query (10 = perfect match, 0 = irrelevant/decorative).\n"
            "Return ONLY a JSON array of entries (no prose, no code fences)."
        )
        content = [{"type": "text", "text": instruction}]
        for idx, b64 in image_parts:
            content.append({"type": "text", "text": f"Image {idx}:"})
            content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{b64}"},
            })

        payload = {
            "model": self.valves.vision_rank_model,
            "messages": [{"role": "user", "content": content}],
            "temperature": 0.0,
            "stream": False,
        }
        url = f"{self.valves.owui_base_url}/api/chat/completions"
        try:
            r = requests.post(
                url,
                headers={**(auth or {}), "Content-Type": "application/json"},
                json=payload,
                timeout=max(60, self.valves.request_timeout + 60),
            )
            r.raise_for_status()
            data = r.json()
            text = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
            if isinstance(text, list):  # some providers return list of parts
                text = "".join(p.get("text", "") for p in text if isinstance(p, dict))
            # Strip any code fences the model may add (triple-backtick blocks).
            _fence = chr(96) * 3  # avoid literal backticks in source
            text = re.sub(_fence + r"(?:json)?|" + _fence, "", text, flags=re.I).strip()
            # Best-effort: find the JSON array region.
            m = re.search(r"\[\s*\{.*\}\s*\]", text, flags=re.S)
            if m:
                text = m.group(0)
            entries = json.loads(text)
        except Exception as e:
            print(f"[DocGen] vision rank request failed: {e}")
            return images

        for entry in entries if isinstance(entries, list) else []:
            try:
                i = int(entry.get("idx", -1))
                if 0 <= i < len(pick):
                    cap = str(entry.get("caption", "")).strip()
                    score = float(entry.get("score", 0) or 0)
                    if cap:
                        pick[i]["caption"] = cap
                        pick[i]["vision_caption"] = cap
                    pick[i]["vision_score"] = max(0.0, min(10.0, score))
            except Exception:
                continue

        pick.sort(key=lambda x: x.get("vision_score", 0.0), reverse=True)
        return pick + rest

    async def _vision_rank_async(self, query: str, images: list, auth: dict) -> list:
        """Run the blocking vision-rank call on a worker thread so the event loop
        stays responsive (OWUI serves the chat-completions endpoint we're hitting).
        """
        if not self.valves.vision_rank_enabled or not images:
            return images
        try:
            return await asyncio.to_thread(self._vision_rank_sync, query, images, auth)
        except Exception as e:
            print(f"[DocGen] vision rank async failed: {e}")
            return images

    def _rank_images(self, query, images):
        q_tokens = set(re.findall(r"\w{3,}", query.lower()))
        scored = []
        for img in images:
            hay = " ".join([img.get("caption", ""), img.get("context", ""), img.get("source", "")]).lower()
            tokens = set(re.findall(r"\w{3,}", hay))
            overlap = len(q_tokens & tokens)
            dbst = {"case_study": 1.5, "solution_brief": 1.3, "methodology": 1.2,
                    "capability": 1.1, "web": 1.0, "general": 1.0}.get(img.get("doc_type", "general"), 1.0)
            fbst = {".pdf": 1.1, ".pptx": 1.2, ".docx": 1.0, ".xlsx": 0.9,
                    ".svg": 1.15, ".png": 1.0, ".jpg": 1.0, ".jpeg": 1.0, ".webp": 1.0
                    }.get(img.get("source_format", ""), 1.0)
            lex_score = (overlap + 0.5) * dbst * fbst
            # Prefer real embedded images over fallback page/slide snapshots:
            # snapshots are low-fidelity composites produced when no big embed
            # image exists. Real embedded images (charts, diagrams) look better
            # in the final DOCX/PPTX and should win ties.
            kind = (img.get("metadata", {}) or {}).get("kind") or img.get("kind")
            kind_penalty = 0.4 if kind in ("slide_snapshot", "page_render") else 1.0
            # Vision score (0-10) dominates when present; lexical breaks ties.
            vscore = img.get("vision_score")
            composite = ((float(vscore) * 100.0 if vscore is not None else 0.0) + lex_score) * kind_penalty
            scored.append((composite, img))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [img for _, img in scored]

    def _rank_text(self, query, chunks):
        q_tokens = set(re.findall(r"\w{3,}", query.lower()))
        scored = []
        for c in chunks:
            tokens = set(re.findall(r"\w{3,}", (c.get("content") or "").lower()))
            scored.append((len(q_tokens & tokens), c))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [c for _, c in scored]

    # ══════════════════════════════════════════════════════════════════════
    # WEB SEARCH
    # ══════════════════════════════════════════════════════════════════════
    def _google_search_text(self, query, num=6):
        out = []
        try:
            r = requests.get("https://www.googleapis.com/customsearch/v1", params={
                "key": self.valves.google_api_key, "cx": self.valves.google_cx,
                "q": query, "num": max(1, min(num, 10)),
            }, timeout=self.valves.request_timeout)
            r.raise_for_status()
            for item in r.json().get("items", []):
                out.append({
                    "content": f"{item.get('title', '')}. {item.get('snippet', '')}",
                    "source": item.get("displayLink", ""),
                    "url": item.get("link", ""), "page": 0, "doc_type": "web",
                })
        except Exception as e:
            print(f"[DocGen] Google text search failed: {e}")
        return out

    def _google_search_images(self, query, num=10):
        out = []
        try:
            r = requests.get("https://www.googleapis.com/customsearch/v1", params={
                "key": self.valves.google_api_key, "cx": self.valves.google_cx,
                "q": query, "searchType": "image", "num": max(1, min(num, 10)),
                "safe": "active", "imgSize": "large",
            }, timeout=self.valves.request_timeout)
            r.raise_for_status()
            for item in r.json().get("items", []):
                out.append({
                    "url": item.get("link", ""),
                    "title": item.get("title", ""),
                    "snippet": item.get("snippet", ""),
                    "source_page": item.get("image", {}).get("contextLink", ""),
                })
        except Exception as e:
            print(f"[DocGen] Google image search failed: {e}")
        return out

    # ── Keyless fallbacks: Wikipedia text + Wikimedia Commons images ──
    def _wikipedia_search_text(self, query, num=6):
        """Free, keyless text search via Wikipedia REST API."""
        out = []
        try:
            r = requests.get(
                "https://en.wikipedia.org/w/api.php",
                params={"action": "query", "format": "json", "list": "search",
                        "srsearch": query, "srlimit": max(1, min(num, 10)),
                        "srprop": "snippet"},
                headers={"User-Agent": "IBM-DocGen/2.0"},
                timeout=self.valves.request_timeout,
            )
            r.raise_for_status()
            for item in r.json().get("query", {}).get("search", []):
                title = item.get("title", "")
                # Strip HTML from snippet
                snippet = re.sub(r"<[^>]+>", "", item.get("snippet", ""))
                url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
                out.append({
                    "content": f"{title}. {snippet}",
                    "source": "en.wikipedia.org",
                    "url": url, "page": 0, "doc_type": "web",
                })
        except Exception as e:
            print(f"[DocGen] Wikipedia text search failed: {e}")
        return out

    # Wikimedia's User-Agent policy requires descriptive identification.
    # https://meta.wikimedia.org/wiki/User-Agent_policy
    _WIKI_UA = "IBM-DocGen/2.1 (https://ibm.com; IBM Consulting) python-requests"

    def _wikipedia_lead_images(self, query, num=5):
        """Query Wikipedia (en) for articles matching the query, return lead images
        (pageimage + thumbnail). Highest relevance for landmarks/places/people.
        Works whenever Wikipedia is reachable — independent of Commons & Google.
        """
        out = []
        try:
            # Step 1: search for matching articles
            r = requests.get(
                "https://en.wikipedia.org/w/api.php",
                params={
                    "action": "query", "format": "json",
                    "generator": "search",
                    "gsrsearch": query,
                    "gsrlimit": max(1, min(num, 10)),
                    "gsrnamespace": 0,  # article namespace
                    "prop": "pageimages|pageprops|info|extracts",
                    "piprop": "original|thumbnail",
                    "pithumbsize": 1200,
                    "pilimit": max(1, min(num, 10)),
                    "exintro": 1, "explaintext": 1, "exchars": 400,
                    "inprop": "url",
                    "redirects": 1,
                },
                headers={"User-Agent": self._WIKI_UA, "Accept": "application/json"},
                timeout=self.valves.request_timeout,
            )
            r.raise_for_status()
            pages = r.json().get("query", {}).get("pages", {})
            # Sort by search rank (Wikipedia returns in search-relevance order via 'index')
            ordered = sorted(
                pages.values(),
                key=lambda p: p.get("index", 999),
            )
            for page in ordered:
                # Prefer the full-resolution original, fall back to 1200px thumb
                img = page.get("original") or page.get("thumbnail") or {}
                url = img.get("source")
                if not url:
                    continue
                out.append({
                    "url": url,
                    "title": page.get("title", "")[:200],
                    "snippet": (page.get("extract") or "")[:400],
                    "source_page": page.get("fullurl", f"https://en.wikipedia.org/wiki/{page.get('title','').replace(' ','_')}"),
                })
        except Exception as e:
            print(f"[DocGen] Wikipedia lead-image search failed: {e}")
        return out

    def _wikimedia_search_images(self, query, num=10):
        """Free, keyless image search via Wikimedia Commons API (no key required)."""
        out = []
        try:
            r = requests.get(
                "https://commons.wikimedia.org/w/api.php",
                params={
                    "action": "query", "format": "json", "generator": "search",
                    "gsrsearch": f"filetype:bitmap {query}",
                    "gsrnamespace": 6,  # File namespace
                    "gsrlimit": max(1, min(num, 20)),
                    "prop": "imageinfo",
                    "iiprop": "url|size|mime|extmetadata",
                    "iiurlwidth": 1200,
                },
                headers={"User-Agent": self._WIKI_UA, "Accept": "application/json"},
                timeout=self.valves.request_timeout,
            )
            r.raise_for_status()
            pages = r.json().get("query", {}).get("pages", {})
            for _, page in pages.items():
                info = (page.get("imageinfo") or [{}])[0]
                mime = info.get("mime", "")
                if not mime.startswith("image/"):
                    continue
                # Skip SVG (rasterization is unreliable) and giant files
                if mime == "image/svg+xml":
                    continue
                if info.get("size", 0) > 8_000_000:
                    continue
                url = info.get("thumburl") or info.get("url", "")
                if not url:
                    continue
                meta = info.get("extmetadata", {})
                title = page.get("title", "").replace("File:", "")
                desc = (meta.get("ImageDescription", {}) or {}).get("value", "")
                desc = re.sub(r"<[^>]+>", "", desc)[:400]
                out.append({
                    "url": url,
                    "title": title[:200],
                    "snippet": desc,
                    "source_page": page.get("fullurl", "commons.wikimedia.org"),
                })
        except Exception as e:
            print(f"[DocGen] Wikimedia image search failed: {e}")
        return out

    def _web_search_text(self, query, num=6):
        """Text search with automatic fallback: Google (if keys) → Wikipedia."""
        if self.valves.google_api_key and self.valves.google_cx:
            results = self._google_search_text(query, num)
            if results:
                return results
        return self._wikipedia_search_text(query, num)

    def _web_search_images(self, query, num=10):
        """Image search with multi-source fallback. Reordered: reliable sources
        first (Wikipedia/Wikimedia), DDG last (returns many bot-blocked hosts).
        Candidates pre-filtered by host blocklist.
          1. Google Programmable Search (if valves set)
          2. Wikipedia article lead-images (upload.wikimedia.org — high relevance)
          3. Wikimedia Commons file search
          4. DuckDuckGo Images (LAST — has many 10s-timeout hosts)
        """
        def _ok(results):
            return self._prefilter_candidates(results) if results else results
        if self.valves.google_api_key and self.valves.google_cx:
            results = _ok(self._google_search_images(query, num))
            if results:
                return results
        try:
            wiki_results = _ok(self._wikipedia_lead_images(query, num=min(num, 5)))
            if wiki_results:
                return wiki_results
        except Exception as e:
            print(f"[DocGen] Wikipedia lead-image lookup failed: {e}")
        try:
            wmc_results = _ok(self._wikimedia_search_images(query, num))
            if wmc_results:
                return wmc_results
        except Exception as e:
            print(f"[DocGen] Wikimedia Commons search failed: {e}")
        try:
            ddg_results = _ok(self._duckduckgo_search_images(query, num))
            if ddg_results:
                return ddg_results
        except Exception as e:
            print(f"[DocGen] DuckDuckGo image search failed: {e}")
        return []

    def _duckduckgo_search_images(self, query, num=10):
        """Keyless image search via DuckDuckGo's image endpoint.

        DuckDuckGo Images returns results from across the web (including Google-
        and Bing-indexed pages) with no API key. Two-step flow:
          1. POST to duckduckgo.com to get a session token (vqd)
          2. GET duckduckgo.com/i.js with the token for JSON image results
        """
        out = []
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                "Accept": "text/html,application/json",
                "Referer": "https://duckduckgo.com/",
            }
            sess = requests.Session()
            sess.headers.update(headers)

            # Step 1: obtain the vqd token
            token_resp = sess.get(
                "https://duckduckgo.com/",
                params={"q": query, "iax": "images", "ia": "images"},
                timeout=self.valves.request_timeout,
            )
            token_resp.raise_for_status()
            m = re.search(r"vqd=[\"']?([\d-]+)[\"']?", token_resp.text)
            if not m:
                # Newer DDG uses JSON-encoded vqd
                m = re.search(r'"vqd":"([\d-]+)"', token_resp.text)
            if not m:
                print(f"[DocGen] DuckDuckGo: no vqd token in response")
                return out
            vqd = m.group(1)

            # Step 2: fetch image JSON
            img_resp = sess.get(
                "https://duckduckgo.com/i.js",
                params={
                    "l": "us-en",
                    "o": "json",
                    "q": query,
                    "vqd": vqd,
                    "f": ",,,,,,",  # filter defaults
                    "p": "1",
                },
                timeout=self.valves.request_timeout,
            )
            img_resp.raise_for_status()
            data = img_resp.json()
            results = data.get("results") or []
            for item in results[:max(1, min(num, 20))]:
                url = item.get("image") or item.get("thumbnail")
                if not url:
                    continue
                # Skip inline data URIs and huge files
                if url.startswith("data:"):
                    continue
                out.append({
                    "url": url,
                    "title": (item.get("title") or "")[:200],
                    "snippet": (item.get("source") or "")[:400],
                    "source_page": item.get("url") or "duckduckgo.com",
                })
        except Exception as e:
            print(f"[DocGen] DuckDuckGo image search failed: {e}")
        return out

    # ══════════════════════════════════════════════════════════════════════
    # RESPONSE PACKAGING
    # ══════════════════════════════════════════════════════════════════════
    def _package(self, query, text_chunks, images, source):
        return json.dumps({
            "source_mode": source,
            "query": query,
            "text_chunks": [
                {
                    "id": f"T{i+1}",
                    "content": c.get("content", ""),
                    "source": c.get("source", ""),
                    "page": c.get("page", 0),
                    "doc_type": c.get("doc_type", "general"),
                    "url": c.get("url", ""),
                }
                for i, c in enumerate(text_chunks)
            ],
            "images": [
                {
                    "id": img["id"],
                    "display_id": img.get("display_id", f"IMG{i+1}"),
                    "caption": img.get("caption", ""),
                    "context": img.get("context", "")[:300],
                    "source": img.get("source", ""),
                    "location": img.get("location", ""),
                    "width": img.get("width"),
                    "height": img.get("height"),
                }
                for i, img in enumerate(images)
            ],
            "next_step": (
                "Now build a sections array (see assemble_document schema). "
                "For each section that deserves an image, set image_id to the "
                "display_id value of the chosen image (e.g. 'IMG1', 'IMG2') — "
                "use the display_id field, not the id field. "
                "Then call assemble_document(session_id, format, title, client_name, sections_json)."
            ),
        }, indent=2)

    # ══════════════════════════════════════════════════════════════════════
    # DOCX BUILDER — pure OOXML, embeds images inline
    # ══════════════════════════════════════════════════════════════════════
    def _build_and_render_docx(self, session_id, title, client_name, sections, emitter):
        doc_parts = []          # w:p / w:tbl XML
        media_files = []        # (filename, bytes) embedded in zip
        rel_entries = []        # relationship <Relationship> rows

        def esc(s):
            return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

        def run_xml(text, size=22, bold=False, italic=False, color="161616"):
            return (
                f'<w:r><w:rPr>'
                f'<w:rFonts w:ascii="Calibri" w:hAnsi="Calibri"/>'
                f'<w:sz w:val="{size}"/>'
                f'<w:color w:val="{color}"/>'
                f'{"<w:b/>" if bold else ""}'
                f'{"<w:i/>" if italic else ""}'
                f'</w:rPr><w:t xml:space="preserve">{esc(text)}</w:t></w:r>'
            )

        def para_xml(runs, align="left", after=120, before=0):
            return (
                f'<w:p><w:pPr>'
                f'<w:jc w:val="{align}"/>'
                f'<w:spacing w:after="{after}" w:before="{before}" w:line="300" w:lineRule="auto"/>'
                f'</w:pPr>{runs}</w:p>'
            )

        def heading_xml(text, level=1, color="0F62FE"):
            sizes = {1: 40, 2: 32, 3: 26, 4: 22}
            sz = sizes.get(level, 22)
            return (
                f'<w:p><w:pPr><w:spacing w:before="360" w:after="160"/></w:pPr>'
                f'{run_xml(text, size=sz, bold=True, color=color)}</w:p>'
            )

        def table_xml(headers, rows, hdr_bg="0F62FE"):
            out = '<w:tbl><w:tblPr><w:tblW w:w="5000" w:type="pct"/><w:tblBorders>'
            for side in ("top", "left", "bottom", "right", "insideH", "insideV"):
                out += f'<w:{side} w:val="single" w:sz="4" w:color="E0E0E0"/>'
            out += '</w:tblBorders></w:tblPr>'
            if headers:
                out += '<w:tr>'
                for h in headers:
                    out += (
                        f'<w:tc><w:tcPr><w:shd w:val="clear" w:color="auto" w:fill="{hdr_bg}"/></w:tcPr>'
                        f'<w:p><w:pPr><w:jc w:val="left"/></w:pPr>'
                        f'{run_xml(h, size=20, bold=True, color="FFFFFF")}</w:p></w:tc>'
                    )
                out += '</w:tr>'
            for ri, row in enumerate(rows):
                bg = "F4F4F4" if ri % 2 == 1 else "FFFFFF"
                out += '<w:tr>'
                for cell in row:
                    out += (
                        f'<w:tc><w:tcPr><w:shd w:val="clear" w:color="auto" w:fill="{bg}"/></w:tcPr>'
                        f'<w:p>{run_xml(str(cell), size=20)}</w:p></w:tc>'
                    )
                out += '</w:tr>'
            out += '</w:tbl>'
            return out

        def add_image_xml(png_bytes, width_px, height_px, caption=None):
            # Register media file + relationship
            idx = len(media_files)
            fname = f"image{idx+1}.png"
            media_files.append((fname, png_bytes))
            rid = f"rIdImg{idx}"
            rel_entries.append(
                f'<Relationship Id="{rid}" '
                f'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" '
                f'Target="media/{fname}"/>'
            )

            # EMU sizing (914400 EMU = 1 inch, ~9525 EMU per pixel at 96 DPI)
            display_px = 500
            aspect = height_px / width_px if width_px else 0.65
            w_emu = int(display_px * 9525)
            h_emu = int(w_emu * aspect)

            doc_parts.append(
                '<w:p><w:pPr><w:jc w:val="center"/><w:spacing w:before="240" w:after="60"/></w:pPr>'
                '<w:r><w:drawing>'
                '<wp:inline xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing" '
                'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
                'xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture" '
                'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
                f'<wp:extent cx="{w_emu}" cy="{h_emu}"/>'
                f'<wp:docPr id="{idx+100}" name="Image {idx+1}"/>'
                '<wp:cNvGraphicFramePr/>'
                '<a:graphic><a:graphicData uri="http://schemas.openxmlformats.org/drawingml/2006/picture">'
                '<pic:pic>'
                f'<pic:nvPicPr><pic:cNvPr id="{idx+100}" name="{fname}"/><pic:cNvPicPr/></pic:nvPicPr>'
                f'<pic:blipFill><a:blip r:embed="{rid}"/><a:stretch><a:fillRect/></a:stretch></pic:blipFill>'
                '<pic:spPr>'
                f'<a:xfrm><a:off x="0" y="0"/><a:ext cx="{w_emu}" cy="{h_emu}"/></a:xfrm>'
                '<a:prstGeom prst="rect"><a:avLst/></a:prstGeom>'
                '</pic:spPr></pic:pic>'
                '</a:graphicData></a:graphic>'
                '</wp:inline></w:drawing></w:r></w:p>'
            )
            if caption:
                cap_run = run_xml(f"Figure — {caption}", size=18, italic=True, color="525252")
                doc_parts.append(para_xml(cap_run, align="center", after=240))

        # ── Cover page ──
        doc_parts.append(para_xml(run_xml(title, size=56, bold=True, color="0F62FE"),
                                   align="left", after=240, before=1200))
        doc_parts.append(para_xml(
            run_xml(f"IBM Consulting  |  Prepared for {client_name}", size=24, color="525252"),
            align="left", after=480
        ))
        doc_parts.append(para_xml(
            run_xml(time.strftime("%B %Y"), size=20, color="525252"),
            align="left", after=120
        ))
        # Page break
        doc_parts.append('<w:p><w:r><w:br w:type="page"/></w:r></w:p>')

        # ── Sections ──
        for idx, section in enumerate(sections, start=1):
            sec_title = section.get("title", f"Section {idx}")
            doc_parts.append(heading_xml(sec_title, level=1))

            for para in section.get("paragraphs", []) or []:
                doc_parts.append(para_xml(run_xml(para, size=22), align="left"))

            bullets = section.get("bullets", []) or []
            for b in bullets:
                doc_parts.append(
                    f'<w:p><w:pPr><w:numPr><w:ilvl w:val="0"/><w:numId w:val="1"/></w:numPr>'
                    f'<w:ind w:left="360"/></w:pPr>{run_xml("• " + str(b), size=22)}</w:p>'
                )

            if section.get("table"):
                t = section["table"]
                doc_parts.append(table_xml(t.get("headers", []), t.get("rows", [])))
                doc_parts.append(para_xml("", after=120))  # spacer

            if section.get("_img_bytes"):
                add_image_xml(
                    section["_img_bytes"],
                    section.get("_img_width", 1200),
                    section.get("_img_height", 800),
                    section.get("image_caption") or section.get("title", ""),
                )

        body_xml = "".join(doc_parts)

        # Full document.xml
        # ── IBM logo footer (every page, default) ──
        logo_png = self._get_ibm_logo_png()
        footer_ref_xml = ""
        footer_xml = None
        footer_rels_xml = None
        if logo_png:
            lw, lh = self._get_ibm_logo_dims()
            # 1/5 of the previous 0.5-inch tall → ~0.1 inch tall, width by aspect.
            # Requested: smaller, left-aligned IBM mark on every page.
            footer_h_emu = 91440  # 0.1 inch
            footer_w_emu = int(footer_h_emu * (lw / max(lh, 1)))
            rel_entries.append(
                '<Relationship Id="rIdFooter" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/footer" '
                'Target="footer1.xml"/>'
            )
            # The footer has its own media rel file.
            footer_rels_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rIdFooterLogo" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" '
                'Target="media/ibm_logo_black.png"/>'
                '</Relationships>'
            )
            # Footer content: right-aligned logo.
            footer_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<w:ftr xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" '
                'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" '
                'xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing" '
                'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
                'xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture">'
                '<w:p><w:pPr><w:jc w:val="left"/></w:pPr>'
                '<w:r><w:drawing>'
                f'<wp:inline distT="0" distB="0" distL="0" distR="0">'
                f'<wp:extent cx="{footer_w_emu}" cy="{footer_h_emu}"/>'
                '<wp:effectExtent l="0" t="0" r="0" b="0"/>'
                '<wp:docPr id="9999" name="IBM Logo"/>'
                '<wp:cNvGraphicFramePr><a:graphicFrameLocks xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" noChangeAspect="1"/></wp:cNvGraphicFramePr>'
                '<a:graphic xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">'
                '<a:graphicData uri="http://schemas.openxmlformats.org/drawingml/2006/picture">'
                '<pic:pic xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture">'
                '<pic:nvPicPr><pic:cNvPr id="9999" name="IBM Logo"/><pic:cNvPicPr/></pic:nvPicPr>'
                '<pic:blipFill><a:blip r:embed="rIdFooterLogo"/><a:stretch><a:fillRect/></a:stretch></pic:blipFill>'
                '<pic:spPr><a:xfrm><a:off x="0" y="0"/>'
                f'<a:ext cx="{footer_w_emu}" cy="{footer_h_emu}"/></a:xfrm>'
                '<a:prstGeom prst="rect"><a:avLst/></a:prstGeom></pic:spPr>'
                '</pic:pic></a:graphicData></a:graphic></wp:inline></w:drawing></w:r></w:p>'
                '</w:ftr>'
            )
            footer_ref_xml = '<w:footerReference w:type="default" r:id="rIdFooter"/>'
            # Also ensure the logo image itself is written in word/media/.
            media_files.append(("ibm_logo_black.png", logo_png))

        doc_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" '
            'xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing" '
            'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
            'xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture">'
            f'<w:body>{body_xml}'
            '<w:sectPr>'
            f'{footer_ref_xml}'
            '<w:pgSz w:w="12240" w:h="15840"/>'
            '<w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440" w:header="720" w:footer="720" w:gutter="0"/>'
            '</w:sectPr></w:body></w:document>'
        )

        # Content types
        footer_override = (
            '<Override PartName="/word/footer1.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.footer+xml"/>'
            if footer_xml else ''
        )
        ct_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Default Extension="png" ContentType="image/png"/>'
            '<Default Extension="jpeg" ContentType="image/jpeg"/>'
            '<Default Extension="jpg" ContentType="image/jpeg"/>'
            '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
            f'{footer_override}'
            '</Types>'
        )
        # Package relationships
        rels_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>'
            '</Relationships>'
        )
        # Document relationships (for images)
        doc_rels = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            + "".join(rel_entries)
            + '</Relationships>'
        )

        # Build the zip
        docx_buf = io.BytesIO()
        with zipfile.ZipFile(docx_buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("[Content_Types].xml", ct_xml)
            zf.writestr("_rels/.rels", rels_xml)
            zf.writestr("word/_rels/document.xml.rels", doc_rels)
            if footer_xml:
                zf.writestr("word/footer1.xml", footer_xml)
                zf.writestr("word/_rels/footer1.xml.rels", footer_rels_xml)
            zf.writestr("word/document.xml", doc_xml)
            for fname, fbytes in media_files:
                zf.writestr(f"word/media/{fname}", fbytes)

        docx_bytes = docx_buf.getvalue()
        docx_b64 = base64.b64encode(docx_bytes).decode()
        data_uri = f"data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,{docx_b64}"

        # Build inline HTML preview
        return self._render_docx_preview(title, client_name, sections, data_uri)

    def _render_docx_preview(self, title, client_name, sections, data_uri):
        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", title)[:50] or "document"

        # Build page HTML
        page_parts = []
        # Cover
        page_parts.append(
            f'<div class="pg" style="display:block;padding:80px 60px;background:#fff;min-height:9in">'
            f'<div style="font-size:36px;font-weight:700;color:{IBM_BLUE_60};margin-bottom:24px">{self._html_esc(title)}</div>'
            f'<div style="font-size:18px;color:{IBM_GRAY_70};margin-bottom:8px">IBM Consulting  |  Prepared for {self._html_esc(client_name)}</div>'
            f'<div style="font-size:14px;color:{IBM_GRAY_70}">{time.strftime("%B %Y")}</div>'
            f'</div>'
        )

        # Sections
        for idx, section in enumerate(sections, start=1):
            parts = []
            parts.append(
                f'<h1 style="font-size:28px;color:{IBM_BLUE_60};font-weight:700;margin:0 0 16px;'
                f'font-family:Calibri,sans-serif">{self._html_esc(section.get("title", ""))}</h1>'
            )
            for para in section.get("paragraphs", []) or []:
                parts.append(
                    f'<p style="font-size:12px;color:{IBM_GRAY_100};line-height:1.5;margin:8px 0;'
                    f'font-family:Calibri,sans-serif">{self._html_esc(para)}</p>'
                )
            bullets = section.get("bullets", []) or []
            if bullets:
                lis = "".join(
                    f'<li style="font-size:12px;color:{IBM_GRAY_100};margin:4px 0;font-family:Calibri,sans-serif">{self._html_esc(b)}</li>'
                    for b in bullets
                )
                parts.append(f'<ul style="padding-left:24px;margin:8px 0">{lis}</ul>')
            if section.get("table"):
                t = section["table"]
                tbl = (
                    f'<table style="width:100%;border-collapse:collapse;margin:12px 0;font-size:11px;font-family:Calibri,sans-serif">'
                )
                if t.get("headers"):
                    tbl += f'<tr style="background:{IBM_BLUE_60}">'
                    tbl += "".join(
                        f'<th style="padding:8px 10px;color:#fff;text-align:left;border:1px solid #ddd">{self._html_esc(h)}</th>'
                        for h in t["headers"]
                    )
                    tbl += '</tr>'
                for ri, row in enumerate(t.get("rows", [])):
                    bg = IBM_GRAY_10 if ri % 2 == 1 else "#FFFFFF"
                    tbl += f'<tr style="background:{bg}">'
                    tbl += "".join(
                        f'<td style="padding:6px 10px;color:#333;border:1px solid #ddd">{self._html_esc(str(c))}</td>'
                        for c in row
                    )
                    tbl += '</tr>'
                tbl += '</table>'
                parts.append(tbl)
            if section.get("_img_bytes"):
                img_b64 = base64.b64encode(section["_img_bytes"]).decode()
                parts.append(
                    f'<div style="text-align:center;margin:16px 0">'
                    f'<img src="data:image/png;base64,{img_b64}" '
                    f'style="max-width:100%;width:480px;height:auto;border-radius:4px"/>'
                    + (
                        f'<div style="font-size:10px;color:{IBM_GRAY_70};font-style:italic;margin-top:6px">'
                        f'Figure — {self._html_esc(section.get("image_caption") or "")}</div>'
                        if section.get("image_caption") else ""
                    )
                    + '</div>'
                )
            page_parts.append(
                f'<div class="pg" style="display:none;padding:60px;background:#fff;min-height:9in;'
                f'page-break-after:always">{"".join(parts)}</div>'
            )

        total = len(page_parts)
        html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
*{{box-sizing:border-box;margin:0}}
html,body{{height:720px;min-height:720px}}
body{{font-family:Calibri,system-ui,sans-serif;background:#f0f2f5;padding:12px;display:flex;align-items:stretch;justify-content:center}}
.dk{{border:2px solid {IBM_BLUE_60};border-radius:10px;overflow:hidden;width:100%;max-width:1280px;height:696px;margin:0 auto;background:#fff;display:flex;flex-direction:column}}
.tb{{display:flex;align-items:center;gap:8px;padding:10px 14px;background:{IBM_BLUE_70};flex-wrap:wrap;flex-shrink:0}}
.b{{border:none;border-radius:4px;padding:6px 14px;font-size:12px;cursor:pointer;
font-family:Calibri,sans-serif;font-weight:600;text-decoration:none;display:inline-block}}
.bw{{background:#fff;color:{IBM_BLUE_70}}} .bg{{background:rgba(255,255,255,0.2);color:#fff}}
.sn{{color:#fff;font-size:12px;min-width:90px;text-align:center}}
.sp{{flex:1}}
.sw{{background:{IBM_GRAY_10};padding:20px;overflow:auto;flex:1;min-height:0}}
.pg{{max-width:8.5in;margin:0 auto 16px;box-shadow:0 2px 8px rgba(0,0,0,0.1);min-height:9in}}
</style></head><body>
<div class="dk">
  <div class="tb">
    <button class="b bw" onclick="nav(-1)">← Prev</button>
    <span class="sn" id="sn">Page 1 / {total}</span>
    <button class="b bw" onclick="nav(1)">Next →</button>
    <span class="sp"></span>
    <a class="b bw" href="{data_uri}" download="{safe_name}.docx">⬇ Download DOCX</a>
  </div>
  <div class="sw">{"".join(page_parts)}</div>
</div>
<script>
(function(){{
  function fit(){{
    try{{
      var fe=window.frameElement;
      if(fe){{
        fe.style.height='720px';
        fe.style.minHeight='720px';
        fe.style.width='100%';
        fe.setAttribute('height','720');
      }}
    }}catch(e){{}}
    try{{window.parent.postMessage({{type:'ibm-docgen-resize',height:720}},'*');}}catch(e){{}}
  }}
  fit();setTimeout(fit,100);setTimeout(fit,500);setTimeout(fit,1500);
  window.addEventListener('load',fit);
  new MutationObserver(fit).observe(document.documentElement,{{attributes:true,childList:true,subtree:false}});
}})();
var cur=0,sl=document.querySelectorAll(".pg"),tot=sl.length;
function nav(d){{sl[cur].style.display="none";cur=Math.max(0,Math.min(tot-1,cur+d));
sl[cur].style.display="block";document.getElementById("sn").textContent="Page "+(cur+1)+" / "+tot;
var sw=document.querySelector(".sw");if(sw)sw.scrollTop=0}}
document.addEventListener("keydown",function(e){{
if(e.key==="ArrowLeft")nav(-1);if(e.key==="ArrowRight")nav(1)}});
</script></body></html>"""

        return HTMLResponse(content=html, headers={"Content-Disposition": "inline"})

    # ══════════════════════════════════════════════════════════════════════
    # XLSX BUILDER — IBM-branded multi-sheet workbook via openpyxl
    # ══════════════════════════════════════════════════════════════════════
    def _build_and_render_xlsx(self, session_id, title, client_name, sections, workbook_spec, emitter):
        if not HAS_XLSX:
            return ("❌ openpyxl is not installed in the Open WebUI Python environment. "
                    "Install it with:\n"
                    "    /Users/pradeepbasavarajappa/.local/share/uv/tools/open-webui/bin/python -m pip install openpyxl\n"
                    "Then restart Open WebUI.")

        # Derive sheet specs. Accept EITHER schema:
        #   A) {title, headers:[...], rows, notes}
        #   B) {sheet_name, columns:[{header,width}], rows, styles:{header_bg,header_fg,alt_row_bg}}
        sheets = []
        if workbook_spec and isinstance(workbook_spec, dict) and workbook_spec.get("sheets"):
            for sh in workbook_spec["sheets"]:
                if not isinstance(sh, dict):
                    continue
                # Determine columns (with widths)
                cols = []
                if sh.get("columns") and isinstance(sh["columns"], list):
                    for col in sh["columns"]:
                        if isinstance(col, dict):
                            cols.append({"header": str(col.get("header", "")), "width": col.get("width")})
                        else:
                            cols.append({"header": str(col), "width": None})
                elif sh.get("headers"):
                    for h in sh["headers"]:
                        cols.append({"header": str(h), "width": None})
                sheets.append({
                    "title": str(sh.get("title") or sh.get("sheet_name") or "Sheet"),
                    "columns": cols,
                    "headers": [c["header"] for c in cols],
                    "rows": [list(r) for r in (sh.get("rows") or [])],
                    "notes": str(sh.get("notes") or ""),
                    "styles": sh.get("styles") or {},
                })
        else:
            # Auto-derive from sections. Summary sheet + one sheet per section with a table,
            # else a single sheet listing section titles + bullet summary.
            summary_rows = []
            for idx, s in enumerate(sections, start=1):
                paras = s.get("paragraphs") or []
                bullets = s.get("bullets") or []
                bullet_preview = " • ".join(bullets[:3]) if bullets else ""
                para_preview = (paras[0] if paras else "")[:300]
                summary_rows.append([idx, s.get("title", ""), para_preview, bullet_preview])
            summary_cols = [
                {"header": "#", "width": 6},
                {"header": "Section", "width": 32},
                {"header": "Overview", "width": 60},
                {"header": "Key Points", "width": 50},
            ]
            sheets.append({
                "title": "Summary",
                "columns": summary_cols,
                "headers": [c["header"] for c in summary_cols],
                "rows": summary_rows,
                "notes": f"Prepared for {client_name} — {time.strftime('%B %Y')}",
                "styles": {},
            })
            for idx, s in enumerate(sections, start=1):
                sec_title = s.get("title", f"Section {idx}")
                # If the section has an explicit table, use it
                tbl = s.get("table") or None
                if tbl and tbl.get("headers") and tbl.get("rows"):
                    cols = [{"header": str(h), "width": None} for h in tbl["headers"]]
                    sheets.append({
                        "title": self._sanitize_sheet_name(sec_title, idx),
                        "columns": cols,
                        "headers": list(tbl["headers"]),
                        "rows": [list(r) for r in tbl["rows"]],
                        "notes": "",
                        "styles": {},
                    })
                else:
                    # Flatten paragraphs + bullets into a 2-col sheet
                    rows = []
                    for p in s.get("paragraphs") or []:
                        rows.append(["Paragraph", p])
                    for b in s.get("bullets") or []:
                        rows.append(["Bullet", b])
                    if not rows:
                        rows.append(["", ""])
                    sheets.append({
                        "title": self._sanitize_sheet_name(sec_title, idx),
                        "columns": [{"header": "Type", "width": 14}, {"header": "Content", "width": 80}],
                        "headers": ["Type", "Content"],
                        "rows": rows,
                        "notes": sec_title,
                        "styles": {},
                    })

        # Build workbook
        wb = Workbook()
        # Remove default sheet
        default = wb.active
        wb.remove(default)

        # RAG + gantt block support (ported from Inline_Visualizer_v5)
        GANTT_BLOCKS = frozenset("\u2588\u2593\u2592\u2591\u25a0\u25aa\u25fc\u2b1b")
        RAG_MAP = {"RED": "DA1E28", "AMBER": "F1C21B", "GREEN": "24A148",
                   "RAG:RED": "DA1E28", "RAG:AMBER": "F1C21B", "RAG:GREEN": "24A148"}

        note_font = Font(name="Calibri", size=10, italic=True, color="525252")
        thin = Side(border_style="thin", color="CCCCCC")
        border = Border(left=thin, right=thin, top=thin, bottom=thin)
        wrap = Alignment(wrap_text=True, vertical="top")

        used_names = set()
        for sh in sheets:
            name = self._unique_sheet_name(sh["title"], used_names)
            used_names.add(name)
            ws = wb.create_sheet(title=name)

            cols = sh.get("columns") or [{"header": h, "width": None} for h in sh.get("headers", [])]
            headers = [c["header"] for c in cols]
            col_count = max(1, len(headers))
            styles = sh.get("styles") or {}
            hdr_bg = (styles.get("header_bg") or "#0F62FE").lstrip("#")
            hdr_fg = (styles.get("header_fg") or "#FFFFFF").lstrip("#")
            alt_bg = (styles.get("alt_row_bg") or "#F4F4F4").lstrip("#")

            hdr_fill = PatternFill(start_color=hdr_bg, end_color=hdr_bg, fill_type="solid")
            hdr_font = Font(name="Calibri", size=11, bold=True, color=hdr_fg)
            body_font = Font(name="Calibri", size=11, color="161616")
            alt_fill = PatternFill(start_color=alt_bg, end_color=alt_bg, fill_type="solid")

            row_cursor = 1
            # Title row (merged)
            ws.cell(row=row_cursor, column=1, value=str(sh["title"]))
            ws.cell(row=row_cursor, column=1).font = Font(name="Calibri", size=16, bold=True, color="0F62FE")
            if col_count > 1:
                ws.merge_cells(start_row=row_cursor, start_column=1, end_row=row_cursor, end_column=col_count)
            ws.row_dimensions[row_cursor].height = 26
            row_cursor += 1

            # Subtitle
            ws.cell(row=row_cursor, column=1, value=f"IBM Consulting  |  Prepared for {client_name}")
            ws.cell(row=row_cursor, column=1).font = note_font
            if col_count > 1:
                ws.merge_cells(start_row=row_cursor, start_column=1, end_row=row_cursor, end_column=col_count)
            row_cursor += 2

            # Headers
            hdr_row = row_cursor
            for ci, h in enumerate(headers, start=1):
                c = ws.cell(row=hdr_row, column=ci, value=str(h))
                c.fill = hdr_fill
                c.font = hdr_font
                c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                c.border = border
            ws.row_dimensions[hdr_row].height = 28
            row_cursor += 1

            first_data_row = row_cursor

            # Body rows with RAG + gantt block detection
            for ri, row in enumerate(sh["rows"]):
                r = row_cursor + ri
                is_alt = ri % 2 == 1
                for ci in range(col_count):
                    val = row[ci] if ci < len(row) else ""
                    cell_str = str(val) if val is not None else ""
                    stripped_upper = cell_str.strip().upper()

                    is_gantt = bool(cell_str.strip()) and all(ch in GANTT_BLOCKS for ch in cell_str.strip())
                    rag_key = stripped_upper if stripped_upper in RAG_MAP else None

                    c = ws.cell(row=r, column=ci + 1)
                    c.border = border
                    c.font = body_font
                    c.alignment = wrap

                    if is_gantt:
                        c.value = ""
                        c.fill = PatternFill(start_color="24A148", end_color="24A148", fill_type="solid")
                    elif rag_key:
                        c.value = cell_str.strip()
                        c.fill = PatternFill(start_color=RAG_MAP[rag_key], end_color=RAG_MAP[rag_key], fill_type="solid")
                        c.font = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
                        c.alignment = Alignment(horizontal="center", vertical="center")
                    else:
                        # Auto-coerce pure numeric strings
                        coerced = val
                        if isinstance(val, str):
                            s2 = val.strip()
                            if s2 and re.fullmatch(r"-?\d+", s2):
                                try: coerced = int(s2)
                                except Exception: pass
                            elif s2 and re.fullmatch(r"-?\d+\.\d+", s2):
                                try: coerced = float(s2)
                                except Exception: pass
                        c.value = coerced
                        if is_alt:
                            c.fill = alt_fill

            last_data_row = row_cursor + max(0, len(sh["rows"]) - 1)

            # Notes line under the table
            if sh.get("notes"):
                notes_row = last_data_row + 2
                ws.cell(row=notes_row, column=1, value=str(sh["notes"]))
                ws.cell(row=notes_row, column=1).font = note_font
                if col_count > 1:
                    ws.merge_cells(start_row=notes_row, start_column=1, end_row=notes_row, end_column=col_count)

            # Column widths — use explicit spec if given, else auto-size
            for ci in range(1, col_count + 1):
                letter = get_column_letter(ci)
                spec_w = cols[ci - 1].get("width") if ci - 1 < len(cols) else None
                if spec_w:
                    ws.column_dimensions[letter].width = float(spec_w)
                else:
                    max_len = 10
                    if ci - 1 < len(headers):
                        max_len = max(max_len, len(str(headers[ci - 1])))
                    for row in sh["rows"]:
                        if ci - 1 < len(row):
                            cell_str = str(row[ci - 1]) if row[ci - 1] is not None else ""
                            max_len = max(max_len, min(60, len(cell_str)))
                    ws.column_dimensions[letter].width = min(60, max(12, int(max_len * 1.1)))

            # Freeze header + auto-filter
            ws.freeze_panes = ws.cell(row=first_data_row, column=1).coordinate
            if col_count > 0 and len(sh["rows"]) > 0:
                last_col_letter = get_column_letter(col_count)
                ws.auto_filter.ref = f"A{hdr_row}:{last_col_letter}{last_data_row}"

        # Ensure at least one sheet
        if not wb.sheetnames:
            ws = wb.create_sheet(title="Sheet1")
            ws["A1"] = title
            ws["A2"] = f"Prepared for {client_name}"

        # Serialize
        buf = io.BytesIO()
        wb.save(buf)
        xlsx_bytes = buf.getvalue()

        b64 = base64.b64encode(xlsx_bytes).decode()
        data_uri = (
            "data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64," + b64
        )

        return self._render_xlsx_preview(title, client_name, sheets, data_uri)

    def _sanitize_sheet_name(self, name: str, idx: int = 0) -> str:
        # Excel sheet names: ≤31 chars, no : \ / ? * [ ]
        cleaned = re.sub(r"[:\\/\?\*\[\]]", " ", str(name or "")).strip()
        if not cleaned:
            cleaned = f"Sheet{idx}"
        return cleaned[:31]

    def _unique_sheet_name(self, name: str, used: set) -> str:
        base = self._sanitize_sheet_name(name, 1)
        if base not in used:
            return base
        i = 2
        while True:
            cand = f"{base[:28]} ({i})"
            if cand not in used:
                return cand
            i += 1

    def _render_xlsx_preview(self, title, client_name, sheets, data_uri):
        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", title)[:50] or "workbook"

        tab_buttons = []
        panels = []
        for i, sh in enumerate(sheets):
            active = " active" if i == 0 else ""
            display = "block" if i == 0 else "none"
            tab_buttons.append(
                f'<button class="tab{active}" onclick="showTab({i})" data-i="{i}">'
                f'{self._html_esc(sh["title"])}</button>'
            )

            # Build HTML table
            headers = sh.get("headers") or []
            rows = sh.get("rows") or []
            notes = sh.get("notes") or ""

            thead = ""
            if headers:
                thead = "<tr>" + "".join(
                    f'<th style="background:{IBM_BLUE_60};color:#fff;padding:8px 12px;'
                    f'text-align:left;border:1px solid #ddd;font-weight:600;font-size:12px">'
                    f'{self._html_esc(h)}</th>' for h in headers
                ) + "</tr>"
            tbody_rows = []
            for ri, row in enumerate(rows):
                bg = IBM_GRAY_10 if ri % 2 == 1 else "#fff"
                tds = "".join(
                    f'<td style="padding:6px 12px;border:1px solid #e0e0e0;'
                    f'font-size:12px;color:{IBM_GRAY_100};vertical-align:top">'
                    f'{self._html_esc(str(c) if c is not None else "")}</td>'
                    for c in row
                )
                tbody_rows.append(f'<tr style="background:{bg}">{tds}</tr>')
            table_html = (
                f'<table style="border-collapse:collapse;width:100%;font-family:Calibri,sans-serif">'
                f'{thead}{"".join(tbody_rows)}</table>'
            )
            note_html = (
                f'<div style="font-size:11px;color:{IBM_GRAY_70};font-style:italic;margin-top:12px">'
                f'{self._html_esc(notes)}</div>' if notes else ""
            )
            panels.append(
                f'<div class="panel" data-i="{i}" style="display:{display}">'
                f'<div style="font-size:20px;font-weight:700;color:{IBM_BLUE_60};margin-bottom:4px">'
                f'{self._html_esc(sh["title"])}</div>'
                f'<div style="font-size:12px;color:{IBM_GRAY_70};margin-bottom:16px">'
                f'IBM Consulting  |  Prepared for {self._html_esc(client_name)}</div>'
                f'{table_html}{note_html}</div>'
            )

        total = len(sheets)
        html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
*{{box-sizing:border-box;margin:0}}
html,body{{height:720px;min-height:720px}}
body{{font-family:Calibri,system-ui,sans-serif;background:#f0f2f5;padding:12px;display:flex;align-items:stretch;justify-content:center}}
.wk{{border:2px solid {IBM_BLUE_60};border-radius:10px;overflow:hidden;width:100%;max-width:1280px;height:696px;margin:0 auto;background:#fff;display:flex;flex-direction:column}}
.tb{{display:flex;align-items:center;gap:8px;padding:10px 14px;background:{IBM_BLUE_70};flex-wrap:wrap;flex-shrink:0}}
.b{{border:none;border-radius:4px;padding:6px 14px;font-size:12px;cursor:pointer;
font-family:Calibri,sans-serif;font-weight:600;text-decoration:none;display:inline-block;background:#fff;color:{IBM_BLUE_70}}}
.sp{{flex:1}}
.title{{color:#fff;font-size:13px;font-weight:600}}
.tabs{{display:flex;gap:2px;padding:8px 14px 0;background:{IBM_GRAY_10};flex-wrap:wrap;border-bottom:1px solid {IBM_GRAY_20};flex-shrink:0}}
.tab{{border:1px solid {IBM_GRAY_20};border-bottom:none;border-radius:6px 6px 0 0;padding:8px 14px;
font-size:12px;cursor:pointer;font-family:Calibri,sans-serif;font-weight:600;
background:#fff;color:{IBM_GRAY_70}}}
.tab.active{{background:{IBM_BLUE_60};color:#fff;border-color:{IBM_BLUE_60}}}
.sw{{background:{IBM_GRAY_10};padding:20px;overflow:auto;flex:1;min-height:0}}
.panel{{background:#fff;padding:24px;border-radius:4px;box-shadow:0 2px 8px rgba(0,0,0,0.08);overflow-x:auto}}
</style></head><body>
<div class="wk">
  <div class="tb">
    <span class="title">📊 {self._html_esc(title)}</span>
    <span class="sp"></span>
    <span class="title">{total} sheet{"s" if total != 1 else ""}</span>
    <a class="b" href="{data_uri}" download="{safe_name}.xlsx">⬇ Download XLSX</a>
  </div>
  <div class="tabs">{"".join(tab_buttons)}</div>
  <div class="sw">{"".join(panels)}</div>
</div>
<script>
(function(){{
  function fit(){{
    try{{
      var fe=window.frameElement;
      if(fe){{
        fe.style.height='720px';
        fe.style.minHeight='720px';
        fe.style.width='100%';
        fe.setAttribute('height','720');
      }}
    }}catch(e){{}}
    try{{window.parent.postMessage({{type:'ibm-docgen-resize',height:720}},'*');}}catch(e){{}}
  }}
  fit();setTimeout(fit,100);setTimeout(fit,500);setTimeout(fit,1500);
  window.addEventListener('load',fit);
  new MutationObserver(fit).observe(document.documentElement,{{attributes:true,childList:true,subtree:false}});
}})();
function showTab(i){{
  document.querySelectorAll(".tab").forEach(function(t){{
    t.classList.toggle("active", parseInt(t.dataset.i,10)===i);
  }});
  document.querySelectorAll(".panel").forEach(function(p){{
    p.style.display = (parseInt(p.dataset.i,10)===i) ? "block" : "none";
  }});
}}
</script></body></html>"""
        return HTMLResponse(content=html, headers={"Content-Disposition": "inline"})

    # ══════════════════════════════════════════════════════════════════════
    # PPTX BUILDER — pure OOXML with image embedding
    # ══════════════════════════════════════════════════════════════════════
    def _build_and_render_pptx(self, session_id, title, client_name, sections, emitter):
        # Slide dimensions in EMU (13.333" x 7.5" for 16:9)
        SLIDE_W_EMU = 12192000
        SLIDE_H_EMU = 6858000

        def esc(s):
            return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        def txt_run(text, size=1800, bold=False, italic=False, color="161616"):
            b_attr = 'b="1" ' if bold else ''
            i_attr = 'i="1" ' if italic else ''
            return (
                f'<a:r><a:rPr lang="en-US" sz="{size}" '
                f'{b_attr}{i_attr}>'
                f'<a:solidFill><a:srgbClr val="{color}"/></a:solidFill>'
                f'<a:latin typeface="Calibri"/>'
                f'</a:rPr><a:t>{esc(text)}</a:t></a:r>'
            )

        def txt_box(x, y, w, h, text, size=1800, bold=False, color="161616", align="l"):
            return (
                f'<p:sp>'
                f'<p:nvSpPr><p:cNvPr id="{uuid.uuid4().int % 10000}" name="TxtBox"/>'
                f'<p:cNvSpPr txBox="1"/><p:nvPr/></p:nvSpPr>'
                f'<p:spPr><a:xfrm><a:off x="{x}" y="{y}"/><a:ext cx="{w}" cy="{h}"/></a:xfrm>'
                f'<a:prstGeom prst="rect"><a:avLst/></a:prstGeom><a:noFill/></p:spPr>'
                f'<p:txBody><a:bodyPr wrap="square" anchor="t"/><a:lstStyle/>'
                f'<a:p><a:pPr algn="{align}"/>'
                + txt_run(text, size=size, bold=bold, color=color)
                + '</a:p></p:txBody></p:sp>'
            )

        def bullet_box(x, y, w, h, bullets, size=1400):
            body = ""
            for b in bullets:
                body += (
                    f'<a:p><a:pPr marL="285750" indent="-285750"><a:buChar char="•"/></a:pPr>'
                    + txt_run(str(b), size=size) + '</a:p>'
                )
            return (
                f'<p:sp><p:nvSpPr><p:cNvPr id="{uuid.uuid4().int % 10000}" name="Bullets"/>'
                f'<p:cNvSpPr txBox="1"/><p:nvPr/></p:nvSpPr>'
                f'<p:spPr><a:xfrm><a:off x="{x}" y="{y}"/><a:ext cx="{w}" cy="{h}"/></a:xfrm>'
                f'<a:prstGeom prst="rect"><a:avLst/></a:prstGeom><a:noFill/></p:spPr>'
                f'<p:txBody><a:bodyPr wrap="square" anchor="t"/><a:lstStyle/>{body}</p:txBody></p:sp>'
            )

        def solid_bg(color):
            return (
                f'<p:sp><p:nvSpPr><p:cNvPr id="9999" name="bg"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr>'
                f'<p:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="{SLIDE_W_EMU}" cy="{SLIDE_H_EMU}"/></a:xfrm>'
                f'<a:prstGeom prst="rect"><a:avLst/></a:prstGeom>'
                f'<a:solidFill><a:srgbClr val="{color}"/></a:solidFill><a:ln><a:noFill/></a:ln></p:spPr></p:sp>'
            )

        def accent_bar():
            return (
                f'<p:sp><p:nvSpPr><p:cNvPr id="9998" name="bar"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr>'
                f'<p:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="228600" cy="{SLIDE_H_EMU}"/></a:xfrm>'
                f'<a:prstGeom prst="rect"><a:avLst/></a:prstGeom>'
                f'<a:solidFill><a:srgbClr val="0F62FE"/></a:solidFill><a:ln><a:noFill/></a:ln></p:spPr></p:sp>'
            )

        def image_xml(rid, x, y, w_emu, h_emu):
            return (
                f'<p:pic><p:nvPicPr><p:cNvPr id="{uuid.uuid4().int % 10000}" name="Image"/>'
                f'<p:cNvPicPr/><p:nvPr/></p:nvPicPr>'
                f'<p:blipFill><a:blip xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" r:embed="{rid}"/>'
                f'<a:stretch><a:fillRect/></a:stretch></p:blipFill>'
                f'<p:spPr><a:xfrm><a:off x="{x}" y="{y}"/><a:ext cx="{w_emu}" cy="{h_emu}"/></a:xfrm>'
                f'<a:prstGeom prst="rect"><a:avLst/></a:prstGeom></p:spPr></p:pic>'
            )

        # Build slides
        slides_xml = []
        slide_rels = []
        media_files = []  # (filename, bytes)

        # ── IBM logo footer (added to every slide) ──
        logo_png = self._get_ibm_logo_png()
        logo_fname = None
        if logo_png:
            logo_fname = "ibm_logo_black.png"
            media_files.append((logo_fname, logo_png))
            lw, lh = self._get_ibm_logo_dims()
            # 1/5 of previous 0.5-inch tall, placed at bottom-LEFT per request.
            logo_h_emu = 91440  # 0.1in
            logo_w_emu = int(logo_h_emu * (lw / max(lh, 1)))
            logo_x_emu = 228600  # 0.25in left margin
            logo_y_emu = SLIDE_H_EMU - logo_h_emu - 114300  # 0.125in bottom margin

        def logo_shape_and_rel(slide_rel_entries: list) -> str:
            """Append a logo image rel to slide_rel_entries and return the <p:pic> XML."""
            if not logo_fname:
                return ""
            rid = f"rId{len(slide_rel_entries)+2}"  # rId1 is layout
            slide_rel_entries.append(
                f'<Relationship Id="{rid}" '
                f'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" '
                f'Target="../media/{logo_fname}"/>'
            )
            return (
                f'<p:pic><p:nvPicPr><p:cNvPr id="{uuid.uuid4().int % 10000}" name="IBM Logo"/>'
                f'<p:cNvPicPr><a:picLocks noChangeAspect="1"/></p:cNvPicPr><p:nvPr/></p:nvPicPr>'
                f'<p:blipFill><a:blip r:embed="{rid}"/><a:stretch><a:fillRect/></a:stretch></p:blipFill>'
                f'<p:spPr><a:xfrm><a:off x="{logo_x_emu}" y="{logo_y_emu}"/>'
                f'<a:ext cx="{logo_w_emu}" cy="{logo_h_emu}"/></a:xfrm>'
                f'<a:prstGeom prst="rect"><a:avLst/></a:prstGeom></p:spPr></p:pic>'
            )

        # Slide 1: Cover
        cover_xml = (
            '<p:sp><p:nvSpPr><p:cNvPr id="1" name="CoverBG"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr>'
            f'<p:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="{SLIDE_W_EMU}" cy="{SLIDE_H_EMU}"/></a:xfrm>'
            '<a:prstGeom prst="rect"><a:avLst/></a:prstGeom>'
            '<a:solidFill><a:srgbClr val="0F62FE"/></a:solidFill><a:ln><a:noFill/></a:ln></p:spPr></p:sp>'
            + txt_box(685800, 2286000, 10820400, 1143000, title, size=3600, bold=True, color="FFFFFF")
            + txt_box(685800, 3657600, 10820400, 914400,
                      f"IBM Consulting  |  Prepared for {client_name}", size=1800, color="FFFFFF")
            + txt_box(685800, 5486400, 10820400, 457200,
                      time.strftime("%B %Y"), size=1200, color="FFFFFF")
        )
        # Every slide needs a relationship to the slideLayout (rId1) or PowerPoint
        # flags the pptx as corrupt. Keep rId2+ for images on that slide.
        _layout_rel = (
            '<Relationship Id="rId1" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideLayout" '
            'Target="../slideLayouts/slideLayout1.xml"/>'
        )
        # Cover keeps IBM blue background; skip logo here (black-on-blue unreadable).
        slides_xml.append(("slide1", cover_xml, [_layout_rel]))

        # Content slides
        for idx, section in enumerate(sections, start=1):
            slide_num = idx + 1
            slide_rel_entries = [_layout_rel]
            shapes = []
            shapes.append(accent_bar())
            shapes.append(
                txt_box(457200, 304800, 11277600, 685800,
                        section.get("title", f"Section {idx}"),
                        size=2600, bold=True, color="161616")
            )

            has_image = bool(section.get("_img_bytes"))

            # Paragraphs + bullets (text zone)
            text_x = 457200
            text_y = 1143000
            text_w = 5943600 if has_image else 11277600
            text_h = 5334000

            paras = section.get("paragraphs", []) or []
            bullets = section.get("bullets", []) or []

            # Combine paragraphs and bullets into one text frame
            if paras or bullets:
                body = ""
                for p in paras:
                    body += f'<a:p><a:pPr algn="l"/>' + txt_run(p, size=1400) + '</a:p>'
                for b in bullets:
                    body += (
                        f'<a:p><a:pPr marL="285750" indent="-285750" algn="l">'
                        f'<a:buChar char="•"/></a:pPr>' + txt_run(str(b), size=1400) + '</a:p>'
                    )
                shapes.append(
                    f'<p:sp><p:nvSpPr><p:cNvPr id="{idx*100+1}" name="Body"/>'
                    f'<p:cNvSpPr txBox="1"/><p:nvPr/></p:nvSpPr>'
                    f'<p:spPr><a:xfrm><a:off x="{text_x}" y="{text_y}"/><a:ext cx="{text_w}" cy="{text_h}"/></a:xfrm>'
                    f'<a:prstGeom prst="rect"><a:avLst/></a:prstGeom><a:noFill/></p:spPr>'
                    f'<p:txBody><a:bodyPr wrap="square" anchor="t"/><a:lstStyle/>{body}</p:txBody></p:sp>'
                )

            # Image on the right
            if has_image:
                media_idx = len(media_files)
                fname = f"image{media_idx+1}.png"
                media_files.append((fname, section["_img_bytes"]))
                rid = f"rId{len(slide_rel_entries)+2}"
                slide_rel_entries.append(
                    f'<Relationship Id="{rid}" '
                    f'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" '
                    f'Target="../media/{fname}"/>'
                )

                # Sizing for right half
                img_x = 6629400
                img_y = 1143000
                img_max_w = 5105400
                img_max_h = 4572000
                aspect = section.get("_img_height", 800) / max(section.get("_img_width", 1200), 1)
                img_w = img_max_w
                img_h = int(img_w * aspect)
                if img_h > img_max_h:
                    img_h = img_max_h
                    img_w = int(img_h / aspect)

                shapes.append(image_xml(rid, img_x, img_y, img_w, img_h))

                # Caption
                if section.get("image_caption"):
                    shapes.append(
                        txt_box(img_x, img_y + img_h + 50000, img_w, 400000,
                                f"Figure — {section['image_caption']}",
                                size=900, color="525252")
                    )

            # IBM logo footer — add last so it lays on top of everything else.
            logo_xml = logo_shape_and_rel(slide_rel_entries)
            if logo_xml:
                shapes.append(logo_xml)
            slide_content = "".join(shapes)
            slides_xml.append((f"slide{slide_num}", slide_content, slide_rel_entries))

        # Assemble slides into XML files
        def wrap_slide(content):
            return (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<p:sld xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
                'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" '
                'xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main">'
                '<p:cSld><p:spTree>'
                '<p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr>'
                '<p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/>'
                '<a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr>'
                f'{content}'
                '</p:spTree></p:cSld></p:sld>'
            )

        # ── Build the PPTX zip ──
        pptx_buf = io.BytesIO()
        with zipfile.ZipFile(pptx_buf, "w", zipfile.ZIP_DEFLATED) as zf:

            # Content types
            overrides = ''.join(
                f'<Override PartName="/ppt/slides/slide{i+1}.xml" '
                f'ContentType="application/vnd.openxmlformats-officedocument.presentationml.slide+xml"/>'
                for i in range(len(slides_xml))
            )
            ct_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                '<Default Extension="xml" ContentType="application/xml"/>'
                '<Default Extension="png" ContentType="image/png"/>'
                '<Default Extension="jpeg" ContentType="image/jpeg"/>'
                '<Default Extension="jpg" ContentType="image/jpeg"/>'
                '<Override PartName="/ppt/presentation.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml"/>'
                '<Override PartName="/ppt/theme/theme1.xml" ContentType="application/vnd.openxmlformats-officedocument.theme+xml"/>'
                '<Override PartName="/ppt/slideMasters/slideMaster1.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.slideMaster+xml"/>'
                '<Override PartName="/ppt/slideLayouts/slideLayout1.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.slideLayout+xml"/>'
                f'{overrides}'
                '</Types>'
            )
            zf.writestr("[Content_Types].xml", ct_xml)

            # Package rels
            zf.writestr("_rels/.rels",
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="ppt/presentation.xml"/>'
                '</Relationships>'
            )

            # presentation.xml
            slide_id_list = ''.join(
                f'<p:sldId id="{256+i}" r:id="rIdSl{i+1}"/>'
                for i in range(len(slides_xml))
            )
            pres_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<p:presentation xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
                'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" '
                'xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main">'
                # Slide master MUST come before slide list (required by schema).
                '<p:sldMasterIdLst><p:sldMasterId id="2147483648" r:id="rIdMaster"/></p:sldMasterIdLst>'
                f'<p:sldIdLst>{slide_id_list}</p:sldIdLst>'
                f'<p:sldSz cx="{SLIDE_W_EMU}" cy="{SLIDE_H_EMU}"/>'
                f'<p:notesSz cx="6858000" cy="9144000"/>'
                '</p:presentation>'
            )
            zf.writestr("ppt/presentation.xml", pres_xml)

            # presentation rels — must include master + theme
            pres_rels = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rIdMaster" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideMaster" '
                'Target="slideMasters/slideMaster1.xml"/>'
                '<Relationship Id="rIdTheme" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme" '
                'Target="theme/theme1.xml"/>'
            )
            for i in range(len(slides_xml)):
                pres_rels += f'<Relationship Id="rIdSl{i+1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide" Target="slides/slide{i+1}.xml"/>'
            pres_rels += '</Relationships>'
            zf.writestr("ppt/_rels/presentation.xml.rels", pres_rels)

            # ── Theme, slide master, slide layout (required by PowerPoint) ──
            theme_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<a:theme xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" name="IBM">'
                '<a:themeElements>'
                '<a:clrScheme name="IBM">'
                '<a:dk1><a:srgbClr val="161616"/></a:dk1>'
                '<a:lt1><a:srgbClr val="FFFFFF"/></a:lt1>'
                '<a:dk2><a:srgbClr val="0F62FE"/></a:dk2>'
                '<a:lt2><a:srgbClr val="F4F4F4"/></a:lt2>'
                '<a:accent1><a:srgbClr val="0F62FE"/></a:accent1>'
                '<a:accent2><a:srgbClr val="8A3FFC"/></a:accent2>'
                '<a:accent3><a:srgbClr val="007D79"/></a:accent3>'
                '<a:accent4><a:srgbClr val="FF7EB6"/></a:accent4>'
                '<a:accent5><a:srgbClr val="FA4D56"/></a:accent5>'
                '<a:accent6><a:srgbClr val="FFC857"/></a:accent6>'
                '<a:hlink><a:srgbClr val="0F62FE"/></a:hlink>'
                '<a:folHlink><a:srgbClr val="8A3FFC"/></a:folHlink>'
                '</a:clrScheme>'
                '<a:fontScheme name="IBM">'
                '<a:majorFont><a:latin typeface="Calibri"/><a:ea typeface=""/><a:cs typeface=""/></a:majorFont>'
                '<a:minorFont><a:latin typeface="Calibri"/><a:ea typeface=""/><a:cs typeface=""/></a:minorFont>'
                '</a:fontScheme>'
                '<a:fmtScheme name="Office">'
                '<a:fillStyleLst>'
                '<a:solidFill><a:schemeClr val="phClr"/></a:solidFill>'
                '<a:solidFill><a:schemeClr val="phClr"/></a:solidFill>'
                '<a:solidFill><a:schemeClr val="phClr"/></a:solidFill>'
                '</a:fillStyleLst>'
                '<a:lnStyleLst>'
                '<a:ln w="9525" cap="flat" cmpd="sng" algn="ctr"><a:solidFill><a:schemeClr val="phClr"/></a:solidFill><a:prstDash val="solid"/></a:ln>'
                '<a:ln w="9525" cap="flat" cmpd="sng" algn="ctr"><a:solidFill><a:schemeClr val="phClr"/></a:solidFill><a:prstDash val="solid"/></a:ln>'
                '<a:ln w="9525" cap="flat" cmpd="sng" algn="ctr"><a:solidFill><a:schemeClr val="phClr"/></a:solidFill><a:prstDash val="solid"/></a:ln>'
                '</a:lnStyleLst>'
                '<a:effectStyleLst>'
                '<a:effectStyle><a:effectLst/></a:effectStyle>'
                '<a:effectStyle><a:effectLst/></a:effectStyle>'
                '<a:effectStyle><a:effectLst/></a:effectStyle>'
                '</a:effectStyleLst>'
                '<a:bgFillStyleLst>'
                '<a:solidFill><a:schemeClr val="phClr"/></a:solidFill>'
                '<a:solidFill><a:schemeClr val="phClr"/></a:solidFill>'
                '<a:solidFill><a:schemeClr val="phClr"/></a:solidFill>'
                '</a:bgFillStyleLst>'
                '</a:fmtScheme>'
                '</a:themeElements>'
                '</a:theme>'
            )
            zf.writestr("ppt/theme/theme1.xml", theme_xml)

            # Slide master — minimal valid content
            slide_master_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<p:sldMaster xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
                'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" '
                'xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main">'
                '<p:cSld><p:bg><p:bgRef idx="1001"><a:schemeClr val="bg1"/></p:bgRef></p:bg>'
                '<p:spTree>'
                '<p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr>'
                '<p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/>'
                '<a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr>'
                '</p:spTree></p:cSld>'
                '<p:clrMap bg1="lt1" tx1="dk1" bg2="lt2" tx2="dk2" accent1="accent1" '
                'accent2="accent2" accent3="accent3" accent4="accent4" accent5="accent5" '
                'accent6="accent6" hlink="hlink" folHlink="folHlink"/>'
                '<p:sldLayoutIdLst><p:sldLayoutId id="2147483649" r:id="rIdLayout1"/></p:sldLayoutIdLst>'
                '</p:sldMaster>'
            )
            zf.writestr("ppt/slideMasters/slideMaster1.xml", slide_master_xml)
            zf.writestr(
                "ppt/slideMasters/_rels/slideMaster1.xml.rels",
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rIdLayout1" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideLayout" '
                'Target="../slideLayouts/slideLayout1.xml"/>'
                '<Relationship Id="rIdTheme" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme" '
                'Target="../theme/theme1.xml"/>'
                '</Relationships>'
            )

            # Slide layout (blank) — minimal valid
            slide_layout_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<p:sldLayout xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
                'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" '
                'xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" '
                'type="blank" preserve="1">'
                '<p:cSld name="Blank"><p:spTree>'
                '<p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr>'
                '<p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/>'
                '<a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr>'
                '</p:spTree></p:cSld>'
                '</p:sldLayout>'
            )
            zf.writestr("ppt/slideLayouts/slideLayout1.xml", slide_layout_xml)
            zf.writestr(
                "ppt/slideLayouts/_rels/slideLayout1.xml.rels",
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rIdMaster" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideMaster" '
                'Target="../slideMasters/slideMaster1.xml"/>'
                '</Relationships>'
            )

            # Each slide + its rels
            for i, (name, content, rel_entries) in enumerate(slides_xml):
                zf.writestr(f"ppt/slides/slide{i+1}.xml", wrap_slide(content))
                if rel_entries:
                    slide_rels_xml = (
                        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                        + "".join(rel_entries) +
                        '</Relationships>'
                    )
                    zf.writestr(f"ppt/slides/_rels/slide{i+1}.xml.rels", slide_rels_xml)

            # Media files
            for fname, fbytes in media_files:
                zf.writestr(f"ppt/media/{fname}", fbytes)

        pptx_bytes = pptx_buf.getvalue()
        pptx_b64 = base64.b64encode(pptx_bytes).decode()
        data_uri = f"data:application/vnd.openxmlformats-officedocument.presentationml.presentation;base64,{pptx_b64}"

        return self._render_pptx_preview(title, client_name, sections, data_uri)

    def _render_pptx_preview(self, title, client_name, sections, data_uri):
        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", title)[:50] or "deck"

        slide_parts = []
        # Cover
        slide_parts.append(
            f'<div class="sl" style="display:block;aspect-ratio:16/9;background:{IBM_BLUE_60};'
            f'padding:60px;color:#fff;font-family:Calibri,sans-serif;position:relative">'
            f'<div style="font-size:36px;font-weight:700;margin-top:120px">{self._html_esc(title)}</div>'
            f'<div style="font-size:18px;margin-top:24px;opacity:0.9">IBM Consulting  |  Prepared for {self._html_esc(client_name)}</div>'
            f'<div style="font-size:14px;margin-top:12px;opacity:0.8">{time.strftime("%B %Y")}</div>'
            f'</div>'
        )

        # Content slides
        for idx, section in enumerate(sections, start=1):
            has_img = bool(section.get("_img_bytes"))
            text_col_w = "50%" if has_img else "100%"

            text_html = f'<h2 style="font-size:26px;color:{IBM_GRAY_100};font-weight:700;margin:0 0 16px">{self._html_esc(section.get("title", ""))}</h2>'
            for p in section.get("paragraphs", []) or []:
                text_html += f'<p style="font-size:13px;color:{IBM_GRAY_100};margin:8px 0;line-height:1.4">{self._html_esc(p)}</p>'
            bullets = section.get("bullets", []) or []
            if bullets:
                lis = "".join(
                    f'<li style="font-size:13px;color:{IBM_GRAY_100};margin:4px 0">{self._html_esc(b)}</li>'
                    for b in bullets
                )
                text_html += f'<ul style="padding-left:20px;margin:8px 0">{lis}</ul>'

            img_html = ""
            if has_img:
                img_b64 = base64.b64encode(section["_img_bytes"]).decode()
                img_html = (
                    f'<div style="flex:0 0 46%;padding-left:20px;display:flex;flex-direction:column;justify-content:center">'
                    f'<img src="data:image/png;base64,{img_b64}" '
                    f'style="max-width:100%;max-height:60vh;height:auto;border-radius:4px;object-fit:contain"/>'
                    + (
                        f'<div style="font-size:10px;color:{IBM_GRAY_70};font-style:italic;margin-top:8px;text-align:center">'
                        f'Figure — {self._html_esc(section.get("image_caption") or "")}</div>'
                        if section.get("image_caption") else ""
                    )
                    + '</div>'
                )

            slide_parts.append(
                f'<div class="sl" style="display:none;aspect-ratio:16/9;background:#fff;'
                f'border-left:8px solid {IBM_BLUE_60};padding:40px 48px;font-family:Calibri,sans-serif;'
                f'position:relative">'
                f'<div style="display:flex;height:100%">'
                f'<div style="flex:1 1 {text_col_w};padding-right:16px">{text_html}</div>'
                f'{img_html}'
                f'</div></div>'
            )

        total = len(slide_parts)
        html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
*{{box-sizing:border-box;margin:0}}
html,body{{height:720px;min-height:720px}}
body{{font-family:Calibri,system-ui,sans-serif;background:#f0f2f5;padding:12px;display:flex;align-items:stretch;justify-content:center}}
.dk{{border:2px solid {IBM_BLUE_60};border-radius:10px;overflow:hidden;width:100%;max-width:1280px;height:696px;margin:0 auto;background:#fff;display:flex;flex-direction:column}}
.tb{{display:flex;align-items:center;gap:8px;padding:10px 14px;background:{IBM_BLUE_70};flex-wrap:wrap;flex-shrink:0}}
.b{{border:none;border-radius:4px;padding:6px 14px;font-size:12px;cursor:pointer;
font-family:Calibri,sans-serif;font-weight:600;text-decoration:none;display:inline-block}}
.bw{{background:#fff;color:{IBM_BLUE_70}}}
.sn{{color:#fff;font-size:12px;min-width:90px;text-align:center}}
.sp{{flex:1}}
.sw{{background:{IBM_GRAY_10};padding:20px;overflow:auto;flex:1;min-height:0}}
.sl{{max-width:100%;width:100%;margin:0 auto 16px;box-shadow:0 2px 8px rgba(0,0,0,0.12);border-radius:4px;overflow:hidden}}
</style></head><body>
<div class="dk">
  <div class="tb">
    <button class="b bw" onclick="nav(-1)">← Prev</button>
    <span class="sn" id="sn">Slide 1 / {total}</span>
    <button class="b bw" onclick="nav(1)">Next →</button>
    <span class="sp"></span>
    <a class="b bw" href="{data_uri}" download="{safe_name}.pptx">⬇ Download PPTX</a>
  </div>
  <div class="sw">{"".join(slide_parts)}</div>
</div>
<script>
(function(){{
  function fit(){{
    try{{
      var fe=window.frameElement;
      if(fe){{
        fe.style.height='720px';
        fe.style.minHeight='720px';
        fe.style.width='100%';
        fe.setAttribute('height','720');
      }}
    }}catch(e){{}}
    try{{window.parent.postMessage({{type:'ibm-docgen-resize',height:720}},'*');}}catch(e){{}}
  }}
  fit();setTimeout(fit,100);setTimeout(fit,500);setTimeout(fit,1500);
  window.addEventListener('load',fit);
  new MutationObserver(fit).observe(document.documentElement,{{attributes:true,childList:true,subtree:false}});
}})();
var cur=0,sl=document.querySelectorAll(".sl"),tot=sl.length;
function nav(d){{sl[cur].style.display="none";cur=Math.max(0,Math.min(tot-1,cur+d));
sl[cur].style.display="block";document.getElementById("sn").textContent="Slide "+(cur+1)+" / "+tot;
var sw=document.querySelector(".sw");if(sw)sw.scrollTop=0}}
document.addEventListener("keydown",function(e){{
if(e.key==="ArrowLeft")nav(-1);if(e.key==="ArrowRight")nav(1)}});
</script></body></html>"""

        return HTMLResponse(content=html, headers={"Content-Disposition": "inline"})

    # ══════════════════════════════════════════════════════════════════════
    # UTILITIES
    # ══════════════════════════════════════════════════════════════════════
    def _ext(self, name: str) -> str:
        if "." not in name: return ""
        return "." + name.rsplit(".", 1)[-1].lower()

    def _classify(self, filename: str) -> str:
        f = (filename or "").lower()
        if "case_study" in f or "case-study" in f or "success" in f: return "case_study"
        if "architecture" in f or "solution" in f or "blueprint" in f: return "solution_brief"
        if "methodology" in f or "approach" in f or "framework" in f: return "methodology"
        if "offering" in f or "capability" in f or "portfolio" in f: return "capability"
        return "general"

    def _humanize(self, stem: str) -> str:
        s = re.sub(r"[_\-]+", " ", stem)
        s = re.sub(r"(\d+)", r" \1", s)
        return s.strip().title()

    def _html_esc(self, s: str) -> str:
        return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

    async def _emit(self, emitter, msg, done=False):
        if emitter:
            await emitter({
                "type": "status",
                "data": {"description": msg, "done": done},
            })
        # Whenever a fresh phase is announced, reset the heartbeat clock so
        # elapsed-time pings are scoped to the current step, not the whole call.
        if not done:
            self._phase = msg
            self._phase_started = time.time()

    def _set_phase(self, text: str):
        """Update the current phase shown by the heartbeat ticker."""
        self._phase = text
        self._phase_started = time.time()

    async def _heartbeat(self, emitter, interval: int = 10):
        """Emit a blue-info status every 'interval' seconds while work runs.

        OWUI renders status events in its info/accent (blue) style, so the
        user keeps seeing a live update of which phase we are in and how long
        it has been running. Cancel this task when the operation completes.
        """
        if not emitter:
            return
        try:
            while True:
                await asyncio.sleep(interval)
                elapsed = int(time.time() - (self._phase_started or time.time()))
                phase = self._phase or "working"
                # Info (blue) icon + bold-ish phrasing. Markdown in status
                # descriptions renders as plain text in OWUI but the info
                # style already shows in blue.
                await emitter({
                    "type": "status",
                    "data": {
                        "description": f"🔵 ⏱️ {elapsed}s — {phase}",
                        "done": False,
                    },
                })
        except asyncio.CancelledError:
            return

    def _start_heartbeat(self, emitter, interval: int = 10):
        """Spawn a heartbeat task; returns the task so callers can cancel it."""
        if not emitter:
            return None
        try:
            return asyncio.create_task(self._heartbeat(emitter, interval))
        except RuntimeError:
            return None

    @staticmethod
    async def _stop_heartbeat(task):
        if not task:
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
