#!/usr/bin/env python3
"""Push IBM_DocGen_WithImages_v2.py + specs + system prompt to the ICA Beta
Open WebUI instance using a personal API key.

Usage:
    export ICA_BETA_API_KEY="sk-..."   # from Beta > Settings > Account > API Keys
    python3 push_to_beta.py

What it does:
    1. GET /api/v1/tools/ — list tools, locate the DocGen tool by id
    2. POST /api/v1/tools/id/{id}/update — push new content + specs + meta
       (if the tool doesn't exist yet, POST /api/v1/tools/create)
    3. Update the model params.system for ibm-m365-model (if present) with
       the latest system_prompt.txt content.

Any HTTP 4xx result is printed with response body for debugging.
"""
import json, os, sys, re, time
from pathlib import Path
import requests

BASE = "https://ica20-beta.ica.ibm.com"
API_KEY = os.environ.get("ICA_BETA_API_KEY", "").strip()
if not API_KEY:
    print("ERROR: set ICA_BETA_API_KEY env var first "
          "(Beta > Settings > Account > API Keys)", file=sys.stderr)
    sys.exit(2)

HEADERS = {"Authorization": f"Bearer {API_KEY}",
           "Content-Type": "application/json",
           "User-Agent": "IBM-DocGen-Push/1.0"}

HERE = Path(__file__).parent
TOOL_PY = HERE / "IBM_DocGen_WithImages_v2.py"
PROMPT_TXT = HERE / "system_prompt.txt"
SEED_PY = HERE / "seed_openwebui.py"
TOOL_ID = "ibm_docgen_with_images"
MODEL_ID = "ibm-m365-model"

def log(msg): print(f"[push] {msg}")

def load_specs_from_seed():
    """Parse SPECS list out of seed_openwebui.py via exec in an isolated ns."""
    src = SEED_PY.read_text()
    m = re.search(r"^SPECS\s*=\s*(\[.*?^\])", src, flags=re.S | re.M)
    if not m:
        raise SystemExit("SPECS list not found in seed_openwebui.py")
    ns = {}
    exec("SPECS = " + m.group(1), ns)
    return ns["SPECS"]

def push_tool():
    content = TOOL_PY.read_text()
    specs = load_specs_from_seed()
    meta = {
        "description": "IBM Consulting DocGen — generates IBM-branded DOCX / PPTX / XLSX with charts, KB 50/50 layout, Wikipedia fallback.",
        "manifest": {
            "title": "IBM DocGen with Images",
            "author": "IBM Consulting",
            "version": "2.2",
        },
    }
    # Check if the tool already exists
    log(f"GET {BASE}/api/v1/tools/")
    r = requests.get(f"{BASE}/api/v1/tools/", headers=HEADERS, timeout=30)
    if r.status_code != 200:
        log(f"  list failed {r.status_code}: {r.text[:300]}"); return False
    tools = r.json() if isinstance(r.json(), list) else (r.json().get("data") or [])
    existing = next((t for t in tools if (t.get("id") == TOOL_ID)), None)

    payload = {
        "id": TOOL_ID,
        "name": "IBM DocGen with Images",
        "content": content,
        "specs": specs,
        "meta": meta,
    }
    if existing:
        url = f"{BASE}/api/v1/tools/id/{TOOL_ID}/update"
        log(f"POST {url}  (tool exists, updating)")
        r = requests.post(url, headers=HEADERS, json=payload, timeout=60)
    else:
        url = f"{BASE}/api/v1/tools/create"
        log(f"POST {url}  (tool is new, creating)")
        r = requests.post(url, headers=HEADERS, json=payload, timeout=60)
    log(f"  -> {r.status_code}")
    if r.status_code >= 400:
        log(f"  body: {r.text[:500]}"); return False
    log(f"  tool pushed: {len(content):,} bytes, {len(specs)} specs")
    return True

def push_prompt():
    """Model endpoint varies by OWUI version. Try /api/v1/models/{id}/update first."""
    prompt = PROMPT_TXT.read_text()
    log(f"GET {BASE}/api/v1/models/")
    r = requests.get(f"{BASE}/api/v1/models/", headers=HEADERS, timeout=30)
    if r.status_code != 200:
        log(f"  models list failed {r.status_code}: {r.text[:200]}"); return False
    models = r.json() if isinstance(r.json(), list) else (r.json().get("data") or [])
    target = next((m for m in models if m.get("id") == MODEL_ID), None)
    if not target:
        log(f"  model {MODEL_ID} not found on Beta — paste prompt manually via Workspace > Models")
        return False
    params = dict(target.get("params") or {})
    params["system"] = prompt
    body = {
        "id": MODEL_ID,
        "name": target.get("name", "IBM M365Docgen"),
        "base_model_id": target.get("base_model_id"),
        "meta": target.get("meta") or {},
        "params": params,
    }
    url = f"{BASE}/api/v1/models/model/update?id={MODEL_ID}"
    log(f"POST {url}")
    r = requests.post(url, headers=HEADERS, json=body, timeout=30)
    log(f"  -> {r.status_code}")
    if r.status_code >= 400:
        log(f"  body: {r.text[:500]}"); return False
    log(f"  prompt pushed: {len(prompt):,} chars")
    return True

if __name__ == "__main__":
    print(f"=== Push to {BASE} ===")
    print(f"    tool file:  {TOOL_PY}  ({TOOL_PY.stat().st_size:,} bytes)")
    print(f"    prompt:     {PROMPT_TXT}  ({PROMPT_TXT.stat().st_size:,} bytes)")
    print()
    ok_tool = push_tool()
    print()
    ok_prompt = push_prompt()
    print()
    print("=" * 40)
    print(f"  tool:   {'✅ pushed' if ok_tool else '❌ failed'}")
    print(f"  prompt: {'✅ pushed' if ok_prompt else '❌ failed or not applicable'}")
    sys.exit(0 if ok_tool else 1)
