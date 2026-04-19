# IBM DocGen — End-to-End Flow (trimmed build, 4893 lines)

Derived by AST-walking `IBM_DocGen_WithImages_v2.py` from every public tool
entrypoint down to the OOXML zip writers. `_emit / _start_heartbeat /
_stop_heartbeat / _set_phase / _eta_for` are no-op stubs kept only for
backwards-compatible call sites and are not shown below.

```mermaid
flowchart TD
    USER[User prompt in Open WebUI chat] --> LLM[Bedrock Claude via litellm]
    LLM -->|tool call| ENTRY{Which tool method?}

    %% === Source gatherers ===
    ENTRY -->|smart shortcut| S1[prepare_content_smart]
    ENTRY -->|notes only| S2[prepare_content_from_notes]
    ENTRY -->|folder only| S3[prepare_content_from_folder]
    ENTRY -->|chat attach| S4[prepare_content_from_attachments]
    ENTRY -->|knowledge coll| S5[prepare_content_from_knowledge]
    ENTRY -->|MCP tool| S6[prepare_content_from_mcp]
    ENTRY -->|web search| S7[prepare_content_from_web_search]
    ENTRY -->|list MCP| S8[list_mcp_tools]
    ENTRY -->|build doc| ASM[assemble_document]

    %% === Source adapters ===
    S1 --> NOTES[_read_owui_notes<br/>SQLite: note table]
    S1 --> FOLDER[_read_owui_folder<br/>SQLite: folder + chat + file]
    S1 --> ATTACH[_extract_attachments_parallel<br/>PDF/DOCX/PPTX/XLSX]
    S1 --> KNOW[_retrieve_text_from_collection<br/>+ _list_collection_files]
    S1 --> MCP[_load_mcp_servers<br/>_discover_mcp_catalog<br/>_rank_mcp_tools]

    S2 --> NOTES
    S3 --> FOLDER
    S4 --> ATTACH
    S5 --> KNOW
    S6 --> MCP
    S7 --> WEB[_web_search_text<br/>Google CSE / DDG fallback]

    %% === Assemble path ===
    ASM --> CAPS[_enforce_content_caps<br/>PPTX 15 slides x 100 words<br/>DOCX 15 pages x 300 words<br/>XLSX 10 sheets x 100 rows]
    CAPS --> CHARTS[_autoinject_charts]
    CHARTS -->|per section w/ numeric table| SPEC[_chart_spec_from_table<br/>auto-pick bar/pie/line]

    CHARTS --> FMT{format?}
    FMT -->|docx| DOCX[_build_and_render_docx]
    FMT -->|pptx| PPTX[_build_and_render_pptx]
    FMT -->|xlsx| XLSX[_build_and_render_xlsx]

    %% === OOXML builders ===
    DOCX --> DX_CHART[add_chart_xml<br/>word/charts/chartN.xml]
    DOCX --> DX_LOGO[_get_ibm_logo_png<br/>+ _get_ibm_logo_dims]
    DOCX --> DX_ZIP[zipfile.ZipFile<br/>document.xml + rels<br/>+ Content_Types]

    PPTX --> PX_CHART[chart_graphic_frame<br/>ppt/charts/chartN.xml]
    PPTX --> PX_LOGO[_get_ibm_logo_png]
    PPTX --> PX_ZIP[zipfile.ZipFile<br/>slideN.xml + rels<br/>+ theme + master + layout]

    XLSX --> XL_LOGO[_get_ibm_logo_png]
    XLSX --> XL_ZIP[zipfile.ZipFile<br/>sheetN.xml + sharedStrings<br/>+ styles + workbook]

    %% === Chart XML ===
    DX_CHART --> OOXML[_ooxml_chart_part_xml<br/>c:chartSpace -> c:barChart / c:pieChart / c:lineChart<br/>IBM Plex Sans + Carbon palette]
    PX_CHART --> OOXML

    %% === Return ===
    DX_ZIP --> RESP[HTMLResponse<br/>Content-Disposition: inline]
    PX_ZIP --> RESP
    XL_ZIP --> RESP
    S1 --> PKG[_package -> JSON to LLM]
    S2 --> PKG
    S3 --> PKG
    S4 --> PKG
    S5 --> PKG
    S6 --> PKG
    S7 --> PKG
    S8 --> PKG
    PKG -.LLM decides next step.-> ASM

    RESP --> IFRAME[Open WebUI iframe<br/>renders inline preview<br/>+ Download button]
```

## Key file paths written into each zip

| Artifact | DOCX | PPTX | XLSX |
|---|---|---|---|
| Content_Types | `[Content_Types].xml` (adds `drawingml.chart+xml` override per chart) | same | same |
| Package rels | `_rels/.rels` | `_rels/.rels` | `_rels/.rels` |
| Main part | `word/document.xml` | `ppt/presentation.xml` + `ppt/slides/slideN.xml` | `xl/workbook.xml` + `xl/worksheets/sheetN.xml` |
| Chart parts | `word/charts/chartN.xml` | `ppt/charts/chartN.xml` | (table-only, no chart in first release) |
| Media | `word/media/` | `ppt/media/` | n/a |
| Footer (page num) | `word/footer1.xml` | in every slide XML | sheet header |

## What the LLM emits vs. what the tool builds

The LLM's only output is a `sections_json` array + `format` + `title` +
`client_name`. For charts, it sets `section.table` (headers + numeric rows)
plus optional `chart_type: "bar" | "pie" | "line"`. The tool does the rest —
it never executes any Python code the LLM wrote.

```mermaid
flowchart LR
    LLM[LLM] -->|sections_json + format + title| TOOL[assemble_document]
    TOOL --> AUTO[_autoinject_charts<br/>reads section.table<br/>stamps section._chart_spec]
    AUTO --> BUILD[builder writes<br/>c:chartSpace part + w:drawing/<br/>p:graphicFrame]
    BUILD --> FILE[editable Office chart<br/>in downloaded .docx / .pptx]
```
