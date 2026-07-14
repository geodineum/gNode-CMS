# gNode-CMS :: CONTRACT primer (SCN)
> one-line: SCN primer — TRUTH = code on disk, this file is a point-in-time compression. Companion: CONTRACT.md (authoritative).

## ::ROLE
Signed Rust extension compiled INTO gNode daemon (gNode=Sun, ValKey=backend). The CMS organ: template render + content store/cache + asset bundle + message-format registry. Stateless command dispatcher; ALL state in ValKey, site-scoped. `name:cms` `feature:cms` (no `tier:` field — daemon resolves tier) (extension.yaml). Discovery via `GNODE_EXT_CMS_PATH` | sibling `../gNode-CMS/`. Reference impl for the gNode extension system. 23 commands, 4 handler families. Canonical branch=`remediation/ch1-deep`.

## ::ANCHOR
- handlers: `src/handlers/{template,content,asset,format}.rs`; each exposes `register()` → daemon dispatcher (template.rs:28-193).
- types: `CommandResult{success|error|success_json}` (template.rs:34,254-259) · `CommandDescriptor{lane,name,category,params_schema,returns_schema,async_capable}` (template.rs:58-79) · `Lane::Fast` (template.rs:59) · `Command{id,parameters:Map}` (template.rs:199-209) · `GeometricTopology{services:HashMap<String,Service>,capability_dimensions:Vec<String>,dimensions}` (template.rs:354-375) · `Service{point:FixedVector(8D),metadata}` (368-375).
- 23 cmds: template[render_template,**render_string**,serve_fragment,list_templates,discover_similar_templates,discover_templates_by_capability,get_template_capabilities] + content[content_store,content_retrieve,template_fragment,asset_bundle] + asset[asset_store,asset_get,asset_delete,asset_list,manifest_set,manifest_get,manifest_delete,manifest_list] + format[register_format,list_formats,detect_format,convert_format] (extension.yaml). render_string=ad-hoc Tera string→{html}, no pre-register (template.rs:85-105,288-350).
- config_schema (`config_schema.yaml`, component `gnode_cms`): CMS_TEMPLATE_CACHE_SIZE(int 256) · CMS_MANIFEST_REBUILD_INTERVAL_SECS(int 30) · CMS_COMPRESSION_LEVEL(enum off|fast|balanced|max=balanced) · CMS_MINIFY(bool true); all mutable.
- lua lib: `functions/gnode_asset.lua` (GNODE_ASSET_*; defs :83-674, build_key :52-69, keys :12-17).
- fcall in: `GNODE_ASSET_{STORE,GET,DELETE,LIST,MANIFEST_SET/GET/DELETE/LIST,BUILD_STATUS,EXISTS}` · `GNODE_CACHE_{SET,GET}` (core lua).
- keyspace: `{site_id}:asset:{id}` `{site_id}:asset:{id}:meta` `{site_id}:asset:manifests` `{site_id}:asset:manifest:{id}` `{site_id}:gnode:bundle:{id}(+:meta)` `{site_id}:template:{id}(+:meta)` `{site_id}:metrics:asset`.
- sig: `extension.sig` + `AUTHOR_PUBKEY` fp `2ff9966fcad06b6d` (ext_verify.rs:116-154, ext_author.rs).

## ::ARCHITECTURE
Rust orchestration + Lua atomicity. Build-time Ed25519-signed extension (canonical hash over extension.yaml + handler_files + gnode_asset → verify_strict vs AUTHOR_PUBKEY). Daemon core calls `register()` at startup → installs `CommandHandlerFn`/`AsyncCommandHandlerFn` into `HashMap<String,_>` + `Vec<CommandDescriptor>`. Dispatch: sync compute | async `Pin<Box<Future>>` → FCALL via `crate::integration::valkey_functions::execute_function(_async)`. Design choices: stateless-dispatcher; ValKey = single source of truth; site-ID scoping via hash-tag `{site_id}`; metadata layered in separate `:meta` keys; composition (Rust=validate/orchestrate, Lua=atomic key ops); format = BASE capability: the four format commands are served SOLELY by the native FormatProcessor (no premium gNode-BROKER dependency, no ValKey fallback, no `#[cfg(feature="cms")]` gate); uninitialized processor → hard error. 8D capability vectors in topology drive template similarity/capability discovery.

## ::IO
- IN: `Command{id,parameters:JSON Map}` from daemon dispatcher (parse via template.rs:199-222). topology read-lock `get_topology_ref()` (services/capability_dimensions/dimensions). `crate::integration` {minify_safe, compress_smart, decode_and_decompress, template_renderer (Tera), current_timestamp}; `crate::config::GNodeSettings`.
- OUT: ValKey via FCALL → GNODE_ASSET_* (asset/manifest CRUD+index) and GNODE_CACHE_{SET,GET} (template/content cache). Writes site-scoped STRING/HASH/SET keys (see ::ANCHOR keyspace). Returns `CommandResult` JSON.
- wire: FCALL `<fn> numkeys [keys] args…`; `site_id` passed as ARG (not enforced in Lua). Lua HASH uses abbreviated fields (`v,sc,bo,ca,ua,ct,sz,cmp,sa,ua,etag`); Rust JSON responses expand to full names. GNODE_CACHE response = JSON string `{ok,...}`. Manifest layout∈`cube|tesseract|grid|custom`, type∈`inline|reference|hybrid`.

## ::CONTRACT
- PROVIDES: 23 daemon commands (signatures+defaults in CONTRACT.md §1) + config_schema (gnode_cms). Stable key schema `{site_id}:asset:*` / `:template:*` / `:gnode:bundle:*`. GNODE_ASSET_* fcall surface for the daemon background builder (bundle keys).
- CONSUMES: ValKey GNODE_ASSET_* + GNODE_CACHE_* (FCALL); `GeometricTopology` (read-lock, template discovery + 8D vectors); `Command` struct; `crate::integration` content procs (minify/compress/decompress) + Tera renderer; `crate::config::GNodeSettings`; `crate::daemon::GNodeDaemon::{get_topology_ref,get_format_processor_ref}`.

## ::USECASES
render+cache HTML (Tera, var substitution) · serve HTML fragments (HTMX headers + ETag) · template similarity discovery (8D vector distance) · capability-constraint filtering · store/retrieve minified+gzip content · template_fragment w/ dep extraction + topology registration · multi-asset bundling (js/css/mixed) · asset CRUD+list (version+compress+etag) · manifest CRUD (cube/tesseract/grid for bg builder) · register/list/detect/convert message formats (JSONSchema + confidence).

## ::LIMITATIONS
- async handlers delegate to sync via connection_manager (asset.rs:737-871) — Lua FCALL has no native async; non-blocking at HTTP, blocking in Lua.
- list_templates / discovery read in-mem topology, NO pagination, NO staleness check — async registration may lag reads.
- manifest/asset `version` optional; NO schema versioning / backward-compat enforcement.
- format commands = single deterministic native path (premium BROKER FCALL + ValKey fallback removed): register_format→`{status:"registered",format_name}` (async +`"async":true`); detect→`{format_name,version,confidence}`; convert→converted value; list→array. Uninitialized processor→hard error. NO divergent `{status,result,timestamp}` wrapper, NO feature-gate.
- compress optional + heuristic (compress_smart may skip) → `compressed` flag may not reflect storage.
- NO asset size validation (Lua + Rust call site) → oversized silently succeed/truncate.
- NO transaction across multi asset/manifest ops; manifest_delete doesn't verify bundle refs.
- asset_list SCAN cursor = string; no pagination consistency if keyset mutates mid-iter.
- template_id vs bundle_id namespace: no collision check.
- Lua↔Rust field-name mapping implicit/unvalidated; manual key construction (template.rs:241, content.rs:290,471) bypasses build_key.
- `site_id` NOT enforced at Lua layer — caller owns scope isolation.
- errors = user-facing JSON, no structured retry codes.

## ::GRAPH
DEPENDS_ON: ValKey(gnode_asset.lua + core GNODE_CACHE_*) · gNode daemon{dispatcher, GeometricTopology, Command, GNodeSettings, format_processor} · crate::integration{Tera, minify/compress/decompress}.
PROVIDES_TO: gNode daemon command dispatcher (23 cmds) · daemon background bundle builder (reads `{site_id}:gnode:bundle:*`, written from manifests).
ADHERES_TO: Ed25519 signed-extension scheme of gNode verifier (ext_verify.rs:116-154 — ADHERES, canonical hash matches shipped files) · GNODE_ASSET_* fcall-name contract of gnode_asset.lua (ADHERES, every called fn registered; Lua superset).
ISOLATED_FROM: comms message format / Geodineum-COMMS (no involvement) · face_mapping key drift (gCube/gTesseract — not CMS) · health-metrics stream.

## ::LATENT
- "23 CMS commands (incl. render_string), four handler families, register() into the daemon dispatcher at init"
- "stateless dispatcher — ValKey is the single source of truth, hash-tagged {site_id} scoping"
- "Lua abbreviates HASH fields, Rust expands them — implicit mapping, GNODE_ASSET_* FCALL"
- "signed extension, Ed25519 fingerprint 2ff9966fcad06b6d, canonical hash over yaml+handlers+lua"
- "8D capability vectors in GeometricTopology drive template similarity/capability discovery"
- "format = BASE capability: sole native FormatProcessor path, one deterministic shape per command, no premium/ValKey fallback, no divergent wrapper"
- "site_id is an FCALL arg, not enforced in Lua — caller owns scope isolation"
- "build_key normalizes braced/bare keys, but manual key construction bypasses it"
