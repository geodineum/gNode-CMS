# gNode-CMS — Integration Contract

> Reflects the canonical `remediation/ch1-deep` branch (the consolidated CMS).
> CH1 default companion extension. Source of truth = code in this repo; keep
> this + `CONTRACT.scn.md` in sync with any change to `extension.yaml`, the four
> handlers, or `gnode_asset.lua` (those changes also require a re-sign).

**Role:** Signed Rust extension compiled into the gNode daemon. Provides Content Management System capabilities — template rendering, content storage/caching, asset bundling, and message-format registration — exposed as **23 registered daemon commands** across four handler families (`src/handlers/{template,content,asset,format}.rs`). State lives entirely in ValKey; the extension itself is a stateless command dispatcher.

`name: cms` · `version: 1.0.0` · `rust_feature: cms` (extension.yaml). No `tier:` field — tier is resolved by the daemon, not declared in the manifest. Ships as the default extension with gNode; serves as the reference implementation for the gNode extension system. Discovery: gNode `build.rs` reads `GNODE_EXT_CMS_PATH`, else a sibling clone at `../gNode-CMS/`.

---

## 1. PROVIDES

23 commands registered into the daemon's command dispatcher at init via each handler's `register()` (e.g. `template.rs:28-193`). Each command is invoked through the daemon's normal command path; parameters arrive as a JSON object (`Command{id, parameters}`), results are `CommandResult` (`{success(Value)|error(String)|success_json(String)}`, `template.rs:34,254-259`).

### 1.1 Template commands (`src/handlers/template.rs`)

| Command | Required params | Optional params | Returns |
|---|---|---|---|
| `render_template` | `template_id: string` | `variables: object`, `cache_output: bool` | `{status:ok, html:string, template_id:string, cached:bool}` (template.rs:34,77-79,199-260) |
| `render_string` | `template: string` (ad-hoc Tera source) | `variables: object` | `{html:string}` — renders an unregistered template string; `Lane::Fast`, async-capable (template.rs:85-105,288-350) |
| `serve_fragment` | `key: string` | `hx_trigger: string`, `cache_control: string` | `{status:ok, html:string, headers:object, key:string}` (template.rs:35,85-101,262-333) |
| `list_templates` | — | — | `{matches:[{template_id, service_id, capabilities:object, dimension_count:int}], count:int}` (template.rs:38,104-122,340-388) |
| `discover_similar_templates` | `template_id: string` | `max_distance: float=0.3` | `{template_id, similar_templates:[{template_id, distance:float, similarity:float, capabilities:object}], count:int, max_distance:float}` (template.rs:39,124-146,390-492) |
| `discover_templates_by_capability` | — | `filters: {capability_name:[operator, threshold]}` operators `< > <= >= = ==` | `{matches:[{template_id, capabilities:object}], count:int, filters_applied:object}` (template.rs:40,148-169,501-617) |
| `get_template_capabilities` | `template_id: string` | — | `{template_id, capabilities:object, metadata:object}` (template.rs:41,171-192,619-666) |

### 1.2 Content commands (`src/handlers/content.rs`)

| Command | Required params | Optional params (defaults) | Returns |
|---|---|---|---|
| `content_store` | `key: string`, `content: string` | `content_type: string`, `ttl: u64=3600`, `headers: object`, `minify: bool`, `gzip: bool` | `{stored:bool, key, content_type, original_size:int, stored_size:int, minified:bool, compressed:bool, ttl:u64}` (content.rs:31,47-75,182-316) |
| `content_retrieve` | `key: string` | `decompress: bool=true` | `{content:string, key, retrieved_at:number, metadata?:object, headers?:object}` (content.rs:32,77-101,318-404) |
| `template_fragment` | `template_id: string`, `content: string` | `variables: object`, `ttl: u64=7200` | `{stored:bool, template_id, dependencies:[string], registered_in_topology:bool, ttl:u64}` (content.rs:33,103-132,407-506) |
| `asset_bundle` | `bundle_id: string`, `assets: [string]`, `bundle_type: string` (`js\|css\|mixed`) | `minify: bool`, `ttl: u64=14400` | `{bundled:bool, bundle_id, assets_included:[string], original_size:int, bundled_size:int, compression_ratio:float, ttl:u64}` (content.rs:34,134-162,509-621) |

### 1.3 Asset & manifest commands (`src/handlers/asset.rs`)

| Command | Required params | Optional params (defaults) | Returns |
|---|---|---|---|
| `asset_store` | `key: string`, `content: string` | `content_type: string`, `ttl: u64=0`, `minify: bool`, `gzip: bool`, `version: string=1` | `{ok:bool, asset_id:string, size:int, content_type:string, etag:string}` (asset.rs:39,71-101,339-429) |
| `asset_get` | `key: string` | `decompress: bool=true` | `{ok:bool, asset_id, content:string, content_type, version:string, etag:string, decompressed?:bool}` (asset.rs:40,103-129,432-489) |
| `asset_delete` | `key: string` | — | `{ok:bool, deleted:bool}` (asset.rs:41,131-152,491-528) |
| `asset_list` | — | `content_type: string`, `cursor: string=0`, `count: u64=100` | `{ok:bool, assets:[{id, content_type, size:int, stored_at:int, version, etag, compressed:bool}], count:int, cursor:string, has_more:bool}` (asset.rs:42,154-179,531-567) |
| `manifest_set` | `manifest_id: string`, `manifest: {layout:string (`cube\|tesseract\|grid\|custom`)}` | manifest also: `type: string (inline\|reference\|hybrid)=inline`, `version: string=1.0.0`, `slot_count:int`, `slots:array`, `sections:object`, `build_options:object` | `{ok:bool, manifest_id, updated:bool, layout:string, slot_count:int, stored_at:int}` (asset.rs:45,181-218,575-622) |
| `manifest_get` | `manifest_id: string` | — | `{ok:bool, manifest:{id, type, version, layout, slot_count, slots, sections, build_options, created_at, updated_at}}` (asset.rs:46,220-241,625-661) |
| `manifest_delete` | `manifest_id: string` | — | `{ok:bool, deleted:bool}` (asset.rs:47,243-264,664-700) |
| `manifest_list` | — | — | `{ok:bool, manifests:[{id, layout, slot_count, version, updated_at}], count:int}` (asset.rs:48,266-286,703-730) |

### 1.4 Format commands (`src/handlers/format.rs`)

| Command | Required params | Optional params (defaults) | Returns |
|---|---|---|---|
| `register_format` | `format_definition: object` | — | `{status:"registered", format_name:string}` (async adds `async:true`) (format.rs:49-58,204-216,285) |
| `list_formats` | — | — | `array` of format-definition objects (deterministic; descriptor declares array) (format.rs:59-68,178-186,219-230) |
| `detect_format` | `message: string` | — | `{format_name:string, version:string, confidence:number[0.0-1.0]}` (format.rs:69-78,112-132,233-244) |
| `convert_format` | `source_format: string`, `target_format: string`, `message: string` | `source_version: string=1.0.0`, `target_version: string=1.0.0` | `object` (converted message in target format) (format.rs:79-88,135-175,247-258) |

---

## 2. CONSUMES / REQUIRES

| Dependency | From component | Expected form | Evidence |
|---|---|---|---|
| `GNODE_ASSET_*` Lua functions | ValKey (gnode_asset.lua) | FCALL: `STORE/GET/DELETE/LIST`, `MANIFEST_SET/GET/DELETE/LIST`, `BUILD_STATUS` — see §3.1 | asset.rs:405-420; gnode_asset.lua:83-674 |
| `GNODE_CACHE_SET` / `GNODE_CACHE_GET` | gNode daemon (core Lua) | `FCALL GNODE_CACHE_SET 0 key content ttl site_id`; `FCALL GNODE_CACHE_GET 0 key site_id` → JSON string `{ok:bool, ...}` | template.rs:244-251,288-298; content.rs:275-301,341-351 |
| `GeometricTopology` (read-lock) | gNode daemon (topology) | `topology.read().ok()?` → guard; `services: HashMap<String, Service{point:FixedVector(8D), metadata:object}>`, `capability_dimensions: Vec<String>`, `dimensions: usize`; reached via `GNodeDaemon::get_topology_ref()` | template.rs:354-378,423-427,522-527 |
| `Command` struct | gNode daemon (command module) | `{id:string, parameters: JsonValue (Map<String,Value>)}`; `parameters.get(k) -> Option<Value>` | template.rs:199-222,332-338 |
| `minify_safe`, `compress_smart`, `decode_and_decompress` | `crate::integration` | `minify_safe(content, ct) -> (string, {minified_size, reduction_ratio})`; `compress_smart(content, ct) -> Result<(string, should_use:bool, {algorithm, compressed_size, compression_ratio})>`; `decode_and_decompress(&str) -> Result<String>` | content.rs:20,216-242,368-377; asset.rs:27,375-399 |
| `template_renderer`, `current_timestamp` | `crate::integration` (Tera, stream_utils) | template registration/rendering; timestamping | template.rs:230-237; content.rs:216-242 |
| `GNodeSettings` | `crate::config` | default config for template rendering | template.rs:225; content.rs:434 |
| Format processor (base-tier native) | `crate::daemon::GNodeDaemon::get_format_processor_ref()` | SOLE backing for all four format commands — format is a **BASE** capability, no premium gNode-BROKER dependency. Custom format definitions persist to ValKey via the processor; detect/convert/list are in-memory native compute. If the processor is uninitialized the command returns a hard error (`"Format processor not initialized"`) — no fallback. | format.rs:97-109,112-121,178-186 |

FCALL is dispatched through `crate::integration::valkey_functions::execute_function` / `execute_function_async` (sync + async variants; asset.rs:26, template.rs:244-251).

---

## 2a. Configuration (`config_schema.yaml`, component `gnode_cms`)

Published to the daemon's config-schema surface; operator-settable per the schema's `mutable` flags.

| Key | Type | Default | Mutable | Meaning |
|---|---|---|---|---|
| `CMS_TEMPLATE_CACHE_SIZE` | int | `256` | yes | In-process template-render cache capacity (entries). |
| `CMS_MANIFEST_REBUILD_INTERVAL_SECS` | int | `30` | yes | Background manifest-builder poll interval (lower = faster asset-change detection, more CPU). |
| `CMS_COMPRESSION_LEVEL` | enum `off\|fast\|balanced\|max` | `balanced` | yes | Content-store compression preset. |
| `CMS_MINIFY` | bool | `true` | yes | HTML/CSS/JS minification on store. |

---

## 3. Wire formats

All persistent state is in ValKey. **Every key is site-scoped via a hash-tag `{site_id}`** for cluster co-location. `site_id` is passed to Lua as an FCALL argument — it is NOT enforced at the Lua layer; the caller is responsible for scope isolation (gnode_asset.lua; limitation below).

### 3.1 `GNODE_ASSET_*` FCALL surface (gnode_asset.lua:83-674)

```
FCALL <fn> numkeys [key ...] arg1 arg2 ...
GNODE_ASSET_STORE(asset_id, content, content_type, ttl:u64, site_id, version?, compressed?:bool)
GNODE_ASSET_GET(asset_id, site_id)
GNODE_ASSET_DELETE(asset_id, site_id)
GNODE_ASSET_LIST(site_id, content_type_filter?, cursor?, count?:u64)
GNODE_ASSET_MANIFEST_SET(manifest_id, manifest_json, site_id)
GNODE_ASSET_MANIFEST_GET(manifest_id, site_id)
GNODE_ASSET_MANIFEST_DELETE(manifest_id, site_id)
GNODE_ASSET_MANIFEST_LIST(site_id)
GNODE_ASSET_BUILD_STATUS(manifest_id, site_id)
```
Lua additionally registers `GNODE_ASSET_BUILD_STATUS` and `GNODE_ASSET_EXISTS` — a superset; not every registered fn is called by the handlers.

### 3.2 ValKey key patterns (evidence: gnode_asset.lua:12-17,107-116,173-174,232-240,314-350,404-427,468-479,526-542,575-602)

| Key | Type | Contents |
|---|---|---|
| `{site_id}:asset:{asset_id}` | STRING | asset content |
| `{site_id}:asset:{asset_id}:meta` | HASH | `id, ct, sz, cmp, sa, ua, v, etag` |
| `{site_id}:asset:manifests` | SET | manifest IDs |
| `{site_id}:asset:manifest:{manifest_id}` | HASH | `id, type, v, layout, sc, slots, sections, bo, ca, ua` |
| `{site_id}:gnode:bundle:{manifest_id}` | STRING | gzip bundle (written by daemon background builder) |
| `{site_id}:gnode:bundle:{manifest_id}:meta` | HASH | `ba, sz, csz, ac, bv` |
| `{site_id}:template:{template_id}` | STRING | template content |
| `{site_id}:template:{template_id}:meta` | HASH | `type, stored_at, registered_in_topology, dependencies, variables` |
| `{site_id}:metrics:asset` | HASH | `stores, gets, misses, deletes, total_bytes_stored, manifest_writes, manifest_deletes` |

### 3.3 HASH field schemas (abbreviated in Lua, expanded in Rust JSON responses)

- **Asset meta** (gnode_asset.lua:125-133,185-205): `id`(string), `ct`(content_type), `sz`(size), `cmp`(compressed bool), `sa`(stored_at int), `ua`(updated_at int), `v`(version), `etag`.
- **Manifest** (gnode_asset.lua:417-427,481-499): `id`, `type`(`inline\|reference\|hybrid`), `v`(version), `layout`(`cube\|tesseract\|grid\|custom`), `sc`(slot_count), `slots`(JSON array), `sections`(JSON object), `bo`(build_options JSON), `ca`(created_at), `ua`(updated_at). Rust responses reconstruct full names (`version, slot_count, build_options, created_at, updated_at`) — mapping is implicit (see Adherence).

### 3.4 `GNODE_CACHE_SET` / `GNODE_CACHE_GET` response
JSON string `{ok:bool, ...}`, or an error on parse failure (template.rs:244-251; content.rs:275-301).

### 3.5 Template topology schema
Service registered as `service_id = "template:{template_id}"`, `point = FixedVector` (8D capability coords), `metadata = object` (template.rs:364-375,429-436,537-538).

### 3.6 Command parameter encoding (JSON)
Template `variables` as objects; `manifest` as object with nested arrays/objects (JSON-encoded into Lua HASH fields); capability `filters` as `{capability_name:[operator, threshold]}`; `format_definition` as nested JSON (template.rs:213-217; asset.rs:323-326; format.rs:98-102).

---

## 4. Public types

| Type | Shape | Evidence |
|---|---|---|
| `CommandResult` | `{success(Value)->Self, error(String)->Self, success_json(String)->Self}` | template.rs:34,254-259 |
| `CommandDescriptor` | `{lane:Lane, name:&str, category:&str, description:&str, params_schema:Value, returns_schema:Value, example:&str, async_capable:bool}` | template.rs:58-79 |
| `Lane` (enum) | `Fast` (descriptor lane field) | template.rs:59 |
| `Command` (from `crate::daemon`) | `{id:string, parameters:Value (Map)}` | template.rs:199-209 |
| `GeometricTopology` | `{services:HashMap<String,Service>, capability_dimensions:Vec<String>, dimensions:usize}` | template.rs:354-375 |
| `Service` (topology) | `{point:FixedVector, metadata:object}` | template.rs:368-375 |

---

## 5. Example

```jsonc
// Store a compressed CSS asset (asset_store)
{ "id": "asset_store",
  "parameters": { "key": "main.css", "content": "<css>",
                  "content_type": "text/css", "minify": true, "gzip": true,
                  "version": "2" } }
// -> {ok:true, asset_id:"main.css", size:..., content_type:"text/css", etag:"..."}

// Define a bundle manifest (manifest_set)
{ "id": "manifest_set",
  "parameters": { "manifest_id": "home", "manifest": {
                  "layout": "cube", "type": "inline", "version": "1.0.0",
                  "slot_count": 6, "slots": [], "sections": {} } } }
// -> {ok:true, manifest_id:"home", updated:false, layout:"cube", slot_count:6, stored_at:...}
```

---

## 6. Adherence (known mismatches / latent risks)

The ecosystem cross-check confirms gNode-CMS **ADHERES** on its two load-bearing external contracts:
- **Signed-extension scheme** — `extension.sig` verifies against `AUTHOR_PUBKEY` (fingerprint `2ff9966fcad06b6d`) over a canonical hash of `extension.yaml` + handler_files + `gnode_asset` lua lib (ext_verify.rs:116-154). CMS ships exactly the files the canonical form covers.
- **`GNODE_ASSET_*` FCALL names** — every function the Rust handlers invoke (STORE, GET, DELETE, LIST, MANIFEST_SET/GET/DELETE/LIST) is registered in gnode_asset.lua; Lua registers a superset (`BUILD_STATUS`, `EXISTS`).

Internal (within-component) risks observed in source — none currently break live interop, all flagged for hardening:

1. **Manual vs `build_key` key namespacing.** Lua `build_key` (gnode_asset.lua:52-69) normalizes both braced and bare `site_id` keys, but Rust handlers also construct keys manually for cache paths (e.g. template.rs:241 `template:{}:output`, content.rs:290,471) that do NOT pass through `build_key`. Risk: inconsistent key namespacing if these diverge.
2. **Implicit Lua↔Rust field mapping.** Manifest/asset HASH fields are abbreviated in Lua (`v, sc, bo, ca, ua`) and reconstructed to full names in Rust JSON. The mapping is implicit and unvalidated (gnode_asset.lua:417-427,481-499).
3. **No template_id / bundle_id namespace check.** `template_fragment` and `asset_bundle` both store via cache functions with auto-generated keys; no validation prevents collision (content.rs:422-425 vs 510-533).
4. **Format commands: single deterministic native path (former divergence retired).** The premium gNode-BROKER FCALL path and any ValKey fallback have been removed; the four format commands are served solely by the base-tier native FormatProcessor. Each returns one deterministic shape: `register_format` → `{status:"registered", format_name}` (async adds `"async":true`, format.rs:212,285); `detect_format` → `{format_name, version, confidence}`; `convert_format` → the converted message value; `list_formats` → an array of format definitions. If the processor is uninitialized the command returns a hard error — there is no divergent `{status,result,timestamp}` wrapper and no `#[cfg(feature="cms")]` gate.
5. **Stale-topology reads.** Discovery commands read the topology under a read-lock with no staleness check or refresh trigger; async service registration may lag (template.rs:423-427).
6. **No asset size validation.** Neither Lua (gnode_asset.lua:106-117) nor the Rust call site (asset.rs:405-428) validates content size before FCALL — oversized assets may silently succeed/truncate.
7. **site_id not enforced at Lua.** `site_id` is a plain FCALL argument; scope isolation is the caller's responsibility.

(For the ecosystem-wide COMMS / face_mapping / health-metrics drift items, see those components' contracts — they do not involve gNode-CMS directly.)
