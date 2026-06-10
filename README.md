# gNode-CMS

Content Management System extension for the gNode daemon — and the reference
implementation of the gNode signed-extension system. If this extension works,
others will too.

Provides four handler families (22 commands total) that compile into the
gNode daemon at build time:

| Family | Commands | What it does |
|---|---|---|
| Template | `render_template`, `serve_fragment`, `list_templates`, `discover_similar_templates`, `discover_templates_by_capability`, `get_template_capabilities` | Tera-engine rendering with DAG dependency tracking and capability-vector template discovery |
| Content | `content_store`, `content_retrieve`, `template_fragment`, `asset_bundle` | Content storage with minification + compression |
| Asset | `asset_store`, `asset_get`, `asset_delete`, `asset_list`, `manifest_set`, `manifest_get`, `manifest_delete`, `manifest_list` | Manifest-driven asset bundling with background builder |
| Format | `register_format`, `list_formats`, `detect_format`, `convert_format` | Message format registration with JSONSchema validation + auto-detection |

Framework-agnostic: works with any CMS, web framework, or background service
that speaks the gNode wire contract (`gNode/COMMAND_SCHEMA.md`).

---

## How it loads

gNode discovers extensions at build time (`gNode/daemon/build.rs`):

1. `GNODE_EXT_DIR` points at a directory of extension checkouts; this repo is
   one of them. The default install clones it to
   `/opt/geodineum/pro/gNode/gNode-CMS/`.
2. `extension.sig` — a 64-byte raw Ed25519 signature over the canonical
   hashes manifest (extension.yaml + each handler file + each Lua library) —
   is verified against the author public key compiled into
   `gNode/daemon/src/ext_author.rs`. Unsigned or wrongly-signed extensions
   are skipped.
3. Verified handler sources are staged into `OUT_DIR` and compiled into the
   daemon; `functions/gnode_asset.lua` is loaded into ValKey alongside
   gNode's own Lua libraries.

There is nothing to install separately — a default Geodineum install ships
this extension and the daemon builds with it.

---

## Layout

```
extension.yaml            # manifest: commands, handler files, Lua libraries
extension.sig             # Ed25519 signature (see above)
src/handlers/             # template.rs, content.rs, asset.rs, format.rs
src/handlers/manifest.txt # handler-file list consumed by build.rs
functions/gnode_asset.lua # asset-bundling FCALL library
```

## License

Dual-licensed MIT OR Apache-2.0 — see `LICENSE-MIT` and `LICENSE-APACHE`.
