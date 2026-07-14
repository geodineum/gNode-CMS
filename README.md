<p align="center">
  <a href="https://geodineum.com">
    <img src=".github/geodineum-logo.png" alt="Geodineum" width="128">
  </a>
</p>

# gNode-CMS

The default content-management extension for the gNode daemon: Tera template
rendering, content storage, asset bundling, and message-format handling, added to
gNode as signed commands.

Built by **Niels Erik Toren** · signed gNode extension (compiles into the daemon); version in `extension.yaml`

---

## What it is

gNode-CMS is a **signed gNode extension**, not a standalone program. It has no
binary of its own - its Rust handlers and Lua library are verified against the
author key and compiled into the gNode daemon through the signed-extension
pipeline. It ships loaded by default with every install and is the reference
implementation of the gNode extension system.

Once loaded, it adds a content-management surface to the daemon: rendering
templates, storing and bundling content and assets, and registering message
formats - all reached the same way as any gNode command, over the ValKey wire.

## Public build surface

What you build against is the **command and Lua surface gNode-CMS adds to the
daemon** - 23 commands across template, content, asset, and format domains, plus
the `GNODE_ASSET_*` FCALL library. You invoke them through the gNode wire
protocol (an `XADD` to the unified stream, or via `gNode-Client`), exactly like a
base command; you never call this repository directly.

That surface has one catalogue, and it is gNode's:
**`gNode/COMMAND_SCHEMA.md`** lists every CMS command and every `GNODE_ASSET_*`
function with its parameters - and gNode's own schema checker verifies it against
a CMS-loaded build, so the catalogue can't silently drift. This extension's
integration specifics live in **[`CONTRACT.md`](CONTRACT.md)**.

**Internal** - the handler sources under `src/handlers/` and
`functions/gnode_asset.lua` are implementation; they are covered by the
extension signature and change together with a re-sign.

## Capabilities

- **Template rendering** - Tera templates with variables, ad-hoc string
  rendering, cached fragments, and capability-vector template discovery.
- **Content storage** - store and retrieve content with optional minification
  and gzip compression.
- **Asset & manifest management** - asset CRUD and bundle-manifest CRUD, with the
  atomic storage operations running as ValKey server-side functions.
- **Message formats** - register custom formats with detection patterns, then
  auto-detect and convert between them, served by gNode's native format engine.

## Contract

The precise integration surface - each command's parameters and returns, the
`GNODE_ASSET_*` FCALL contract, the ValKey key patterns, and the configuration
schema - is in **[`CONTRACT.md`](CONTRACT.md)**. Agents should prime from
**[`CONTRACT.scn.md`](CONTRACT.scn.md)**. The full command catalogue is
`gNode/COMMAND_SCHEMA.md`.

## Quick start

gNode-CMS is already loaded on any standard install - there is nothing to
install. Confirm the daemon sees it, then call a CMS command over the wire (the
braces are a literal ValKey cluster hash-tag):

```sh
AUTH="$(sudo cat /etc/geodineum/credentials/valkey.password)"

# Is the extension loaded?
REDISCLI_AUTH="$AUTH" redis-cli -p 47445 XADD '{mysite}:gnode:unified:production' '*' \
    id req-1 t c c extension_list p '{}' ss mysite sn node-1 ts 1718000000000
REDISCLI_AUTH="$AUTH" redis-cli -p 47445 GET '{mysite}:res:req-1'   # lists "cms"

# Store a minified, compressed asset (asset_store) the same way
REDISCLI_AUTH="$AUTH" redis-cli -p 47445 XADD '{mysite}:gnode:unified:production' '*' \
    id req-2 t c c asset_store ss mysite sn node-1 ts 1718000000000 \
    p '{"key":"main.css","content":"body{color:#111}","content_type":"text/css","minify":true,"gzip":true}'
```

Each command's parameters are in `gNode/COMMAND_SCHEMA.md`. In practice the
WordPress themes reach these commands through `gCore` and `gNode-Client` rather
than raw `XADD`.

## Limits worth knowing

- **Not standalone.** gNode-CMS compiles into the gNode daemon via the
  signed-extension pipeline; there is no separate binary and no Cargo feature to
  toggle.
- **Signature-gated.** The daemon loads it only if `extension.sig` verifies
  against the baked-in author key; a modified or unsigned extension is skipped and
  the daemon runs with a reduced surface. Any change to a signed file must be
  re-signed in the same commit.
- **Loaded by default, opt-out only.** It ships with every install; set
  `GEODINEUM_SKIP_CMS=true` at build time to leave it out.
- **Format commands are a base capability** - there is no premium gate; the four
  format commands are served by gNode's native format engine.

## Collaborate

Contributions are welcome. Open issues and pick up work on the ecosystem board
at [geodineum.com](https://geodineum.com); issues tagged `good-first-issue` are
a good place to start.

- Fork, branch, and open a pull request against `main`.
- Any change to a wire contract must update **both** `CONTRACT.md` and
  `CONTRACT.scn.md` in the same commit.
- A change to a signed extension must be re-signed in the same commit.

## Author & support

Built by **Niels Erik Toren**.

If you want to support the work:

| Currency | Address |
|---|---|
| Bitcoin (BTC) | `bc1qwf78fjgapt2gcts4mwf3gnfkclvqgtlg4gpu4d` |
| Ethereum (ETH) | `0xf38b517Dd2005d93E0BDc1e9807665074c5eC731` / `nierto.eth` |
| Monero (XMR) | `8BPaSoq1pEJH4LgbGNQ92kFJA3oi2frE4igHvdP9Lz2giwhFo2VnNvGT8XABYasjtoVY2Qb3LVHv6CP3qwcJ8UnyRtjWRZ5` |

## Disclaimer

This software is provided **"as is"**, without warranty of any kind, express or
implied. Use of this software is entirely at your own risk. In no event shall the
author or contributors be held liable for any damages arising from the use or
inability to use this software.

## License

Licensed under either of

* [Apache License, Version 2.0](LICENSE-APACHE)
* [MIT License](LICENSE-MIT)

at your option.
