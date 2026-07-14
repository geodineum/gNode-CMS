// Asset Management Command Handlers
//
// Handles: asset_store, asset_get, asset_delete, asset_list,
//          manifest_set, manifest_get, manifest_delete, manifest_list
//
// These commands manage individual assets and bundle manifests via the
// gnode_asset.lua FCALL functions. The background AssetBuilder reads manifests
// and produces compressed bundles.
//
// Two-layer architecture:
//   Layer 1 (Lua): asset CRUD + manifest CRUD + index management
//   Layer 2 (Rust): content processing (minification, compression) + validation

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use serde::Deserialize;
use log::{debug, warn};
use serde_json::{Value, json};
use crate::daemon::{Command, GNodeDaemon};
use crate::GeometricTopology;
use crate::integration::valkey_functions::execute_function;
use crate::integration::{minify_safe, compress_smart, decode_and_decompress};

use super::types::{CommandResult, CommandHandlerFn, AsyncCommandHandlerFn, CommandDescriptor, Lane, parse_parameters};


/// Register all asset management command handlers
pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    // Sync handlers — asset CRUD
    handlers.insert("asset_store".to_string(), handle_asset_store as CommandHandlerFn);
    handlers.insert("asset_get".to_string(), handle_asset_get as CommandHandlerFn);
    handlers.insert("asset_delete".to_string(), handle_asset_delete as CommandHandlerFn);
    handlers.insert("asset_list".to_string(), handle_asset_list as CommandHandlerFn);

    // Sync handlers — manifest CRUD
    handlers.insert("manifest_set".to_string(), handle_manifest_set as CommandHandlerFn);
    handlers.insert("manifest_get".to_string(), handle_manifest_get as CommandHandlerFn);
    handlers.insert("manifest_delete".to_string(), handle_manifest_delete as CommandHandlerFn);
    handlers.insert("manifest_list".to_string(), handle_manifest_list as CommandHandlerFn);

    // Async handlers — asset CRUD
    async_handlers.insert("asset_store".to_string(), handle_asset_store_async as AsyncCommandHandlerFn);
    async_handlers.insert("ASSET_STORE".to_string(), handle_asset_store_async as AsyncCommandHandlerFn);
    async_handlers.insert("asset_get".to_string(), handle_asset_get_async as AsyncCommandHandlerFn);
    async_handlers.insert("ASSET_GET".to_string(), handle_asset_get_async as AsyncCommandHandlerFn);
    async_handlers.insert("asset_delete".to_string(), handle_asset_delete_async as AsyncCommandHandlerFn);
    async_handlers.insert("ASSET_DELETE".to_string(), handle_asset_delete_async as AsyncCommandHandlerFn);
    async_handlers.insert("asset_list".to_string(), handle_asset_list_async as AsyncCommandHandlerFn);
    async_handlers.insert("ASSET_LIST".to_string(), handle_asset_list_async as AsyncCommandHandlerFn);

    // Async handlers — manifest CRUD
    async_handlers.insert("manifest_set".to_string(), handle_manifest_set_async as AsyncCommandHandlerFn);
    async_handlers.insert("MANIFEST_SET".to_string(), handle_manifest_set_async as AsyncCommandHandlerFn);
    async_handlers.insert("manifest_get".to_string(), handle_manifest_get_async as AsyncCommandHandlerFn);
    async_handlers.insert("MANIFEST_GET".to_string(), handle_manifest_get_async as AsyncCommandHandlerFn);
    async_handlers.insert("manifest_delete".to_string(), handle_manifest_delete_async as AsyncCommandHandlerFn);
    async_handlers.insert("MANIFEST_DELETE".to_string(), handle_manifest_delete_async as AsyncCommandHandlerFn);
    async_handlers.insert("manifest_list".to_string(), handle_manifest_list_async as AsyncCommandHandlerFn);
    async_handlers.insert("MANIFEST_LIST".to_string(), handle_manifest_list_async as AsyncCommandHandlerFn);

    // Command descriptors
    descriptors.push(CommandDescriptor {
        name: "asset_store",
        category: "asset",
        description: "Store an asset with optional minification and compression",
        params_schema: json!({
            "type": "object",
            "properties": {
                "key": {"type": "string", "description": "Asset identifier"},
                "content": {"type": "string", "description": "Asset content"},
                "content_type": {"type": "string", "description": "MIME type (auto-detected if omitted)"},
                "ttl": {"type": "integer", "description": "TTL in seconds (0 = no expiry)", "default": 0},
                "minify": {"type": "boolean", "description": "Minify content before storing", "default": false},
                "gzip": {"type": "boolean", "description": "Gzip compress content", "default": false},
                "version": {"type": "string", "description": "Asset version string", "default": "1"}
            },
            "required": ["key", "content"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "asset_id": {"type": "string"},
                "size": {"type": "integer"},
                "content_type": {"type": "string"},
                "etag": {"type": "string"}
            }
        }),
        example: r#"{"cmd":"asset_store","params":{"key":"face_0","content":"<div>Front face</div>","content_type":"text/html"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "asset_get",
        category: "asset",
        description: "Retrieve a stored asset with metadata",
        params_schema: json!({
            "type": "object",
            "properties": {
                "key": {"type": "string", "description": "Asset identifier"},
                "decompress": {"type": "boolean", "description": "Auto-decompress if compressed", "default": true}
            },
            "required": ["key"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "asset_id": {"type": "string"},
                "content": {"type": "string"},
                "content_type": {"type": "string"},
                "version": {"type": "string"},
                "etag": {"type": "string"}
            }
        }),
        example: r#"{"cmd":"asset_get","params":{"key":"face_0"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "asset_delete",
        category: "asset",
        description: "Delete a stored asset and its metadata",
        params_schema: json!({
            "type": "object",
            "properties": {
                "key": {"type": "string", "description": "Asset identifier"}
            },
            "required": ["key"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "deleted": {"type": "boolean"}
            }
        }),
        example: r#"{"cmd":"asset_delete","params":{"key":"face_0"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "asset_list",
        category: "asset",
        description: "List assets for the site with optional content type filter",
        params_schema: json!({
            "type": "object",
            "properties": {
                "content_type": {"type": "string", "description": "Filter by MIME type (optional)"},
                "cursor": {"type": "string", "description": "Pagination cursor", "default": "0"},
                "count": {"type": "integer", "description": "Max results per page", "default": 100}
            }
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "assets": {"type": "array"},
                "count": {"type": "integer"},
                "cursor": {"type": "string"},
                "has_more": {"type": "boolean"}
            }
        }),
        example: r#"{"cmd":"asset_list","params":{"content_type":"text/html"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "manifest_set",
        category: "asset",
        description: "Create or update a bundle manifest definition",
        params_schema: json!({
            "type": "object",
            "properties": {
                "manifest_id": {"type": "string", "description": "Manifest identifier (e.g. 'main')"},
                "manifest": {
                    "type": "object",
                    "description": "Manifest definition",
                    "properties": {
                        "layout": {"type": "string", "description": "Layout type: cube, tesseract, grid, custom"},
                        "type": {"type": "string", "enum": ["inline", "reference", "hybrid"], "default": "inline"},
                        "version": {"type": "string", "default": "1.0.0"},
                        "slot_count": {"type": "integer"},
                        "slots": {"type": "array", "description": "Content slots"},
                        "sections": {"type": "object", "description": "Additional sections (posts, navigation, metadata)"},
                        "build_options": {"type": "object", "description": "Build configuration (compress, ttl, etc.)"}
                    },
                    "required": ["layout"]
                }
            },
            "required": ["manifest_id", "manifest"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "manifest_id": {"type": "string"},
                "updated": {"type": "boolean"},
                "layout": {"type": "string"}
            }
        }),
        example: r#"{"cmd":"manifest_set","params":{"manifest_id":"main","manifest":{"layout":"cube","slot_count":6,"slots":[]}}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "manifest_get",
        category: "asset",
        description: "Retrieve a bundle manifest definition",
        params_schema: json!({
            "type": "object",
            "properties": {
                "manifest_id": {"type": "string", "description": "Manifest identifier"}
            },
            "required": ["manifest_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "manifest": {"type": "object"}
            }
        }),
        example: r#"{"cmd":"manifest_get","params":{"manifest_id":"main"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "manifest_delete",
        category: "asset",
        description: "Delete a bundle manifest and its built bundle",
        params_schema: json!({
            "type": "object",
            "properties": {
                "manifest_id": {"type": "string", "description": "Manifest identifier"}
            },
            "required": ["manifest_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "deleted": {"type": "boolean"}
            }
        }),
        example: r#"{"cmd":"manifest_delete","params":{"manifest_id":"main"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "manifest_list",
        category: "asset",
        description: "List all bundle manifests for the site",
        params_schema: json!({
            "type": "object",
            "properties": {}
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "manifests": {"type": "array"},
                "count": {"type": "integer"}
            }
        }),
        example: r#"{"cmd":"manifest_list","params":{}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
}


// ============================================================================
// Parameter structs
// ============================================================================

#[derive(Debug, Deserialize)]
struct AssetStoreParams {
    key: String,
    content: String,
    content_type: Option<String>,
    ttl: Option<u64>,
    minify: Option<bool>,
    gzip: Option<bool>,
    version: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AssetGetParams {
    key: String,
    decompress: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct AssetDeleteParams {
    key: String,
}

#[derive(Debug, Deserialize)]
struct AssetListParams {
    content_type: Option<String>,
    cursor: Option<String>,
    count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ManifestSetParams {
    manifest_id: String,
    manifest: Value,
}

#[derive(Debug, Deserialize)]
struct ManifestIdParams {
    manifest_id: String,
}


// ============================================================================
// Sync handlers — Asset CRUD
// ============================================================================

/// Handle 'asset_store' — store an asset with optional minification/compression
pub fn handle_asset_store(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling asset_store command: {}", command.id);
    }

    let params = match parse_parameters::<AssetStoreParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    if params.key.is_empty() {
        return CommandResult::error("Missing required parameter: key");
    }

    // Auto-detect content type
    let content_type = params.content_type.unwrap_or_else(|| {
        if params.key.ends_with(".html") || params.content.trim_start().starts_with('<') {
            "text/html".to_string()
        } else if params.key.ends_with(".js") {
            "application/javascript".to_string()
        } else if params.key.ends_with(".css") {
            "text/css".to_string()
        } else if params.key.ends_with(".json") || params.content.trim_start().starts_with('{') {
            "application/json".to_string()
        } else {
            "text/plain".to_string()
        }
    });

    // Process: minify if requested
    let (processed_content, _minify_stats) = if params.minify.unwrap_or(false) {
        let (minified, stats) = minify_safe(&params.content, &content_type);
        (minified, Some(stats))
    } else {
        (params.content.clone(), None)
    };

    // Process: compress if requested
    let (final_content, compressed) = if params.gzip.unwrap_or(false) {
        match compress_smart(&processed_content, &content_type) {
            Ok((compressed_data, should_use, _stats)) => {
                if should_use {
                    (compressed_data, true)
                } else {
                    (processed_content, false)
                }
            },
            Err(e) => {
                warn!("Compression failed for asset '{}': {}. Storing uncompressed.", params.key, e);
                (processed_content, false)
            }
        }
    } else {
        (processed_content, false)
    };

    let ttl = params.ttl.unwrap_or(0);
    let version = params.version.unwrap_or_else(|| "1".to_string());
    let compressed_str = if compressed { "true" } else { "false" };

    match execute_function(
        conn,
        "GNODE_ASSET_STORE",
        &[],
        &[
            &params.key,
            &final_content,
            &content_type,
            &ttl.to_string(),
            site_id,
            &version,
            compressed_str,
        ],
        site_id,
        debug_mode,
    ) {
        Ok(result) => {
            match serde_json::from_str::<Value>(&result) {
                Ok(parsed) => CommandResult::success(parsed),
                Err(_) => CommandResult::success(json!({"ok": true, "asset_id": params.key})),
            }
        },
        Err(e) => CommandResult::error(format!("Failed to store asset: {}", e)),
    }
}

/// Handle 'asset_get' — retrieve an asset with auto-decompression
pub fn handle_asset_get(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling asset_get command: {}", command.id);
    }

    let params = match parse_parameters::<AssetGetParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    if params.key.is_empty() {
        return CommandResult::error("Missing required parameter: key");
    }

    match execute_function(
        conn,
        "GNODE_ASSET_GET",
        &[],
        &[&params.key, site_id],
        site_id,
        debug_mode,
    ) {
        Ok(result) => {
            match serde_json::from_str::<Value>(&result) {
                Ok(mut parsed) => {
                    // Auto-decompress if the asset is compressed and decompress is requested
                    let should_decompress = params.decompress.unwrap_or(true);
                    let is_compressed = parsed.get("compressed")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);

                    if should_decompress && is_compressed {
                        if let Some(content) = parsed.get("content").and_then(|v| v.as_str()) {
                            match decode_and_decompress(content) {
                                Ok(decompressed) => {
                                    parsed["content"] = json!(decompressed);
                                    parsed["decompressed"] = json!(true);
                                },
                                Err(e) => {
                                    warn!("Failed to decompress asset '{}': {}", params.key, e);
                                }
                            }
                        }
                    }
                    CommandResult::success(parsed)
                },
                Err(_) => CommandResult::error("Failed to parse asset response"),
            }
        },
        Err(e) => CommandResult::error(format!("Failed to get asset: {}", e)),
    }
}

/// Handle 'asset_delete' — delete an asset and metadata
pub fn handle_asset_delete(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling asset_delete command: {}", command.id);
    }

    let params = match parse_parameters::<AssetDeleteParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    if params.key.is_empty() {
        return CommandResult::error("Missing required parameter: key");
    }

    match execute_function(
        conn,
        "GNODE_ASSET_DELETE",
        &[],
        &[&params.key, site_id],
        site_id,
        debug_mode,
    ) {
        Ok(result) => {
            match serde_json::from_str::<Value>(&result) {
                Ok(parsed) => CommandResult::success(parsed),
                Err(_) => CommandResult::success(json!({"ok": true, "deleted": true})),
            }
        },
        Err(e) => CommandResult::error(format!("Failed to delete asset: {}", e)),
    }
}

/// Handle 'asset_list' — list assets with optional content type filter
pub fn handle_asset_list(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling asset_list command: {}", command.id);
    }

    let params = match parse_parameters::<AssetListParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    let ct_filter = params.content_type.unwrap_or_default();
    let cursor = params.cursor.unwrap_or_else(|| "0".to_string());
    let count = params.count.unwrap_or(100);

    match execute_function(
        conn,
        "GNODE_ASSET_LIST",
        &[],
        &[site_id, &ct_filter, &cursor, &count.to_string()],
        site_id,
        debug_mode,
    ) {
        Ok(result) => {
            match serde_json::from_str::<Value>(&result) {
                Ok(parsed) => CommandResult::success(parsed),
                Err(_) => CommandResult::success(json!({"ok": true, "assets": [], "count": 0})),
            }
        },
        Err(e) => CommandResult::error(format!("Failed to list assets: {}", e)),
    }
}


// ============================================================================
// Sync handlers — Manifest CRUD
// ============================================================================

/// Handle 'manifest_set' — create or update a bundle manifest
pub fn handle_manifest_set(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling manifest_set command: {}", command.id);
    }

    let params = match parse_parameters::<ManifestSetParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    if params.manifest_id.is_empty() {
        return CommandResult::error("Missing required parameter: manifest_id");
    }

    // Validate the manifest has a layout field
    if params.manifest.get("layout").is_none() {
        return CommandResult::error("Manifest must include a 'layout' field (cube, tesseract, grid, custom)");
    }

    // Serialize manifest to JSON string for Lua
    let manifest_json = match serde_json::to_string(&params.manifest) {
        Ok(json) => json,
        Err(e) => return CommandResult::error(format!("Failed to serialize manifest: {}", e)),
    };

    match execute_function(
        conn,
        "GNODE_ASSET_MANIFEST_SET",
        &[],
        &[&params.manifest_id, &manifest_json, site_id],
        site_id,
        debug_mode,
    ) {
        Ok(result) => {
            match serde_json::from_str::<Value>(&result) {
                Ok(parsed) => CommandResult::success(parsed),
                Err(_) => CommandResult::success(json!({"ok": true, "manifest_id": params.manifest_id})),
            }
        },
        Err(e) => CommandResult::error(format!("Failed to set manifest: {}", e)),
    }
}

/// Handle 'manifest_get' — retrieve a manifest definition
pub fn handle_manifest_get(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling manifest_get command: {}", command.id);
    }

    let params = match parse_parameters::<ManifestIdParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    if params.manifest_id.is_empty() {
        return CommandResult::error("Missing required parameter: manifest_id");
    }

    match execute_function(
        conn,
        "GNODE_ASSET_MANIFEST_GET",
        &[],
        &[&params.manifest_id, site_id],
        site_id,
        debug_mode,
    ) {
        Ok(result) => {
            match serde_json::from_str::<Value>(&result) {
                Ok(parsed) => CommandResult::success(parsed),
                Err(_) => CommandResult::error("Failed to parse manifest response"),
            }
        },
        Err(e) => CommandResult::error(format!("Failed to get manifest: {}", e)),
    }
}

/// Handle 'manifest_delete' — delete a manifest and its built bundle
pub fn handle_manifest_delete(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling manifest_delete command: {}", command.id);
    }

    let params = match parse_parameters::<ManifestIdParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    if params.manifest_id.is_empty() {
        return CommandResult::error("Missing required parameter: manifest_id");
    }

    match execute_function(
        conn,
        "GNODE_ASSET_MANIFEST_DELETE",
        &[],
        &[&params.manifest_id, site_id],
        site_id,
        debug_mode,
    ) {
        Ok(result) => {
            match serde_json::from_str::<Value>(&result) {
                Ok(parsed) => CommandResult::success(parsed),
                Err(_) => CommandResult::success(json!({"ok": true, "deleted": true})),
            }
        },
        Err(e) => CommandResult::error(format!("Failed to delete manifest: {}", e)),
    }
}

/// Handle 'manifest_list' — list all manifests for the site
pub fn handle_manifest_list(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling manifest_list command: {}", command.id);
    }

    match execute_function(
        conn,
        "GNODE_ASSET_MANIFEST_LIST",
        &[],
        &[site_id],
        site_id,
        debug_mode,
    ) {
        Ok(result) => {
            match serde_json::from_str::<Value>(&result) {
                Ok(parsed) => CommandResult::success(parsed),
                Err(_) => CommandResult::success(json!({"ok": true, "manifests": [], "count": 0})),
            }
        },
        Err(e) => CommandResult::error(format!("Failed to list manifests: {}", e)),
    }
}


// ============================================================================
// Async handlers — delegate to sync via connection pool (FCALL is synchronous)
// ============================================================================

pub fn handle_asset_store_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        match crate::integration::connection_manager::get_connection() {
            Ok(mut sync_conn) => {
                handle_asset_store(command, &mut sync_conn, &GNodeDaemon::get_topology_ref(), site_id, debug_mode)
            },
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}

pub fn handle_asset_get_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        match crate::integration::connection_manager::get_connection() {
            Ok(mut sync_conn) => {
                handle_asset_get(command, &mut sync_conn, &GNodeDaemon::get_topology_ref(), site_id, debug_mode)
            },
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}

pub fn handle_asset_delete_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        match crate::integration::connection_manager::get_connection() {
            Ok(mut sync_conn) => {
                handle_asset_delete(command, &mut sync_conn, &GNodeDaemon::get_topology_ref(), site_id, debug_mode)
            },
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}

pub fn handle_asset_list_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        match crate::integration::connection_manager::get_connection() {
            Ok(mut sync_conn) => {
                handle_asset_list(command, &mut sync_conn, &GNodeDaemon::get_topology_ref(), site_id, debug_mode)
            },
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}

pub fn handle_manifest_set_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        match crate::integration::connection_manager::get_connection() {
            Ok(mut sync_conn) => {
                handle_manifest_set(command, &mut sync_conn, &GNodeDaemon::get_topology_ref(), site_id, debug_mode)
            },
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}

pub fn handle_manifest_get_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        match crate::integration::connection_manager::get_connection() {
            Ok(mut sync_conn) => {
                handle_manifest_get(command, &mut sync_conn, &GNodeDaemon::get_topology_ref(), site_id, debug_mode)
            },
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}

pub fn handle_manifest_delete_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        match crate::integration::connection_manager::get_connection() {
            Ok(mut sync_conn) => {
                handle_manifest_delete(command, &mut sync_conn, &GNodeDaemon::get_topology_ref(), site_id, debug_mode)
            },
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}

pub fn handle_manifest_list_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        match crate::integration::connection_manager::get_connection() {
            Ok(mut sync_conn) => {
                handle_manifest_list(command, &mut sync_conn, &GNodeDaemon::get_topology_ref(), site_id, debug_mode)
            },
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}
