// Content Management Command Handlers
//
// Handles: content_store, content_retrieve, template_fragment, asset_bundle
// These provide content storage, retrieval, and bundling with ValKey caching.

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
use crate::daemon::Command;
use crate::GeometricTopology;
use crate::integration::valkey_functions::{execute_function, execute_function_async};
use crate::integration::processor::stream_utils::current_timestamp;
use crate::integration::{minify_safe, compress_smart, decode_and_decompress};

use super::types::{CommandResult, CommandHandlerFn, AsyncCommandHandlerFn, CommandDescriptor, Lane, parse_parameters};

/// Register all content command handlers
pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    // Sync handlers
    handlers.insert("content_store".to_string(), handle_content_store as CommandHandlerFn);
    handlers.insert("content_retrieve".to_string(), handle_content_retrieve as CommandHandlerFn);
    handlers.insert("template_fragment".to_string(), handle_template_fragment as CommandHandlerFn);
    handlers.insert("asset_bundle".to_string(), handle_asset_bundle as CommandHandlerFn);

    // Async handlers
    async_handlers.insert("content_store".to_string(), handle_content_store_async as AsyncCommandHandlerFn);
    async_handlers.insert("CONTENT_STORE".to_string(), handle_content_store_async as AsyncCommandHandlerFn);
    async_handlers.insert("content_retrieve".to_string(), handle_content_retrieve_async as AsyncCommandHandlerFn);
    async_handlers.insert("CONTENT_RETRIEVE".to_string(), handle_content_retrieve_async as AsyncCommandHandlerFn);
    async_handlers.insert("template_fragment".to_string(), handle_template_fragment_async as AsyncCommandHandlerFn);
    async_handlers.insert("TEMPLATE_FRAGMENT".to_string(), handle_template_fragment_async as AsyncCommandHandlerFn);
    async_handlers.insert("asset_bundle".to_string(), handle_asset_bundle_async as AsyncCommandHandlerFn);
    async_handlers.insert("ASSET_BUNDLE".to_string(), handle_asset_bundle_async as AsyncCommandHandlerFn);

    // Command descriptors
    descriptors.push(CommandDescriptor {
        name: "content_store",
        category: "content",
        description: "Store content with metadata and optional compression",
        params_schema: json!({
            "type": "object",
            "properties": {
                "key": {"type": "string", "description": "Storage key for the content"},
                "content": {"type": "string", "description": "The content to store"},
                "content_type": {"type": "string", "description": "MIME type (e.g. text/html)", "default": "auto-detected"},
                "ttl": {"type": "integer", "description": "Time-to-live in seconds", "default": 3600},
                "compress": {"type": "boolean", "description": "Enable gzip compression", "default": false}
            },
            "required": ["key", "content"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "stored": {"type": "boolean"},
                "key": {"type": "string"},
                "content_type": {"type": "string"},
                "original_size": {"type": "integer"},
                "stored_size": {"type": "integer"}
            }
        }),
        example: r#"{"cmd":"content_store","params":{"key":"page/home","content":"<h1>Hello</h1>","content_type":"text/html","ttl":3600}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "content_retrieve",
        category: "content",
        description: "Retrieve stored content by key",
        params_schema: json!({
            "type": "object",
            "properties": {
                "key": {"type": "string", "description": "Storage key to retrieve"},
                "decompress": {"type": "boolean", "description": "Auto-decompress if compressed", "default": true}
            },
            "required": ["key"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "content": {"type": "string"},
                "key": {"type": "string"},
                "metadata": {"type": "object"},
                "retrieved_at": {"type": "number"}
            }
        }),
        example: r#"{"cmd":"content_retrieve","params":{"key":"page/home"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "template_fragment",
        category: "content",
        description: "Render a template fragment with variables",
        params_schema: json!({
            "type": "object",
            "properties": {
                "template_id": {"type": "string", "description": "Template identifier"},
                "variables": {
                    "type": "object",
                    "description": "Template variables for rendering",
                    "additionalProperties": {"type": "string"}
                },
                "ttl": {"type": "integer", "description": "Cache TTL in seconds", "default": 7200}
            },
            "required": ["template_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "stored": {"type": "boolean"},
                "template_id": {"type": "string"},
                "dependencies": {"type": "array", "items": {"type": "string"}},
                "registered_in_topology": {"type": "boolean"}
            }
        }),
        example: r#"{"cmd":"template_fragment","params":{"template_id":"header","variables":{"title":"Home"},"ttl":3600}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "asset_bundle",
        category: "content",
        description: "Bundle multiple assets into a single response",
        params_schema: json!({
            "type": "object",
            "properties": {
                "assets": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Asset keys to bundle"
                }
            },
            "required": ["assets"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "bundled": {"type": "boolean"},
                "bundle_id": {"type": "string"},
                "assets_bundled": {"type": "integer"},
                "original_size": {"type": "integer"},
                "stored_size": {"type": "integer"}
            }
        }),
        example: r#"{"cmd":"asset_bundle","params":{"assets":["styles/main.css","styles/theme.css"]}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
}

// =========================================================================
// Sync handlers
// =========================================================================

/// Parameters for content_store command
#[derive(Debug, Deserialize)]
struct ContentStoreParams {
    key: String,
    content: String,
    content_type: Option<String>,
    ttl: Option<u64>,
    headers: Option<HashMap<String, String>>,
    minify: Option<bool>,
    gzip: Option<bool>,
}

/// Handle 'content_store' command - Store HTML/JS/CSS content with metadata
pub fn handle_content_store(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling content_store command: {}", command.id);
    }
    
    // Parse parameters
    let params = match parse_parameters::<ContentStoreParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };
    
    // Validate content type
    let content_type = params.content_type.unwrap_or_else(|| {
        // Auto-detect content type from key or content
        if params.key.ends_with(".html") || params.content.trim_start().starts_with('<') {
            "text/html".to_string()
        } else if params.key.ends_with(".js") || params.content.contains("function") || params.content.contains("const") {
            "application/javascript".to_string()
        } else if params.key.ends_with(".css") || params.content.contains("@") || params.content.contains("{") {
            "text/css".to_string()
        } else if params.key.ends_with(".json") || params.content.trim_start().starts_with('{') {
            "application/json".to_string()
        } else {
            "text/plain".to_string()
        }
    });
    
    // Process content (minify if requested)
    let (processed_content, minify_stats) = if params.minify.unwrap_or(false) {
        let (minified, stats) = minify_safe(&params.content, &content_type);
        (minified, Some(stats))
    } else {
        (params.content.clone(), None)
    };

    // Compress content (if requested)
    let (final_content, compression_stats) = if params.gzip.unwrap_or(false) {
        match compress_smart(&processed_content, &content_type) {
            Ok((compressed, should_decompress, stats)) => {
                if should_decompress {
                    (compressed, Some(stats))
                } else {
                    // Compression not beneficial or not applied
                    (processed_content.clone(), None)
                }
            },
            Err(e) => {
                warn!("Compression failed: {}. Storing uncompressed.", e);
                (processed_content.clone(), None)
            }
        }
    } else {
        (processed_content, None)
    };

    // Build metadata
    let mut metadata = HashMap::new();
    metadata.insert("content_type".to_string(), content_type.clone());
    metadata.insert("original_size".to_string(), params.content.len().to_string());
    metadata.insert("stored_size".to_string(), final_content.len().to_string());
    metadata.insert("stored_at".to_string(), current_timestamp().to_string());

    // Add minification metadata if applied
    if let Some(stats) = minify_stats {
        metadata.insert("minified".to_string(), "true".to_string());
        metadata.insert("minified_size".to_string(), stats.minified_size.to_string());
        metadata.insert("minify_reduction_ratio".to_string(), format!("{:.3}", stats.reduction_ratio));
    }

    // Add compression metadata if applied
    if let Some(stats) = compression_stats {
        metadata.insert("compressed".to_string(), "true".to_string());
        metadata.insert("compression_algorithm".to_string(), stats.algorithm.clone());
        metadata.insert("compressed_size".to_string(), stats.compressed_size.to_string());
        metadata.insert("compression_ratio".to_string(), format!("{:.3}", stats.compression_ratio));
    }
    
    // Add custom headers
    if let Some(headers) = params.headers {
        for (key, value) in headers {
            metadata.insert(format!("header_{}", key.to_lowercase()), value);
        }
    }
    
    // Store content using GNODE_CACHE_SET function
    let ttl = params.ttl.unwrap_or(3600); // Default 1 hour

    match execute_function(
        conn,
        "GNODE_CACHE_SET",
        &[],
        &[
            &params.key,
            &final_content,
            &ttl.to_string(),
            site_id,
        ],
        site_id,
        debug_mode
    ) {
        Ok(_) => {
            // Store metadata separately
            let metadata_key = format!("{}:meta", params.key);
            let metadata_json = serde_json::to_string(&metadata)
                .unwrap_or_else(|_| "{}".to_string());
            
            let _ = execute_function(
                conn,
                "GNODE_CACHE_SET",
                &[],
                &[&metadata_key, &metadata_json, &ttl.to_string(), site_id],
                site_id,
                debug_mode
            );
            
            CommandResult::success(json!({
                "stored": true,
                "key": params.key,
                "content_type": content_type,
                "original_size": params.content.len(),
                "stored_size": final_content.len(),
                "minified": metadata.contains_key("minified"),
                "compressed": metadata.contains_key("compressed"),
                "ttl": ttl
            }))
        },
        Err(e) => CommandResult::error(format!("Failed to store content: {}", e))
    }
}

/// Handle 'content_retrieve' command - Retrieve content with proper headers
pub fn handle_content_retrieve(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling content_retrieve command: {}", command.id);
    }
    
    // Parse parameters
    let key = command.parameters.get("key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CommandResult::error("Missing 'key' parameter"));
    
    let key = match key {
        Ok(k) => k.to_string(),
        Err(e) => return e,
    };
    
    // Retrieve content
    let stored_content = match execute_function(
        conn,
        "GNODE_CACHE_GET",
        &[],
        &[&key, site_id],
        site_id,
        debug_mode
    ) {
        Ok(content) => content,
        Err(e) => return CommandResult::error(format!("Content not found: {}", e))
    };

    // Retrieve metadata
    let metadata_key = format!("{}:meta", key);
    let metadata = execute_function(
        conn,
        "GNODE_CACHE_GET",
        &[],
        &[&metadata_key, site_id],
        site_id,
        debug_mode
    ).ok()
    .and_then(|meta_json| serde_json::from_str::<HashMap<String, String>>(&meta_json).ok())
    .unwrap_or_default();

    // Decompress if needed
    let content = if metadata.get("compressed").map(|v| v == "true").unwrap_or(false) {
        match decode_and_decompress(&stored_content) {
            Ok(decompressed) => decompressed,
            Err(e) => {
                warn!("Decompression failed: {}. Returning stored content.", e);
                stored_content
            }
        }
    } else {
        stored_content
    };

    // Build response with content and headers
    let mut response = json!({
        "content": content,
        "key": key,
        "retrieved_at": current_timestamp()
    });
    
    // Add metadata to response
    if !metadata.is_empty() {
        response["metadata"] = serde_json::to_value(&metadata).unwrap_or_default();
        
        // Extract headers for convenience
        let mut headers = HashMap::new();
        for (meta_key, meta_value) in &metadata {
            if let Some(header_name) = meta_key.strip_prefix("header_") {
                headers.insert(header_name.to_string(), meta_value.clone());
            }
        }
        
        if !headers.is_empty() {
            response["headers"] = serde_json::to_value(&headers).unwrap_or_default();
        }
    }
    
    CommandResult::success(response)
}


/// Handle 'template_fragment' command - Cache template fragments with dependencies (Enhanced with Tera)
pub fn handle_template_fragment(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling template_fragment command: {}", command.id);
    }

    // Parse parameters
    #[derive(Debug, Deserialize)]
    struct TemplateParams {
        template_id: String,
        content: String,
        variables: Option<HashMap<String, String>>,
        ttl: Option<u64>,
    }

    let params = match parse_parameters::<TemplateParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    // Get config for template_renderer
    let config = crate::config::GNodeSettings::default();

    // Register template with Tera engine + topology (validates syntax, tracks dependencies, detects cycles)
    let extracted_partials = match crate::integration::template_renderer::register_template(
        &params.template_id,
        &params.content,
        &config
    ) {
        Ok(partials) => partials,
        Err(e) => return CommandResult::error(format!("Failed to register template with Tera: {}", e)),
    };

    if debug_mode {
        debug!(
            "Template '{}' registered in Tera with {} dependencies: {:?}",
            params.template_id,
            extracted_partials.len(),
            extracted_partials
        );
    }

    // Build template metadata
    let mut metadata = HashMap::new();
    metadata.insert("type".to_string(), "template_fragment".to_string());
    metadata.insert("stored_at".to_string(), current_timestamp().to_string());
    metadata.insert("registered_in_topology".to_string(), "true".to_string());

    // Use extracted partials as authoritative source of dependencies
    if !extracted_partials.is_empty() {
        metadata.insert("dependencies".to_string(), extracted_partials.join(","));
    }

    if let Some(vars) = &params.variables {
        metadata.insert("variables".to_string(), serde_json::to_string(vars).unwrap_or_default());
    }

    // Store template content in cache (for redundancy)
    let template_key = format!("template:{}", params.template_id);
    let ttl = params.ttl.unwrap_or(7200); // Default 2 hours for templates

    match execute_function(
        conn,
        "GNODE_CACHE_SET",
        &[],
        &[&template_key, &params.content, &ttl.to_string(), site_id],
        site_id,
        debug_mode
    ) {
        Ok(_) => {
            // Store metadata
            let metadata_key = format!("{}:meta", template_key);
            let metadata_json = serde_json::to_string(&metadata).unwrap_or_default();

            let _ = execute_function(
                conn,
                "GNODE_CACHE_SET",
                &[],
                &[&metadata_key, &metadata_json, &ttl.to_string(), site_id],
                site_id,
                debug_mode
            );

            CommandResult::success(json!({
                "stored": true,
                "template_id": params.template_id,
                "dependencies": extracted_partials,
                "registered_in_topology": true,
                "ttl": ttl
            }))
        },
        Err(e) => CommandResult::error(format!("Failed to store template in cache: {}", e))
    }
}

/// Handle 'asset_bundle' command - Bundle and cache multiple assets
pub fn handle_asset_bundle(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling asset_bundle command: {}", command.id);
    }
    
    // Parse parameters
    #[derive(Debug, Deserialize)]
    struct BundleParams {
        bundle_id: String,
        assets: Vec<String>, // List of asset keys to bundle
        bundle_type: String, // "js", "css", or "mixed"
        minify: Option<bool>,
        ttl: Option<u64>,
    }
    
    let params = match parse_parameters::<BundleParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };
    
    // Retrieve and bundle assets
    let mut bundled_content = String::new();
    let mut total_size = 0;
    let mut successful_assets = Vec::new();
    
    for asset_key in &params.assets {
        match execute_function(
            conn,
            "GNODE_CACHE_GET",
            &[],
            &[asset_key, site_id],
            site_id,
            debug_mode
        ) {
            Ok(content) => {
                total_size += content.len();
                
                // Add content with appropriate separators
                match params.bundle_type.as_str() {
                    "css" => {
                        bundled_content.push_str(&content);
                        bundled_content.push('\n');
                    },
                    "js" => {
                        bundled_content.push_str(&content);
                        bundled_content.push_str(";\n");
                    },
                    _ => {
                        bundled_content.push_str(&content);
                        bundled_content.push('\n');
                    }
                }
                
                successful_assets.push(asset_key.clone());
            },
            Err(_) => {
                // Asset not found, continue with others
                continue;
            }
        }
    }
    
    if successful_assets.is_empty() {
        return CommandResult::error("No assets found to bundle");
    }
    
    // Minify if requested
    let final_content = if params.minify.unwrap_or(false) {
        let content_type = match params.bundle_type.as_str() {
            "css" => "text/css",
            "js" => "application/javascript",
            _ => "text/plain",
        };
        let (minified, _stats) = minify_safe(&bundled_content, content_type);
        minified
    } else {
        bundled_content
    };
    
    // Store bundled asset
    let bundle_key = format!("bundle:{}", params.bundle_id);
    let ttl = params.ttl.unwrap_or(14400); // Default 4 hours for bundles
    
    match execute_function(
        conn,
        "GNODE_CACHE_SET",
        &[],
        &[&bundle_key, &final_content, &ttl.to_string(), site_id],
        site_id,
        debug_mode
    ) {
        Ok(_) => {
            CommandResult::success(json!({
                "bundled": true,
                "bundle_id": params.bundle_id,
                "assets_included": successful_assets,
                "original_size": total_size,
                "bundled_size": final_content.len(),
                "compression_ratio": if total_size > 0 { 
                    (total_size as f64 - final_content.len() as f64) / total_size as f64 
                } else { 0.0 },
                "ttl": ttl
            }))
        },
        Err(e) => CommandResult::error(format!("Failed to store bundle: {}", e))
    }
}

// =========================================================================
// Async handlers
// =========================================================================

/// Async version of handle_content_store (Phase 4: Async Architecture)
/// Non-blocking content storage with ValKey caching
pub fn handle_content_store_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async content_store command: {}", command.id);
        }

        // Extract parameters
        let key = match command.parameters.get("key").and_then(|v| v.as_str()) {
            Some(k) if !k.is_empty() => k.to_string(),
            _ => return CommandResult::error("Missing or empty 'key' parameter"),
        };

        let content = match command.parameters.get("content").and_then(|v| v.as_str()) {
            Some(c) => c.to_string(),
            _ => return CommandResult::error("Missing 'content' parameter"),
        };

        let content_type = command.parameters.get("content_type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                // Auto-detect content type
                if key.ends_with(".html") || content.trim_start().starts_with('<') {
                    "text/html".to_string()
                } else if key.ends_with(".js") {
                    "application/javascript".to_string()
                } else if key.ends_with(".css") {
                    "text/css".to_string()
                } else if key.ends_with(".json") || content.trim_start().starts_with('{') {
                    "application/json".to_string()
                } else {
                    "text/plain".to_string()
                }
            });

        let ttl = command.parameters.get("ttl")
            .and_then(|v| v.as_u64())
            .unwrap_or(3600);

        let minify = command.parameters.get("minify")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Process content (minify if requested)
        let (processed_content, minified) = if minify {
            let (minified_content, _stats) = minify_safe(&content, &content_type);
            (minified_content, true)
        } else {
            (content.clone(), false)
        };

        // Store content using async execute_function
        match execute_function_async(
            conn,
            "GNODE_CACHE_SET",
            &[],
            &[&key, &processed_content, &ttl.to_string(), site_id],
            debug_mode
        ).await {
            Ok(_) => {
                // Store metadata
                let metadata_key = format!("{}:meta", key);
                let metadata = json!({
                    "content_type": content_type,
                    "original_size": content.len(),
                    "stored_size": processed_content.len(),
                    "minified": minified,
                    "stored_at": current_timestamp()
                });
                let metadata_json = metadata.to_string();

                let _ = execute_function_async(
                    conn,
                    "GNODE_CACHE_SET",
                    &[],
                    &[&metadata_key, &metadata_json, &ttl.to_string(), site_id],
                    debug_mode
                ).await;

                CommandResult::success(json!({
                    "stored": true,
                    "key": key,
                    "content_type": content_type,
                    "original_size": content.len(),
                    "stored_size": processed_content.len(),
                    "minified": minified,
                    "ttl": ttl,
                    "async": true
                }))
            },
            Err(e) => CommandResult::error(format!("Failed to store content: {}", e))
        }
    })
}

/// Async version of handle_content_retrieve (Phase 4: Async Architecture)
/// Non-blocking content retrieval with ValKey caching
pub fn handle_content_retrieve_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async content_retrieve command: {}", command.id);
        }

        // Extract key parameter
        let key = match command.parameters.get("key").and_then(|v| v.as_str()) {
            Some(k) if !k.is_empty() => k.to_string(),
            _ => return CommandResult::error("Missing or empty 'key' parameter"),
        };

        // Retrieve content using async execute_function
        let stored_content = match execute_function_async(
            conn,
            "GNODE_CACHE_GET",
            &[],
            &[&key, site_id],
            debug_mode
        ).await {
            Ok(content) => content,
            Err(e) => return CommandResult::error(format!("Content not found: {}", e))
        };

        // Retrieve metadata
        let metadata_key = format!("{}:meta", key);
        let metadata: Option<Value> = execute_function_async(
            conn,
            "GNODE_CACHE_GET",
            &[],
            &[&metadata_key, site_id],
            debug_mode
        ).await.ok()
            .and_then(|meta_json| serde_json::from_str(&meta_json).ok());

        // Decompress if needed
        let content = if metadata.as_ref()
            .and_then(|m| m.get("compressed"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            match decode_and_decompress(&stored_content) {
                Ok(decompressed) => decompressed,
                Err(_) => stored_content,
            }
        } else {
            stored_content
        };

        // Build response
        let mut response = json!({
            "content": content,
            "key": key,
            "retrieved_at": current_timestamp(),
            "async": true
        });

        if let Some(meta) = metadata {
            response["metadata"] = meta;
        }

        CommandResult::success(response)
    })
}

/// Async version of handle_template_fragment (Phase 4: Async Architecture)
/// Non-blocking template fragment storage
pub fn handle_template_fragment_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async template_fragment command: {}", command.id);
        }

        // Extract parameters
        let template_id = match command.parameters.get("template_id").and_then(|v| v.as_str()) {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => return CommandResult::error("Missing or empty 'template_id' parameter"),
        };

        let content = match command.parameters.get("content").and_then(|v| v.as_str()) {
            Some(c) => c.to_string(),
            _ => return CommandResult::error("Missing 'content' parameter"),
        };

        let ttl = command.parameters.get("ttl")
            .and_then(|v| v.as_u64())
            .unwrap_or(7200); // Default 2 hours for templates

        // Register template with Tera engine (sync but fast in-memory operation)
        let config = crate::config::GNodeSettings::default();
        let extracted_partials = match crate::integration::template_renderer::register_template(
            &template_id,
            &content,
            &config
        ) {
            Ok(partials) => partials,
            Err(e) => return CommandResult::error(format!("Failed to register template with Tera: {}", e)),
        };

        // Store template content in cache
        let template_key = format!("template:{}", template_id);

        match execute_function_async(
            conn,
            "GNODE_CACHE_SET",
            &[],
            &[&template_key, &content, &ttl.to_string(), site_id],
            debug_mode
        ).await {
            Ok(_) => {
                // Store metadata
                let metadata_key = format!("{}:meta", template_key);
                let metadata = json!({
                    "type": "template_fragment",
                    "stored_at": current_timestamp(),
                    "dependencies": extracted_partials,
                    "registered_in_topology": true
                });
                let metadata_json = metadata.to_string();

                let _ = execute_function_async(
                    conn,
                    "GNODE_CACHE_SET",
                    &[],
                    &[&metadata_key, &metadata_json, &ttl.to_string(), site_id],
                    debug_mode
                ).await;

                CommandResult::success(json!({
                    "stored": true,
                    "template_id": template_id,
                    "dependencies": extracted_partials,
                    "registered_in_topology": true,
                    "ttl": ttl,
                    "async": true
                }))
            },
            Err(e) => CommandResult::error(format!("Failed to store template in cache: {}", e))
        }
    })
}

/// Async version of handle_asset_bundle (Phase 4: Async Architecture)
/// Non-blocking asset bundle creation
pub fn handle_asset_bundle_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async asset_bundle command: {}", command.id);
        }

        // Extract parameters
        let bundle_id = match command.parameters.get("bundle_id").and_then(|v| v.as_str()) {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => return CommandResult::error("Missing or empty 'bundle_id' parameter"),
        };

        let assets: Vec<String> = match command.parameters.get("assets") {
            Some(Value::Array(arr)) => arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            _ => return CommandResult::error("Missing or invalid 'assets' parameter (expected array)"),
        };

        if assets.is_empty() {
            return CommandResult::error("Assets array cannot be empty");
        }

        let bundle_type = command.parameters.get("bundle_type")
            .and_then(|v| v.as_str())
            .unwrap_or("mixed")
            .to_string();

        let minify = command.parameters.get("minify")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let ttl = command.parameters.get("ttl")
            .and_then(|v| v.as_u64())
            .unwrap_or(3600);

        // Retrieve all assets asynchronously
        let mut bundle_content = String::new();
        let mut bundled_assets = Vec::new();
        let mut failed_assets = Vec::new();

        for asset_key in &assets {
            match execute_function_async(
                conn,
                "GNODE_CACHE_GET",
                &[],
                &[asset_key, site_id],
                debug_mode
            ).await {
                Ok(content) => {
                    // Add separator comment for JS/CSS
                    if bundle_type == "js" || bundle_type == "css" {
                        bundle_content.push_str(&format!("\n/* Asset: {} */\n", asset_key));
                    }
                    bundle_content.push_str(&content);
                    bundle_content.push('\n');
                    bundled_assets.push(asset_key.clone());
                },
                Err(_) => {
                    failed_assets.push(asset_key.clone());
                }
            }
        }

        if bundled_assets.is_empty() {
            return CommandResult::error("No assets could be retrieved for bundling");
        }

        // Minify if requested
        let content_type = match bundle_type.as_str() {
            "js" => "application/javascript",
            "css" => "text/css",
            _ => "text/plain",
        };

        let (final_content, was_minified) = if minify {
            let (minified, _) = minify_safe(&bundle_content, content_type);
            (minified, true)
        } else {
            (bundle_content.clone(), false)
        };

        // Store bundle
        let bundle_key = format!("bundle:{}", bundle_id);

        match execute_function_async(
            conn,
            "GNODE_CACHE_SET",
            &[],
            &[&bundle_key, &final_content, &ttl.to_string(), site_id],
            debug_mode
        ).await {
            Ok(_) => {
                // Store metadata
                let metadata_key = format!("{}:meta", bundle_key);
                let metadata = json!({
                    "bundle_type": bundle_type,
                    "assets": bundled_assets,
                    "failed_assets": failed_assets,
                    "original_size": bundle_content.len(),
                    "stored_size": final_content.len(),
                    "minified": was_minified,
                    "stored_at": current_timestamp()
                });
                let metadata_json = metadata.to_string();

                let _ = execute_function_async(
                    conn,
                    "GNODE_CACHE_SET",
                    &[],
                    &[&metadata_key, &metadata_json, &ttl.to_string(), site_id],
                    debug_mode
                ).await;

                CommandResult::success(json!({
                    "bundled": true,
                    "bundle_id": bundle_id,
                    "bundle_type": bundle_type,
                    "assets_bundled": bundled_assets.len(),
                    "assets_failed": failed_assets.len(),
                    "original_size": bundle_content.len(),
                    "stored_size": final_content.len(),
                    "minified": was_minified,
                    "ttl": ttl,
                    "async": true
                }))
            },
            Err(e) => CommandResult::error(format!("Failed to store bundle: {}", e))
        }
    })
}
