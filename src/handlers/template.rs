// Template Command Handlers
//
// Handles: render_template, serve_fragment, list_templates, 
//          discover_similar_templates, discover_templates_by_capability,
//          get_template_capabilities
// These provide Tera template rendering, fragment serving, and template discovery.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use serde::Deserialize;
use log::debug;
use serde_json::{Value, json};
use crate::daemon::Command;
use crate::GeometricTopology;
use crate::integration::valkey_functions::execute_function;

use super::types::{
    CommandResult, CommandDescriptor, CommandHandlerFn, AsyncCommandHandlerFn, parse_parameters,
    fixed_vector_to_capabilities, calculate_euclidean_distance,
};

/// Register all template command handlers
pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    // Sync handlers - rendering
    handlers.insert("render_template".to_string(), handle_render_template as CommandHandlerFn);
    handlers.insert("serve_fragment".to_string(), handle_serve_fragment as CommandHandlerFn);

    // Sync handlers - discovery (Phase 2C)
    handlers.insert("list_templates".to_string(), handle_list_templates as CommandHandlerFn);
    handlers.insert("discover_similar_templates".to_string(), handle_discover_similar_templates as CommandHandlerFn);
    handlers.insert("discover_templates_by_capability".to_string(), handle_discover_templates_by_capability as CommandHandlerFn);
    handlers.insert("get_template_capabilities".to_string(), handle_get_template_capabilities as CommandHandlerFn);

    // Async handlers
    async_handlers.insert("render_template".to_string(), handle_render_template_async as AsyncCommandHandlerFn);
    async_handlers.insert("RENDER_TEMPLATE".to_string(), handle_render_template_async as AsyncCommandHandlerFn);
    async_handlers.insert("serve_fragment".to_string(), handle_serve_fragment_async as AsyncCommandHandlerFn);
    async_handlers.insert("SERVE_FRAGMENT".to_string(), handle_serve_fragment_async as AsyncCommandHandlerFn);
    async_handlers.insert("list_templates".to_string(), handle_list_templates_async as AsyncCommandHandlerFn);
    async_handlers.insert("LIST_TEMPLATES".to_string(), handle_list_templates_async as AsyncCommandHandlerFn);
    async_handlers.insert("discover_similar_templates".to_string(), handle_discover_similar_templates_async as AsyncCommandHandlerFn);
    async_handlers.insert("DISCOVER_SIMILAR_TEMPLATES".to_string(), handle_discover_similar_templates_async as AsyncCommandHandlerFn);
    async_handlers.insert("discover_templates_by_capability".to_string(), handle_discover_templates_by_capability_async as AsyncCommandHandlerFn);
    async_handlers.insert("DISCOVER_TEMPLATES_BY_CAPABILITY".to_string(), handle_discover_templates_by_capability_async as AsyncCommandHandlerFn);
    async_handlers.insert("get_template_capabilities".to_string(), handle_get_template_capabilities_async as AsyncCommandHandlerFn);
    async_handlers.insert("GET_TEMPLATE_CAPABILITIES".to_string(), handle_get_template_capabilities_async as AsyncCommandHandlerFn);

    // Command descriptors
    descriptors.push(CommandDescriptor {
        name: "render_template",
        category: "template",
        description: "Render a Tera template with variables",
        params_schema: json!({
            "type": "object",
            "properties": {
                "template_id": {"type": "string", "description": "Template identifier"},
                "variables": {"type": "object", "description": "Template variables"}
            },
            "required": ["template_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "rendered": {"type": "string", "description": "Rendered template output"}
            }
        }),
        example: r#"{"cmd":"render_template","params":{"template_id":"homepage","variables":{"title":"Welcome"}}}"#,
        async_capable: true,
    });

    descriptors.push(CommandDescriptor {
        name: "serve_fragment",
        category: "template",
        description: "Serve a pre-rendered template fragment from cache",
        params_schema: json!({
            "type": "object",
            "properties": {
                "fragment_id": {"type": "string", "description": "Fragment identifier"}
            },
            "required": ["fragment_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "html": {"type": "string", "description": "Cached fragment HTML"},
                "headers": {"type": "object", "description": "HTTP headers"}
            }
        }),
        example: r#"{"cmd":"serve_fragment","params":{"fragment_id":"nav-header"}}"#,
        async_capable: true,
    });

    descriptors.push(CommandDescriptor {
        name: "list_templates",
        category: "template",
        description: "List all registered templates",
        params_schema: json!({
            "type": "object",
            "properties": {}
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "matches": {"type": "array", "description": "Array of template IDs and metadata"},
                "count": {"type": "integer", "description": "Number of templates"}
            }
        }),
        example: r#"{"cmd":"list_templates","params":{}}"#,
        async_capable: true,
    });

    descriptors.push(CommandDescriptor {
        name: "discover_similar_templates",
        category: "template",
        description: "Find templates similar to a given template by capability distance",
        params_schema: json!({
            "type": "object",
            "properties": {
                "template_id": {"type": "string", "description": "Reference template identifier"},
                "limit": {"type": "integer", "description": "Maximum results to return", "default": 5}
            },
            "required": ["template_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "similar_templates": {"type": "array", "description": "Array of similar templates with distances"},
                "count": {"type": "integer", "description": "Number of matches"}
            }
        }),
        example: r#"{"cmd":"discover_similar_templates","params":{"template_id":"homepage","limit":5}}"#,
        async_capable: true,
    });

    descriptors.push(CommandDescriptor {
        name: "discover_templates_by_capability",
        category: "template",
        description: "Find templates matching capability requirements",
        params_schema: json!({
            "type": "object",
            "properties": {
                "capabilities": {"type": "object", "description": "Capability to value map for filtering"},
                "limit": {"type": "integer", "description": "Maximum results to return"}
            }
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "matches": {"type": "array", "description": "Matching templates sorted by distance"},
                "count": {"type": "integer", "description": "Number of matches"}
            }
        }),
        example: r#"{"cmd":"discover_templates_by_capability","params":{"capabilities":{"cacheability":0.8,"complexity":0.3},"limit":10}}"#,
        async_capable: true,
    });

    descriptors.push(CommandDescriptor {
        name: "get_template_capabilities",
        category: "template",
        description: "Get the capability vector for a specific template",
        params_schema: json!({
            "type": "object",
            "properties": {
                "template_id": {"type": "string", "description": "Template identifier"}
            },
            "required": ["template_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "template_id": {"type": "string", "description": "Template identifier"},
                "capabilities": {"type": "object", "description": "Capability dimension values"}
            }
        }),
        example: r#"{"cmd":"get_template_capabilities","params":{"template_id":"homepage"}}"#,
        async_capable: true,
    });
}

// =========================================================================
// Sync handlers - Rendering
// =========================================================================

/// Handle 'render_template' command - Render a registered template with variables
pub fn handle_render_template(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling render_template command: {}", command.id);
    }

    // Parse parameters
    #[derive(Debug, Deserialize)]
    struct RenderTemplateParams {
        template_id: String,
        variables: Option<Value>,
        cache_output: Option<bool>,
    }

    let params = match parse_parameters::<RenderTemplateParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    // Get config (create minimal config for template_renderer)
    let config = crate::config::GNodeSettings::default();

    // Render template using template_renderer module
    let variables = params.variables.unwrap_or_else(|| json!({}));

    let html = match crate::integration::template_renderer::render_template(
        &params.template_id,
        &variables,
        &config
    ) {
        Ok(output) => output,
        Err(e) => return CommandResult::error(format!("Failed to render template: {}", e)),
    };

    // Cache output if requested
    if params.cache_output.unwrap_or(false) {
        let cache_key = format!("template:{}:output", params.template_id);
        let ttl = 3600; // 1 hour TTL

        let _ = execute_function(
            conn,
            "GNODE_CACHE_SET",
            &[],
            &[&cache_key, &html, &ttl.to_string(), site_id],
            site_id,
            debug_mode
        );
    }

    CommandResult::success(json!({
        "status": "ok",
        "html": html,
        "template_id": params.template_id,
        "cached": params.cache_output.unwrap_or(false)
    }))
}

/// Handle 'serve_fragment' command - Serve cached HTML fragment with HTMX headers
pub fn handle_serve_fragment(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling serve_fragment command: {}", command.id);
    }

    // Parse parameters
    #[derive(Debug, Deserialize)]
    struct ServeFragmentParams {
        key: String,
        hx_trigger: Option<String>,
        cache_control: Option<String>,
    }

    let params = match parse_parameters::<ServeFragmentParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    // Retrieve HTML from ValKey cache
    let html = match execute_function(
        conn,
        "GNODE_CACHE_GET",
        &[],
        &[&params.key, site_id],
        site_id,
        debug_mode
    ) {
        Ok(content) => content,
        Err(e) => return CommandResult::error(format!("Fragment not found: {}", e)),
    };

    // Calculate ETag for caching (using simple hash)
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    html.hash(&mut hasher);
    let etag = format!("{:x}", hasher.finish());

    // Build HTMX-compatible response
    let mut headers = HashMap::new();
    headers.insert(
        "Content-Type".to_string(),
        "text/html; charset=utf-8".to_string()
    );
    headers.insert(
        "Cache-Control".to_string(),
        params.cache_control.unwrap_or_else(|| "public, max-age=31536000, immutable".to_string())
    );
    headers.insert(
        "ETag".to_string(),
        format!("\"{}\"", etag)
    );

    // Add HX-Trigger header if specified
    if let Some(hx_trigger) = params.hx_trigger {
        headers.insert("HX-Trigger".to_string(), hx_trigger);
    }

    CommandResult::success(json!({
        "status": "ok",
        "html": html,
        "headers": headers,
        "key": params.key
    }))
}

// =========================================================================
// Sync handlers - Discovery
// =========================================================================

///
/// Returns all templates stored in the geometric topology with their IDs and basic metadata.
/// This is a simple query command for browsing available templates without filtering.
pub fn handle_list_templates(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling list_templates command: {}", command.id);
    }

    // Get topology reference
    let topology = crate::daemon::GNodeDaemon::get_topology_ref();
    let topology_guard = match topology.read() {
        Ok(guard) => guard,
        Err(e) => return CommandResult::error(format!("Failed to acquire topology lock: {}", e)),
    };

    // Find all template services
    let mut templates: Vec<Value> = Vec::new();
    for (id, service) in &topology_guard.services {
        // Only process template services
        if id.starts_with("template:") {
            let template_id = id.strip_prefix("template:").unwrap_or(id);

            // Convert FixedVector to capabilities HashMap
            let capabilities = fixed_vector_to_capabilities(&service.point, &topology_guard.capability_dimensions);

            templates.push(json!({
                "template_id": template_id,
                "service_id": id,
                "capabilities": capabilities,
                "dimension_count": topology_guard.dimensions,
            }));
        }
    }

    if debug_mode {
        debug!("Found {} templates", templates.len());
    }

    // Return response with matches array
    CommandResult::success(json!({
        "matches": templates,
        "count": templates.len()
    }))
}

/// Handle 'discover_similar_templates' command - Find templates by geometric similarity
///
/// Uses 8D Euclidean distance to find templates similar to a reference template.
/// This enables template recommendation, A/B variant discovery, and layout consistency analysis.
pub fn handle_discover_similar_templates(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling discover_similar_templates command: {}", command.id);
    }

    // Parse parameters
    #[derive(Debug, Deserialize)]
    struct DiscoverSimilarParams {
        template_id: String,
        #[serde(default = "default_max_distance")]
        max_distance: f64,
    }

    fn default_max_distance() -> f64 {
        0.3 // Default: 30% difference threshold
    }

    let params = match parse_parameters::<DiscoverSimilarParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    // Get topology reference
    let topology = crate::daemon::GNodeDaemon::get_topology_ref();
    let topology_guard = match topology.read() {
        Ok(guard) => guard,
        Err(e) => return CommandResult::error(format!("Failed to acquire topology lock: {}", e)),
    };

    // Get template's capabilities from topology
    let service_id = format!("template:{}", params.template_id);
    let template_service = match topology_guard.services.get(&service_id) {
        Some(svc) => svc,
        None => return CommandResult::error(format!("Template not found: {}", params.template_id)),
    };

    let template_caps = fixed_vector_to_capabilities(&template_service.point, &topology_guard.capability_dimensions);

    // Discover similar templates via geometric distance
    let mut candidates: Vec<(String, f64)> = Vec::new();
    for (id, service) in &topology_guard.services {
        // Only process template services, skip self
        if !id.starts_with("template:") || id == &service_id {
            continue;
        }

        // Calculate Euclidean distance in 8D space
        let service_caps = fixed_vector_to_capabilities(&service.point, &topology_guard.capability_dimensions);
        let distance = calculate_euclidean_distance(&template_caps, &service_caps);

        // Include if within threshold
        if distance <= params.max_distance {
            candidates.push((id.clone(), distance));
        }
    }

    // Sort by distance (most similar first)
    candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    // Format results with template IDs (strip "template:" prefix)
    let results: Vec<Value> = candidates.iter()
        .map(|(id, distance)| {
            let template_id = id.strip_prefix("template:").unwrap_or(id);
            let similarity = if params.max_distance > 0.0 {
                1.0 - (distance / params.max_distance)
            } else {
                1.0
            };

            let service = &topology_guard.services[id];
            let caps = fixed_vector_to_capabilities(&service.point, &topology_guard.capability_dimensions);

            json!({
                "template_id": template_id,
                "distance": distance,
                "similarity": similarity,
                "capabilities": caps
            })
        })
        .collect();

    if debug_mode {
        debug!("Found {} similar templates for '{}' within distance {}",
            results.len(), params.template_id, params.max_distance);
    }

    CommandResult::success(json!({
        "template_id": params.template_id,
        "similar_templates": results,
        "count": results.len(),
        "max_distance": params.max_distance
    }))
}

/// Handle 'discover_templates_by_capability' command - Filter templates by capability constraints
///
/// Enables finding templates matching specific criteria like:
/// - Cacheable templates (cacheability > 0.8)
/// - Simple templates (complexity < 0.3)
/// - Interactive forms (interactivity > 0.7)
/// - Static content (data_density < 0.2)
pub fn handle_discover_templates_by_capability(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling discover_templates_by_capability command: {}", command.id);
    }

    // Parse parameters
    // Format: {"filters": {"complexity": ["<", 0.3], "cacheability": [">", 0.8]}}
    // Empty filters {} returns all templates
    let filters = match command.parameters.get("filters").and_then(|v| v.as_object()) {
        Some(f) => f,
        None => return CommandResult::error("Missing 'filters' parameter (must be object)"),
    };

    // Allow empty filters - returns all templates when no constraints specified

    // Get topology reference
    let topology = crate::daemon::GNodeDaemon::get_topology_ref();
    let topology_guard = match topology.read() {
        Ok(guard) => guard,
        Err(e) => return CommandResult::error(format!("Failed to acquire topology lock: {}", e)),
    };

    // Find matching templates
    let mut matches: Vec<Value> = Vec::new();
    for (id, service) in &topology_guard.services {
        // Only process template services
        if !id.starts_with("template:") {
            continue;
        }

        // Convert FixedVector to capabilities HashMap
        let service_caps = fixed_vector_to_capabilities(&service.point, &topology_guard.capability_dimensions);

        // Check if service matches all filters
        let mut all_match = true;
        for (cap_name, constraint) in filters {
            let constraint_arr = match constraint.as_array() {
                Some(arr) => arr,
                None => {
                    return CommandResult::error(format!(
                        "Filter for '{}' must be array [operator, value]", cap_name
                    ));
                }
            };

            if constraint_arr.len() != 2 {
                return CommandResult::error(format!(
                    "Filter for '{}' must have exactly 2 elements: [operator, value]", cap_name
                ));
            }

            let operator = match constraint_arr[0].as_str() {
                Some(op) => op,
                None => {
                    return CommandResult::error(format!(
                        "Operator for '{}' must be string", cap_name
                    ));
                }
            };

            let threshold = match constraint_arr[1].as_f64() {
                Some(val) => val,
                None => {
                    return CommandResult::error(format!(
                        "Threshold for '{}' must be number", cap_name
                    ));
                }
            };

            let cap_value = service_caps.get(cap_name)
                .copied()
                .unwrap_or(0.0);

            let matches_constraint = match operator {
                "<" => cap_value < threshold,
                ">" => cap_value > threshold,
                "=" | "==" => (cap_value - threshold).abs() < 0.01,
                "<=" => cap_value <= threshold,
                ">=" => cap_value >= threshold,
                _ => {
                    return CommandResult::error(format!(
                        "Unknown operator '{}' (valid: <, >, <=, >=, =, ==)", operator
                    ));
                }
            };

            if !matches_constraint {
                all_match = false;
                break;
            }
        }

        if all_match {
            let template_id = id.strip_prefix("template:").unwrap_or(id);
            matches.push(json!({
                "template_id": template_id,
                "capabilities": service_caps
            }));
        }
    }

    if debug_mode {
        debug!("Found {} templates matching {} capability filters", matches.len(), filters.len());
    }

    CommandResult::success(json!({
        "matches": matches,
        "count": matches.len(),
        "filters_applied": filters
    }))
}

/// Handle 'get_template_capabilities' command - Retrieve template's 8D capability vector
///
/// Returns the geometric capability vector and metadata for a specific template.
/// Useful for inspecting template properties and understanding geometric positioning.
pub fn handle_get_template_capabilities(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling get_template_capabilities command: {}", command.id);
    }

    // Parse parameters
    let template_id = match command.parameters.get("template_id").and_then(|v| v.as_str()) {
        Some(id) => id,
        None => return CommandResult::error("Missing 'template_id' parameter"),
    };

    // Get topology reference
    let topology = crate::daemon::GNodeDaemon::get_topology_ref();
    let topology_guard = match topology.read() {
        Ok(guard) => guard,
        Err(e) => return CommandResult::error(format!("Failed to acquire topology lock: {}", e)),
    };

    // Get template service
    let service_id = format!("template:{}", template_id);
    let service = match topology_guard.services.get(&service_id) {
        Some(svc) => svc,
        None => return CommandResult::error(format!("Template not found: {}", template_id)),
    };

    // Convert FixedVector to capabilities HashMap
    let capabilities = fixed_vector_to_capabilities(&service.point, &topology_guard.capability_dimensions);

    if debug_mode {
        debug!("Retrieved capabilities for template '{}'", template_id);
    }

    CommandResult::success(json!({
        "template_id": template_id,
        "capabilities": capabilities,
        "metadata": service.metadata
    }))
}


// =========================================================================
// Async handlers
// =========================================================================

/// Async version of handle_render_template
pub fn handle_render_template_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async render_template command: {}", command.id);
        }

        #[derive(Debug, Deserialize)]
        struct RenderTemplateParams {
            template_id: String,
            variables: Option<Value>,
            cache_output: Option<bool>,
        }

        let params: RenderTemplateParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        let config = crate::config::GNodeSettings::default();
        let variables = params.variables.unwrap_or_else(|| json!({}));

        let html = match crate::integration::template_renderer::render_template(
            &params.template_id,
            &variables,
            &config
        ) {
            Ok(output) => output,
            Err(e) => return CommandResult::error(format!("Failed to render template: {}", e)),
        };

        // Cache output if requested (async)
        if params.cache_output.unwrap_or(false) {
            let cache_key = format!("template:{}:output", params.template_id);
            let ttl = "3600";

            let _: redis::RedisResult<()> = redis::cmd("FCALL")
                .arg("GNODE_CACHE_SET")
                .arg(0)
                .arg(&cache_key)
                .arg(&html)
                .arg(ttl)
                .arg(site_id)
                .query_async(conn)
                .await;
        }

        CommandResult::success(json!({
            "status": "ok",
            "html": html,
            "template_id": params.template_id,
            "cached": params.cache_output.unwrap_or(false),
            "async": true
        }))
    })
}

/// Async version of handle_serve_fragment
pub fn handle_serve_fragment_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async serve_fragment command: {}", command.id);
        }

        #[derive(Debug, Deserialize)]
        struct ServeFragmentParams {
            key: String,
            hx_trigger: Option<String>,
            cache_control: Option<String>,
        }

        let params: ServeFragmentParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        // Retrieve HTML from ValKey cache (async)
        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_CACHE_GET")
            .arg(0)
            .arg(&params.key)
            .arg(site_id)
            .query_async(conn)
            .await;

        let html = match result {
            Ok(content) => content,
            Err(e) => return CommandResult::error(format!("Fragment not found: {}", e)),
        };

        // Calculate ETag
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        html.hash(&mut hasher);
        let etag = format!("{:x}", hasher.finish());

        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "text/html; charset=utf-8".to_string());
        headers.insert(
            "Cache-Control".to_string(),
            params.cache_control.unwrap_or_else(|| "public, max-age=31536000, immutable".to_string())
        );
        headers.insert("ETag".to_string(), format!("\"{}\"", etag));

        if let Some(hx_trigger) = params.hx_trigger {
            headers.insert("HX-Trigger".to_string(), hx_trigger);
        }

        CommandResult::success(json!({
            "status": "ok",
            "html": html,
            "headers": headers,
            "key": params.key,
            "async": true
        }))
    })
}

/// Async version of handle_list_templates
pub fn handle_list_templates_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async list_templates command: {}", command.id);
        }

        let topology = crate::daemon::GNodeDaemon::get_topology_ref();
        let topology_guard = match topology.read() {
            Ok(guard) => guard,
            Err(e) => return CommandResult::error(format!("Failed to acquire topology lock: {}", e)),
        };

        let mut templates: Vec<Value> = Vec::new();
        for (id, service) in &topology_guard.services {
            if id.starts_with("template:") {
                let template_id = id.strip_prefix("template:").unwrap_or(id);
                let capabilities = fixed_vector_to_capabilities(&service.point, &topology_guard.capability_dimensions);

                templates.push(json!({
                    "template_id": template_id,
                    "service_id": id,
                    "capabilities": capabilities,
                    "dimension_count": topology_guard.dimensions,
                }));
            }
        }

        CommandResult::success(json!({
            "matches": templates,
            "count": templates.len(),
            "async": true
        }))
    })
}

/// Async version of handle_discover_similar_templates
pub fn handle_discover_similar_templates_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async discover_similar_templates command: {}", command.id);
        }

        #[derive(Debug, Deserialize)]
        struct DiscoverSimilarParams {
            template_id: String,
            #[serde(default)]
            max_distance: Option<f64>,
        }

        let params: DiscoverSimilarParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        let max_distance = params.max_distance.unwrap_or(0.3);

        let topology = crate::daemon::GNodeDaemon::get_topology_ref();
        let topology_guard = match topology.read() {
            Ok(guard) => guard,
            Err(e) => return CommandResult::error(format!("Failed to acquire topology lock: {}", e)),
        };

        let service_id = format!("template:{}", params.template_id);
        let template_service = match topology_guard.services.get(&service_id) {
            Some(svc) => svc,
            None => return CommandResult::error(format!("Template not found: {}", params.template_id)),
        };

        let template_caps = fixed_vector_to_capabilities(&template_service.point, &topology_guard.capability_dimensions);

        let mut candidates: Vec<(String, f64)> = Vec::new();
        for (id, service) in &topology_guard.services {
            if !id.starts_with("template:") || id == &service_id {
                continue;
            }

            let service_caps = fixed_vector_to_capabilities(&service.point, &topology_guard.capability_dimensions);
            let distance = calculate_euclidean_distance(&template_caps, &service_caps);

            if distance <= max_distance {
                candidates.push((id.clone(), distance));
            }
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let results: Vec<Value> = candidates.iter()
            .map(|(id, distance)| {
                let template_id = id.strip_prefix("template:").unwrap_or(id);
                let similarity = if max_distance > 0.0 { 1.0 - (distance / max_distance) } else { 1.0 };
                let service = &topology_guard.services[id];
                let caps = fixed_vector_to_capabilities(&service.point, &topology_guard.capability_dimensions);

                json!({
                    "template_id": template_id,
                    "distance": distance,
                    "similarity": similarity,
                    "capabilities": caps
                })
            })
            .collect();

        CommandResult::success(json!({
            "template_id": params.template_id,
            "similar_templates": results,
            "count": results.len(),
            "max_distance": max_distance,
            "async": true
        }))
    })
}

/// Async version of handle_discover_templates_by_capability
pub fn handle_discover_templates_by_capability_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async discover_templates_by_capability command: {}", command.id);
        }

        let filters = match command.parameters.get("filters").and_then(|v| v.as_object()) {
            Some(f) => f.clone(),
            None => return CommandResult::error("Missing 'filters' parameter (must be object)"),
        };

        let topology = crate::daemon::GNodeDaemon::get_topology_ref();
        let topology_guard = match topology.read() {
            Ok(guard) => guard,
            Err(e) => return CommandResult::error(format!("Failed to acquire topology lock: {}", e)),
        };

        let mut matches: Vec<Value> = Vec::new();
        for (id, service) in &topology_guard.services {
            if !id.starts_with("template:") {
                continue;
            }

            let service_caps = fixed_vector_to_capabilities(&service.point, &topology_guard.capability_dimensions);

            let mut all_match = true;
            for (cap_name, constraint) in &filters {
                let constraint_arr = match constraint.as_array() {
                    Some(arr) if arr.len() == 2 => arr,
                    _ => {
                        return CommandResult::error(format!(
                            "Filter for '{}' must be array [operator, value]", cap_name
                        ));
                    }
                };

                let operator = constraint_arr[0].as_str().unwrap_or("");
                let threshold = constraint_arr[1].as_f64().unwrap_or(0.0);
                let cap_value = service_caps.get(cap_name).copied().unwrap_or(0.0);

                let passes = match operator {
                    ">" => cap_value > threshold,
                    ">=" => cap_value >= threshold,
                    "<" => cap_value < threshold,
                    "<=" => cap_value <= threshold,
                    "=" | "==" => (cap_value - threshold).abs() < 0.0001,
                    _ => false,
                };

                if !passes {
                    all_match = false;
                    break;
                }
            }

            if all_match {
                let template_id = id.strip_prefix("template:").unwrap_or(id);
                matches.push(json!({
                    "template_id": template_id,
                    "service_id": id,
                    "capabilities": service_caps
                }));
            }
        }

        CommandResult::success(json!({
            "matches": matches,
            "count": matches.len(),
            "filters_applied": filters.len(),
            "async": true
        }))
    })
}

/// Async version of handle_get_template_capabilities
pub fn handle_get_template_capabilities_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async get_template_capabilities command: {}", command.id);
        }

        let template_id = match command.parameters.get("template_id").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => return CommandResult::error("Missing 'template_id' parameter"),
        };

        let topology = crate::daemon::GNodeDaemon::get_topology_ref();
        let topology_guard = match topology.read() {
            Ok(guard) => guard,
            Err(e) => return CommandResult::error(format!("Failed to acquire topology lock: {}", e)),
        };

        let service_id = format!("template:{}", template_id);
        let service = match topology_guard.services.get(&service_id) {
            Some(svc) => svc,
            None => return CommandResult::error(format!("Template not found: {}", template_id)),
        };

        let capabilities = fixed_vector_to_capabilities(&service.point, &topology_guard.capability_dimensions);

        CommandResult::success(json!({
            "template_id": template_id,
            "capabilities": capabilities,
            "dimension_names": topology_guard.capability_dimensions,
            "async": true
        }))
    })
}
