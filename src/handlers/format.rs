// Format Command Handlers
//
// Handles: register_format, list_formats, detect_format, convert_format
// These provide message format registration, detection, and conversion.
//
// All four are backed by the base-tier native FormatProcessor (the canonical
// wire-format engine). The former premium gNode-BROKER FCALL path is gone:
// format is a BASE capability, so this reference extension no longer depends
// on a premium one. Custom format definitions are persisted to ValKey by the
// processor; detect/convert/list are pure in-memory native compute.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use log::{debug, warn};
use serde_json::{Value, json};
use crate::daemon::{Command, GNodeDaemon};
use crate::GeometricTopology;

use super::types::{CommandResult, CommandDescriptor, Lane, CommandHandlerFn, AsyncCommandHandlerFn};

/// Register all format command handlers
pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    // Sync handlers
    handlers.insert("register_format".to_string(), handle_register_format as CommandHandlerFn);
    handlers.insert("list_formats".to_string(), handle_list_formats as CommandHandlerFn);
    handlers.insert("detect_format".to_string(), handle_detect_format as CommandHandlerFn);
    handlers.insert("convert_format".to_string(), handle_convert_format as CommandHandlerFn);

    // Async handlers
    async_handlers.insert("register_format".to_string(), handle_register_format_async as AsyncCommandHandlerFn);
    async_handlers.insert("REGISTER_FORMAT".to_string(), handle_register_format_async as AsyncCommandHandlerFn);
    async_handlers.insert("list_formats".to_string(), handle_list_formats_async as AsyncCommandHandlerFn);
    async_handlers.insert("LIST_FORMATS".to_string(), handle_list_formats_async as AsyncCommandHandlerFn);
    async_handlers.insert("detect_format".to_string(), handle_detect_format_async as AsyncCommandHandlerFn);
    async_handlers.insert("DETECT_FORMAT".to_string(), handle_detect_format_async as AsyncCommandHandlerFn);
    async_handlers.insert("convert_format".to_string(), handle_convert_format_async as AsyncCommandHandlerFn);
    async_handlers.insert("CONVERT_FORMAT".to_string(), handle_convert_format_async as AsyncCommandHandlerFn);

    // Descriptors
    descriptors.push(CommandDescriptor {
        name: "register_format",
        category: "format",
        description: "Register a custom message format schema",
        params_schema: json!({"type": "object", "required": ["format_definition"], "properties": {"format_definition": {"type": "object", "required": ["name", "schema", "patterns"], "description": "The format definition object", "properties": {"name": {"type": "string", "description": "Unique format identifier"}, "schema": {"type": "object", "description": "JSONSchema defining the message structure"}, "patterns": {"type": "array", "items": {"type": "object"}, "description": "Detection patterns for auto-identifying this format"}}}}}),
        returns_schema: json!({"type": "object", "properties": {"status": {"type": "string"}, "format_name": {"type": "string"}}}),
        example: r#"{"cmd":"register_format","params":{"format_definition":{"name":"my_format","schema":{"type":"object"},"patterns":[{"pattern_type":"prefix","pattern":"{\"fmt\":","confidence":0.9}]}}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "list_formats",
        category: "format",
        description: "List all registered message formats",
        params_schema: json!({"type": "object", "properties": {}}),
        returns_schema: json!({"type": "array", "items": {"type": "object", "description": "Format definition including name, schema, and detection patterns"}}),
        example: r#"{"cmd":"list_formats","params":{}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "detect_format",
        category: "format",
        description: "Auto-detect the format of a raw message",
        params_schema: json!({"type": "object", "required": ["message"], "properties": {"message": {"type": "string", "description": "The raw message to detect the format of"}}}),
        returns_schema: json!({"type": "object", "properties": {"format_name": {"type": "string"}, "version": {"type": "string"}, "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0}}}),
        example: r#"{"cmd":"detect_format","params":{"message":"{\"i\":\"1\",\"c\":\"ping\",\"p\":{}}"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "convert_format",
        category: "format",
        description: "Convert a message between registered formats",
        params_schema: json!({"type": "object", "required": ["source_format", "target_format", "message"], "properties": {"source_format": {"type": "string", "description": "Source format name"}, "source_version": {"type": "string", "default": "1.0.0", "description": "Source format version"}, "target_format": {"type": "string", "description": "Target format name"}, "target_version": {"type": "string", "default": "1.0.0", "description": "Target format version"}, "message": {"type": "string", "description": "The message to convert"}}}),
        returns_schema: json!({"type": "object", "description": "The converted message in the target format"}),
        example: r#"{"cmd":"convert_format","params":{"source_format":"compact_json","target_format":"standard_json","message":"{\"i\":\"1\",\"c\":\"ping\",\"p\":{}}"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
}

// =========================================================================
// Native core (shared by the sync and async handlers)
// =========================================================================

/// Register a format definition into the global native registry.
/// Returns the (format_name, definition) on success, or an error message.
fn native_register(command: &Command) -> Result<(String, Value), String> {
    let definition = command
        .parameters
        .get("format_definition")
        .cloned()
        .ok_or_else(|| "Missing format_definition parameter".to_string())?;

    let processor = GNodeDaemon::get_format_processor_ref()
        .ok_or_else(|| "Format processor not initialized".to_string())?;

    let name = processor.register(&definition).map_err(|e| e.to_string())?;
    Ok((name, definition))
}

/// Detect the wire format of a raw message via the native engine.
fn native_detect(command: &Command) -> CommandResult {
    let message = match command.parameters.get("message").and_then(|v| v.as_str()) {
        Some(msg) => msg,
        None => return CommandResult::error("Missing or invalid message parameter"),
    };

    let processor = match GNodeDaemon::get_format_processor_ref() {
        Some(p) => p,
        None => return CommandResult::error("Format processor not initialized"),
    };

    match processor.detect(message.as_bytes()) {
        Ok(Some((format_name, version, confidence))) => CommandResult::success(json!({
            "format_name": format_name,
            "version": version,
            "confidence": confidence,
        })),
        Ok(None) => CommandResult::error("Unable to detect message format"),
        Err(e) => CommandResult::error(format!("Error detecting format: {}", e)),
    }
}

/// Convert a message between two registered formats via the native engine.
fn native_convert(command: &Command) -> CommandResult {
    let source_format = match command.parameters.get("source_format").and_then(|v| v.as_str()) {
        Some(fmt) => fmt,
        None => return CommandResult::error("Missing or invalid source_format parameter"),
    };
    let source_version = command.parameters.get("source_version").and_then(|v| v.as_str()).unwrap_or("1.0.0");

    let target_format = match command.parameters.get("target_format").and_then(|v| v.as_str()) {
        Some(fmt) => fmt,
        None => return CommandResult::error("Missing or invalid target_format parameter"),
    };
    let target_version = command.parameters.get("target_version").and_then(|v| v.as_str()).unwrap_or("1.0.0");

    let message = match command.parameters.get("message").and_then(|v| v.as_str()) {
        Some(msg) => msg,
        None => return CommandResult::error("Missing or invalid message parameter"),
    };

    let processor = match GNodeDaemon::get_format_processor_ref() {
        Some(p) => p,
        None => return CommandResult::error("Format processor not initialized"),
    };

    match processor.convert(
        message.as_bytes(),
        source_format,
        Some(source_version),
        target_format,
        Some(target_version),
    ) {
        Ok(bytes) => {
            // JSON targets parse back to a structured value; binary/RESP3
            // targets are returned as a UTF-8 (lossy) string.
            match serde_json::from_slice::<Value>(&bytes) {
                Ok(value) => CommandResult::success(value),
                Err(_) => CommandResult::success(Value::String(String::from_utf8_lossy(&bytes).into_owned())),
            }
        },
        Err(e) => CommandResult::error(format!("Error converting format: {}", e)),
    }
}

/// List all registered formats via the native engine.
fn native_list() -> CommandResult {
    match GNodeDaemon::get_format_processor_ref() {
        Some(processor) => match processor.list_formats() {
            Ok(formats) => CommandResult::success(formats),
            Err(e) => CommandResult::error(format!("Error listing formats: {}", e)),
        },
        None => CommandResult::error("Format processor not initialized"),
    }
}

// =========================================================================
// Sync handlers
// =========================================================================

/// Handle 'register_format' command
pub fn handle_register_format(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling register_format command: {}", command.id);
    }

    match native_register(command) {
        Ok((name, definition)) => {
            if let Some(processor) = GNodeDaemon::get_format_processor_ref() {
                let namespace = GNodeDaemon::get_topology_namespace();
                if let Err(e) = processor.persist_format(conn, namespace, &definition) {
                    warn!("Format {} registered but not persisted to ValKey: {}", name, e);
                }
            }
            CommandResult::success(json!({"status": "registered", "format_name": name}))
        },
        Err(e) => CommandResult::error(e),
    }
}

/// Handle 'list_formats' command
pub fn handle_list_formats(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling list_formats command: {}", command.id);
    }
    native_list()
}

/// Handle 'detect_format' command
pub fn handle_detect_format(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling detect_format command: {}", command.id);
    }
    native_detect(command)
}

/// Handle 'convert_format' command
pub fn handle_convert_format(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling convert_format command: {}", command.id);
    }
    native_convert(command)
}

// =========================================================================
// Async handlers (fast-lane hot path)
// =========================================================================

/// Async version of handle_register_format
pub fn handle_register_format_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async register_format command: {}", command.id);
        }

        match native_register(command) {
            Ok((name, definition)) => {
                if let Some(processor) = GNodeDaemon::get_format_processor_ref() {
                    let namespace = GNodeDaemon::get_topology_namespace();
                    if let Err(e) = processor.persist_format_async(conn, namespace, &definition).await {
                        warn!("Format {} registered but not persisted to ValKey: {}", name, e);
                    }
                }
                CommandResult::success(json!({"status": "registered", "format_name": name, "async": true}))
            },
            Err(e) => CommandResult::error(e),
        }
    })
}

/// Async version of handle_list_formats
pub fn handle_list_formats_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async list_formats command: {}", command.id);
        }
        native_list()
    })
}

/// Async version of handle_detect_format
pub fn handle_detect_format_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async detect_format command: {}", command.id);
        }
        native_detect(command)
    })
}

/// Async version of handle_convert_format
pub fn handle_convert_format_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async convert_format command: {}", command.id);
        }
        native_convert(command)
    })
}
