// Format Command Handlers
//
// Handles: register_format, list_formats, detect_format, convert_format
// These provide message format registration, detection, and conversion.
// Sync handlers are feature-gated with #[cfg(feature = "cms")].

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use log::{debug, warn, error};
use serde_json::{Value, json};
use crate::daemon::Command;
use crate::GeometricTopology;
use crate::integration::valkey_functions::execute_function;

use super::types::{CommandResult, CommandDescriptor, CommandHandlerFn, AsyncCommandHandlerFn};

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
        params_schema: json!({"type": "object", "required": ["name", "schema", "patterns"], "properties": {"name": {"type": "string", "description": "Unique format identifier"}, "schema": {"type": "object", "description": "JSONSchema defining the message structure"}, "patterns": {"type": "array", "items": {"type": "object"}, "description": "Detection patterns for auto-identifying this format"}}}),
        returns_schema: json!({"type": "object", "properties": {"status": {"type": "string"}, "format_name": {"type": "string"}}}),
        example: r#"{"cmd":"register_format","params":{"name":"my_format","schema":{"type":"object"},"patterns":[{"pattern_type":"prefix","pattern":"{\"fmt\":","confidence":0.9}]}}"#,
        async_capable: true,
    });
    descriptors.push(CommandDescriptor {
        name: "list_formats",
        category: "format",
        description: "List all registered message formats",
        params_schema: json!({"type": "object", "properties": {}}),
        returns_schema: json!({"type": "array", "items": {"type": "object", "description": "Format definition including name, schema, and detection patterns"}}),
        example: r#"{"cmd":"list_formats","params":{}}"#,
        async_capable: true,
    });
    descriptors.push(CommandDescriptor {
        name: "detect_format",
        category: "format",
        description: "Auto-detect the format of a raw message",
        params_schema: json!({"type": "object", "required": ["message"], "properties": {"message": {"type": "string", "description": "The raw message to detect the format of"}}}),
        returns_schema: json!({"type": "object", "properties": {"format_name": {"type": "string"}, "version": {"type": "string"}, "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0}}}),
        example: r#"{"cmd":"detect_format","params":{"message":"{\"i\":\"1\",\"c\":\"ping\",\"p\":{}}"}}"#,
        async_capable: true,
    });
    descriptors.push(CommandDescriptor {
        name: "convert_format",
        category: "format",
        description: "Convert a message between registered formats",
        params_schema: json!({"type": "object", "required": ["source_format", "target_format", "message"], "properties": {"source_format": {"type": "string", "description": "Source format name"}, "source_version": {"type": "string", "default": "1.0.0", "description": "Source format version"}, "target_format": {"type": "string", "description": "Target format name"}, "target_version": {"type": "string", "default": "1.0.0", "description": "Target format version"}, "message": {"type": "string", "description": "The message to convert"}}}),
        returns_schema: json!({"type": "object", "description": "The converted message in the target format"}),
        example: r#"{"cmd":"convert_format","params":{"source_format":"compact_json","target_format":"standard_json","message":"{\"i\":\"1\",\"c\":\"ping\",\"p\":{}}"}}"#,
        async_capable: true,
    });
}

// =========================================================================
// Sync handlers
// =========================================================================

/// Handle 'register_format' command
#[cfg(feature = "cms")]
pub fn handle_register_format(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling register_format command: {}", command.id);
    }

    // Extract format definition from parameters
    let format_definition = match command.parameters.get("format_definition") {
        Some(definition) => definition,
        _ => return CommandResult::error("Missing format_definition parameter"),
    };

    // Try Rust native implementation first
    #[cfg(feature = "cms")]
    {
        use crate::daemon::GNodeDaemon;

        if let Some(format_proc) = GNodeDaemon::get_format_processor_ref() {
            match format_proc.register_format_from_command(command) {
                Ok(format_name) => {
                    if debug_mode {
                        debug!("Rust native format_register succeeded for: {}", format_name);
                    }
                    return CommandResult::success(json!({
                        "status": "registered",
                        "format_name": format_name
                    }));
                },
                Err(e) => {
                    warn!("Rust format_register failed: {}, trying ValKey fallback", e);
                    // Fall through to ValKey fallback
                }
            }
        } else if debug_mode {
            debug!("Format processor not available, using ValKey fallback");
        }
    }

    // ValKey fallback
    let result = execute_function(
        conn,
        "GNODE_REGISTER_FORMAT",
        &[],
        &[&format_definition.to_string()],
        site_id,
        debug_mode
    );

    match result {
        Ok(json_str) => {
            if debug_mode {
                debug!("ValKey format_register fallback succeeded");
            }
            CommandResult::success_json(json_str)
        },
        Err(e) => {
            let error_msg = format!("Both Rust and ValKey format_register failed: {}", e);
            error!("{}", error_msg);
            CommandResult::error(error_msg)
        }
    }
}

#[cfg(not(feature = "cms"))]
pub fn handle_register_format(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling register_format command: {}", command.id);
    }

    // Extract format definition from parameters
    let format_definition = match command.parameters.get("format_definition") {
        Some(definition) => definition,
        _ => return CommandResult::error("Missing format_definition parameter"),
    };

    // ValKey only (format feature disabled)
    let result = execute_function(
        conn,
        "GNODE_REGISTER_FORMAT",
        &[],
        &[&format_definition.to_string()],
        site_id,
        debug_mode
    );

    match result {
        Ok(json_str) => {
            // ValKey function returns {"status":"ok","result":{...},"timestamp":...}
            // We need to unwrap and extract the inner "result" field
            match serde_json::from_str::<Value>(&json_str) {
                Ok(response) => {
                    if let Some(result_data) = response.get("result") {
                        CommandResult::success(result_data.clone())
                    } else {
                        // No result field, return the whole response
                        CommandResult::success(response)
                    }
                },
                Err(e) => {
                    warn!("Failed to parse ValKey function response: {}", e);
                    CommandResult::error(format!("Invalid response from ValKey function: {}", e))
                }
            }
        },
        Err(e) => CommandResult::error(format!("Error registering format: {}", e)),
    }
}

/// Handle 'list_formats' command
#[cfg(feature = "cms")]
pub fn handle_list_formats(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling list_formats command: {}", command.id);
    }

    // Try Rust native implementation first
    #[cfg(feature = "cms")]
    {
        use crate::daemon::GNodeDaemon;

        if let Some(format_proc) = GNodeDaemon::get_format_processor_ref() {
            match format_proc.list_formats() {
                Ok(formats) => {
                    if debug_mode {
                        debug!("Rust native list_formats succeeded");
                    }
                    return CommandResult::success(formats);
                },
                Err(e) => {
                    warn!("Rust list_formats failed: {}, trying ValKey fallback", e);
                    // Fall through to ValKey fallback
                }
            }
        } else if debug_mode {
            debug!("Format processor not available, using ValKey fallback");
        }
    }

    // ValKey fallback
    let result = execute_function(
        conn,
        "GNODE_LIST_FORMATS",
        &[],
        &[],
        site_id,
        debug_mode
    );

    match result {
        Ok(json_str) => {
            if debug_mode {
                debug!("ValKey list_formats fallback succeeded");
            }
            CommandResult::success_json(json_str)
        },
        Err(e) => {
            let error_msg = format!("Both Rust and ValKey list_formats failed: {}", e);
            log::error!("{}", error_msg);
            CommandResult::error(error_msg)
        }
    }
}

#[cfg(not(feature = "cms"))]
pub fn handle_list_formats(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling list_formats command: {}", command.id);
    }

    // ValKey only (format feature disabled)
    let result = execute_function(
        conn,
        "GNODE_LIST_FORMATS",
        &[],
        &[],
        site_id,
        debug_mode
    );

    match result {
        Ok(json_str) => {
            // ValKey function returns {"status":"ok","result":[...],"timestamp":...}
            // We need to unwrap and extract the inner "result" field
            match serde_json::from_str::<Value>(&json_str) {
                Ok(response) => {
                    if let Some(result_data) = response.get("result") {
                        CommandResult::success(result_data.clone())
                    } else {
                        // No result field, return the whole response
                        CommandResult::success(response)
                    }
                },
                Err(e) => {
                    warn!("Failed to parse ValKey function response: {}", e);
                    CommandResult::error(format!("Invalid response from ValKey function: {}", e))
                }
            }
        },
        Err(e) => CommandResult::error(format!("Error listing formats: {}", e)),
    }
}

/// Handle 'detect_format' command
#[cfg(feature = "cms")]
pub fn handle_detect_format(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling detect_format command: {}", command.id);
    }

    // Extract message from parameters
    let message = match command.parameters.get("message") {
        Some(Value::String(msg)) => msg,
        _ => return CommandResult::error("Missing or invalid message parameter"),
    };

    // Note: FormatProcessor doesn't expose public API for detect_format
    // Use ValKey implementation directly

    // ValKey fallback
    let result = execute_function(
        conn,
        "GNODE_DETECT_FORMAT",
        &[],
        &[message],
        site_id,
        debug_mode
    );

    match result {
        Ok(json_str) => {
            if debug_mode {
                debug!("ValKey detect_format fallback succeeded");
            }
            // ValKey function returns {"status":"ok","result":{...},"timestamp":...}
            // We need to unwrap and extract the inner "result" field
            match serde_json::from_str::<Value>(&json_str) {
                Ok(response) => {
                    if let Some(result_data) = response.get("result") {
                        CommandResult::success(result_data.clone())
                    } else {
                        // No result field, return the whole response
                        CommandResult::success(response)
                    }
                },
                Err(e) => {
                    warn!("Failed to parse ValKey function response: {}", e);
                    CommandResult::error(format!("Invalid response from ValKey function: {}", e))
                }
            }
        },
        Err(e) => {
            let error_msg = format!("Both Rust and ValKey detect_format failed: {}", e);
            log::error!("{}", error_msg);
            CommandResult::error(error_msg)
        }
    }
}

#[cfg(not(feature = "cms"))]
pub fn handle_detect_format(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling detect_format command: {}", command.id);
    }

    // Extract message from parameters
    let message = match command.parameters.get("message") {
        Some(Value::String(msg)) => msg,
        _ => return CommandResult::error("Missing or invalid message parameter"),
    };

    // ValKey only (format feature disabled)
    let result = execute_function(
        conn,
        "GNODE_DETECT_FORMAT",
        &[],
        &[message],
        site_id,
        debug_mode
    );

    match result {
        Ok(json_str) => {
            // ValKey function returns {"status":"ok","result":{...},"timestamp":...}
            // We need to unwrap and extract the inner "result" field
            match serde_json::from_str::<Value>(&json_str) {
                Ok(response) => {
                    if let Some(result_data) = response.get("result") {
                        CommandResult::success(result_data.clone())
                    } else {
                        // No result field, return the whole response
                        CommandResult::success(response)
                    }
                },
                Err(e) => {
                    warn!("Failed to parse ValKey function response: {}", e);
                    CommandResult::error(format!("Invalid response from ValKey function: {}", e))
                }
            }
        },
        Err(e) => CommandResult::error(format!("Error detecting format: {}", e)),
    }
}

/// Handle 'convert_format' command
#[cfg(feature = "cms")]
pub fn handle_convert_format(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling convert_format command: {}", command.id);
    }

    // Extract parameters
    let source_format = match command.parameters.get("source_format") {
        Some(Value::String(fmt)) => fmt,
        _ => return CommandResult::error("Missing or invalid source_format parameter"),
    };

    let source_version = match command.parameters.get("source_version") {
        Some(Value::String(ver)) => ver,
        _ => "1.0.0", // Default version
    };

    let target_format = match command.parameters.get("target_format") {
        Some(Value::String(fmt)) => fmt,
        _ => return CommandResult::error("Missing or invalid target_format parameter"),
    };

    let target_version = match command.parameters.get("target_version") {
        Some(Value::String(ver)) => ver,
        _ => "1.0.0", // Default version
    };

    let message = match command.parameters.get("message") {
        Some(Value::String(msg)) => msg,
        _ => return CommandResult::error("Missing or invalid message parameter"),
    };

    // Note: FormatProcessor doesn't expose public API for transform_from_to
    // Use ValKey implementation directly

    // ValKey fallback
    let result = execute_function(
        conn,
        "GNODE_CONVERT_FORMAT",
        &[],
        &[source_format, source_version, target_format, target_version, message],
        site_id,
        debug_mode
    );

    match result {
        Ok(json_str) => {
            if debug_mode {
                debug!("ValKey convert_format fallback succeeded");
            }

            // ValKey functions return wrapped responses: {"status":"ok","result":<data>,"timestamp":...}
            // The result field contains a JSON-encoded string (not an object) that needs to be parsed
            match serde_json::from_str::<Value>(&json_str) {
                Ok(valkey_response) => {
                    if let Some(result_value) = valkey_response.get("result") {
                        // If result is a string, parse it as JSON (ValKey encodes the converted message as a string)
                        if let Some(result_str) = result_value.as_str() {
                            // Parse the JSON string to get the actual converted message object
                            match serde_json::from_str::<Value>(result_str) {
                                Ok(parsed_result) => CommandResult::success(parsed_result),
                                Err(_) => {
                                    // If parsing fails, return the string as-is (might be an error message)
                                    CommandResult::success(result_value.clone())
                                }
                            }
                        } else {
                            // Result is not a string, return as-is
                            CommandResult::success(result_value.clone())
                        }
                    } else {
                        // Fallback: return the whole response if no result field
                        CommandResult::success_json(json_str)
                    }
                },
                Err(e) => {
                    // If parsing fails, try using the response as-is
                    log::warn!("Failed to parse ValKey response, using as-is: {}", e);
                    CommandResult::success_json(json_str)
                }
            }
        },
        Err(e) => {
            let error_msg = format!("Both Rust and ValKey convert_format failed: {}", e);
            log::error!("{}", error_msg);
            CommandResult::error(error_msg)
        }
    }
}

#[cfg(not(feature = "cms"))]
pub fn handle_convert_format(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling convert_format command: {}", command.id);
    }

    // Extract parameters
    let source_format = match command.parameters.get("source_format") {
        Some(Value::String(fmt)) => fmt,
        _ => return CommandResult::error("Missing or invalid source_format parameter"),
    };

    let source_version = match command.parameters.get("source_version") {
        Some(Value::String(ver)) => ver,
        _ => "1.0.0", // Default version
    };

    let target_format = match command.parameters.get("target_format") {
        Some(Value::String(fmt)) => fmt,
        _ => return CommandResult::error("Missing or invalid target_format parameter"),
    };

    let target_version = match command.parameters.get("target_version") {
        Some(Value::String(ver)) => ver,
        _ => "1.0.0", // Default version
    };

    let message = match command.parameters.get("message") {
        Some(Value::String(msg)) => msg,
        _ => return CommandResult::error("Missing or invalid message parameter"),
    };

    // ValKey only (format feature disabled)
    let result = execute_function(
        conn,
        "GNODE_CONVERT_FORMAT",
        &[],
        &[source_format, source_version, target_format, target_version, message],
        site_id,
        debug_mode
    );

    match result {
        Ok(json_str) => {
            // ValKey function returns {"status":"ok","result":{...},"timestamp":...}
            // We need to unwrap and extract the inner "result" field
            match serde_json::from_str::<Value>(&json_str) {
                Ok(response) => {
                    if let Some(result_data) = response.get("result") {
                        CommandResult::success(result_data.clone())
                    } else {
                        // No result field, return the whole response
                        CommandResult::success(response)
                    }
                },
                Err(e) => {
                    warn!("Failed to parse ValKey function response: {}", e);
                    CommandResult::error(format!("Invalid response from ValKey function: {}", e))
                }
            }
        },
        Err(e) => CommandResult::error(format!("Error converting format: {}", e)),
    }
}

// =========================================================================
// Async handlers
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

        let format_definition = match command.parameters.get("format_definition") {
            Some(definition) => definition.to_string(),
            _ => return CommandResult::error("Missing format_definition parameter"),
        };

        // Try Rust native implementation first
        #[cfg(feature = "cms")]
        {
            use crate::daemon::GNodeDaemon;
            if let Some(format_proc) = GNodeDaemon::get_format_processor_ref() {
                match format_proc.register_format_from_command(command) {
                    Ok(format_name) => {
                        if debug_mode {
                            debug!("Rust native format_register succeeded for: {}", format_name);
                        }
                        return CommandResult::success(json!({
                            "status": "registered",
                            "format_name": format_name,
                            "async": true
                        }));
                    },
                    Err(e) => {
                        if debug_mode {
                            debug!("Rust format_register failed: {}, trying ValKey fallback", e);
                        }
                    }
                }
            }
        }

        // ValKey async fallback
        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_REGISTER_FORMAT")
            .arg(0)
            .arg(&format_definition)
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => {
                match serde_json::from_str::<Value>(&json_str) {
                    Ok(response) => {
                        if let Some(result_data) = response.get("result") {
                            CommandResult::success(result_data.clone())
                        } else {
                            CommandResult::success(response)
                        }
                    },
                    Err(_) => CommandResult::success_json(json_str)
                }
            },
            Err(e) => CommandResult::error(format!("Error registering format: {}", e))
        }
    })
}

/// Async version of handle_list_formats
pub fn handle_list_formats_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async list_formats command: {}", command.id);
        }

        // Try Rust native implementation first
        #[cfg(feature = "cms")]
        {
            use crate::daemon::GNodeDaemon;
            if let Some(format_proc) = GNodeDaemon::get_format_processor_ref() {
                match format_proc.list_formats() {
                    Ok(formats) => {
                        if debug_mode {
                            debug!("Rust native list_formats succeeded");
                        }
                        return CommandResult::success(json!({
                            "formats": formats,
                            "async": true
                        }));
                    },
                    Err(e) => {
                        if debug_mode {
                            debug!("Rust list_formats failed: {}, trying ValKey fallback", e);
                        }
                    }
                }
            }
        }

        // ValKey async fallback
        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_LIST_FORMATS")
            .arg(0)
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => {
                match serde_json::from_str::<Value>(&json_str) {
                    Ok(response) => {
                        if let Some(result_data) = response.get("result") {
                            CommandResult::success(result_data.clone())
                        } else {
                            CommandResult::success(response)
                        }
                    },
                    Err(_) => CommandResult::success_json(json_str)
                }
            },
            Err(e) => CommandResult::error(format!("Error listing formats: {}", e))
        }
    })
}

/// Async version of handle_detect_format
pub fn handle_detect_format_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async detect_format command: {}", command.id);
        }

        let message = match command.parameters.get("message") {
            Some(Value::String(msg)) => msg.clone(),
            _ => return CommandResult::error("Missing or invalid message parameter"),
        };

        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_DETECT_FORMAT")
            .arg(0)
            .arg(&message)
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => {
                match serde_json::from_str::<Value>(&json_str) {
                    Ok(response) => {
                        if let Some(result_data) = response.get("result") {
                            CommandResult::success(result_data.clone())
                        } else {
                            CommandResult::success(response)
                        }
                    },
                    Err(_) => CommandResult::success_json(json_str)
                }
            },
            Err(e) => CommandResult::error(format!("Error detecting format: {}", e))
        }
    })
}

/// Async version of handle_convert_format
pub fn handle_convert_format_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async convert_format command: {}", command.id);
        }

        let source_format = match command.parameters.get("source_format") {
            Some(Value::String(fmt)) => fmt.clone(),
            _ => return CommandResult::error("Missing or invalid source_format parameter"),
        };

        let source_version = command.parameters.get("source_version")
            .and_then(|v| v.as_str())
            .unwrap_or("1.0.0")
            .to_string();

        let target_format = match command.parameters.get("target_format") {
            Some(Value::String(fmt)) => fmt.clone(),
            _ => return CommandResult::error("Missing or invalid target_format parameter"),
        };

        let target_version = command.parameters.get("target_version")
            .and_then(|v| v.as_str())
            .unwrap_or("1.0.0")
            .to_string();

        let message = match command.parameters.get("message") {
            Some(Value::String(msg)) => msg.clone(),
            _ => return CommandResult::error("Missing or invalid message parameter"),
        };

        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_CONVERT_FORMAT")
            .arg(0)
            .arg(&source_format)
            .arg(&source_version)
            .arg(&target_format)
            .arg(&target_version)
            .arg(&message)
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => {
                match serde_json::from_str::<Value>(&json_str) {
                    Ok(valkey_response) => {
                        if let Some(result_value) = valkey_response.get("result") {
                            if let Some(result_str) = result_value.as_str() {
                                match serde_json::from_str::<Value>(result_str) {
                                    Ok(parsed_result) => CommandResult::success(parsed_result),
                                    Err(_) => CommandResult::success(result_value.clone())
                                }
                            } else {
                                CommandResult::success(result_value.clone())
                            }
                        } else {
                            CommandResult::success_json(json_str)
                        }
                    },
                    Err(_) => CommandResult::success_json(json_str)
                }
            },
            Err(e) => CommandResult::error(format!("Error converting format: {}", e))
        }
    })
}
