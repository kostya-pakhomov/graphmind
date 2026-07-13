//! MCP Protocol — JSON-RPC 2.0 реализация для MCP

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// JSON-RPC запрос
#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Value,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

/// JSON-RPC ответ
#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

/// MCP инициализация запрос
#[derive(Debug, Deserialize)]
pub struct InitializeRequest {
    pub protocol_version: String,
    pub capabilities: Value,
    pub client_info: ClientInfo,
}

#[derive(Debug, Deserialize)]
pub struct ClientInfo {
    pub name: String,
    pub version: Option<String>,
}

/// MCP инициализация ответ
pub fn initialize_response(version: &str) -> Value {
    serde_json::json!({
        "protocolVersion": version,
        "capabilities": {
            "tools": {}
        },
        "serverInfo": {
            "name": "graphmind-v2",
            "version": "0.1.0"
        }
    })
}

/// MCP список инструментов ответ
pub fn tools_list_response(tools: &[Value]) -> Value {
    serde_json::json!({
        "tools": tools
    })
}

/// MCP вызов инструмента ответ
pub fn tool_result_response(result: Value) -> Value {
    // Преобразуем результат в строку, так как MCP требует text как строку
    let text = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
    serde_json::json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ]
    })
}
