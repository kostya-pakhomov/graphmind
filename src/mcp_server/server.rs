//! MCP Server — stdio transport + event loop

use crate::mcp_server::protocol::{JsonRpcResponse, JsonRpcError, initialize_response, tools_list_response, tool_result_response};
use crate::mcp_server::handler::McpHandler;
use crate::actors::{S0Actor, L2Actor, L1Actor, L0Actor, SearchActor, ChainActor, WorkspaceManager, PlanActor, InferenceActor, CuriosityEngine, TrustFirewall};
use crate::queue::QueueProcessor;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::RwLock;
use std::sync::Arc;
use tracing::{info, warn, error};

/// Запустить MCP сервер (базовая версия без SearchActor и ChainActor)
pub async fn run_mcp_server(
    s0: Arc<S0Actor>,
    l2: Arc<RwLock<L2Actor>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let handler = McpHandler::new(s0, l2);
    run_mcp_server_with_handler(handler).await
}

/// Запустить MCP сервер с SearchActor и ChainActor
pub async fn run_mcp_server_full(
    s0: Arc<S0Actor>,
    l2: Arc<RwLock<L2Actor>>,
    search: Arc<RwLock<SearchActor>>,
    chain: Arc<ChainActor>,
) -> Result<(), Box<dyn std::error::Error>> {
    let handler = McpHandler::with_search_and_chain(s0, l2, search, chain);
    run_mcp_server_with_handler(handler).await
}

/// Запустить MCP сервер (полный набор + QueueProcessor для durable pipeline)
///
/// Если `queue` задан, `record_action` и `flush_session_memory` пишут в
/// `pending_actions.json` (durable path). Если None — fallback на прямой
/// `S0Actor::push` (см. `McpHandler`).
pub async fn run_mcp_server_full_with_queue(
    s0: Arc<S0Actor>,
    l2: Arc<RwLock<L2Actor>>,
    search: Arc<RwLock<SearchActor>>,
    chain: Arc<ChainActor>,
    queue: Option<Arc<QueueProcessor>>,
) -> Result<(), Box<dyn std::error::Error>> {
    run_mcp_server_full_with_queue_and_workspace(s0, l2, search, chain, queue, None).await
}

/// Полный набор: queue + WorkspaceManager (для detect_workspace_from_context,
/// create_workspace, bootstrap_memory).
pub async fn run_mcp_server_full_with_queue_and_workspace(
    s0: Arc<S0Actor>,
    l2: Arc<RwLock<L2Actor>>,
    search: Arc<RwLock<SearchActor>>,
    chain: Arc<ChainActor>,
    queue: Option<Arc<QueueProcessor>>,
    workspace: Option<Arc<WorkspaceManager>>,
) -> Result<(), Box<dyn std::error::Error>> {
    run_mcp_server_full_with_all(
        s0,
        l2,
        search,
        chain,
        queue,
        workspace,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

/// Полный набор + L1 + L0 + Plan (для consolidate_workspace + 12 plan_* tools).
#[allow(clippy::too_many_arguments)]
pub async fn run_mcp_server_full_with_all(
    s0: Arc<S0Actor>,
    l2: Arc<RwLock<L2Actor>>,
    search: Arc<RwLock<SearchActor>>,
    chain: Arc<ChainActor>,
    queue: Option<Arc<QueueProcessor>>,
    workspace: Option<Arc<WorkspaceManager>>,
    l1: Option<Arc<RwLock<L1Actor>>>,
    l0: Option<Arc<RwLock<L0Actor>>>,
    plan: Option<Arc<RwLock<PlanActor>>>,
    inference: Option<Arc<InferenceActor>>,
    curiosity: Option<Arc<CuriosityEngine>>,
    trust: Option<Arc<TrustFirewall>>,
    event_tx: Option<tokio::sync::mpsc::UnboundedSender<crate::actors::MemoryEvent>>,
    orchestrator: Option<Arc<crate::actors::MemoryOrchestrator>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = McpHandler::with_search_and_chain(s0, l2, search, chain);
    if let Some(q) = queue {
        handler = handler.with_queue(q);
        info!("MCP server: QueueProcessor attached (durable record_action/flush)");
    } else {
        warn!("MCP server: no QueueProcessor attached (record_action falls back to S0 push)");
    }
    if let Some(w) = workspace {
        handler = handler.with_workspace_manager(w);
        info!("MCP server: WorkspaceManager attached (real detect/create/bootstrap)");
    } else {
        warn!("MCP server: no WorkspaceManager attached (workspace tools will return error)");
    }
    if let Some(l1a) = l1 {
        handler = handler.with_l1(l1a);
        info!("MCP server: L1Actor attached (consolidate_workspace can autogen L1)");
    }
    if let Some(l0a) = l0 {
        handler = handler.with_l0(l0a);
        info!("MCP server: L0Actor attached (consolidate_workspace can autogen L0)");
    }
    if let Some(pa) = plan {
        handler = handler.with_plan(pa);
        info!("MCP server: PlanActor attached (12 plan_* tools enabled)");
    }
    if let Some(inf) = inference {
        handler = handler.with_inference(inf);
        info!("MCP server: InferenceActor attached (causal reasoning tools)");
    }
    if let Some(cur) = curiosity {
        handler = handler.with_curiosity(cur);
        info!("MCP server: CuriosityEngine attached (irritation/curiosity tools)");
    }
    if let Some(tr) = trust {
        handler = handler.with_trust(tr);
        info!("MCP server: TrustFirewall attached (verify_input)");
    }
    if let Some(tx) = event_tx {
        handler = handler.with_event_tx(tx);
    }
    if let Some(orch) = orchestrator {
        handler = handler.with_orchestrator(orch);
        info!("MCP server: MemoryOrchestrator attached (orchestrator_status, CycleTrigger)");
    }
    run_mcp_server_with_handler(handler).await
}

/// Запустить MCP сервер с готовым handler
async fn run_mcp_server_with_handler(handler: McpHandler) -> Result<(), Box<dyn std::error::Error>> {
    let stdin = tokio::io::stdin();
    let mut writer = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);
    
    writer.flush().await?;
    
    let mut line = String::new();
    // Принудительно читаем первую строку максимально быстро
    if let Ok(_) = reader.read_line(&mut line).await {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            if let Some(response) = handle_request(trimmed, &handler).await {
                let response_json = serde_json::to_string(&response)?;
                writer.write_all(response_json.as_bytes()).await?;
                writer.write_all(b"\n").await?;
                writer.flush().await?;
            }
        }
    }

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                // EOF - клиент закрыл соединение
                break;
            }
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                
                info!("MCP Server: received request: {}", trimmed);
                
                // Обработать JSON-RPC запрос
                match handle_request(trimmed, &handler).await {
                    Some(response) => {
                        let response_json = serde_json::to_string(&response)?;
                        writer.write_all(response_json.as_bytes()).await?;
                        writer.write_all(b"\n").await?;
                        writer.flush().await?;
                        info!("MCP Server: sent response: {}", response_json);
                    }
                    None => {
                        info!("MCP Server: notification or empty request received");
                    }
                }
            }
            Err(_) => {
                break;
            }
        }
    }
    
    Ok(())
}

/// Обработать JSON-RPC запрос
async fn handle_request(line: &str, handler: &McpHandler) -> Option<JsonRpcResponse> {
    // Парсим JSON
    let raw: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            return Some(JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: Value::Null,
                result: None,
                error: Some(JsonRpcError {
                    code: -32700, // Parse error
                    message: format!("Invalid JSON: {}", e),
                }),
            });
        }
    };
    
    // Проверяем jsonrpc версию
    if raw.get("jsonrpc").and_then(|v| v.as_str()) != Some("2.0") {
        return Some(JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id: Value::Null,
            result: None,
            error: Some(JsonRpcError {
                code: -32600, // Invalid Request
                message: "Invalid JSON-RPC version".to_string(),
            }),
        });
    }
    
    // Извлекаем id, method, params
    let id = raw.get("id").cloned().unwrap_or(Value::Null);
    let method = match raw.get("method").and_then(|v| v.as_str()) {
        Some(m) => m.to_string(),
        None => {
            return Some(JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id,
                result: None,
                error: Some(JsonRpcError {
                    code: -32600,
                    message: "Missing method".to_string(),
                }),
            });
        }
    };
    let params = raw.get("params").cloned().unwrap_or(Value::Null);
    
    // Если нет id - это notification, не отвечаем
    let has_id = raw.get("id").is_some();
    
    // Специальная обработка notification'ов (методы без id)
    if !has_id {
        // notifications/initialized - просто игнорируем, как требует MCP spec
        if method == "notifications/initialized" {
            return None;
        }
        // Другие notification'ы тоже игнорируем
        return None;
    }
    
    // Обрабатываем метод
    let result = match method.as_str() {
        "initialize" => {
            // MCP initialization
            let version = params.get("protocolVersion").and_then(|v| v.as_str()).unwrap_or("2024-11-05");
            initialize_response(version)
        }
        "tools/list" => {
            // Список инструментов
            tools_list_response(&McpHandler::list_tools())
        }
        "tools/call" => {
            // Вызов инструмента
            let name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);
            let tool_result = handler.handle_tool(name, arguments).await;
            tool_result_response(tool_result)
        }
        "ping" => {
            // Ping - просто возвращаем пустой ответ
            serde_json::json!({})
        }
        // Standard MCP methods that Kodik may call - return empty results
        "resources/list" | "resources/templates/list" | "resources/read" | "prompts/list" | "logging/setLevel" => {
            // These are not supported by this server, return empty results
            match method.as_str() {
                "resources/list" | "resources/templates/list" => serde_json::json!({"resources": []}),
                "resources/read" => serde_json::json!({"contents": []}),
                "prompts/list" => serde_json::json!({"prompts": []}),
                _ => serde_json::json!({})
            }
        }
        _ => {
            // Неизвестный метод
            return Some(JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id,
                result: None,
                error: Some(JsonRpcError {
                    code: -32601, // Method not found
                    message: format!("Method not found: {}", method),
                }),
            });
        }
    };
    
    // Если нет id - не отвечаем (notification)
    if !has_id {
        return None;
    }
    
    Some(JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id,
        result: Some(result),
        error: None,
    })
}
