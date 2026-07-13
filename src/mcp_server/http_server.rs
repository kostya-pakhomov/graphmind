//! MCP HTTP Server — streamable HTTP transport для GraphMind v2

use crate::mcp_server::protocol::{initialize_response, tools_list_response, tool_result_response};
use crate::mcp_server::handler::McpHandler;
use crate::actors::{S0Actor, L2Actor, L1Actor, L0Actor, SearchActor, ChainActor, WorkspaceManager, PlanActor, InferenceActor, CuriosityEngine, TrustFirewall};
use crate::queue::QueueProcessor;
use serde_json::Value;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tracing::{info, error, warn};
use tokio::sync::broadcast;
use uuid::Uuid;

/// Запустить MCP HTTP сервер
pub async fn run_mcp_http_server(
    s0: Arc<S0Actor>,
    l2: Arc<RwLock<L2Actor>>,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    run_mcp_http_server_with_queue(s0, l2, addr, None).await
}

/// HTTP-вариант с опциональным QueueProcessor (durable record_action/flush).
pub async fn run_mcp_http_server_with_queue(
    s0: Arc<S0Actor>,
    l2: Arc<RwLock<L2Actor>>,
    addr: &str,
    queue: Option<Arc<QueueProcessor>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    info!("MCP HTTP Server listening on {}", addr);

    let mut base = crate::mcp_server::handler::McpHandler::new(s0, l2);
    if let Some(q) = queue {
        base = base.with_queue(q);
        info!("MCP HTTP server: QueueProcessor attached (durable record_action/flush)");
    } else {
        warn!("MCP HTTP server: no QueueProcessor attached (record_action falls back to S0 push)");
    }
    let handler = Arc::new(base);
    run_http_listener_loop(listener, handler).await
}

/// HTTP-вариант с queue + WorkspaceManager (для detect/create/bootstrap).
pub async fn run_mcp_http_server_with_queue_and_workspace(
    s0: Arc<S0Actor>,
    l2: Arc<RwLock<L2Actor>>,
    addr: &str,
    queue: Option<Arc<QueueProcessor>>,
    workspace: Option<Arc<WorkspaceManager>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    info!("MCP HTTP Server listening on {}", addr);

    let mut base = crate::mcp_server::handler::McpHandler::new(s0, l2);
    if let Some(q) = queue {
        base = base.with_queue(q);
        info!("MCP HTTP server: QueueProcessor attached (durable record_action/flush)");
    } else {
        warn!("MCP HTTP server: no QueueProcessor attached (record_action falls back to S0 push)");
    }
    if let Some(w) = workspace {
        base = base.with_workspace_manager(w);
        info!("MCP HTTP server: WorkspaceManager attached (real workspace tools)");
    } else {
        warn!("MCP HTTP server: no WorkspaceManager attached (workspace tools will return error)");
    }
    let handler = Arc::new(base);
    run_http_listener_loop(listener, handler).await
}

/// HTTP-вариант с ПОЛНЫМ набором акторов (search/chain/l1/l0/plan) — паритет со stdio.
///
/// Раньше HTTP-транспорт поднимал только S0/L2/Queue/Workspace, из-за чего
/// memory_query/search/get_chain/consolidate/plan_* возвращали *_NOT_INITIALIZED.
/// Теперь оба транспорта строят один и тот же handler (`build_full`), так что
/// gRPC-мост как способ дотянуться до полного набора инструментов больше не нужен.
#[allow(clippy::too_many_arguments)]
pub async fn run_mcp_http_server_full_with_all(
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
    addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    info!("MCP HTTP Server (full parity) listening on {}", addr);
    let mut handler = McpHandler::build_full(
        s0, l2, Some(search), Some(chain), queue, workspace, l1, l0, plan,
    );
    if let Some(inf) = inference {
        handler = handler.with_inference(inf);
    }
    if let Some(cur) = curiosity {
        handler = handler.with_curiosity(cur);
    }
    if let Some(tr) = trust {
        handler = handler.with_trust(tr);
    }
    if let Some(tx) = event_tx {
        handler = handler.with_event_tx(tx);
    }
    if let Some(orch) = orchestrator {
        handler = handler.with_orchestrator(orch);
    }
    run_http_listener_loop(listener, Arc::new(handler)).await
}

/// Общий цикл приёма соединений (выделен, чтобы не дублировать код в двух entrypoints).
async fn run_http_listener_loop(
    listener: TcpListener,
    handler: Arc<crate::mcp_server::handler::McpHandler>,
) -> Result<(), Box<dyn std::error::Error>> {
    let sessions: Arc<RwLock<HashMap<String, (broadcast::Sender<String>, broadcast::Receiver<String>)>>>
        = Arc::new(RwLock::new(HashMap::new()));
    // Тир-гейт open-core: по умолчанию обслуживаем локальную машину/приватную сеть,
    // публичные источники отклоняем (GRAPHMIND_ALLOW_EXTERNAL=true открывает наружу).
    let allow_external = crate::mcp_server::net_guard::allow_external_from_env();
    if allow_external {
        info!("MCP HTTP: GRAPHMIND_ALLOW_EXTERNAL=true — внешние подключения разрешены (командный/облачный тир)");
    }
    loop {
        let (socket, peer_addr) = listener.accept().await?;
        if !crate::mcp_server::net_guard::is_source_allowed(peer_addr.ip(), allow_external) {
            warn!(
                "MCP HTTP: внешнее подключение {} отклонено (GRAPHMIND_ALLOW_EXTERNAL=false — локальный/Free-контур; для команды/облака выставьте =true)",
                peer_addr.ip()
            );
            drop(socket);
            continue;
        }
        let handler = handler.clone();
        let sessions = sessions.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, handler, sessions).await {
                error!("Connection error from {}: {}", peer_addr, e);
            }
        });
    }
}

/// Обработать HTTP соединение
async fn handle_connection(
    socket: tokio::net::TcpStream,
    handler: Arc<crate::mcp_server::handler::McpHandler>,
    sessions: Arc<RwLock<HashMap<String, (broadcast::Sender<String>, broadcast::Receiver<String>)>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, mut writer) = socket.into_split();
    let mut buf_reader = BufReader::new(reader);
    
    let mut request_line = String::new();
    if buf_reader.read_line(&mut request_line).await? == 0 { return Ok(()); }
    
    let parts: Vec<&str> = request_line.trim().split_whitespace().collect();
    if parts.len() < 2 { return Ok(()); }
    let (method, full_path) = (parts[0], parts[1]);
    // Разделяем путь и query parameters
    let path = full_path.split('?').next().unwrap_or(full_path);

    let mut headers = std::collections::HashMap::new();
    loop {
        let mut line = String::new();
        buf_reader.read_line(&mut line).await?;
        if line.trim().is_empty() { break; }
        if let Some((k, v)) = line.trim().split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }

    info!("Received request: {} {}", method, path);

    if method == "GET" && path == "/sse" {
        // Создаем новую сессию
        let session_id = Uuid::new_v4().to_string();
        let (tx, rx) = broadcast::channel::<String>(100);
        
        // Сохраняем сессию
        {
            let mut sessions_write = sessions.write().await;
            sessions_write.insert(session_id.clone(), (tx, rx));
        }
        info!("Created new SSE session: {}", session_id);
        
        // Отправляем заголовки с session_id
        let response_headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\nMCP-Session-ID: {}\r\n\r\n",
            session_id
        );
        writer.write_all(response_headers.as_bytes()).await?;
        
        // Отправляем относительный путь для POST-запросов
        let endpoint_msg = format!("event: endpoint\ndata: /message?session_id={}\n\n", session_id);
        writer.write_all(endpoint_msg.as_bytes()).await?;
        writer.flush().await?;

        // Подписываемся на сообщения этой сессии
        let mut rx = {
            let sessions_read = sessions.read().await;
            sessions_read.get(&session_id)
                .map(|(tx, _)| tx.subscribe())
                .ok_or("Session not found")?
        };
        
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Ok(m) => {
                            info!("SSE: Sending message to client: {}", m);
                            // Отправляем только data: {json}\n\n
                            let sse_msg = format!("data: {}\n\n", m);
                            
                            if writer.write_all(sse_msg.as_bytes()).await.is_err() {
                                break;
                            }
                            if writer.flush().await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    // Keep-alive ping
                    writer.write_all(b":\n\n").await?;
                    if writer.flush().await.is_err() {
                        break;
                    }
                }
            }
        }
        
        // Удаляем сессию при закрытии
        let mut sessions_write = sessions.write().await;
        sessions_write.remove(&session_id);
        info!("Closed SSE session: {}", session_id);
    } else if method == "POST" && path == "/message" {
        let len: usize = headers.get("content-length").and_then(|l| l.parse().ok()).unwrap_or(0);
        let mut body = vec![0u8; len];
        buf_reader.read_exact(&mut body).await?;
        let body_str = String::from_utf8_lossy(&body);

        // Извлекаем session_id из query параметров или заголовка
        let mut session_id = if let Some(query) = full_path.split('?').nth(1) {
            query.split('&')
                .find(|p| p.starts_with("session_id="))
                .and_then(|p| p.split('=').nth(1))
                .map(|s| s.to_string())
                .unwrap_or_else(|| headers.get("mcp-session-id").cloned().unwrap_or_default())
        } else {
            headers.get("mcp-session-id").cloned().unwrap_or_default()
        };

        // Если session_id всё ещё пустой, берем последнюю активную сессию
        if session_id.is_empty() {
            let sessions_read = sessions.read().await;
            if let Some(last_id) = sessions_read.keys().last() {
                session_id = last_id.clone();
                info!("Using last active session for POST: {}", session_id);
            }
        }

        let response = handle_mcp_request(&body_str, &handler).await;
        let response_json = serde_json::to_string(&response)?;
        
        info!("Sending response to session {}: {}", session_id, response_json);
        
        // Отправляем результат в SSE канал этой сессии
        {
            let sessions_read = sessions.read().await;
            if !session_id.is_empty() {
                if let Some((tx, _)) = sessions_read.get(&session_id) {
                    let _ = tx.send(response_json);
                } else {
                    warn!("Session not found for POST: {}", session_id);
                    // Хак: если сессия не найдена, шлем во все активные
                    for (id, (tx, _)) in sessions_read.iter() {
                        info!("Broadcasting to fallback session: {}", id);
                        let _ = tx.send(response_json.clone());
                    }
                }
            } else {
                // Если session_id вообще нет, шлем во все активные
                warn!("No session_id in POST, broadcasting to all sessions");
                for (id, (tx, _)) in sessions_read.iter() {
                    info!("Broadcasting to session: {}", id);
                    let _ = tx.send(response_json.clone());
                }
            }
        }

        // Отвечаем 200 OK с CORS
        let response_headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\nAccess-Control-Allow-Origin: *\r\nContent-Type: application/json\r\nMCP-Session-ID: {}\r\n\r\n",
            session_id
        );
        writer.write_all(response_headers.as_bytes()).await?;
        return Ok(());
    } else if method == "POST" && path == "/mcp" {
        // Streamable HTTP (MCP 2025-03-26): JSON-RPC в ТЕЛЕ POST, ответ — прямо в теле HTTP
        // (не окольно через SSE-канал, как /message). Переиспользуем handle_mcp_request.
        // Для клиентов Cursor / Codex / OpenCode, которым классический HTTP+SSE не подходит.
        let len: usize = headers.get("content-length").and_then(|l| l.parse().ok()).unwrap_or(0);
        let mut body = vec![0u8; len];
        buf_reader.read_exact(&mut body).await?;
        let body_str = String::from_utf8_lossy(&body);

        let parsed: Option<Value> = serde_json::from_str(&body_str).ok();
        let is_notification = parsed.as_ref()
            .map(|v| v.get("method").is_some() && v.get("id").is_none())
            .unwrap_or(false);
        let is_initialize = parsed.as_ref()
            .and_then(|v| v.get("method").and_then(|m| m.as_str()))
            .map(|m| m == "initialize")
            .unwrap_or(false);

        if is_notification {
            // Уведомление (напр. notifications/initialized) — тела ответа нет.
            let resp = "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n";
            writer.write_all(resp.as_bytes()).await?;
            return Ok(());
        }

        let response = handle_mcp_request(&body_str, &handler).await;
        let response_body = serde_json::to_string(&response)?;
        // Сессия по спеке присваивается на initialize. Сервер stateless на уровне HTTP
        // (состояние — в RocksDB), поэтому id выдаём на initialize для клиентов, что его ждут,
        // и не валидируем строго на последующих запросах.
        let session_line = if is_initialize {
            format!("Mcp-Session-Id: {}\r\n", Uuid::new_v4())
        } else {
            String::new()
        };
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{}Access-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{}",
            response_body.len(), session_line, response_body
        );
        writer.write_all(resp.as_bytes()).await?;
        return Ok(());
    } else if (method == "GET" || method == "DELETE") && path == "/mcp" {
        // GET /mcp: серверный SSE-поток не предлагаем → 405 (спек-корректно).
        // DELETE /mcp: завершение сессии — принимаем как no-op (состояние в RocksDB).
        let (code, msg) = if method == "DELETE" {
            ("200 OK", "{}")
        } else {
            ("405 Method Not Allowed", "{\"error\":\"SSE stream not offered; use POST /mcp\"}")
        };
        let resp = format!(
            "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nAllow: POST, DELETE\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{}",
            code, msg.len(), msg
        );
        writer.write_all(resp.as_bytes()).await?;
        return Ok(());
    } else if method == "GET" && path == "/health" {
        let body = "{\"status\":\"ok\",\"service\":\"graphmind-v2-mcp\"}";
        let resp = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
        writer.write_all(resp.as_bytes()).await?;
    } else {
        // 404 Not Found
        let body = "{\"error\":\"Not Found\"}";
        let resp = format!("HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
        writer.write_all(resp.as_bytes()).await?;
        warn!("Unknown request: {} {}", method, path);
    }
    
    Ok(())
}

/// Обработать MCP JSON-RPC запрос
async fn handle_mcp_request(body: &str, handler: &McpHandler) -> Value {
    info!("Processing MCP request: {}", body);
    let raw: Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => {
            return serde_json::json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {
                    "code": -32700,
                    "message": format!("Invalid JSON: {}", e)
                }
            });
        }
    };
    
    // Проверяем jsonrpc версию
    if raw.get("jsonrpc").and_then(|v| v.as_str()) != Some("2.0") {
        return serde_json::json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": {
                "code": -32600,
                "message": "Invalid JSON-RPC version"
            }
        });
    }
    
    let id = raw.get("id").cloned().unwrap_or(Value::Null);
    let method = match raw.get("method").and_then(|v| v.as_str()) {
        Some(m) => m.to_string(),
        None => {
            return serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32600,
                    "message": "Missing method"
                }
            });
        }
    };
    let params = raw.get("params").cloned().unwrap_or(Value::Null);
    
    // Обрабатываем метод
    let result = match method.as_str() {
        "initialize" => {
            initialize_response("2024-11-05")
        },
        "tools/list" => tools_list_response(&McpHandler::list_tools()),
        "tools/call" => {
            let name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);
            let tool_result = handler.handle_tool(name, arguments).await;
            tool_result_response(tool_result)
        }
        "ping" => serde_json::json!({}),
        _ => {
            return serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32601,
                    "message": format!("Method not found: {}", method)
                }
            });
        }
    };
    
    // JSON-RPC 2.0: успешный ответ содержит result и НЕ содержит error
    // (было "error": null → невалидно, строгие клиенты, напр. Claude Code, отвергают).
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}
