// WebSocket JSON-RPC handler: единый канал для команд и событий.
// Клиент отправляет {method, params, id} → сервер отвечает {id, result}.
// Сервер push-ит {event, data} при новых узлах, flush, action.

use std::sync::Arc;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use futures_util::{StreamExt, SinkExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::WebState;
use crate::graph::{Level, Node, NodeId};
use crate::actors::{Plan, PlanStatus};

/// WS upgrade handler: /ws
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_connection(socket, state))
}

/// Главная петля: читаем сообщения, обрабатываем, отвечаем.
/// Каждая команда обрабатывается в отдельном tokio::spawn — panic не убивает сервер.
async fn handle_connection(socket: WebSocket, state: Arc<WebState>) {
    eprintln!("graphmind-v2: WS client connected");
    let (mut sender, mut receiver) = socket.split();

    // Канал для отправки сообщений обратно клиенту
    let (tx, mut rx) = mpsc::channel::<String>(32);

    // Задача: читаем из канала и отправляем в WebSocket
    let send_task = tokio::spawn(async move {
        while let Some(text) = rx.recv().await {
            if text.is_empty() {
                // Пустая строка = ping (keepalive)
                if sender.send(Message::Ping(vec![])).await.is_err() {
                    break;
                }
            } else {
                if sender.send(Message::Text(text)).await.is_err() {
                    break;
                }
            }
        }
    });

    // Keepalive: ping каждые 30s
    let tx_keepalive = tx.clone();
    let keepalive = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        interval.tick().await;
        loop {
            interval.tick().await;
            if tx_keepalive.send(String::new()).await.is_err() {
                break;
            }
        }
    });

    // Читаем входящие сообщения
    while let Some(msg_result) = receiver.next().await {
        let msg = match msg_result {
            Ok(m) => m,
            Err(e) => {
                eprintln!("graphmind-v2: WS recv error: {}", e);
                break;
            }
        };
        match msg {
            Message::Text(text) => {
                let req: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = send_msg(&tx, json!({"id": null, "error": {"message": format!("invalid JSON: {}", e)}})).await;
                        continue;
                    }
                };
                let id = req.get("id").cloned().unwrap_or(Value::Null);
                let method = req.get("method").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let params = req.get("params").cloned().unwrap_or(json!({}));

                // Обработка в отдельном spawn — panic изолирован
                let tx2 = tx.clone();
                let state2 = state.clone();
                tokio::spawn(async move {
                    let result = dispatch_method(&method, &params, &state2).await;
                    let _ = send_msg(&tx2, json!({"id": id, "result": result})).await;
                });
            }
            Message::Pong(_) => { /* keepalive ответ */ }
            Message::Close(_) => {
                eprintln!("graphmind-v2: WS client closed connection");
                break;
            }
            _ => {}
        }
    }

    keepalive.abort();
    send_task.abort();
    eprintln!("graphmind-v2: WS client disconnected");
}

/// Отправить JSON-сообщение через канал в WebSocket.
async fn send_msg(tx: &mpsc::Sender<String>, msg: Value) -> Result<(), ()> {
    let text = serde_json::to_string(&msg).map_err(|_| ())?;
    tx.send(text).await.map_err(|_| ())
}

/// Маршрутизация method → handler.
async fn dispatch_method(method: &str, params: &Value, state: &Arc<WebState>) -> Value {
    match method {
        "status" => cmd_status(state).await,
        "connections" => cmd_connections(state).await,
        "s0" => cmd_s0(state).await,
        "workspaces.list" => cmd_workspaces_list(state).await,
        "workspaces.archive" => cmd_workspaces_archive(params, state).await,
        "nodes.list" => cmd_nodes_list(params, state).await,
        "nodes.search" => cmd_nodes_search(params, state).await,
        "chain.get" => cmd_chain_get(params, state).await,
        "risks.predict" => cmd_risks_predict(params, state).await,
        "plans.list" => cmd_plans_list(params, state).await,
        "plans.create" => cmd_plans_create(params, state).await,
        "plans.get" => cmd_plans_get(params, state).await,
        "plans.delete" => cmd_plans_delete(params, state).await,
        "plans.approve" => cmd_plans_approve(params, state).await,
        "plans.reject" => cmd_plans_reject(params, state).await,
        "plans.decompose" => cmd_plans_decompose(params, state).await,
        "plans.claim" => cmd_plans_claim(params, state).await,
        "plans.complete" => cmd_plans_complete(params, state).await,
        "plans.problem" => cmd_plans_problem(params, state).await,
        "plans.resolve" => cmd_plans_resolve(params, state).await,
        "plans.archive" => cmd_plans_archive(params, state).await,
        "plans.update" => cmd_plans_update(params, state).await,
        "chat.send" => cmd_chat_send(params, state).await,
        "settings.get" => cmd_settings_get(state).await,
        "settings.set" => cmd_settings_set(params).await,
        _ => json!({"error": {"message": format!("unknown method: {}", method)}}),
    }
}

// === Status ===

async fn cmd_status(state: &Arc<WebState>) -> Value {
    let uptime = state.start_time.elapsed().as_secs();
    let s0_count = state.s0.len().await;
    let ws_count = match &state.workspace {
        Some(ws) => ws.list_workspaces(None).await.map(|v| v.len()).unwrap_or(0),
        None => 0,
    };
    let l2_count = {
        let l2 = state.l2.read().await;
        l2.list_all_nodes().await.map(|v| v.len()).unwrap_or(0)
    };
    json!({
        "uptime_secs": uptime,
        "uptime_human": format_uptime(uptime),
        "s0_count": s0_count,
        "l2_count": l2_count,
        "workspace_count": ws_count,
        "version": env!("CARGO_PKG_VERSION"),
    })
}

async fn cmd_connections(state: &Arc<WebState>) -> Value {
    let s0_count = state.s0.len().await;
    json!({
        "mcp_transport": "stdio",
        "mcp_connected": true,
        "s0_entries": s0_count,
        "uptime_secs": state.start_time.elapsed().as_secs(),
    })
}

async fn cmd_s0(state: &Arc<WebState>) -> Value {
    let entries = state.s0.get_recent(20).await;
    let items: Vec<Value> = entries.iter().map(|e| json!({
        "id": e.id,
        "summary": e.summary,
        "timestamp": e.timestamp.to_rfc3339(),
    })).collect();
    json!({"entries": items, "total": items.len()})
}

// === Workspaces ===

async fn cmd_workspaces_list(state: &Arc<WebState>) -> Value {
    let workspaces = match &state.workspace {
        Some(ws) => ws.list_workspaces(None).await.unwrap_or_default(),
        None => return json!({"workspaces": [], "active": null}),
    };
    let active = match &state.workspace {
        Some(ws) => ws.get_active_workspace_id().await,
        None => None,
    };
    let l2 = state.l2.read().await;
    let mut items = Vec::new();
    for ws_info in &workspaces {
        let node_count = l2.count_by_workspace(&ws_info.id).await.unwrap_or(0);
        items.push(json!({
            "id": ws_info.id,
            "name": ws_info.name,
            "path": ws_info.path,
            "status": format!("{:?}", ws_info.status),
            "node_count": node_count,
            "is_active": active.as_deref() == Some(&ws_info.id),
        }));
    }
    json!({"workspaces": items, "active": active})
}

async fn cmd_workspaces_archive(params: &Value, state: &Arc<WebState>) -> Value {
    let id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    match &state.workspace {
        Some(ws) => match ws.archive_workspace(id).await {
            Ok(true) => json!({"ok": true}),
            Ok(false) => json!({"error": "not found"}),
            Err(e) => json!({"error": e.to_string()}),
        },
        None => json!({"error": "workspace manager not available"}),
    }
}

// === Nodes ===

async fn cmd_nodes_list(params: &Value, state: &Arc<WebState>) -> Value {
    let level = params.get("level").and_then(|v| v.as_str());
    let workspace_id = params.get("workspace_id").and_then(|v| v.as_str());
    let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
    let limit = limit.min(500);

    let l2 = state.l2.read().await;
    let nodes = match workspace_id {
        Some(ws_id) => l2.list_by_workspace(ws_id).await.unwrap_or_default(),
        None => l2.list_all_nodes().await.unwrap_or_default(),
    };
    let filtered: Vec<Value> = nodes.into_iter()
        .filter(|n| match level {
            Some(lvl) => format!("{:?}", n.level).eq_ignore_ascii_case(lvl),
            None => true,
        })
        .take(limit)
        .map(|n| node_to_json(&n))
        .collect();
    json!({"nodes": filtered, "total": filtered.len()})
}

async fn cmd_nodes_search(params: &Value, state: &Arc<WebState>) -> Value {
    let q = params.get("q").and_then(|v| v.as_str()).unwrap_or("");
    let level = params.get("level").and_then(|v| v.as_str());
    let workspace_id = params.get("workspace_id").and_then(|v| v.as_str());
    let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(20) as usize;

    let results = match &state.search {
        Some(search) => {
            let search = search.read().await;
            let keywords: Vec<String> = q.split_whitespace().map(|w| w.to_lowercase()).collect();
            let filters = crate::actors::SearchFilters {
                level: parse_level(&level.map(|s| s.to_string())),
                status: None,
                node_type: None,
                tags: Vec::new(),
                workspace_id: workspace_id.map(|s| s.to_string()),
            };
            search.keyword_search(&keywords, limit, &filters).await.unwrap_or_default()
        }
        None => return json!({"results": [], "total": 0}),
    };
    let items: Vec<Value> = results.iter().map(|r| json!({
        "id": r.node_id,
        "content": r.content,
        "level": format!("{:?}", r.level),
        "node_type": format!("{:?}", r.node_type),
        "score": r.score,
        "workspace_id": r.metadata.workspace_id,
    })).collect();
    json!({"results": items, "total": items.len()})
}

// === Chain ===

async fn cmd_chain_get(params: &Value, state: &Arc<WebState>) -> Value {
    let id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let dir = params.get("dir").and_then(|v| v.as_str()).unwrap_or("backward");
    match &state.chain {
        Some(chain) => {
            let res = match dir {
                "forward_pre" => chain.chain_forward_pre(&NodeId(id.into()), 5).await,
                "forward_post" => chain.chain_forward_post(&NodeId(id.into()), 5).await,
                _ => chain.chain_backward_from_node(&NodeId(id.into()), 5).await,
            };
            match res {
                Ok(r) => json!({
                    "entries": r.entries.iter().map(|e| json!({
                        "node_id": e.node_id,
                        "depth": e.depth,
                        "relation": e.relation.map(|r| format!("{:?}", r)),
                    })).collect::<Vec<_>>(),
                    "reached_root": r.reached_root,
                    "max_depth_reached": r.max_depth_reached,
                }),
                Err(e) => json!({"error": e.to_string()}),
            }
        }
        None => json!({"error": "chain actor not available"}),
    }
}

async fn cmd_risks_predict(params: &Value, state: &Arc<WebState>) -> Value {
    let id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    match &state.inference {
        Some(inf) => match inf.predict_risks(&NodeId(id.into())).await {
            Ok(r) => serde_json::to_value(&r).unwrap_or(json!({"error": "serialize failed"})),
            Err(e) => json!({"error": e.to_string()}),
        },
        None => json!({"error": "inference actor not available"}),
    }
}

// === Plans ===

async fn cmd_plans_list(params: &Value, state: &Arc<WebState>) -> Value {
    let status = params.get("status").and_then(|v| v.as_str());
    let level = params.get("level").and_then(|v| v.as_str());
    let plans = match &state.plan {
        Some(plan) => {
            let plan = plan.read().await;
            let status_filter = match status {
                Some("created") => Some(PlanStatus::Created),
                Some("in_progress") => Some(PlanStatus::InProgress),
                Some("pending_review") => Some(PlanStatus::PendingReview),
                Some("approved") => Some(PlanStatus::Approved),
                Some("rejected") => Some(PlanStatus::Rejected),
                Some("problem") => Some(PlanStatus::Problem),
                Some("done") => Some(PlanStatus::Done),
                Some("archived") => Some(PlanStatus::Archived),
                _ => None,
            };
            plan.plan_status(status_filter).await.unwrap_or_default()
        }
        None => return json!({"plans": [], "total": 0}),
    };
    let filtered: Vec<&Plan> = plans.iter().filter(|p| match level {
        Some(lvl) => p.level.as_str().eq_ignore_ascii_case(lvl),
        None => true,
    }).collect();
    json!({
        "plans": filtered.iter().map(|p| plan_to_json(p)).collect::<Vec<_>>(),
        "total": filtered.len(),
    })
}

async fn cmd_plans_create(params: &Value, state: &Arc<WebState>) -> Value {
    let description = params.get("description").and_then(|v| v.as_str()).unwrap_or("");
    let autonomous = params.get("autonomous_mode").and_then(|v| v.as_bool()).unwrap_or(false);
    match &state.plan {
        Some(plan) => match plan.read().await.plan_create_p0(description.to_string(), autonomous).await {
            Ok(p) => plan_to_json(&p),
            Err(e) => json!({"error": e.to_string()}),
        },
        None => json!({"error": "plan actor not available"}),
    }
}

async fn cmd_plans_get(params: &Value, state: &Arc<WebState>) -> Value {
    let id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    match &state.plan {
        Some(plan) => match plan.read().await.get_plan(id).await {
            Ok(Some(p)) => plan_to_json(&p),
            Ok(None) => json!({"error": "not found"}),
            Err(e) => json!({"error": e.to_string()}),
        },
        None => json!({"error": "plan actor not available"}),
    }
}

async fn cmd_plans_delete(params: &Value, state: &Arc<WebState>) -> Value {
    let id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    match &state.plan {
        Some(plan) => match plan.read().await.plan_delete(id, true).await {
            Ok(true) => json!({"ok": true}),
            Ok(false) => json!({"error": "not found"}),
            Err(e) => json!({"error": e.to_string()}),
        },
        None => json!({"error": "plan actor not available"}),
    }
}

async fn cmd_plans_approve(params: &Value, state: &Arc<WebState>) -> Value {
    let id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    match &state.plan {
        Some(plan) => match plan.read().await.plan_approve_p1(id).await {
            Ok(p) => plan_to_json(&p),
            Err(e) => json!({"error": e.to_string()}),
        },
        None => json!({"error": "plan actor not available"}),
    }
}

async fn cmd_plans_reject(params: &Value, state: &Arc<WebState>) -> Value {
    let id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let reason = params.get("reason").and_then(|v| v.as_str()).unwrap_or("");
    match &state.plan {
        Some(plan) => match plan.read().await.plan_reject_p1(id, reason.to_string()).await {
            Ok(p) => plan_to_json(&p),
            Err(e) => json!({"error": e.to_string()}),
        },
        None => json!({"error": "plan actor not available"}),
    }
}

async fn cmd_plans_decompose(params: &Value, state: &Arc<WebState>) -> Value {
    let id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    match &state.plan {
        Some(plan) => match plan.read().await.plan_decompose(id).await {
            Ok(children) => json!({
                "children": children.iter().map(plan_to_json).collect::<Vec<_>>(),
                "total": children.len(),
            }),
            Err(e) => json!({"error": e.to_string()}),
        },
        None => json!({"error": "plan actor not available"}),
    }
}

async fn cmd_plans_claim(params: &Value, state: &Arc<WebState>) -> Value {
    let id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let agent_id = params.get("agent_id").and_then(|v| v.as_str()).unwrap_or("ui-user");
    match &state.plan {
        Some(plan) => match plan.read().await.plan_claim(id, agent_id.to_string()).await {
            Ok(p) => plan_to_json(&p),
            Err(e) => json!({"error": e.to_string()}),
        },
        None => json!({"error": "plan actor not available"}),
    }
}

async fn cmd_plans_complete(params: &Value, state: &Arc<WebState>) -> Value {
    let id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let result = params.get("result").and_then(|v| v.as_str()).unwrap_or("");
    match &state.plan {
        Some(plan) => match plan.read().await.plan_complete(id, result.to_string()).await {
            Ok(p) => plan_to_json(&p),
            Err(e) => json!({"error": e.to_string()}),
        },
        None => json!({"error": "plan actor not available"}),
    }
}

async fn cmd_plans_problem(params: &Value, state: &Arc<WebState>) -> Value {
    let id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let comment = params.get("comment").and_then(|v| v.as_str()).unwrap_or("");
    match &state.plan {
        Some(plan) => match plan.read().await.plan_set_problem(id, comment.to_string()).await {
            Ok(p) => plan_to_json(&p),
            Err(e) => json!({"error": e.to_string()}),
        },
        None => json!({"error": "plan actor not available"}),
    }
}

async fn cmd_plans_resolve(params: &Value, state: &Arc<WebState>) -> Value {
    let id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let resolution = params.get("resolution").and_then(|v| v.as_str()).unwrap_or("");
    match &state.plan {
        Some(plan) => match plan.read().await.plan_resolve_problem(id, resolution.to_string()).await {
            Ok(p) => plan_to_json(&p),
            Err(e) => json!({"error": e.to_string()}),
        },
        None => json!({"error": "plan actor not available"}),
    }
}

async fn cmd_plans_archive(params: &Value, state: &Arc<WebState>) -> Value {
    let id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    match &state.plan {
        Some(plan) => match plan.read().await.plan_archive(id).await {
            Ok(p) => plan_to_json(&p),
            Err(e) => json!({"error": e.to_string()}),
        },
        None => json!({"error": "plan actor not available"}),
    }
}

// === Chat (LLM-агент) ===

/// chat.send: пользователь пишет сообщение → LLM-агент отвечает с контекстом памяти.
/// Агент имеет прямой доступ к L2Actor/ChainActor/SearchActor — в обход MCP handler.
async fn cmd_chat_send(params: &Value, state: &Arc<WebState>) -> Value {
    let user_msg = params.get("message").and_then(|v| v.as_str()).unwrap_or("");
    if user_msg.is_empty() {
        return json!({"error": "message is required"});
    }

    // Живой LLM-клиент из конфигурации сервера (.env) — тот же, что у акторов.
    // Панель настроек только отображает активную конфигурацию (не источник для чата).
    let disabled = match &state.config {
        Some(c) => matches!(
            c.read().await.llm.provider,
            crate::actors::LlmProvider::Disabled | crate::actors::LlmProvider::Mock
        ),
        None => true,
    };
    if disabled {
        return json!({"error": "LLM отключён в конфигурации сервера (.env: GRAPHMIND_LLM_PROVIDER). Укажите провайдера/base_url/model/ключ и перезапустите сервер."});
    }
    let Some(llm) = state.llm.as_ref() else {
        return json!({"error": "LLM-клиент не инициализирован на сервере."});
    };

    // Системный промпт: контекст памяти
    let system_prompt = build_memory_context(state).await;

    match llm.chat(&system_prompt, user_msg).await {
        Ok(reply) => json!({"reply": reply, "role": "assistant"}),
        Err(e) => json!({"error": e.to_string(), "reply": format!("Ошибка LLM: {}", e), "role": "assistant"}),
    }
}

/// Сформировать системный промпт с контекстом памяти для LLM-агента.
async fn build_memory_context(state: &Arc<WebState>) -> String {
    let mut ctx = String::new();
    ctx.push_str("Ты — агент памяти GraphMind v2. Ты работаешь внутри системы памяти и имеешь прямой доступ к узлам, рёбрам, причинным цепочкам и планам.\n");
    ctx.push_str("ВНИМАНИЕ: ты работаешь в режиме ограниченного демонстрационного функционала (MVP). Не все возможности реализованы. Предупреждай пользователя, если запрос выходит за рамки демо.\n");
    ctx.push_str("Твоя задача: помогать управлять памятью — прогнозировать риски, отслеживать причины и следствия, планировать, актуализировать и достраивать узлы и связи.\n\n");

    // Текущий workspace
    let ws_name = match &state.workspace {
        Some(ws) => match ws.get_active_workspace_id().await {
            Some(id) => {
                let workspaces = ws.list_workspaces(None).await.unwrap_or_default();
                workspaces.iter().find(|w| w.id == id).map(|w| w.name.clone()).unwrap_or_else(|| id.clone())
            }
            None => "(не активен)".to_string(),
        },
        None => "(недоступен)".to_string(),
    };
    ctx.push_str(&format!("Активный workspace: {}\n", ws_name));

    // Последние узлы L2
    let recent_nodes: Vec<String> = {
        let l2 = state.l2.read().await;
        l2.list_all_nodes().await
            .unwrap_or_default()
            .into_iter()
            .take(10)
            .map(|n| format!("  - [{}] {}", format!("{:?}", n.level), n.content.chars().take(80).collect::<String>()))
            .collect()
    };
    if !recent_nodes.is_empty() {
        ctx.push_str("Последние узлы в памяти:\n");
        for n in &recent_nodes {
            ctx.push_str(n);
            ctx.push('\n');
        }
    }

    // Количество планов по статусам
    if let Some(plan) = &state.plan {
        let plan = plan.read().await;
        let total = plan.plan_status(None).await.map(|v| v.len()).unwrap_or(0);
        ctx.push_str(&format!("\nВсего планов: {}\n", total));
    }

    ctx.push_str("\nОтвечай кратко и по делу. Если пользователь просит создать узел или план — опиши, что нужно сделать, но не пытайся выполнить действие напрямую.");
    ctx
}

// === Plans: update ===

/// plans.update: обновить описание и/или статус плана.
/// Статус меняется через соответствующие методы PlanActor.
async fn cmd_plans_update(params: &Value, state: &Arc<WebState>) -> Value {
    let id = params.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let description = params.get("description").and_then(|v| v.as_str());
    let new_status = params.get("status").and_then(|v| v.as_str());

    if id.is_empty() {
        return json!({"error": "id is required"});
    }

    match &state.plan {
        Some(plan) => {
            let plan = plan.write().await;

            // Обновить описание (если передано)
            if let Some(desc) = description {
                if let Ok(Some(mut p)) = plan.get_plan(id).await {
                    p.description = desc.to_string();
                    // Сохраняем через внутренний метод
                    if let Err(e) = plan.update_plan(id, p).await {
                        return json!({"error": e.to_string()});
                    }
                }
            }

            // Обновить статус (если передано)
            if let Some(status) = new_status {
                let result = match status {
                    "in_progress" => plan.set_status(id, PlanStatus::InProgress).await,
                    "pending_review" => plan.set_status(id, PlanStatus::PendingReview).await,
                    "approved" => plan.set_status(id, PlanStatus::Approved).await,
                    "rejected" => plan.set_status(id, PlanStatus::Rejected).await,
                    "problem" => plan.set_status(id, PlanStatus::Problem).await,
                    "done" => plan.set_status(id, PlanStatus::Done).await,
                    "archived" => plan.set_status(id, PlanStatus::Archived).await,
                    "created" => plan.set_status(id, PlanStatus::Created).await,
                    _ => Ok(()),
                };
                if let Err(e) = result {
                    return json!({"error": e.to_string()});
                }
            }

            match plan.get_plan(id).await {
                Ok(Some(p)) => plan_to_json(&p),
                _ => json!({"ok": true}),
            }
        }
        None => json!({"error": "plan actor not available"}),
    }
}

// === Settings ===

async fn cmd_settings_get(state: &Arc<WebState>) -> Value {
    // Отражаем ЖИВУЮ конфигурацию сервера (.env), а не дефолты settings.json,
    // иначе панель показывает LLM/эмбеддинги «disabled», хотя они работают.
    // Ключи маскируются в view_from_config — сырой api_key клиенту не уходит.
    let settings = match &state.config {
        Some(cfg) => super::settings::view_from_config(&*cfg.read().await),
        None => super::settings::load(),
    };
    super::settings::to_json(&settings)
}

async fn cmd_settings_set(params: &Value) -> Value {
    match super::settings::from_json(params) {
        Ok(mut settings) => {
            // Маска-плейсхолдер вместо ключа → не сохраняем её как значение
            // (реальный ключ живёт в .env сервера, не в settings.json).
            if settings.llm.api_key == super::settings::MASKED_SECRET {
                settings.llm.api_key.clear();
            }
            if settings.embedding.api_key == super::settings::MASKED_SECRET {
                settings.embedding.api_key.clear();
            }
            match super::settings::save(&settings) {
                Ok(_) => json!({"ok": true, "note": "UI-настройки сохранены. Эндпоинты LLM/эмбеддингов сервер берёт из .env — панель показывает активную конфигурацию."}),
                Err(e) => json!({"error": e}),
            }
        }
        Err(e) => json!({"error": e}),
    }
}

// === Хелперы ===

fn parse_level(s: &Option<String>) -> Option<Level> {
    match s.as_deref()?.to_lowercase().as_str() {
        "l0" => Some(Level::L0),
        "l1" => Some(Level::L1),
        "l2" => Some(Level::L2),
        "gkl" => Some(Level::GKL),
        _ => None,
    }
}

fn node_to_json(n: &Node) -> Value {
    json!({
        "id": n.id.0,
        "content": n.content,
        "level": format!("{:?}", n.level),
        "node_type": format!("{:?}", n.node_type),
        "status": format!("{:?}", n.status),
        "workspace_id": n.metadata.workspace_id,
        "parent_id": n.metadata.parent_id,
        "tags": n.metadata.tags,
        "created_at": n.created_at.to_rfc3339(),
    })
}

fn plan_to_json(p: &Plan) -> Value {
    json!({
        "id": p.id,
        "level": p.level.as_str(),
        "status": format!("{:?}", p.status),
        "description": p.description,
        "parent_id": p.parent_id,
        "autonomous_mode": p.autonomous_mode,
        "quality_score": p.quality_score,
        "claimed_by": p.claimed_by,
        "result": p.result,
        "problem_comment": p.problem_comment,
        "created_at": p.created_at.to_rfc3339(),
        "updated_at": p.updated_at.to_rfc3339(),
    })
}

fn format_uptime(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 { format!("{}h {}m", h, m) }
    else if m > 0 { format!("{}m {}s", m, s) }
    else { format!("{}s", s) }
}
