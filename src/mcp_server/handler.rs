//! MCP Handler — диспетчеризация MCP инструментов

use serde_json::Value;
use crate::actors::{Actor, S0Actor, L2Actor, L1Actor, L0Actor, SearchActor, ChainActor, WorkspaceManager, PlanActor, PlanStatus, InferenceActor, CuriosityEngine, TrustFirewall, SourceType, MemoryEvent, MemoryOrchestrator};
use crate::graph::Node;
use crate::queue::QueueProcessor;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;

/// MCP Handler с доступом к акторам
pub struct McpHandler {
    pub s0: Arc<S0Actor>,
    pub l2: Arc<RwLock<L2Actor>>,
    pub l1: Option<Arc<RwLock<L1Actor>>>,
    pub l0: Option<Arc<RwLock<L0Actor>>>,
    pub search: Option<Arc<RwLock<SearchActor>>>,
    pub chain: Option<Arc<ChainActor>>,
    pub queue: Option<Arc<QueueProcessor>>,
    pub workspace: Option<Arc<WorkspaceManager>>,
    pub plan: Option<Arc<RwLock<PlanActor>>>,
    /// Причинный слой (dream_reflection / predict_risks / find_contradictions) над L2.
    pub inference: Option<Arc<InferenceActor>>,
    /// Любопытство: неопределённости графа → irritation / curiosity-задачи (над L2).
    pub curiosity: Option<Arc<CuriosityEngine>>,
    /// Доверие: верификация входа по источнику/консистентности (над L2).
    pub trust: Option<Arc<TrustFirewall>>,
    /// Шина событий координатора памяти (эмитим NodeWritten / ActionRecorded / TrustFirewallBlock).
    pub event_tx: Option<UnboundedSender<MemoryEvent>>,
    /// Координатор памяти (для инструмента orchestrator_status).
    pub orchestrator: Option<Arc<MemoryOrchestrator>>,
    // Другие акторы можно добавить по мере необходимости
}

impl McpHandler {
    pub fn new(s0: Arc<S0Actor>, l2: Arc<RwLock<L2Actor>>) -> Self {
        Self {
            s0,
            l2,
            l1: None,
            l0: None,
            search: None,
            chain: None,
            queue: None,
            workspace: None,
            plan: None,
            inference: None,
            curiosity: None,
            trust: None,
            event_tx: None,
            orchestrator: None,
        }
    }

    pub fn with_search_and_chain(
        s0: Arc<S0Actor>,
        l2: Arc<RwLock<L2Actor>>,
        search: Arc<RwLock<SearchActor>>,
        chain: Arc<ChainActor>,
    ) -> Self {
        Self {
            s0,
            l2,
            l1: None,
            l0: None,
            search: Some(search),
            chain: Some(chain),
            queue: None,
            workspace: None,
            plan: None,
            inference: None,
            curiosity: None,
            trust: None,
            event_tx: None,
            orchestrator: None,
        }
    }

    /// Attach L1Actor (для `consolidate_workspace`).
    pub fn with_l1(mut self, l1: Arc<RwLock<L1Actor>>) -> Self {
        self.l1 = Some(l1);
        self
    }

    /// Attach L0Actor (для `consolidate_workspace`).
    pub fn with_l0(mut self, l0: Arc<RwLock<L0Actor>>) -> Self {
        self.l0 = Some(l0);
        self
    }

    /// Attach PlanActor (для 12 plan_* tools).
    pub fn with_plan(mut self, plan: Arc<RwLock<PlanActor>>) -> Self {
        self.plan = Some(plan);
        self
    }

    /// Attach a QueueProcessor so `record_action` / `flush_session_memory`
    /// can use the durable pipeline instead of pushing straight to S0.
    pub fn with_queue(mut self, queue: Arc<QueueProcessor>) -> Self {
        self.queue = Some(queue);
        self
    }

    /// Attach a WorkspaceManager so `detect_workspace_from_context`,
    /// `create_workspace`, and `bootstrap_memory` use real storage instead
    /// of returning a hard-coded `default` workspace.
    pub fn with_workspace_manager(mut self, workspace: Arc<WorkspaceManager>) -> Self {
        self.workspace = Some(workspace);
        self
    }

    /// Полный конструктор: собрать handler со всеми акторами разом.
    /// Используется и stdio-, и HTTP-транспортом — гарантирует, что оба видят
    /// один и тот же набор инструментов (паритет транспортов).
    #[allow(clippy::too_many_arguments)]
    pub fn build_full(
        s0: Arc<S0Actor>,
        l2: Arc<RwLock<L2Actor>>,
        search: Option<Arc<RwLock<SearchActor>>>,
        chain: Option<Arc<ChainActor>>,
        queue: Option<Arc<QueueProcessor>>,
        workspace: Option<Arc<WorkspaceManager>>,
        l1: Option<Arc<RwLock<L1Actor>>>,
        l0: Option<Arc<RwLock<L0Actor>>>,
        plan: Option<Arc<RwLock<PlanActor>>>,
    ) -> Self {
        Self { s0, l2, l1, l0, search, chain, queue, workspace, plan, inference: None, curiosity: None, trust: None, event_tx: None, orchestrator: None }
    }

    /// Attach InferenceActor (причинный слой: dream/predict/contradictions).
    pub fn with_inference(mut self, inference: Arc<InferenceActor>) -> Self {
        self.inference = Some(inference);
        self
    }

    /// Attach CuriosityEngine (get_irritation_report / list_curiosity_tasks / close_curiosity_task).
    pub fn with_curiosity(mut self, curiosity: Arc<CuriosityEngine>) -> Self {
        self.curiosity = Some(curiosity);
        self
    }

    /// Attach TrustFirewall (verify_input).
    pub fn with_trust(mut self, trust: Arc<TrustFirewall>) -> Self {
        self.trust = Some(trust);
        self
    }

    /// Attach the coordinator event bus (эмиттеры шлют события координатору).
    pub fn with_event_tx(mut self, tx: UnboundedSender<MemoryEvent>) -> Self {
        self.event_tx = Some(tx);
        self
    }

    /// Attach the MemoryOrchestrator (для инструмента orchestrator_status).
    pub fn with_orchestrator(mut self, orchestrator: Arc<MemoryOrchestrator>) -> Self {
        self.orchestrator = Some(orchestrator);
        self
    }

    /// Обработать вызов MCP инструмента
    pub async fn handle_tool(&self, name: &str, params: Value) -> Value {
        match name {
            "record_action" => self.record_action(params).await,
            "get_s0_context" => self.get_s0_context(params).await,
            "flush_session_memory" => self.flush_session_memory(params).await,
            "propose_new_memory" => self.propose_new_memory(params).await,
            "update_node" => self.update_node(params).await,
            "fetch_l2_atoms" => self.fetch_l2_atoms(params).await,
            "list_memory" => self.list_memory(params).await,
            "get_chain" => self.get_chain(params).await,
            // 03-causal-reasoning (InferenceActor выведен в MCP)
            "find_contradictions" => self.find_contradictions(params).await,
            "predict_risks" => self.predict_risks(params).await,
            "dream_reflection" => self.dream_reflection(params).await,
            "propose_causal_link" => self.propose_causal_link(params).await,
            // 09-trust-curiosity (CuriosityEngine / TrustFirewall над L2)
            "get_irritation_report" => self.get_irritation_report(params).await,
            "list_curiosity_tasks" => self.list_curiosity_tasks(params).await,
            "close_curiosity_task" => self.close_curiosity_task(params).await,
            "verify_input" => self.verify_input(params).await,
            "orchestrator_status" => self.orchestrator_status(params).await,
            "memory_query" => self.memory_query(params).await,
            "detect_workspace_from_context" => self.detect_workspace_from_context(params).await,
            "create_workspace" => self.create_workspace(params).await,
            "bootstrap_memory" => self.bootstrap_memory(params).await,
            // 02-storage extensions
            "link_nodes" => self.link_nodes(params).await,
            "archive_node" => self.archive_node(params).await,
            "restore_node" => self.restore_node(params).await,
            "unlink_edge" => self.unlink_edge(params).await,
            "list_edges" => self.list_edges(params).await,
            // 05-workspace extensions
            "list_workspaces" => self.list_workspaces(params).await,
            "switch_workspace" => self.switch_workspace(params).await,
            "archive_workspace" => self.archive_workspace(params).await,
            // 02-storage + 04-vector-search extensions (Block 2)
            "search_nodes" => self.search_nodes(params).await,
            "vector_search" => self.vector_search(params).await,
            "suggest_related" => self.suggest_related(params).await,
            // 06-cross-workspace (Block 3)
            "fetch_from_workspace" => self.fetch_from_workspace(params).await,
            "find_workspace_overlaps" => self.find_workspace_overlaps(params).await,
            "suggest_cross_workspace_links" => self.suggest_cross_workspace_links(params).await,
            // 07-memory-lifecycle (Block 4)
            "consolidate_workspace" => self.consolidate_workspace(params).await,
            // 07-memory-lifecycle skeleton (Block 6)
            "route_l1" => self.route_l1(params).await,
            "search_l0_clusters" => self.search_l0_clusters(params).await,
            // 12-plan (Block 5)
            "plan_create_p0" => self.plan_create_p0(params).await,
            "plan_propose_p1" => self.plan_propose_p1(params).await,
            "plan_approve_p1" => self.plan_approve_p1(params).await,
            "plan_reject_p1" => self.plan_reject_p1(params).await,
            "plan_decompose" => self.plan_decompose(params).await,
            "plan_claim" => self.plan_claim(params).await,
            "plan_complete" => self.plan_complete(params).await,
            "plan_set_problem" => self.plan_set_problem(params).await,
            "plan_resolve_problem" => self.plan_resolve_problem(params).await,
            "plan_status" => self.plan_status(params).await,
            "plan_delete" => self.plan_delete(params).await,
            "plan_archive" => self.plan_archive(params).await,
            _ => {
                serde_json::json!({
                    "ok": false,
                    "code": "METHOD_NOT_FOUND",
                    "message": format!("Unknown tool: {}", name)
                })
            }
        }
    }
    
    /// Найти противоречия среди узлов (InferenceActor → CausalEngine).
    async fn find_contradictions(&self, _params: Value) -> Value {
        let inf = match &self.inference {
            Some(i) => i,
            None => return serde_json::json!({"ok": false, "code": "INFERENCE_NOT_INITIALIZED", "error": "InferenceActor не подключён"}),
        };
        match inf.find_contradictions().await {
            Ok(list) => serde_json::json!({
                "ok": true,
                "count": list.len(),
                "contradictions": serde_json::to_value(&list).unwrap_or(Value::Null),
            }),
            Err(e) => serde_json::json!({"ok": false, "code": "FIND_CONTRADICTIONS_FAILED", "error": e.to_string()}),
        }
    }

    /// Прогноз рисков от узла-причины (forward_pre по цепочке).
    async fn predict_risks(&self, params: Value) -> Value {
        let cause_id = params.get("cause_id").and_then(|v| v.as_str()).unwrap_or("");
        if cause_id.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "cause_id обязателен"});
        }
        let inf = match &self.inference {
            Some(i) => i,
            None => return serde_json::json!({"ok": false, "code": "INFERENCE_NOT_INITIALIZED", "error": "InferenceActor не подключён"}),
        };
        match inf.predict_risks(&crate::graph::NodeId(cause_id.to_string())).await {
            Ok(r) => serde_json::json!({"ok": true, "prediction": serde_json::to_value(&r).unwrap_or(Value::Null)}),
            Err(e) => serde_json::json!({"ok": false, "code": "PREDICT_RISKS_FAILED", "error": e.to_string()}),
        }
    }

    /// Найти повторяющиеся причинные паттерны и вывести правила IF/THEN.
    async fn dream_reflection(&self, _params: Value) -> Value {
        let inf = match &self.inference {
            Some(i) => i,
            None => return serde_json::json!({"ok": false, "code": "INFERENCE_NOT_INITIALIZED", "error": "InferenceActor не подключён"}),
        };
        match inf.dream_reflection().await {
            Ok(rules) => serde_json::json!({"ok": true, "count": rules.len(), "rules": serde_json::to_value(&rules).unwrap_or(Value::Null)}),
            Err(e) => serde_json::json!({"ok": false, "code": "DREAM_REFLECTION_FAILED", "error": e.to_string()}),
        }
    }

    /// Предложить причинную связь между двумя узлами (LLM по смыслу). НЕ создаёт ребро —
    /// это предложение, человек подтверждает через link_nodes.
    async fn propose_causal_link(&self, params: Value) -> Value {
        let source_id = params.get("source_id").and_then(|v| v.as_str()).unwrap_or("");
        let target_id = params.get("target_id").and_then(|v| v.as_str()).unwrap_or("");
        if source_id.is_empty() || target_id.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "source_id и target_id обязательны"});
        }
        let inf = match &self.inference {
            Some(i) => i,
            None => return serde_json::json!({"ok": false, "code": "INFERENCE_NOT_INITIALIZED", "error": "InferenceActor не подключён"}),
        };
        match inf
            .propose_causal_link(
                &crate::graph::NodeId(source_id.to_string()),
                &crate::graph::NodeId(target_id.to_string()),
            )
            .await
        {
            Ok(r) => serde_json::json!({"ok": true, "proposal": serde_json::to_value(&r).unwrap_or(Value::Null)}),
            Err(e) => serde_json::json!({"ok": false, "code": "PROPOSE_CAUSAL_LINK_FAILED", "error": e.to_string()}),
        }
    }

    /// Отчёт о раздражении / эмоц. состоянии по неопределённостям графа (CuriosityEngine над L2).
    async fn get_irritation_report(&self, _params: Value) -> Value {
        let cur = match &self.curiosity {
            Some(c) => c,
            None => return serde_json::json!({"ok": false, "code": "CURIOSITY_NOT_INITIALIZED", "error": "CuriosityEngine не подключён"}),
        };
        let report = cur.get_report().await;
        serde_json::json!({"ok": true, "report": serde_json::to_value(&report).unwrap_or(Value::Null)})
    }

    /// Сгенерировать curiosity-задачи по текущим неопределённостям графа.
    async fn list_curiosity_tasks(&self, _params: Value) -> Value {
        let cur = match &self.curiosity {
            Some(c) => c,
            None => return serde_json::json!({"ok": false, "code": "CURIOSITY_NOT_INITIALIZED", "error": "CuriosityEngine не подключён"}),
        };
        let tasks = cur.generate_tasks().await;
        serde_json::json!({"ok": true, "count": tasks.len(), "tasks": serde_json::to_value(&tasks).unwrap_or(Value::Null)})
    }

    /// Закрыть (завершить) curiosity-задачу по id.
    async fn close_curiosity_task(&self, params: Value) -> Value {
        let id = params.get("task_id").and_then(|v| v.as_str())
            .or_else(|| params.get("id").and_then(|v| v.as_str()))
            .unwrap_or("");
        if id.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "task_id обязателен"});
        }
        let cur = match &self.curiosity {
            Some(c) => c,
            None => return serde_json::json!({"ok": false, "code": "CURIOSITY_NOT_INITIALIZED", "error": "CuriosityEngine не подключён"}),
        };
        let closed = cur.complete_task(id).await;

        // B4: если caller знает источник и корректность — учим фаервол на закрытии задачи
        // (репутация источника + калибровка). Без was_correct — поведение как раньше.
        if let (Some(trust), Some(was_correct)) =
            (&self.trust, params.get("was_correct").and_then(|v| v.as_bool()))
        {
            trust.recalibrate(was_correct).await;
            if let Some(src) = params.get("source_id").and_then(|v| v.as_str()) {
                trust.update_reputation(src, was_correct).await;
            }
        }

        serde_json::json!({"ok": true, "closed": closed, "task_id": id})
    }

    /// Диагностика координатора памяти: счётчики workspace, что консолидируется, решения.
    async fn orchestrator_status(&self, _params: Value) -> Value {
        match &self.orchestrator {
            Some(o) => serde_json::json!({"ok": true, "status": o.status().await}),
            None => serde_json::json!({
                "ok": false,
                "code": "ORCHESTRATOR_NOT_INITIALIZED",
                "error": "MemoryOrchestrator не подключён"
            }),
        }
    }

    /// Разобрать строковый source_type в enum (snake_case или PascalCase).
    fn parse_source_type(s: &str) -> SourceType {
        match s {
            "user_direct" | "UserDirect" => SourceType::UserDirect,
            "user_document" | "UserDocument" => SourceType::UserDocument,
            "web_search" | "WebSearch" => SourceType::WebSearch,
            "agent_internal" | "AgentInternal" => SourceType::AgentInternal,
            "external_api" | "ExternalAPI" => SourceType::ExternalAPI,
            _ => SourceType::Unknown,
        }
    }

    /// Верифицировать вход по источнику и консистентности с памятью (TrustFirewall над L2).
    async fn verify_input(&self, params: Value) -> Value {
        let content = params.get("input").and_then(|v| v.as_str())
            .or_else(|| params.get("content").and_then(|v| v.as_str()))
            .unwrap_or("");
        if content.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "input обязателен"});
        }
        let source_id = params.get("source").and_then(|v| v.as_str())
            .or_else(|| params.get("source_id").and_then(|v| v.as_str()))
            .unwrap_or("unknown");
        let source_type = Self::parse_source_type(
            params.get("source_type").and_then(|v| v.as_str()).unwrap_or("user_direct"),
        );
        let trust = match &self.trust {
            Some(t) => t,
            None => return serde_json::json!({"ok": false, "code": "TRUST_NOT_INITIALIZED", "error": "TrustFirewall не подключён"}),
        };
        let report = trust.verify_input(source_id, source_type, content).await;
        serde_json::json!({"ok": true, "report": serde_json::to_value(&report).unwrap_or(Value::Null)})
    }

    /// Список всех доступных инструментов.
    ///
    /// Должен быть в синхронизации с `handle_tool()`. Если добавляешь новый tool:
    /// 1) добавь ветку в `handle_tool`,
    /// 2) добавь запись здесь (иначе Kodik не увидит tool через `tools/list`),
    /// 3) добавь в `mcp.json autoApprove` если он должен быть без подтверждения.
    ///
    /// На 2026-07-04: 39 tools = 11 базовых + 28 расширений (Blocks 1-6 + Config).
    pub fn list_tools() -> Vec<Value> {
        vec![
            // ===== 01-session (3) =====
            serde_json::json!({
                "name": "record_action",
                "description": "Записать действие в кратковременную память сессии (через QueueProcessor)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "summary": {"type": "string", "description": "Краткое описание (до 200 символов)"},
                        "raw_text": {"type": "string", "description": "Длинный текст / логи / дифф"},
                        "related_nodes": {"type": "array", "items": {"type": "string"}}
                    },
                    "required": ["summary"]
                }
            }),
            serde_json::json!({
                "name": "get_s0_context",
                "description": "Получить последние действия S0 (S0Actor.get_recent)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "limit": {"type": "number", "description": "Максимум записей (default 10, hard cap = capacity 20)"}
                    }
                }
            }),
            serde_json::json!({
                "name": "flush_session_memory",
                "description": "Завершить сессию: snapshot S0 → re-enqueue → s0.clear → queue.drain_to_l2",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "summary": {"type": "string"},
                        "related_nodes": {"type": "array", "items": {"type": "string"}},
                        "force": {"type": "boolean", "description": "Принудительный flush (V2.1: triggers causal_reflection)"}
                    },
                    "required": ["summary"]
                }
            }),
            // ===== 02-storage (8) =====
            serde_json::json!({
                "name": "propose_new_memory",
                "description": "Создать новый узел памяти (L2Actor.add_node). parent_id = cluster parent (L0→L1→L2), workspace_id = storage partition. См. bug_report/001.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "level": {"type": "string", "enum": ["L2", "L1", "L0", "GKL"]},
                        "node_type": {"type": "string", "enum": ["atom", "cause", "effect", "rule"]},
                        "content": {"type": "string"},
                        "parent_id": {"type": "string", "description": "Cluster parent (L0→L1→L2 иерархия). Не путать с workspace_id."},
                        "workspace_id": {"type": "string", "description": "Storage partition. Если не указан, берётся активный workspace, иначе 'global'/'default' по scope."},
                        "scope": {"type": "string", "enum": ["workspace", "global"]}
                    },
                    "required": ["level", "node_type", "content"]
                }
            }),            serde_json::json!({
                "name": "update_node",
                "description": "Обновить существующий узел (content)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "node_id": {"type": "string"},
                        "scope": {"type": "string", "enum": ["workspace", "global"]},
                        "content": {"type": "string"}
                    },
                    "required": ["node_id"]
                }
            }),
            serde_json::json!({
                "name": "fetch_l2_atoms",
                "description": "Получить полный текст узлов по atom_ids",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "atom_ids": {"type": "array", "items": {"type": "string"}},
                        "scope": {"type": "string", "enum": ["workspace", "global"]}
                    },
                    "required": ["atom_ids"]
                }
            }),
            serde_json::json!({
                "name": "list_memory",
                "description": "Показать все карточки в памяти (L2-узлы, новые сверху): id, уровень, тип, статус, содержимое, дата. Для инспекции — увидеть, что реально сохранено",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "limit": {"type": "number", "description": "макс. сколько показать (по умолчанию 100)"}
                    }
                }
            }),
            // get_chain: раньше диспатчился, но отсутствовал в list_tools → клиент его не видел.
            serde_json::json!({
                "name": "get_chain",
                "description": "Обход причинной цепочки (backward: симптом→корень; forward_pre: причина→эффекты; forward_post: действие→последствия)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "anchor": {
                            "type": "object",
                            "properties": {
                                "type": {"type": "string", "enum": ["node", "symptom"]},
                                "id": {"type": "string"},
                                "text": {"type": "string"}
                            }
                        },
                        "direction": {"type": "string", "enum": ["backward", "forward_pre", "forward_post"], "default": "backward"},
                        "max_depth": {"type": "number", "default": 3},
                        "scope": {"type": "string", "enum": ["workspace", "global"]}
                    },
                    "required": ["anchor", "direction"]
                }
            }),
            serde_json::json!({
                "name": "link_nodes",
                "description": "Создать edge между двумя узлами (L2Actor.add_edge). workspace_id опционален; если не указан — берётся активный workspace. Нужен для BFS в suggest_related. См. bug_report/001.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "from_id": {"type": "string"},
                        "to_id": {"type": "string"},
                        "relation": {"type": "string"},
                        "confidence": {"type": "number"},
                        "workspace_id": {"type": "string", "description": "Storage partition для edge (см. bug_report/001)."}
                    },
                    "required": ["from_id", "to_id"]
                }
            }),
            serde_json::json!({
                "name": "archive_node",
                "description": "Soft-archive узла (L2Actor.archive_node)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "node_id": {"type": "string"}
                    },
                    "required": ["node_id"]
                }
            }),
            serde_json::json!({
                "name": "restore_node",
                "description": "Восстановить узел из архива (L2Actor.restore_node)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "node_id": {"type": "string"}
                    },
                    "required": ["node_id"]
                }
            }),
            serde_json::json!({
                "name": "unlink_edge",
                "description": "Удалить edge по edge_id (L2Actor.delete_edge). edge_id можно получить через list_edges.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "edge_id": {"type": "string"}
                    },
                    "required": ["edge_id"]
                }
            }),
            serde_json::json!({
                "name": "list_edges",
                "description": "Найти рёбра по фильтру (from_id и/или to_id). Возвращает edge_id, нужный для unlink_edge.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "from_id": {"type": "string"},
                        "to_id": {"type": "string"}
                    }
                }
            }),
            // ===== 04-vector-search (3) =====
            serde_json::json!({
                "name": "search_nodes",
                "description": "Keyword-поиск по узлам (SearchActor.keyword_search)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "limit": {"type": "number", "default": 10},
                        "workspace_id": {"type": "string"},
                        "level": {"type": "string", "enum": ["L0", "L1", "L2", "GKL", "S0"]}
                    },
                    "required": ["query"]
                }
            }),
            serde_json::json!({
                "name": "vector_search",
                "description": "Vector similarity search (char-bag fallback в V2.0; EmbeddingProvider в V2.1)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "text": {"type": "string"},
                        "vector": {"type": "array", "items": {"type": "number"}},
                        "top_k": {"type": "number", "default": 10},
                        "min_score": {"type": "number", "default": 0.0},
                        "workspace_id": {"type": "string"}
                    }
                }
            }),
            serde_json::json!({
                "name": "suggest_related",
                "description": "BFS по edges_from, вернуть N ближайших соседей (1-hop, max_depth)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "node_id": {"type": "string"},
                        "max_depth": {"type": "number", "default": 2},
                        "top_k": {"type": "number", "default": 10}
                    },
                    "required": ["node_id"]
                }
            }),
            // memory_query: раньше диспатчился, но отсутствовал в list_tools → клиент его не видел.
            serde_json::json!({
                "name": "memory_query",
                "description": "Поиск по памяти (V2.0: keyword-совпадение по контенту L2; семантика/вектор в V2.1)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "depth": {"type": "number", "enum": [0, 1, 2], "default": 1},
                        "scope": {"type": "string", "enum": ["workspace", "global"], "default": "workspace"}
                    },
                    "required": ["query"]
                }
            }),
            // ===== 03-causal-reasoning (InferenceActor, выведены в MCP) =====
            serde_json::json!({
                "name": "find_contradictions",
                "description": "Найти противоречия среди фактов/причин/следствий/правил (CausalEngine)",
                "inputSchema": {"type": "object", "properties": {}}
            }),
            serde_json::json!({
                "name": "predict_risks",
                "description": "Прогноз рисков от узла-причины: возможные эффекты и уровень риска (forward_pre)",
                "inputSchema": {"type": "object", "properties": {"cause_id": {"type": "string"}}, "required": ["cause_id"]}
            }),
            serde_json::json!({
                "name": "dream_reflection",
                "description": "Найти повторяющиеся причинные паттерны и вывести правила IF/THEN",
                "inputSchema": {"type": "object", "properties": {}}
            }),
            serde_json::json!({
                "name": "propose_causal_link",
                "description": "Предложить причинную связь между двумя узлами (LLM по смыслу): тип+уверенность+обоснование. НЕ создаёт ребро — предложение для подтверждения через link_nodes",
                "inputSchema": {"type": "object", "properties": {"source_id": {"type": "string"}, "target_id": {"type": "string"}}, "required": ["source_id", "target_id"]}
            }),
            // ===== 09-trust-curiosity (CuriosityEngine / TrustFirewall над L2) =====
            serde_json::json!({
                "name": "get_irritation_report",
                "description": "Отчёт о раздражении/эмоц. состоянии по неопределённостям графа (Cause без Effect, Effect без Cause, противоречия)",
                "inputSchema": {"type": "object", "properties": {}}
            }),
            serde_json::json!({
                "name": "list_curiosity_tasks",
                "description": "Список задач-исследований по текущим неопределённостям графа",
                "inputSchema": {"type": "object", "properties": {}}
            }),
            serde_json::json!({
                "name": "close_curiosity_task",
                "description": "Завершить curiosity-задачу по id",
                "inputSchema": {"type": "object", "properties": {"task_id": {"type": "string"}}, "required": ["task_id"]}
            }),
            serde_json::json!({
                "name": "verify_input",
                "description": "Верифицировать вход по источнику, консистентности с памятью, verifiability и tone-аномалиям (TrustFirewall)",
                "inputSchema": {"type": "object", "properties": {"input": {"type": "string"}, "source": {"type": "string"}, "source_type": {"type": "string", "enum": ["user_direct", "user_document", "web_search", "agent_internal", "external_api"]}}, "required": ["input"]}
            }),
            serde_json::json!({
                "name": "orchestrator_status",
                "description": "Диагностика координатора памяти: per-workspace счётчики новых узлов, что консолидируется сейчас, последние решения (CycleTrigger)",
                "inputSchema": {"type": "object", "properties": {}}
            }),
            // ===== 05-workspace-management (5) =====
            serde_json::json!({
                "name": "detect_workspace_from_context",
                "description": "Определить workspace по cwd (WorkspaceManager.detect_workspace)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "cwd": {"type": "string"}
                    },
                    "required": ["cwd"]
                }
            }),
            serde_json::json!({
                "name": "create_workspace",
                "description": "Создать новый workspace (WorkspaceManager.create_workspace)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "path": {"type": "string"}
                    },
                    "required": ["name", "path"]
                }
            }),
            serde_json::json!({
                "name": "list_workspaces",
                "description": "Список всех workspace с node_count / edge_count / status",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            }),
            serde_json::json!({
                "name": "switch_workspace",
                "description": "Переключить активный workspace (WorkspaceManager.switch_workspace)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace_id": {"type": "string"}
                    },
                    "required": ["workspace_id"]
                }
            }),
            serde_json::json!({
                "name": "archive_workspace",
                "description": "Soft-archive workspace (WorkspaceManager.archive_workspace)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace_id": {"type": "string"}
                    },
                    "required": ["workspace_id"]
                }
            }),
            // ===== 06-cross-workspace (3) =====
            serde_json::json!({
                "name": "fetch_from_workspace",
                "description": "Получить узлы из конкретного workspace (L2Actor.list_by_workspace + keyword filter)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace_id": {"type": "string"},
                        "query": {"type": "string"},
                        "limit": {"type": "number", "default": 20}
                    },
                    "required": ["workspace_id"]
                }
            }),
            serde_json::json!({
                "name": "find_workspace_overlaps",
                "description": "Jaccard similarity по node_ids между source и target workspaces",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "source_workspace": {"type": "string"},
                        "min_similarity": {"type": "number", "default": 0.3},
                        "limit": {"type": "number", "default": 10}
                    },
                    "required": ["source_workspace"]
                }
            }),
            serde_json::json!({
                "name": "suggest_cross_workspace_links",
                "description": "Найти shared tags / shared nodes между workspace'ами (V2.0 fallback, GKL в V2.1)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace_id": {"type": "string"},
                        "limit": {"type": "number", "default": 10}
                    },
                    "required": ["workspace_id"]
                }
            }),
            // ===== 07-memory-lifecycle (2) =====
            serde_json::json!({
                "name": "bootstrap_memory",
                "description": "Автоподстройка памяти: detect workspace + memory_query + recent context",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "cwd": {"type": "string"},
                        "query": {"type": "string"},
                        "depth": {"type": "number", "enum": [0, 1, 2]}
                    },
                    "required": ["cwd", "query"]
                }
            }),
            serde_json::json!({
                "name": "consolidate_workspace",
                "description": "drain queue → L2 → L1 autogen → L0 autogen (full pipeline)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace_id": {"type": "string"}
                    },
                    "required": ["workspace_id"]
                }
            }),
            // ===== 07-memory-lifecycle skeleton (2, V2.0 marked) =====
            serde_json::json!({
                "name": "route_l1",
                "description": "[V2.0 skeleton] Прокинуть L2-атом в L1-домен (full multi-domain scoring в V2.1)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace_id": {"type": "string"},
                        "l2_atom_id": {"type": "string"}
                    },
                    "required": ["workspace_id", "l2_atom_id"]
                }
            }),
            serde_json::json!({
                "name": "search_l0_clusters",
                "description": "Поиск по L0-кластерам workspace: keyword-фильтрация по content + tags, возвращает кластеры с member_domain_ids и хаб",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workspace_id": {"type": "string"},
                        "query": {"type": "string"},
                        "limit": {"type": "number", "default": 10}
                    },
                    "required": ["workspace_id", "query"]
                }
            }),
            // ===== 12-plan (12, Block 5) =====
            serde_json::json!({
                "name": "plan_create_p0",
                "description": "Создать P0-план (верхний уровень: описание проблемы)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "description": {"type": "string"},
                        "autonomous_mode": {"type": "boolean", "default": false}
                    },
                    "required": ["description"]
                }
            }),
            serde_json::json!({
                "name": "plan_propose_p1",
                "description": "Предложить P1-подплан для P0 (требует review)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "p0_id": {"type": "string"},
                        "description": {"type": "string"}
                    },
                    "required": ["p0_id", "description"]
                }
            }),
            serde_json::json!({
                "name": "plan_approve_p1",
                "description": "Одобрить P1-подплан (reviewed → approved)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "p1_id": {"type": "string"}
                    },
                    "required": ["p1_id"]
                }
            }),
            serde_json::json!({
                "name": "plan_reject_p1",
                "description": "Отклонить P1-подплан с причиной",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "p1_id": {"type": "string"},
                        "reason": {"type": "string"}
                    },
                    "required": ["p1_id", "reason"]
                }
            }),
            serde_json::json!({
                "name": "plan_decompose",
                "description": "Декомпозировать узел плана в P2/P3 children",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "node_id": {"type": "string"}
                    },
                    "required": ["node_id"]
                }
            }),
            serde_json::json!({
                "name": "plan_claim",
                "description": "Sub-agent забирает P3-план в работу",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "p3_id": {"type": "string"},
                        "agent_id": {"type": "string"}
                    },
                    "required": ["p3_id", "agent_id"]
                }
            }),
            serde_json::json!({
                "name": "plan_complete",
                "description": "Завершить P3-план с результатом",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "p3_id": {"type": "string"},
                        "result": {"type": "string"}
                    },
                    "required": ["p3_id", "result"]
                }
            }),
            serde_json::json!({
                "name": "plan_set_problem",
                "description": "Пометить план как проблемный (problem_comment)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "plan_id": {"type": "string"},
                        "problem_comment": {"type": "string"}
                    },
                    "required": ["plan_id", "problem_comment"]
                }
            }),
            serde_json::json!({
                "name": "plan_resolve_problem",
                "description": "Разрешить проблему плана с описанием решения",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "plan_id": {"type": "string"},
                        "resolution": {"type": "string"}
                    },
                    "required": ["plan_id", "resolution"]
                }
            }),
            serde_json::json!({
                "name": "plan_status",
                "description": "Список планов с фильтром по статусу (created/in_progress/approved/...)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "filter": {"type": "string", "enum": ["created", "in_progress", "pending_review", "approved", "rejected", "problem", "done", "archived"]}
                    }
                }
            }),
            serde_json::json!({
                "name": "plan_delete",
                "description": "Удалить план (force=true для не-archived)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "plan_id": {"type": "string"},
                        "force": {"type": "boolean", "default": false}
                    },
                    "required": ["plan_id"]
                }
            }),
            serde_json::json!({
                "name": "plan_archive",
                "description": "Архивировать план (не удалять, скрыть из active)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "plan_id": {"type": "string"}
                    },
                    "required": ["plan_id"]
                }
            })
        ]
    }
    
    // ==================== Реализация инструментов ====================
    
    async fn record_action(&self, params: Value) -> Value {
        let summary = params.get("summary").and_then(|v| v.as_str()).unwrap_or("");
        if summary.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "summary is required"
            });
        }
        let raw_text = params.get("raw_text").and_then(|v| v.as_str()).map(String::from);
        let related_nodes: Vec<String> = params
            .get("related_nodes")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default();

        tracing::info!(
            "MCP: record_action('{}', related_nodes={})",
            summary,
            related_nodes.len()
        );

        // Активность для координатора памяти (idle-детекция).
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(MemoryEvent::ActionRecorded { workspace: "default".to_string() });
        }

        // Build PendingAction
        let mut action = crate::queue::PendingAction::new_record(summary, "mcp_server");
        action.raw_text = raw_text;
        action.related_nodes = related_nodes;
        let action_id = action.id.clone();

        // Preferred path: enqueue via QueueProcessor (durable, async, single writer).
        if let Some(queue) = &self.queue {
            match queue.enqueue(action).await {
                Ok(id) => {
                    return serde_json::json!({
                        "ok": true,
                        "id": id,
                        "queued": true,
                        "consolidation_triggered": false
                    });
                }
                Err(e) => {
                    tracing::error!("MCP: record_action enqueue failed: {}", e);
                    return serde_json::json!({
                        "ok": false,
                        "code": "QUEUE_APPEND_FAILED",
                        "error": e.to_string()
                    });
                }
            }
        }

        // Fallback: direct S0 push (no durable queue attached).
        // Mirrors QueueProcessor::process_action for RecordAction.
        let s0 = &self.s0;
        let entry = crate::actors::S0Entry {
            id: action_id.clone(),
            source: action.source.clone(),
            summary: action.summary.clone(),
            timestamp: action.timestamp,
        };
        match s0.push(entry).await {
            Some(evicted) => tracing::info!(
                "MCP: record_action pushed to S0 (evicted id={})",
                evicted.id
            ),
            None => tracing::info!("MCP: record_action pushed to S0 (no eviction)"),
        }
        drop(s0);

        serde_json::json!({
            "ok": true,
            "id": action_id,
            "queued": false,
            "consolidation_triggered": false
        })
    }

    async fn get_s0_context(&self, params: Value) -> Value {
        let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
        tracing::info!("MCP: get_s0_context(limit={})", limit);

        let s0 = &self.s0;
        let recent = s0.get_recent(limit).await;
        let total = s0.size().await;
        drop(s0);

        let actions: Vec<Value> = recent
            .into_iter()
            .map(|e| {
                serde_json::json!({
                    "id": e.id,
                    "source": e.source,
                    "summary": e.summary,
                    "timestamp": e.timestamp.to_rfc3339(),
                })
            })
            .collect();

        serde_json::json!({
            "ok": true,
            "actions": actions,
            "total": total
        })
    }

    async fn flush_session_memory(&self, params: Value) -> Value {
        let summary = params.get("summary").and_then(|v| v.as_str()).unwrap_or("");
        let force = params
            .get("force")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let related_nodes: Vec<String> = params
            .get("related_nodes")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default();

        tracing::info!(
            "MCP: flush_session_memory('{}', force={}, related_nodes={})",
            summary,
            force,
            related_nodes.len()
        );

        // Bug 005 регрессия 5: snapshot S0 → re-enqueue как ProposeNewMemory
        // (не RecordAction!) → s0.clear → drain_to_l2 → s0.clear.
        // Оригинальный баг: re-enqueue создавал RecordAction → process_action
        // пушал их ОБРАТНО в S0 (processor.rs:143), поэтому S0 не очищался.
        // Фикс: re-enqueue как ProposeNewMemory → process_action создаёт L2-узлы
        // и НЕ пушит в S0. S0 очищается до и после drain.
        //
        // Bug 005 workspace: flush должен направлять узлы в активный workspace,
        // а не в хардкод "default". scope в PendingAction → workspace_id в Node.
        let ws_id = match &self.workspace {
            Some(ws) => ws.get_active_workspace_id().await
                .unwrap_or_else(|| "default".to_string()),
            None => "default".to_string(),
        };

        // Pre-drain: обработать RecordAction'ы из очереди, чтобы они попали в S0
        // до snapshot. Иначе record_action, поставленный в очередь прямо перед
        // flush, теряется: flush не находит его в S0, а drain_to_l2 обрабатывает
        // RecordAction (пушит в S0), который потом очищается s0.clear().
        if let Some(queue) = &self.queue {
            let _ = queue.drain_to_l2(&ws_id).await;
        }

        let snapshot = self.s0.get_all().await;
        let flushed_count = snapshot.len();

        // Очищаем S0 ДО re-enqueue — flush-записи не должны оставаться в S0.
        self.s0.clear().await;

        // Re-enqueue S0-записей как ProposeNewMemory actions.
        // RecordAction не создаёт L2-узлы (by design, bug_report/002),
        // а ProposeNewMemory — создаёт через process_action → l2.add_node.
        // scope = активный workspace_id → build_node_from_propose → Node.workspace_id.
        let mut enqueued = 0usize;
        if let Some(queue) = &self.queue {
            for entry in &snapshot {
                let content = if entry.summary.is_empty() {
                    format!("(flushed S0 entry from {})", entry.source)
                } else {
                    entry.summary.clone()
                };
                let action = crate::queue::PendingAction::new_propose(
                    format!("flush: {}", entry.summary),
                    content,
                    "L2",
                    "atom",
                    "",
                    &ws_id,
                    entry.source.clone(),
                );
                match queue.enqueue(action).await {
                    Ok(_) => enqueued += 1,
                    Err(e) => {
                        tracing::warn!("flush: failed to re-enqueue S0 entry {}: {}", entry.id, e);
                    }
                }
            }
        }

        // Drain очереди: ProposeNewMemory actions превращаются в L2-узлы.
        // Bug 005 регрессия 4: new_l2_count == new_l2_atoms, а не drained_actions.
        let (drained_actions, new_l2_atoms) = if let Some(queue) = &self.queue {
            match queue.drain_to_l2(&ws_id).await {
                Ok(stats) => (stats.done, stats.l2_atoms_created),
                Err(e) => {
                    tracing::error!("MCP: flush_session_memory drain_to_l2 failed: {}", e);
                    (0, 0)
                }
            }
        } else {
            (0, 0)
        };

        // Bug 005 регрессия 5: повторная очистка S0 после drain. На случай,
        // если process_action для RecordAction (из обычной очереди, не из flush)
        // пушит записи в S0 во время drain.
        self.s0.clear().await;

        let _ = (summary, force, related_nodes); // reserved for future causal_reflection hook

        serde_json::json!({
            "ok": true,
            "flushed_s0_count": flushed_count,
            "enqueued": enqueued,
            "drained_actions": drained_actions,
            "new_l2_atoms": new_l2_atoms,
            // Bug 005 регрессия 4: new_l2_count == new_l2_atoms (раньше равнялся
            // drained_actions и противоречил new_l2_atoms). Alias на 1 релиз.
            "new_l2_count": new_l2_atoms,
            "consolidated": true
        })
    }

    async fn propose_new_memory(&self, params: Value) -> Value {
        let level_str = params.get("level").and_then(|v| v.as_str()).unwrap_or("L2");
        let node_type_str = params.get("node_type").and_then(|v| v.as_str()).unwrap_or("atom");
        let content = params.get("content").and_then(|v| v.as_str()).unwrap_or("");
        // parent_id: cluster hierarchy (L0 → L1 → L2). См. bug_report/001.
        let parent_id = params.get("parent_id").and_then(|v| v.as_str());
        // workspace_id: storage partition. Может быть передан явно, иначе
        // выводится из scope / активного workspace.
        let workspace_id_param = params.get("workspace_id").and_then(|v| v.as_str());
        let scope = params.get("scope").and_then(|v| v.as_str()).unwrap_or("workspace");

        tracing::info!("MCP: propose_new_memory(level={}, type={}, scope={}, content='{}')", level_str, node_type_str, scope, content);

        // Map node_type string to NodeType
        let node_type = match node_type_str {
            "atom" => crate::graph::NodeType::Atom,
            "cause" => crate::graph::NodeType::Cause,
            "effect" => crate::graph::NodeType::Effect,
            "rule" => crate::graph::NodeType::Rule,
            _ => crate::graph::NodeType::Atom,
        };

        // Map level string to Level
        let level = match level_str {
            "L0" => crate::graph::Level::L0,
            "L1" => crate::graph::Level::L1,
            "L2" => crate::graph::Level::L2,
            "GKL" => crate::graph::Level::GKL,
            _ => crate::graph::Level::L2,
        };

        // Create node
        let mut node = crate::graph::Node::new(node_type, content);
        node = node.with_level(level);

        // 1. Cluster parent (L0 → L1 → L2). Никогда не путаем с workspace.
        if let Some(pid) = parent_id {
            node = node.with_parent(pid);
        }

        // 2. Workspace: явный параметр > scope=global > активный workspace > "default".
        //    Раньше (bug 001) parent_id перезаписывал workspace — исправлено.
        let ws_id: String = if let Some(wid) = workspace_id_param {
            wid.to_string()
        } else if scope == "global" {
            "global".to_string()
        } else if let Some(ws) = &self.workspace {
            ws.get_active_workspace_id()
                .await
                .unwrap_or_else(|| "default".to_string())
        } else {
            "default".to_string()
        };
        node = node.with_workspace(&ws_id);
        // Фаервол-гейт (мягкий, strict=false — прямой путь доверенного главного агента).
        // Block/низкое доверие → Draft (на ревью), явная манипуляция → отказ.
        let mut gate_warning: Option<String> = None;
        if let Some(trust) = &self.trust {
            let source_id = params.get("source_id").and_then(|v| v.as_str()).unwrap_or("user:direct");
            let source_type = Self::parse_source_type(
                params.get("source_type").and_then(|v| v.as_str()).unwrap_or("user_direct"),
            );
            match trust.gate(source_id, source_type, content, false).await {
                crate::actors::GateOutcome::Allow { status, warning, .. } => {
                    node.status = status;
                    gate_warning = warning;
                }
                crate::actors::GateOutcome::Refuse { reason, trust_score } => {
                    tracing::warn!("MCP: propose_new_memory отклонён фаерволом: {}", reason);
                    if let Some(tx) = &self.event_tx {
                        let _ = tx.send(MemoryEvent::TrustFirewallBlock {
                            source_id: source_id.to_string(),
                            reason: reason.clone(),
                        });
                    }
                    return serde_json::json!({
                        "ok": false,
                        "code": "TRUST_BLOCKED",
                        "error": reason,
                        "trust_score": trust_score
                    });
                }
            }
        }

        // Store via L2Actor (guard дропаем до сети, чтобы не держать read-lock на L2
        // во время embed).
        let add_result = {
            let l2 = self.l2.read().await;
            l2.add_node(&node).await
        };
        match add_result {
            Ok(node_id) => {
                tracing::info!("MCP: node created: {} (status={:?})", node_id.0, node.status);
                // Индексируем в SearchActor только Active-узлы: Draft (на ревью) не должен
                // всплывать в vector_search (см. node-status: draft скрыт из поиска).
                if node.status == crate::graph::Status::Active {
                    if let Some(search) = &self.search {
                        let s = search.read().await;
                        let emb = s.embed_text(&node.content).await;
                        if let Err(e) = s.index_node(&node, &emb).await {
                            tracing::warn!("MCP: propose_new_memory index_node failed: {}", e);
                        }
                    }
                    // CycleTrigger: считаем только Active-узлы.
                    if let Some(tx) = &self.event_tx {
                        let ws = node.metadata.workspace_id.clone().unwrap_or_else(|| "default".to_string());
                        let _ = tx.send(MemoryEvent::NodeWritten { workspace: ws });
                    }
                }
                // Bug 004 fix: инкрементируем счётчик узлов workspace
                if let Some(ws_manager) = &self.workspace {
                    let _ = ws_manager.bump_node_count(&ws_id, 1).await;
                }
                let mut resp = serde_json::json!({
                    "ok": true,
                    "node_id": node_id.0,
                    "level": level_str,
                    "status": format!("{:?}", node.status)
                });
                if let Some(w) = gate_warning {
                    resp["warning"] = serde_json::json!(w);
                }
                resp
            }
            Err(e) => {
                tracing::error!("MCP: failed to create node: {}", e);
                serde_json::json!({
                    "ok": false,
                    "error": e.to_string()
                })
            }
        }
    }
    
    async fn update_node(&self, params: Value) -> Value {
        let node_id_str = params.get("node_id").and_then(|v| v.as_str()).unwrap_or("");
        let content = params.get("content").and_then(|v| v.as_str());
        let scope = params.get("scope").and_then(|v| v.as_str()).unwrap_or("workspace");
        
        tracing::info!("MCP: update_node('{}', scope={})", node_id_str, scope);
        
        if node_id_str.is_empty() {
            return serde_json::json!({
                "ok": false,
                "error": "node_id is required"
            });
        }
        
        let node_id = crate::graph::NodeId::from_string(node_id_str);
        let l2 = self.l2.read().await;
        
        // Fetch existing node
        match l2.get_node(&node_id).await {
            Ok(Some(mut node)) => {
                // Update content if provided
                if let Some(new_content) = content {
                    node.content = new_content.to_string();
                    node.updated_at = chrono::Utc::now();

                    // Фаервол-гейт (strict=false): при явной манипуляции роняем в Draft,
                    // иначе сохраняем текущий статус (не «воскрешаем» Draft/Archived).
                    if let Some(trust) = &self.trust {
                        let source_id = params.get("source_id").and_then(|v| v.as_str()).unwrap_or("user:direct");
                        let source_type = Self::parse_source_type(
                            params.get("source_type").and_then(|v| v.as_str()).unwrap_or("user_direct"),
                        );
                        if let crate::actors::GateOutcome::Allow { status: crate::graph::Status::Draft, .. } =
                            trust.gate(source_id, source_type, new_content, false).await
                        {
                            node.status = crate::graph::Status::Draft;
                        }
                    }

                    // Save updated node
                    match l2.add_node(&node).await {
                        Ok(_) => {
                            tracing::info!("MCP: node updated: {}", node_id_str);
                            serde_json::json!({
                                "ok": true,
                                "updated": true,
                                "node_id": node_id_str
                            })
                        }
                        Err(e) => {
                            tracing::error!("MCP: failed to update node: {}", e);
                            serde_json::json!({
                                "ok": false,
                                "error": e.to_string()
                            })
                        }
                    }
                } else {
                    // No content to update, just acknowledge
                    serde_json::json!({
                        "ok": true,
                        "updated": true,
                        "node_id": node_id_str
                    })
                }
            }
            Ok(None) => {
                tracing::warn!("MCP: node not found: {}", node_id_str);
                serde_json::json!({
                    "ok": false,
                    "error": "node not found"
                })
            }
            Err(e) => {
                tracing::error!("MCP: failed to fetch node: {}", e);
                serde_json::json!({
                    "ok": false,
                    "error": e.to_string()
                })
            }
        }
    }
    
    async fn fetch_l2_atoms(&self, params: Value) -> Value {
        let atom_ids: Vec<&str> = params.get("atom_ids")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        let scope = params.get("scope").and_then(|v| v.as_str()).unwrap_or("workspace");
        
        tracing::info!("MCP: fetch_l2_atoms(count={}, scope={})", atom_ids.len(), scope);
        
        // Fetch nodes via L2Actor
        let l2 = self.l2.read().await;
        let mut atoms = Vec::new();
        
        for id_str in atom_ids {
            let node_id = crate::graph::NodeId::from_string(id_str);
            match l2.get_node(&node_id).await {
                Ok(Some(node)) => {
                    atoms.push(serde_json::json!({
                        "id": node.id.0,
                        "node_type": format!("{:?}", node.node_type).to_lowercase(),
                        "content": node.content,
                        "level": format!("{:?}", node.level),
                        "created_at": node.created_at.to_rfc3339()
                    }));
                }
                Ok(None) => {
                    tracing::warn!("MCP: node not found: {}", id_str);
                }
                Err(e) => {
                    tracing::error!("MCP: failed to fetch node {}: {}", id_str, e);
                }
            }
        }
        
        serde_json::json!({
            "ok": true,
            "atoms": atoms
        })
    }

    /// Показать всё, что лежит в памяти (L2-узлы) — для инспекции «что реально сохранено».
    async fn list_memory(&self, params: Value) -> Value {
        let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
        let l2 = self.l2.read().await;
        match l2.list_all_nodes().await {
            Ok(mut nodes) => {
                nodes.sort_by(|a, b| b.created_at.cmp(&a.created_at)); // новые сверху
                let total = nodes.len();
                nodes.truncate(limit);
                let items: Vec<Value> = nodes.iter().map(|n| serde_json::json!({
                    "id": n.id.0,
                    "level": format!("{:?}", n.level),
                    "node_type": format!("{:?}", n.node_type).to_lowercase(),
                    "status": format!("{:?}", n.status),
                    "content": n.content,
                    "created_at": n.created_at.to_rfc3339(),
                })).collect();
                serde_json::json!({"ok": true, "total": total, "shown": items.len(), "nodes": items})
            }
            Err(e) => serde_json::json!({"ok": false, "code": "LIST_MEMORY_FAILED", "error": e.to_string()}),
        }
    }

    async fn get_chain(&self, params: Value) -> Value {
        let direction = params.get("direction").and_then(|v| v.as_str()).unwrap_or("backward");
        let max_depth = params.get("max_depth").and_then(|v| v.as_u64()).unwrap_or(3) as usize;
        let anchor = params.get("anchor").cloned().unwrap_or(Value::Null);
        let scope = params.get("scope").and_then(|v| v.as_str()).unwrap_or("workspace");
        
        tracing::info!("MCP: get_chain(direction='{}', max_depth={}, scope={})", direction, max_depth, scope);
        
        // Проверяем наличие ChainActor
        let chain_actor = match &self.chain {
            Some(chain) => chain,
            None => {
                return serde_json::json!({
                    "ok": false,
                    "error": "ChainActor not initialized"
                });
            }
        };
        
        // Определяем тип обхода
        let result = match direction {
            "backward" => {
                // Приоритет: anchor.id (node_id) → anchor.text (symptom)
                if let Some(id_str) = anchor.get("id").and_then(|v| v.as_str()) {
                    let node_id = crate::graph::NodeId::from_string(id_str);
                    chain_actor.chain_backward_from_node(&node_id, max_depth).await
                } else {
                    let symptom_text = anchor.get("text").and_then(|v| v.as_str()).unwrap_or("");
                    if symptom_text.is_empty() {
                        return serde_json::json!({
                            "ok": false,
                            "error": "Anchor id (node_id) or text (symptom) is required for backward chain"
                        });
                    }
                    chain_actor.chain_backward(symptom_text, max_depth).await
                }
            }
            "forward_pre" => {
                // Извлекаем cause_id из anchor
                let cause_id_str = anchor.get("id").and_then(|v| v.as_str());
                if let Some(id_str) = cause_id_str {
                    let node_id = crate::graph::NodeId::from_string(id_str);
                    chain_actor.chain_forward_pre(&node_id, max_depth).await
                } else {
                    return serde_json::json!({
                        "ok": false,
                        "error": "Anchor id (cause_id) is required for forward_pre chain"
                    });
                }
            }
            "forward_post" => {
                // Извлекаем action_node_id из anchor
                let action_id_str = anchor.get("id").and_then(|v| v.as_str());
                if let Some(id_str) = action_id_str {
                    let node_id = crate::graph::NodeId::from_string(id_str);
                    chain_actor.chain_forward_post(&node_id, max_depth).await
                } else {
                    return serde_json::json!({
                        "ok": false,
                        "error": "Anchor id (action_node_id) is required for forward_post chain"
                    });
                }
            }
            _ => {
                return serde_json::json!({
                    "ok": false,
                    "error": format!("Unknown direction: {}", direction)
                });
            }
        };
        
        match result {
            Ok(chain_result) => {
                let entries: Vec<Value> = chain_result.entries.iter().map(|entry| {
                    let mut obj = serde_json::json!({
                        "node_id": entry.node_id.0,
                        "depth": entry.depth,
                    });
                    if let Some(edge) = &entry.edge {
                        obj["edge"] = serde_json::json!({
                            "source": edge.source.0,
                            "target": edge.target.0,
                            "relation": format!("{:?}", edge.relation).to_lowercase(),
                        });
                    }
                    if let Some(relation) = &entry.relation {
                        obj["relation"] = serde_json::json!(format!("{:?}", relation).to_lowercase());
                    }
                    obj
                }).collect();
                
                serde_json::json!({
                    "ok": true,
                    "chain": entries,
                    "reached_root": chain_result.reached_root,
                    "max_depth_reached": chain_result.max_depth_reached
                })
            }
            Err(e) => {
                tracing::error!("MCP: get_chain error: {}", e);
                serde_json::json!({
                    "ok": false,
                    "error": e.to_string()
                })
            }
        }
    }
    
    async fn memory_query(&self, params: Value) -> Value {
        let query = params.get("query").and_then(|v| v.as_str()).unwrap_or("");
        let depth = params.get("depth").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
        let scope = params.get("scope").and_then(|v| v.as_str()).unwrap_or("workspace");
        
        tracing::info!("MCP: memory_query(query='{}', depth={}, scope={})", query, depth, scope);
        
        // Если query пустой, возвращаем ошибку
        if query.is_empty() {
            return serde_json::json!({
                "ok": false,
                "error": "Query is required for memory_query"
            });
        }
        
        // Определяем top_k на основе depth
        let top_k = match depth {
            0 => 5,   // shallow
            1 => 10,  // medium
            2 => 20,  // deep
            _ => 10,
        };
        
        // Семантический recall через SearchActor (vector + keyword hybrid).
        // load_all_nodes (на старте) и propose_new_memory (в сессии) индексируют
        // узлы в этот же актор, поэтому здесь ходим в вектор, а не делаем
        // подстрочный contains() по всей строке запроса (прежнее поведение
        // возвращало 0 на любом непословном запросе).
        if let Some(search) = &self.search {
            let s = search.read().await;
            let query_vector = s.embed_text(query).await;
            let workspace_id = match scope {
                "global" => Some("global".to_string()),
                _ => None, // workspace scope: не сужаем жёстко (узлы могут быть в разных ws)
            };
            let filters = crate::actors::SearchFilters {
                level: None,
                status: None,
                node_type: None,
                tags: Vec::new(),
                workspace_id,
            };
            let keywords: Vec<String> = query.split_whitespace().map(|w| w.to_lowercase()).collect();
            match s.hybrid_search(&query_vector, &keywords, top_k, filters).await {
                Ok(hits) if !hits.is_empty() => {
                    let results: Vec<Value> = hits
                        .iter()
                        .map(|r| {
                            serde_json::json!({
                                "id": r.node_id.0,
                                "node_type": format!("{:?}", r.node_type).to_lowercase(),
                                "content": r.content,
                                "level": format!("{:?}", r.level),
                                "score": r.score,
                            })
                        })
                        .collect();
                    let total = results.len();
                    return serde_json::json!({
                        "ok": true,
                        "results": results,
                        "total_found": total,
                        "depth": depth,
                        "backend": s.embedding_backend_label()
                    });
                }
                Ok(_) => tracing::info!("MCP: memory_query hybrid empty → fallback to substring"),
                Err(e) => tracing::warn!("MCP: memory_query hybrid_search failed: {} → fallback", e),
            }
        }

        // Fallback (SearchActor не подключён или hybrid пуст): backend-listing + substring.
        let l2_guard = self.l2.read().await;
        let mut all_nodes = Vec::new();
        
        // Используем backend для получения всех узлов по префиксу "node:"
        let backend = l2_guard.backend();
        match backend.list_keys("node:").await {
            Ok(keys) => {
                tracing::info!("MCP: memory_query found {} node keys", keys.len());
                for key in keys {
                    // Извлекаем node_id из ключа "node:{id}"
                    if let Some(id_str) = key.strip_prefix("node:") {
                        let node_id = crate::graph::NodeId(id_str.to_string());
                        match l2_guard.get_node(&node_id).await {
                            Ok(Some(node)) => {
                                tracing::info!("MCP: memory_query loaded node {} type {:?}", id_str, node.node_type);
                                all_nodes.push(node);
                            },
                            Ok(None) => {
                                tracing::warn!("MCP: memory_query node {} not found", id_str);
                            },
                            Err(e) => {
                                tracing::warn!("MCP: memory_query failed to get node {}: {}", id_str, e);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!("MCP: memory_query failed to list keys: {}", e);
            }
        }

        // Фильтруем по workspace и выполняем keyword search
        let mut results = Vec::new();
        let query_lower = query.to_lowercase();
        
        for node in all_nodes {
            // Проверяем workspace filter
            if scope == "workspace" {
                if let Some(ref ws_id) = node.metadata.workspace_id {
                    if ws_id != "default" && !ws_id.starts_with("ws_") {
                        continue;
                    }
                }
            } else if scope == "global" {
                if let Some(ref ws_id) = node.metadata.workspace_id {
                    if ws_id != "global" {
                        continue;
                    }
                }
            }
            
            // Keyword matching
            let content_lower = node.content.to_lowercase();
            if content_lower.contains(&query_lower) {
                results.push(serde_json::json!({
                    "id": node.id.0,
                    "node_type": format!("{:?}", node.node_type).to_lowercase(),
                    "content": node.content,
                    "level": format!("{:?}", node.level),
                    "score": 1.0, // exact match score
                    "created_at": node.created_at.to_rfc3339()
                }));
            }
        }
        
        // Сортируем по score и ограничиваем top_k
        results.truncate(top_k);
        
        serde_json::json!({
            "ok": true,
            "results": results,
            "total_found": results.len(),
            "depth": depth
        })
    }
    
    async fn detect_workspace_from_context(&self, params: Value) -> Value {
        let cwd = params.get("cwd").and_then(|v| v.as_str()).unwrap_or("");
        if cwd.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "cwd is required"
            });
        }
        tracing::info!("MCP: detect_workspace_from_context('{}')", cwd);

        // Preferred path: WorkspaceManager.detect_workspace — ищет совпадение по path,
        // при отсутствии — создаёт новый workspace с name = basename(cwd).
        if let Some(ws) = &self.workspace {
            match ws.detect_workspace(cwd).await {
                Ok(Some(ws_id)) => match ws.get_workspace(&ws_id).await {
                    Ok(Some(found)) => {
                        return serde_json::json!({
                            "ok": true,
                            "workspace_id": found.id,
                            "name": found.name,
                            "path": found.path,
                            "status": format!("{:?}", found.status).to_lowercase(),
                            "created": false
                        });
                    }
                    Ok(None) => {
                        return serde_json::json!({
                            "ok": false,
                            "code": "WORKSPACE_NOT_FOUND",
                            "error": format!("workspace {} vanished after detect", ws_id)
                        });
                    }
                    Err(e) => {
                        tracing::error!("MCP: detect_workspace get_workspace failed: {}", e);
                        return serde_json::json!({
                            "ok": false,
                            "code": "BACKEND_ERROR",
                            "error": e.to_string()
                        });
                    }
                },
                Ok(None) => {
                    return serde_json::json!({
                        "ok": true,
                        "workspace_id": Value::Null,
                        "message": "no workspace detected"
                    });
                }
                Err(e) => {
                    tracing::error!("MCP: detect_workspace failed: {}", e);
                    return serde_json::json!({
                        "ok": false,
                        "code": "DETECT_FAILED",
                        "error": e.to_string()
                    });
                }
            }
        }

        // Fallback: WorkspaceManager не подключён — return ok:false (no-stub policy).
        serde_json::json!({
            "ok": false,
            "code": "WORKSPACE_MANAGER_NOT_INITIALIZED",
            "error": "WorkspaceManager not attached to McpHandler; cannot detect workspace"
        })
    }

    async fn create_workspace(&self, params: Value) -> Value {
        let name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let path = params.get("path").and_then(|v| v.as_str()).map(String::from);
        if name.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "name is required"
            });
        }
        tracing::info!("MCP: create_workspace('{}', path={:?})", name, path);

        if let Some(ws) = &self.workspace {
            match ws.create_workspace(name.to_string(), path.clone()).await {
                Ok(workspace) => {
                    return serde_json::json!({
                        "ok": true,
                        "workspace_id": workspace.id,
                        "name": workspace.name,
                        "path": workspace.path,
                        "status": format!("{:?}", workspace.status).to_lowercase(),
                        "created_at": workspace.created_at.to_rfc3339()
                    });
                }
                Err(e) => {
                    tracing::error!("MCP: create_workspace failed: {}", e);
                    return serde_json::json!({
                        "ok": false,
                        "code": "CREATE_FAILED",
                        "error": e.to_string()
                    });
                }
            }
        }

        serde_json::json!({
            "ok": false,
            "code": "WORKSPACE_MANAGER_NOT_INITIALIZED",
            "error": "WorkspaceManager not attached to McpHandler; cannot create workspace"
        })
    }

    async fn bootstrap_memory(&self, params: Value) -> Value {
        let cwd = params.get("cwd").and_then(|v| v.as_str()).unwrap_or("");
        let query = params.get("query").and_then(|v| v.as_str()).unwrap_or("");
        let depth = params.get("depth").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
        if cwd.is_empty() || query.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "cwd and query are required"
            });
        }
        tracing::info!(
            "MCP: bootstrap_memory(cwd='{}', query='{}', depth={})",
            cwd, query, depth
        );

        // 1. Detect (or create) the workspace for this cwd.
        let workspace_id = if let Some(ws) = &self.workspace {
            match ws.detect_workspace(cwd).await {
                Ok(Some(id)) => id,
                Ok(None) => "default".to_string(),
                Err(e) => {
                    tracing::error!("MCP: bootstrap detect_workspace failed: {}", e);
                    return serde_json::json!({
                        "ok": false,
                        "code": "DETECT_FAILED",
                        "error": e.to_string()
                    });
                }
            }
        } else {
            "default".to_string()
        };

        // 2. Run memory_query against the detected workspace scope.
        let query_result = self
            .memory_query(serde_json::json!({
                "query": query,
                "depth": depth,
                "scope": "workspace"
            }))
            .await;

        let results = query_result
            .get("results")
            .cloned()
            .unwrap_or(serde_json::json!([]));
        let total_found = query_result
            .get("total_found")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        serde_json::json!({
            "ok": true,
            "workspace_id": workspace_id,
            "query_results": results,
            "total_found": total_found,
            "depth": depth
        })
    }

    // ============ 02-storage: link / archive / restore / unlink ============

    async fn link_nodes(&self, params: Value) -> Value {
        let from_id = params.get("from_id").and_then(|v| v.as_str()).unwrap_or("");
        let to_id = params.get("to_id").and_then(|v| v.as_str()).unwrap_or("");
        let relation_str = params.get("relation").and_then(|v| v.as_str()).unwrap_or("RelatedTo");
        let confidence = params.get("confidence").and_then(|v| v.as_f64()).map(|f| f as f32);
        // workspace_id: storage partition для edge. До этой правки (bug_report/001)
        // ребро всегда создавалось с workspace_id=None, и BFS в suggest_related
        // не находил связанные узлы.
        let workspace_id_param = params.get("workspace_id").and_then(|v| v.as_str());
        if from_id.is_empty() || to_id.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "from_id and to_id are required"
            });
        }

        // Bug 005 регрессия 2: case-insensitive matching — пользователи передают
        // "leadsto", а не "LeadsTo". Без этого ребро создавалось как RelatedTo
        // (default) или отклонялось, и chain_forward_pre не находил LeadsTo рёбер.
        let relation = match relation_str.to_lowercase().as_str() {
            "relatedto" => crate::graph::Relation::RelatedTo,
            "leadsto" => crate::graph::Relation::LeadsTo,
            "explainedby" => crate::graph::Relation::ExplainedBy,
            "derivedfrom" => crate::graph::Relation::DerivedFrom,
            "dependson" => crate::graph::Relation::DependsOn,
            "inhibits" => crate::graph::Relation::Inhibits,
            "contradicts" => crate::graph::Relation::Contradicts,
            "implements" => crate::graph::Relation::Implements,
            "supersedes" => crate::graph::Relation::Supersedes,
            other => {
                return serde_json::json!({
                    "ok": false,
                    "code": "INVALID_PARAMS",
                    "error": format!("Unknown relation: {}", other)
                });
            }
        };

        let mut edge = crate::graph::Edge::new(
            crate::graph::NodeId::from_string(from_id),
            crate::graph::NodeId::from_string(to_id),
            relation,
        );
        if let Some(c) = confidence {
            edge = edge.with_confidence(c);
        }
        // Привязка к workspace: явный параметр > активный workspace > None.
        let edge_workspace: Option<String> = if let Some(wid) = workspace_id_param {
            Some(wid.to_string())
        } else if let Some(ws) = &self.workspace {
            ws.get_active_workspace_id().await
        } else {
            None
        };
        if let Some(wid) = edge_workspace {
            edge = edge.with_workspace(wid);
        }

        let l2 = self.l2.read().await;
        match l2.add_edge(&edge).await {
            Ok(eid) => serde_json::json!({
                "ok": true,
                "edge_id": eid.0,
                "from_id": from_id,
                "to_id": to_id,
                "relation": relation_str,
                "confidence": edge.confidence,
                "workspace_id": edge.workspace_id
            }),            Err(e) => serde_json::json!({
                "ok": false,
                "code": "LINK_FAILED",
                "error": e.to_string()
            }),
        }
    }

    async fn archive_node(&self, params: Value) -> Value {
        let node_id_str = params.get("node_id").and_then(|v| v.as_str()).unwrap_or("");
        if node_id_str.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "node_id is required"
            });
        }
        let node_id = crate::graph::NodeId::from_string(node_id_str);
        let l2 = self.l2.read().await;
        match l2.archive_node(&node_id).await {
            Ok(Some(status)) => serde_json::json!({
                "ok": true,
                "node_id": node_id_str,
                "status": format!("{:?}", status).to_lowercase(),
                "action": "archived"
            }),
            Ok(None) => serde_json::json!({
                "ok": false,
                "code": "NODE_NOT_FOUND",
                "error": format!("node {} not found", node_id_str)
            }),
            Err(e) => serde_json::json!({
                "ok": false,
                "code": "ARCHIVE_FAILED",
                "error": e.to_string()
            }),
        }
    }

    async fn restore_node(&self, params: Value) -> Value {
        let node_id_str = params.get("node_id").and_then(|v| v.as_str()).unwrap_or("");
        if node_id_str.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "node_id is required"
            });
        }
        let node_id = crate::graph::NodeId::from_string(node_id_str);
        let l2 = self.l2.read().await;
        match l2.restore_node(&node_id).await {
            Ok(Some(status)) => serde_json::json!({
                "ok": true,
                "node_id": node_id_str,
                "status": format!("{:?}", status).to_lowercase(),
                "action": "restored"
            }),
            Ok(None) => serde_json::json!({
                "ok": false,
                "code": "NODE_NOT_FOUND",
                "error": format!("node {} not found", node_id_str)
            }),
            Err(e) => serde_json::json!({
                "ok": false,
                "code": "RESTORE_FAILED",
                "error": e.to_string()
            }),
        }
    }

    /// Bug 005 регрессия 3: получить edge_id для unlink_edge. Без этого инструмента
    /// клиент не имеет легального способа узнать id существующего ребра.
    async fn list_edges(&self, params: Value) -> Value {
        let from_id = params.get("from_id").and_then(|v| v.as_str());
        let to_id = params.get("to_id").and_then(|v| v.as_str());

        let from_node = from_id.map(|s| crate::graph::NodeId::from_string(s));
        let to_node = to_id.map(|s| crate::graph::NodeId::from_string(s));

        let l2 = self.l2.read().await;
        match l2.find_edges(from_node.as_ref(), to_node.as_ref()).await {
            Ok(edges) => {
                let items: Vec<Value> = edges
                    .iter()
                    .map(|e| {
                        serde_json::json!({
                            "edge_id": e.id.0,
                            "from_id": e.source.0,
                            "to_id": e.target.0,
                            "relation": format!("{:?}", e.relation),
                            "confidence": e.confidence,
                            "workspace_id": e.workspace_id,
                        })
                    })
                    .collect();
                serde_json::json!({
                    "ok": true,
                    "edges": items,
                    "count": items.len()
                })
            }
            Err(e) => serde_json::json!({
                "ok": false,
                "code": "LIST_EDGES_FAILED",
                "error": e.to_string()
            }),
        }
    }

    async fn unlink_edge(&self, params: Value) -> Value {
        let edge_id_str = params.get("edge_id").and_then(|v| v.as_str()).unwrap_or("");
        if edge_id_str.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "edge_id is required"
            });
        }
        let edge_id = crate::graph::EdgeId(edge_id_str.to_string());
        let l2 = self.l2.read().await;
        match l2.delete_edge(&edge_id).await {
            Ok(true) => serde_json::json!({
                "ok": true,
                "edge_id": edge_id_str,
                "action": "unlinked"
            }),
            Ok(false) => serde_json::json!({
                "ok": false,
                "code": "EDGE_NOT_FOUND",
                "error": format!("edge {} not found", edge_id_str)
            }),
            Err(e) => serde_json::json!({
                "ok": false,
                "code": "UNLINK_FAILED",
                "error": e.to_string()
            }),
        }
    }

    // ============ 05-workspace-management: list / switch / archive ============

    async fn list_workspaces(&self, _params: Value) -> Value {
        let ws = match &self.workspace {
            Some(w) => w,
            None => {
                return serde_json::json!({
                    "ok": false,
                    "code": "WORKSPACE_MANAGER_NOT_INITIALIZED",
                    "error": "WorkspaceManager not attached"
                });
            }
        };
        match ws.list_workspaces(None).await {
            Ok(list) => {
                // Bug 005: пересчитываем node_count из реального хранилища L2,
                // а не из инкрементального счётчика WorkspaceManager.
                // Счётчик рассинхронизируется при создании узлов через process_action
                // (путь flush), который не вызывает bump_node_count.
                let l2 = self.l2.read().await;
                let mut summaries: Vec<Value> = Vec::with_capacity(list.len());
                for w in &list {
                    let real_node_count = l2.count_by_workspace(&w.id).await.unwrap_or(w.node_count);
                    // Bug 004 (хвост): edge_count тоже из хранилища, а не из кэша WorkspaceManager.
                    let real_edge_count = l2.count_edges_by_workspace(&w.id).await.unwrap_or(w.edge_count);
                    summaries.push(serde_json::json!({
                        "id": w.id,
                        "name": w.name,
                        "path": w.path,
                        "status": format!("{:?}", w.status).to_lowercase(),
                        "node_count": real_node_count,
                        "edge_count": real_edge_count,
                        "created_at": w.created_at.to_rfc3339(),
                        "updated_at": w.updated_at.to_rfc3339(),
                    }));
                }
                let active = ws.get_active_workspace_id().await;
                serde_json::json!({
                    "ok": true,
                    "workspaces": summaries,
                    "count": summaries.len(),
                    "active_workspace_id": active
                })
            }
            Err(e) => serde_json::json!({
                "ok": false,
                "code": "LIST_FAILED",
                "error": e.to_string()
            }),
        }
    }

    async fn switch_workspace(&self, params: Value) -> Value {
        let workspace_id = params.get("workspace_id").and_then(|v| v.as_str()).unwrap_or("");
        if workspace_id.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "workspace_id is required"
            });
        }
        let ws = match &self.workspace {
            Some(w) => w,
            None => {
                return serde_json::json!({
                    "ok": false,
                    "code": "WORKSPACE_MANAGER_NOT_INITIALIZED",
                    "error": "WorkspaceManager not attached"
                });
            }
        };
        match ws.switch_workspace(workspace_id).await {
            Ok(true) => serde_json::json!({
                "ok": true,
                "workspace_id": workspace_id,
                "action": "switched",
                "active_workspace_id": ws.get_active_workspace_id().await
            }),
            Ok(false) => serde_json::json!({
                "ok": false,
                "code": "WORKSPACE_NOT_FOUND",
                "error": format!("workspace {} not found", workspace_id)
            }),
            Err(e) => serde_json::json!({
                "ok": false,
                "code": "SWITCH_FAILED",
                "error": e.to_string()
            }),
        }
    }

    // ============ 02-storage + 04-vector-search: search_nodes / vector_search / suggest_related ============

    async fn search_nodes(&self, params: Value) -> Value {
        let query = params.get("query").and_then(|v| v.as_str()).unwrap_or("");
        let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
        let workspace_id = params.get("workspace_id").and_then(|v| v.as_str()).map(String::from);
        let level_str = params.get("level").and_then(|v| v.as_str());

        if query.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "query is required"
            });
        }

        let search = match &self.search {
            Some(s) => s,
            None => {
                return serde_json::json!({
                    "ok": false,
                    "code": "SEARCH_ACTOR_NOT_INITIALIZED",
                    "error": "SearchActor not attached to McpHandler"
                });
            }
        };
        let search_guard = search.read().await;

        let level = match level_str {
            Some("L0") => Some(crate::graph::Level::L0),
            Some("L1") => Some(crate::graph::Level::L1),
            Some("L2") => Some(crate::graph::Level::L2),
            Some("GKL") => Some(crate::graph::Level::GKL),
            Some("S0") => Some(crate::graph::Level::S0),
            _ => None,
        };

        let filters = crate::actors::SearchFilters {
            level,
            status: None,
            node_type: None,
            tags: Vec::new(),
            workspace_id,
        };

        let keywords: Vec<String> = query.split_whitespace().map(String::from).collect();
        match search_guard.keyword_search(&keywords, limit, &filters).await {
            Ok(results) => {
                let items: Vec<Value> = results
                    .iter()
                    .map(|r| {
                        serde_json::json!({
                            "node_id": r.node_id.0,
                            "score": r.score,
                            "node_type": format!("{:?}", r.node_type).to_lowercase(),
                            "level": format!("{:?}", r.level),
                            "content": r.content,
                        })
                    })
                    .collect();
                serde_json::json!({
                    "ok": true,
                    "results": items,
                    "count": items.len(),
                    "query": query,
                    "backend": "keyword_search"
                })
            }
            Err(e) => serde_json::json!({
                "ok": false,
                "code": "SEARCH_FAILED",
                "error": e.to_string()
            }),
        }
    }

    async fn vector_search(&self, params: Value) -> Value {
        let text = params.get("text").and_then(|v| v.as_str());
        let vector = params.get("vector").and_then(|v| v.as_array());
        let top_k = params.get("top_k").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
        let min_score = params
            .get("min_score")
            .and_then(|v| v.as_f64())
            .map(|f| f as f32)
            .unwrap_or(0.0);
        let workspace_id = params.get("workspace_id").and_then(|v| v.as_str()).map(String::from);

        if text.is_none() && vector.is_none() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "either text or vector is required"
            });
        }

        let search_guard = match &self.search {
            Some(s) => s.read().await,
            None => {
                return serde_json::json!({
                    "ok": false,
                    "code": "SEARCH_ACTOR_NOT_INITIALIZED",
                    "error": "SearchActor not attached to McpHandler"
                });
            }
        };

        // Вектор запроса: явный vector, иначе embed_text (реальный провайдер, если
        // подключён; иначе char-bag). Тот же путь эмбеддинга, что и при индексации —
        // вектор запроса и векторы узлов сопоставимы.
        let query_vector: Vec<f32> = if let Some(arr) = vector {
            arr.iter()
                .filter_map(|v| v.as_f64().map(|f| f as f32))
                .collect()
        } else if let Some(t) = text {
            search_guard.embed_text(t).await
        } else {
            return serde_json::json!({
                "ok": false,
                "code": "EMBEDDING_DISABLED",
                "error": "no text or vector provided"
            });
        };

        let query = crate::actors::SearchQuery {
            query_vector,
            top_k,
            filters: crate::actors::SearchFilters {
                level: None,
                status: None,
                node_type: None,
                tags: Vec::new(),
                workspace_id,
            },
        };

        match search_guard.search(&query).await {
            Ok(mut results) => {
                if min_score > 0.0 {
                    results.retain(|r| r.score >= min_score);
                }
                let items: Vec<Value> = results
                    .iter()
                    .map(|r| {
                        serde_json::json!({
                            "node_id": r.node_id.0,
                            "score": r.score,
                            "content": r.content,
                            "level": format!("{:?}", r.level),
                        })
                    })
                    .collect();
                serde_json::json!({
                    "ok": true,
                    "results": items,
                    "count": items.len(),
                    "backend": search_guard.embedding_backend_label(),
                    "note": "backend=openai-compatible → реальные эмбеддинги; char_bag_fallback → провайдер выключен"
                })
            }
            Err(e) => serde_json::json!({
                "ok": false,
                "code": "VECTOR_SEARCH_FAILED",
                "error": e.to_string()
            }),
        }
    }

    async fn suggest_related(&self, params: Value) -> Value {
        let node_id_str = params.get("node_id").and_then(|v| v.as_str()).unwrap_or("");
        let max_depth = params.get("max_depth").and_then(|v| v.as_u64()).unwrap_or(2) as usize;
        let top_k = params.get("top_k").and_then(|v| v.as_u64()).unwrap_or(10) as usize;

        if node_id_str.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "node_id is required"
            });
        }

        let node_id = crate::graph::NodeId::from_string(node_id_str);
        let l2 = self.l2.read().await;

        // BFS по edges_from: для каждого node возвращаем 1-hop соседей.
        // Сейчас L2Actor.edges_from даёт только out-edges; для suggest_related
        // в V2.0 ограничимся 1-hop (max_depth > 1 = multi-hop через цепочку).
        let mut visited: std::collections::HashSet<crate::graph::NodeId> = std::collections::HashSet::new();
        visited.insert(node_id.clone());
        let mut frontier: Vec<(crate::graph::NodeId, usize, String)> = vec![(node_id.clone(), 0, "self".to_string())];
        let mut suggestions: Vec<(crate::graph::NodeId, usize, String, f32)> = Vec::new();

        for _ in 0..max_depth {
            let mut next_frontier: Vec<(crate::graph::NodeId, usize, String)> = Vec::new();
            for (current_id, depth, relation) in frontier.iter() {
                let edges = l2.edges_from(current_id).await.unwrap_or_default();
                for edge in edges {
                    if visited.insert(edge.target.clone()) {
                        let rel = format!("{:?}", edge.relation).to_lowercase();
                        let score = 1.0_f32 / (1.0_f32 + *depth as f32); // ближе = выше
                        suggestions.push((edge.target.clone(), depth + 1, rel.clone(), score));
                        next_frontier.push((edge.target.clone(), depth + 1, rel));
                    }
                }
            }
            if next_frontier.is_empty() {
                break;
            }
            frontier = next_frontier;
        }

        // Сортировка по score, top_k
        suggestions.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));
        suggestions.truncate(top_k);

        // Получаем контент для каждого suggestion
        let mut items: Vec<Value> = Vec::new();
        for (id, depth, relation, score) in suggestions {
            if let Ok(Some(node)) = l2.get_node(&id).await {
                items.push(serde_json::json!({
                    "node_id": id.0,
                    "depth": depth,
                    "relation": relation,
                    "score": score,
                    "content": node.content,
                    "node_type": format!("{:?}", node.node_type).to_lowercase(),
                }));
            }
        }

        serde_json::json!({
            "ok": true,
            "source_node_id": node_id_str,
            "suggestions": items,
            "count": items.len(),
            "max_depth": max_depth
        })
    }

    // ============ 06-cross-workspace: fetch / find_overlaps / suggest_links ============

    async fn fetch_from_workspace(&self, params: Value) -> Value {
        let workspace_id = params.get("workspace_id").and_then(|v| v.as_str()).unwrap_or("");
        let query = params.get("query").and_then(|v| v.as_str()).unwrap_or("");
        let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(20) as usize;

        if workspace_id.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "workspace_id is required"
            });
        }

        let l2 = self.l2.read().await;
        let nodes = match l2.list_by_workspace(workspace_id).await {
            Ok(n) => n,
            Err(e) => {
                return serde_json::json!({
                    "ok": false,
                    "code": "FETCH_FAILED",                    "error": e.to_string()
                });
            }
        };

        let q_lower = query.to_lowercase();
        let filtered: Vec<&Node> = if query.is_empty() {
            nodes.iter().collect()
        } else {
            nodes
                .iter()
                .filter(|n| n.content.to_lowercase().contains(&q_lower))
                .collect()
        };
        let total_found = filtered.len();

        let items: Vec<Value> = filtered
            .iter()
            .take(limit)
            .map(|n| {
                serde_json::json!({
                    "node_id": n.id.0,
                    "node_type": format!("{:?}", n.node_type).to_lowercase(),
                    "content": n.content,
                    "status": format!("{:?}", n.status).to_lowercase(),
                    "workspace_id": n.metadata.workspace_id,
                })
            })
            .collect();

        serde_json::json!({
            "ok": true,
            "workspace_id": workspace_id,
            "query": query,
            "results": items,
            "count": items.len(),
            "total_found": total_found
        })
    }

    async fn find_workspace_overlaps(&self, params: Value) -> Value {
        let source = params.get("source_workspace").and_then(|v| v.as_str()).unwrap_or("");
        let min_similarity = params
            .get("min_similarity")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.3) as f32;
        let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;

        if source.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "source_workspace is required"
            });
        }

        let ws = match &self.workspace {
            Some(w) => w,
            None => {
                return serde_json::json!({
                    "ok": false,
                    "code": "WORKSPACE_MANAGER_NOT_INITIALIZED",
                    "error": "WorkspaceManager not attached"
                });
            }
        };

        // Получаем L2-узлы source workspace
        let source_nodes = match self.l2.read().await.list_by_workspace(source).await {
            Ok(n) => n,
            Err(e) => {
                return serde_json::json!({
                    "ok": false,
                    "code": "SOURCE_LIST_FAILED",                    "error": e.to_string()
                });
            }
        };

        // Список всех workspace'ов
        let all_workspaces = match ws.list_workspaces(None).await {
            Ok(w) => w,
            Err(e) => {
                return serde_json::json!({
                    "ok": false,
                    "code": "LIST_FAILED",
                    "error": e.to_string()
                });
            }
        };

        // Считаем Jaccard similarity по node_ids между source и каждым target workspace
        let source_ids: std::collections::HashSet<String> =
            source_nodes.iter().map(|n| n.id.0.clone()).collect();

        let mut overlaps: Vec<Value> = Vec::new();
        for target in all_workspaces.iter().filter(|w| w.id != source) {
            let target_nodes = match self.l2.read().await.list_by_workspace(&target.id).await {
                Ok(n) => n,
                Err(_) => continue,
            };
            let target_ids: std::collections::HashSet<String> =
                target_nodes.iter().map(|n| n.id.0.clone()).collect();
            if target_ids.is_empty() && source_ids.is_empty() {
                continue;
            }
            let intersection = source_ids.intersection(&target_ids).count();
            let union = source_ids.union(&target_ids).count();
            let similarity = if union == 0 { 0.0 } else { intersection as f32 / union as f32 };

            if similarity >= min_similarity {
                overlaps.push(serde_json::json!({
                    "workspace_id": target.id,
                    "name": target.name,
                    "similarity": similarity,
                    "shared_nodes": intersection,
                    "total_nodes_in_target": target_ids.len(),
                }));
            }
        }

        // Сортируем по убыванию similarity
        overlaps.sort_by(|a, b| {
            let sa = a.get("similarity").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let sb = b.get("similarity").and_then(|v| v.as_f64()).unwrap_or(0.0);
            sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
        });
        overlaps.truncate(limit);

        serde_json::json!({
            "ok": true,
            "source_workspace": source,
            "min_similarity": min_similarity,
            "overlaps": overlaps,
            "count": overlaps.len()
        })
    }

    async fn suggest_cross_workspace_links(&self, params: Value) -> Value {
        let workspace_id = params.get("workspace_id").and_then(|v| v.as_str()).unwrap_or("");
        let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;

        if workspace_id.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "workspace_id is required"
            });
        }

        // Собираем теги workspace'а из его L2-узлов
        let ws_nodes = match self.l2.read().await.list_by_workspace(workspace_id).await {
            Ok(n) => n,
            Err(e) => {
                return serde_json::json!({
                    "ok": false,
                    "code": "WORKSPACE_LIST_FAILED",                    "error": e.to_string()
                });
            }
        };
        let ws_tags: std::collections::HashSet<String> = ws_nodes
            .iter()
            .flat_map(|n| n.metadata.tags.iter().cloned())
            .collect();

        // Все GKL-узлы, ранжированные по overlap тегов
        // (для V2.0 без GKLactor'а в McpHandler используем find_by_tags невозможно;
        // делаем V2.0 fallback: возвращаем L2-узлы из ДРУГИХ workspace, у которых есть общие теги)
        let ws = match &self.workspace {
            Some(w) => w,
            None => {
                return serde_json::json!({
                    "ok": false,
                    "code": "WORKSPACE_MANAGER_NOT_INITIALIZED",
                    "error": "WorkspaceManager not attached"
                });
            }
        };
        let all_workspaces = match ws.list_workspaces(None).await {
            Ok(w) => w,
            Err(e) => {
                return serde_json::json!({
                    "ok": false,
                    "code": "LIST_FAILED",
                    "error": e.to_string()
                });
            }
        };

        let mut suggestions: Vec<Value> = Vec::new();
        for target in all_workspaces.iter().filter(|w| w.id != workspace_id) {
            let target_nodes = match self.l2.read().await.list_by_workspace(&target.id).await {                Ok(n) => n,
                Err(_) => continue,
            };

            // Найти узлы target с общими тегами с workspace
            let shared: Vec<&Node> = target_nodes
                .iter()
                .filter(|n| n.metadata.tags.iter().any(|t| ws_tags.contains(t)))
                .collect();

            if !shared.is_empty() {
                suggestions.push(serde_json::json!({
                    "target_workspace_id": target.id,
                    "target_workspace_name": target.name,
                    "shared_tag_count": ws_tags
                        .iter()
                        .filter(|t| target_nodes.iter().any(|n| n.metadata.tags.contains(t)))
                        .count(),
                    "shared_node_count": shared.len(),
                    "shared_nodes": shared.iter().take(5).map(|n| serde_json::json!({
                        "node_id": n.id.0,
                        "content": n.content,
                        "tags": n.metadata.tags,
                    })).collect::<Vec<_>>(),
                }));
            }
        }

        suggestions.sort_by(|a, b| {
            let sa = a.get("shared_node_count").and_then(|v| v.as_u64()).unwrap_or(0);
            let sb = b.get("shared_node_count").and_then(|v| v.as_u64()).unwrap_or(0);
            sb.cmp(&sa)
        });
        suggestions.truncate(limit);

        serde_json::json!({
            "ok": true,
            "workspace_id": workspace_id,
            "workspace_tags_count": ws_tags.len(),
            "suggestions": suggestions,
            "count": suggestions.len(),
            "note": "V2.0: link suggestions based on shared tags between workspaces. Real GKL integration in V2.1."
        })
    }

    // ============ 07-memory-lifecycle: consolidate_workspace ============

    async fn consolidate_workspace(&self, params: Value) -> Value {
        let workspace_id = params.get("workspace_id").and_then(|v| v.as_str()).unwrap_or("");
        if workspace_id.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "workspace_id is required"
            });
        }
        tracing::info!("MCP: consolidate_workspace('{}')", workspace_id);

        // Единая последовательность консолидации — тот же ConsolidateRunner, что зовёт
        // координатор памяти при CycleTrigger (drain → L2 → L1 autogen → L0 autogen).
        let runner = crate::actors::ConsolidateRunner::new(
            self.l2.clone(),
            self.queue.clone(),
            self.l1.clone(),
            self.l0.clone(),
        );
        match runner.run(workspace_id).await {
            Ok(s) => {
                // Ручная консолидация сбрасывает счётчик координатора, чтобы он не
                // запускал лишний авто-цикл поверх уже сделанного.
                if let Some(orch) = &self.orchestrator {
                    orch.note_consolidated(workspace_id).await;
                }
                // Переиндексация: L0/L1 создаются при консолидации, но не
                // индексируются в SearchActor автоматически. Перезагружаем индекс.
                if let Some(search) = &self.search {
                    let search_guard = search.read().await;
                    if let Err(e) = search_guard.load_all_nodes().await {
                        tracing::warn!("consolidate_workspace re-index failed: {}", e);
                    }
                }
                serde_json::json!({
                "ok": true,
                "workspace_id": workspace_id,
                "drained_from_queue": s.drained_from_queue,
                "l2_atoms": s.l2_atoms,
                "l2_edges": s.l2_edges,
                "new_l1_count": s.new_l1_count,
                "new_l0_count": s.new_l0_count,
                "consolidated": true
            })
            }
            Err(e) => {
                tracing::error!("consolidate_workspace failed: {}", e);
                serde_json::json!({
                    "ok": false,
                    "code": "CONSOLIDATE_FAILED",
                    "error": e.to_string()
                })
            }
        }
    }

    async fn archive_workspace(&self, params: Value) -> Value {
        let workspace_id = params.get("workspace_id").and_then(|v| v.as_str()).unwrap_or("");
        if workspace_id.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "workspace_id is required"
            });
        }
        let ws = match &self.workspace {
            Some(w) => w,
            None => {
                return serde_json::json!({
                    "ok": false,
                    "code": "WORKSPACE_MANAGER_NOT_INITIALIZED",
                    "error": "WorkspaceManager not attached"
                });
            }
        };
        match ws.archive_workspace(workspace_id).await {
            Ok(true) => serde_json::json!({
                "ok": true,
                "workspace_id": workspace_id,
                "action": "archived",
                "active_workspace_id": ws.get_active_workspace_id().await
            }),
            Ok(false) => serde_json::json!({
                "ok": false,
                "code": "WORKSPACE_NOT_FOUND",
                "error": format!("workspace {} not found", workspace_id)
            }),
            Err(e) => serde_json::json!({
                "ok": false,
                "code": "ARCHIVE_FAILED",
                "error": e.to_string()
            }),
        }
    }

    // ============ 07-memory-lifecycle: route_l1 / search_l0_clusters (Block 6 skeleton) ============

    /// V2.0 SKELETON: route_l1 — прокидывает L2-атом в L1-домен через L1Actor.
    /// V2.1 заменит на полную версию с LLM-эвристикой и multi-domain scoring.
    async fn route_l1(&self, params: Value) -> Value {
        let workspace_id = params.get("workspace_id").and_then(|v| v.as_str()).unwrap_or("");
        let l2_atom_id = params.get("l2_atom_id").and_then(|v| v.as_str()).unwrap_or("");
        if workspace_id.is_empty() || l2_atom_id.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "workspace_id and l2_atom_id are required"
            });
        }
        let l1_arc = match &self.l1 {
            Some(l) => l,
            None => return serde_json::json!({
                "ok": false,
                "code": "L1_ACTOR_NOT_INITIALIZED",
                "error": "L1Actor not attached"
            }),
        };
        let atom_id = crate::graph::NodeId::from_string(l2_atom_id);
        match l1_arc.read().await.find_domain_for_atom(&atom_id).await {
            Ok(Some(domain)) => {
                let items: Vec<Value> = domain
                    .member_atom_ids
                    .iter()
                    .map(|id| serde_json::json!({"node_id": id.0}))
                    .collect();
                serde_json::json!({
                    "ok": true,
                    "l2_atom_id": l2_atom_id,
                    "domain_id": domain.node.id.0,
                    "domain_name": domain.node.content,
                    "domain_tags": domain.node.metadata.tags,
                    "workspace_id": workspace_id,
                    "member_atoms": items,
                    "member_count": domain.member_atom_ids.len(),
                    "v20_skeleton": true,
                    "note": "V2.0 skeleton — full route_l1 with multi-domain scoring in V2.1"
                })
            }
            Ok(None) => serde_json::json!({
                "ok": true,
                "l2_atom_id": l2_atom_id,
                "workspace_id": workspace_id,
                "domain_id": Value::Null,
                "member_atoms": [],
                "member_count": 0,
                "v20_skeleton": true,
                "note": "atom not routed to any L1 domain yet. Run consolidate_workspace first."
            }),
            Err(e) => serde_json::json!({
                "ok": false,
                "code": "ROUTE_FAILED",
                "error": e.to_string()
            }),
        }
    }

    /// Поиск по L0-кластерам workspace: keyword-фильтрация по content + tags.
    /// Возвращает кластеры с member_ids и хаб (если есть).
    async fn search_l0_clusters(&self, params: Value) -> Value {
        let workspace_id = params.get("workspace_id").and_then(|v| v.as_str()).unwrap_or("");
        let query = params.get("query").and_then(|v| v.as_str()).unwrap_or("");
        let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
        if workspace_id.is_empty() || query.is_empty() {
            return serde_json::json!({
                "ok": false,
                "code": "INVALID_PARAMS",
                "error": "workspace_id and query are required"
            });
        }
        let l0_arc = match &self.l0 {
            Some(l) => l,
            None => return serde_json::json!({
                "ok": false,
                "code": "L0_ACTOR_NOT_INITIALIZED",
                "error": "L0Actor not attached"
            }),
        };
        let clusters = match l0_arc.read().await.list_clusters(workspace_id).await {
            Ok(c) => c,
            Err(e) => {
                return serde_json::json!({
                    "ok": false,
                    "code": "L0_LIST_FAILED",
                    "error": e.to_string()
                });
            }
        };
        // Хаб workspace (один на workspace, если создан)
        let hub = match l0_arc.read().await.get_hub(workspace_id).await {
            Ok(Some(h)) => serde_json::json!({
                "hub_id": h.node.id.0,
                "name": h.node.content,
                "member_cluster_ids": h.member_ids.iter().map(|id| id.0.clone()).collect::<Vec<_>>(),
            }),
            _ => serde_json::Value::Null,
        };

        // Keyword-фильтрация: content + tags
        let q_lower = query.to_lowercase();
        let query_words: Vec<&str> = q_lower.split_whitespace().collect();
        let mut filtered: Vec<&_> = clusters
            .iter()
            .filter(|c| {
                let content = c.node.content.to_lowercase();
                let tags_joined = c.node.metadata.tags.join(" ").to_lowercase();
                // Совпадение по любому слову из запроса
                query_words.iter().any(|w| content.contains(w) || tags_joined.contains(w))
            })
            .collect();
        filtered.truncate(limit);
        let items: Vec<Value> = filtered
            .iter()
            .map(|c| {
                serde_json::json!({
                    "cluster_id": c.node.id.0,
                    "name": c.node.content,
                    "tags": c.node.metadata.tags,
                    "level": format!("{:?}", c.node.level).to_lowercase(),
                    "status": format!("{:?}", c.node.status).to_lowercase(),
                    "member_domain_ids": c.member_ids.iter().map(|id| id.0.clone()).collect::<Vec<_>>(),
                })
            })
            .collect();
        serde_json::json!({
            "ok": true,
            "workspace_id": workspace_id,
            "query": query,
            "clusters": items,
            "count": items.len(),
            "total_clusters": clusters.len(),
            "hub": hub,
        })
    }

    // ============ 12-plan: 12 plan_* tools ============

    fn plan_to_json(p: &crate::actors::Plan) -> Value {
        serde_json::json!({
            "id": p.id,
            "level": p.level.as_str(),
            "status": format!("{:?}", p.status).to_lowercase(),
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

    async fn plan_create_p0(&self, params: Value) -> Value {
        let description = params.get("description").and_then(|v| v.as_str()).unwrap_or("");
        let autonomous_mode = params.get("autonomous_mode").and_then(|v| v.as_bool()).unwrap_or(false);
        if description.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "description is required"});
        }
        let plan_arc = match &self.plan {
            Some(p) => p,
            None => return serde_json::json!({"ok": false, "code": "PLAN_ACTOR_NOT_INITIALIZED", "error": "PlanActor not attached"}),
        };
        match plan_arc.read().await.plan_create_p0(description.to_string(), autonomous_mode).await {
            Ok(p) => serde_json::json!({"ok": true, "plan": Self::plan_to_json(&p)}),
            Err(e) => serde_json::json!({"ok": false, "code": "CREATE_FAILED", "error": e.to_string()}),
        }
    }

    async fn plan_propose_p1(&self, params: Value) -> Value {
        let p0_id = params.get("p0_id").and_then(|v| v.as_str()).unwrap_or("");
        let description = params.get("description").and_then(|v| v.as_str()).unwrap_or("");
        if p0_id.is_empty() || description.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "p0_id and description are required"});
        }
        let plan_arc = match &self.plan {
            Some(p) => p,
            None => return serde_json::json!({"ok": false, "code": "PLAN_ACTOR_NOT_INITIALIZED", "error": "PlanActor not attached"}),
        };
        match plan_arc.read().await.plan_propose_p1(p0_id, description.to_string()).await {
            Ok(p) => serde_json::json!({"ok": true, "plan": Self::plan_to_json(&p)}),
            Err(e) => serde_json::json!({"ok": false, "code": "PROPOSE_FAILED", "error": e.to_string()}),
        }
    }

    async fn plan_approve_p1(&self, params: Value) -> Value {
        let p1_id = params.get("p1_id").and_then(|v| v.as_str()).unwrap_or("");
        if p1_id.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "p1_id is required"});
        }
        let plan_arc = match &self.plan {
            Some(p) => p,
            None => return serde_json::json!({"ok": false, "code": "PLAN_ACTOR_NOT_INITIALIZED", "error": "PlanActor not attached"}),
        };
        match plan_arc.read().await.plan_approve_p1(p1_id).await {
            Ok(p) => serde_json::json!({"ok": true, "plan": Self::plan_to_json(&p)}),
            Err(e) => serde_json::json!({"ok": false, "code": "APPROVE_FAILED", "error": e.to_string()}),
        }
    }

    async fn plan_reject_p1(&self, params: Value) -> Value {
        let p1_id = params.get("p1_id").and_then(|v| v.as_str()).unwrap_or("");
        let reason = params.get("reason").and_then(|v| v.as_str()).unwrap_or("");
        if p1_id.is_empty() || reason.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "p1_id and reason are required"});
        }
        let plan_arc = match &self.plan {
            Some(p) => p,
            None => return serde_json::json!({"ok": false, "code": "PLAN_ACTOR_NOT_INITIALIZED", "error": "PlanActor not attached"}),
        };
        match plan_arc.read().await.plan_reject_p1(p1_id, reason.to_string()).await {
            Ok(p) => serde_json::json!({"ok": true, "plan": Self::plan_to_json(&p)}),
            Err(e) => serde_json::json!({"ok": false, "code": "REJECT_FAILED", "error": e.to_string()}),
        }
    }

    async fn plan_decompose(&self, params: Value) -> Value {
        let node_id = params.get("node_id").and_then(|v| v.as_str()).unwrap_or("");
        if node_id.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "node_id is required"});
        }
        let plan_arc = match &self.plan {
            Some(p) => p,
            None => return serde_json::json!({"ok": false, "code": "PLAN_ACTOR_NOT_INITIALIZED", "error": "PlanActor not attached"}),
        };
        match plan_arc.read().await.plan_decompose(node_id).await {
            Ok(plans) => {
                let items: Vec<Value> = plans.iter().map(|p| Self::plan_to_json(p)).collect();
                serde_json::json!({"ok": true, "children": items, "count": items.len()})
            }
            Err(e) => serde_json::json!({"ok": false, "code": "DECOMPOSE_FAILED", "error": e.to_string()}),
        }
    }

    async fn plan_claim(&self, params: Value) -> Value {
        let p3_id = params.get("p3_id").and_then(|v| v.as_str()).unwrap_or("");
        let agent_id = params.get("agent_id").and_then(|v| v.as_str()).unwrap_or("");
        if p3_id.is_empty() || agent_id.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "p3_id and agent_id are required"});
        }
        let plan_arc = match &self.plan {
            Some(p) => p,
            None => return serde_json::json!({"ok": false, "code": "PLAN_ACTOR_NOT_INITIALIZED", "error": "PlanActor not attached"}),
        };
        match plan_arc.read().await.plan_claim(p3_id, agent_id.to_string()).await {
            Ok(p) => serde_json::json!({"ok": true, "plan": Self::plan_to_json(&p)}),
            Err(e) => serde_json::json!({"ok": false, "code": "CLAIM_FAILED", "error": e.to_string()}),
        }
    }

    async fn plan_complete(&self, params: Value) -> Value {
        let p3_id = params.get("p3_id").and_then(|v| v.as_str()).unwrap_or("");
        let result = params.get("result").and_then(|v| v.as_str()).unwrap_or("");
        if p3_id.is_empty() || result.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "p3_id and result are required"});
        }
        let plan_arc = match &self.plan {
            Some(p) => p,
            None => return serde_json::json!({"ok": false, "code": "PLAN_ACTOR_NOT_INITIALIZED", "error": "PlanActor not attached"}),
        };
        match plan_arc.read().await.plan_complete(p3_id, result.to_string()).await {
            Ok(p) => serde_json::json!({"ok": true, "plan": Self::plan_to_json(&p)}),
            Err(e) => serde_json::json!({"ok": false, "code": "COMPLETE_FAILED", "error": e.to_string()}),
        }
    }

    async fn plan_set_problem(&self, params: Value) -> Value {
        let plan_id = params.get("plan_id").and_then(|v| v.as_str()).unwrap_or("");
        let comment = params.get("problem_comment").and_then(|v| v.as_str()).unwrap_or("");
        if plan_id.is_empty() || comment.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "plan_id and problem_comment are required"});
        }
        let plan_arc = match &self.plan {
            Some(p) => p,
            None => return serde_json::json!({"ok": false, "code": "PLAN_ACTOR_NOT_INITIALIZED", "error": "PlanActor not attached"}),
        };
        match plan_arc.read().await.plan_set_problem(plan_id, comment.to_string()).await {
            Ok(p) => serde_json::json!({"ok": true, "plan": Self::plan_to_json(&p)}),
            Err(e) => serde_json::json!({"ok": false, "code": "SET_PROBLEM_FAILED", "error": e.to_string()}),
        }
    }

    async fn plan_resolve_problem(&self, params: Value) -> Value {
        let plan_id = params.get("plan_id").and_then(|v| v.as_str()).unwrap_or("");
        let resolution = params.get("resolution").and_then(|v| v.as_str()).unwrap_or("");
        if plan_id.is_empty() || resolution.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "plan_id and resolution are required"});
        }
        let plan_arc = match &self.plan {
            Some(p) => p,
            None => return serde_json::json!({"ok": false, "code": "PLAN_ACTOR_NOT_INITIALIZED", "error": "PlanActor not attached"}),
        };
        match plan_arc.read().await.plan_resolve_problem(plan_id, resolution.to_string()).await {
            Ok(p) => serde_json::json!({"ok": true, "plan": Self::plan_to_json(&p)}),
            Err(e) => serde_json::json!({"ok": false, "code": "RESOLVE_FAILED", "error": e.to_string()}),
        }
    }

    async fn plan_status(&self, params: Value) -> Value {
        let status_str = params.get("filter").and_then(|v| v.as_str());
        let status_filter = match status_str {
            Some(s) => match s {
                "created" => Some(PlanStatus::Created),
                "in_progress" => Some(PlanStatus::InProgress),
                "pending_review" => Some(PlanStatus::PendingReview),
                "approved" => Some(PlanStatus::Approved),
                "rejected" => Some(PlanStatus::Rejected),
                "problem" => Some(PlanStatus::Problem),
                "done" => Some(PlanStatus::Done),
                "archived" => Some(PlanStatus::Archived),
                other => {
                    return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": format!("unknown status: {}", other)});
                }
            },
            None => None,
        };
        let plan_arc = match &self.plan {
            Some(p) => p,
            None => return serde_json::json!({"ok": false, "code": "PLAN_ACTOR_NOT_INITIALIZED", "error": "PlanActor not attached"}),
        };
        match plan_arc.read().await.plan_status(status_filter).await {
            Ok(plans) => {
                let items: Vec<Value> = plans.iter().map(|p| Self::plan_to_json(p)).collect();
                serde_json::json!({"ok": true, "plans": items, "count": items.len()})
            }
            Err(e) => serde_json::json!({"ok": false, "code": "STATUS_FAILED", "error": e.to_string()}),
        }
    }

    async fn plan_delete(&self, params: Value) -> Value {
        let plan_id = params.get("plan_id").and_then(|v| v.as_str()).unwrap_or("");
        let force = params.get("force").and_then(|v| v.as_bool()).unwrap_or(false);
        if plan_id.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "plan_id is required"});
        }
        let plan_arc = match &self.plan {
            Some(p) => p,
            None => return serde_json::json!({"ok": false, "code": "PLAN_ACTOR_NOT_INITIALIZED", "error": "PlanActor not attached"}),
        };
        match plan_arc.read().await.plan_delete(plan_id, force).await {
            Ok(true) => serde_json::json!({"ok": true, "plan_id": plan_id, "action": "deleted", "force": force}),
            Ok(false) => serde_json::json!({"ok": false, "code": "PLAN_NOT_FOUND", "error": format!("plan {} not found", plan_id)}),
            Err(e) => serde_json::json!({"ok": false, "code": "DELETE_FAILED", "error": e.to_string()}),
        }
    }

    async fn plan_archive(&self, params: Value) -> Value {
        let plan_id = params.get("plan_id").and_then(|v| v.as_str()).unwrap_or("");
        if plan_id.is_empty() {
            return serde_json::json!({"ok": false, "code": "INVALID_PARAMS", "error": "plan_id is required"});
        }
        let plan_arc = match &self.plan {
            Some(p) => p,
            None => return serde_json::json!({"ok": false, "code": "PLAN_ACTOR_NOT_INITIALIZED", "error": "PlanActor not attached"}),
        };
        match plan_arc.read().await.plan_archive(plan_id).await {
            Ok(p) => serde_json::json!({"ok": true, "plan": Self::plan_to_json(&p)}),
            Err(e) => serde_json::json!({"ok": false, "code": "ARCHIVE_FAILED", "error": e.to_string()}),
        }
    }
}
