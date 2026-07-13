//! MCP Tools definitions

use rust_mcp_sdk::schema::{Tool, ToolInputSchema, JsonSchema};

/// Список всех MCP инструментов GraphMind v2
pub fn list_mcp_tools() -> Vec<Tool> {
    vec![
        // Session Tools (S0)
        Tool {
            name: "record_action".to_string(),
            description: "Записать действие в кратковременную память сессии (S0). \
                          Запись попадает в очередь pending_actions.json и в S0-буфер. \
                          ВНИМАНИЕ: record_action НЕ создаёт L2-атомы — это S0-only паттерн. \
                          Для долгосрочной L2-персистенции используй propose_new_memory(\
                          level=\"L2\", content=...). \
                          Чтобы данные пережили рестарт MCP-сервера — вызови flush_session_memory(force=true) \
                          перед рестартом. См. bug_report/002.".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "summary": {"type": "string", "description": "Краткое описание (до 200 символов)"},
                    "raw_text": {"type": "string", "description": "Длинный текст / логи / дифф"},
                    "related_nodes": {"type": "array", "items": {"type": "string"}, "description": "ID связанных L2-узлов"},
                }),
                required: vec!["summary".to_string()],
            },
        },
        Tool {
            name: "get_s0_context".to_string(),
            description: "Получить последние действия S0".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "limit": {"type": "number", "description": "Максимум записей"}
                }),
                required: vec![],
            },
        },
        Tool {
            name: "flush_session_memory".to_string(),
            description: "Завершить сессию: flush S0 → drain pending queue → consolidate. \
                          ВНИМАНИЕ (bug 002): `drained_actions` — это счётчик ОБРАБОТАННЫХ actions \
                          (любого типа), а не созданных L2-атомов. \
                          `new_l2_atoms` — реально созданные L2-атомы (только ProposeNewMemory, \
                          RecordAction остаётся S0-only by design). \
                          Если ты вызывал record_action, а не propose_new_memory — после flush \
                          `new_l2_atoms` будет 0; для L2-персистенции нужен propose_new_memory. \
                          Возвращаемые поля: flushed_s0_count, enqueued, enqueue_failed, \
                          drained_actions, new_l2_atoms, new_l2_count (legacy alias).".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "summary": {"type": "string"},
                    "related_nodes": {"type": "array", "items": {"type": "string"}},
                    "force": {"type": "boolean", "description": "При true: автозапуск causal_reflection + suggest_chain_reorg"}
                }),
                required: vec!["summary".to_string()],
            },
        },
        // Storage Tools
        Tool {
            name: "propose_new_memory".to_string(),
            description: "Создать новый узел памяти".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "level": {"type": "string", "enum": ["L2", "L1", "L0", "GKL"]},
                    "node_type": {"type": "string", "enum": ["atom", "cause", "effect", "rule", "cluster", "hub", "domain"]},
                    "content": {"type": "string"},
                    "parent_id": {"type": "string"},
                    "scope": {"type": "string", "enum": ["workspace", "global"]},
                }),
                required: vec!["level".to_string(), "node_type".to_string(), "content".to_string()],
            },
        },
        Tool {
            name: "update_node".to_string(),
            description: "Обновить существующий узел".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "node_id": {"type": "string"},
                    "scope": {"type": "string", "enum": ["workspace", "global"]},
                    "content": {"type": "string"},
                }),
                required: vec!["node_id".to_string()],
            },
        },
        Tool {
            name: "fetch_l2_atoms".to_string(),
            description: "Получить полный текст узлов".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "atom_ids": {"type": "array", "items": {"type": "string"}, "description": "До 50 ID за раз"},
                    "scope": {"type": "string", "enum": ["workspace", "global"]},
                }),
                required: vec!["atom_ids".to_string()],
            },
        },
        // Causal Tools
        Tool {
            name: "get_chain".to_string(),
            description: "Обход причинных цепочек".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "anchor": {"type": "object"},
                    "direction": {"type": "string", "enum": ["backward", "forward_pre", "forward_post"]},
                    "max_depth": {"type": "number"},
                    "scope": {"type": "string", "enum": ["workspace", "global"]},
                }),
                required: vec!["anchor".to_string(), "direction".to_string()],
            },
        },
        // Vector Search
        Tool {
            name: "memory_query".to_string(),
            description: "Семантический поиск (vector + keyword)".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "query": {"type": "string"},
                    "depth": {"type": "number", "enum": [0, 1, 2]},
                    "scope": {"type": "string", "enum": ["workspace", "global"]},
                }),
                required: vec!["query".to_string()],
            },
        },
        Tool {
            name: "search_nodes".to_string(),
            description: "Keyword-поиск по L2 узлам (без embedding, работает с SearchActor)".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "query": {"type": "string"},
                    "limit": {"type": "number", "description": "default 10"},
                    "level": {"type": "string", "enum": ["L0","L1","L2","GKL","S0"]},
                    "workspace_id": {"type": "string"},
                }),
                required: vec!["query".to_string()],
            },
        },
        Tool {
            name: "vector_search".to_string(),
            description: "Vector поиск (V2.0: char-bag fallback; V2.1: EmbeddingProvider)".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "text": {"type": "string"},
                    "vector": {"type": "array", "items": {"type": "number"}},
                    "top_k": {"type": "number", "description": "default 10"},
                    "min_score": {"type": "number"},
                    "workspace_id": {"type": "string"},
                }),
                required: vec![],
            },
        },
        Tool {
            name: "suggest_related".to_string(),
            description: "BFS по edges_from для поиска связанных узлов (1-hop+multi-hop)".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "node_id": {"type": "string"},
                    "max_depth": {"type": "number", "description": "default 2"},
                    "top_k": {"type": "number", "description": "default 10"},
                }),
                required: vec!["node_id".to_string()],
            },
        },
        // 06-cross-workspace (Block 3)
        Tool {
            name: "fetch_from_workspace".to_string(),
            description: "Получить L2-узлы из конкретного workspace с keyword-фильтром".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "workspace_id": {"type": "string"},
                    "query": {"type": "string"},
                    "limit": {"type": "number", "description": "default 20"},
                }),
                required: vec!["workspace_id".to_string()],
            },
        },
        Tool {
            name: "find_workspace_overlaps".to_string(),
            description: "Найти workspace'ы с похожим набором L2-узлов (Jaccard similarity)".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "source_workspace": {"type": "string"},
                    "min_similarity": {"type": "number", "description": "0.0..1.0, default 0.3"},
                    "limit": {"type": "number", "description": "default 10"},
                }),
                required: vec!["source_workspace".to_string()],
            },
        },
        Tool {
            name: "suggest_cross_workspace_links".to_string(),
            description: "Найти workspace'ы, разделяющие теги с указанным (V2.0: tag-based; V2.1: GKL integration)".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "workspace_id": {"type": "string"},
                    "limit": {"type": "number", "description": "default 10"},
                }),
                required: vec!["workspace_id".to_string()],
            },
        },
        // Workspace Tools
        Tool {
            name: "detect_workspace_from_context".to_string(),
            description: "Определить workspace по cwd".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "cwd": {"type": "string"},
                }),
                required: vec!["cwd".to_string()],
            },
        },
        Tool {
            name: "create_workspace".to_string(),
            description: "Создать новый workspace".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "name": {"type": "string"},
                    "path": {"type": "string"},
                }),
                required: vec!["name".to_string(), "path".to_string()],
            },
        },
        Tool {
            name: "list_workspaces".to_string(),
            description: "Список всех workspace'ов".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({}),
                required: vec![],
            },
        },
        Tool {
            name: "switch_workspace".to_string(),
            description: "Переключить активный workspace".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "workspace_id": {"type": "string"},
                }),
                required: vec!["workspace_id".to_string()],
            },
        },
        Tool {
            name: "archive_workspace".to_string(),
            description: "Архивировать workspace".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "workspace_id": {"type": "string"},
                }),
                required: vec!["workspace_id".to_string()],
            },
        },
        // Storage Extensions (02-storage)
        Tool {
            name: "link_nodes".to_string(),
            description: "Связать два узла ребром (relation + confidence)".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "from_id": {"type": "string"},
                    "to_id": {"type": "string"},
                    "relation": {"type": "string", "enum": ["RelatedTo","LeadsTo","ExplainedBy","DerivedFrom","DependsOn","Inhibits","Contradicts","Implements","Supersedes"]},
                    "confidence": {"type": "number", "description": "0.0..1.0, default 1.0"},
                }),
                required: vec!["from_id".to_string(), "to_id".to_string()],
            },
        },
        Tool {
            name: "archive_node".to_string(),
            description: "Soft delete узла (status=Archived)".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "node_id": {"type": "string"},
                }),
                required: vec!["node_id".to_string()],
            },
        },
        Tool {
            name: "restore_node".to_string(),
            description: "Восстановить узел из Archived в Active".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "node_id": {"type": "string"},
                }),
                required: vec!["node_id".to_string()],
            },
        },
        // Admin Tools
        Tool {
            name: "bootstrap_memory".to_string(),
            description: "Автоподстройка памяти: bootstrap_memory".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "cwd": {"type": "string"},
                    "query": {"type": "string"},
                    "depth": {"type": "number", "enum": [0, 1, 2]},
                }),
                required: vec!["cwd".to_string(), "query".to_string()],
            },
        },
        // 07-memory-lifecycle (Block 4)
        Tool {
            name: "consolidate_workspace".to_string(),
            description: "Полная консолидация workspace: drain queue → L2 → L1 autogen → L0 autogen".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "workspace_id": {"type": "string"},
                }),
                required: vec!["workspace_id".to_string()],
            },
        },
        // 07-memory-lifecycle skeleton (Block 6) — V2.0: trait + dummy; full in V2.1
        Tool {
            name: "route_l1".to_string(),
            description: "V2.0 SKELETON: прокинуть L2-атом в L1-домен".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "workspace_id": {"type": "string"},
                    "l2_atom_id": {"type": "string"},
                }),
                required: vec!["workspace_id".to_string(), "l2_atom_id".to_string()],
            },
        },
        Tool {
            name: "search_l0_clusters".to_string(),
            description: "Поиск по L0-кластерам workspace: keyword-фильтрация по content + tags".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "workspace_id": {"type": "string"},
                    "query": {"type": "string"},
                    "limit": {"type": "number"},
                }),
                required: vec!["workspace_id".to_string(), "query".to_string()],
            },
        },
        // 12-plan (Block 5) — 12 plan_* tools
        Tool {
            name: "plan_create_p0".to_string(),
            description: "P0: создать эпик".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "description": {"type": "string"},
                    "autonomous_mode": {"type": "boolean"},
                }),
                required: vec!["description".to_string()],
            },
        },
        Tool {
            name: "plan_propose_p1".to_string(),
            description: "P1: предложить story под P0".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "p0_id": {"type": "string"},
                    "description": {"type": "string"},
                }),
                required: vec!["p0_id".to_string(), "description".to_string()],
            },
        },
        Tool {
            name: "plan_approve_p1".to_string(),
            description: "P1: approve".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({"p1_id": {"type": "string"}}),
                required: vec!["p1_id".to_string()],
            },
        },
        Tool {
            name: "plan_reject_p1".to_string(),
            description: "P1: reject с причиной".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "p1_id": {"type": "string"},
                    "reason": {"type": "string"},
                }),
                required: vec!["p1_id".to_string(), "reason".to_string()],
            },
        },
        Tool {
            name: "plan_decompose".to_string(),
            description: "P0→P1, P1→P2, P2→P3 (V2.0: LLM через OrchestratorActor)".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({"node_id": {"type": "string"}}),
                required: vec!["node_id".to_string()],
            },
        },
        Tool {
            name: "plan_claim".to_string(),
            description: "P3: claim by agent".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "p3_id": {"type": "string"},
                    "agent_id": {"type": "string"},
                }),
                required: vec!["p3_id".to_string(), "agent_id".to_string()],
            },
        },
        Tool {
            name: "plan_complete".to_string(),
            description: "P3: complete с result".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "p3_id": {"type": "string"},
                    "result": {"type": "string"},
                }),
                required: vec!["p3_id".to_string(), "result".to_string()],
            },
        },
        Tool {
            name: "plan_set_problem".to_string(),
            description: "Установить problem_comment на plan".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "plan_id": {"type": "string"},
                    "problem_comment": {"type": "string"},
                }),
                required: vec!["plan_id".to_string(), "problem_comment".to_string()],
            },
        },
        Tool {
            name: "plan_resolve_problem".to_string(),
            description: "Resolve problem".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "plan_id": {"type": "string"},
                    "resolution": {"type": "string"},
                }),
                required: vec!["plan_id".to_string(), "resolution".to_string()],
            },
        },
        Tool {
            name: "plan_status".to_string(),
            description: "List all plans (optionally filtered by status)".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "filter": {"type": "string", "enum": ["created", "in_progress", "pending_review", "approved", "rejected", "problem", "done", "archived"]},
                }),
                required: vec![],
            },
        },
        Tool {
            name: "plan_delete".to_string(),
            description: "Hard delete (force=true) or soft delete (force=false)".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({
                    "plan_id": {"type": "string"},
                    "force": {"type": "boolean"},
                }),
                required: vec!["plan_id".to_string()],
            },
        },
        Tool {
            name: "plan_archive".to_string(),
            description: "Soft delete (status → Archived)".to_string(),
            input_schema: ToolInputSchema {
                r#type: "object".to_string(),
                properties: serde_json::json!({"plan_id": {"type": "string"}}),
                required: vec!["plan_id".to_string()],
            },
        },
    ]
}
