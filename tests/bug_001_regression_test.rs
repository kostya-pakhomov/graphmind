//! Integration regression test for bug_report/001:
//! `propose_new_memory` конфликт `parent_id` (cluster hierarchy L0→L1→L2)
//! и `workspace_id` (storage partition).
//!
//! Сценарий — дословно по bug_report/001 §"Тестовый сценарий для регрессии":
//!   1. switch_workspace(ws_test) → active_workspace_id = ws_test
//!   2. propose_new_memory(level=L0, content="L0-A", scope=workspace)
//!   3. propose_new_memory(level=L1, content="L1-B", parent_id=L0-A, scope=workspace)
//!   4. propose_new_memory(level=L2, content="L2-C", parent_id=L1-B, scope=workspace)
//!   5. fetch_from_workspace(ws_test) → 3 узла
//!   6. link_nodes(L0-A → L1-B) + link_nodes(L1-B → L2-C) → edges в ws_test
//!   7. suggest_related(L0-A, depth=2) → L1-B + L2-C
//!
//! Покрывает: P3-be1eead5, P3-95d80a20, P2-1fa23612.

use std::sync::Arc;
use graphmind_v2::actors::{
    Actor, L2Actor, S0Actor, WorkspaceManager,
};
use graphmind_v2::graph::{Level, Node, NodeId, NodeType};
use graphmind_v2::mcp_server::McpHandler;
use graphmind_v2::persistence::InMemoryBackend;
use serde_json::json;
use tokio::sync::RwLock;

/// Построить McpHandler с реальным WorkspaceManager поверх InMemoryBackend
/// (как в bug_report/001 — никакой реальной FS, всё в памяти).
async fn setup() -> (McpHandler, Arc<WorkspaceManager>, Arc<RwLock<L2Actor>>) {
    let s0 = Arc::new(S0Actor::new());
    let backend: Arc<dyn graphmind_v2::persistence::StorageBackend> =
        Arc::new(InMemoryBackend::new());
    let l2_actor = Arc::new(RwLock::new(L2Actor::new(backend)));
    let ws_backend: Arc<dyn graphmind_v2::persistence::StorageBackend> =
        Arc::new(InMemoryBackend::new());
    let ws_mgr = Arc::new(WorkspaceManager::new(ws_backend));
    let handler = McpHandler::new(s0, l2_actor.clone())
        .with_workspace_manager(ws_mgr.clone());
    (handler, ws_mgr, l2_actor)
}

/// Достать node_id из JSON-ответа propose_new_memory.
fn node_id_of(resp: &serde_json::Value) -> String {
    resp["node_id"].as_str().expect("node_id in propose_new_memory response").to_string()
}

#[tokio::test]
async fn test_bug_001_regression_propose_link_suggest() {
    // 1. setup + создаём workspace (auto-active) + переключаемся на него явно
    let (handler, ws_mgr, l2) = setup().await;
    let ws = ws_mgr
        .create_workspace("regression".to_string(), Some("/tmp/regression".to_string()))
        .await
        .unwrap();
    let ws_id = ws.id.clone();
    assert_eq!(ws_mgr.get_active_workspace_id().await.as_deref(), Some(ws_id.as_str()));

    // 2. L0 без parent_id
    let r_l0 = handler
        .handle_tool("propose_new_memory", json!({
            "level": "L0",
            "node_type": "cluster",
            "content": "L0-A",
            "scope": "workspace",
        }))
        .await;
    assert_eq!(r_l0["ok"], json!(true), "L0 propose failed: {}", r_l0);
    let l0_id = node_id_of(&r_l0);

    // 3. L1 с parent_id = L0
    let r_l1 = handler
        .handle_tool("propose_new_memory", json!({
            "level": "L1",
            "node_type": "cause",
            "content": "L1-B",
            "parent_id": l0_id,
            "scope": "workspace",
        }))
        .await;
    assert_eq!(r_l1["ok"], json!(true), "L1 propose failed: {}", r_l1);
    let l1_id = node_id_of(&r_l1);

    // 4. L2 с parent_id = L1
    let r_l2 = handler
        .handle_tool("propose_new_memory", json!({
            "level": "L2",
            "node_type": "atom",
            "content": "L2-C",
            "parent_id": l1_id,
            "scope": "workspace",
        }))
        .await;
    assert_eq!(r_l2["ok"], json!(true), "L2 propose failed: {}", r_l2);
    let l2_id = node_id_of(&r_l2);

    // 5. fetch_from_workspace(ws) → 3 узла. Это КЛЮЧЕВАЯ проверка bug 001:
    //    до фикса возвращал 0 (узлы лежали в псевдо-workspace'ах с id=L0/L1).
    let r_fetch = handler
        .handle_tool("fetch_from_workspace", json!({
            "workspace_id": ws_id,
            "limit": 100,
        }))
        .await;
    assert_eq!(r_fetch["ok"], json!(true));
    assert_eq!(
        r_fetch["count"].as_u64().unwrap(),
        3,
        "fetch_from_workspace should return 3 nodes, got: {}",
        r_fetch
    );

    // Прямая проверка через L2Actor: parent_id != workspace_id (это суть bug 001)
    {
        let l2_guard = l2.read().await;
        let l0 = l2_guard.get_node(&graphmind_v2::graph::NodeId::from_string(l0_id.clone()))
            .await.unwrap().unwrap();
        assert_eq!(l0.metadata.workspace_id.as_deref(), Some(ws_id.as_str()));
        assert_eq!(l0.metadata.parent_id, None, "L0 root must have parent_id=None");
        assert_eq!(l0.level, Level::L0);

        let l1 = l2_guard.get_node(&graphmind_v2::graph::NodeId::from_string(l1_id.clone()))
            .await.unwrap().unwrap();
        assert_eq!(l1.metadata.workspace_id.as_deref(), Some(ws_id.as_str()));
        assert_eq!(l1.metadata.parent_id.as_deref(), Some(l0_id.as_str()));
        assert_eq!(l1.level, Level::L1);

        let l2_node = l2_guard.get_node(&graphmind_v2::graph::NodeId::from_string(l2_id.clone()))
            .await.unwrap().unwrap();
        assert_eq!(l2_node.metadata.workspace_id.as_deref(), Some(ws_id.as_str()));
        assert_eq!(l2_node.metadata.parent_id.as_deref(), Some(l1_id.as_str()));
        assert_eq!(l2_node.level, Level::L2);
    }

    // 6. link_nodes с workspace_id (берёт активный workspace) — регресс bug 001: до
    //    фикса edges создавались с workspace_id=None, BFS в suggest_related возвращал 0.
    let r_link1 = handler
        .handle_tool("link_nodes", json!({
            "from_id": l0_id,
            "to_id": l1_id,
            "relation": "LeadsTo",
        }))
        .await;
    assert_eq!(r_link1["ok"], json!(true), "link_nodes L0->L1 failed: {}", r_link1);
    let r_link2 = handler
        .handle_tool("link_nodes", json!({
            "from_id": l1_id,
            "to_id": l2_id,
            "relation": "LeadsTo",
        }))
        .await;
    assert_eq!(r_link2["ok"], json!(true), "link_nodes L1->L2 failed: {}", r_link2);

    // Проверим, что edges действительно привязаны к workspace
    {
        let l2_guard = l2.read().await;
        let from_l0 = l2_guard.edges_from(&graphmind_v2::graph::NodeId::from_string(l0_id.clone()))
            .await.unwrap();
        assert_eq!(from_l0.len(), 1, "L0 must have 1 out-edge");
        assert_eq!(
            from_l0[0].workspace_id.as_deref(),
            Some(ws_id.as_str()),
            "edge must be tied to active workspace_id"
        );
    }

    // 7. suggest_related(L0, depth=2) → L1 + L2
    let r_suggest = handler
        .handle_tool("suggest_related", json!({
            "node_id": l0_id,
            "max_depth": 2,
            "top_k": 10,
        }))
        .await;
    assert_eq!(r_suggest["ok"], json!(true));
    let items = r_suggest["suggestions"].as_array().expect("suggestions array");
    let found_ids: std::collections::HashSet<String> = items
        .iter()
        .map(|s| s["node_id"].as_str().unwrap().to_string())
        .collect();
    assert!(
        found_ids.contains(&l1_id),
        "suggest_related must include L1, got {:?}",
        found_ids
    );
    assert!(
        found_ids.contains(&l2_id),
        "suggest_related must include L2 (depth=2), got {:?}",
        found_ids
    );
}

/// Регресс: явно переданный workspace_id_param в propose_new_memory
/// побеждает любой fallback (включая активный workspace).
#[tokio::test]
async fn test_bug_001_explicit_workspace_id_wins() {
    let (handler, ws_mgr, _l2) = setup().await;
    let _active_ws = ws_mgr
        .create_workspace("active".to_string(), None)
        .await
        .unwrap();
    let _other_ws = ws_mgr
        .create_workspace("other".to_string(), None)
        .await
        .unwrap();
    let other_id = _other_ws.id.clone();

    let r = handler
        .handle_tool("propose_new_memory", json!({
            "level": "L2",
            "node_type": "atom",
            "content": "explicit-ws",
            "workspace_id": other_id, // ← не активный, а явно запрошенный
            "scope": "workspace",
        }))
        .await;
    assert_eq!(r["ok"], json!(true));
    let id = node_id_of(&r);

    // fetch_from_workspace(active_ws) не должен найти этот узел
    let r_active = handler
        .handle_tool("fetch_from_workspace", json!({
            "workspace_id": _active_ws.id,
            "limit": 100,
        }))
        .await;
    assert_eq!(r_active["count"].as_u64().unwrap(), 0);

    // А fetch_from_workspace(other_ws) — должен
    let r_other = handler
        .handle_tool("fetch_from_workspace", json!({
            "workspace_id": other_id,
            "limit": 100,
        }))
        .await;
    assert_eq!(r_other["count"].as_u64().unwrap(), 1);
    let _ = id; // silence unused
}

/// Регресс: scope=global → workspace_id="global", а не активный workspace.
#[tokio::test]
async fn test_bug_001_global_scope_uses_global_workspace() {
    let (handler, ws_mgr, l2) = setup().await;
    let active = ws_mgr
        .create_workspace("active".to_string(), None)
        .await
        .unwrap();

    let r = handler
        .handle_tool("propose_new_memory", json!({
            "level": "L2",
            "node_type": "atom",
            "content": "global-atom",
            "scope": "global",
        }))
        .await;
    assert_eq!(r["ok"], json!(true));
    let id = node_id_of(&r);

    let l2_guard = l2.read().await;
    let node = l2_guard
        .get_node(&graphmind_v2::graph::NodeId::from_string(id))
        .await.unwrap().unwrap();
    assert_eq!(
        node.metadata.workspace_id.as_deref(),
        Some("global"),
        "scope=global must produce workspace_id='global', got {:?}",
        node.metadata.workspace_id
    );

    // И не должен попасть в активный workspace
    let _ = active;
}

/// Sanity: после исправления list_by_parent (deprecated) и list_by_workspace
/// (новое имя) возвращают одно и то же. Это покрывает кросс-совместимость
/// для внешних клиентов, использующих старое имя в течение 1 релиза.
#[tokio::test]
async fn test_bug_001_list_by_parent_alias_still_works() {
    let backend: Arc<dyn graphmind_v2::persistence::StorageBackend> =
        Arc::new(InMemoryBackend::new());
    let l2 = L2Actor::new(backend);
    let mut n = Node::new(NodeType::Atom, "x");
    n.metadata.workspace_id = Some("ws-x".to_string());
    n.metadata.parent_id = Some("some-cluster".to_string());
    l2.add_node(&n).await.unwrap();

    let via_workspace = l2.list_by_workspace("ws-x").await.unwrap();
    #[allow(deprecated)]
    let via_parent = l2.list_by_parent("ws-x").await.unwrap();
    assert_eq!(via_workspace.len(), 1);
    assert_eq!(via_parent.len(), 1);
    assert_eq!(via_workspace[0].id, via_parent[0].id);
    let _ = NodeId::from_string("dummy"); // silence unused import if any
}
