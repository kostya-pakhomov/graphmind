//! Регресс-тесты по хвосту bug 004 и по bug 005 R3.
//!
//! Контекст: багрепорты 004–009 были устранены в коммите `58ad36d9`, но
//! (а) для них не добавили автотестов, (б) в 004 остался незакрытый хвост —
//! `edge_count` в `list_workspaces` отдавался из кэша WorkspaceManager (≈0),
//! а не пересчитывался из хранилища как `node_count`. Этот файл закрывает хвост
//! (метод `L2Actor::count_edges_by_workspace` + проводка в `list_workspaces`)
//! и фиксирует контракт `list_edges → unlink_edge` (bug 005 R3), чтобы кнопка
//! «удалить ребро» не стала снова мёртвой.
//!
//! Баги 008 (backward-цепочка по LeadsTo) и 009 (фильтр уровня в поиске) требуют
//! ChainActor/SearchActor в обвязке и покрыты основным сьютом (158 тестов) плюс
//! смоуком живого сервера; здесь не дублируются.

use std::sync::Arc;
use graphmind_v2::actors::L2Actor;
use graphmind_v2::actors::{S0Actor, WorkspaceManager};
use graphmind_v2::graph::{Edge, Node, NodeType, Relation};
use graphmind_v2::mcp_server::McpHandler;
use graphmind_v2::persistence::InMemoryBackend;
use serde_json::json;
use tokio::sync::RwLock;

/// McpHandler с реальным WorkspaceManager поверх InMemoryBackend (как в bug_001-тесте).
async fn setup() -> (McpHandler, Arc<WorkspaceManager>, Arc<RwLock<L2Actor>>) {
    let s0 = Arc::new(S0Actor::new());
    let backend: Arc<dyn graphmind_v2::persistence::StorageBackend> =
        Arc::new(InMemoryBackend::new());
    let l2_actor = Arc::new(RwLock::new(L2Actor::new(backend)));
    let ws_backend: Arc<dyn graphmind_v2::persistence::StorageBackend> =
        Arc::new(InMemoryBackend::new());
    let ws_mgr = Arc::new(WorkspaceManager::new(ws_backend));
    let handler = McpHandler::new(s0, l2_actor.clone()).with_workspace_manager(ws_mgr.clone());
    (handler, ws_mgr, l2_actor)
}

fn node_id_of(resp: &serde_json::Value) -> String {
    resp["node_id"].as_str().expect("node_id in propose response").to_string()
}

/// Bug 004 (хвост): `list_workspaces` пересчитывает edge_count из хранилища,
/// а не из инкрементального кэша WorkspaceManager (застревавшего на ≈0).
#[tokio::test]
async fn test_bug_004_tail_edge_count_recomputed_from_store() {
    let (handler, ws_mgr, _l2) = setup().await;
    let ws = ws_mgr
        .create_workspace("edgecount".into(), Some("/tmp/edgecount".into()))
        .await
        .unwrap();
    let ws_id = ws.id.clone();

    // 2 узла в активном workspace
    let a = node_id_of(
        &handler
            .handle_tool("propose_new_memory", json!({
                "level": "L2", "node_type": "atom", "content": "A", "scope": "workspace"}))
            .await,
    );
    let b = node_id_of(
        &handler
            .handle_tool("propose_new_memory", json!({
                "level": "L2", "node_type": "atom", "content": "B", "scope": "workspace"}))
            .await,
    );

    // 1 ребро A->B
    let rl = handler
        .handle_tool("link_nodes", json!({"from_id": a, "to_id": b, "relation": "LeadsTo"}))
        .await;
    assert_eq!(rl["ok"], json!(true), "link_nodes failed: {}", rl);

    // list_workspaces → наш ws: node_count=2, edge_count=1 (из хранилища)
    let rw = handler.handle_tool("list_workspaces", json!({})).await;
    assert_eq!(rw["ok"], json!(true));
    let wss = rw["workspaces"].as_array().expect("workspaces array");
    let ours = wss
        .iter()
        .find(|w| w["id"] == json!(ws_id))
        .unwrap_or_else(|| panic!("our workspace {} not in {}", ws_id, rw));
    assert_eq!(ours["node_count"].as_u64().unwrap(), 2, "node_count из хранилища");
    assert_eq!(
        ours["edge_count"].as_u64().unwrap(),
        1,
        "bug 004 (хвост): edge_count должен пересчитываться из хранилища (был кэш ≈0): {}",
        ours
    );
}

/// Юнит: `count_edges_by_workspace` считает рёбра по принадлежности source-узла
/// (симметрично `count_by_workspace` по узлам). Документирует by-source семантику.
#[tokio::test]
async fn test_count_edges_by_workspace_counts_by_source() {
    let backend: Arc<dyn graphmind_v2::persistence::StorageBackend> =
        Arc::new(InMemoryBackend::new());
    let l2 = L2Actor::new(backend);

    // n1,n2 в ws-a; n3 в ws-b
    let mut n1 = Node::new(NodeType::Atom, "n1");
    n1.metadata.workspace_id = Some("ws-a".into());
    let mut n2 = Node::new(NodeType::Atom, "n2");
    n2.metadata.workspace_id = Some("ws-a".into());
    let mut n3 = Node::new(NodeType::Atom, "n3");
    n3.metadata.workspace_id = Some("ws-b".into());
    l2.add_node(&n1).await.unwrap();
    l2.add_node(&n2).await.unwrap();
    l2.add_node(&n3).await.unwrap();

    // n1->n2 (source в ws-a), n1->n3 (source в ws-a), n3->n2 (source в ws-b)
    l2.add_edge(&Edge::new(n1.id.clone(), n2.id.clone(), Relation::LeadsTo)).await.unwrap();
    l2.add_edge(&Edge::new(n1.id.clone(), n3.id.clone(), Relation::LeadsTo)).await.unwrap();
    l2.add_edge(&Edge::new(n3.id.clone(), n2.id.clone(), Relation::LeadsTo)).await.unwrap();

    assert_eq!(l2.count_edges_by_workspace("ws-a").await.unwrap(), 2, "рёбра с source в ws-a");
    assert_eq!(l2.count_edges_by_workspace("ws-b").await.unwrap(), 1, "рёбра с source в ws-b");
    assert_eq!(l2.count_edges_by_workspace("ws-none").await.unwrap(), 0, "пустой workspace → 0");
}

/// Bug 005 R3: `list_edges` отдаёт `edge_id`, которым `unlink_edge` затем удаляет
/// ребро. До фикса edge_id было негде взять → «удалить ребро» была мёртвой кнопкой.
#[tokio::test]
async fn test_bug_005_r3_list_edges_then_unlink() {
    let (handler, ws_mgr, _l2) = setup().await;
    let _ws = ws_mgr.create_workspace("edges".into(), None).await.unwrap();
    let a = node_id_of(
        &handler
            .handle_tool("propose_new_memory", json!({
                "level": "L2", "node_type": "atom", "content": "A", "scope": "workspace"}))
            .await,
    );
    let b = node_id_of(
        &handler
            .handle_tool("propose_new_memory", json!({
                "level": "L2", "node_type": "atom", "content": "B", "scope": "workspace"}))
            .await,
    );
    assert_eq!(
        handler
            .handle_tool("link_nodes", json!({"from_id": a, "to_id": b, "relation": "LeadsTo"}))
            .await["ok"],
        json!(true)
    );

    // list_edges → 1 ребро с edge_id
    let rle = handler.handle_tool("list_edges", json!({"from_id": a})).await;
    assert_eq!(rle["ok"], json!(true), "list_edges failed: {}", rle);
    let edges = rle["edges"].as_array().expect("edges array");
    assert_eq!(edges.len(), 1, "одно ребро A->B: {}", rle);
    let edge_id = edges[0]["edge_id"].as_str().expect("edge_id").to_string();

    // unlink_edge по edge_id
    let ru = handler.handle_tool("unlink_edge", json!({"edge_id": edge_id})).await;
    assert_eq!(ru["ok"], json!(true), "unlink_edge failed: {}", ru);

    // теперь рёбер нет
    let rle2 = handler.handle_tool("list_edges", json!({"from_id": a})).await;
    assert_eq!(rle2["edges"].as_array().unwrap().len(), 0, "ребро удалено: {}", rle2);
}
