//! L2Actor -- persistent long-term memory for atoms, causes, effects, rules.
//!
//! Based on TECH-SPEC.md Section 4.2 L2: durable store, key layout:
//!   "node:{node_id}"  -> JSON-serialized Node
//!   "edge:{edge_id}"  -> JSON-serialized Edge
//!   "nodeidx:by_parent:{workspace_id}:{node_id}" -> node_id  (for list_by_workspace; имя индекса историческое)
//!   "nodeidx:by_type:{node_type}:{node_id}" -> node_id (for list_by_type)

use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::graph::{Edge, EdgeId, Level, Node, NodeId, NodeType, Status};
use crate::persistence::StorageBackend;
use super::Actor;

const NODE_PREFIX: &str = "node:";
const EDGE_PREFIX: &str = "edge:";
const NODE_BY_PARENT_PREFIX: &str = "nodeidx:by_parent:";
const NODE_BY_TYPE_PREFIX: &str = "nodeidx:by_type:";

fn node_key(id: &NodeId) -> String {
    format!("{NODE_PREFIX}{}", id.0)
}

fn edge_key(id: &EdgeId) -> String {
    format!("{EDGE_PREFIX}{}", id.0)
}

fn node_by_parent_key(parent: &str, id: &NodeId) -> String {
    format!("{NODE_BY_PARENT_PREFIX}{parent}:{}", id.0)
}

fn node_by_type_key(node_type: NodeType, id: &NodeId) -> String {
    format!("{NODE_BY_TYPE_PREFIX}{}:{}", node_type_name(node_type), id.0)
}

fn node_type_name(t: NodeType) -> &'static str {
    match t {
        NodeType::Atom => "atom",
        NodeType::Cause => "cause",
        NodeType::Effect => "effect",
        NodeType::Rule => "rule",
        NodeType::Cluster => "cluster",
        NodeType::Hub => "hub",
        NodeType::Domain => "domain",
    }
}

/// What we serialize into the backend. Trims Node to the durable fields.
/// `status` is stored so `archive_node` / `restore_node` can survive restarts.
/// `parent_id` is the cluster hierarchy parent (L0 → L1 → L2);
/// `workspace_id` is the storage partition. See bug_report/001.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredNode {
    id: NodeId,
    node_type: NodeType,
    /// level кластера (L0/L1/L2/GKL). Без этого поля L0/L1 десериализовались
    /// как L2 — см. bug 001 (StoredNode в node.rs имеет level, а в l2.rs — нет).
    #[serde(default)]
    level: Level,
    content: String,
    parent_id: Option<String>,
    workspace_id: Option<String>,
    tags: Vec<String>,
    /// Bug 006: #[serde(default)] — L0/L1 узлы (StoredL0Node/StoredL1Node) не
    /// имеют поля status, но делят с L2 общий ключ node:{id} и индекс
    /// nodeidx:by_parent:. Без default десериализация падает с
    /// «missing field `status`» при консолидации.
    #[serde(default)]
    status: Status,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl From<&Node> for StoredNode {
    fn from(n: &Node) -> Self {
        Self {
            id: n.id.clone(),
            node_type: n.node_type,
            level: n.level,
            content: n.content.clone(),
            parent_id: n.metadata.parent_id.clone(),
            workspace_id: n.metadata.workspace_id.clone(),
            tags: n.metadata.tags.clone(),
            status: n.status,
            created_at: n.created_at,
        }
    }
}

/// L2Actor -- durable CRUD over a `StorageBackend`.
pub struct L2Actor {
    backend: Arc<dyn StorageBackend>,
    /// Edge index: source_id -> Vec<edge_id>. In-memory only;
    /// edges themselves stay in the backend. Acceptable for now:
    /// a rebuild on startup is cheap (scan EDGE_PREFIX).
    edge_index: RwLock<std::collections::HashMap<NodeId, Vec<EdgeId>>>,
}

impl L2Actor {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            backend,
            edge_index: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Access the underlying backend (useful for tests and direct ops).
    pub fn backend(&self) -> &Arc<dyn StorageBackend> {
        &self.backend
    }

    /// Store a node. Returns the assigned id (= `node.id`).
    pub async fn add_node(&self, node: &Node) -> anyhow::Result<NodeId> {
        let stored = StoredNode::from(node);
        let bytes = serde_json::to_vec(&stored)?;
        self.backend.put(&node_key(&node.id), bytes).await?;

        if let Some(parent) = &stored.workspace_id {
            self.backend
                .put(&node_by_parent_key(parent, &node.id), node.id.0.as_bytes().to_vec())
                .await?;
        }
        self.backend
            .put(
                &node_by_type_key(node.node_type, &node.id),
                node.id.0.as_bytes().to_vec(),
            )
            .await?;
        Ok(node.id.clone())
    }

    /// Fetch a node by id, returning `None` if absent.
    pub async fn get_node(&self, id: &NodeId) -> anyhow::Result<Option<Node>> {
        let Some(bytes) = self.backend.get(&node_key(id)).await? else {
            return Ok(None);
        };
        let stored: StoredNode = serde_json::from_slice(&bytes)?;
        Ok(Some(stored.into()))
    }

    /// List all nodes whose `workspace_id` matches the given workspace.
    ///
    /// Это ПЕРЕИМЕНОВАННЫЙ `list_by_parent` (см. bug_report/001, релиз с этим
    /// фиксом). Контракт не поменялся: индекс `nodeidx:by_parent:{ws}:{id}`
    /// уже хранит `workspace_id`, а не cluster parent. Новое имя отражает
    /// фактическую семантику и отделяет «storage partition» от «cluster hierarchy».
    /// Старый `list_by_parent` оставлен как deprecated alias на 1 релиз.
    pub async fn list_by_workspace(&self, workspace_id: &str) -> anyhow::Result<Vec<Node>> {
        let prefix = format!("{NODE_BY_PARENT_PREFIX}{workspace_id}:");
        let keys = self.backend.list_keys(&prefix).await?;
        let mut nodes = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(id_str) = k.strip_prefix(&prefix) {
                let id = NodeId(id_str.to_string());
                if let Some(n) = self.get_node(&id).await? {
                    nodes.push(n);
                }
            }
        }
        Ok(nodes)
    }

    /// Считает узлы workspace без загрузки полных Node — только по индексу.
    /// Надёжнее инкрементального счётчика WorkspaceManager, который рассинхронизируется
    /// при создании узлов через process_action (путь flush). См. bug 005.
    pub async fn count_by_workspace(&self, workspace_id: &str) -> anyhow::Result<usize> {
        let prefix = format!("{NODE_BY_PARENT_PREFIX}{workspace_id}:");
        Ok(self.backend.list_keys(&prefix).await?.len())
    }

    /// Считает рёбра workspace: те, чей source-узел принадлежит workspace.
    /// Bug 004 (хвост): `edge_count` в `list_workspaces` отдавался из кэша WorkspaceManager
    /// (застревал на ≈0) и не пересчитывался из хранилища, в отличие от `node_count`.
    /// Симметрично `count_by_workspace` — источник правды хранилище, не инкремент-счётчик.
    pub async fn count_edges_by_workspace(&self, workspace_id: &str) -> anyhow::Result<usize> {
        let prefix = format!("{NODE_BY_PARENT_PREFIX}{workspace_id}:");
        let node_ids: std::collections::HashSet<NodeId> = self
            .backend
            .list_keys(&prefix)
            .await?
            .into_iter()
            .filter_map(|k| k.strip_prefix(&prefix).map(|s| NodeId(s.to_string())))
            .collect();
        if node_ids.is_empty() {
            return Ok(0);
        }
        let edges = self.list_all_edges().await?;
        Ok(edges
            .into_iter()
            .filter(|e| node_ids.contains(&e.source))
            .count())
    }

    /// DEPRECATED: используй `list_by_workspace`. Имя метода путало cluster
    /// parent (L0→L1→L2) с storage partition (workspace). Оставлен на 1 релиз
    /// как backward-compat alias (см. code-quality.md §2). Удалить в v0.3.0.
    #[deprecated(note = "используй list_by_workspace — это storage partition, не cluster parent")]
    #[allow(dead_code)] // публичный API: оставлен на 1 релиз как alias, см. bug_report/001
    pub async fn list_by_parent(&self, workspace_id: &str) -> anyhow::Result<Vec<Node>> {
        self.list_by_workspace(workspace_id).await
    }    /// List all nodes of a given type.
    pub async fn list_by_type(&self, t: NodeType) -> anyhow::Result<Vec<Node>> {
        let prefix = format!("{NODE_BY_TYPE_PREFIX}{}:", node_type_name(t));
        let keys = self.backend.list_keys(&prefix).await?;
        let mut nodes = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(id_str) = k.strip_prefix(&prefix) {
                let id = NodeId(id_str.to_string());
                if let Some(n) = self.get_node(&id).await? {
                    nodes.push(n);
                }
            }
        }
        Ok(nodes)
    }

    /// Store an edge and update the in-memory edge index.
    pub async fn add_edge(&self, edge: &Edge) -> anyhow::Result<EdgeId> {
        let bytes = serde_json::to_vec(edge)?;
        self.backend.put(&edge_key(&edge.id), bytes).await?;
        self.edge_index
            .write()
            .await
            .entry(edge.source.clone())
            .or_default()
            .push(edge.id.clone());
        Ok(edge.id.clone())
    }

    /// Fetch an edge by id.
    pub async fn get_edge(&self, id: &EdgeId) -> anyhow::Result<Option<Edge>> {
        let Some(bytes) = self.backend.get(&edge_key(id)).await? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    /// List all edges whose `source` matches the given node id.
    ///
    /// Читаем из backend, а не только из per-instance `edge_index`: смежность должна быть
    /// консистентна между разными L2Actor над одним backend. Иначе обход причинных цепочек
    /// (ChainActor/InferenceActor держат СВОЙ L2Actor) не видит рёбра, добавленные через
    /// L2Actor хендлера — predict_risks/get_chain возвращали пусто на живых данных.
    pub async fn edges_from(&self, source: &NodeId) -> anyhow::Result<Vec<Edge>> {
        let keys = self.backend.list_keys(EDGE_PREFIX).await?;
        let mut edges = Vec::new();
        for key in keys {
            if let Some(bytes) = self.backend.get(&key).await? {
                if let Ok(e) = serde_json::from_slice::<Edge>(&bytes) {
                    if &e.source == source {
                        edges.push(e);
                    }
                }
            }
        }
        Ok(edges)
    }

    /// Total nodes + edges stored.
    pub async fn node_count(&self) -> anyhow::Result<usize> {
        Ok(self.backend.list_keys(NODE_PREFIX).await?.len())
    }

    pub async fn edge_count(&self) -> anyhow::Result<usize> {
        Ok(self.backend.list_keys(EDGE_PREFIX).await?.len())
    }

    /// Все узлы (скан `NODE_PREFIX`). Нужно потребителям слоя познания
    /// (CuriosityEngine/TrustFirewall), которым надо обойти весь граф, а не только
    /// по типу/родителю. Читаем из backend → консистентно между L2Actor'ами (см. `edges_from`).
    pub async fn list_all_nodes(&self) -> anyhow::Result<Vec<Node>> {
        let keys = self.backend.list_keys(NODE_PREFIX).await?;
        let mut nodes = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(bytes) = self.backend.get(&key).await? {
                if let Ok(stored) = serde_json::from_slice::<StoredNode>(&bytes) {
                    nodes.push(stored.into());
                }
            }
        }
        Ok(nodes)
    }

    /// Все рёбра (скан `EDGE_PREFIX`). Для in-edges и полного обхода графа когниции.
    pub async fn list_all_edges(&self) -> anyhow::Result<Vec<Edge>> {
        let keys = self.backend.list_keys(EDGE_PREFIX).await?;
        let mut edges = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(bytes) = self.backend.get(&key).await? {
                if let Ok(e) = serde_json::from_slice::<Edge>(&bytes) {
                    edges.push(e);
                }
            }
        }
        Ok(edges)
    }

    /// Найти рёбра по фильтру (from_id, to_id, relation). Любой параметр опционален.
    /// Bug 005 регрессия 3: нужен способ получить edge_id без сохранения с момента
    /// link_nodes — иначе unlink_edge неработоспособен (мёртвая кнопка).
    pub async fn find_edges(
        &self,
        from_id: Option<&NodeId>,
        to_id: Option<&NodeId>,
    ) -> anyhow::Result<Vec<Edge>> {
        let mut edges = self.list_all_edges().await?;
        if let Some(from) = from_id {
            edges.retain(|e| &e.source == from);
        }
        if let Some(to) = to_id {
            edges.retain(|e| &e.target == to);
        }
        Ok(edges)
    }

    /// Входящие рёбра узла (source→target, где target == `target`). Для CauseMissing
    /// (у следствия нет причины) в CuriosityEngine — L2 сам отдаёт только out-edges.
    pub async fn edges_to(&self, target: &NodeId) -> anyhow::Result<Vec<Edge>> {
        Ok(self
            .list_all_edges()
            .await?
            .into_iter()
            .filter(|e| &e.target == target)
            .collect())
    }

    /// Hard-delete a node and its index entries. Returns true if the node existed.
    pub async fn delete_node(&self, id: &NodeId) -> anyhow::Result<bool> {
        let Some(node) = self.get_node(id).await? else {
            return Ok(false);
        };
        self.backend.delete(&node_key(id)).await?;
        if let Some(parent) = &node.metadata.workspace_id {
            let _ = self.backend.delete(&node_by_parent_key(parent, id)).await;
        }
        let _ = self
            .backend
            .delete(&node_by_type_key(node.node_type, id))
            .await;
        // Also drop all edges incident to this node from the index.
        // (Edges themselves stay in backend; will be removed by caller via unlink_edge.)
        self.edge_index.write().await.remove(id);
        Ok(true)
    }

    /// Hard-delete an edge. Returns true if the edge existed.
    pub async fn delete_edge(&self, id: &EdgeId) -> anyhow::Result<bool> {
        let Some(edge) = self.get_edge(id).await? else {
            return Ok(false);
        };
        self.backend.delete(&edge_key(id)).await?;
        // Remove from in-memory index.
        let mut index = self.edge_index.write().await;
        if let Some(list) = index.get_mut(&edge.source) {
            list.retain(|eid| eid != id);
        }
        Ok(true)
    }

    /// Mark a node as Archived (soft delete). Returns the new status, or None if not found.
    pub async fn archive_node(&self, id: &NodeId) -> anyhow::Result<Option<Status>> {
        let Some(mut node) = self.get_node(id).await? else {
            return Ok(None);
        };
        node.status = Status::Archived;
        node.updated_at = chrono::Utc::now();
        self.add_node(&node).await?;
        Ok(Some(Status::Archived))
    }

    /// Restore an Archived node to Active. Returns the new status, or None if not found.
    pub async fn restore_node(&self, id: &NodeId) -> anyhow::Result<Option<Status>> {
        let Some(mut node) = self.get_node(id).await? else {
            return Ok(None);
        };
        node.status = Status::Active;
        node.updated_at = chrono::Utc::now();
        self.add_node(&node).await?;
        Ok(Some(Status::Active))
    }
}
impl From<StoredNode> for Node {
    fn from(s: StoredNode) -> Self {
        Node {
            id: s.id,
            node_type: s.node_type,
            // level теперь хранится в StoredNode — см. From<&Node> выше.
            // Раньше здесь было жёсткое L2, что ломало L0/L1 иерархию (bug 001).
            level: s.level,
            content: s.content,
            metadata: crate::graph::Metadata {
                parent_id: s.parent_id,
                workspace_id: s.workspace_id,
                tags: s.tags,
            },
            status: s.status,
            created_at: s.created_at,
            updated_at: s.created_at,
        }
}
}
#[async_trait]
impl Actor for L2Actor {    fn name(&self) -> &str {
        "L2Actor"
    }

    async fn size(&self) -> usize {
        // best-effort: report total keys (nodes + edges + indexes)
        self.backend.count().await.unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Relation;
    use crate::persistence::InMemoryBackend;
    use chrono::Utc;

    fn backend() -> Arc<dyn StorageBackend> {
        Arc::new(InMemoryBackend::new())
    }

    fn make_node(content: &str, ty: NodeType, workspace: Option<&str>) -> Node {
        let mut n = Node::new(ty, content);
        n.level = Level::L2;
        n.metadata.workspace_id = workspace.map(String::from);
        n.created_at = Utc::now();
        n
    }

    /// Построить узел с parent_id + workspace_id. Используется в новых тестах
    /// после фикса bug 001 (parent_id != workspace_id).
    fn make_node_with_parent(content: &str, ty: NodeType, parent: Option<&str>, workspace: Option<&str>) -> Node {
        let mut n = Node::new(ty, content);
        n.level = Level::L2;
        n.metadata.parent_id = parent.map(String::from);
        n.metadata.workspace_id = workspace.map(String::from);
        n.created_at = Utc::now();
        n
    }
    #[tokio::test]
    async fn test_add_and_get_node() {
        let actor = L2Actor::new(backend());
        let n = make_node("hello", NodeType::Atom, None);
        let id = actor.add_node(&n).await.unwrap();

        let fetched = actor.get_node(&id).await.unwrap().unwrap();
        assert_eq!(fetched.content, "hello");
        assert_eq!(fetched.node_type, NodeType::Atom);
    }

    #[tokio::test]
    async fn test_get_missing_node_returns_none() {
        let actor = L2Actor::new(backend());
        let result = actor.get_node(&NodeId("missing".into())).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_list_by_workspace() {
        // Контракт: list_by_workspace(ws) возвращает узлы, у которых
        // workspace_id == ws. Это новое имя для list_by_parent — см. bug_report/001.
        let actor = L2Actor::new(backend());
        let n1 = make_node("a", NodeType::Atom, Some("ws1"));
        let n2 = make_node("b", NodeType::Atom, Some("ws1"));
        let n3 = make_node("c", NodeType::Atom, Some("ws2"));
        actor.add_node(&n1).await.unwrap();
        actor.add_node(&n2).await.unwrap();
        actor.add_node(&n3).await.unwrap();

        let ws1_nodes = actor.list_by_workspace("ws1").await.unwrap();
        assert_eq!(ws1_nodes.len(), 2);
        let ws2_nodes = actor.list_by_workspace("ws2").await.unwrap();
        assert_eq!(ws2_nodes.len(), 1);
        let empty = actor.list_by_workspace("nope").await.unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_node_roundtrip_preserves_parent_id() {
        // Регресс bug 001: parent_id (cluster) и workspace_id (storage) —
        // независимые поля. После add_node + get_node оба должны сохраниться.
        let actor = L2Actor::new(backend());
        let n = make_node_with_parent("atom", NodeType::Atom, Some("l0-id"), Some("ws-x"));
        let id = actor.add_node(&n).await.unwrap();

        let fetched = actor.get_node(&id).await.unwrap().unwrap();
        assert_eq!(fetched.metadata.parent_id.as_deref(), Some("l0-id"));
        assert_eq!(fetched.metadata.workspace_id.as_deref(), Some("ws-x"));
    }

    #[tokio::test]
    async fn test_list_by_workspace_does_not_match_parent_field() {
        // Регресс bug 001: list_by_workspace("ws") НЕ должен находить узлы,
        // у которых parent_id == "ws" (это cluster-родитель, не workspace).
        // Узел: parent_id = "ws-l0", workspace_id = "ws-real".
        let actor = L2Actor::new(backend());
        let n = make_node_with_parent("atom", NodeType::Atom, Some("ws-l0"), Some("ws-real"));
        actor.add_node(&n).await.unwrap();

        // Поиск по "ws-l0" (parent_id) не должен находить узел — он в "ws-real".
        let found = actor.list_by_workspace("ws-l0").await.unwrap();
        assert!(found.is_empty(), "list_by_workspace must filter by workspace_id, not parent_id");
        // А по workspace_id — должен.
        let found_ws = actor.list_by_workspace("ws-real").await.unwrap();
        assert_eq!(found_ws.len(), 1);
    }

    #[tokio::test]
    async fn test_list_by_type() {        let actor = L2Actor::new(backend());
        actor.add_node(&make_node("a", NodeType::Atom, None)).await.unwrap();
        actor.add_node(&make_node("b", NodeType::Atom, None)).await.unwrap();
        actor.add_node(&make_node("c", NodeType::Cause, None)).await.unwrap();

        let atoms = actor.list_by_type(NodeType::Atom).await.unwrap();
        assert_eq!(atoms.len(), 2);
        let causes = actor.list_by_type(NodeType::Cause).await.unwrap();
        assert_eq!(causes.len(), 1);
        let effects = actor.list_by_type(NodeType::Effect).await.unwrap();
        assert!(effects.is_empty());
    }

    #[tokio::test]
    async fn test_node_count() {
        let actor = L2Actor::new(backend());
        assert_eq!(actor.node_count().await.unwrap(), 0);
        actor.add_node(&make_node("a", NodeType::Atom, None)).await.unwrap();
        actor.add_node(&make_node("b", NodeType::Atom, None)).await.unwrap();
        assert_eq!(actor.node_count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_add_and_get_edge() {
        let actor = L2Actor::new(backend());
        let src = make_node("src", NodeType::Cause, None);
        let tgt = make_node("tgt", NodeType::Effect, None);
        actor.add_node(&src).await.unwrap();
        actor.add_node(&tgt).await.unwrap();

        let edge = Edge::new(src.id.clone(), tgt.id.clone(), Relation::LeadsTo);
        let eid = actor.add_edge(&edge).await.unwrap();

        let fetched = actor.get_edge(&eid).await.unwrap().unwrap();
        assert_eq!(fetched.source, src.id);
        assert_eq!(fetched.target, tgt.id);
        assert_eq!(fetched.relation, Relation::LeadsTo);
    }

    #[tokio::test]
    async fn test_edge_roundtrip_preserves_workspace_id() {
        // Регресс bug 001: ребро должно сохранять workspace_id при записи в
        // backend и чтении обратно. До этой правки Edge не имел workspace_id —
        // BFS в suggest_related не мог отфильтровать edges по workspace.
        let actor = L2Actor::new(backend());
        let src = make_node("src", NodeType::Cause, None);
        let tgt = make_node("tgt", NodeType::Effect, None);
        actor.add_node(&src).await.unwrap();
        actor.add_node(&tgt).await.unwrap();

        let edge = Edge::new(src.id.clone(), tgt.id.clone(), Relation::LeadsTo)
            .with_workspace("ws-test");
        let eid = actor.add_edge(&edge).await.unwrap();

        let fetched = actor.get_edge(&eid).await.unwrap().unwrap();
        assert_eq!(fetched.workspace_id.as_deref(), Some("ws-test"));

        // Без with_workspace поле остаётся None (старые рёбра в RocksDB
        // десериализуются как None — backward compatible).
        let edge_no_ws = Edge::new(src.id.clone(), tgt.id.clone(), Relation::RelatedTo);
        let eid_no_ws = actor.add_edge(&edge_no_ws).await.unwrap();
        let fetched_no_ws = actor.get_edge(&eid_no_ws).await.unwrap().unwrap();
        assert!(fetched_no_ws.workspace_id.is_none());
    }
    #[tokio::test]
    async fn test_edges_from_node() {
        let actor = L2Actor::new(backend());
        let a = make_node("a", NodeType::Cause, None);
        let b = make_node("b", NodeType::Effect, None);
        let c = make_node("c", NodeType::Effect, None);
        actor.add_node(&a).await.unwrap();
        actor.add_node(&b).await.unwrap();
        actor.add_node(&c).await.unwrap();

        actor.add_edge(&Edge::new(a.id.clone(), b.id.clone(), Relation::LeadsTo)).await.unwrap();
        actor.add_edge(&Edge::new(a.id.clone(), c.id.clone(), Relation::LeadsTo)).await.unwrap();
        actor.add_edge(&Edge::new(b.id.clone(), c.id.clone(), Relation::RelatedTo)).await.unwrap();

        let from_a = actor.edges_from(&a.id).await.unwrap();
        assert_eq!(from_a.len(), 2);

        let from_b = actor.edges_from(&b.id).await.unwrap();
        assert_eq!(from_b.len(), 1);

        let from_c = actor.edges_from(&c.id).await.unwrap();
        assert!(from_c.is_empty());
    }

    #[tokio::test]
    async fn test_edge_count() {
        let actor = L2Actor::new(backend());
        let a = make_node("a", NodeType::Atom, None);
        let b = make_node("b", NodeType::Atom, None);
        actor.add_node(&a).await.unwrap();
        actor.add_node(&b).await.unwrap();

        assert_eq!(actor.edge_count().await.unwrap(), 0);
        actor.add_edge(&Edge::new(a.id.clone(), b.id.clone(), Relation::RelatedTo)).await.unwrap();
        assert_eq!(actor.edge_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_get_missing_edge_returns_none() {
        let actor = L2Actor::new(backend());
        let result = actor.get_edge(&EdgeId("missing".into())).await.unwrap();
        assert!(result.is_none());
    }
}
