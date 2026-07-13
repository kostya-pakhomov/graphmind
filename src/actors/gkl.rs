//! GKLactor -- глобальная память (cross-workspace knowledge layer).
//!
//! Based on TECH-SPEC.md Section 4.2 GKL: кросс-проектные знания.
//! GKLactor хранит узлы с префиксом `gkl_` в ID, которые переиспользуются
//! между workspace. Примеры: gkl_L2_framework, gkl_L1_general.
//!
//! Особенности:
//!   - Узлы имеют scope: "global" (не workspace-specific)
//!   - ID начинаются с префикса "gkl_"
//!   - Поддержка иерархии: L1 (framework) → L2 (атомы)
//!   - Vector search для поиска релевантных глобальных знаний

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::graph::{Edge, EdgeId, Level, Node, NodeId, NodeType, Relation, Metadata};
use crate::persistence::StorageBackend;

use super::Actor;

const GKL_PREFIX: &str = "gkl_";
const GKL_NODE_PREFIX: &str = "node:";
const GKL_EDGE_PREFIX: &str = "edge:";
const GKL_BY_TYPE_PREFIX: &str = "nodeidx:by_type:";
const GKL_BY_LEVEL_PREFIX: &str = "nodeidx:by_level:";

fn node_key(id: &NodeId) -> String {
    format!("{GKL_NODE_PREFIX}{}", id.0)
}

fn edge_key(id: &EdgeId) -> String {
    format!("{GKL_EDGE_PREFIX}{}", id.0)
}

fn node_by_type_key(node_type: NodeType, id: &NodeId) -> String {
    format!("{GKL_BY_TYPE_PREFIX}{}:{}", node_type_name(node_type), id.0)
}

fn node_by_level_key(level: Level, id: &NodeId) -> String {
    format!("{GKL_BY_LEVEL_PREFIX}{}:{}", level_name(level), id.0)
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

fn level_name(l: Level) -> &'static str {
    match l {
        Level::S0 => "s0",
        Level::L0 => "l0",
        Level::L1 => "l1",
        Level::L2 => "l2",
        Level::GKL => "gkl",
    }
}

/// Сериализуемая версия GKL-узла
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredGklNode {
    id: NodeId,
    node_type: NodeType,
    level: Level,
    content: String,
    tags: Vec<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    /// Флаг: является ли узел хабом (агрегатором)
    is_hub: bool,
}

impl From<&GklNode> for StoredGklNode {
    fn from(node: &GklNode) -> Self {
        Self {
            id: node.node.id.clone(),
            node_type: node.node.node_type,
            level: node.node.level,
            content: node.node.content.clone(),
            tags: node.node.metadata.tags.clone(),
            created_at: node.node.created_at,
            is_hub: node.is_hub,
        }
    }
}

impl From<StoredGklNode> for GklNode {
    fn from(s: StoredGklNode) -> Self {
        GklNode {
            node: Node {
                id: s.id,
                node_type: s.node_type,
                level: s.level,
                content: s.content,
                metadata: Metadata {
                    parent_id: None, // GKL хранится вне cluster hierarchy
                    workspace_id: None, // GKL узлы не привязаны к workspace
                    tags: s.tags,
                },
                status: crate::graph::Status::Active,
                created_at: s.created_at,
                updated_at: s.created_at,
            },
            is_hub: s.is_hub,
        }
    }
}

/// GKL-узел с дополнительными флагами
#[derive(Debug, Clone)]
pub struct GklNode {
    pub node: Node,
    /// Является ли узел хабом (агрегатором других GKL-узлов)
    pub is_hub: bool,
}

/// Результат поиска в GKL
#[derive(Debug, Clone)]
pub struct GklSearchResult {
    pub nodes: Vec<GklNode>,
    pub total_count: usize,
}

/// GKLactor -- управление глобальной памятью
pub struct GKLactor {
    backend: Arc<dyn StorageBackend>,
    /// Кэш GKL-узлов в памяти
    cache: RwLock<HashMap<NodeId, GklNode>>,
    /// Индекс: parent_id -> child_ids (для иерархии)
    children_index: RwLock<HashMap<NodeId, HashSet<NodeId>>>,
}

impl GKLactor {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            backend,
            cache: RwLock::new(HashMap::new()),
            children_index: RwLock::new(HashMap::new()),
        }
    }

    /// Access the underlying backend
    pub fn backend(&self) -> &Arc<dyn StorageBackend> {
        &self.backend
    }

    /// Создать NodeId с префиксом gkl_
    pub fn make_gkl_id(suffix: &str) -> NodeId {
        NodeId::from_string(format!("{}{}", GKL_PREFIX, suffix))
    }

    /// Проверить, является ли ID GKL-узлом
    pub fn is_gkl_id(id: &NodeId) -> bool {
        id.0.starts_with(GKL_PREFIX)
    }

    /// Сохранить GKL-узел
    pub async fn save_node(&self, node: &GklNode) -> anyhow::Result<NodeId> {
        // Проверяем, что ID начинается с gkl_
        if !Self::is_gkl_id(&node.node.id) {
            return Err(anyhow::anyhow!("GKL node ID must start with '{}'", GKL_PREFIX));
        }

        let stored = StoredGklNode::from(node);
        let bytes = serde_json::to_vec(&stored)?;
        self.backend.put(&node_key(&node.node.id), bytes).await?;

        // Индексы
        self.backend
            .put(&node_by_type_key(node.node.node_type, &node.node.id), node.node.id.0.as_bytes().to_vec())
            .await?;
        self.backend
            .put(&node_by_level_key(node.node.level, &node.node.id), node.node.id.0.as_bytes().to_vec())
            .await?;

        // Кэш
        self.cache.write().await.insert(node.node.id.clone(), node.clone());

        Ok(node.node.id.clone())
    }

    /// Загрузить GKL-узел по ID
    pub async fn get_node(&self, id: &NodeId) -> anyhow::Result<Option<GklNode>> {
        // Проверяем кэш
        if let Some(node) = self.cache.read().await.get(id) {
            return Ok(Some(node.clone()));
        }

        // Проверяем, что ID GKL
        if !Self::is_gkl_id(id) {
            return Ok(None);
        }

        // Читаем из хранилища
        let Some(bytes) = self.backend.get(&node_key(id)).await? else {
            return Ok(None);
        };
        let stored: StoredGklNode = serde_json::from_slice(&bytes)?;
        let node = GklNode::from(stored);

        // Кэшируем
        self.cache.write().await.insert(id.clone(), node.clone());
        Ok(Some(node))
    }

    /// Загрузить все узлы указанного типа
    pub async fn list_by_type(&self, node_type: NodeType) -> anyhow::Result<Vec<GklNode>> {
        let prefix = format!("{GKL_BY_TYPE_PREFIX}{}:", node_type_name(node_type));
        let keys = self.backend.list_keys(&prefix).await?;
        let mut nodes = Vec::new();

        for k in keys {
            if let Some(id_str) = k.strip_prefix(&prefix) {
                let id = NodeId(id_str.to_string());
                if let Some(node) = self.get_node(&id).await? {
                    nodes.push(node);
                }
            }
        }
        Ok(nodes)
    }

    /// Загрузить все узлы указанного уровня
    pub async fn list_by_level(&self, level: Level) -> anyhow::Result<Vec<GklNode>> {
        let prefix = format!("{GKL_BY_LEVEL_PREFIX}{}:", level_name(level));
        let keys = self.backend.list_keys(&prefix).await?;
        let mut nodes = Vec::new();

        for k in keys {
            if let Some(id_str) = k.strip_prefix(&prefix) {
                let id = NodeId(id_str.to_string());
                if let Some(node) = self.get_node(&id).await? {
                    nodes.push(node);
                }
            }
        }
        Ok(nodes)
    }

    /// Загрузить все L1-узлы (хабы/домены)
    pub async fn list_l1_hubs(&self) -> anyhow::Result<Vec<GklNode>> {
        let prefix = format!("{GKL_BY_LEVEL_PREFIX}l1:");
        let keys = self.backend.list_keys(&prefix).await?;
        let mut nodes = Vec::new();

        for k in keys {
            if let Some(id_str) = k.strip_prefix(&prefix) {
                let id = NodeId(id_str.to_string());
                if let Some(node) = self.get_node(&id).await? {
                    if node.is_hub {
                        nodes.push(node);
                    }
                }
            }
        }
        Ok(nodes)
    }

    /// Загрузить все L2-атомы
    pub async fn list_l2_atoms(&self) -> anyhow::Result<Vec<GklNode>> {
        let prefix = format!("{GKL_BY_LEVEL_PREFIX}l2:");
        let keys = self.backend.list_keys(&prefix).await?;
        let mut nodes = Vec::new();

        for k in keys {
            if let Some(id_str) = k.strip_prefix(&prefix) {
                let id = NodeId(id_str.to_string());
                if let Some(node) = self.get_node(&id).await? {
                    nodes.push(node);
                }
            }
        }
        Ok(nodes)
    }

    /// Создать связь между GKL-узлами
    pub async fn add_edge(&self, edge: &Edge) -> anyhow::Result<EdgeId> {
        let bytes = serde_json::to_vec(edge)?;
        self.backend.put(&edge_key(&edge.id), bytes).await?;

        // Обновляем индекс children
        let mut index = self.children_index.write().await;
        index.entry(edge.source.clone()).or_default().insert(edge.target.clone());

        Ok(edge.id.clone())
    }

    /// Загрузить дочерние узлы данного узла
    pub async fn get_children(&self, parent_id: &NodeId) -> anyhow::Result<Vec<GklNode>> {
        let children_ids = self.children_index.read().await.get(parent_id).cloned().unwrap_or_default();
        let mut children = Vec::new();

        for child_id in children_ids {
            if let Some(child) = self.get_node(&child_id).await? {
                children.push(child);
            }
        }
        Ok(children)
    }

    /// Найти узлы по тегам
    pub async fn find_by_tags(&self, tags: &[String]) -> anyhow::Result<Vec<GklNode>> {
        let all_nodes = self.list_all().await?;
        let mut result = Vec::new();

        for node in all_nodes {
            let has_any_tag = tags.iter().any(|tag| node.node.metadata.tags.contains(tag));
            if has_any_tag {
                result.push(node);
            }
        }
        Ok(result)
    }

    /// Загрузить все GKL-узлы
    pub async fn list_all(&self) -> anyhow::Result<Vec<GklNode>> {
        let keys = self.backend.list_keys(GKL_NODE_PREFIX).await?;
        let mut nodes = Vec::new();

        for k in keys {
            if let Some(id_str) = k.strip_prefix(GKL_NODE_PREFIX) {
                let id = NodeId(id_str.to_string());
                if Self::is_gkl_id(&id) {
                    if let Some(node) = self.get_node(&id).await? {
                        nodes.push(node);
                    }
                }
            }
        }
        Ok(nodes)
    }

    /// Удалить GKL-узел
    pub async fn delete_node(&self, id: &NodeId) -> anyhow::Result<()> {
        if !Self::is_gkl_id(id) {
            return Ok(()); // не GKL-узел, игнорируем
        }

        // Удаляем из кэша
        self.cache.write().await.remove(id);

        // Удаляем из индекса children
        let mut index = self.children_index.write().await;
        index.remove(id);
        for children in index.values_mut() {
            children.remove(id);
        }

        // Удаляем из хранилища
        self.backend.delete(&node_key(id)).await?;
        self.backend.delete(&node_by_type_key(NodeType::Atom, id)).await?; // может не быть
        self.backend.delete(&node_by_level_key(Level::L2, id)).await?; // может не быть

        Ok(())
    }

    /// Статистика GKL
    pub async fn stats(&self) -> anyhow::Result<GklStats> {
        let all_nodes = self.list_all().await?;
        let l1_count = all_nodes.iter().filter(|n| n.node.level == Level::L1).count();
        let l2_count = all_nodes.iter().filter(|n| n.node.level == Level::L2).count();
        let hub_count = all_nodes.iter().filter(|n| n.is_hub).count();

        Ok(GklStats {
            total_nodes: all_nodes.len(),
            l1_count,
            l2_count,
            hub_count,
        })
    }

    /// Импортировать узел из workspace в GKL
    pub async fn import_from_workspace(&self, node: &Node, gkl_id: &NodeId) -> anyhow::Result<GklNode> {
        let gkl_node = GklNode {
            node: Node {
                id: gkl_id.clone(),
                node_type: node.node_type,
                level: node.level,
                content: node.content.clone(),
                metadata: Metadata {
                    parent_id: None, // GKL не участвует в cluster hierarchy
                    workspace_id: None, // GKL не привязан к workspace
                    tags: node.metadata.tags.clone(),
                },
                status: crate::graph::Status::Active,
                created_at: node.created_at,
                updated_at: node.created_at,
            },
            is_hub: false,
        };

        self.save_node(&gkl_node).await?;
        Ok(gkl_node)
    }
}

/// Статистика GKL
#[derive(Debug, Clone)]
pub struct GklStats {
    pub total_nodes: usize,
    pub l1_count: usize,
    pub l2_count: usize,
    pub hub_count: usize,
}

#[async_trait]
impl Actor for GKLactor {
    fn name(&self) -> &str {
        "GKLactor"
    }

    async fn size(&self) -> usize {
        self.cache.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::InMemoryBackend;
    use chrono::Utc;

    fn backend() -> Arc<dyn StorageBackend> {
        Arc::new(InMemoryBackend::new())
    }

    fn make_gkl_atom(content: &str, id_suffix: &str) -> GklNode {
        let id = GKLactor::make_gkl_id(id_suffix);
        let mut node = Node::with_id(id, NodeType::Atom, content);
        node.level = Level::L2;
        node.created_at = Utc::now();
        GklNode {
            node,
            is_hub: false,
        }
    }

    fn make_gkl_hub(content: &str, id_suffix: &str) -> GklNode {
        let id = GKLactor::make_gkl_id(id_suffix);
        let mut node = Node::with_id(id, NodeType::Hub, content);
        node.level = Level::L1;
        node.created_at = Utc::now();
        GklNode {
            node,
            is_hub: true,
        }
    }

    #[tokio::test]
    async fn test_make_gkl_id() {
        let id = GKLactor::make_gkl_id("L2_test");
        assert!(id.0.starts_with("gkl_"));
        assert!(id.0.contains("L2_test"));
    }

    #[tokio::test]
    async fn test_save_and_get_node() {
        let actor = GKLactor::new(backend());
        let node = make_gkl_atom("Test atom", "L2_test_atom");

        let id = actor.save_node(&node).await.unwrap();
        let fetched = actor.get_node(&id).await.unwrap().unwrap();

        assert_eq!(fetched.node.content, "Test atom");
        assert_eq!(fetched.node.node_type, NodeType::Atom);
        assert!(!fetched.is_hub);
    }

    #[tokio::test]
    async fn test_save_hub() {
        let actor = GKLactor::new(backend());
        let hub = make_gkl_hub("Framework hub", "L1_framework");

        let id = actor.save_node(&hub).await.unwrap();
        let fetched = actor.get_node(&id).await.unwrap().unwrap();

        assert_eq!(fetched.node.node_type, NodeType::Hub);
        assert!(fetched.is_hub);
        assert_eq!(fetched.node.level, Level::L1);
    }

    #[tokio::test]
    async fn test_non_gkl_id_rejected() {
        let actor = GKLactor::new(backend());
        let mut node = Node::new(NodeType::Atom, "Test");
        node.id = NodeId::from_string("not_gkl_id");
        let gkl_node = GklNode {
            node,
            is_hub: false,
        };

        let result = actor.save_node(&gkl_node).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_by_type() {
        let actor = GKLactor::new(backend());
        actor.save_node(&make_gkl_atom("Atom 1", "L2_atom1")).await.unwrap();
        actor.save_node(&make_gkl_atom("Atom 2", "L2_atom2")).await.unwrap();

        let atoms = actor.list_by_type(NodeType::Atom).await.unwrap();
        assert_eq!(atoms.len(), 2);
    }

    #[tokio::test]
    async fn test_list_by_level() {
        let actor = GKLactor::new(backend());
        actor.save_node(&make_gkl_atom("L2 atom", "L2_test")).await.unwrap();
        actor.save_node(&make_gkl_hub("L1 hub", "L1_test")).await.unwrap();

        let l2_nodes = actor.list_by_level(Level::L2).await.unwrap();
        let l1_nodes = actor.list_by_level(Level::L1).await.unwrap();

        assert_eq!(l2_nodes.len(), 1);
        assert_eq!(l1_nodes.len(), 1);
    }

    #[tokio::test]
    async fn test_list_l1_hubs() {
        let actor = GKLactor::new(backend());
        actor.save_node(&make_gkl_hub("Framework hub", "L1_framework")).await.unwrap();
        actor.save_node(&make_gkl_atom("Not a hub", "L1_not_hub")).await.unwrap(); // is_hub=false

        let hubs = actor.list_l1_hubs().await.unwrap();
        assert_eq!(hubs.len(), 1);
        assert!(hubs[0].is_hub);
    }

    #[tokio::test]
    async fn test_list_l2_atoms() {
        let actor = GKLactor::new(backend());
        actor.save_node(&make_gkl_atom("Atom 1", "L2_atom1")).await.unwrap();
        actor.save_node(&make_gkl_atom("Atom 2", "L2_atom2")).await.unwrap();

        let atoms = actor.list_l2_atoms().await.unwrap();
        assert_eq!(atoms.len(), 2);
    }

    #[tokio::test]
    async fn test_add_edge_and_get_children() {
        let actor = GKLactor::new(backend());
        let hub = make_gkl_hub("Framework hub", "L1_framework");
        let atom1 = make_gkl_atom("Atom 1", "L2_atom1");
        let atom2 = make_gkl_atom("Atom 2", "L2_atom2");

        actor.save_node(&hub).await.unwrap();
        actor.save_node(&atom1).await.unwrap();
        actor.save_node(&atom2).await.unwrap();

        let edge1 = Edge::new(hub.node.id.clone(), atom1.node.id.clone(), Relation::RelatedTo);
        let edge2 = Edge::new(hub.node.id.clone(), atom2.node.id.clone(), Relation::RelatedTo);

        actor.add_edge(&edge1).await.unwrap();
        actor.add_edge(&edge2).await.unwrap();

        let children = actor.get_children(&hub.node.id).await.unwrap();
        assert_eq!(children.len(), 2);
    }

    #[tokio::test]
    async fn test_find_by_tags() {
        let actor = GKLactor::new(backend());
        let mut node1 = make_gkl_atom("Atom 1", "L2_atom1");
        node1.node.metadata.tags = vec!["rust".to_string(), "framework".to_string()];
        let mut node2 = make_gkl_atom("Atom 2", "L2_atom2");
        node2.node.metadata.tags = vec!["python".to_string()];

        actor.save_node(&node1).await.unwrap();
        actor.save_node(&node2).await.unwrap();

        let rust_nodes = actor.find_by_tags(&["rust".to_string()]).await.unwrap();
        assert_eq!(rust_nodes.len(), 1);
        assert!(rust_nodes[0].node.metadata.tags.contains(&"rust".to_string()));
    }

    #[tokio::test]
    async fn test_delete_node() {
        let actor = GKLactor::new(backend());
        let node = make_gkl_atom("To delete", "L2_delete");

        let id = actor.save_node(&node).await.unwrap();
        assert!(actor.get_node(&id).await.unwrap().is_some());

        actor.delete_node(&id).await.unwrap();
        assert!(actor.get_node(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_stats() {
        let actor = GKLactor::new(backend());
        actor.save_node(&make_gkl_atom("Atom 1", "L2_atom1")).await.unwrap();
        actor.save_node(&make_gkl_atom("Atom 2", "L2_atom2")).await.unwrap();
        actor.save_node(&make_gkl_hub("Hub", "L1_hub")).await.unwrap();

        let stats = actor.stats().await.unwrap();
        assert_eq!(stats.total_nodes, 3);
        assert_eq!(stats.l2_count, 2);
        assert_eq!(stats.l1_count, 1);
        assert_eq!(stats.hub_count, 1);
    }

    #[tokio::test]
    async fn test_import_from_workspace() {
        let actor = GKLactor::new(backend());
        let mut ws_node = Node::new(NodeType::Atom, "Workspace atom");
        ws_node.level = Level::L2;
        ws_node.metadata.workspace_id = Some("ws1".to_string());
        ws_node.metadata.tags = vec!["test".to_string()];
        ws_node.created_at = Utc::now();

        let gkl_id = GKLactor::make_gkl_id("L2_imported");
        let gkl_node = actor.import_from_workspace(&ws_node, &gkl_id).await.unwrap();

        assert!(GKLactor::is_gkl_id(&gkl_node.node.id));
        assert_eq!(gkl_node.node.content, "Workspace atom");
        assert!(gkl_node.node.metadata.workspace_id.is_none()); // GKL не имеет workspace
        assert!(gkl_node.node.metadata.tags.contains(&"test".to_string()));
    }

    #[tokio::test]
    async fn test_cache() {
        let actor = GKLactor::new(backend());
        let node = make_gkl_atom("Cached atom", "L2_cached");

        actor.save_node(&node).await.unwrap();

        // Первый запрос — из хранилища
        let n1 = actor.get_node(&node.node.id).await.unwrap().unwrap();
        // Второй запрос — из кэша
        let n2 = actor.get_node(&node.node.id).await.unwrap().unwrap();

        assert_eq!(n1.node.content, n2.node.content);
        assert_eq!(actor.size().await, 1);
    }
}
