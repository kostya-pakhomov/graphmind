//! L0Actor -- хабы и кластеры (агрегация L1-доменов).
//!
//! Based on TECH-SPEC.md Section 4.2 L0: кластеризация доменов.
//! L0Actor агрегирует L1-домены в кластеры и хабы:
//!   - Кластер (Cluster) — группировка доменов по тематике/связям
//!   - Хаб (Hub) — агрегатор нескольких кластеров
//!
//! Алгоритм автогенерации:
//!   1. Собрать все L1-домены workspace
//!   2. Вычислить similarity между доменами (text overlap + edges)
//!   3. Кластеризовать домены (greedy clustering)
//!   4. Создать L0-кластеры и хаб

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::graph::{Edge, EdgeId, Level, Node, NodeId, NodeType, Relation, Metadata};
use crate::persistence::StorageBackend;

use super::Actor;
use super::LlmClient;

const L0_NODE_PREFIX: &str = "node:";
const L0_EDGE_PREFIX: &str = "edge:";
const L0_BY_PARENT_PREFIX: &str = "nodeidx:by_parent:";
const L0_BY_TYPE_CLUSTER_PREFIX: &str = "nodeidx:by_type:cluster:";
const L0_BY_TYPE_HUB_PREFIX: &str = "nodeidx:by_type:hub:";

fn node_key(id: &NodeId) -> String {
    format!("{L0_NODE_PREFIX}{}", id.0)
}

fn edge_key(id: &EdgeId) -> String {
    format!("{L0_EDGE_PREFIX}{}", id.0)
}

fn node_by_parent_key(parent: &str, id: &NodeId) -> String {
    format!("{L0_BY_PARENT_PREFIX}{parent}:{}", id.0)
}

fn node_by_type_cluster_key(id: &NodeId) -> String {
    format!("{L0_BY_TYPE_CLUSTER_PREFIX}{}", id.0)
}

fn node_by_type_hub_key(id: &NodeId) -> String {
    format!("{L0_BY_TYPE_HUB_PREFIX}{}", id.0)
}

/// Сериализуемая версия L0-узла (кластер или хаб)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredL0Node {
    id: NodeId,
    node_type: NodeType,
    #[serde(default)]
    level: Level,
    content: String,
    workspace_id: Option<String>,
    tags: Vec<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    /// IDs дочерних узлов (доменов для кластера, кластеров для хаба).
    /// Bug 005 регрессия 1: #[serde(default)] — L2/L1 узлы не имеют member_ids,
    /// но делят общий индекс nodeidx:by_parent:. Без default list_clusters
    /// падает с «missing field `member_ids`».
    #[serde(default)]
    member_ids: Vec<NodeId>,
}

impl From<&L0Node> for StoredL0Node {
    fn from(node: &L0Node) -> Self {
        Self {
            id: node.node.id.clone(),
            node_type: node.node.node_type,
            level: node.node.level,
            content: node.node.content.clone(),
            workspace_id: node.node.metadata.workspace_id.clone(),
            tags: node.node.metadata.tags.clone(),
            created_at: node.node.created_at,
            member_ids: node.member_ids.clone(),
        }
    }
}

impl From<StoredL0Node> for L0Node {
    fn from(s: StoredL0Node) -> Self {
        L0Node {
            node: Node {
                id: s.id,
                node_type: s.node_type,
                level: s.level,
                content: s.content,
                metadata: Metadata {
                    parent_id: None, // L0 хабы — корни иерархии, parent_id не нужен
                    workspace_id: s.workspace_id,
                    tags: s.tags,
                },                status: crate::graph::Status::Active,
                created_at: s.created_at,
                updated_at: s.created_at,
            },
            member_ids: s.member_ids,
        }
    }
}

/// L0-узел (кластер или хаб) с информацией о членах
#[derive(Debug, Clone)]
pub struct L0Node {
    pub node: Node,
    /// IDs дочерних узлов (домены для кластера, кластеры для хаба)
    pub member_ids: Vec<NodeId>,
}

/// Результат автогенерации L0-структуры
#[derive(Debug, Clone)]
pub struct L0AutogenResult {
    /// Созданные кластеры
    pub clusters: Vec<L0Node>,
    /// Созданный хаб (один на workspace)
    pub hub: Option<L0Node>,
    /// Домены, которые не вошли ни в один кластер
    pub orphan_domains: Vec<NodeId>,
}

/// L0Actor -- автогенерация и управление хабами/кластерами
pub struct L0Actor {
    backend: Arc<dyn StorageBackend>,
    /// Кэш L0-узлов в памяти
    cache: RwLock<HashMap<NodeId, L0Node>>,
    /// Индекс: domain_id -> cluster_id
    domain_to_cluster: RwLock<HashMap<NodeId, NodeId>>,
    /// Индекс: cluster_id -> hub_id
    cluster_to_hub: RwLock<HashMap<NodeId, NodeId>>,
    /// LLM для именования кластеров (опционально; без него — эвристика-склейка).
    llm: Option<LlmClient>,
}

impl L0Actor {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            backend,
            cache: RwLock::new(HashMap::new()),
            domain_to_cluster: RwLock::new(HashMap::new()),
            cluster_to_hub: RwLock::new(HashMap::new()),
            llm: None,
        }
    }

    /// Подключить LLM-клиент: кластеры именуются осмысленно (по образцу P-оси),
    /// а не склейкой первых доменов. Группировка остаётся Jaccard-эвристикой.
    pub fn with_llm(mut self, llm: LlmClient) -> Self {
        self.llm = Some(llm);
        self
    }

    /// Access the underlying backend
    pub fn backend(&self) -> &Arc<dyn StorageBackend> {
        &self.backend
    }

    /// Сохранить L0-узел (кластер или хаб)
    pub async fn save_node(&self, node: &L0Node) -> anyhow::Result<NodeId> {
        let stored = StoredL0Node::from(node);
        let bytes = serde_json::to_vec(&stored)?;
        self.backend.put(&node_key(&node.node.id), bytes).await?;

        // Индексы
        if let Some(ws) = &node.node.metadata.workspace_id {
            self.backend
                .put(&node_by_parent_key(ws, &node.node.id), node.node.id.0.as_bytes().to_vec())
                .await?;
        }

        match node.node.node_type {
            NodeType::Cluster => {
                self.backend
                    .put(&node_by_type_cluster_key(&node.node.id), node.node.id.0.as_bytes().to_vec())
                    .await?;
                // Индекс domain -> cluster
                let mut map = self.domain_to_cluster.write().await;
                for member_id in &node.member_ids {
                    map.insert(member_id.clone(), node.node.id.clone());
                }
            }
            NodeType::Hub => {
                self.backend
                    .put(&node_by_type_hub_key(&node.node.id), node.node.id.0.as_bytes().to_vec())
                    .await?;
                // Индекс cluster -> hub
                let mut map = self.cluster_to_hub.write().await;
                for member_id in &node.member_ids {
                    map.insert(member_id.clone(), node.node.id.clone());
                }
            }
            _ => {}
        }

        // Кэш
        self.cache.write().await.insert(node.node.id.clone(), node.clone());

        Ok(node.node.id.clone())
    }

    /// Загрузить L0-узел по ID
    pub async fn get_node(&self, id: &NodeId) -> anyhow::Result<Option<L0Node>> {
        // Проверяем кэш
        if let Some(node) = self.cache.read().await.get(id) {
            return Ok(Some(node.clone()));
        }

        // Читаем из хранилища
        let Some(bytes) = self.backend.get(&node_key(id)).await? else {
            return Ok(None);
        };
        let stored: StoredL0Node = serde_json::from_slice(&bytes)?;
        let node = L0Node::from(stored);

        // Кэшируем
        self.cache.write().await.insert(id.clone(), node.clone());
        Ok(Some(node))
    }

    /// Загрузить все кластеры workspace
    pub async fn list_clusters(&self, workspace_id: &str) -> anyhow::Result<Vec<L0Node>> {
        let prefix = format!("{L0_BY_PARENT_PREFIX}{workspace_id}:");
        let keys = self.backend.list_keys(&prefix).await?;
        let mut clusters = Vec::new();

        for k in keys {
            if let Some(id_str) = k.strip_prefix(&prefix) {
                let id = NodeId(id_str.to_string());
                if let Some(node) = self.get_node(&id).await? {
                    // Bug 005: индекс by_parent общий для L0/L1/L2 — фильтруем
                    // только кластеры (get_node уже десериализует через serde default).
                    if node.node.node_type == NodeType::Cluster {
                        clusters.push(node);
                    }
                }
            }
        }
        Ok(clusters)
    }

    /// Загрузить хаб workspace
    pub async fn get_hub(&self, workspace_id: &str) -> anyhow::Result<Option<L0Node>> {
        // Ищем хаб по parent
        let prefix = format!("{L0_BY_PARENT_PREFIX}{workspace_id}:");
        let keys = self.backend.list_keys(&prefix).await?;

        for k in keys {
            if let Some(id_str) = k.strip_prefix(&prefix) {
                let id = NodeId(id_str.to_string());
                if let Some(node) = self.get_node(&id).await? {
                    if node.node.node_type == NodeType::Hub {
                        return Ok(Some(node));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Найти кластер, содержащий данный домен
    pub async fn find_cluster_for_domain(&self, domain_id: &NodeId) -> anyhow::Result<Option<L0Node>> {
        // Проверяем индекс
        if let Some(cluster_id) = self.domain_to_cluster.read().await.get(domain_id) {
            return self.get_node(cluster_id).await;
        }
        Ok(None)
    }

    /// Найти хаб, содержащий данный кластер
    pub async fn find_hub_for_cluster(&self, cluster_id: &NodeId) -> anyhow::Result<Option<L0Node>> {
        // Проверяем индекс
        if let Some(hub_id) = self.cluster_to_hub.read().await.get(cluster_id) {
            return self.get_node(hub_id).await;
        }
        Ok(None)
    }

    /// Автогенерация L0-структуры из L1-доменов workspace
    ///
    /// Алгоритм:
    /// 1. Собрать все L1-домены workspace
    /// 2. Вычислить similarity между доменами (text overlap)
    /// 3. Кластеризовать домены (greedy: объединяем similarity > threshold)
    /// 4. Создать L0-кластеры и хаб
    pub async fn autogenerate_l0(
        &self,
        workspace_id: &str,
        l1_domains: &[Node],
    ) -> anyhow::Result<L0AutogenResult> {
        if l1_domains.is_empty() {
            return Ok(L0AutogenResult {
                clusters: Vec::new(),
                hub: None,
                orphan_domains: Vec::new(),
            });
        }

        // 1. Вычисляем similarity между доменами
        // Порог 0.05 — даже слабо похожие домены группируются (Jaccard по словам).
        // Ниже — только если домены совсем не пересекаются по словам.
        let threshold = 0.05;
        let mut adjacency: HashMap<NodeId, HashSet<NodeId>> = HashMap::new();

        for i in 0..l1_domains.len() {
            for j in (i + 1)..l1_domains.len() {
                let sim = self.compute_text_similarity(&l1_domains[i].content, &l1_domains[j].content);
                if sim > threshold {
                    adjacency.entry(l1_domains[i].id.clone()).or_default().insert(l1_domains[j].id.clone());
                    adjacency.entry(l1_domains[j].id.clone()).or_default().insert(l1_domains[i].id.clone());
                }
            }
        }

        // 2. Находим connected components (кластеры)
        let mut visited = HashSet::new();
        let mut clusters: Vec<Vec<NodeId>> = Vec::new();

        for domain in l1_domains {
            if visited.contains(&domain.id) {
                continue;
            }

            let mut component = Vec::new();
            let mut queue = vec![domain.id.clone()];

            while let Some(node_id) = queue.pop() {
                if visited.contains(&node_id) {
                    continue;
                }
                visited.insert(node_id.clone());
                component.push(node_id.clone());

                if let Some(neighbors) = adjacency.get(&node_id) {
                    for neighbor in neighbors {
                        if !visited.contains(neighbor) {
                            queue.push(neighbor.clone());
                        }
                    }
                }
            }

            if !component.is_empty() {
                clusters.push(component);
            }
        }

        // 3. Создаём L0-кластеры
        let mut created_clusters = Vec::new();
        let mut orphan_domains = Vec::new();

        for cluster_members in clusters {
            if cluster_members.len() == 1 {
                // Изолированный домен — оставляем как orphan, обработаем ниже
                orphan_domains.push(cluster_members[0].clone());
                continue;
            }

            // Название кластера: LLM (если подключён), иначе эвристика-склейка.
            let cluster_content = match self.llm_cluster_name(&cluster_members, l1_domains).await {
                Some(name) => name,
                None => self.generate_cluster_summary(&cluster_members, l1_domains),
            };

            let mut cluster_node = Node::new(NodeType::Cluster, cluster_content);
            cluster_node.level = Level::L0;
            cluster_node.metadata.workspace_id = Some(workspace_id.to_string());

            let l0_cluster = L0Node {
                node: cluster_node,
                member_ids: cluster_members,
            };

            self.save_node(&l0_cluster).await?;
            created_clusters.push(l0_cluster);
        }

        // 3.5. Orphan-домены: если они есть, создаём из них один общий кластер,
        // чтобы L0 всегда имел структуру (ораны не теряются).
        if !orphan_domains.is_empty() {
            let cluster_content = match self.llm_cluster_name(&orphan_domains, l1_domains).await {
                Some(name) => name,
                None => self.generate_cluster_summary(&orphan_domains, l1_domains),
            };

            let mut cluster_node = Node::new(NodeType::Cluster, cluster_content);
            cluster_node.level = Level::L0;
            cluster_node.metadata.workspace_id = Some(workspace_id.to_string());

            let l0_cluster = L0Node {
                node: cluster_node,
                member_ids: orphan_domains.clone(),
            };

            self.save_node(&l0_cluster).await?;
            created_clusters.push(l0_cluster);
            orphan_domains.clear();
        }

        // 4. Создаём хаб (агрегатор всех кластеров)
        let hub = if !created_clusters.is_empty() {
            let hub_member_ids: Vec<NodeId> = created_clusters.iter().map(|c| c.node.id.clone()).collect();
            let hub_content = format!("Хаб: {} кластеров, {} доменов", created_clusters.len(), l1_domains.len());

            let mut hub_node = Node::new(NodeType::Hub, hub_content);
            hub_node.level = Level::L0;
            hub_node.metadata.workspace_id = Some(workspace_id.to_string());

            let hub = L0Node {
                node: hub_node,
                member_ids: hub_member_ids,
            };

            self.save_node(&hub).await?;
            Some(hub)
        } else {
            None
        };

        Ok(L0AutogenResult {
            clusters: created_clusters,
            hub,
            orphan_domains,
        })
    }

    /// Создать edge между L0-узлом и дочерним узлом
    pub async fn add_member_edge(
        &self,
        parent_id: &NodeId,
        child_id: &NodeId,
    ) -> anyhow::Result<EdgeId> {
        let edge = Edge::new(parent_id.clone(), child_id.clone(), Relation::RelatedTo);
        let bytes = serde_json::to_vec(&edge)?;
        self.backend.put(&edge_key(&edge.id), bytes).await?;
        Ok(edge.id.clone())
    }

    /// Удалить L0-узел
    pub async fn delete_node(&self, node_id: &NodeId) -> anyhow::Result<()> {
        // Удаляем из кэша
        if let Some(node) = self.cache.write().await.remove(node_id) {
            // Удаляем из индексов
            match node.node.node_type {
                NodeType::Cluster => {
                    let mut map = self.domain_to_cluster.write().await;
                    for member_id in &node.member_ids {
                        map.remove(member_id);
                    }
                }
                NodeType::Hub => {
                    let mut map = self.cluster_to_hub.write().await;
                    for member_id in &node.member_ids {
                        map.remove(member_id);
                    }
                }
                _ => {}
            }
        }

        // Удаляем из хранилища
        self.backend.delete(&node_key(node_id)).await?;
        if let Some(ws) = self.cache.read().await.get(node_id).and_then(|n| n.node.metadata.workspace_id.as_deref()) {
            self.backend.delete(&node_by_parent_key(ws, node_id)).await?;
        }

        match self.cache.read().await.get(node_id).map(|n| n.node.node_type) {
            Some(NodeType::Cluster) => {
                self.backend.delete(&node_by_type_cluster_key(node_id)).await?;
            }
            Some(NodeType::Hub) => {
                self.backend.delete(&node_by_type_hub_key(node_id)).await?;
            }
            _ => {}
        }

        Ok(())
    }

    /// Получить статистику L0-структуры workspace
    pub async fn stats(&self, workspace_id: &str) -> anyhow::Result<L0Stats> {
        let clusters = self.list_clusters(workspace_id).await?;
        let hub = self.get_hub(workspace_id).await?;
        let total_domains: usize = clusters.iter().map(|c| c.member_ids.len()).sum();

        Ok(L0Stats {
            cluster_count: clusters.len(),
            hub_count: if hub.is_some() { 1 } else { 0 },
            total_domains,
            avg_domains_per_cluster: if clusters.is_empty() {
                0.0
            } else {
                total_domains as f64 / clusters.len() as f64
            },
        })
    }

    /// Вычислить текстовую similarity (Jaccard index)
    fn compute_text_similarity(&self, text1: &str, text2: &str) -> f64 {
        let words1: HashSet<String> = text1
            .to_lowercase()
            .split_whitespace()
            .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
            .filter(|s| !s.is_empty() && s.len() > 2)
            .collect();

        let words2: HashSet<String> = text2
            .to_lowercase()
            .split_whitespace()
            .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
            .filter(|s| !s.is_empty() && s.len() > 2)
            .collect();

        if words1.is_empty() || words2.is_empty() {
            return 0.0;
        }

        let intersection = words1.intersection(&words2).count();
        let union = words1.union(&words2).count();

        if union == 0 {
            return 0.0;
        }

        intersection as f64 / union as f64
    }

    /// Сгенерировать краткое описание кластера
    /// Имя кластера через LLM (одна строка). `None` при disabled/ошибке → вызывающий
    /// откатится на `generate_cluster_summary` (эвристика-склейка).
    async fn llm_cluster_name(&self, domain_ids: &[NodeId], domains: &[Node]) -> Option<String> {
        let llm = self.llm.as_ref().filter(|c| c.is_enabled())?;
        let contents: Vec<&str> = domain_ids
            .iter()
            .filter_map(|id| domains.iter().find(|d| &d.id == id))
            .map(|d| d.content.as_str())
            .collect();
        if contents.is_empty() {
            return None;
        }
        let prompt = format!(
            "Дай короткое имя (2-4 слова) области знаний, объединяющей эти домены:\n{}\n\
             Верни ТОЛЬКО имя, без пояснений и кавычек.",
            contents.join("\n")
        );
        match llm
            .chat("Ты именуешь области знаний. Отвечаешь одной строкой.", &prompt)
            .await
        {
            Ok(s) => {
                let name = s.trim().lines().next().unwrap_or("").trim().trim_matches('"').to_string();
                if name.is_empty() {
                    None
                } else {
                    Some(format!("Кластер: {}", name))
                }
            }
            Err(e) => {
                tracing::warn!("L0: LLM-именование кластера не удалось: {}", e);
                None
            }
        }
    }

    fn generate_cluster_summary(&self, domain_ids: &[NodeId], domains: &[Node]) -> String {
        let mut domain_contents: Vec<&str> = Vec::new();
        for domain_id in domain_ids {
            if let Some(domain) = domains.iter().find(|d| d.id == *domain_id) {
                domain_contents.push(&domain.content);
            }
        }

        let preview: Vec<&str> = domain_contents.iter().take(3).copied().collect();
        let joined = preview.join(", ");

        if domain_contents.len() <= 3 {
            format!("Кластер: {}", joined)
        } else {
            format!("Кластер ({} доменов): {} и др.", domain_contents.len(), joined)
        }
    }
}

/// Статистика L0-структуры
#[derive(Debug, Clone)]
pub struct L0Stats {
    pub cluster_count: usize,
    pub hub_count: usize,
    pub total_domains: usize,
    pub avg_domains_per_cluster: f64,
}

#[async_trait]
impl Actor for L0Actor {
    fn name(&self) -> &str {
        "L0Actor"
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

    fn make_domain(content: &str, workspace: Option<&str>) -> Node {
        let mut n = Node::new(NodeType::Domain, content);
        n.level = Level::L1;
        n.metadata.workspace_id = workspace.map(String::from);
        n.created_at = Utc::now();
        n
    }

    #[tokio::test]
    async fn test_save_and_get_cluster() {
        let actor = L0Actor::new(backend());
        let cluster = L0Node {
            node: Node::new(NodeType::Cluster, "Test cluster").with_level(Level::L0),
            member_ids: vec![],
        };

        let id = actor.save_node(&cluster).await.unwrap();
        let fetched = actor.get_node(&id).await.unwrap().unwrap();

        assert_eq!(fetched.node.content, "Test cluster");
        assert_eq!(fetched.node.node_type, NodeType::Cluster);
    }

    #[tokio::test]
    async fn test_save_and_get_hub() {
        let actor = L0Actor::new(backend());
        let hub = L0Node {
            node: Node::new(NodeType::Hub, "Test hub").with_level(Level::L0),
            member_ids: vec![],
        };

        let id = actor.save_node(&hub).await.unwrap();
        let fetched = actor.get_node(&id).await.unwrap().unwrap();

        assert_eq!(fetched.node.content, "Test hub");
        assert_eq!(fetched.node.node_type, NodeType::Hub);
    }

    #[tokio::test]
    async fn test_autogenerate_single_cluster() {
        let actor = L0Actor::new(backend());

        // 3 похожих домена
        let d1 = make_domain("Аутентификация через Clerk", Some("ws1"));
        let d2 = make_domain("Clerk middleware", Some("ws1"));
        let d3 = make_domain("Clerk React client", Some("ws1"));

        let domains = vec![d1, d2, d3];
        let result = actor.autogenerate_l0("ws1", &domains).await.unwrap();

        assert_eq!(result.clusters.len(), 1);
        assert_eq!(result.orphan_domains.len(), 0);
        assert_eq!(result.clusters[0].member_ids.len(), 3);
    }

    #[tokio::test]
    async fn test_autogenerate_multiple_clusters() {
        let actor = L0Actor::new(backend());

        // Кластер 1: очень похожие домены (Clerk)
        let d1 = make_domain("Clerk authentication setup", Some("ws1"));
        let d2 = make_domain("Clerk middleware integration", Some("ws1"));
        // Кластер 2: очень похожие домены (Database)
        let d3 = make_domain("PostgreSQL database schema", Some("ws1"));
        let d4 = make_domain("PostgreSQL database migrations", Some("ws1"));

        let domains = vec![d1, d2, d3, d4];
        let result = actor.autogenerate_l0("ws1", &domains).await.unwrap();

        // Ожидаем 2 кластера (Clerk и PostgreSQL)
        assert!(result.clusters.len() >= 2, "Expected at least 2 clusters, got {}", result.clusters.len());
        assert_eq!(result.orphan_domains.len(), 0);
    }

    #[tokio::test]
    async fn test_autogenerate_with_hub() {
        let actor = L0Actor::new(backend());

        let d1 = make_domain("Аутентификация через Clerk", Some("ws1"));
        let d2 = make_domain("Clerk middleware", Some("ws1"));

        let domains = vec![d1, d2];
        let result = actor.autogenerate_l0("ws1", &domains).await.unwrap();

        assert_eq!(result.clusters.len(), 1);
        assert!(result.hub.is_some());
        assert_eq!(result.hub.unwrap().member_ids.len(), 1);
    }

    #[tokio::test]
    async fn test_find_cluster_for_domain() {
        let actor = L0Actor::new(backend());

        let d1 = make_domain("Аутентификация через Clerk", Some("ws1"));
        let d2 = make_domain("Clerk middleware", Some("ws1"));

        let domains = vec![d1.clone(), d2.clone()];
        let result = actor.autogenerate_l0("ws1", &domains).await.unwrap();
        let cluster_id = result.clusters[0].node.id.clone();

        let cluster = actor.find_cluster_for_domain(&d1.id).await.unwrap().unwrap();
        assert_eq!(cluster.node.id, cluster_id);
    }

    #[tokio::test]
    async fn test_list_clusters() {
        let actor = L0Actor::new(backend());

        // ws1: аутентификация
        let d1 = make_domain("Clerk authentication setup", Some("ws1"));
        let d2 = make_domain("Clerk middleware integration", Some("ws1"));
        // ws2: база данных
        let d3 = make_domain("PostgreSQL database schema", Some("ws2"));
        let d4 = make_domain("PostgreSQL database migrations", Some("ws2"));

        let result1 = actor.autogenerate_l0("ws1", &[d1, d2]).await.unwrap();
        let result2 = actor.autogenerate_l0("ws2", &[d3, d4]).await.unwrap();

        // Проверяем, что кластеры созданы
        assert_eq!(result1.clusters.len(), 1, "ws1 clusters: {:?}", result1);
        assert_eq!(result2.clusters.len(), 1, "ws2 clusters: {:?}", result2);

        // Проверяем workspace_id у кластеров
        let ws1_cluster_id = &result1.clusters[0].node.id;
        let ws2_cluster_id = &result2.clusters[0].node.id;
        let ws1_cluster = actor.get_node(ws1_cluster_id).await.unwrap().unwrap();
        let ws2_cluster = actor.get_node(ws2_cluster_id).await.unwrap().unwrap();
        assert_eq!(ws1_cluster.node.metadata.workspace_id, Some("ws1".to_string()));
        assert_eq!(ws2_cluster.node.metadata.workspace_id, Some("ws2".to_string()));

        let ws1_clusters = actor.list_clusters("ws1").await.unwrap();
        let ws2_clusters = actor.list_clusters("ws2").await.unwrap();

        assert_eq!(ws1_clusters.len(), 1, "ws1 list_clusters: {:?}", ws1_clusters);
        assert_eq!(ws2_clusters.len(), 1, "ws2 list_clusters: {:?}", ws2_clusters);
    }

    #[tokio::test]
    async fn test_delete_cluster() {
        let actor = L0Actor::new(backend());

        let d1 = make_domain("Аутентификация через Clerk", Some("ws1"));
        let d2 = make_domain("Clerk middleware", Some("ws1"));

        let result = actor.autogenerate_l0("ws1", &[d1.clone(), d2.clone()]).await.unwrap();
        let cluster_id = result.clusters[0].node.id.clone();

        assert!(actor.get_node(&cluster_id).await.unwrap().is_some());

        actor.delete_node(&cluster_id).await.unwrap();

        assert!(actor.get_node(&cluster_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_stats() {
        let actor = L0Actor::new(backend());

        let d1 = make_domain("Аутентификация через Clerk", Some("ws1"));
        let d2 = make_domain("Clerk middleware", Some("ws1"));
        let d3 = make_domain("PostgreSQL Drizzle schema", Some("ws1"));
        let d4 = make_domain("Database migrations", Some("ws1"));

        actor.autogenerate_l0("ws1", &[d1, d2, d3, d4]).await.unwrap();

        let stats = actor.stats("ws1").await.unwrap();
        assert!(stats.cluster_count >= 1);
        assert!(stats.hub_count >= 1);
    }

    #[tokio::test]
    async fn test_compute_text_similarity() {
        let actor = L0Actor::new(backend());

        // Одинаковые тексты
        let sim1 = actor.compute_text_similarity("Clerk middleware", "Clerk middleware");
        assert!((sim1 - 1.0).abs() < 0.01);

        // Похожие тексты
        let sim2 = actor.compute_text_similarity("Clerk middleware", "Clerk React client");
        assert!(sim2 > 0.0);
        assert!(sim2 < 1.0);

        // Разные тексты
        let sim3 = actor.compute_text_similarity("Clerk middleware", "PostgreSQL schema");
        assert!(sim3 < 0.5);
    }
}
