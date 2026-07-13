//! L1Actor -- автогенерация доменов (L1) из L2-атомов.
//!
//! Based on TECH-SPEC.md Section 4.2 L1: domain autogeneration from L2 atoms.
//! L1Actor агрегирует L2-узлы в домены на основе:
//!   1. Семантической группировки через LLM (если подключён `LlmClient`) — атомам
//!      НЕ нужны рёбра, поэтому durable-конвейер (propose_new_memory без link_nodes)
//!      тоже консолидируется, а не даёт 0 доменов.
//!   2. Иначе — связности по рёбрам (connected components) как fallback.
//!   3. Workspace-группировки.
//!
//! Алгоритм автогенерации:
//!   1. Собрать все L2-атомы workspace
//!   2. Построить граф связности (connected components)
//!   3. Для каждого компонента сгенерировать L1-домен
//!   4. Создать edges DerivedFrom между L1 и L2

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::graph::{Edge, EdgeId, Level, Node, NodeId, NodeType, Relation, Metadata};
use crate::persistence::StorageBackend;

use super::Actor;
use super::LlmClient;

const L1_NODE_PREFIX: &str = "node:";
const L1_EDGE_PREFIX: &str = "edge:";
const L1_BY_PARENT_PREFIX: &str = "nodeidx:by_parent:";
const L1_BY_TYPE_PREFIX: &str = "nodeidx:by_type:domain:";

fn node_key(id: &NodeId) -> String {
    format!("{L1_NODE_PREFIX}{}", id.0)
}

fn edge_key(id: &EdgeId) -> String {
    format!("{L1_EDGE_PREFIX}{}", id.0)
}

fn node_by_parent_key(parent: &str, id: &NodeId) -> String {
    format!("{L1_BY_PARENT_PREFIX}{parent}:{}", id.0)
}

fn node_by_type_key(id: &NodeId) -> String {
    format!("{L1_BY_TYPE_PREFIX}{}", id.0)
}

/// Сериализуемая версия L1-узла (домена)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredL1Node {
    id: NodeId,
    node_type: NodeType,
    #[serde(default)]
    level: Level,
    content: String,
    workspace_id: Option<String>,
    tags: Vec<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    /// IDs L2-атомов, которые входят в этот домен.
    /// #[serde(default)] — L2/L0 узлы не имеют member_atom_ids, но делят
    /// общий индекс nodeidx:by_parent:. Без default list_domains падает.
    #[serde(default)]
    member_atom_ids: Vec<NodeId>,
}

impl From<&L1Domain> for StoredL1Node {
    fn from(domain: &L1Domain) -> Self {
        Self {
            id: domain.node.id.clone(),
            node_type: domain.node.node_type,
            level: domain.node.level,
            content: domain.node.content.clone(),
            workspace_id: domain.node.metadata.workspace_id.clone(),
            tags: domain.node.metadata.tags.clone(),
            created_at: domain.node.created_at,
            member_atom_ids: domain.member_atom_ids.clone(),
        }
    }
}

impl From<StoredL1Node> for L1Domain {
    fn from(s: StoredL1Node) -> Self {
        L1Domain {
            node: Node {
                id: s.id,
                node_type: s.node_type,
                level: s.level,
                content: s.content,
                metadata: Metadata {
                    parent_id: None, // L1 домены автогенерятся из L2, явный parent_id опционален
                    workspace_id: s.workspace_id,
                    tags: s.tags,
                },                status: crate::graph::Status::Active,
                created_at: s.created_at,
                updated_at: s.created_at,
            },
            member_atom_ids: s.member_atom_ids,
        }
    }
}

/// L1-домен с информацией о входящих L2-атомах
#[derive(Debug, Clone)]
pub struct L1Domain {
    pub node: Node,
    /// IDs атомов, которые входят в этот домен
    pub member_atom_ids: Vec<NodeId>,
}

/// Результат автогенерации доменов
#[derive(Debug, Clone)]
pub struct AutogenResult {
    /// Созданные L1-домены
    pub created_domains: Vec<L1Domain>,
    /// Обновлённые домены (если атомы перераспределились)
    pub updated_domains: Vec<L1Domain>,
    /// Атомы, которые не вошли ни в один домен (изолированные)
    pub orphan_atoms: Vec<NodeId>,
}

/// L1Actor -- автогенерация и управление доменами
pub struct L1Actor {
    backend: Arc<dyn StorageBackend>,
    /// Кэш L1-доменов в памяти (для быстрого доступа)
    domain_cache: RwLock<HashMap<NodeId, L1Domain>>,
    /// Индекс: atom_id -> domain_id (для быстрого поиска домена атома)
    atom_to_domain: RwLock<HashMap<NodeId, NodeId>>,
    /// LLM для семантической группировки/именования (опционально; без него — fallback).
    llm: Option<LlmClient>,
}

impl L1Actor {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            backend,
            domain_cache: RwLock::new(HashMap::new()),
            atom_to_domain: RwLock::new(HashMap::new()),
            llm: None,
        }
    }

    /// Подключить LLM-клиент: `autogenerate_domains` начнёт группировать атомы
    /// семантически (по образцу P-оси), а не только по рёбрам. Без клиента —
    /// прежнее поведение (связность по рёбрам).
    pub fn with_llm(mut self, llm: LlmClient) -> Self {
        self.llm = Some(llm);
        self
    }

    /// Access the underlying backend
    pub fn backend(&self) -> &Arc<dyn StorageBackend> {
        &self.backend
    }

    /// Сохранить L1-домен в хранилище
    pub async fn save_domain(&self, domain: &L1Domain) -> anyhow::Result<NodeId> {
        let stored = StoredL1Node::from(domain);
        let bytes = serde_json::to_vec(&stored)?;
        self.backend.put(&node_key(&domain.node.id), bytes).await?;

        // Индексы
        if let Some(ws) = &domain.node.metadata.workspace_id {
            self.backend
                .put(&node_by_parent_key(ws, &domain.node.id), domain.node.id.0.as_bytes().to_vec())
                .await?;
        }
        self.backend
            .put(&node_by_type_key(&domain.node.id), domain.node.id.0.as_bytes().to_vec())
            .await?;

        // Кэш
        self.domain_cache
            .write()
            .await
            .insert(domain.node.id.clone(), domain.clone());

        // Индекс атом -> домен
        let mut atom_map = self.atom_to_domain.write().await;
        for atom_id in &domain.member_atom_ids {
            atom_map.insert(atom_id.clone(), domain.node.id.clone());
        }

        Ok(domain.node.id.clone())
    }

    /// Загрузить L1-домен по ID
    pub async fn get_domain(&self, id: &NodeId) -> anyhow::Result<Option<L1Domain>> {
        // Проверяем кэш
        if let Some(domain) = self.domain_cache.read().await.get(id) {
            return Ok(Some(domain.clone()));
        }

        // Читаем из хранилища
        let Some(bytes) = self.backend.get(&node_key(id)).await? else {
            return Ok(None);
        };
        let stored: StoredL1Node = serde_json::from_slice(&bytes)?;
        let domain = L1Domain::from(stored);

        // Кэшируем
        self.domain_cache.write().await.insert(id.clone(), domain.clone());
        Ok(Some(domain))
    }

    /// Загрузить все домены workspace
    pub async fn list_domains(&self, workspace_id: &str) -> anyhow::Result<Vec<L1Domain>> {
        let prefix = format!("{L1_BY_PARENT_PREFIX}{workspace_id}:");
        let keys = self.backend.list_keys(&prefix).await?;
        let mut domains = Vec::with_capacity(keys.len());

        for k in keys {
            if let Some(id_str) = k.strip_prefix(&prefix) {
                let id = NodeId(id_str.to_string());
                if let Some(domain) = self.get_domain(&id).await? {
                    // Bug 005: индекс by_parent общий для L0/L1/L2 — фильтруем
                    // только домены, иначе L2-атомы попадают в list_domains.
                    if domain.node.node_type == NodeType::Domain {
                        domains.push(domain);
                    }
                }
            }
        }
        Ok(domains)
    }

    /// Найти домен, содержащий данный атом
    pub async fn find_domain_for_atom(&self, atom_id: &NodeId) -> anyhow::Result<Option<L1Domain>> {
        // Проверяем индекс
        if let Some(domain_id) = self.atom_to_domain.read().await.get(atom_id) {
            return self.get_domain(domain_id).await;
        }

        // Ищем в хранилище (перебор доменов workspace)
        // TODO: оптимизировать через отдельный индекс
        Ok(None)
    }

    /// Автогенерация доменов из L2-атомов workspace
    ///
    /// Алгоритм:
    /// 1. Собрать все L2-атомы workspace через L2Actor
    /// 2. Построить граф связности через edges
    /// 3. Найти connected components
    /// 4. Для каждого компонента создать L1-домен
    /// 5. Создать edges DerivedFrom между L1 и L2
    pub async fn autogenerate_domains(
        &self,
        workspace_id: &str,
        l2_atoms: &[Node],
        l2_edges: &[Edge],
    ) -> anyhow::Result<AutogenResult> {
        // LLM-путь (пункт 1.3): семантическая группировка атомов БЕЗ опоры на рёбра.
        // Durable propose_new_memory рёбер не создаёт, поэтому связность-путь ниже давал
        // 0 доменов (каждый атом — свой компонент → orphan). LLM группирует по смыслу.
        if let Some(llm) = self.llm.as_ref().filter(|c| c.is_enabled()) {
            match self.llm_group_atoms(llm, l2_atoms).await {
                Ok(groups) if !groups.is_empty() => {
                    return self.build_domains_from_groups(workspace_id, groups).await;
                }
                Ok(_) => {
                    tracing::warn!("L1: LLM вернул пустую группировку — fallback на связность по рёбрам");
                }
                Err(e) => {
                    tracing::warn!("L1: LLM-группировка не удалась ({}), fallback на связность по рёбрам", e);
                }
            }
        }

        // 1. Строим граф связности атомов
        let mut adjacency: HashMap<NodeId, HashSet<NodeId>> = HashMap::new();
        for edge in l2_edges {
            adjacency.entry(edge.source.clone()).or_default().insert(edge.target.clone());
            adjacency.entry(edge.target.clone()).or_default().insert(edge.source.clone());
        }

        // 2. Находим connected components
        let mut visited = HashSet::new();
        let mut components: Vec<Vec<NodeId>> = Vec::new();

        for atom in l2_atoms {
            if visited.contains(&atom.id) {
                continue;
            }

            // BFS для поиска компонента связности
            let mut component = Vec::new();
            let mut queue = vec![atom.id.clone()];

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
                components.push(component);
            }
        }

        // 3. Создаём L1-домены для каждого компонента
        let mut created_domains = Vec::new();
        let mut orphan_atoms = Vec::new();

        for component in components {
            if component.len() == 1 {
                // Изолированный атом
                orphan_atoms.push(component[0].clone());
                continue;
            }

            // Генерируем название домена на основе контента атомов
            let domain_content = self.generate_domain_summary(&component, l2_atoms);

            let mut domain_node = Node::new(NodeType::Domain, domain_content);
            domain_node.level = Level::L1;
            domain_node.metadata.workspace_id = Some(workspace_id.to_string());

            let domain = L1Domain {
                node: domain_node,
                member_atom_ids: component.clone(),
            };

            self.save_domain(&domain).await?;
            created_domains.push(domain);
        }

        Ok(AutogenResult {
            created_domains,
            updated_domains: Vec::new(),
            orphan_atoms,
        })
    }

    /// Создать edge между L1-доменом и L2-атомом
    pub async fn add_domain_atom_edge(
        &self,
        domain_id: &NodeId,
        atom_id: &NodeId,
    ) -> anyhow::Result<EdgeId> {
        let edge = Edge::new(domain_id.clone(), atom_id.clone(), Relation::DerivedFrom);
        let bytes = serde_json::to_vec(&edge)?;
        self.backend.put(&edge_key(&edge.id), bytes).await?;
        Ok(edge.id.clone())
    }

    /// Удалить L1-домен (и связанные edges)
    pub async fn delete_domain(&self, domain_id: &NodeId) -> anyhow::Result<()> {
        // Удаляем из кэша
        if let Some(domain) = self.domain_cache.write().await.remove(domain_id) {
            // Удаляем из индекса атомов
            let mut atom_map = self.atom_to_domain.write().await;
            for atom_id in &domain.member_atom_ids {
                atom_map.remove(atom_id);
            }
        }

        // Удаляем из хранилища
        self.backend.delete(&node_key(domain_id)).await?;
        if let Some(ws) = self.domain_cache.read().await.get(domain_id).and_then(|d| d.node.metadata.workspace_id.as_deref()) {
            self.backend.delete(&node_by_parent_key(ws, domain_id)).await?;
        }
        self.backend.delete(&node_by_type_key(domain_id)).await?;

        Ok(())
    }

    /// Построить домены из LLM-группировки [(имя, [atom_ids])].
    async fn build_domains_from_groups(
        &self,
        workspace_id: &str,
        groups: Vec<(String, Vec<NodeId>)>,
    ) -> anyhow::Result<AutogenResult> {
        let mut created_domains = Vec::new();
        for (name, members) in groups {
            if members.is_empty() {
                continue;
            }
            let mut domain_node = Node::new(NodeType::Domain, name);
            domain_node.level = Level::L1;
            domain_node.metadata.workspace_id = Some(workspace_id.to_string());
            let domain = L1Domain {
                node: domain_node,
                member_atom_ids: members,
            };
            self.save_domain(&domain).await?;
            created_domains.push(domain);
        }
        Ok(AutogenResult {
            created_domains,
            updated_domains: Vec::new(),
            orphan_atoms: Vec::new(),
        })
    }

    /// Спросить у LLM группировку атомов по темам. Возврат: [(имя_домена, [atom_ids])].
    async fn llm_group_atoms(
        &self,
        llm: &LlmClient,
        atoms: &[Node],
    ) -> anyhow::Result<Vec<(String, Vec<NodeId>)>> {
        if atoms.is_empty() {
            return Ok(Vec::new());
        }
        let prompt = Self::build_grouping_prompt(atoms);
        let system = "Ты организуешь факты памяти в тематические домены. Отвечаешь СТРОГО JSON-массивом, без пояснений.";
        let response = llm.chat(system, &prompt).await?;
        Ok(Self::parse_grouping_response(&response, atoms))
    }

    /// Промпт группировки (чистая функция — юнит-тестируется отдельно от сети).
    fn build_grouping_prompt(atoms: &[Node]) -> String {
        let mut lines = String::new();
        for (i, a) in atoms.iter().enumerate() {
            lines.push_str(&format!("{}: {}\n", i, a.content.replace('\n', " ")));
        }
        format!(
            "Сгруппируй факты по темам (доменам). Факты по индексам:\n{}\n\
             Верни ТОЛЬКО JSON-массив групп, формат:\n\
             [{{\"name\": \"короткое имя домена (2-4 слова)\", \"members\": [индексы фактов]}}]\n\
             Правила: каждый факт в ровно одной группе; близкие по смыслу факты — в один домен.",
            lines
        )
    }

    /// Разбор ответа LLM в группы (чистая функция). Индексы → NodeId по позиции.
    /// Терпима к обёрткам (```json, префиксный текст): берём срез от первого '[' до
    /// последнего ']'. Некорректный ответ → пусто (вызовет fallback на рёбра).
    fn parse_grouping_response(response: &str, atoms: &[Node]) -> Vec<(String, Vec<NodeId>)> {
        let (start, end) = match (response.find('['), response.rfind(']')) {
            (Some(s), Some(e)) if e > s => (s, e),
            _ => return Vec::new(),
        };
        let parsed: serde_json::Value = match serde_json::from_str(&response[start..=end]) {
            Ok(v) => v,
            Err(_) => return Vec::new(),
        };
        let arr = match parsed.as_array() {
            Some(a) => a,
            None => return Vec::new(),
        };
        let mut out = Vec::new();
        for group in arr {
            let name = group
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("Домен")
                .to_string();
            let members: Vec<NodeId> = group
                .get("members")
                .and_then(|m| m.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_u64())
                        .filter_map(|i| atoms.get(i as usize))
                        .map(|a| a.id.clone())
                        .collect()
                })
                .unwrap_or_default();
            if !members.is_empty() {
                out.push((name, members));
            }
        }
        out
    }

    /// Сгенерировать краткое описание домена на основе атомов
    fn generate_domain_summary(&self, atom_ids: &[NodeId], atoms: &[Node]) -> String {
        // Собираем контент атомов
        let mut atom_contents: Vec<&str> = Vec::new();
        for atom_id in atom_ids {
            if let Some(atom) = atoms.iter().find(|a| a.id == *atom_id) {
                atom_contents.push(&atom.content);
            }
        }

        // Простая эвристика: берём первые 3 атома и объединяем
        let preview: Vec<&str> = atom_contents.iter().take(3).copied().collect();
        let joined = preview.join(", ");

        // Генерируем summary
        if atom_contents.len() <= 3 {
            format!("Домен: {}", joined)
        } else {
            format!("Домен ({} атомов): {} и др.", atom_contents.len(), joined)
        }
    }

    /// Перегенерировать домены (после изменений в L2)
    pub async fn regenerate_domains(
        &self,
        workspace_id: &str,
        l2_atoms: &[Node],
        l2_edges: &[Edge],
    ) -> anyhow::Result<AutogenResult> {
        // 1. Загружаем текущие домены
        let current_domains = self.list_domains(workspace_id).await?;

        // 2. Строим текущее покрытие атомов доменами
        let mut current_atom_coverage: HashMap<NodeId, NodeId> = HashMap::new();
        for domain in &current_domains {
            for atom_id in &domain.member_atom_ids {
                current_atom_coverage.insert(atom_id.clone(), domain.node.id.clone());
            }
        }

        // 3. Автогенерируем новые домены
        let autogen = self.autogenerate_domains(workspace_id, l2_atoms, l2_edges).await?;

        // 4. Сравниваем покрытия и находим изменения
        let mut updated_domains = Vec::new();
        for new_domain in &autogen.created_domains {
            // Проверяем, есть ли изменения в составе атомов
            let mut changed = false;
            for atom_id in &new_domain.member_atom_ids {
                if let Some(old_domain_id) = current_atom_coverage.get(atom_id) {
                    if old_domain_id != &new_domain.node.id {
                        changed = true;
                        break;
                    }
                } else {
                    changed = true;
                    break;
                }
            }

            if changed {
                updated_domains.push(new_domain.clone());
            }
        }

        Ok(AutogenResult {
            created_domains: autogen.created_domains,
            updated_domains,
            orphan_atoms: autogen.orphan_atoms,
        })
    }

    /// Получить статистику L1-доменов workspace
    pub async fn stats(&self, workspace_id: &str) -> anyhow::Result<L1Stats> {
        let domains = self.list_domains(workspace_id).await?;
        let total_atoms: usize = domains.iter().map(|d| d.member_atom_ids.len()).sum();

        Ok(L1Stats {
            domain_count: domains.len(),
            total_atoms,
            avg_atoms_per_domain: if domains.is_empty() {
                0.0
            } else {
                total_atoms as f64 / domains.len() as f64
            },
        })
    }
}

/// Статистика L1-доменов
#[derive(Debug, Clone)]
pub struct L1Stats {
    pub domain_count: usize,
    pub total_atoms: usize,
    pub avg_atoms_per_domain: f64,
}

#[async_trait]
impl Actor for L1Actor {
    fn name(&self) -> &str {
        "L1Actor"
    }

    async fn size(&self) -> usize {
        self.domain_cache.read().await.len()
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

    fn make_atom(content: &str, workspace: Option<&str>) -> Node {
        let mut n = Node::new(NodeType::Atom, content);
        n.level = Level::L2;
        n.metadata.workspace_id = workspace.map(String::from);
        n.created_at = Utc::now();
        n
    }

    #[tokio::test]
    async fn test_save_and_get_domain() {
        let actor = L1Actor::new(backend());
        let domain = L1Domain {
            node: Node::new(NodeType::Domain, "Test domain").with_level(Level::L1),
            member_atom_ids: vec![],
        };

        let id = actor.save_domain(&domain).await.unwrap();
        let fetched = actor.get_domain(&id).await.unwrap().unwrap();

        assert_eq!(fetched.node.content, "Test domain");
        assert_eq!(fetched.node.level, Level::L1);
    }

    #[tokio::test]
    async fn test_autogenerate_single_domain() {
        let actor = L1Actor::new(backend());

        // Создаём 3 связанных атома
        let a1 = make_atom("Атом 1", Some("ws1"));
        let a2 = make_atom("Атом 2", Some("ws1"));
        let a3 = make_atom("Атом 3", Some("ws1"));

        let e1 = Edge::new(a1.id.clone(), a2.id.clone(), Relation::RelatedTo);
        let e2 = Edge::new(a2.id.clone(), a3.id.clone(), Relation::LeadsTo);

        let atoms = vec![a1.clone(), a2.clone(), a3.clone()];
        let edges = vec![e1, e2];

        let result = actor.autogenerate_domains("ws1", &atoms, &edges).await.unwrap();

        assert_eq!(result.created_domains.len(), 1);
        assert_eq!(result.orphan_atoms.len(), 0);
        assert_eq!(result.created_domains[0].member_atom_ids.len(), 3);
    }

    #[tokio::test]
    async fn test_autogenerate_multiple_domains() {
        let actor = L1Actor::new(backend());

        // Группа 1: a1-a2
        let a1 = make_atom("Атом 1", Some("ws1"));
        let a2 = make_atom("Атом 2", Some("ws1"));
        // Группа 2: a3-a4
        let a3 = make_atom("Атом 3", Some("ws1"));
        let a4 = make_atom("Атом 4", Some("ws1"));

        let e1 = Edge::new(a1.id.clone(), a2.id.clone(), Relation::RelatedTo);
        let e2 = Edge::new(a3.id.clone(), a4.id.clone(), Relation::RelatedTo);

        let atoms = vec![a1, a2, a3, a4];
        let edges = vec![e1, e2];

        let result = actor.autogenerate_domains("ws1", &atoms, &edges).await.unwrap();

        assert_eq!(result.created_domains.len(), 2);
        assert_eq!(result.orphan_atoms.len(), 0);
    }

    #[tokio::test]
    async fn test_autogenerate_with_orphans() {
        let actor = L1Actor::new(backend());

        // Группа: a1-a2
        let a1 = make_atom("Атом 1", Some("ws1"));
        let a2 = make_atom("Атом 2", Some("ws1"));
        // Изолированный: a3
        let a3 = make_atom("Атом 3", Some("ws1"));

        let e1 = Edge::new(a1.id.clone(), a2.id.clone(), Relation::RelatedTo);

        let atoms = vec![a1, a2, a3.clone()];
        let edges = vec![e1];

        let result = actor.autogenerate_domains("ws1", &atoms, &edges).await.unwrap();

        assert_eq!(result.created_domains.len(), 1);
        assert_eq!(result.orphan_atoms.len(), 1);
        assert!(result.orphan_atoms.contains(&a3.id));
    }

    #[tokio::test]
    async fn test_find_domain_for_atom() {
        let actor = L1Actor::new(backend());

        let a1 = make_atom("Атом 1", Some("ws1"));
        let a2 = make_atom("Атом 2", Some("ws1"));
        let e1 = Edge::new(a1.id.clone(), a2.id.clone(), Relation::RelatedTo);

        let atoms = vec![a1.clone(), a2.clone()];
        let edges = vec![e1];

        let result = actor.autogenerate_domains("ws1", &atoms, &edges).await.unwrap();
        let domain_id = result.created_domains[0].node.id.clone();

        // Находим домен для a1
        let domain = actor.find_domain_for_atom(&a1.id).await.unwrap().unwrap();
        assert_eq!(domain.node.id, domain_id);
    }

    #[tokio::test]
    async fn test_list_domains() {
        let actor = L1Actor::new(backend());

        let a1 = make_atom("Атом 1", Some("ws1"));
        let a2 = make_atom("Атом 2", Some("ws1"));
        let a3 = make_atom("Атом 3", Some("ws2"));
        let a4 = make_atom("Атом 4", Some("ws2"));

        let e1 = Edge::new(a1.id.clone(), a2.id.clone(), Relation::RelatedTo);
        let e2 = Edge::new(a3.id.clone(), a4.id.clone(), Relation::RelatedTo);

        // ws1
        actor.autogenerate_domains("ws1", &[a1, a2], &[e1.clone()]).await.unwrap();
        // ws2
        actor.autogenerate_domains("ws2", &[a3, a4], &[e2]).await.unwrap();

        let ws1_domains = actor.list_domains("ws1").await.unwrap();
        let ws2_domains = actor.list_domains("ws2").await.unwrap();

        assert_eq!(ws1_domains.len(), 1);
        assert_eq!(ws2_domains.len(), 1);
    }

    #[tokio::test]
    async fn test_delete_domain() {
        let actor = L1Actor::new(backend());

        let a1 = make_atom("Атом 1", Some("ws1"));
        let a2 = make_atom("Атом 2", Some("ws1"));
        let e1 = Edge::new(a1.id.clone(), a2.id.clone(), Relation::RelatedTo);

        let result = actor.autogenerate_domains("ws1", &[a1.clone(), a2.clone()], &[e1]).await.unwrap();
        let domain_id = result.created_domains[0].node.id.clone();

        // Проверяем, что домен есть
        assert!(actor.get_domain(&domain_id).await.unwrap().is_some());

        // Удаляем
        actor.delete_domain(&domain_id).await.unwrap();

        // Проверяем, что домена нет
        assert!(actor.get_domain(&domain_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_stats() {
        let actor = L1Actor::new(backend());

        let a1 = make_atom("Атом 1", Some("ws1"));
        let a2 = make_atom("Атом 2", Some("ws1"));
        let a3 = make_atom("Атом 3", Some("ws1"));
        let e1 = Edge::new(a1.id.clone(), a2.id.clone(), Relation::RelatedTo);
        let e2 = Edge::new(a2.id.clone(), a3.id.clone(), Relation::RelatedTo);

        actor.autogenerate_domains("ws1", &[a1, a2, a3], &[e1, e2]).await.unwrap();

        let stats = actor.stats("ws1").await.unwrap();
        assert_eq!(stats.domain_count, 1);
        assert_eq!(stats.total_atoms, 3);
        assert!((stats.avg_atoms_per_domain - 3.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_domain_cache() {
        let actor = L1Actor::new(backend());

        let domain = L1Domain {
            node: Node::new(NodeType::Domain, "Cached domain").with_level(Level::L1),
            member_atom_ids: vec![],
        };

        let id = actor.save_domain(&domain).await.unwrap();

        // Первый запрос — из хранилища
        let d1 = actor.get_domain(&id).await.unwrap().unwrap();
        // Второй запрос — из кэша
        let d2 = actor.get_domain(&id).await.unwrap().unwrap();

        assert_eq!(d1.node.content, d2.node.content);
        assert_eq!(actor.size().await, 1);
    }

    // --- LLM-группировка (пункт 1.3): чистые функции промпта/парсинга ---

    #[test]
    fn llm_prompt_lists_atoms_by_index() {
        let atoms = vec![
            Node::new(NodeType::Atom, "факт про 1С и НДС"),
            Node::new(NodeType::Atom, "встреча в пятницу"),
        ];
        let p = L1Actor::build_grouping_prompt(&atoms);
        assert!(p.contains("0: факт про 1С и НДС"));
        assert!(p.contains("1: встреча в пятницу"));
        assert!(p.contains("JSON"));
    }

    #[test]
    fn llm_parse_maps_indices_to_ids() {
        let atoms = vec![
            Node::new(NodeType::Atom, "x"),
            Node::new(NodeType::Atom, "y"),
            Node::new(NodeType::Atom, "z"),
        ];
        let resp = r#"Группы: [{"name":"Учёт","members":[0,2]},{"name":"Прочее","members":[1]}]"#;
        let groups = L1Actor::parse_grouping_response(resp, &atoms);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0, "Учёт");
        assert_eq!(groups[0].1, vec![atoms[0].id.clone(), atoms[2].id.clone()]);
        assert_eq!(groups[1].1, vec![atoms[1].id.clone()]);
    }

    #[test]
    fn llm_parse_malformed_returns_empty() {
        let atoms = vec![Node::new(NodeType::Atom, "x")];
        assert!(L1Actor::parse_grouping_response("no json here", &atoms).is_empty());
        assert!(L1Actor::parse_grouping_response("[garbage", &atoms).is_empty());
    }
}
