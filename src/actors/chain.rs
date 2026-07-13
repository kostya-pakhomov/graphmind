// ChainActor - обход причинных цепочек (backward/forward-pre/forward-post)

use crate::actors::l2::L2Actor;
use crate::graph::{Edge, NodeId, NodeType, Relation};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Запись о прохождении цепочки
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainEntry {
    pub node_id: NodeId,
    pub depth: usize,
    pub edge: Option<Edge>,
    pub relation: Option<Relation>,
}

/// Результат обхода цепочки
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainResult {
    pub entries: Vec<ChainEntry>,
    pub reached_root: bool,
    pub max_depth_reached: bool,
}

/// ChainActor - обход причинных цепочек
pub struct ChainActor {
    l2: Arc<L2Actor>,
    /// Кэш пройденных узлов для избежания циклов
    visited: RwLock<HashSet<NodeId>>,
}

impl ChainActor {
    pub fn new(l2: Arc<L2Actor>) -> Self {
        Self {
            l2,
            visited: RwLock::new(HashSet::new()),
        }
    }

    /// Backward обход: от симптома к причине (поиск по тексту симптома)
    ///
    /// Находит effect-узлы по тексту, затем делегирует в chain_backward_from_node.
    pub async fn chain_backward(
        &self,
        symptom_text: &str,
        max_depth: usize,
    ) -> Result<ChainResult> {
        let effect_nodes = self.find_effects_by_symptom(symptom_text).await?;
        if effect_nodes.is_empty() {
            return Ok(ChainResult {
                entries: Vec::new(),
                reached_root: false,
                max_depth_reached: false,
            });
        }
        // Объединяем результаты backward-обхода от каждого найденного effect-узла
        let mut combined = ChainResult {
            entries: Vec::new(),
            reached_root: false,
            max_depth_reached: false,
        };
        for node_id in &effect_nodes {
            let partial = self.chain_backward_from_node(node_id, max_depth).await?;
            if partial.reached_root {
                combined.reached_root = true;
            }
            if partial.max_depth_reached {
                combined.max_depth_reached = true;
            }
            for entry in partial.entries {
                if !combined.entries.iter().any(|e| e.node_id == entry.node_id) {
                    combined.entries.push(entry);
                }
            }
        }
        Ok(combined)
    }

    /// Backward обход: от конкретного узла к корневой причине
    ///
    /// Идёт двумя путями одновременно:
    ///   1. Исходящие ExplainedBy рёбра (effect → cause)
    ///   2. Входящие LeadsTo рёбра (effect ← cause, т.е. cause → effect в обратном направлении)
    pub async fn chain_backward_from_node(
        &self,
        start_node_id: &NodeId,
        max_depth: usize,
    ) -> Result<ChainResult> {
        let mut result = ChainResult {
            entries: Vec::new(),
            reached_root: false,
            max_depth_reached: false,
        };

        let mut queue: VecDeque<(NodeId, usize)> = VecDeque::new();
        queue.push_back((start_node_id.clone(), 0));

        let mut visited = HashSet::new();

        while let Some((node_id, depth)) = queue.pop_front() {
            if depth > max_depth {
                result.max_depth_reached = true;
                continue;
            }

            if visited.contains(&node_id) {
                continue;
            }
            visited.insert(node_id.clone());

            if let Some(node) = self.l2.get_node(&node_id).await? {
                result.entries.push(ChainEntry {
                    node_id: node_id.clone(),
                    depth,
                    edge: None,
                    relation: None,
                });

                // Cause-узел — корень цепочки
                if node.node_type == NodeType::Cause {
                    result.reached_root = true;
                    continue;
                }

                // Путь 1: исходящие ExplainedBy (effect → cause)
                let out_edges = self.l2.edges_from(&node_id).await?;
                for edge in out_edges {
                    if matches!(edge.relation, Relation::ExplainedBy) {
                        queue.push_back((edge.target.clone(), depth + 1));
                    }
                }

                // Путь 2: входящие LeadsTo (cause → effect, идём backward от effect к cause)
                let in_edges = self.l2.edges_to(&node_id).await?;
                for edge in in_edges {
                    if matches!(edge.relation, Relation::LeadsTo) {
                        queue.push_back((edge.source.clone(), depth + 1));
                    }
                }
            }
        }

        Ok(result)
    }

    /// Forward-pre обход: от причины к рискам (что может сломаться)
    /// 
    /// Идём от cause через leads_to к effect
    pub async fn chain_forward_pre(
        &self,
        cause_id: &NodeId,
        max_depth: usize,
    ) -> Result<ChainResult> {
        let mut result = ChainResult {
            entries: Vec::new(),
            reached_root: false,
            max_depth_reached: false,
        };

        // Проверяем, что начальный узел - cause
        if let Some(node) = self.l2.get_node(cause_id).await? {
            if node.node_type != NodeType::Cause {
                return Err(anyhow::anyhow!(
                    "Node {:?} is not a cause node",
                    cause_id.0
                ));
            }

            result.entries.push(ChainEntry {
                node_id: cause_id.clone(),
                depth: 0,
                edge: None,
                relation: None,
            });

            // BFS от cause через leads_to
            let mut queue: VecDeque<(NodeId, usize)> = VecDeque::new();
            queue.push_back((cause_id.clone(), 0));

            let mut visited = HashSet::new();
            visited.insert(cause_id.clone());

            while let Some((node_id, depth)) = queue.pop_front() {
                if depth >= max_depth {
                    result.max_depth_reached = true;
                    continue;
                }

                let edges = self.l2.edges_from(&node_id).await?;
                for edge in edges {
                    if matches!(edge.relation, Relation::LeadsTo) {
                        if !visited.contains(&edge.target) {
                            visited.insert(edge.target.clone());

                            if let Some(target_node) = self.l2.get_node(&edge.target).await? {
                                result.entries.push(ChainEntry {
                                    node_id: edge.target.clone(),
                                    depth: depth + 1,
                                    edge: Some(edge.clone()),
                                    relation: Some(edge.relation.clone()),
                                });

                                // Если это effect - добавляем в очередь для дальнейшего обхода
                                if target_node.node_type == NodeType::Effect {
                                    queue.push_back((edge.target.clone(), depth + 1));
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Forward-post обход: от действия к проверенным последствиям
    /// 
    /// Проверяем, какие ожидаемые effects действительно наблюдались
    pub async fn chain_forward_post(
        &self,
        action_node_id: &NodeId,
        max_depth: usize,
    ) -> Result<ChainResult> {
        // Сначала идём как forward-pre
        let pre_result = self.chain_forward_pre(action_node_id, max_depth).await?;

        // Затем проверяем каждый effect на наличие подтверждений
        let mut verified_entries = Vec::new();
        for entry in &pre_result.entries {
            if let Some(node) = self.l2.get_node(&entry.node_id).await? {
                // Проверяем, есть ли у effect predicted=true и observed_in
                if node.node_type == NodeType::Effect {
                    // В реальной реализации здесь была бы проверка в persistence
                    // на наличие фактических наблюдений
                    verified_entries.push(entry.clone());
                } else {
                    verified_entries.push(entry.clone());
                }
            }
        }

        Ok(ChainResult {
            entries: verified_entries,
            reached_root: pre_result.reached_root,
            max_depth_reached: pre_result.max_depth_reached,
        })
    }

    /// Post-mortem обход: полная цепочка до корневой причины
    pub async fn chain_post_mortem(
        &self,
        symptom_text: &str,
    ) -> Result<ChainResult> {
        // Backward с max_depth=usize::MAX
        self.chain_backward(symptom_text, usize::MAX).await
    }

    /// Поиск effect-узлов по тексту симптома
    async fn find_effects_by_symptom(&self, symptom_text: &str) -> Result<Vec<NodeId>> {
        let mut results = Vec::new();
        
        // Получляем все узлы типа effect
        let effect_nodes = self.l2.list_by_type(NodeType::Effect).await?;
        
        for node in effect_nodes {
            // Проверяем content
            if node.content.to_lowercase().contains(&symptom_text.to_lowercase()) {
                results.push(node.id.clone());
                continue;
            }
            
            // Проверяем causal.symptom если это effect
            // В реальной реализации нужно парсить JSON content
        }
        
        Ok(results)
    }

    /// Очистка кэша посещённых узлов
    pub async fn clear_visited(&self) {
        let mut visited = self.visited.write().await;
        visited.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::l2::L2Actor;
    use crate::graph::NodeType;
    use crate::persistence::InMemoryBackend;
    use std::sync::Arc;

    async fn create_test_chain_actor() -> ChainActor {
        let backend = Arc::new(InMemoryBackend::new());
        let l2 = Arc::new(L2Actor::new(backend));
        ChainActor::new(l2)
    }

    #[tokio::test]
    async fn test_chain_backward_empty() {
        let actor = create_test_chain_actor().await;
        let result = actor.chain_backward("test symptom", 3).await.unwrap();
        assert!(result.entries.is_empty());
        assert!(!result.reached_root);
    }

    #[tokio::test]
    async fn test_chain_forward_pre_from_non_cause() {
        let actor = create_test_chain_actor().await;
        
        // Создаём не-cause узел через L2Actor
        let node_id = actor.l2.add_node(
            &crate::graph::Node::new(NodeType::Atom, "test atom"),
        ).await.unwrap();
        
        let result = actor.chain_forward_pre(&node_id, 3).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_chain_post_mortem_unlimited_depth() {
        let actor = create_test_chain_actor().await;
        let result = actor.chain_post_mortem("production outage").await.unwrap();
        // Пока нет данных - пусто
        assert!(result.entries.is_empty());
    }
}
