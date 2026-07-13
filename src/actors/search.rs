//! SearchActor -- vector search + ranking.
//!
//! Based on TECH-SPEC.md Section 9: Vector Search (usearch default, MemoryIndex fallback).
//! SearchActor обеспечивает:
//!   - Векторный поиск по embedding'ам узлов
//!   - Фильтрация по метаданным (level, status, tags, workspace_id)
//!   - Гибридный поиск (vector + keyword BM25)
//!   - Ranking с учётом confidence, recency, relevance

use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::graph::{Level, Node, NodeId, NodeType, Status, Metadata};
use crate::persistence::StorageBackend;

use super::Actor;
use super::EmbeddingProvider;

const EMBEDDING_DIM: usize = 384; // default dimension

/// Фильтры для поиска
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchFilters {
    pub level: Option<Level>,
    pub status: Option<Status>,
    pub node_type: Option<NodeType>,
    pub tags: Vec<String>,
    pub workspace_id: Option<String>,
}

/// Результат поиска
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub node_id: NodeId,
    pub score: f32,
    pub node_type: NodeType,
    pub level: Level,
    pub content: String,
    pub metadata: Metadata,
}

/// Запрос на поиск
#[derive(Debug, Clone)]
pub struct SearchQuery {
    /// Вектор запроса (embedding)
    pub query_vector: Vec<f32>,
    /// Количество результатов
    pub top_k: usize,
    /// Фильтры
    pub filters: SearchFilters,
}

/// Векторизованный узел
#[derive(Debug, Clone)]
pub struct VectorNode {
    pub node: Node,
    pub embedding: Vec<f32>,
}

impl VectorNode {
    pub fn new(node: Node, embedding: Vec<f32>) -> Self {
        Self { node, embedding }
    }
}

/// Trait для векторного индекса
#[async_trait]
pub trait VectorIndex: Send + Sync {
    /// Добавить/обновить вектор
    async fn upsert(&mut self, id: &NodeId, embedding: &[f32], metadata: &Metadata) -> anyhow::Result<()>;
    
    /// Поиск ближайших соседей
    async fn search(
        &self,
        query: &[f32],
        top_k: usize,
        filters: Option<&SearchFilters>,
    ) -> anyhow::Result<Vec<(NodeId, f32)>>;
    
    /// Удалить вектор
    async fn delete(&mut self, id: &NodeId) -> anyhow::Result<()>;
    
    /// Количество векторов
    async fn count(&self) -> anyhow::Result<usize>;
}

/// In-memory векторный индекс (fallback для тестов, cosine similarity)
#[derive(Clone)]
pub struct MemoryIndex {
    /// id -> (embedding, metadata)
    vectors: HashMap<NodeId, (Vec<f32>, Metadata)>,
}

impl MemoryIndex {
    pub fn new() -> Self {
        Self {
            vectors: HashMap::new(),
        }
    }

    /// Cosine similarity между двумя векторами
    fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        if a.is_empty() || b.is_empty() || a.len() != b.len() {
            return 0.0;
        }

        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

        if norm_a < 1e-6 || norm_b < 1e-6 {
            return 0.0;
        }

        dot / (norm_a * norm_b)
    }

    /// Проверить, удовлетворяет ли метаданные фильтрам
    fn matches_filters(metadata: &Metadata, filters: &SearchFilters) -> bool {
        if let Some(ref ws) = filters.workspace_id {
            if metadata.workspace_id.as_ref() != Some(ws) {
                return false;
            }
        }

        if !filters.tags.is_empty() {
            let has_tag = filters.tags.iter().any(|tag| metadata.tags.contains(tag));
            if !has_tag {
                return false;
            }
        }

        true
    }
}

#[async_trait]
impl VectorIndex for MemoryIndex {
    async fn upsert(&mut self, id: &NodeId, embedding: &[f32], metadata: &Metadata) -> anyhow::Result<()> {
        self.vectors.insert(id.clone(), (embedding.to_vec(), metadata.clone()));
        Ok(())
    }

    async fn search(
        &self,
        query: &[f32],
        top_k: usize,
        filters: Option<&SearchFilters>,
    ) -> anyhow::Result<Vec<(NodeId, f32)>> {
        let mut scores: Vec<(NodeId, f32)> = Vec::new();

        for (id, (embedding, metadata)) in &self.vectors {
            // Применяем фильтры
            if let Some(f) = filters {
                if !Self::matches_filters(metadata, f) {
                    continue;
                }
            }

            let similarity = Self::cosine_similarity(query, embedding);
            scores.push((id.clone(), similarity));
        }

        // Сортируем по убыванию similarity
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Возвращаем top_k
        Ok(scores.into_iter().take(top_k).collect())
    }

    async fn delete(&mut self, id: &NodeId) -> anyhow::Result<()> {
        self.vectors.remove(id);
        Ok(())
    }

    async fn count(&self) -> anyhow::Result<usize> {
        Ok(self.vectors.len())
    }
}

/// SearchActor -- vector search + ranking
pub struct SearchActor {
    backend: Arc<dyn StorageBackend>,
    /// Векторный индекс
    vector_index: RwLock<Box<dyn VectorIndex>>,
    /// Кэш узлов (id -> node)
    node_cache: RwLock<HashMap<NodeId, Node>>,
    /// Провайдер эмбеддингов (опционально; без него — char-bag fallback).
    embedding: Option<EmbeddingProvider>,
}

impl SearchActor {
    pub fn new(backend: Arc<dyn StorageBackend>, vector_index: Box<dyn VectorIndex>) -> Self {
        Self {
            backend,
            vector_index: RwLock::new(vector_index),
            node_cache: RwLock::new(HashMap::new()),
            embedding: None,
        }
    }

    /// Создать SearchActor с in-memory индексом (для тестов)
    pub fn with_memory_index(backend: Arc<dyn StorageBackend>) -> Self {
        Self::new(backend, Box::new(MemoryIndex::new()))
    }

    /// Подключить реальный провайдер эмбеддингов (пункт 1.1): `embed_text` начнёт
    /// звать модель вместо char-bag заглушки. При выключенном провайдере — fallback.
    pub fn with_embedding_provider(mut self, provider: EmbeddingProvider) -> Self {
        self.embedding = Some(provider);
        self
    }

    /// Эмбеддинг текста: реальный провайдер (если включён), иначе char-bag fallback.
    /// Единая точка — используется и индексацией (load_all_nodes/index-on-propose),
    /// и запросом (vector_search), так что вектор запроса и векторы узлов сопоставимы.
    pub async fn embed_text(&self, text: &str) -> Vec<f32> {
        if let Some(p) = self.embedding.as_ref().filter(|p| p.is_enabled()) {
            match p.embed(text).await {
                Ok(v) => return v,
                Err(e) => tracing::warn!("SearchActor: embed не удался ({}), char-bag fallback", e),
            }
        }
        self.generate_dummy_embedding(text)
    }

    /// Метка активного эмбеддинг-бэкенда (для честных ответов инструментов).
    pub fn embedding_backend_label(&self) -> &'static str {
        match self.embedding.as_ref() {
            Some(p) => p.backend_label(),
            None => "char_bag_fallback",
        }
    }

    /// Добавить узел с embedding'ом
    pub async fn index_node(&self, node: &Node, embedding: &[f32]) -> anyhow::Result<()> {
        // Сохраняем в кэш
        self.node_cache.write().await.insert(node.id.clone(), node.clone());

        // Добавляем в векторный индекс
        self.vector_index
            .write()
            .await
            .upsert(&node.id, embedding, &node.metadata)
            .await?;

        Ok(())
    }

    /// Удалить узел из индекса
    pub async fn remove_node(&self, node_id: &NodeId) -> anyhow::Result<()> {
        self.node_cache.write().await.remove(node_id);
        self.vector_index.write().await.delete(node_id).await?;
        Ok(())
    }

    /// Векторный поиск
    pub async fn search(&self, query: &SearchQuery) -> anyhow::Result<Vec<SearchResult>> {
        let results = self.vector_index
            .read()
            .await
            .search(&query.query_vector, query.top_k, Some(&query.filters))
            .await?;

        let mut search_results = Vec::new();
        for (node_id, score) in results {
            // Пытаемся получить узел из кэша
            let node = if let Some(n) = self.node_cache.read().await.get(&node_id) {
                n.clone()
            } else {
                continue;
            };

            // Дополнительные фильтры (level/status/node_type) — MemoryIndex
            // фильтрует только по workspace_id/tags в Metadata, эти поля — в Node.
            if let Some(level) = query.filters.level {
                if node.level != level {
                    continue;
                }
            }
            if let Some(ref status) = query.filters.status {
                if node.status != *status {
                    continue;
                }
            }
            if let Some(ref node_type) = query.filters.node_type {
                if node.node_type != *node_type {
                    continue;
                }
            }

            search_results.push(SearchResult {
                node_id,
                score,
                node_type: node.node_type,
                level: node.level,
                content: node.content,
                metadata: node.metadata,
            });
        }

        Ok(search_results)
    }

    /// Гибридный поиск (vector + keyword BM25)
    pub async fn hybrid_search(
        &self,
        query_vector: &[f32],
        keywords: &[String],
        top_k: usize,
        filters: SearchFilters,
    ) -> anyhow::Result<Vec<SearchResult>> {
        // 1. Векторный поиск
        let vector_query = SearchQuery {
            query_vector: query_vector.to_vec(),
            top_k: top_k * 2, // берём больше для гибридного ранжирования
            filters: filters.clone(),
        };
        let vector_results = self.search(&vector_query).await?;

        // 2. Keyword поиск (BM25-style scoring)
        let keyword_results = self.keyword_search(keywords, top_k * 2, &filters).await?;

        // 3. Объединяем результаты с весами (0.7 vector + 0.3 keyword)
        let mut combined_scores: HashMap<NodeId, f32> = HashMap::new();
        let mut node_map: HashMap<NodeId, SearchResult> = HashMap::new();

        for result in vector_results {
            *combined_scores.entry(result.node_id.clone()).or_insert(0.0) += result.score * 0.7;
            node_map.insert(result.node_id.clone(), result);
        }

        for result in keyword_results {
            *combined_scores.entry(result.node_id.clone()).or_insert(0.0) += result.score * 0.3;
            node_map.insert(result.node_id.clone(), result);
        }

        // 4. Сортируем и возвращаем top_k
        let mut ranked: Vec<(NodeId, f32)> = combined_scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let final_results = ranked
            .into_iter()
            .take(top_k)
            .filter_map(|(id, score)| {
                node_map.remove(&id).map(|mut result| {
                    result.score = score;
                    result
                })
            })
            .collect();

        Ok(final_results)
    }

    /// Keyword поиск (упрощённый TF-IDF style)
    ///
    /// Публичный метод, чтобы MCP-инструмент `search_nodes` мог
    /// использовать его напрямую без EmbeddingProvider.
    pub async fn keyword_search(
        &self,
        keywords: &[String],
        top_k: usize,
        filters: &SearchFilters,
    ) -> anyhow::Result<Vec<SearchResult>> {
        let mut scores: Vec<(NodeId, f32, Node)> = Vec::new();

        for (node_id, node) in self.node_cache.read().await.iter() {
            // Фильтр по уровню (L0/L1/L2)
            if let Some(level) = filters.level {
                if node.level != level {
                    continue;
                }
            }

            // Фильтр по статусу
            if let Some(ref status) = filters.status {
                if node.status != *status {
                    continue;
                }
            }

            // Фильтр по типу узла
            if let Some(ref node_type) = filters.node_type {
                if node.node_type != *node_type {
                    continue;
                }
            }

            if let Some(ref ws) = filters.workspace_id {
                if node.metadata.workspace_id.as_ref() != Some(ws) {
                    continue;
                }
            }

            if !filters.tags.is_empty() {
                let has_tag = filters.tags.iter().any(|tag| node.metadata.tags.contains(tag));
                if !has_tag {
                    continue;
                }
            }

            // Считаем score на основе совпадения keywords в content
            let content_lower = node.content.to_lowercase();
            let match_count = keywords.iter().filter(|kw| content_lower.contains(&kw.to_lowercase())).count();
            let score = if keywords.is_empty() {
                0.0
            } else {
                match_count as f32 / keywords.len() as f32
            };

            if score > 0.0 {
                scores.push((
                    node_id.clone(),
                    score,
                    node.clone(),
                ));
            }
        }

        // Сортируем по убыванию score
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        Ok(scores
            .into_iter()
            .take(top_k)
            .map(|(id, score, node)| SearchResult {
                node_id: id,
                score,
                node_type: node.node_type,
                level: node.level,
                content: node.content,
                metadata: node.metadata,
            })
            .collect())
    }

    /// Получить статистику поиска
    pub async fn stats(&self) -> anyhow::Result<SearchStats> {
        let vector_count = self.vector_index.read().await.count().await?;
        let cache_count = self.node_cache.read().await.len();

        Ok(SearchStats {
            vector_count,
            cache_count,
        })
    }

    /// Очистить кэш
    pub async fn clear_cache(&self) {
        self.node_cache.write().await.clear();
    }

    /// Загрузить все узлы из backend при старте.
    /// Читает L2-атомы, L1-домены и L0-кластеры — у L1/L0 есть поле `level`,
    /// у L2 его нет (по умолчанию `Level::L2`).
    pub async fn load_all_nodes(&self) -> anyhow::Result<usize> {
        use crate::graph::StoredNode;

        let node_keys = self.backend.list_keys("node:").await?;
        let mut loaded_count = 0;

        for key in node_keys {
            if let Some(data) = self.backend.get(&key).await? {
                // Десериализуем как Value, чтобы извлечь level (L1/L0 имеют level, L2 — нет)
                let Ok(value) = serde_json::from_slice::<serde_json::Value>(&data) else {
                    continue;
                };
                let level = value
                    .get("level")
                    .and_then(|v| serde_json::from_value::<Level>(v.clone()).ok())
                    .unwrap_or(Level::L2);

                // Общие поля (id, node_type, content, workspace_id, tags, created_at)
                let Ok(stored) = serde_json::from_value::<StoredNode>(value) else {
                    continue;
                };
                let node = Node {
                    id: stored.id.clone(),
                    node_type: stored.node_type,
                    level,
                    content: stored.content.clone(),
                    metadata: Metadata {
                        parent_id: stored.parent_id,
                        tags: stored.tags,
                        workspace_id: stored.workspace_id,
                    },
                    status: Status::Active,
                    created_at: stored.created_at,
                    updated_at: stored.created_at,
                };

                let embedding = self.embed_text(&node.content).await;
                self.index_node(&node, &embedding).await?;
                loaded_count += 1;
            }
        }

        tracing::info!("SearchActor loaded {} nodes from backend", loaded_count);
        Ok(loaded_count)
    }
    
    /// Сгенерировать embedding для произвольного текста.
    ///
    /// Используется в `vector_search` tool, когда `EmbeddingProvider` не подключён
    /// (V2.0 fallback на char-bag embedding, как в `load_all_nodes`). В V2.1+
    /// будет заменено на реальный `EmbeddingProvider::embed()`.
    pub fn generate_dummy_embedding(&self, content: &str) -> Vec<f32> {
        // Простой хеш на основе символов для детерминизма
        let mut embedding = vec![0.0f32; EMBEDDING_DIM];
        let bytes = content.as_bytes();
        
        for (i, &byte) in bytes.iter().enumerate() {
            let idx = i % EMBEDDING_DIM;
            embedding[idx] += (byte as f32) / 255.0;
        }
        
        // Нормализуем (простая L2 нормализация)
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in embedding.iter_mut() {
                *x /= norm;
            }
        }
        
        embedding
    }
}

/// Статистика SearchActor
#[derive(Debug, Clone)]
pub struct SearchStats {
    pub vector_count: usize,
    pub cache_count: usize,
}

#[async_trait]
impl Actor for SearchActor {
    fn name(&self) -> &str {
        "SearchActor"
    }

    async fn size(&self) -> usize {
        self.vector_index.read().await.count().await.unwrap_or(0)
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

    fn make_node(content: &str, node_type: NodeType, level: Level, workspace: Option<&str>) -> Node {
        let mut n = Node::new(node_type, content);
        n.level = level;
        n.metadata.workspace_id = workspace.map(String::from);
        n.created_at = Utc::now();
        n
    }

    fn make_embedding(seed: f32) -> Vec<f32> {
        (0..EMBEDDING_DIM).map(|i| ((i as f32 + seed) * 0.01).sin()).collect()
    }

    #[tokio::test]
    async fn test_cosine_similarity_identical() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let sim = MemoryIndex::cosine_similarity(&a, &b);
        assert!((sim - 1.0).abs() < 1e-5);
    }

    #[tokio::test]
    async fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let sim = MemoryIndex::cosine_similarity(&a, &b);
        assert!(sim.abs() < 1e-5);
    }

    #[tokio::test]
    async fn test_index_and_search() {
        let actor = SearchActor::with_memory_index(backend());

        let node1 = make_node("Clerk authentication", NodeType::Atom, Level::L2, Some("ws1"));
        let node2 = make_node("PostgreSQL database", NodeType::Atom, Level::L2, Some("ws1"));
        let node3 = make_node("Clerk middleware", NodeType::Atom, Level::L2, Some("ws2"));

        let emb1 = make_embedding(1.0);
        let emb2 = make_embedding(2.0);
        let emb3 = make_embedding(1.1); // похож на emb1

        actor.index_node(&node1, &emb1).await.unwrap();
        actor.index_node(&node2, &emb2).await.unwrap();
        actor.index_node(&node3, &emb3).await.unwrap();

        // Ищем по query, похожему на emb1
        let query = SearchQuery {
            query_vector: make_embedding(1.0),
            top_k: 2,
            filters: SearchFilters::default(),
        };

        let results = actor.search(&query).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].node_id, node1.id); // самый близкий
    }

    #[tokio::test]
    async fn test_search_with_filters() {
        let actor = SearchActor::with_memory_index(backend());

        let node1 = make_node("Clerk auth", NodeType::Atom, Level::L2, Some("ws1"));
        let node2 = make_node("PostgreSQL", NodeType::Atom, Level::L2, Some("ws2"));

        actor.index_node(&node1, &make_embedding(1.0)).await.unwrap();
        actor.index_node(&node2, &make_embedding(2.0)).await.unwrap();

        let query = SearchQuery {
            query_vector: make_embedding(1.0),
            top_k: 5,
            filters: SearchFilters {
                workspace_id: Some("ws1".to_string()),
                ..Default::default()
            },
        };

        let results = actor.search(&query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].node_id, node1.id);
    }

    #[tokio::test]
    async fn test_keyword_search() {
        let actor = SearchActor::with_memory_index(backend());

        let node1 = make_node("Clerk authentication setup", NodeType::Atom, Level::L2, Some("ws1"));
        let node2 = make_node("PostgreSQL database", NodeType::Atom, Level::L2, Some("ws1"));

        actor.index_node(&node1, &make_embedding(1.0)).await.unwrap();
        actor.index_node(&node2, &make_embedding(2.0)).await.unwrap();

        let results = actor.keyword_search(&["clerk".to_string()], 5, &SearchFilters::default()).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].node_id, node1.id);
    }

    #[tokio::test]
    async fn test_hybrid_search() {
        let actor = SearchActor::with_memory_index(backend());

        let node1 = make_node("Clerk authentication setup", NodeType::Atom, Level::L2, Some("ws1"));
        let node2 = make_node("PostgreSQL database", NodeType::Atom, Level::L2, Some("ws1"));

        actor.index_node(&node1, &make_embedding(1.0)).await.unwrap();
        actor.index_node(&node2, &make_embedding(2.0)).await.unwrap();

        let results = actor.hybrid_search(
            &make_embedding(1.0),
            &["clerk".to_string()],
            5,
            SearchFilters::default(),
        ).await.unwrap();

        // Гибридный поиск возвращает оба узла (оба проходят фильтрацию),
        // но node1 имеет более высокий score из-за keyword matching
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].node_id, node1.id); // clerk keyword match
    }

    #[tokio::test]
    async fn test_remove_node() {
        let actor = SearchActor::with_memory_index(backend());

        let node = make_node("Test", NodeType::Atom, Level::L2, Some("ws1"));
        actor.index_node(&node, &make_embedding(1.0)).await.unwrap();

        let count_before = actor.stats().await.unwrap().vector_count;
        assert_eq!(count_before, 1);

        actor.remove_node(&node.id).await.unwrap();

        let count_after = actor.stats().await.unwrap().vector_count;
        assert_eq!(count_after, 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let actor = SearchActor::with_memory_index(backend());

        for i in 0..5 {
            let node = make_node(&format!("Node {}", i), NodeType::Atom, Level::L2, Some("ws1"));
            actor.index_node(&node, &make_embedding(i as f32)).await.unwrap();
        }

        let stats = actor.stats().await.unwrap();
        assert_eq!(stats.vector_count, 5);
        assert_eq!(stats.cache_count, 5);
    }

    #[tokio::test]
    async fn test_clear_cache() {
        let actor = SearchActor::with_memory_index(backend());

        for i in 0..3 {
            let node = make_node(&format!("Node {}", i), NodeType::Atom, Level::L2, Some("ws1"));
            actor.index_node(&node, &make_embedding(i as f32)).await.unwrap();
        }

        actor.clear_cache().await;

        let stats = actor.stats().await.unwrap();
        assert_eq!(stats.cache_count, 0);
        assert_eq!(stats.vector_count, 3); // векторы остались
    }

    #[tokio::test]
    async fn test_search_with_workspace_filter() {
        let actor = SearchActor::with_memory_index(backend());

        let node1 = make_node("WS1 node", NodeType::Atom, Level::L2, Some("ws1"));
        let node2 = make_node("WS2 node", NodeType::Atom, Level::L2, Some("ws2"));

        actor.index_node(&node1, &make_embedding(1.0)).await.unwrap();
        actor.index_node(&node2, &make_embedding(2.0)).await.unwrap();

        let query = SearchQuery {
            query_vector: make_embedding(1.0),
            top_k: 5,
            filters: SearchFilters {
                workspace_id: Some("ws1".to_string()),
                ..Default::default()
            },
        };

        let results = actor.search(&query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].node_id, node1.id);
    }

    #[tokio::test]
    async fn test_search_with_tag_filter() {
        let actor = SearchActor::with_memory_index(backend());

        let mut node1 = make_node("Tagged node", NodeType::Atom, Level::L2, Some("ws1"));
        node1.metadata.tags = vec!["rust".to_string(), "framework".to_string()];
        let node2 = make_node("Untagged node", NodeType::Atom, Level::L2, Some("ws1"));

        actor.index_node(&node1, &make_embedding(1.0)).await.unwrap();
        actor.index_node(&node2, &make_embedding(2.0)).await.unwrap();

        let query = SearchQuery {
            query_vector: make_embedding(1.0),
            top_k: 5,
            filters: SearchFilters {
                tags: vec!["rust".to_string()],
                ..Default::default()
            },
        };

        let results = actor.search(&query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].node_id, node1.id);
    }

    #[tokio::test]
    async fn test_memory_index_matches_filters() {
        let metadata = Metadata {
            parent_id: None,
            workspace_id: Some("ws1".to_string()),
            tags: vec!["rust".to_string(), "test".to_string()],
        };
        let filters = SearchFilters {
            workspace_id: Some("ws1".to_string()),
            tags: vec!["rust".to_string()],
            ..Default::default()
        };

        assert!(MemoryIndex::matches_filters(&metadata, &filters));

        let wrong_ws = SearchFilters {
            workspace_id: Some("ws2".to_string()),
            ..Default::default()
        };

        assert!(!MemoryIndex::matches_filters(&metadata, &wrong_ws));
    }
}
