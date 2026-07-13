//! Обработчики gRPC методов MemoryService.
//!
//! Каждый RPC метод диспетчеризирует вызов к соответствующему актору.

use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{info, warn, error};
use uuid::Uuid;
use chrono::Utc;

use crate::actors::{S0Actor, S0Entry, L2Actor, L1Actor, L0Actor, GKLactor, SearchActor, ChainActor, CausalEngine, InferenceActor, SearchFilters, WorkspaceManager, WorkspaceStatus, CuriosityEngine, TrustFirewall};
use crate::graph::{Graph, Node as GraphNode, NodeType, Level, Metadata, Status as GraphStatus, Edge, Relation, EdgeId, NodeId, Provenance};
use crate::graphmind::memory_service_server::MemoryService;

// Импортируем сгенерированные типы из proto
use crate::graphmind::{
    // Requests
    RecordActionRequest, GetS0ContextRequest, FlushSessionMemoryRequest,
    ProposeNewMemoryRequest, UpdateNodeRequest, FetchL2AtomsRequest,
    ArchiveNodeRequest, RestoreNodeRequest, SearchNodesRequest,
    GetChainRequest, LinkNodesRequest, ProposeCausalLinkRequest,
    FindContradictionsRequest, PredictRisksRequest, DreamReflectionRequest,
    VectorSearchRequest, MemoryQueryRequest, SuggestRelatedRequest,
    DetectWorkspaceFromContextRequest, CreateWorkspaceRequest, SwitchWorkspaceRequest,
    ListWorkspacesRequest, ArchiveWorkspaceRequest,
    FetchFromWorkspaceRequest, SuggestCrossWorkspaceLinksRequest, FindWorkspaceOverlapsRequest,
    ConsolidateWorkspaceRequest, SearchL0ClustersRequest, RouteL1Request,
    BootstrapMemoryRequest, GetIndexStatusRequest,
    VerifyInputRequest, GetIrritationReportRequest, ListCuriosityTasksRequest,
    CloseCuriosityTaskRequest, GenerateVerificationQuestionsRequest,
    CompareVerificationResponseRequest, FinalizeVerificationRequest,
    // Responses
    RecordActionResponse, GetS0ContextResponse, FlushSessionMemoryResponse,
    ProposeNewMemoryResponse, UpdateNodeResponse, FetchL2AtomsResponse,
    ArchiveNodeResponse, RestoreNodeResponse, SearchNodesResponse,
    GetChainResponse, LinkNodesResponse, ProposeCausalLinkResponse,
    FindContradictionsResponse, PredictRisksResponse, DreamReflectionResponse,
    VectorSearchResponse, MemoryQueryResponse, SuggestRelatedResponse,
    DetectWorkspaceFromContextResponse, CreateWorkspaceResponse, SwitchWorkspaceResponse,
    ListWorkspacesResponse, ArchiveWorkspaceResponse,
    FetchFromWorkspaceResponse, SuggestCrossWorkspaceLinksResponse, FindWorkspaceOverlapsResponse,
    ConsolidateWorkspaceResponse, SearchL0ClustersResponse, RouteL1Response,
    BootstrapMemoryResponse, GetIndexStatusResponse,
    VerifyInputResponse, GetIrritationReportResponse, ListCuriosityTasksResponse,
    CloseCuriosityTaskResponse, GenerateVerificationQuestionsResponse,
    CompareVerificationResponseResponse, FinalizeVerificationResponse,
    // Types
    Action,
    Node as ProtoNode,
};

/// Convert Graph Node to proto Node
fn proto_node_from_graph_node(node: &GraphNode) -> ProtoNode {
    ProtoNode {
        id: node.id.0.clone(),
        node_type: node_type_to_proto(node.node_type),
        content: node.content.clone(),
        level: level_to_proto(node.level),
        scope: String::new(), // TODO: from metadata
        status: status_to_proto(node.status),
        tags: node.metadata.tags.clone(),
        summary: String::new(), // TODO: from metadata or content
        parent_id: node.metadata.workspace_id.clone().unwrap_or_default(),
        causal: None, // TODO: from node metadata if causal type
        created_at: node.created_at.timestamp() as i64,
        updated_at: node.updated_at.timestamp() as i64,
    }
}

fn node_type_to_proto(t: NodeType) -> String {
    match t {
        NodeType::Atom => "atom".to_string(),
        NodeType::Cause => "cause".to_string(),
        NodeType::Effect => "effect".to_string(),
        NodeType::Rule => "rule".to_string(),
        NodeType::Cluster => "cluster".to_string(),
        NodeType::Hub => "hub".to_string(),
        NodeType::Domain => "domain".to_string(),
    }
}

fn level_to_proto(l: Level) -> String {
    match l {
        Level::S0 => "S0".to_string(),
        Level::L2 => "L2".to_string(),
        Level::L1 => "L1".to_string(),
        Level::L0 => "L0".to_string(),
        Level::GKL => "GKL".to_string(),
    }
}

fn status_to_proto(s: GraphStatus) -> String {
    match s {
        GraphStatus::Active => "active".to_string(),
        GraphStatus::Draft => "draft".to_string(),
        GraphStatus::Archived => "archived".to_string(),
    }
}

fn relation_to_proto(r: &Relation) -> String {
    match r {
        Relation::RelatedTo => "related_to".to_string(),
        Relation::DependsOn => "depends_on".to_string(),
        Relation::Implements => "implements".to_string(),
        Relation::Supersedes => "supersedes".to_string(),
        Relation::LeadsTo => "leads_to".to_string(),
        Relation::ExplainedBy => "explained_by".to_string(),
        Relation::DerivedFrom => "derived_from".to_string(),
        Relation::Inhibits => "inhibits".to_string(),
        Relation::Contradicts => "contradicts".to_string(),
    }
}

/// Обработчик всех MCP инструментов.
///
/// Хранит Arc к каждому актору для диспетчеризации вызовов.
pub struct MemoryServiceHandler {
    s0: Arc<S0Actor>,
    l2: Arc<L2Actor>,
    l1: Arc<L1Actor>,
    l0: Arc<L0Actor>,
    gkl: Arc<GKLactor>,
    search: Arc<SearchActor>,
    chain: Arc<ChainActor>,
    causal_engine: Arc<CausalEngine>,
    inference: Arc<InferenceActor>,
    workspace_manager: Arc<WorkspaceManager>,
    curiosity_engine: Arc<CuriosityEngine>,
    trust_firewall: Arc<TrustFirewall>,
    graph: Arc<RwLock<Graph>>,
}

impl MemoryServiceHandler {
    pub fn new(
        s0: Arc<S0Actor>,
        l2: Arc<L2Actor>,
        l1: Arc<L1Actor>,
        l0: Arc<L0Actor>,
        gkl: Arc<GKLactor>,
        search: Arc<SearchActor>,
        chain: Arc<ChainActor>,
        causal_engine: Arc<CausalEngine>,
        inference: Arc<InferenceActor>,
        workspace_manager: Arc<WorkspaceManager>,
        curiosity_engine: Arc<CuriosityEngine>,
        trust_firewall: Arc<TrustFirewall>,
        graph: Arc<RwLock<Graph>>,
    ) -> Self {
        Self {
            s0, l2, l1, l0, gkl, search, chain, causal_engine, inference, workspace_manager, curiosity_engine, trust_firewall, graph,
        }
    }
}

#[tonic::async_trait]
impl MemoryService for MemoryServiceHandler {
    // ==================== Session Tools ====================

    async fn record_action(
        &self,
        request: Request<RecordActionRequest>,
    ) -> Result<Response<RecordActionResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: record_action(summary={})", req.summary);
        
        // Create S0Entry
        let entry = S0Entry {
            id: Uuid::new_v4().to_string(),
            source: "grpc".to_string(),
            summary: req.summary,
            timestamp: Utc::now(),
        };
        
        // Push to S0
        let evicted = self.s0.push(entry.clone()).await;
        
        // Check if consolidation should be triggered
        let consolidation_triggered = evicted.is_some();
        
        Ok(Response::new(RecordActionResponse {
            id: entry.id,
            consolidation_triggered,
        }))
    }

    async fn get_s0_context(
        &self,
        request: Request<GetS0ContextRequest>,
    ) -> Result<Response<GetS0ContextResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit > 0 { req.limit as usize } else { 20 };
        info!("RPC: get_s0_context(limit={})", limit);
        
        // Get recent entries from S0
        let entries = self.s0.get_recent(limit).await;
        
        // Convert to proto Action messages
        let actions = entries.into_iter().map(|e| Action {
            id: e.id,
            summary: e.summary,
            raw_text: String::new(), // S0 doesn't store raw_text
            related_nodes: vec![],
            causal_context: None,
            timestamp: e.timestamp.timestamp() as i64,
        }).collect();
        
        Ok(Response::new(GetS0ContextResponse { actions }))
    }

    async fn flush_session_memory(
        &self,
        request: Request<FlushSessionMemoryRequest>,
    ) -> Result<Response<FlushSessionMemoryResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: flush_session_memory(force={}, summary={})", req.force, req.summary);
        
        // 1. Записать summary как action в S0
        if !req.summary.is_empty() {
            let entry = S0Entry {
                id: Uuid::new_v4().to_string(),
                source: "grpc-flush".to_string(),
                summary: req.summary,
                timestamp: Utc::now(),
            };
            self.s0.push(entry).await;
        }
        
        // 2. Запустить консолидацию если force=true
        if req.force {
            // В реальной реализации здесь был бы вызов consolidate_workspace
            info!("Force consolidation requested");
        }
        
        Ok(Response::new(FlushSessionMemoryResponse {
            consolidated: true,
            new_l2_count: 0,
        }))
    }

    // ==================== Storage Tools ====================

    async fn propose_new_memory(
        &self,
        request: Request<ProposeNewMemoryRequest>,
    ) -> Result<Response<ProposeNewMemoryResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: propose_new_memory(level={}, type={})", req.level, req.node_type);
        
        // Parse node_type from proto string to graph NodeType
        let node_type = match req.node_type.as_str() {
            "atom" => NodeType::Atom,
            "cause" => NodeType::Cause,
            "effect" => NodeType::Effect,
            "rule" => NodeType::Rule,
            "cluster" => NodeType::Cluster,
            "hub" => NodeType::Hub,
            "domain" => NodeType::Domain,
            _ => return Err(Status::invalid_argument(format!("Unknown node_type: {}", req.node_type))),
        };
        
        // Generate node ID
        let node_id = crate::graph::NodeId(
            if req.id_hint.is_empty() {
                Uuid::new_v4().to_string()
            } else {
                req.id_hint.clone()
            }
        );
        
        // Create Graph Node
        let mut node = GraphNode::new(node_type, req.content.clone());
        node.id = node_id.clone();
        node.level = match req.level.as_str() {
            "L2" => Level::L2,
            "L1" => Level::L1,
            "L0" => Level::L0,
            "GKL" => Level::GKL, // Will need special handling
            _ => Level::L2,
        };
        node.metadata = Metadata {
            parent_id: None, // TODO: from request (cluster parent) — gRPC пока не принимает
            workspace_id: None, // TODO: from active workspace
            tags: req.tags.clone(),
        };
        node.status = match req.status.as_str() {
            "draft" => GraphStatus::Draft,
            "archived" => GraphStatus::Archived,
            _ => GraphStatus::Active,
        };
        
        // Store in L2
        match self.l2.add_node(&node).await {
            Ok(_) => {
                // Also add to graph
                let mut graph = self.graph.write().await;
                graph.add_node(node);
                drop(graph);
                
                Ok(Response::new(ProposeNewMemoryResponse {
                    node_id: node_id.0,
                    level: req.level,
                }))
            }
            Err(e) => Err(Status::internal(format!("Failed to store node: {}", e))),
        }
    }

    async fn update_node(
        &self,
        request: Request<UpdateNodeRequest>,
    ) -> Result<Response<UpdateNodeResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: update_node(node_id={})", req.node_id);
        
        let node_id = crate::graph::NodeId(req.node_id);
        
        // Get existing node
        match self.l2.get_node(&node_id).await {
            Ok(Some(mut node)) => {
                // Apply updates
                if !req.content.is_empty() {
                    node.content = req.content;
                }
                if !req.status.is_empty() {
                    node.status = match req.status.as_str() {
                        "draft" => GraphStatus::Draft,
                        "archived" => GraphStatus::Archived,
                        _ => GraphStatus::Active,
                    };
                }
                if !req.summary.is_empty() {
                    // summary could go to metadata or content depending on use case
                    // For now, we'll keep it simple
                }
                node.updated_at = Utc::now();
                
                // Save updated node
                match self.l2.add_node(&node).await {
                    Ok(_) => {
                        // Update graph as well
                        let mut graph = self.graph.write().await;
                        graph.add_node(node);
                        drop(graph);
                        
                        Ok(Response::new(UpdateNodeResponse { updated: true }))
                    }
                    Err(e) => Err(Status::internal(format!("Failed to update node: {}", e))),
                }
            }
            Ok(None) => Err(Status::not_found(format!("Node {} not found", node_id.0))),
            Err(e) => Err(Status::internal(format!("Failed to fetch node: {}", e))),
        }
    }

    async fn fetch_l2_atoms(
        &self,
        request: Request<FetchL2AtomsRequest>,
    ) -> Result<Response<FetchL2AtomsResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: fetch_l2_atoms(count={})", req.atom_ids.len());
        
        let mut atoms = Vec::with_capacity(req.atom_ids.len());
        
        for id_str in req.atom_ids {
            let node_id = crate::graph::NodeId(id_str);
            match self.l2.get_node(&node_id).await {
                Ok(Some(node)) => {
                    atoms.push(proto_node_from_graph_node(&node));
                }
                Ok(None) => {
                    // Node not found, skip silently
                }
                Err(e) => {
                    warn!("Failed to fetch node {}: {}", node_id.0, e);
                }
            }
        }
        
        Ok(Response::new(FetchL2AtomsResponse { atoms }))
    }

    async fn archive_node(
        &self,
        request: Request<ArchiveNodeRequest>,
    ) -> Result<Response<ArchiveNodeResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: archive_node(node_id={})", req.node_id);
        
        let node_id = crate::graph::NodeId(req.node_id);
        
        // Get existing node
        match self.l2.get_node(&node_id).await {
            Ok(Some(mut node)) => {
                node.status = GraphStatus::Archived;
                node.updated_at = Utc::now();
                
                // Save updated node
                match self.l2.add_node(&node).await {
                    Ok(_) => {
                        // Update graph as well
                        let mut graph = self.graph.write().await;
                        graph.add_node(node);
                        drop(graph);
                        
                        Ok(Response::new(ArchiveNodeResponse { archived: true }))
                    }
                    Err(e) => Err(Status::internal(format!("Failed to archive node: {}", e))),
                }
            }
            Ok(None) => Err(Status::not_found(format!("Node {} not found", node_id.0))),
            Err(e) => Err(Status::internal(format!("Failed to fetch node: {}", e))),
        }
    }

    async fn restore_node(
        &self,
        request: Request<RestoreNodeRequest>,
    ) -> Result<Response<RestoreNodeResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: restore_node(node_id={})", req.node_id);
        
        let node_id = NodeId(req.node_id);
        
        // Получить узел
        match self.l2.get_node(&node_id).await.map_err(|e| Status::internal(format!("Get node failed: {}", e)))? {
            Some(mut node) => {
                node.status = GraphStatus::Active;
                node.updated_at = Utc::now();
                
                // Сохранить обновлённый узел
                match self.l2.add_node(&node).await {
                    Ok(_) => {
                        // Обновить в графе
                        let mut graph = self.graph.write().await;
                        graph.add_node(node);
                        drop(graph);
                        
                        Ok(Response::new(RestoreNodeResponse { restored: true }))
                    }
                    Err(e) => Err(Status::internal(format!("Failed to restore node: {}", e))),
                }
            }
            None => Err(Status::not_found(format!("Node {} not found", node_id.0))),
        }
    }

    async fn search_nodes(
        &self,
        request: Request<SearchNodesRequest>,
    ) -> Result<Response<SearchNodesResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: search_nodes(query={}, limit={})", req.query, req.limit);
        
        // Пока нет векторного индекса - используем keyword поиск через L2Actor
        // В реальной реализации здесь был бы векторный поиск через SearchActor
        
        let top_k = if req.limit > 0 { req.limit as usize } else { 10 };
        
        // Простой поиск по keyword в кэше SearchActor
        let keywords: Vec<String> = req.query.split_whitespace().map(|s| s.to_string()).collect();
        
        // Используем hybrid_search с пустым вектором (fallback на keyword)
        let dummy_vector = vec![0.0f32; 384]; // EMBEDDING_DIM
        let filters = SearchFilters::default();
        
        match self.search.hybrid_search(&dummy_vector, &keywords, top_k, filters).await {
            Ok(results) => {
                let nodes = results.into_iter().map(|r| ProtoNode {
                    id: r.node_id.0,
                    node_type: node_type_to_proto(r.node_type),
                    content: r.content,
                    level: level_to_proto(r.level),
                    scope: String::new(),
                    status: status_to_proto(GraphStatus::Active),
                    tags: r.metadata.tags,
                    summary: String::new(),
                    parent_id: r.metadata.workspace_id.unwrap_or_default(),
                    causal: None,
                    created_at: 0,
                    updated_at: 0,
                }).collect();
                
                Ok(Response::new(SearchNodesResponse { results: nodes }))
            }
            Err(e) => Err(Status::internal(format!("Search failed: {}", e))),
        }
    }

    // ==================== Causal Reasoning Tools ====================

    async fn get_chain(
        &self,
        request: Request<GetChainRequest>,
    ) -> Result<Response<GetChainResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: get_chain(direction={}, depth={})", req.direction, req.max_depth);
        
        let max_depth = if req.max_depth > 0 { req.max_depth as usize } else { 3 };
        
        let result = match req.direction.as_str() {
            "backward" => {
                // От симптома к причине
                let anchor_text = if let Some(ref a) = req.anchor {
                    a.text.clone()
                } else {
                    return Err(Status::invalid_argument("Anchor text required for backward"));
                };
                self.chain.chain_backward(&anchor_text, max_depth).await
            }
            "forward_pre" => {
                // От причины к рискам
                let node_id = if let Some(ref a) = req.anchor {
                    NodeId(a.id.clone())
                } else {
                    return Err(Status::invalid_argument("Node ID required for forward_pre"));
                };
                self.chain.chain_forward_pre(&node_id, max_depth).await
            }
            "forward_post" => {
                // От действия к проверенным последствиям
                let node_id = if let Some(ref a) = req.anchor {
                    NodeId(a.id.clone())
                } else {
                    return Err(Status::invalid_argument("Node ID required for forward_post"));
                };
                self.chain.chain_forward_post(&node_id, max_depth).await
            }
            "post_mortem" => {
                // Полная цепочка до корневой причины
                let anchor_text = if let Some(ref a) = req.anchor {
                    a.text.clone()
                } else {
                    return Err(Status::invalid_argument("Anchor text required for post_mortem"));
                };
                self.chain.chain_post_mortem(&anchor_text).await
            }
            _ => return Err(Status::invalid_argument(format!("Unknown direction: {}", req.direction))),
        };
        
        match result {
            Ok(chain_result) => {
                // Собираем все node_id из записей
                let node_ids: Vec<NodeId> = chain_result.entries.iter().map(|e| e.node_id.clone()).collect();
                
                // Получаем все узлы пакетно
                let mut nodes_map = std::collections::HashMap::new();
                for nid in &node_ids {
                    match self.l2.get_node(nid).await {
                        Ok(Some(n)) => { nodes_map.insert(nid.clone(), n); }
                        _ => {}
                    }
                }
                
                let entries = chain_result.entries.into_iter().map(|e| {
                    let node = nodes_map.get(&e.node_id).map(|n| crate::graphmind::Node {
                        id: n.id.0.clone(),
                        node_type: node_type_to_proto(n.node_type),
                        content: n.content.clone(),
                        level: level_to_proto(n.level),
                        scope: String::new(),
                        status: status_to_proto(n.status),
                        tags: n.metadata.tags.clone(),
                        summary: String::new(),
                        parent_id: n.metadata.workspace_id.clone().unwrap_or_default(),
                        causal: None,
                        created_at: 0,
                        updated_at: 0,
                    });
                    
                    let relation = e.relation.map(|r| relation_to_proto(&r)).unwrap_or_default();
                    
                    crate::graphmind::ChainEntry {
                        node,
                        relation,
                        confidence: 1.0,
                    }
                }).collect();
                
                Ok(Response::new(GetChainResponse {
                    entries,
                }))
            }
            Err(e) => Err(Status::internal(format!("Chain traversal failed: {}", e))),
        }
    }

    async fn link_nodes(
        &self,
        request: Request<LinkNodesRequest>,
    ) -> Result<Response<LinkNodesResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: link_nodes(source={}, target={}, relation={})", req.source_id, req.target_id, req.relation);
        
        // Проверяем существование узлов
        let source_id = NodeId(req.source_id);
        let target_id = NodeId(req.target_id);
        
        let source_exists = self.l2.get_node(&source_id).await.map_err(|e| Status::internal(format!("Get node failed: {}", e)))?.is_some();
        let target_exists = self.l2.get_node(&target_id).await.map_err(|e| Status::internal(format!("Get node failed: {}", e)))?.is_some();
        
        if !source_exists {
            return Err(Status::not_found(format!("Source node {} not found", source_id.0)));
        }
        if !target_exists {
            return Err(Status::not_found(format!("Target node {} not found", target_id.0)));
        }
        
        // Преобразуем relation из proto строки
        let relation = match req.relation.as_str() {
            "related_to" => Relation::RelatedTo,
            "depends_on" => Relation::DependsOn,
            "implements" => Relation::Implements,
            "supersedes" => Relation::Supersedes,
            "leads_to" => Relation::LeadsTo,
            "explained_by" => Relation::ExplainedBy,
            "derived_from" => Relation::DerivedFrom,
            "inhibits" => Relation::Inhibits,
            "contradicts" => Relation::Contradicts,
            _ => return Err(Status::invalid_argument(format!("Unknown relation: {}", req.relation))),
        };
        
        // Создаём связь. workspace_id будет выставлен отдельной правкой
        // (P3-95d80a20 + P2-1fa23612) — gRPC пока не принимает workspace_id
        // в LinkEdgeRequest, поэтому None.
        let edge = Edge {
            id: EdgeId(Uuid::new_v4().to_string()),
            source: source_id,
            target: target_id,
            relation,
            confidence: 1.0,
            provenance: Provenance::Manual,
            workspace_id: None,
            created_at: Utc::now(),
        };
        
        // Сохраняем в L2Actor
        match self.l2.add_edge(&edge).await {
            Ok(edge_id) => {
                // Также добавляем в граф
                let mut graph = self.graph.write().await;
                graph.add_edge(edge);
                drop(graph);
                
                Ok(Response::new(LinkNodesResponse {
                    linked: true,
                }))
            }
            Err(e) => Err(Status::internal(format!("Failed to create link: {}", e))),
        }
    }

    async fn propose_causal_link(
        &self,
        request: Request<ProposeCausalLinkRequest>,
    ) -> Result<Response<ProposeCausalLinkResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: propose_causal_link(confidence={})", req.confidence);
        
        // TODO: inference через CausalEngine
        Err(Status::unimplemented("propose_causal_link not yet implemented"))
    }

    async fn find_contradictions(
        &self,
        request: Request<FindContradictionsRequest>,
    ) -> Result<Response<FindContradictionsResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: find_contradictions");
        
        // TODO: поиск противоречий через CausalEngine
        Err(Status::unimplemented("find_contradictions not yet implemented"))
    }

    async fn predict_risks(
        &self,
        request: Request<PredictRisksRequest>,
    ) -> Result<Response<PredictRisksResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: predict_risks(cause_id={})", req.cause_id);
        
        // TODO: прогноз рисков через InferenceActor
        Err(Status::unimplemented("predict_risks not yet implemented"))
    }

    async fn dream_reflection(
        &self,
        request: Request<DreamReflectionRequest>,
    ) -> Result<Response<DreamReflectionResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: dream_reflection(workspace_id={:?})", req.workspace_id);
        
        // TODO: dream reflection через InferenceActor
        Err(Status::unimplemented("dream_reflection not yet implemented"))
    }

    // ==================== Vector Search Tools ====================

    async fn vector_search(
        &self,
        request: Request<VectorSearchRequest>,
    ) -> Result<Response<VectorSearchResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: vector_search(query={}, top_k={})", req.query, req.top_k);
        
        // Пока нет реального embedding'а - используем keyword поиск
        let top_k = if req.top_k > 0 { req.top_k as usize } else { 10 };
        let keywords: Vec<String> = req.query.split_whitespace().map(|s| s.to_string()).collect();
        let dummy_vector = vec![0.0f32; 384];
        let filters = SearchFilters::default();
        
        match self.search.hybrid_search(&dummy_vector, &keywords, top_k, filters).await {
            Ok(results) => {
                let search_results = results.into_iter().map(|r| crate::graphmind::SearchResult {
                    node: Some(crate::graphmind::Node {
                        id: r.node_id.0,
                        node_type: node_type_to_proto(r.node_type),
                        content: r.content,
                        level: level_to_proto(r.level),
                        scope: String::new(),
                        status: status_to_proto(GraphStatus::Active),
                        tags: r.metadata.tags,
                        summary: String::new(),
                        parent_id: r.metadata.workspace_id.unwrap_or_default(),
                        causal: None,
                        created_at: 0,
                        updated_at: 0,
                    }),
                    score: 1.0,
                    match_type: "keyword".to_string(),
                }).collect();
                
                Ok(Response::new(VectorSearchResponse { results: search_results }))
            }
            Err(e) => Err(Status::internal(format!("Vector search failed: {}", e))),
        }
    }

    async fn memory_query(
        &self,
        request: Request<MemoryQueryRequest>,
    ) -> Result<Response<MemoryQueryResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: memory_query(query={}, depth={})", req.query, req.depth);
        
        let top_k = if req.depth > 0 { (req.depth * 5) as usize } else { 10 };
        
        // Keyword поиск через SearchActor
        let keywords: Vec<String> = req.query.split_whitespace().map(|s| s.to_string()).collect();
        let dummy_vector = vec![0.0f32; 384]; // Fallback на keyword
        let filters = SearchFilters::default();
        
        match self.search.hybrid_search(&dummy_vector, &keywords, top_k, filters).await {
            Ok(results) => {
                let nodes = results.into_iter().map(|r| ProtoNode {
                    id: r.node_id.0,
                    node_type: node_type_to_proto(r.node_type),
                    content: r.content,
                    level: level_to_proto(r.level),
                    scope: String::new(),
                    status: status_to_proto(GraphStatus::Active),
                    tags: r.metadata.tags,
                    summary: String::new(),
                    parent_id: r.metadata.workspace_id.unwrap_or_default(),
                    causal: None,
                    created_at: 0,
                    updated_at: 0,
                }).collect();
                
                Ok(Response::new(MemoryQueryResponse {
                    nodes,
                    edges: vec![],
                    summary: String::new(),
                }))
            }
            Err(e) => Err(Status::internal(format!("Memory query failed: {}", e))),
        }
    }

    async fn suggest_related(
        &self,
        request: Request<SuggestRelatedRequest>,
    ) -> Result<Response<SuggestRelatedResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: suggest_related(node_id={})", req.node_id);
        
        // 1. Получить узел по ID
        let node_id = NodeId(req.node_id);
        match self.l2.get_node(&node_id).await.map_err(|e| Status::internal(format!("Get node failed: {}", e)))? {
            Some(node) => {
                // 2. Искать похожие узлы по content
                let keywords: Vec<String> = node.content.split_whitespace().take(5).map(|s| s.to_string()).collect();
                let dummy_vector = vec![0.0f32; 384];
                let filters = SearchFilters::default();
                
                match self.search.hybrid_search(&dummy_vector, &keywords, 5, filters).await {
                    Ok(results) => {
                        // Исключить сам узел из результатов
                        let suggestions = results.into_iter()
                            .filter(|r| r.node_id != node_id)
                            .map(|r| crate::graphmind::RelatedNode {
                                node_id: r.node_id.0,
                                summary: r.content,
                                similarity: 0.5,
                                relation: "related_to".to_string(),
                            }).collect();
                        
                        Ok(Response::new(SuggestRelatedResponse { suggestions }))
                    }
                    Err(e) => Err(Status::internal(format!("Search failed: {}", e))),
                }
            }
            None => Err(Status::not_found(format!("Node {} not found", node_id.0))),
        }
    }

    // ==================== Workspace Management Tools ====================

    async fn detect_workspace_from_context(
        &self,
        request: Request<DetectWorkspaceFromContextRequest>,
    ) -> Result<Response<DetectWorkspaceFromContextResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: detect_workspace_from_context(cwd={})", req.cwd);
        
        match self.workspace_manager.detect_workspace(&req.cwd).await {
            Ok(Some(ws_id)) => Ok(Response::new(DetectWorkspaceFromContextResponse {
                workspace_id: ws_id.clone(),
                name: ws_id,
                path: req.cwd.clone(),
            })),
            Ok(None) => Err(Status::not_found("No workspace detected".to_string())),
            Err(e) => Err(Status::internal(format!("Failed to detect workspace: {}", e))),
        }
    }

    async fn create_workspace(
        &self,
        request: Request<CreateWorkspaceRequest>,
    ) -> Result<Response<CreateWorkspaceResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: create_workspace(name={}, path={})", req.name, req.path);
        
        let path = if req.path.is_empty() { None } else { Some(req.path) };
        
        match self.workspace_manager.create_workspace(req.name, path).await {
            Ok(workspace) => {
                Ok(Response::new(CreateWorkspaceResponse {
                    workspace_id: workspace.id,
                    created_at: String::new(),
                }))
            }
            Err(e) => Err(Status::internal(format!("Failed to create workspace: {}", e))),
        }
    }

    async fn switch_workspace(
        &self,
        request: Request<SwitchWorkspaceRequest>,
    ) -> Result<Response<SwitchWorkspaceResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: switch_workspace(workspace_id={})", req.workspace_id);
        
        match self.workspace_manager.switch_workspace(&req.workspace_id).await {
            Ok(true) => Ok(Response::new(SwitchWorkspaceResponse {
                switched: true,
                previous_id: String::new(),
            })),
            Ok(false) => Err(Status::not_found(format!("Workspace {} not found", req.workspace_id))),
            Err(e) => Err(Status::internal(format!("Failed to switch workspace: {}", e))),
        }
    }

    async fn list_workspaces(
        &self,
        request: Request<ListWorkspacesRequest>,
    ) -> Result<Response<ListWorkspacesResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: list_workspaces(status={})", req.status);
        
        // Определяем фильтр по статусу
        let status_filter = match req.status.as_str() {
            "active" => Some(WorkspaceStatus::Active),
            "archived" => Some(WorkspaceStatus::Archived),
            _ => None,
        };
        
        match self.workspace_manager.list_workspaces(status_filter).await {
            Ok(workspaces) => {
                let ws_list = workspaces.into_iter().map(|ws| crate::graphmind::Workspace {
                    id: ws.id,
                    name: ws.name,
                    path: ws.path.unwrap_or_default(),
                    status: match ws.status {
                        WorkspaceStatus::Active => "active".to_string(),
                        WorkspaceStatus::Archived => "archived".to_string(),
                    },
                    created_at: ws.created_at.timestamp() as i64,
                }).collect();
                
                Ok(Response::new(ListWorkspacesResponse {
                    workspaces: ws_list,
                }))
            }
            Err(e) => Err(Status::internal(format!("Failed to list workspaces: {}", e))),
        }
    }

    async fn archive_workspace(
        &self,
        request: Request<ArchiveWorkspaceRequest>,
    ) -> Result<Response<ArchiveWorkspaceResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: archive_workspace(workspace_id={})", req.workspace_id);
        
        match self.workspace_manager.archive_workspace(&req.workspace_id).await {
            Ok(true) => Ok(Response::new(ArchiveWorkspaceResponse {
                archived: true,
            })),
            Ok(false) => Err(Status::not_found(format!("Workspace {} not found", req.workspace_id))),
            Err(e) => Err(Status::internal(format!("Failed to archive workspace: {}", e))),
        }
    }

    // ==================== Cross-Workspace Tools ====================

    async fn fetch_from_workspace(
        &self,
        request: Request<FetchFromWorkspaceRequest>,
    ) -> Result<Response<FetchFromWorkspaceResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: fetch_from_workspace(workspace_id={})", req.workspace_id);
        
        // Проверяем существование workspace
        match self.workspace_manager.get_workspace(&req.workspace_id).await {
            Ok(Some(_)) => {
                // Пока нет реализации cross-workspace query - возвращаем заглушку
                Ok(Response::new(FetchFromWorkspaceResponse {
                    nodes: vec![],
                    edges: vec![],
                    summary: String::new(),
                }))
            }
            Ok(None) => Err(Status::not_found(format!("Workspace {} not found", req.workspace_id))),
            Err(e) => Err(Status::internal(format!("Failed to fetch workspace: {}", e))),
        }
    }

    async fn suggest_cross_workspace_links(
        &self,
        request: Request<SuggestCrossWorkspaceLinksRequest>,
    ) -> Result<Response<SuggestCrossWorkspaceLinksResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: suggest_cross_workspace_links(from={}, to={})", req.from_workspace_id, req.to_workspace_id);
        
        // Проверяем существование workspace'ов
        let from_exists = self.workspace_manager.get_workspace(&req.from_workspace_id).await.map_err(|e| Status::internal(format!("Get workspace failed: {}", e)))?.is_some();
        let to_exists = self.workspace_manager.get_workspace(&req.to_workspace_id).await.map_err(|e| Status::internal(format!("Get workspace failed: {}", e)))?.is_some();
        
        if !from_exists || !to_exists {
            return Err(Status::not_found("One or both workspaces not found".to_string()));
        }
        
        // Пока нет реализации cross-workspace link suggestions - возвращаем заглушку
        Ok(Response::new(SuggestCrossWorkspaceLinksResponse {
            links: vec![],
        }))
    }

    async fn find_workspace_overlaps(
        &self,
        request: Request<FindWorkspaceOverlapsRequest>,
    ) -> Result<Response<FindWorkspaceOverlapsResponse>, Status> {
        info!("RPC: find_workspace_overlaps");
        
        // Пока нет реализации анализа пересечений между workspace'ами
        // В будущем: семантический анализ контента узлов из разных workspace
        Ok(Response::new(FindWorkspaceOverlapsResponse {
            overlaps: vec![],
        }))
    }

    // ==================== Memory Lifecycle Tools ====================

    async fn consolidate_workspace(
        &self,
        request: Request<ConsolidateWorkspaceRequest>,
    ) -> Result<Response<ConsolidateWorkspaceResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: consolidate_workspace(force={})", req.force);
        
        // 1. Получить все записи из S0
        let s0_entries = self.s0.get_recent(100).await; // берём последние 100 записей
        
        if s0_entries.is_empty() {
            return Ok(Response::new(ConsolidateWorkspaceResponse {
                consolidated: false,
                new_l2_count: 0,
            }));
        }
        
        // 2. Для каждой записи S0 создать/обновить узлы L2
        let mut new_l2_count = 0;
        for entry in &s0_entries {
            // Простая эвристика: создаём atom-узел для каждого action
            let node = GraphNode::new(NodeType::Atom, entry.summary.clone());
            let node_id = NodeId(entry.id.clone());
            
            match self.l2.add_node(&node).await {
                Ok(_) => {
                    new_l2_count += 1;
                    // Также добавляем в граф
                    let mut graph = self.graph.write().await;
                    graph.add_node(node);
                    drop(graph);
                }
                Err(e) => {
                    warn!("Failed to consolidate entry {}: {}", entry.id, e);
                }
            }
        }
        
        // 3. Очистить S0 после консолидации
        // В реальной реализации S0Actor.clear() или eviction policy
        
        Ok(Response::new(ConsolidateWorkspaceResponse {
            consolidated: true,
            new_l2_count,
        }))
    }

    async fn search_l0_clusters(
        &self,
        request: Request<SearchL0ClustersRequest>,
    ) -> Result<Response<SearchL0ClustersResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: search_l0_clusters(query={})", req.query);
        
        // Пока нет L0Actor.search() - используем keyword поиск через SearchActor
        let keywords: Vec<String> = req.query.split_whitespace().map(|s| s.to_string()).collect();
        let dummy_vector = vec![0.0f32; 384];
        let filters = SearchFilters::default();
        
        match self.search.hybrid_search(&dummy_vector, &keywords, 10, filters).await {
            Ok(results) => {
                let clusters: Vec<crate::graphmind::L0Cluster> = results.into_iter().map(|r| crate::graphmind::L0Cluster {
                    id: r.node_id.0.clone(),
                    name: r.content.chars().take(50).collect(),
                    member_ids: vec![r.node_id.0],
                    coherence: 0.8,
                }).collect();
                
                Ok(Response::new(SearchL0ClustersResponse { clusters }))
            }
            Err(e) => Err(Status::internal(format!("L0 cluster search failed: {}", e))),
        }
    }

    async fn route_l1(
        &self,
        request: Request<RouteL1Request>,
    ) -> Result<Response<RouteL1Response>, Status> {
        let req = request.into_inner();
        info!("RPC: route_l1(hub_id={})", req.hub_id);
        
        // Пока нет L1Actor.search() - используем keyword поиск через SearchActor
        let keywords: Vec<String> = req.hub_id.split_whitespace().map(|s| s.to_string()).collect();
        let dummy_vector = vec![0.0f32; 384];
        let filters = SearchFilters::default();
        
        match self.search.hybrid_search(&dummy_vector, &keywords, 10, filters).await {
            Ok(results) => {
                let domains: Vec<crate::graphmind::L1Domain> = results.into_iter().map(|r| crate::graphmind::L1Domain {
                    id: r.node_id.0.clone(),
                    name: r.content.chars().take(50).collect(),
                    hub_id: req.hub_id.clone(),
                    relevance: 0.8,
                }).collect();
                
                Ok(Response::new(RouteL1Response { domains }))
            }
            Err(e) => Err(Status::internal(format!("L1 routing failed: {}", e))),
        }
    }

    // ==================== Admin / System Tools ====================

    async fn bootstrap_memory(
        &self,
        request: Request<BootstrapMemoryRequest>,
    ) -> Result<Response<BootstrapMemoryResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: bootstrap_memory(cwd={}, query={})", req.cwd, req.query);
        
        // 1. Детектируем workspace
        let workspace_id = format!("ws_{}", req.cwd.replace("/", "_").replace("\\", "_").replace(":", ""));
        
        // 2. Выполняем memory_query
        let top_k = 10;
        let keywords: Vec<String> = req.query.split_whitespace().map(|s| s.to_string()).collect();
        let dummy_vector = vec![0.0f32; 384];
        let filters = SearchFilters::default();
        
        let nodes = match self.search.hybrid_search(&dummy_vector, &keywords, top_k, filters).await {
            Ok(results) => results.into_iter().map(|r| ProtoNode {
                id: r.node_id.0,
                node_type: node_type_to_proto(r.node_type),
                content: r.content,
                level: level_to_proto(r.level),
                scope: String::new(),
                status: status_to_proto(GraphStatus::Active),
                tags: r.metadata.tags,
                summary: String::new(),
                parent_id: r.metadata.workspace_id.unwrap_or_default(),
                causal: None,
                created_at: 0,
                updated_at: 0,
            }).collect(),
            Err(_) => vec![],
        };
        
        Ok(Response::new(BootstrapMemoryResponse {
            workspace_id,
            workspace_name: req.cwd.clone(),
            recent_nodes: nodes,
            recent_actions: vec![],
        }))
    }

    async fn get_index_status(
        &self,
        request: Request<GetIndexStatusRequest>,
    ) -> Result<Response<GetIndexStatusResponse>, Status> {
        info!("RPC: get_index_status");
        
        // Получить статистику SearchActor
        match self.search.stats().await {
            Ok(stats) => Ok(Response::new(GetIndexStatusResponse {
                indexed_nodes: stats.vector_count as i32,
                last_rebuild: String::new(),
                pending_sync: 0,
                vector_backend: "in-memory".to_string(),
            })),
            Err(e) => Err(Status::internal(format!("Failed to get index stats: {}", e))),
        }
    }

    // ==================== Trust + Curiosity Tools ====================

    async fn verify_input(
        &self,
        request: Request<VerifyInputRequest>,
    ) -> Result<Response<VerifyInputResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: verify_input(source_id={})", req.source_id);
        
        // Определяем тип источника (пока UserDirect по умолчанию)
        let source_type = crate::actors::SourceType::UserDirect;
        
        let report = self.trust_firewall.verify_input(&req.source_id, source_type, &req.content).await;
        
        Ok(Response::new(VerifyInputResponse {
            trust_score: report.trust_score,
            firewall_confidence: report.firewall_confidence,
            recommendation: format!("{:?}", report.recommendation),
            source_trust: None,
            consistency: None,
            verifiability: None,
            intent: None,
            tone_anomaly: None,
            action_analysis: None,
            alternative_explanations: vec![],
            warnings: vec![],
        }))
    }

    async fn get_irritation_report(
        &self,
        request: Request<GetIrritationReportRequest>,
    ) -> Result<Response<GetIrritationReportResponse>, Status> {
        info!("RPC: get_irritation_report");
        
        let report = self.curiosity_engine.get_report().await;
        
        Ok(Response::new(GetIrritationReportResponse {
            score: report.irritation_score,
            emotional_state: format!("{:?}", report.emotional_state),
            tone_hint: String::new(),
            open_tasks: report.active_tasks as i32,
            unresolved_contradictions: 0,
            avg_confidence: 0.0,
            top_curiosity_tasks: vec![],
        }))
    }

    async fn list_curiosity_tasks(
        &self,
        request: Request<ListCuriosityTasksRequest>,
    ) -> Result<Response<ListCuriosityTasksResponse>, Status> {
        info!("RPC: list_curiosity_tasks");
        
        let tasks = self.curiosity_engine.get_active_tasks().await;
        
        let task_list = tasks.into_iter().map(|t| crate::graphmind::CuriosityTask {
            id: t.id,
            title: t.description.clone(),
            uncertainty_type: String::new(),
            priority: format!("{:.2}", t.priority),
            status: match t.status {
                crate::actors::TaskStatus::Pending => "Open".to_string(),
                crate::actors::TaskStatus::InProgress => "Investigating".to_string(),
                crate::actors::TaskStatus::Completed => "Resolved".to_string(),
                crate::actors::TaskStatus::Abandoned => "Closed".to_string(),
            },
            description: t.description,
            related_nodes: vec![],
        }).collect();
        
        Ok(Response::new(ListCuriosityTasksResponse {
            tasks: task_list,
        }))
    }

    async fn close_curiosity_task(
        &self,
        request: Request<CloseCuriosityTaskRequest>,
    ) -> Result<Response<CloseCuriosityTaskResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: close_curiosity_task(task_id={})", req.task_id);
        
        // Пытаемся завершить задачу
        if self.curiosity_engine.complete_task(&req.task_id).await {
            Ok(Response::new(CloseCuriosityTaskResponse {
                closed: true,
                closure_warning: String::new(),
            }))
        } else {
            Err(Status::not_found(format!("Task {} not found", req.task_id)))
        }
    }

    async fn generate_verification_questions(
        &self,
        request: Request<GenerateVerificationQuestionsRequest>,
    ) -> Result<Response<GenerateVerificationQuestionsResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: generate_verification_questions(user_id={})", req.user_id);
        
        // Пока нет полной реализации генерации вопросов
        // В будущем: генерировать вопросы на основе trust signals
        Ok(Response::new(GenerateVerificationQuestionsResponse {
            questions: vec![
                crate::graphmind::VerificationQuestion {
                    question_id: "q1".to_string(),
                    situation: "Вы уверены в этой информации?".to_string(),
                    prediction: None,
                }
            ],
        }))
    }

    async fn compare_verification_response(
        &self,
        request: Request<CompareVerificationResponseRequest>,
    ) -> Result<Response<CompareVerificationResponseResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: compare_verification_response(question_id={})", req.question_id);
        
        // Упрощённая реализация: если ответ содержит "да" или "уверен" - высокий score
        let actual_response = req.actual_response.to_lowercase();
        let matches = actual_response.contains("да") || actual_response.contains("уверен");
        let score = if matches { 0.8 } else { 0.3 };
        
        Ok(Response::new(CompareVerificationResponseResponse {
            causal_match_score: score,
            anomaly_explanation: if matches { String::new() } else { "Ответ не соответствует ожидаемому паттерну".to_string() },
        }))
    }

    async fn finalize_verification(
        &self,
        request: Request<FinalizeVerificationRequest>,
    ) -> Result<Response<FinalizeVerificationResponse>, Status> {
        let req = request.into_inner();
        info!("RPC: finalize_verification(user_id={})", req.user_id);
        
        // Обновить репутацию источника на основе верификации
        self.trust_firewall.update_reputation(&req.user_id, true).await;
        
        Ok(Response::new(FinalizeVerificationResponse {
            is_authentic: true,
            match_score: 0.9,
            anomalies: vec![],
            recommendation: "Accept".to_string(),
        }))
    }
}
