use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Server;
use tracing::{info, error};

use crate::actors::{S0Actor, L2Actor, L1Actor, L0Actor, GKLactor, SearchActor, ChainActor, CausalEngine, InferenceActor, WorkspaceManager, CuriosityEngine, TrustFirewall};

use crate::grpc::handlers::MemoryServiceHandler;
use crate::graph::Graph;

/// gRPC сервер для MCP Bridge.
///
/// Запускает MemoryService на указанном адресе (по умолчанию 0.0.0.0:50051).
pub struct GrpcServer {
    addr: SocketAddr,
}

impl GrpcServer {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    /// Запуск сервера с переданными акторами.
    pub async fn run(
        &self,
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
    ) -> Result<(), Box<dyn std::error::Error>> {
        let service = MemoryServiceHandler::new(
            s0, l2, l1, l0, gkl, search, chain, causal_engine, inference, workspace_manager, curiosity_engine, trust_firewall, graph,
        );

        info!("Starting gRPC MCP Bridge on {}", self.addr);

        Server::builder()
            .add_service(crate::graphmind::memory_service_server::MemoryServiceServer::new(service))
            .serve(self.addr)
            .await?;

        Ok(())
    }
}

/// Запустить gRPC сервер с указанными акторами.
///
/// Если GRAPHMIND_GRPC_ADDR не задан, используется 0.0.0.0:50051.
pub async fn start_grpc_server(
    addr: Option<SocketAddr>,
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
) -> Result<(), Box<dyn std::error::Error>> {
    let listen_addr = addr.unwrap_or_else(|| "0.0.0.0:50051".parse().unwrap());
    let server = GrpcServer::new(listen_addr);
    server.run(s0, l2, l1, l0, gkl, search, chain, causal_engine, inference, workspace_manager, curiosity_engine, trust_firewall, graph).await
}
