//! Storage Actors -- S0/L2/L1/L0/GKL memory tiers.
//!
//! Based on TECH-SPEC.md Section 4 Storage Architecture.

mod causal_engine;
mod config;
mod chain;
mod consolidate;
mod coordinator;
mod curiosity_engine;
mod embedding;
mod gkl;
mod inference_actor;
mod l0;
mod l1;
mod l2;
mod llm_client;
mod plan_actor;
mod orchestrator;
mod search;
mod s0;
mod trust_firewall;
mod workspace_manager;

pub use causal_engine::{CausalEngine, InferredRelation, CalibratedConfidence, Contradiction, ConflictType, InferenceContext};
pub use config::{Config, McpMode, BackendKind, LlmConfig, LlmProvider, EmbeddingConfig, EmbeddingProviderKind, RocksDbConfig, QueueConfig};
pub use chain::{ChainActor, ChainEntry, ChainResult};
pub use consolidate::{ConsolidateRunner, ConsolidateStats};
pub use coordinator::{MemoryOrchestrator, MemoryEvent, CoordinatorCfg, DecisionLog};
pub use curiosity_engine::{CuriosityEngine, UncertaintyMarker, UncertaintyType, CuriosityTask, EmotionalState, TaskStatus};
pub use embedding::EmbeddingProvider;
pub use gkl::{GKLactor, GklNode, GklStats, GklSearchResult};
pub use inference_actor::{InferenceActor, GeneratedRule, RiskPrediction, PredictedEffect, RiskLevel, GeneralizationLevel};
pub use l0::{L0Actor, L0Node, L0AutogenResult, L0Stats};
pub use l1::{L1Actor, L1Domain, AutogenResult, L1Stats};
pub use l2::L2Actor;
pub use llm_client::LlmClient;
pub use plan_actor::{PlanActor, Plan, PlanLevel, PlanStatus};
pub use orchestrator::{OrchestratorActor, SizeCategory};
pub use search::{SearchActor, SearchQuery, SearchResult, SearchFilters, VectorIndex, MemoryIndex, VectorNode, SearchStats};
pub use s0::{S0Actor, S0Entry};
pub use trust_firewall::{TrustFirewall, TrustReport, TrustDecision, SourceTrust, SourceType, GateOutcome};
pub use workspace_manager::{WorkspaceManager, Workspace, WorkspaceStatus};

use async_trait::async_trait;

/// Common interface for all storage actors.
#[async_trait]
pub trait Actor: Send + Sync {
    /// Human-readable actor name (for logging).
    fn name(&self) -> &str;

    /// Number of items currently stored.
    async fn size(&self) -> usize;
}
