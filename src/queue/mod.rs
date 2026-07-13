//! Memory Queue — pending_actions.json storage and processor
//!
//! Sub-agents write actions here; main agent processes them periodically.

mod pending_store;
mod processor;

pub use pending_store::PendingStore;
pub use processor::QueueProcessor;

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Action types supported by the queue
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ActionType {
    RecordAction,
    ProposeNewMemory,
    FetchFromWorkspace,
}

/// A pending action written by a sub-agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingAction {
    pub id: String,
    #[serde(rename = "type")]
    pub action_type: ActionType,
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_text: Option<String>,
    #[serde(default)]
    pub related_nodes: Vec<String>,
    pub source: String,
    pub timestamp: DateTime<Utc>,
    #[serde(default)]
    pub status: PendingStatus,
    // For propose_new_memory
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

impl PendingAction {
    pub fn new_record(summary: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            action_type: ActionType::RecordAction,
            summary: summary.into(),
            raw_text: None,
            related_nodes: Vec::new(),
            source: source.into(),
            timestamp: Utc::now(),
            status: PendingStatus::Pending,
            level: None,
            node_type: None,
            content: None,
            parent_id: None,
            scope: None,
        }
    }

    pub fn new_propose(
        summary: impl Into<String>,
        content: impl Into<String>,
        level: impl Into<String>,
        node_type: impl Into<String>,
        parent_id: impl Into<String>,
        scope: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            action_type: ActionType::ProposeNewMemory,
            summary: summary.into(),
            raw_text: None,
            related_nodes: Vec::new(),
            source: source.into(),
            timestamp: Utc::now(),
            status: PendingStatus::Pending,
            level: Some(level.into()),
            node_type: Some(node_type.into()),
            content: Some(content.into()),
            parent_id: Some(parent_id.into()),
            scope: Some(scope.into()),
        }
    }
}

/// Status of a pending action
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PendingStatus {
    Pending,
    Processing,
    Done,
    Failed,
}

impl Default for PendingStatus {
    fn default() -> Self {
        Self::Pending
    }
}

/// Queue file path helper
pub fn queue_path(workspace_root: &std::path::Path) -> std::path::PathBuf {
    workspace_root.join("S0").join("pending_actions.json")
}
